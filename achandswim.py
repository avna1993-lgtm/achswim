import time
import re
import yaml
from dataclasses import dataclass
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, Optional, List, Dict, Tuple
from ftfcu_appworx import Apwx, JobTime
from oracledb import Connection as DbConnection
from datetime import datetime

_version_ = "1.051"


class AppWorxEnum(StrEnum):
    # HOST = auto()
    # SID = auto()
    # PORT = auto()
    TNS_SERVICE_NAME = auto()
    INPUT_ACH_FILE_FULLPATH = auto()
    OUTPUT_ACH_FILE_FULLPATH = auto()
    OUTPUT_SWIM_FILE_FULLPATH = auto()
    GL_ACCTNBR = auto()
    CASH_BOX_NUMBER = auto()
    DEBUG = auto()
    REASONTYPE = auto()
    HOLD_CODE = auto()
    HOLD_LENGTH = auto()
    # REPORT = auto()
    # REPORT_PATH = auto()
    EFFDATE = auto()
    FTFCU_ROUTENBR = auto()
    CONFIG_FILE_PATH = auto()
    RPTONLY_YN = auto()

    def __str__(self):
        return self.name


@dataclass
class ScriptData:
    apwx: Apwx
    dbh: DbConnection
    config: Any
    newach_data: List[List[str]]
    all_onus_data: List[List[Any]]
    bad_desc: List[str]
    entry_hash_cnt: int
    total_credit: int
    total_debit: int
    total_record_cnt: int
    file_header: str
    holddate: str
    swim_file_info: Dict[str, Any]
    swm_lines: List[str]
    gl_cr_line: str
    sth: Dict[str, Any]


def run(apwx: Apwx, current_time: float) -> bool:
    """Main execution function for ACH to SWIM conversion"""
    print(f"Job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 35)

    script_data = initialize(apwx)

    if not parse_ach_file(script_data):
        return exit_early(script_data, 'parseACHFile', True)

    if not write_new_ach_file(script_data):
        return exit_early(script_data, 'writeNewACHFile', True)

    if not get_swim_file_info(script_data):
        return exit_early(script_data, 'getSwimFileInfo', True)

    if not get_hold_biz_date(script_data):
        return exit_early(script_data, 'getholdbizdate', True)

    if not loop_through_onus_data_create_swim_file_and_stage_holds(script_data):
        return exit_early(script_data, 'loopThroughOnUsDataCreateSWIMFileAndStageHolds', True)

    if not is_report_only(script_data.apwx):
        script_data.dbh.commit()
        print("Database changes committed.")
    else:
        print("Report-only mode: No database changes committed.")

    disconnect_db(script_data)

    print("-" * 35)
    print(f"Job completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return True


def initialize(apwx: Apwx) -> ScriptData:
    """Initialize script data and database connection"""
    dbh = apwx.db_connect(autocommit=False)
    config = get_config(apwx)

    script_data = ScriptData(
        apwx=apwx,
        dbh=dbh,
        config=config,
        newach_data=[],
        all_onus_data=[],
        bad_desc=[],
        entry_hash_cnt=0,
        total_credit=0,
        total_debit=0,
        total_record_cnt=0,
        file_header="",
        holddate="",
        swim_file_info={},
        swm_lines=[],
        gl_cr_line="",
        sth={}
    )

    prepare_statements(script_data)
    return script_data


def parse_ach_file(script_data: ScriptData) -> bool:
    """Parse ACH file and extract on-us data"""
    current_batch = []
    onus_data = []
    is_onus = 0
    ftfcu_routenbr = script_data.apwx.args.FTFCU_ROUTENBR

    try:
        with open(script_data.apwx.args.INPUT_ACH_FILE_FULLPATH, 'r') as achfile:
            for line in achfile:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('101'):  # File Header Record
                    """
                    ACH File Header Record (Record Type Code 1):
                    Position 1: Record Type Code (1)
                    Position 2-3: Priority Code (01)
                    Position 4-13: Immediate Destination (061000146)
                    Position 14-23: Immediate Origin (3211803792)
                    Position 24-29: File Creation Date (250811)
                    Position 30-33: File Creation Time (0832)
                    Position 34: File ID Modifier (A)
                    Position 35-37: Record Size (094)
                    Position 38-39: Blocking Factor (10)
                    Position 40: Format Code (1)
                    Position 41-63: Immediate Destination Name (FRB Atlanta)
                    Position 64-86: Immediate Origin Name (First Tech Federal Cred)
                    Position 87-94: Reference Code
                    """
                    script_data.file_header = line
                    continue

                elif line.startswith('9'):  # File Control Record
                    """
                    ACH File Control Record (Record Type Code 9):
                    Position 1: Record Type Code (9)
                    Position 2-7: Batch Count
                    Position 8-13: Block Count
                    Position 14-21: Entry/Addenda Count
                    Position 22-31: Entry Hash
                    Position 32-43: Total Debit Entry Dollar Amount
                    Position 44-55: Total Credit Entry Dollar Amount
                    Position 56-94: Reserved
                    """
                    break  # end of file

                elif line.startswith('8'):  # Batch Control Record
                    """
                    ACH Batch Control Record (Record Type Code 8):
                    Position 1: Record Type Code (8)
                    Position 2-4: Service Class Code
                    Position 5-10: Entry/Addenda Count
                    Position 11-20: Entry Hash
                    Position 21-32: Total Debit Entry Dollar Amount
                    Position 33-44: Total Credit Entry Dollar Amount
                    Position 45-54: Company Identification
                    Position 55-73: Message Authentication Code
                    Position 74-79: Reserved
                    Position 80-87: Originating DFI Identification
                    Position 88-94: Batch Number
                    """
                    if current_batch:
                        if is_onus and onus_data:
                            last_line = onus_data.pop()
                            current_batch.append(last_line)
                            onus_data = []
                            is_onus = 0
                        script_data.newach_data.append(current_batch[:])
                        current_batch = []
                    continue

                elif line.startswith('5'):  # Batch Header Record
                    """
                    ACH Batch Header Record (Record Type Code 5):
                    Position 1: Record Type Code (5)
                    Position 2-4: Service Class Code (200=Mixed, 220=Credits Only, 225=Debits Only)
                    Position 5-20: Company Name
                    Position 21-40: Company Discretionary Data
                    Position 41-50: Company Identification
                    Position 51-53: Standard Entry Class Code
                    Position 54-63: Company Entry Description
                    Position 64-69: Company Descriptive Date
                    Position 70-75: Effective Entry Date
                    Position 76-78: Settlement Date
                    Position 79: Originator Status Code
                    Position 80-87: Originating DFI Identification
                    Position 88-94: Batch Number
                    """
                    current_batch = [line]
                    continue

                elif line.startswith('7'):  # Addenda Record
                    """
                    ACH Addenda Record (Record Type Code 7):
                    Position 1: Record Type Code (7)
                    Position 2-3: Addenda Type Code (05=Standard)
                    Position 4-83: Payment Related Information
                    Position 84-87: Addenda Sequence Number
                    Position 88-94: Entry Detail Sequence Number
                    """
                    if not is_onus:
                        current_batch.append(line)
                    elif 'EXT OAO' not in line:
                        last_line = onus_data.pop()
                        current_batch.append(last_line)
                        current_batch.append(line)
                        onus_data = []
                        is_onus = 0
                    else:
                        # Extract description from addenda record for OAO transfers
                        # Position 4-83: Payment Related Information (80 characters)
                        match = re.match(r'^705(.+)\d{3}$', line)
                        if match:
                            description = match.group(1).strip()  # Extract payment info
                            description = description[:128]  # limit to 128 characters
                            onus_data.append(description)
                            script_data.all_onus_data.append(onus_data[:])
                            onus_data = []
                            is_onus = 0
                    continue

                elif line.startswith('6'):  # Entry Detail Record
                    """
                    ACH Entry Detail Record (Record Type Code 6):
                    Position 1: Record Type Code (6)
                    Position 2-3: Transaction Code (22=Checking Credit, 23=Checking Debit, 32=Savings Credit, 33=Savings Debit)
                    Position 4-11: Receiving DFI Identification (8 digits)
                    Position 12: Check Digit
                    Position 13-29: DFI Account Number (17 characters, right justified, space filled)
                    Position 30-39: Amount (10 digits, zero filled)
                    Position 40-54: Individual Identification Number (15 characters)
                    Position 55-76: Individual Name (22 characters)
                    Position 77: Discretionary Data
                    Position 78: Addenda Record Indicator (0=No Addenda, 1=Addenda Included)
                    Position 79-94: Trace Number (15 digits: 8-digit ODFI + 7-digit sequence)
                    """
                    if is_onus and onus_data:
                        last_line = onus_data.pop()
                        current_batch.append(last_line)
                        onus_data = []
                        is_onus = 0

                    # Check if this is an on-us transaction (our routing number)
                    if re.match(f'^6[23]2{ftfcu_routenbr}', line):
                        rtnbr, acctnbr, ach_tc, amt, trace_nbr, sequence_nbr, has_addenda = parse_ach_detail_record(
                            line)
                        is_onus = sequence_nbr
                        onus_data = [rtnbr, acctnbr, ach_tc, amt, trace_nbr, line]

                        continue
                    else:
                        current_batch.append(line)
                        continue

        return True
    except Exception as e:
        print(f"Error parsing ACH file: {e}")
        return False


def parse_ach_detail_record(ach_detail_rec: str) -> Tuple[str, str, str, int, str, str, str]:
    """Parse ACH detail record and extract components"""
    # Position 4-12: Receiving DFI Identification (9 digits including check digit)
    rtnbr = ach_detail_rec[3:12]  # routing number (9 digits)

    # Position 13-29: DFI Account Number (17 characters, right justified, space filled)
    acctnbr = ach_detail_rec[12:29].replace(' ', '')  # account number (remove spaces)

    # Position 2-3: Transaction Code (22=Checking Credit, 23=Checking Debit, etc.)
    ach_tc = ach_detail_rec[1:3]  # transaction code (2 digits)

    # Position 30-39: Amount (10 digits, zero filled, in cents)
    amt = int(ach_detail_rec[29:39].lstrip('0') or '0')  # amount in cents

    # Position 79-94: Trace Number (15 digits: 8-digit ODFI + 7-digit sequence)
    trace_nbr = ach_detail_rec[79:94]  # trace number (15 digits)

    # Position 78: Addenda Record Indicator (0=No Addenda, 1=Addenda Included)
    has_addenda = ach_detail_rec[78]  # addenda indicator

    # Last 7 digits of trace number = sequence number
    sequence_nbr = trace_nbr[-7:]  # sequence number (7 digits)

    return rtnbr, acctnbr, ach_tc, amt, trace_nbr, sequence_nbr, has_addenda


def loop_through_onus_data_create_swim_file_and_stage_holds(script_data: ScriptData) -> bool:
    """Process on-us data, create SWIM file and stage holds"""
    try:
        for data in script_data.all_onus_data:
            # Convert amount from cents to dollars
            amount = f"{(data[3] * 0.01):.2f}"

            # Always create SWIM lines for both report-only and normal mode
            script_data.swim_file_info['glDrAmt'] += data[3]  # Add to GL debit amount
            swim_line = create_swim_line(script_data, data)
            script_data.swm_lines.append(swim_line)
            script_data.swim_file_info['swimCt'] += 1

            # Only insert holds if not in report-only mode
            if not is_report_only(script_data.apwx):
                if not insert_ach_hold(script_data, {
                    'acctnbr': data[1],  # Account number from position 13-29
                    'amount': amount,  # Amount converted to dollars
                    'tracenbr': data[4]  # Trace number from position 79-94
                }):
                    print(f"Could not stage hold for account: {data[1]}")
            else:
                print(f"Report-only mode: Would stage hold for account {data[1]} amount ${amount}")

        offset_gl(script_data)
        script_data.swm_lines.append(script_data.gl_cr_line)

        # Write SWIM file (generated in both report-only and normal mode)
        with open(script_data.apwx.args.OUTPUT_SWIM_FILE_FULLPATH, 'w') as f:
            f.write('\n'.join(script_data.swm_lines) + '\n')

        return True
    except Exception as e:
        print(f"Error processing on-us data: {e}")
        return False


def insert_ach_hold(script_data: ScriptData, data: Dict[str, Any]) -> bool:
    """Insert ACH hold record into database"""
    try:
        # Get SQL from config file
        sql = script_data.config.get("insert_ach_hold")

        cursor = script_data.dbh.cursor()
        cursor.execute(sql, {
            'acctnbr': data['acctnbr'],
            'amount': data['amount'],
            'reason': script_data.apwx.args.REASONTYPE,
            'reasontype': script_data.apwx.args.REASONTYPE,
            'holddate': script_data.holddate,
            'tracenbr': data['tracenbr'],
            'acctholdcd': script_data.apwx.args.HOLD_CODE
        })
        return True
    except Exception as e:
        print(f"Could not insert hold for {data['acctnbr']}: {e}")
        return False


def create_swim_line(script_data: ScriptData, data: List[Any]) -> str:
    """Create SWIM file line from ACH data"""
    cash_box_nbr = script_data.apwx.args.CASH_BOX_NUMBER
    amt = data[3]  # Amount in cents from ACH record position 30-39

    # Get transaction code mapping from SWIM file info
    osi_tc = script_data.swim_file_info['achToDnaTc'][data[2]]['osiTC']  # data[2] = transaction code
    desc = script_data.swim_file_info['achToDnaTc'][data[2]]['desc']

    # Format SWIM line using the format string from swim_file_info
    line = script_data.swim_file_info['swimFmt'].format(
        data[1],  # Account number from position 13-29
        osi_tc,  # OSI transaction code
        amt,  # Amount in cents
        '',  # Empty field
        script_data.swim_file_info['effDate'],  # Effective date
        desc,  # Description
        data[4],  # Trace number from position 79-94
        cash_box_nbr,  # Cash box number
        '', '',  # Empty fields
        'EL', 'INTR', 'IMED',  # Fixed values
        '', '', '', ''  # Empty fields
    )
    return line


def write_new_ach_file(script_data: ScriptData) -> bool:
    """Write new ACH file with processed data"""
    try:
        newfile_data = rebuild_file(script_data)
        with open(script_data.apwx.args.OUTPUT_ACH_FILE_FULLPATH, 'w') as f:
            f.write('\n'.join(newfile_data) + '\n')
        return True
    except Exception as e:
        print(f"Error writing new ACH file: {e}")
        return False


def rebuild_batch(script_data: ScriptData, batch_data: List[str], control_nbr: int) -> List[str]:
    """Rebuild ACH batch with updated control numbers"""
    if not batch_data:
        return []

    batch_header = batch_data[0]
    # Extract service class code from position 2-4 of batch header
    service_class_code = batch_header[1:4]
    # Extract company ID from position 41-50 of batch header
    company_id = batch_header[40:49]
    # Extract ODFI from position 80-87 of batch header
    odfi = batch_header[79:87]

    entry_hash_total = 0
    debit_total = 0
    credit_total = 0
    total_record_cnt = 0

    # Format new batch number as 7-digit zero-filled
    new_number = f"{control_nbr:07d}"
    # Replace last 7 characters (position 88-94) with new batch number
    batch_header = batch_header[:-7] + new_number

    new_batch = [batch_header]

    for line in batch_data[1:]:
        if line.startswith('6'):  # Entry Detail Record
            rtnbr, acctnbr, ach_tc, amt, trace_nbr, sequence_nbr, has_addenda = parse_ach_detail_record(line)
            # Use first 8 digits of routing number for hash calculation
            rtnbr_no_checksum = rtnbr[:8]
            entry_hash_total += int(rtnbr_no_checksum)

            # Check transaction code to determine if credit or debit
            if ach_tc.endswith('2'):  # Credit transactions (22, 32, etc.)
                credit_total += amt
            elif ach_tc.endswith('7'):  # Debit transactions (27, 37, etc.)
                debit_total += amt

        total_record_cnt += 1
        new_batch.append(line)

    if not total_record_cnt:
        return []

    # Fixed-length fields for batch control record
    message_auth_cd = ' ' * 18  # Position 55-73: Message Authentication Code
    reserved = ' ' * 8  # Position 74-79: Reserved
    entry_hash = str(entry_hash_total)[-10:]  # Take rightmost 10 digits

    # Update script totals
    script_data.entry_hash_cnt += int(entry_hash)
    script_data.total_credit += credit_total
    script_data.total_debit += debit_total
    script_data.total_record_cnt += total_record_cnt

    # Build batch control record (Record Type Code 8)
    batch_control_footer = f"8{service_class_code}{total_record_cnt:06d}{int(entry_hash):010d}{debit_total:012d}{credit_total:012d}{company_id}{message_auth_cd}{reserved}{odfi}{new_number}"

    new_batch.append(batch_control_footer)
    return new_batch


def rebuild_file(script_data: ScriptData) -> List[str]:
    """Rebuild complete ACH file"""
    # Start with original file header (Record Type Code 1)
    file_header = script_data.file_header
    ach_data = script_data.newach_data

    batch_cnt = 0
    line_cnt = 1  # Start with 1 for file control record
    new_ach_file = [file_header]

    # Process each batch
    for batch_data in ach_data:
        batch_cnt += 1
        batch_file = rebuild_batch(script_data, batch_data, batch_cnt)

        if not batch_file:
            batch_cnt -= 1
            continue

        new_ach_file.extend(batch_file)

    # Calculate final totals for file control record
    entry_hash = str(script_data.entry_hash_cnt)[-10:]  # Rightmost 10 digits
    line_cnt += len(new_ach_file)

    # ACH files must be in blocks of 10 records
    mod = line_cnt % 10
    extra_lines = (10 - mod) if mod else 0
    new_count = line_cnt + extra_lines
    extra_row = '9' * 94  # Padding record
    block_cnt = new_count // 10
    reserved = ' ' * 39  # Position 56-94: Reserved

    # Build file control record (Record Type Code 9)
    file_control_footer = f"9{batch_cnt:06d}{block_cnt:06d}{script_data.total_record_cnt:08d}{int(entry_hash):010d}{script_data.total_debit:012d}{script_data.total_credit:012d}{reserved}"

    new_ach_file.append(file_control_footer)

    # Add padding records to make block count even
    for _ in range(extra_lines):
        new_ach_file.append(extra_row)

    return new_ach_file


def offset_gl(script_data: ScriptData) -> bool:
    """Create GL offset line for SWIM file"""
    cash_box_nbr = script_data.apwx.args.CASH_BOX_NUMBER
    gl_nbr = script_data.apwx.args.GL_ACCTNBR

    # Create GL credit line to offset the debits
    script_data.gl_cr_line = script_data.swim_file_info['swimFmt'].format(
        gl_nbr,  # GL account number
        'GLD',  # GL debit transaction code
        script_data.swim_file_info['glDrAmt'],  # Total debit amount
        '',  # Empty field
        script_data.swim_file_info['effDate'],  # Effective date
        'DNA ACH GL debit offset',  # Description
        '',  # Empty trace number
        cash_box_nbr,  # Cash box number
        '', '',  # Empty fields
        'EL', 'INTR', 'IMED',  # Fixed values
        '', '', '', ''  # Empty fields
    )
    return True


def prepare_statements(script_data: ScriptData) -> bool:
    """Prepare database statements"""
    try:
        # Statements are prepared as needed in the functions that use them
        script_data.sth = {}
        return True
    except Exception as e:
        print(f"Error preparing statements: {e}")
        return False


def get_swim_file_info(script_data: ScriptData) -> bool:
    """Initialize SWIM file information"""
    eff_date = script_data.apwx.args.EFFDATE

    script_data.swim_file_info = {
        'effDate': eff_date,
        'swimCt': 0,
        'extPmtCt': 0,
        'rptFmt': "%-25s%-30s%-15s%-15s%-25s%-100s",
        'swimFmt': '{:0>17s}{:<4s}{:>010d}{:>06s}{:>8s}{:<45s}{:>15s}{:>10s}{:>4s}{:>4s}{:<4s}{:<4s}{:<4s}{:>10s}{:<4s}{:<4s}{:1s}',
        'glDrAmt': 0,
        'glCrAmt': 0,
        'extPmtAmt': 0,
        'achToDnaTc': {
            '22': {'desc': 'OAO External Transfer', 'osiTC': 'DEPD'},  # Checking Credit
            '32': {'desc': 'OAO External Transfer', 'osiTC': 'DEPD'},  # Savings Credit
        }
    }
    return True


def validate_route_number(script_data: ScriptData, rtnbr: str) -> bool:
    """Validate routing number"""
    if not rtnbr.isdigit():
        print("Routing number must be all digits")
        return False

    if len(rtnbr) != 9:
        print("Routing number too short")
        return False

    if not is_valid_fed_symbol(rtnbr):
        print("Invalid fed district")
        return False

    if not validate_routing_number_checksum(rtnbr):
        print("Invalid checksum")
        return False

    return True


def validate_routing_number_checksum(rtnbr: str) -> bool:
    """Validate routing number checksum using standard algorithm"""
    digits = [int(d) for d in rtnbr]
    # Standard routing number checksum calculation
    checksum = (
                       digits[0] * 3 + digits[1] * 7 + digits[2] * 1 +
                       digits[3] * 3 + digits[4] * 7 + digits[5] * 1 +
                       digits[6] * 3 + digits[7] * 7 + digits[8] * 1
               ) % 10

    return checksum == 0


def is_valid_fed_symbol(rtnbr: str) -> bool:
    """Check if routing number has valid federal district (positions 3-4)"""
    fed_district = rtnbr[2:4]
    fed_districts = [
        '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12',
        '21', '22', '23', '24', '25', '26', '27', '28', '29', '20', '21', '22',
        '31', '32', '33', '34', '35', '36', '37', '38', '39', '30', '31', '32'
    ]

    return fed_district in fed_districts


def exit_early(script_data: ScriptData, sub: str, db_disconnect: bool = False) -> bool:
    """Handle early exit with cleanup"""
    print(f"There was an issue running {sub}")
    print("Cleaning up and exiting")

    if db_disconnect and script_data.dbh:
        script_data.dbh.rollback()
        disconnect_db(script_data)

    return False


def get_hold_biz_date(script_data: ScriptData) -> bool:
    """Get business date for hold using SQL from config"""
    try:
        # Get SQL from config file
        sql = script_data.config.get("get_hold_biz_date")

        cursor = script_data.dbh.cursor()
        cursor.execute(sql, {'days': script_data.apwx.args.HOLD_LENGTH})
        result = cursor.fetchone()
        script_data.holddate = result[0] + ' 16:00'  # Add time component
        return True
    except Exception as e:
        print(f"Error getting hold business date: {e}")
        return False


def disconnect_db(script_data: ScriptData) -> bool:
    """Disconnect from database"""
    try:
        for handle_name, handle in script_data.sth.items():
            if is_debug_mode(script_data.apwx):
                print(f"Disconnecting handle {handle_name}")
            if hasattr(handle, 'close'):
                handle.close()

        script_data.dbh.close()
        return True
    except Exception as e:
        print(f"Error disconnecting from database: {e}")
        return False


def is_report_only(apwx: Apwx) -> bool:
    """Check if running in report-only mode. No database updates will occur"""
    return apwx.args.RPTONLY_YN.upper() == "Y"


def is_debug_mode(apwx: Apwx) -> bool:
    """Check if debug mode is enabled. More output is written to the logs"""
    return apwx.args.DEBUG.upper() == "Y"


def get_config(apwx: Apwx) -> Any:
    """Load configuration from YAML file"""
    with open(apwx.args.CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f)
    return config


def get_apwx() -> Apwx:
    """Get AppWorx instance"""
    return Apwx(["OSIUPDATE", "OSIUPDATE_PW"])


def parse_args(apwx: Apwx) -> Apwx:
    """Parse command line arguments"""
    parser = apwx.parser

    # parser.add_arg(AppWorxEnum.HOST, type=str, required=True)
    # parser.add_arg(AppWorxEnum.SID, type=str, required=True)
    # parser.add_arg(AppWorxEnum.PORT, type=int, default=1521, required=False)
    parser.add_arg(AppWorxEnum.TNS_SERVICE_NAME, type=str, required=True)
    parser.add_arg(AppWorxEnum.INPUT_ACH_FILE_FULLPATH, type=str, required=True)
    parser.add_arg(AppWorxEnum.OUTPUT_ACH_FILE_FULLPATH, type=str, required=True)
    parser.add_arg(AppWorxEnum.OUTPUT_SWIM_FILE_FULLPATH, type=str, required=True)
    parser.add_arg(AppWorxEnum.GL_ACCTNBR, type=str, required=True)
    parser.add_arg(AppWorxEnum.CASH_BOX_NUMBER, type=str, required=True)
    parser.add_arg(AppWorxEnum.DEBUG, choices=["Y", "N"], default="N", required=False)
    parser.add_arg(AppWorxEnum.REASONTYPE, type=str, default="OAO new member ACH hold", required=False)
    parser.add_arg(AppWorxEnum.HOLD_CODE, type=str, default="AHLD", required=False)
    parser.add_arg(AppWorxEnum.HOLD_LENGTH, type=str, default="4", required=False)
    # parser.add_arg(AppWorxEnum.REPORT, type=str, required=False)
    # parser.add_arg(AppWorxEnum.REPORT_PATH, type=str, required=False)
    parser.add_arg(AppWorxEnum.EFFDATE, type=str, required=True)
    parser.add_arg(AppWorxEnum.FTFCU_ROUTENBR, type=str, default="321180379", required=False)
    parser.add_arg(AppWorxEnum.CONFIG_FILE_PATH, type=r"(.yml|.yaml)$", required=True)
    parser.add_arg(AppWorxEnum.RPTONLY_YN, choices=["Y", "N"], default="N", required=False)
    # parser.add_arg("ROW_LIMIT", type=int, default=0, required=False)


    apwx.parse_args()
    return apwx


if __name__ == "__main__":
    JobTime().print_start()
    run(parse_args(get_apwx()), time.time())
    JobTime().print_end()
