"""
Helper functions for writing Daily Add Tracker data to Google Sheets.
This module handles the DAILY sheets that contain landing page performance data from Atria.

Sheet structure:
- Each DAILY tab has a URL in cell A4 (e.g., "wesmyle.com/nl/pages/starter-kit-ebrush-sub")
- Data columns start from row with headers containing: Clicks, Add to cart, etc.
- Date column has dates like "30 November", "1 December", etc.
"""

import os
import json
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from config_store import get_setting

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


# =============================================================================
# COLUMN MAPPING CONFIGURATION
# =============================================================================
# This is where you configure the mapping between Atria fields and Google Sheet columns.
#
# Format:
#   ColumnMapping(atria_field, sheet_column)
#
# - atria_field: The exact column name from the Atria report (case-sensitive)
# - sheet_column: The column header name in Google Sheets (case-insensitive for matching)
#
# To add a new mapping, simply add a new ColumnMapping to the COLUMN_MAPPINGS list.
# To disable a mapping, comment it out or remove it from the list.
# =============================================================================

class ColumnMapping:
    """Represents a mapping from an Atria field to a Google Sheet column."""

    def __init__(self, atria_field: str, sheet_column: str):
        """
        Create a column mapping.

        Args:
            atria_field: The exact column name from Atria report (case-sensitive)
            sheet_column: The column header in Google Sheets (case-insensitive)
        """
        self.atria_field = atria_field
        self.sheet_column = sheet_column.lower()  # Store lowercase for matching

    def __repr__(self):
        return f"ColumnMapping('{self.atria_field}' -> '{self.sheet_column}')"


# =============================================================================
# CONFIGURE YOUR MAPPINGS HERE
# =============================================================================
# Each mapping defines: Atria Field Name -> Google Sheet Column Name
#
# Atria Report Fields (EXACT column headers from Atria screenshot):
#   - Landing page              (URL - used for matching, not written)
#   - Landing page views        (number of landing page views)
#   - Spend                     (e.g., €947.52)
#   - Link clicks               (number of link clicks)
#   - ATC                       (Add to Cart count)
#   - Checkouts Initiated       (checkout count)
#   - Purchases                 (purchase count)
#   - ROAS                      (return on ad spend, e.g., 3.01)
#   - AOV                       (average order value, e.g., €73.01)
#   - CPM                       (cost per mille, e.g., €10.17)
#   - CPC (link click)          (cost per click, e.g., €1.42)
#   - CTR (link click)          (click-through rate, e.g., 0.71%)
#   - Cost per landing          (cost per landing page view)
#
# Google Sheet Columns (from your DAILY sheets - case insensitive):
#   - Sessions, Clicks, Add to cart, Started checkout, Purchase
#   - ROAS, AOV, CPM, CPC, CTR, Spend, etc.
# =============================================================================

COLUMN_MAPPINGS: List[ColumnMapping] = [
    # Atria Field                    -> Google Sheet Column
    ColumnMapping("Landing page views",    "Sessions"),
    ColumnMapping("Spend",                 "Spend"),
    ColumnMapping("Link clicks",           "Clicks"),
    ColumnMapping("ATC",                   "Add to cart"),
    ColumnMapping("Checkouts Initiated",   "Started checkout"),
    ColumnMapping("Purchases",             "Purchase"),
    ColumnMapping("ROAS",                  "ROAS"),
    ColumnMapping("AOV",                   "AOV"),
    ColumnMapping("CPM",                   "CPM"),
    ColumnMapping("CPC (link click)",      "CPC"),
    ColumnMapping("CTR (link click)",      "CTR"),
    # ColumnMapping("Cost per landing",    "Cost per landing"),  # Uncomment if needed
]


def get_column_mappings() -> List[ColumnMapping]:
    """
    Get the list of configured column mappings.

    Returns:
        List of ColumnMapping objects
    """
    return COLUMN_MAPPINGS


def print_column_mappings():
    """Print all configured column mappings for debugging."""
    print("\n[MAPPING] Configured column mappings:")
    print("-" * 50)
    for mapping in COLUMN_MAPPINGS:
        print(f"  Atria: '{mapping.atria_field}' -> Sheet: '{mapping.sheet_column}'")
    print("-" * 50)


def get_daily_add_tracker_spreadsheet():
    """
    Get the Daily Add Tracker Google Sheets spreadsheet.

    Stored value required:
        DAILY_ADD_TRACKER_SHEET_URL - Full Google Sheets URL
            Example: https://docs.google.com/spreadsheets/d/1d8KEO_R3PUsEdnTvt1MmSrYd0MbHxPIQOt1hv0m0zVs/edit?gid=1449465974

    Returns:
        gspread.Spreadsheet: The spreadsheet object
    """
    # Get credentials
    env_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if env_json:
        info = json.loads(env_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("auth.json", scopes=SCOPES)

    # Authorize
    gc = gspread.authorize(creds)

    # Get full URL from local config
    sheet_url = get_setting("DAILY_ADD_TRACKER_SHEET_URL")

    if not sheet_url:
        raise ValueError("DAILY_ADD_TRACKER_SHEET_URL is not set. Please add it via the GUI settings or config.db.")

    # Parse spreadsheet ID from URL
    spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not spreadsheet_match:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {sheet_url}")

    spreadsheet_id = spreadsheet_match.group(1)

    # Open spreadsheet
    ss = gc.open_by_key(spreadsheet_id)

    return ss


def get_daily_worksheets(spreadsheet) -> List[gspread.Worksheet]:
    """
    Get all DAILY worksheets from the spreadsheet.
    Excludes any sheet with "NOT IN USE" in the title.

    Args:
        spreadsheet: gspread.Spreadsheet object

    Returns:
        List of gspread.Worksheet objects for DAILY sheets
    """
    daily_sheets = []

    for ws in spreadsheet.worksheets():
        title = ws.title.upper()
        # Include sheets with DAILY in name, exclude NOT IN USE
        if "DAILY" in title and "NOT IN USE" not in title:
            daily_sheets.append(ws)
            print(f"  Found DAILY sheet: {ws.title}")

    return daily_sheets


def find_url_in_sheet(worksheet) -> Optional[str]:
    """
    Find the landing page URL in a DAILY sheet.
    The URL is typically in the first few rows (A1-A10).

    Args:
        worksheet: gspread.Worksheet object

    Returns:
        URL string if found, None otherwise
    """
    try:
        # Get first 10 rows of column A
        first_rows = worksheet.col_values(1)[:10]

        for i, cell_value in enumerate(first_rows):
            cell_str = str(cell_value).strip()
            # Check if it looks like a URL (contains domain-like pattern)
            if cell_str and ('wesmyle' in cell_str.lower() or
                           '.com' in cell_str.lower() or
                           '/pages/' in cell_str.lower()):
                print(f"    URL found in row {i+1}: {cell_str}")
                return cell_str

        return None
    except Exception as e:
        print(f"    Error finding URL: {e}")
        return None


def find_header_row(worksheet) -> Tuple[Optional[int], Dict[str, int]]:
    """
    Find the header row and column mapping in a DAILY sheet.
    The header row contains column names like: Clicks, Add to cart, Purchase, etc.

    IMPORTANT: We look for rows that have MULTIPLE data headers together to avoid
    finding the wrong row. The data headers row should have columns like:
    Clicks, Add to cart, Started checkout, Purchase, ROAS, AOV, CPM, CPC, CTR, Spend

    Args:
        worksheet: gspread.Worksheet object

    Returns:
        Tuple of (header_row_number, column_mapping dict)
        column_mapping maps header names to column indices (1-based)
    """
    try:
        # Get more rows to search for headers (header row might be around row 25 or beyond)
        all_values = worksheet.get_all_values()[:100]
        print(f"    Searching for header row in first {len(all_values)} rows...")

        best_match_row = None
        best_match_count = 0
        best_column_mapping = {}

        # Required headers that indicate this is the correct data row
        # Must have several of these together to be the right row
        required_headers = ['clicks', 'add to cart', 'purchase', 'roas', 'aov', 'cpm', 'cpc', 'ctr', 'spend', 'started checkout', 'cr']

        for row_idx, row in enumerate(all_values, start=1):
            row_lower = [str(cell).strip().lower() for cell in row]
            row_text = ' '.join(row_lower)

            # Count how many required headers are in this row
            match_count = sum(1 for h in required_headers if h in row_text)

            # We need at least 5 matches to consider this the header row
            if match_count >= 5 and match_count > best_match_count:
                # Found a better header row candidate
                column_mapping = {}
                for col_idx, cell in enumerate(row, start=1):
                    cell_str = str(cell).strip()
                    if cell_str:
                        column_mapping[cell_str.lower()] = col_idx

                best_match_row = row_idx
                best_match_count = match_count
                best_column_mapping = column_mapping
                print(f"    Found header candidate at row {row_idx} with {match_count} matches: {list(column_mapping.keys())[:8]}...")

        if best_match_row:
            print(f"    Header row selected: row {best_match_row} (best match with {best_match_count} headers)")
            print(f"    Available columns: {list(best_column_mapping.keys())}")
            return best_match_row, best_column_mapping

        print(f"    No header row found with required headers")
        return None, {}
    except Exception as e:
        print(f"    Error finding header row: {e}")
        return None, {}


def find_date_row(worksheet, date_obj: datetime, header_row: int) -> Optional[int]:
    """
    Find the row for a specific date in a DAILY sheet.
    Dates are in format like "30 November", "1 December", "15 December".

    Args:
        worksheet: gspread.Worksheet object
        date_obj: datetime object for the target date
        header_row: Row number of headers (data starts after this)

    Returns:
        Row number if found, None otherwise
    """
    try:
        day = date_obj.day
        month_full = date_obj.strftime('%B')   # "December"
        month_short = date_obj.strftime('%b')  # "Dec"

        # Format date in multiple ways for matching
        date_formats = [
            f"{day} {month_full}",           # "15 December" or "5 December"
            f"{day:02d} {month_full}",       # "05 December" (with leading zero)
            f"{month_full} {day}",           # "December 15"
            f"{day} {month_short}",          # "15 Dec"
            f"{day:02d} {month_short}",      # "05 Dec"
        ]

        print(f"    Looking for date formats: {date_formats}")

        # Get ALL values from column A (dates can be far down like row 338+)
        date_column = worksheet.col_values(1)
        print(f"    Searching {len(date_column)} rows in column A")

        for row_idx, cell_value in enumerate(date_column, start=1):
            if row_idx <= header_row:
                continue  # Skip header and above

            cell_str = str(cell_value).strip()
            if not cell_str:
                continue

            cell_lower = cell_str.lower()

            for date_fmt in date_formats:
                if date_fmt.lower() == cell_lower or cell_lower.startswith(date_fmt.lower()):
                    print(f"    Date '{cell_str}' found at row {row_idx}")
                    return row_idx

        print(f"    Date not found in any row")
        return None
    except Exception as e:
        print(f"    Error finding date row: {e}")
        return None


def normalize_url(url: str) -> str:
    """
    Normalize a URL for comparison by removing protocol, www, trailing slashes.

    Args:
        url: URL string to normalize

    Returns:
        Normalized URL string
    """
    if not url:
        return ""

    url = str(url).lower().strip()
    # Remove protocol
    url = url.replace('https://', '').replace('http://', '')
    # Remove www.
    url = url.replace('www.', '')
    # Remove trailing slash
    url = url.rstrip('/')

    return url


def match_atria_data_to_url(atria_data: List[Dict], target_url: str) -> Optional[Dict]:
    """
    Find the Atria data row that matches the target URL.

    Args:
        atria_data: List of dictionaries from Atria extraction
        target_url: URL from the DAILY sheet (e.g., "wesmyle.com/nl/pages/starter-kit-ebrush-sub")

    Returns:
        Dictionary with matched data, None if not found
    """
    # target_normalized = normalize_url(target_url)
    target_normalized = target_url
    if not target_normalized:
        print(f"    [URL] Target URL is empty")
        return None

    print(f"    [URL] Looking for: {target_normalized}")

    for row in atria_data:
        landing_page = row.get('Landing page', '')
        # landing_normalized = normalize_url(landing_page)
        landing_normalized = landing_page

        if not landing_normalized:
            continue

        # Try exact match first
        if landing_normalized == target_normalized:
            print(f"    [URL] Exact match found: {landing_page}")
            return row

        # Try partial match (URL contains target or target contains URL)
        if target_normalized in landing_normalized or landing_normalized in target_normalized:
            print(f"    [URL] Partial match found: {landing_page}")
            return row

        # Try matching just the path part after /pages/ or /products/
        for path_marker in ['/pages/', '/products/']:
            if path_marker in target_normalized and path_marker in landing_normalized:
                target_path = target_normalized.split(path_marker)[-1]
                landing_path = landing_normalized.split(path_marker)[-1]
                if target_path == landing_path:
                    print(f"    [URL] Path match found: {landing_page}")
                    return row

    print(f"    [URL] No match found. Available Atria URLs:")
    for i, row in enumerate(atria_data[:5]):
        lp = row.get('Landing page', 'N/A')
        print(f"         {i+1}. {lp}")
    if len(atria_data) > 5:
        print(f"         ... and {len(atria_data) - 5} more")

    return None


def map_atria_to_sheet_columns(atria_row: Dict, sheet_column_mapping: Dict[str, int]) -> Dict[int, any]:
    """
    Map Atria data fields to the DAILY sheet columns using the configured COLUMN_MAPPINGS.

    This function uses the COLUMN_MAPPINGS list defined at the top of this file.
    Each ColumnMapping specifies which Atria field goes to which Sheet column.

    Args:
        atria_row: Dictionary with Atria data (keys are Atria field names)
        sheet_column_mapping: Dictionary mapping sheet column names (lowercase) to column indices

    Returns:
        Dictionary mapping column indices to values to write
    """
    result = {}

    # Use the configured mappings
    for mapping in COLUMN_MAPPINGS:
        atria_field = mapping.atria_field
        sheet_column = mapping.sheet_column  # Already lowercase

        # Check if Atria row has this field
        if atria_field in atria_row:
            value = atria_row[atria_field]

            # Check if sheet has this column
            if sheet_column in sheet_column_mapping:
                col_index = sheet_column_mapping[sheet_column]
                result[col_index] = parse_value(value)

    return result


def parse_value(value) -> float:
    """
    Parse a value string to a float.
    Handles formats like: "1,234", "€1,234.56", "1.5%", "1.2K", "2.5M"

    Args:
        value: Value to parse

    Returns:
        Parsed float value
    """
    if value is None or value == '' or value == '-':
        return 0.0

    # Convert to string
    value_str = str(value).strip()

    # Remove currency symbols and percentage
    value_str = value_str.replace('€', '').replace('%', '').replace('$', '').strip()

    # Handle K (thousands) and M (millions)
    multiplier = 1
    if value_str.endswith('K'):
        multiplier = 1000
        value_str = value_str[:-1]
    elif value_str.endswith('M'):
        multiplier = 1000000
        value_str = value_str[:-1]

    # Remove commas and parse
    try:
        return float(value_str.replace(',', '')) * multiplier
    except ValueError:
        return 0.0


def write_data_to_daily_sheet(worksheet, row_num: int, column_data: Dict[int, any]):
    """
    Write data to a specific row in a DAILY sheet.
    Only updates cells that have values in column_data, preserves formulas.

    Args:
        worksheet: gspread.Worksheet object
        row_num: Row number to update
        column_data: Dictionary mapping column indices (1-based) to values
    """
    try:
        updates = []
        for col_idx, value in column_data.items():
            cell_ref = gspread.utils.rowcol_to_a1(row_num, col_idx)
            updates.append({
                'range': cell_ref,
                'values': [[value]]
            })

        if updates:
            worksheet.batch_update(updates, value_input_option='USER_ENTERED')
            print(f"    Updated {len(updates)} cells in row {row_num}")
    except Exception as e:
        print(f"    Error writing data: {e}")
        raise


def write_atria_data_to_sheets(date_obj: datetime, atria_data_report1: List[Dict], atria_data_report2: List[Dict] = None):
    """
    Main function to write Atria data to all DAILY sheets.

    Args:
        date_obj: datetime object for the date being processed
        atria_data_report1: List of dictionaries from Atria Report 1
        atria_data_report2: List of dictionaries from Atria Report 2 (optional)
    """
    print("\n[DAILY ADD TRACKER] Writing data to Google Sheets...")
    print(f"  Target date: {date_obj.strftime('%d %B %Y')}")

    # Print configured mappings
    print_column_mappings()

    try:
        # Combine data from both reports if available
        all_atria_data = list(atria_data_report1) if atria_data_report1 else []
        if atria_data_report2:
            all_atria_data.extend(atria_data_report2)

        if not all_atria_data:
            print("  No Atria data to write")
            return False

        print(f"\n  Total Atria rows: {len(all_atria_data)}")

        # Debug: Show available landing pages from Atria with ALL their data
        print("\n" + "=" * 80)
        print("ATRIA DATA EXTRACTED (Landing Page -> Data)")
        print("=" * 80)
        for i, row in enumerate(all_atria_data):
            lp = row.get('Landing page', 'N/A')
            print(f"\n  [{i+1}] URL: {lp}")
            print(f"      Data fields:")
            for key, value in row.items():
                if key != 'Landing page':
                    print(f"        {key}: {value}")
        print("=" * 80)

        # Get spreadsheet
        spreadsheet = get_daily_add_tracker_spreadsheet()
        print(f"\n  Opened spreadsheet: {spreadsheet.title}")

        # Get all DAILY worksheets
        daily_sheets = get_daily_worksheets(spreadsheet)
        print(f"  Found {len(daily_sheets)} DAILY sheets")

        sheets_updated = 0
        sheets_skipped = 0

        for worksheet in daily_sheets:
            print("\n" + "-" * 80)
            print(f"PROCESSING SHEET: {worksheet.title}")
            print("-" * 80)

            # Find URL in the sheet
            sheet_url = find_url_in_sheet(worksheet)
            if not sheet_url:
                print(f"  [SKIP] No URL found in sheet")
                sheets_skipped += 1
                continue

            print(f"  Sheet URL: {sheet_url}")

            # Find matching Atria data
            matched_data = match_atria_data_to_url(all_atria_data, sheet_url)
            if not matched_data:
                print(f"  [SKIP] No matching Atria data found for this URL")
                print(f"         Looking for: {sheet_url}")
                sheets_skipped += 1
                continue

            atria_url = matched_data.get('Landing page', 'N/A')
            print(f"  Atria URL matched: {atria_url}")
            print(f"  [MATCH] Sheet URL <-> Atria URL matched successfully!")

            # Find header row and column mapping
            header_row, column_mapping = find_header_row(worksheet)
            if not header_row:
                print(f"  [SKIP] Could not find header row in sheet")
                sheets_skipped += 1
                continue

            print(f"  Header row: {header_row}")
            print(f"  Sheet columns available: {list(column_mapping.keys())}")

            # Find the row for the target date
            date_row = find_date_row(worksheet, date_obj, header_row)
            if not date_row:
                print(f"  [SKIP] Could not find row for date: {date_obj.strftime('%d %B')}")
                sheets_skipped += 1
                continue

            print(f"  Target row for date: {date_row}")

            # Map Atria data to sheet columns
            column_data = map_atria_to_sheet_columns(matched_data, column_mapping)
            if not column_data:
                print(f"  [SKIP] No mappable data found")
                sheets_skipped += 1
                continue

            # Log the write operation clearly
            print(f"\n  [WRITING DATA]")
            print(f"  Sheet: {worksheet.title}")
            print(f"  Sheet URL: {sheet_url}")
            print(f"  Atria URL: {atria_url}")
            print(f"  Target Row: {date_row}")
            print(f"  Date: {date_obj.strftime('%d %B %Y')}")
            print(f"\n  Data being written ({len(column_data)} values):")
            print(f"  " + "-" * 60)
            print(f"  {'Atria Field':<25} | {'Sheet Column':<20} | {'Value':<15}")
            print(f"  " + "-" * 60)

            for mapping in COLUMN_MAPPINGS:
                atria_field = mapping.atria_field
                sheet_col = mapping.sheet_column

                if sheet_col in column_mapping:
                    col_idx = column_mapping[sheet_col]
                    if col_idx in column_data:
                        value = column_data[col_idx]
                        print(f"  {atria_field:<25} | {sheet_col:<20} | {value:<15}")
                    else:
                        atria_value = matched_data.get(atria_field, "N/A")
                        print(f"  {atria_field:<25} | {sheet_col:<20} | (no atria value: {atria_value})")
                else:
                    print(f"  {atria_field:<25} | {sheet_col:<20} | (column not in sheet)")

            print(f"  " + "-" * 60)

            # Write data to sheet
            write_data_to_daily_sheet(worksheet, date_row, column_data)
            print(f"  [SUCCESS] Data written to {worksheet.title} row {date_row}")
            sheets_updated += 1

        print(f"\n[DAILY ADD TRACKER] Completed:")
        print(f"  Sheets updated: {sheets_updated}")
        print(f"  Sheets skipped: {sheets_skipped}")
        return sheets_updated > 0

    except Exception as e:
        print(f"\n[DAILY ADD TRACKER] Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def debug_sheet_structure(worksheet_name: str = None):
    """
    Debug function to inspect DAILY sheet structure.
    Prints URL, headers, and sample date rows for troubleshooting.

    Args:
        worksheet_name: Optional specific worksheet name to inspect.
                       If None, inspects all DAILY sheets.
    """
    print("\n[DEBUG] Inspecting sheet structure...")

    try:
        spreadsheet = get_daily_add_tracker_spreadsheet()
        print(f"Spreadsheet: {spreadsheet.title}")

        daily_sheets = get_daily_worksheets(spreadsheet)

        for ws in daily_sheets:
            if worksheet_name and worksheet_name.lower() not in ws.title.lower():
                continue

            print(f"\n{'='*60}")
            print(f"Sheet: {ws.title}")
            print('='*60)

            # Get first 30 rows
            all_values = ws.get_all_values()[:30]

            # Find URL
            url = find_url_in_sheet(ws)
            print(f"URL found: {url}")

            # Find header row
            header_row, col_map = find_header_row(ws)
            print(f"Header row: {header_row}")
            print(f"Columns: {col_map}")

            # Show first few rows after header
            if header_row and header_row < len(all_values):
                print(f"\nSample data rows:")
                for i in range(header_row, min(header_row + 5, len(all_values))):
                    row_data = all_values[i] if i < len(all_values) else []
                    print(f"  Row {i+1}: {row_data[:6]}...")  # First 6 columns

    except Exception as e:
        print(f"Debug error: {e}")
        import traceback
        traceback.print_exc()
