"""
Helper functions for writing DataAds data to Google Sheets.

Key differences from Atria (add_tracker_helpers.py):
- Uses DATADS_SHEET_URL setting (separate sheet)
- Auto-creates worksheet tabs if landing page URL not found
- Auto-adds date rows if not present
- Updates existing date rows on rerun
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

class ColumnMapping:
    """Represents a mapping from a DataAds field to a Google Sheet column."""

    def __init__(self, datads_field: str, sheet_column: str):
        self.datads_field = datads_field
        self.sheet_column = sheet_column
        self.sheet_column_lower = sheet_column.lower()

    def __repr__(self):
        return f"ColumnMapping('{self.datads_field}' -> '{self.sheet_column}')"


# DataAds Field Name -> Google Sheet Column Name
COLUMN_MAPPINGS: List[ColumnMapping] = [
    # Common metrics (same sheet column names as Atria)
    ColumnMapping("Landing Page Views",         "Sessions"),
    ColumnMapping("Spend",                      "Spend"),
    ColumnMapping("Add to Cart",                "Add to cart"),
    ColumnMapping("Initiate Checkout",          "Started checkout"),
    ColumnMapping("Purchases",                  "Purchase"),
    ColumnMapping("Purchase ROAS",              "ROAS"),
    ColumnMapping("Average Order Value (AOV)",  "AOV"),
    ColumnMapping("CPM",                        "CPM"),
    ColumnMapping("CPC",                        "CPC"),
    ColumnMapping("CTR (Link Click Rate)",      "CTR"),
    # Extra DataAds metrics
    ColumnMapping("Conversion Rate",            "CR"),
    ColumnMapping("Cost per Landing Page Views", "Cost per landing"),
    ColumnMapping("Add to Cart / Clicks",       "ATC / Clicks"),
    ColumnMapping("Purchase / Add to Cart",     "Purchase / ATC"),
    ColumnMapping("Purchase / Clicks",          "Purchase / Clicks"),
    ColumnMapping("Cost per Purchase",          "Cost per purchase"),
]

# Ordered list of sheet column headers for new worksheets
SHEET_HEADERS = ["Date"] + [m.sheet_column for m in COLUMN_MAPPINGS]

# Row where URL goes, and row where headers go in new sheets
URL_ROW = 1
HEADER_ROW_NUM = 3
DATA_START_ROW = 4


def print_column_mappings():
    """Print all configured column mappings for debugging."""
    print("\n[MAPPING] Configured column mappings:")
    print("-" * 50)
    for mapping in COLUMN_MAPPINGS:
        print(f"  DataAds: '{mapping.datads_field}' -> Sheet: '{mapping.sheet_column}'")
    print("-" * 50)


# =============================================================================
# SPREADSHEET ACCESS
# =============================================================================

def _get_gspread_client():
    """Get authenticated gspread client."""
    env_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if env_json:
        info = json.loads(env_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("auth.json", scopes=SCOPES)
    return gspread.authorize(creds)


def get_datads_spreadsheet():
    """
    Get the DataAds Google Sheets spreadsheet.

    Setting key: DATADS_SHEET_URL
    """
    gc = _get_gspread_client()

    sheet_url = get_setting("DATADS_SHEET_URL")
    if not sheet_url:
        raise ValueError("DATADS_SHEET_URL is not set. Please add it via the GUI settings or config.db.")

    spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not spreadsheet_match:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {sheet_url}")

    spreadsheet_id = spreadsheet_match.group(1)
    return gc.open_by_key(spreadsheet_id)


def get_daily_worksheets(spreadsheet) -> List[gspread.Worksheet]:
    """Get all daily worksheets (tabs starting with 'daily_')."""
    daily_sheets = []
    for ws in spreadsheet.worksheets():
        title = ws.title.lower()
        if title.startswith("daily_") and "not in use" not in title:
            daily_sheets.append(ws)
            print(f"  Found sheet: {ws.title}")
    return daily_sheets


# =============================================================================
# URL MATCHING & EXTRACTION
# =============================================================================

def normalize_url(url: str) -> str:
    """Normalize a URL: remove protocol, www, trailing slash, lowercase."""
    if not url:
        return ""
    url = str(url).lower().strip()
    url = url.replace('https://', '').replace('http://', '').replace('www.', '')
    return url.rstrip('/')


def strip_query_params(url: str) -> str:
    """Remove query parameters from a URL."""
    return url.split('?')[0] if url else ""


def extract_country_from_url(url: str) -> str:
    """Extract country code from wesmyle URL (TLD or path)."""
    if not url:
        return ""
    url = normalize_url(url)

    if url.startswith('wesmyle.'):
        tld = url.split('/')[0].split('.')[-1]
        if len(tld) == 2 and tld != 'co':
            return tld

    if 'wesmyle.com/' in url:
        after_domain = url.split('wesmyle.com/')[-1]
        path_parts = after_domain.split('/')
        if path_parts and len(path_parts[0]) == 2:
            return path_parts[0]

    return ""


def find_url_in_sheet(worksheet) -> Optional[str]:
    """Find the landing page URL in the first 10 rows of column A."""
    try:
        first_rows = worksheet.col_values(1)[:10]
        for i, cell_value in enumerate(first_rows):
            cell_str = str(cell_value).strip()
            if cell_str and ('wesmyle' in cell_str.lower() or
                             '.com' in cell_str.lower() or
                             '/pages/' in cell_str.lower() or
                             '/products/' in cell_str.lower() or
                             'http' in cell_str.lower()):
                return cell_str
        return None
    except Exception as e:
        print(f"    Error finding URL: {e}")
        return None


def match_datads_row_to_url(datads_data: List[Dict], target_url: str) -> Optional[Dict]:
    """
    Find the DataAds data row matching the target URL.
    Uses priority matching: exact > base URL > path+country.
    """
    if not target_url:
        return None

    target_norm = normalize_url(target_url)
    target_base = strip_query_params(target_norm)
    target_country = extract_country_from_url(target_url)

    for row in datads_data:
        landing_page = row.get('Landing page', '')
        if not landing_page or landing_page == 'Unknown':
            continue

        landing_norm = normalize_url(landing_page)
        landing_base = strip_query_params(landing_norm)
        landing_country = extract_country_from_url(landing_page)

        # Exact match
        if landing_norm == target_norm:
            return row

        # Base URL match (without query params), same country
        if (target_base in landing_base or landing_base in target_base) and target_country == landing_country:
            return row

        # Path-only match with country check
        for marker in ['/pages/', '/products/']:
            if marker in target_base and marker in landing_base:
                target_path = target_base.split(marker)[-1]
                landing_path = landing_base.split(marker)[-1]
                if target_path == landing_path and target_country == landing_country:
                    return row

    return None


# =============================================================================
# SHEET NAME GENERATION
# =============================================================================

def generate_sheet_name(landing_page_url: str, existing_names: List[str]) -> str:
    """
    Generate a worksheet tab name from a landing page URL.

    Examples:
        https://wesmyle.com/nl/pages/back-to-routine -> daily_back-to-routine-nl
        https://wesmyle.de/pages/starter-kit-ebrush-single-de -> daily_starter-kit-ebrush-single-de
        Unknown -> daily_unknown
    """
    if not landing_page_url or landing_page_url == 'Unknown':
        base_name = "daily_unknown"
    else:
        url = normalize_url(landing_page_url)
        url = strip_query_params(url)

        # Extract the meaningful path segment
        path_part = ""
        for marker in ['/pages/', '/products/']:
            if marker in url:
                path_part = url.split(marker)[-1]
                break

        if not path_part:
            # Use last path segment
            parts = url.rstrip('/').split('/')
            path_part = parts[-1] if parts else "unknown"

        # Add country suffix if from .com with country path (not already in path)
        country = extract_country_from_url(landing_page_url)
        if country and not path_part.endswith(f"-{country}") and 'wesmyle.com' in normalize_url(landing_page_url):
            path_part = f"{path_part}-{country}"

        # Clean up the name
        # Remove special chars except hyphens and underscores
        path_part = re.sub(r'[^a-zA-Z0-9_\-]', '-', path_part)
        # Collapse multiple hyphens
        path_part = re.sub(r'-+', '-', path_part).strip('-')

        base_name = f"daily_{path_part}"

    # Google Sheets tab name limit is 100 chars
    if len(base_name) > 95:
        base_name = base_name[:95]

    # Handle duplicates by appending a number
    final_name = base_name
    existing_lower = [n.lower() for n in existing_names]
    counter = 2
    while final_name.lower() in existing_lower:
        suffix = f"-{counter}"
        final_name = base_name[:95 - len(suffix)] + suffix
        counter += 1

    return final_name


# =============================================================================
# HEADER & DATE ROW MANAGEMENT
# =============================================================================

def find_header_row(worksheet) -> Tuple[Optional[int], Dict[str, int]]:
    """
    Find the header row and build column mapping.
    Looks for rows with multiple known metric column names.
    """
    try:
        all_values = worksheet.get_all_values()[:100]
        required_headers = [h.lower() for h in SHEET_HEADERS[1:]]  # Skip "Date"

        best_row = None
        best_count = 0
        best_mapping = {}

        for row_idx, row in enumerate(all_values, start=1):
            row_lower = [str(cell).strip().lower() for cell in row]
            match_count = sum(1 for h in required_headers if h in row_lower)

            if match_count >= 5 and match_count > best_count:
                column_mapping = {}
                for col_idx, cell in enumerate(row, start=1):
                    cell_str = str(cell).strip()
                    if cell_str:
                        column_mapping[cell_str.lower()] = col_idx
                best_row = row_idx
                best_count = match_count
                best_mapping = column_mapping

        if best_row:
            print(f"    Header row: {best_row} ({best_count} columns matched)")
            return best_row, best_mapping

        return None, {}
    except Exception as e:
        print(f"    Error finding header row: {e}")
        return None, {}


def find_date_row(worksheet, date_obj: datetime, header_row: int) -> Optional[int]:
    """
    Find the row for a specific date.
    Checks multiple date formats.
    """
    try:
        day = date_obj.day
        year = date_obj.year
        month_full = date_obj.strftime('%B')
        month_short = date_obj.strftime('%b')

        date_formats = [
            # With year (exact)
            f"{day:02d}/{date_obj.month:02d}/{year}",
            f"{date_obj.month:02d}/{day:02d}/{year}",
            f"{year}-{date_obj.month:02d}-{day:02d}",
            # Without year
            f"{day} {month_full}",
            f"{day:02d} {month_full}",
            f"{month_full} {day}",
            f"{day} {month_short}",
            f"{day:02d} {month_short}",
            # Our own format
            f"{day:02d}/{month_full}/{year}",
        ]

        date_column = worksheet.col_values(1)

        for row_idx, cell_value in enumerate(date_column, start=1):
            if row_idx <= header_row:
                continue
            cell_str = str(cell_value).strip()
            if not cell_str:
                continue

            for fmt in date_formats:
                if cell_str.lower() == fmt.lower() or cell_str.lower().startswith(fmt.lower()):
                    print(f"    Date '{cell_str}' found at row {row_idx}")
                    return row_idx

        return None
    except Exception as e:
        print(f"    Error finding date row: {e}")
        return None


def _parse_date_from_cell(cell_str: str) -> Optional[datetime]:
    """
    Try to parse a date from a cell value string.
    Handles formats like: "5 April", "05 April", "April 5",
    "05/04/2026", "04/05/2026", "2026-04-05", "05/April/2026"
    """
    cell_str = cell_str.strip()
    if not cell_str:
        return None

    from calendar import month_name, month_abbr
    month_names = {m.lower(): i for i, m in enumerate(month_name) if m}
    month_abbrs = {m.lower(): i for i, m in enumerate(month_abbr) if m}

    # Try "5 April" or "05 April"
    m = re.match(r'^(\d{1,2})\s+([A-Za-z]+)$', cell_str)
    if m:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        month_num = month_names.get(month_str) or month_abbrs.get(month_str)
        if month_num and 1 <= day <= 31:
            # Assume current year
            return datetime(datetime.now().year, month_num, day)

    # Try "April 5"
    m = re.match(r'^([A-Za-z]+)\s+(\d{1,2})$', cell_str)
    if m:
        month_str = m.group(1).lower()
        day = int(m.group(2))
        month_num = month_names.get(month_str) or month_abbrs.get(month_str)
        if month_num and 1 <= day <= 31:
            return datetime(datetime.now().year, month_num, day)

    # Try dd/mm/yyyy
    for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d/%B/%Y']:
        try:
            return datetime.strptime(cell_str, fmt)
        except ValueError:
            continue

    return None


def add_date_row(worksheet, date_obj: datetime, header_row: int) -> int:
    """
    Add a new date row in correct chronological order.
    Older dates stay above, newer dates go below.

    Returns:
        Row number where the date was added.
    """
    date_label = f"{date_obj.day} {date_obj.strftime('%B')} {date_obj.year}"
    # Normalize target date to midnight for comparison
    target_date = datetime(date_obj.year, date_obj.month, date_obj.day)

    try:
        date_column = worksheet.col_values(1)

        # Collect all existing date rows with their parsed dates
        date_rows = []  # list of (row_number, parsed_datetime)
        last_data_row = header_row

        for row_idx in range(header_row + 1, len(date_column) + 1):
            cell_str = str(date_column[row_idx - 1]).strip() if row_idx - 1 < len(date_column) else ""
            if cell_str:
                last_data_row = row_idx
                parsed = _parse_date_from_cell(cell_str)
                if parsed:
                    date_rows.append((row_idx, parsed))

        if not date_rows:
            # No existing dates — just add after header
            new_row = header_row + 1
            worksheet.update_cell(new_row, 1, date_label)
            print(f"    Added date '{date_label}' at row {new_row} (first entry)")
            return new_row

        # Find where to insert: before the first date that is AFTER target_date
        insert_before_row = None
        for row_num, existing_date in date_rows:
            if existing_date > target_date:
                insert_before_row = row_num
                break

        if insert_before_row:
            # Insert a new row at this position (pushes existing rows down)
            worksheet.insert_row([date_label], insert_before_row)
            print(f"    Inserted date '{date_label}' at row {insert_before_row} (chronological order)")
            return insert_before_row
        else:
            # Target date is after all existing dates — append at end
            new_row = last_data_row + 1
            worksheet.update_cell(new_row, 1, date_label)
            print(f"    Added date '{date_label}' at row {new_row} (newest date)")
            return new_row

    except Exception as e:
        print(f"    Error adding date row: {e}")
        raise


# =============================================================================
# WORKSHEET CREATION
# =============================================================================

def create_daily_worksheet(spreadsheet, landing_page_url: str, existing_names: List[str]) -> gspread.Worksheet:
    """
    Create a new DAILY worksheet tab for a landing page.

    Structure:
        Row 1: Landing page URL
        Row 2: (empty)
        Row 3: Headers (Date, Sessions, Spend, ...)
        Row 4+: Data rows
    """
    sheet_name = generate_sheet_name(landing_page_url, existing_names)
    print(f"    Creating new worksheet: '{sheet_name}'")

    # Create worksheet with enough rows and columns
    worksheet = spreadsheet.add_worksheet(
        title=sheet_name,
        rows=400,
        cols=len(SHEET_HEADERS) + 2
    )

    # Row 1: Landing page URL
    worksheet.update_cell(URL_ROW, 1, landing_page_url)

    # Row 3: Headers
    header_cells = []
    for col_idx, header in enumerate(SHEET_HEADERS, start=1):
        header_cells.append({
            'range': gspread.utils.rowcol_to_a1(HEADER_ROW_NUM, col_idx),
            'values': [[header]]
        })
    worksheet.batch_update(header_cells, value_input_option='USER_ENTERED')

    print(f"    Created worksheet '{sheet_name}' with URL and headers")
    return worksheet


# =============================================================================
# VALUE PARSING
# =============================================================================

def parse_value(value) -> float:
    """
    Parse a value string to a float.
    Handles: "1,234", "€1,234.56", "1.5%", "1.2K", "2.5M"
    """
    if value is None or value == '' or value == '-':
        return 0.0

    value_str = str(value).strip()
    value_str = value_str.replace('€', '').replace('%', '').replace('$', '').strip()

    multiplier = 1
    if value_str.endswith('K'):
        multiplier = 1000
        value_str = value_str[:-1]
    elif value_str.endswith('M'):
        multiplier = 1000000
        value_str = value_str[:-1]

    try:
        return float(value_str.replace(',', '')) * multiplier
    except ValueError:
        return 0.0


def map_datads_to_sheet_columns(datads_row: Dict, sheet_column_mapping: Dict[str, int]) -> Dict[int, float]:
    """Map DataAds data fields to sheet columns using COLUMN_MAPPINGS."""
    result = {}
    for mapping in COLUMN_MAPPINGS:
        if mapping.datads_field in datads_row:
            value = datads_row[mapping.datads_field]
            if mapping.sheet_column_lower in sheet_column_mapping:
                col_index = sheet_column_mapping[mapping.sheet_column_lower]
                result[col_index] = parse_value(value)
    return result


# =============================================================================
# DATA WRITING
# =============================================================================

def write_data_to_sheet(worksheet, row_num: int, column_data: Dict[int, float]):
    """Write data to a specific row, only updating cells with values."""
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


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def write_datads_data_to_sheets(date_obj: datetime, datads_data: List[Dict]):
    """
    Main function to write DataAds data to Google Sheets.

    For each extracted landing page:
    1. Find matching worksheet tab (by URL)
    2. If no match -> create a new tab with URL + headers
    3. Find header row -> column mapping
    4. Find date row -> if missing, add it
    5. Write metric values

    Args:
        date_obj: datetime object for the date being processed
        datads_data: List of dictionaries from DataAds extraction
    """
    print("\n[DATADS SHEETS] Writing data to Google Sheets...")
    print(f"  Target date: {date_obj.strftime('%d %B %Y')}")

    print_column_mappings()

    if not datads_data:
        print("  No DataAds data to write")
        return False

    print(f"\n  Total DataAds rows: {len(datads_data)}")

    # Show extracted data summary
    print("\n" + "=" * 80)
    print("DATADS DATA SUMMARY")
    print("=" * 80)
    for i, row in enumerate(datads_data):
        lp = row.get('Landing page', 'N/A')
        purchases = row.get('Purchases', '?')
        spend = row.get('Spend', '?')
        roas = row.get('Purchase ROAS', '?')
        print(f"  [{i+1}] {lp} | Purchases: {purchases} | Spend: {spend} | ROAS: {roas}")
    print("=" * 80)

    try:
        spreadsheet = get_datads_spreadsheet()
        print(f"\n  Opened spreadsheet: {spreadsheet.title}")

        # Get existing daily worksheets
        daily_sheets = get_daily_worksheets(spreadsheet)
        print(f"  Found {len(daily_sheets)} existing daily sheets")

        # Build lookup: URL -> worksheet
        url_to_sheet = {}
        for ws in daily_sheets:
            sheet_url = find_url_in_sheet(ws)
            if sheet_url:
                url_to_sheet[ws.title] = (ws, sheet_url)

        existing_names = [ws.title for ws in spreadsheet.worksheets()]

        sheets_updated = 0
        sheets_created = 0
        sheets_skipped = 0

        for datads_row in datads_data:
            landing_page = datads_row.get('Landing page', '')
            if not landing_page:
                print(f"\n  [SKIP] Row with no landing page URL")
                sheets_skipped += 1
                continue

            print(f"\n" + "-" * 80)
            print(f"  PROCESSING: {landing_page}")
            print("-" * 80)

            # Try to find matching worksheet
            matched_ws = None
            for ws_title, (ws, sheet_url) in url_to_sheet.items():
                if match_datads_row_to_url([datads_row], sheet_url):
                    matched_ws = ws
                    print(f"  [MATCH] Found existing sheet: '{ws_title}'")
                    break

            # If no match, create new worksheet
            if not matched_ws:
                if landing_page == 'Unknown':
                    print(f"  [SKIP] Skipping 'Unknown' landing page - not creating sheet")
                    sheets_skipped += 1
                    continue

                print(f"  [NEW] No matching sheet found - creating new one...")
                matched_ws = create_daily_worksheet(spreadsheet, landing_page, existing_names)
                existing_names.append(matched_ws.title)
                # Add to our lookup so future runs find it
                url_to_sheet[matched_ws.title] = (matched_ws, landing_page)
                sheets_created += 1

            # Find header row
            header_row, column_mapping = find_header_row(matched_ws)
            if not header_row:
                print(f"  [SKIP] No header row found in '{matched_ws.title}'")
                sheets_skipped += 1
                continue

            # Find or create date row
            date_row = find_date_row(matched_ws, date_obj, header_row)
            if date_row:
                print(f"  Date row exists at row {date_row} - will update")
            else:
                print(f"  Date not found - adding new date row...")
                date_row = add_date_row(matched_ws, date_obj, header_row)

            # Map data to columns
            column_data = map_datads_to_sheet_columns(datads_row, column_mapping)
            if not column_data:
                print(f"  [SKIP] No mappable data found")
                sheets_skipped += 1
                continue

            # Log what we're writing
            print(f"\n  [WRITING] Sheet: {matched_ws.title} | Row: {date_row} | Values: {len(column_data)}")
            print(f"  " + "-" * 60)
            print(f"  {'DataAds Field':<30} | {'Sheet Column':<20} | {'Value':<15}")
            print(f"  " + "-" * 60)
            for mapping in COLUMN_MAPPINGS:
                if mapping.datads_field in datads_row and mapping.sheet_column_lower in column_mapping:
                    col_idx = column_mapping[mapping.sheet_column_lower]
                    if col_idx in column_data:
                        print(f"  {mapping.datads_field:<30} | {mapping.sheet_column:<20} | {column_data[col_idx]}")
            print(f"  " + "-" * 60)

            # Write data
            write_data_to_sheet(matched_ws, date_row, column_data)
            print(f"  [SUCCESS] Data written to '{matched_ws.title}' row {date_row}")
            sheets_updated += 1

        print(f"\n[DATADS SHEETS] Completed:")
        print(f"  Sheets updated: {sheets_updated}")
        print(f"  Sheets created: {sheets_created}")
        print(f"  Sheets skipped: {sheets_skipped}")
        return sheets_updated > 0 or sheets_created > 0

    except Exception as e:
        print(f"\n[DATADS SHEETS] Error: {e}")
        import traceback
        traceback.print_exc()
        return False
