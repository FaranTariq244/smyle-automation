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


# Default DataAds Field Name -> Google Sheet Column Name
DEFAULT_COLUMN_MAPPINGS: List[ColumnMapping] = [
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
    ColumnMapping("Conversion Rate",            "CR"),
    ColumnMapping("Cost per Landing Page Views", "Cost per landing"),
    ColumnMapping("Add to Cart / Clicks",       "ATC / Clicks"),
    ColumnMapping("Purchase / Add to Cart",     "Purchase / ATC"),
    ColumnMapping("Purchase / Clicks",          "Purchase / Clicks"),
    ColumnMapping("Cost per Purchase",          "Cost per purchase"),
]

# Backwards compat alias
COLUMN_MAPPINGS = DEFAULT_COLUMN_MAPPINGS


def get_column_mappings(mode: str = 'daily') -> List[dict]:
    """
    Get column mappings for the given mode from config store.
    Falls back to DEFAULT_COLUMN_MAPPINGS if not configured.

    Returns list of dicts: [{"datads_field": ..., "sheet_column": ...}, ...]
    """
    key = f"DATADS_{mode.upper()}_MAPPINGS"
    raw = get_setting(key)
    if raw:
        try:
            mappings = json.loads(raw)
            if isinstance(mappings, list) and len(mappings) > 0:
                return mappings
        except (json.JSONDecodeError, TypeError):
            pass
    # Return defaults
    return [{"datads_field": m.datads_field, "sheet_column": m.sheet_column}
            for m in DEFAULT_COLUMN_MAPPINGS]


def get_column_mapping_objects(mode: str = 'daily') -> List[ColumnMapping]:
    """Get column mappings as ColumnMapping objects for the given mode."""
    raw_mappings = get_column_mappings(mode)
    return [ColumnMapping(m["datads_field"], m["sheet_column"]) for m in raw_mappings]


def get_sheet_headers(mode: str = 'daily') -> List[str]:
    """Get ordered sheet headers for the given mode."""
    mappings = get_column_mapping_objects(mode)
    return ["Date"] + [m.sheet_column for m in mappings]


# Default headers (for backward compat)
SHEET_HEADERS = ["Date"] + [m.sheet_column for m in DEFAULT_COLUMN_MAPPINGS]

# Row where URL goes, and row where headers go in new sheets
URL_ROW = 1
HEADER_ROW_NUM = 3
DATA_START_ROW = 4


def print_column_mappings(mode: str = 'daily'):
    """Print all configured column mappings for debugging."""
    mappings = get_column_mapping_objects(mode)
    print(f"\n[MAPPING] Configured column mappings ({mode}):")
    print("-" * 50)
    for mapping in mappings:
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


def get_worksheets_by_prefix(spreadsheet, prefix: str = "daily") -> List[gspread.Worksheet]:
    """Get all worksheets with a given prefix (e.g. 'daily_' or 'weekly_')."""
    sheets = []
    prefix_lower = f"{prefix}_"
    for ws in spreadsheet.worksheets():
        title = ws.title.lower()
        if title.startswith(prefix_lower) and "not in use" not in title:
            sheets.append(ws)
            print(f"  Found sheet: {ws.title}")
    return sheets


def get_daily_worksheets(spreadsheet) -> List[gspread.Worksheet]:
    """Get all daily worksheets (tabs starting with 'daily_')."""
    return get_worksheets_by_prefix(spreadsheet, "daily")


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

def generate_sheet_name(landing_page_url: str, existing_names: List[str],
                        prefix: str = "daily") -> str:
    """
    Generate a worksheet tab name from a landing page URL.

    Args:
        prefix: 'daily' or 'weekly'

    Examples:
        https://wesmyle.com/nl/pages/back-to-routine -> daily_back-to-routine-nl
        https://wesmyle.de/pages/starter-kit-ebrush-single-de -> daily_starter-kit-ebrush-single-de
        Unknown -> daily_unknown
    """
    if not landing_page_url or landing_page_url == 'Unknown':
        base_name = f"{prefix}_unknown"
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

        base_name = f"{prefix}_{path_part}"

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

def find_header_row(worksheet, mode: str = 'daily') -> Tuple[Optional[int], Dict[str, int]]:
    """
    Find the header row and build column mapping.
    Looks for rows with multiple known metric column names.
    Also extends the header row with any new columns from current mappings.
    """
    try:
        all_values = worksheet.get_all_values()[:100]

        # Use both default and configured headers for flexible detection
        configured_headers = get_sheet_headers(mode)
        all_known = set(h.lower() for h in configured_headers[1:])
        all_known.update(h.lower() for h in SHEET_HEADERS[1:])

        best_row = None
        best_count = 0
        best_mapping = {}

        for row_idx, row in enumerate(all_values, start=1):
            row_lower = [str(cell).strip().lower() for cell in row]
            match_count = sum(1 for h in all_known if h in row_lower)

            if match_count >= 3 and match_count > best_count:
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

            # Extend header row with new columns from current mappings
            current_mappings = get_column_mapping_objects(mode)
            next_col = max(best_mapping.values()) + 1 if best_mapping else 2
            new_headers = []

            for m in current_mappings:
                if m.sheet_column_lower not in best_mapping:
                    best_mapping[m.sheet_column_lower] = next_col
                    new_headers.append((next_col, m.sheet_column))
                    next_col += 1

            if new_headers:
                updates = []
                for col_idx, header_name in new_headers:
                    cell_ref = gspread.utils.rowcol_to_a1(best_row, col_idx)
                    updates.append({'range': cell_ref, 'values': [[header_name]]})
                worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                print(f"    Added {len(new_headers)} new columns: {[h[1] for h in new_headers]}")

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
            # With year (our format: "5 April 2026")
            f"{day} {month_full} {year}",
            f"{day:02d} {month_full} {year}",
            # Other with year
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

    # Try "5 April 2026" or "05 April 2026" (with year)
    m = re.match(r'^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$', cell_str)
    if m:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        year = int(m.group(3))
        month_num = month_names.get(month_str) or month_abbrs.get(month_str)
        if month_num and 1 <= day <= 31:
            return datetime(year, month_num, day)

    # Try "5 April" or "05 April" (without year)
    m = re.match(r'^(\d{1,2})\s+([A-Za-z]+)$', cell_str)
    if m:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        month_num = month_names.get(month_str) or month_abbrs.get(month_str)
        if month_num and 1 <= day <= 31:
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

def create_daily_worksheet(spreadsheet, landing_page_url: str, existing_names: List[str],
                           mode: str = 'daily') -> gspread.Worksheet:
    """
    Create a new worksheet tab for a landing page.

    Structure:
        Row 1: Landing page URL
        Row 2: (empty)
        Row 3: Headers (Date, Sessions, Spend, ...)
        Row 4+: Data rows

    Args:
        mode: 'daily' or 'weekly' - affects tab name prefix and header columns
    """
    prefix = "weekly" if mode == "weekly" else "daily"
    sheet_name = generate_sheet_name(landing_page_url, existing_names, prefix=prefix)
    print(f"    Creating new worksheet: '{sheet_name}'")

    headers = get_sheet_headers(mode)

    worksheet = spreadsheet.add_worksheet(
        title=sheet_name,
        rows=400,
        cols=len(headers) + 2
    )

    # Row 1: Landing page URL
    worksheet.update_cell(URL_ROW, 1, landing_page_url)

    # Row 3: Headers
    header_cells = []
    for col_idx, header in enumerate(headers, start=1):
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


def map_datads_to_sheet_columns(datads_row: Dict, sheet_column_mapping: Dict[str, int],
                                mode: str = 'daily') -> Dict[int, float]:
    """Map DataAds data fields to sheet columns using configured mappings."""
    result = {}
    mappings = get_column_mapping_objects(mode)
    for mapping in mappings:
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
        _mode = 'daily'
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

        import time as _time

        sheets_updated = 0
        sheets_created = 0
        sheets_skipped = 0

        for idx, datads_row in enumerate(datads_data):
            # Pace API calls to avoid rate limits
            if idx > 0 and idx % 3 == 0:
                _time.sleep(1)

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
            header_row, column_mapping = find_header_row(matched_ws, mode=_mode)
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
            for mapping in get_column_mapping_objects(_mode):
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


# =============================================================================
# WEEKLY REPORT - DATE RANGE HELPERS
# =============================================================================

def format_date_range_label(start_date: datetime, end_date: datetime) -> str:
    """Format a date range label like '31 Mar - 6 Apr 2026'."""
    if start_date.month == end_date.month and start_date.year == end_date.year:
        return f"{start_date.day} - {end_date.day} {end_date.strftime('%b')} {end_date.year}"
    elif start_date.year == end_date.year:
        return f"{start_date.day} {start_date.strftime('%b')} - {end_date.day} {end_date.strftime('%b')} {end_date.year}"
    else:
        return f"{start_date.day} {start_date.strftime('%b')} {start_date.year} - {end_date.day} {end_date.strftime('%b')} {end_date.year}"


def find_date_range_row(worksheet, start_date: datetime, end_date: datetime,
                        header_row: int) -> Optional[int]:
    """
    Find an existing row that matches this date range in a weekly tab.
    Searches for the date range label string.
    """
    label = format_date_range_label(start_date, end_date)
    try:
        date_column = worksheet.col_values(1)
        for row_idx, cell_value in enumerate(date_column, start=1):
            if row_idx <= header_row:
                continue
            if str(cell_value).strip() == label:
                print(f"    Date range '{label}' found at row {row_idx}")
                return row_idx
        return None
    except Exception as e:
        print(f"    Error finding date range row: {e}")
        return None


def _parse_date_range_from_cell(cell_str: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Parse a date range string like '31 Mar - 6 Apr 2026' or '1 - 7 Apr 2026'.
    Returns (start_date, end_date) or None.
    """
    cell_str = cell_str.strip()
    if ' - ' not in cell_str:
        return None

    from calendar import month_name, month_abbr
    month_names = {m.lower(): i for i, m in enumerate(month_name) if m}
    month_abbrs = {m.lower(): i for i, m in enumerate(month_abbr) if m}

    def resolve_month(s):
        s = s.lower()
        return month_names.get(s) or month_abbrs.get(s)

    parts = cell_str.split(' - ')
    if len(parts) != 2:
        return None

    left = parts[0].strip().split()
    right = parts[1].strip().split()

    try:
        # Right side always has day month year (or day month year)
        if len(right) >= 3:
            end_day = int(right[0])
            end_month = resolve_month(right[1])
            end_year = int(right[2])
        elif len(right) == 2:
            # e.g. "6 Apr" - no year, assume current year
            end_day = int(right[0])
            end_month = resolve_month(right[1])
            end_year = datetime.now().year
        else:
            return None

        if not end_month:
            return None

        # Left side: could be "31 Mar" or just "1"
        if len(left) >= 2:
            start_day = int(left[0])
            start_month = resolve_month(left[1])
            start_year = end_year  # Same year context
        elif len(left) == 1:
            start_day = int(left[0])
            start_month = end_month
            start_year = end_year
        else:
            return None

        if not start_month:
            start_month = end_month

        return (datetime(start_year, start_month, start_day),
                datetime(end_year, end_month, end_day))
    except (ValueError, TypeError):
        return None


def add_date_range_row(worksheet, start_date: datetime, end_date: datetime,
                       header_row: int) -> int:
    """
    Add a new date range row in correct chronological order.
    Newest dates go on top (closer to header), older dates go down.

    Returns row number where the date range was added.
    """
    label = format_date_range_label(start_date, end_date)
    target_start = datetime(start_date.year, start_date.month, start_date.day)

    try:
        date_column = worksheet.col_values(1)

        # Collect existing date range rows
        range_rows = []  # list of (row_number, start_datetime)
        last_data_row = header_row

        for row_idx in range(header_row + 1, len(date_column) + 1):
            cell_str = str(date_column[row_idx - 1]).strip() if row_idx - 1 < len(date_column) else ""
            if cell_str:
                last_data_row = row_idx
                parsed = _parse_date_range_from_cell(cell_str)
                if parsed:
                    range_rows.append((row_idx, parsed[0]))  # Use start date for ordering

        if not range_rows:
            new_row = header_row + 1
            worksheet.update_cell(new_row, 1, label)
            print(f"    Added date range '{label}' at row {new_row} (first entry)")
            return new_row

        # Newest on top: insert before the first existing row that is OLDER than target
        insert_before_row = None
        for row_num, existing_start in range_rows:
            if existing_start < target_start:
                insert_before_row = row_num
                break

        if insert_before_row:
            worksheet.insert_row([label], insert_before_row)
            print(f"    Inserted date range '{label}' at row {insert_before_row} (newest on top)")
            return insert_before_row
        else:
            # Target is older than all existing - append at end
            new_row = last_data_row + 1
            worksheet.update_cell(new_row, 1, label)
            print(f"    Added date range '{label}' at row {new_row} (oldest)")
            return new_row

    except Exception as e:
        print(f"    Error adding date range row: {e}")
        raise


# =============================================================================
# WEEKLY SUMMARY PAGE
# =============================================================================

SUMMARY_SHEET_NAME = "Highest spend LPs per week"


def _get_or_create_summary_sheet(spreadsheet, mode: str = 'weekly') -> gspread.Worksheet:
    """Get or create the weekly summary sheet."""
    for ws in spreadsheet.worksheets():
        if ws.title == SUMMARY_SHEET_NAME:
            print(f"  Found existing summary sheet: '{SUMMARY_SHEET_NAME}'")
            return ws

    print(f"  Creating summary sheet: '{SUMMARY_SHEET_NAME}'")
    ws = spreadsheet.add_worksheet(title=SUMMARY_SHEET_NAME, rows=1000, cols=25)
    return ws


def _find_summary_section(worksheet, date_range_label: str) -> Optional[Tuple[int, int]]:
    """
    Find an existing section in the summary sheet for this date range.
    Returns (section_start_row, section_end_row) or None.
    A section starts with the date range label in col A and ends at the
    row before the next date range label or the last data row.
    """
    try:
        col_a = worksheet.col_values(1)
        section_start = None
        section_end = None

        for row_idx, cell in enumerate(col_a, start=1):
            cell_str = str(cell).strip()
            if cell_str == date_range_label:
                section_start = row_idx
            elif section_start and _parse_date_range_from_cell(cell_str):
                section_end = row_idx - 1
                break

        if section_start and not section_end:
            # Find actual last row with data in this section
            section_end = section_start
            for row_idx in range(section_start + 1, len(col_a) + 1):
                cell_str = str(col_a[row_idx - 1]).strip() if row_idx - 1 < len(col_a) else ""
                if cell_str:
                    section_end = row_idx
                else:
                    # Check if next row also empty (end of section)
                    if row_idx < len(col_a):
                        next_cell = str(col_a[row_idx]).strip()
                        if not next_cell:
                            break
                    else:
                        break

        return (section_start, section_end) if section_start else None
    except Exception as e:
        print(f"    Error finding summary section: {e}")
        return None


def _delete_rows_range(worksheet, start_row: int, end_row: int):
    """Delete a range of rows from a worksheet."""
    if start_row > end_row:
        return
    worksheet.delete_rows(start_row, end_row)


def _categorize_landing_page(url: str) -> str:
    """
    Categorize a landing page URL into a region group.

    Rules:
        - URL contains 'currency=GBP' -> UK
        - Domain is wesmyle.de -> DE
        - URL contains '/nl/' -> NL
        - Domain is wesmyle.com without '/nl/' -> REO
        - Anything else -> Other
    """
    url_lower = url.lower()
    if 'currency=gbp' in url_lower:
        return 'UK'
    if 'wesmyle.de' in url_lower:
        return 'DE'
    if '/nl/' in url_lower:
        return 'NL'
    if 'wesmyle.com' in url_lower:
        return 'REO'
    return 'Other'


# Category display order for the summary sheet
_CATEGORY_ORDER = ['NL', 'DE', 'REO', 'UK', 'Other']


def write_summary_section(worksheet, start_date: datetime, end_date: datetime,
                          datads_data: List[Dict], mode: str = 'weekly'):
    """
    Write or update a section in the summary sheet for a date range.
    Newest date ranges go to the top.

    Section structure:
        Row: Date range label (e.g. "31 Mar - 6 Apr 2026")
        Row: Headers (Landing page, metric1, metric2, ...)
        Row: "NL" category label
        Row: NL LP1 URL + metrics
        Row: NL LP2 URL + metrics
        Row: (empty separator)
        Row: "DE" category label
        Row: DE LP1 URL + metrics
        Row: (empty separator)
        ... (REO, UK, Other)
        Row: (empty separator)
    """
    date_range_label = format_date_range_label(start_date, end_date)
    mappings = get_column_mapping_objects(mode)
    headers = ["Landing page"] + [m.sheet_column for m in mappings]

    print(f"\n  [SUMMARY] Writing section for '{date_range_label}'")

    # Check if section already exists
    existing = _find_summary_section(worksheet, date_range_label)
    if existing:
        print(f"    Removing existing section (rows {existing[0]}-{existing[1]})")
        _delete_rows_range(worksheet, existing[0], existing[1])

    # Categorize landing pages into groups
    categories: Dict[str, List[Dict]] = {cat: [] for cat in _CATEGORY_ORDER}
    for datads_row in datads_data:
        lp = datads_row.get('Landing page', '')
        if not lp or lp == 'Unknown':
            continue
        cat = _categorize_landing_page(lp)
        categories[cat].append(datads_row)

    # Build section rows
    section_rows = []
    section_rows.append([date_range_label])  # Date range label
    section_rows.append(headers)  # Headers

    for cat in _CATEGORY_ORDER:
        rows = categories[cat]
        if not rows:
            continue
        # Category label row
        section_rows.append([cat])
        print(f"    Category '{cat}': {len(rows)} landing pages")
        for datads_row in rows:
            lp = datads_row.get('Landing page', '')
            row_values = [lp]
            for mapping in mappings:
                raw_val = datads_row.get(mapping.datads_field, '')
                row_values.append(parse_value(raw_val) if raw_val else '')
            section_rows.append(row_values)
        # Empty separator row after each category
        section_rows.append([''])

    # If no data was added (all rows were Unknown), add an empty separator
    if len(section_rows) == 2:
        section_rows.append([''])

    # Determine insert position - newest on top
    col_a = worksheet.col_values(1)

    # Find first existing date range section
    insert_row = 1
    target_start = datetime(start_date.year, start_date.month, start_date.day)

    for row_idx, cell in enumerate(col_a, start=1):
        cell_str = str(cell).strip()
        parsed = _parse_date_range_from_cell(cell_str)
        if parsed:
            existing_start = parsed[0]
            if existing_start < target_start:
                insert_row = row_idx
                break
            else:
                # This section is newer or same, skip past it
                continue
    else:
        # No older sections found; append after all content
        insert_row = len(col_a) + 1 if col_a else 1

    # If inserting at top (row 1) or before existing content
    if insert_row <= len(col_a):
        # Insert rows by shifting content down
        for i, row_data in enumerate(reversed(section_rows)):
            worksheet.insert_row(row_data, insert_row, value_input_option='USER_ENTERED')
        print(f"    Inserted {len(section_rows)} rows at position {insert_row}")
    else:
        # Append at end using a single batch update (avoids rate limits)
        max_cols = max(len(row) for row in section_rows)
        # Pad all rows to the same width
        padded = [row + [''] * (max_cols - len(row)) for row in section_rows]
        end_row = insert_row + len(padded) - 1
        start_cell = gspread.utils.rowcol_to_a1(insert_row, 1)
        end_cell = gspread.utils.rowcol_to_a1(end_row, max_cols)
        cell_range = f"{start_cell}:{end_cell}"
        worksheet.update(cell_range, padded, value_input_option='USER_ENTERED')
        print(f"    Batch-wrote {len(padded)} rows at {cell_range}")


# =============================================================================
# WEEKLY ORCHESTRATOR
# =============================================================================

def write_datads_weekly_data_to_sheets(start_date: datetime, end_date: datetime,
                                       datads_data: List[Dict]):
    """
    Write weekly DataAds data to Google Sheets.

    1. Write to individual weekly_<name> tabs (same as daily but with date range label)
    2. Write/update the summary sheet ("Highest spend LPs per week")

    Args:
        start_date: Start of the week
        end_date: End of the week
        datads_data: List of dictionaries from DataAds extraction
    """
    date_range_label = format_date_range_label(start_date, end_date)
    print(f"\n[DATADS WEEKLY SHEETS] Writing weekly data to Google Sheets...")
    print(f"  Date range: {date_range_label}")

    print_column_mappings('weekly')

    if not datads_data:
        print("  No DataAds data to write")
        return False

    print(f"\n  Total DataAds rows: {len(datads_data)}")

    # Show data summary
    print("\n" + "=" * 80)
    print("DATADS WEEKLY DATA SUMMARY")
    print("=" * 80)
    for i, row in enumerate(datads_data):
        lp = row.get('Landing page', 'N/A')
        purchases = row.get('Purchases', '?')
        spend = row.get('Spend', '?')
        roas = row.get('Purchase ROAS', '?')
        print(f"  [{i+1}] {lp} | Purchases: {purchases} | Spend: {spend} | ROAS: {roas}")
    print("=" * 80)

    try:
        _mode = 'weekly'
        spreadsheet = get_datads_spreadsheet()
        print(f"\n  Opened spreadsheet: {spreadsheet.title}")

        # --- Part 1: Individual weekly tabs ---
        print(f"\n  --- INDIVIDUAL WEEKLY TABS ---")
        weekly_sheets = get_worksheets_by_prefix(spreadsheet, "weekly")
        print(f"  Found {len(weekly_sheets)} existing weekly sheets")

        # Build URL lookup
        url_to_sheet = {}
        for ws in weekly_sheets:
            sheet_url = find_url_in_sheet(ws)
            if sheet_url:
                url_to_sheet[ws.title] = (ws, sheet_url)

        existing_names = [ws.title for ws in spreadsheet.worksheets()]

        sheets_updated = 0
        sheets_created = 0
        sheets_skipped = 0

        import time as _time

        for idx, datads_row in enumerate(datads_data):
            landing_page = datads_row.get('Landing page', '')
            if not landing_page or landing_page == 'Unknown':
                sheets_skipped += 1
                continue

            # Pace API calls to avoid rate limits (1s pause every 3 tabs)
            if idx > 0 and idx % 3 == 0:
                _time.sleep(1)

            print(f"\n" + "-" * 80)
            print(f"  PROCESSING: {landing_page}")
            print("-" * 80)

            # Find matching worksheet
            matched_ws = None
            for ws_title, (ws, sheet_url) in url_to_sheet.items():
                if match_datads_row_to_url([datads_row], sheet_url):
                    matched_ws = ws
                    print(f"  [MATCH] Found existing sheet: '{ws_title}'")
                    break

            if not matched_ws:
                print(f"  [NEW] Creating new weekly tab...")
                matched_ws = create_daily_worksheet(spreadsheet, landing_page,
                                                     existing_names, mode='weekly')
                existing_names.append(matched_ws.title)
                url_to_sheet[matched_ws.title] = (matched_ws, landing_page)
                sheets_created += 1

            # Find header row
            header_row, column_mapping = find_header_row(matched_ws, mode=_mode)
            if not header_row:
                print(f"  [SKIP] No header row found in '{matched_ws.title}'")
                sheets_skipped += 1
                continue

            # Find or create date range row
            date_row = find_date_range_row(matched_ws, start_date, end_date, header_row)
            if date_row:
                print(f"  Date range row exists at row {date_row} - will update")
            else:
                print(f"  Date range not found - adding new row...")
                date_row = add_date_range_row(matched_ws, start_date, end_date, header_row)

            # Map and write data
            column_data = map_datads_to_sheet_columns(datads_row, column_mapping, mode='weekly')
            if not column_data:
                print(f"  [SKIP] No mappable data found")
                sheets_skipped += 1
                continue

            write_data_to_sheet(matched_ws, date_row, column_data)
            print(f"  [SUCCESS] Data written to '{matched_ws.title}' row {date_row}")
            sheets_updated += 1

        # Pause before summary to let API quota recover
        print(f"\n  Waiting for API quota to recover...")
        _time.sleep(5)

        # --- Part 2: Summary sheet ---
        print(f"\n  --- SUMMARY SHEET ---")
        summary_ws = _get_or_create_summary_sheet(spreadsheet, mode='weekly')
        write_summary_section(summary_ws, start_date, end_date, datads_data, mode='weekly')

        print(f"\n[DATADS WEEKLY SHEETS] Completed:")
        print(f"  Weekly tabs updated: {sheets_updated}")
        print(f"  Weekly tabs created: {sheets_created}")
        print(f"  Skipped: {sheets_skipped}")
        print(f"  Summary sheet: updated")
        return sheets_updated > 0 or sheets_created > 0

    except Exception as e:
        print(f"\n[DATADS WEEKLY SHEETS] Error: {e}")
        import traceback
        traceback.print_exc()
        return False
