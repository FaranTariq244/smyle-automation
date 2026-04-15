"""
Google Sheets read/write helpers.
All operations strictly aligned to COLUMNS defined in columns.py.
"""

from typing import Dict, List
from services.sheets.client import get_worksheet
from services.sheets.columns import COLUMNS


def ensure_headers():
    """
    Ensure the worksheet has the correct headers in row 1.

    If headers don't match COLUMNS exactly:
    - Updates row 1 with correct headers from COLUMNS
    - Does NOT delete any data rows

    This enforces that the worksheet structure matches COLUMNS at all times.
    """
    ws = get_worksheet()
    current = ws.row_values(1)

    # Only update headers if they don't match, without deleting data
    if current != COLUMNS:
        ws.update("A1", [COLUMNS])


def append_rows(rows: List[Dict]):
    """
    Write rows to the worksheet. If a row with the same date (Column C) already
    exists, updates that row. Otherwise appends a new row.

    Args:
        rows: List of dictionaries where keys MUST match COLUMNS exactly.
              Any missing key becomes an empty string.
              Keys not in COLUMNS are ignored.
              The order of values follows COLUMNS order (not dict order).

    The function:
    1. Ensures headers are correct
    2. Converts each dict to a list of values aligned to COLUMNS
    3. For each row, checks if Column C (Date) already has a matching date
       - If found: updates that existing row (prevents duplicates)
       - If not found: appends to the first empty row
    """
    import time

    ws = get_worksheet()
    ensure_headers()
    values = [[row.get(col, "") for col in COLUMNS] for row in rows]

    # Read Column C (Date) to check for existing entries
    column_c_values = ws.col_values(3)  # Column C = Date

    for row_values in values:
        row_date = str(row_values[2]).strip()  # Column C is index 2 (Date)

        # Check if this date already exists in the sheet
        existing_row = None
        if row_date:
            for idx, cell_value in enumerate(column_c_values[1:], start=2):  # Skip header
                if str(cell_value).strip() == row_date:
                    existing_row = idx
                    break

        if existing_row:
            # Update the existing row
            print(f"  [SHEET] Date {row_date} found at row {existing_row} - updating existing row")
            target_row = existing_row
        else:
            # Find the first empty row in Column E (Conversion data column)
            column_e_values = ws.col_values(5)  # Column E
            target_row = 2  # Start from row 2 (after header)
            for idx, value in enumerate(column_e_values[1:], start=2):
                if not value or str(value).strip() == "":
                    target_row = idx
                    break
            else:
                target_row = len(column_e_values) + 1
            print(f"  [SHEET] Date {row_date} not found - writing to new row {target_row}")

        # Write with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ws.update(f"A{target_row}", [row_values], value_input_option='USER_ENTERED')
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"  Retry {attempt + 1}/{max_retries} - waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise


def read_all() -> List[Dict]:
    """
    Read all data from the worksheet.

    Returns:
        List of dictionaries where each dict represents a row.
        Keys are the column headers from row 1 (enforced to match COLUMNS).
        Empty cells become empty strings.

    Assumes row 1 contains headers (enforced by ensure_headers()).

    Example:
        [
            {"Email": "test@example.com", "Username": "testuser", ...},
            {"Email": "other@example.com", "Username": "otheruser", ...}
        ]
    """
    ws = get_worksheet()
    ensure_headers()
    return ws.get_all_records()
