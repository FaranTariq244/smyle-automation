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
    Append rows to the worksheet.

    Args:
        rows: List of dictionaries where keys MUST match COLUMNS exactly.
              Any missing key becomes an empty string.
              Keys not in COLUMNS are ignored.
              The order of values follows COLUMNS order (not dict order).

    Example:
        append_rows([
            {"Email": "test@example.com", "Username": "testuser"},
            {"Email": "other@example.com", "Username": "otheruser"}
        ])

    The function:
    1. Ensures headers are correct
    2. Converts each dict to a list of values aligned to COLUMNS
    3. Finds the first empty row based on Column E (Conversion data column)
    4. Appends the values starting from that row (ignoring formatting-only rows)
    """
    import time

    ws = get_worksheet()
    ensure_headers()
    values = [[row.get(col, "") for col in COLUMNS] for row in rows]

    # Find the first empty row in Column E (index 4 in COLUMNS - "Conversion (<4 = red, 4-5 = orange, 5 > green)")
    # This ensures we ignore rows that only have formatting but no actual data
    # Column E is the 5th column (A=1, B=2, C=3, D=4, E=5)
    column_e_values = ws.col_values(5)  # Column E

    # Find first empty cell in Column E (skip header row)
    start_row = 2  # Start from row 2 (after header)
    for idx, value in enumerate(column_e_values[1:], start=2):  # Skip header at index 0
        if not value or str(value).strip() == "":
            start_row = idx
            break
    else:
        # If all cells have values, append after the last row
        start_row = len(column_e_values) + 1

    # Retry logic for handling temporary Google Sheets API errors
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ws.update(f"A{start_row}", values, value_input_option='USER_ENTERED')
            break  # Success - exit retry loop
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} - waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise  # Final attempt failed - re-raise the exception


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
