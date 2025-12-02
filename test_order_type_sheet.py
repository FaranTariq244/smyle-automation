"""
Test script to check for Nov 1 data in ORDER_TYPE_SHEET_URL
"""

from dotenv import load_dotenv
from services.sheets.helpers import get_order_type_worksheet
from datetime import datetime
import os

def col_index_to_letter(col_idx):
    """Convert column index (1-based) to letter (A, B, ..., Z, AA, AB, ...)"""
    result = ''
    while col_idx > 0:
        col_idx -= 1
        result = chr(65 + (col_idx % 26)) + result
        col_idx //= 26
    return result


def check_date_data(date_obj):
    """
    Check if data exists for a specific date in the Order Type sheet.

    Args:
        date_obj: datetime object for the date to check
    """
    try:
        print("=" * 80)
        print(f"CHECKING ORDER TYPE SHEET FOR {date_obj.strftime('%B %d, %Y')}".center(80))
        print("=" * 80)

        # Load environment
        load_dotenv()

        # Get worksheet
        print("\n[1/3] Connecting to Google Sheets...")
        ws = get_order_type_worksheet()
        print(f"  [OK] Connected to: {ws.spreadsheet.title}")
        print(f"  [OK] Worksheet: {ws.title}")

        # Format date as shown in screenshot: "Oct 14" or "Nov 01"
        date_str = date_obj.strftime('%b %d')
        # Alternative format without leading zero
        date_str_alt = date_obj.strftime('%b %-d') if os.name != 'nt' else date_obj.strftime('%b %d').replace(' 0', ' ')

        print(f"\n[2/3] Searching for date '{date_str}' (or '{date_str_alt}')...")

        # Find the row with matching date
        target_row = None
        target_col = None

        # Get all values to search
        all_values = ws.get_all_values()

        # Search for the date in the sheet (exact match)
        for row_idx, row in enumerate(all_values, start=1):
            for col_idx, cell_value in enumerate(row, start=1):
                cell_str = str(cell_value).strip()
                # Exact match or match with year
                if cell_str == date_str or cell_str == date_str_alt or \
                   cell_str.startswith(date_str + ' ') or cell_str.startswith(date_str_alt + ' '):
                    target_row = row_idx
                    target_col = col_idx
                    print(f"  [OK] Found date '{cell_value}' at row {target_row}, column {target_col}")
                    break
            if target_row:
                break

        if not target_row:
            print(f"  [NOT FOUND] Could not find date '{date_str}' or '{date_str_alt}' in the sheet")
            print(f"\n  First few rows of the sheet:")
            for i, row in enumerate(all_values[:5], start=1):
                print(f"    Row {i}: {row[:10]}...")  # Show first 10 columns
            return

        col_letter = col_index_to_letter(target_col)
        print(f"  [OK] Data column: '{col_letter}'")

        print(f"\n[3/3] Reading data from column {col_letter}...")

        # Define the cell mapping based on write_order_type_data
        cell_mapping = {
            f'{col_letter}8': 'first_subscription > Net Revenue',
            f'{col_letter}9': 'first_single > Net Revenue',
            f'{col_letter}11': 'repeat_subscription > Net Revenue',
            f'{col_letter}12': 'repeat_single > Net Revenue',
            f'{col_letter}16': 'first_subscription > Count',
            f'{col_letter}17': 'first_single > Count',
            f'{col_letter}19': 'repeat_subscription > Count',
            f'{col_letter}20': 'repeat_single > Count',
            f'{col_letter}33': 'Marketing Spend',
            f'{col_letter}46': 'Klaviyo (Purchases - NC)',
            f'{col_letter}47': 'Klaviyo ((Revenue - NC Revenue) / 1.21)',
        }

        # Read all the cells at once
        cell_ranges = list(cell_mapping.keys())
        values = ws.batch_get(cell_ranges)

        print("\n" + "=" * 80)
        print(f"DATA FOR {date_str}".center(80))
        print("=" * 80)

        # Display the data
        print("\nORDER TYPE METRICS - NET REVENUE:")
        print("-" * 60)
        has_data = False

        for i, cell_range in enumerate(cell_ranges):
            cell_value = values[i][0][0] if values[i] and values[i][0] else ''
            label = cell_mapping[cell_range]

            # Format the output
            if cell_value:
                has_data = True
                try:
                    # Try to parse as number
                    num_value = float(cell_value.replace(',', '').replace('€', '').strip())

                    if 'Net Revenue' in label or 'Spend' in label or 'Klaviyo ((Revenue' in label:
                        print(f"  {label:<45} €{num_value:,.2f}")
                    elif 'Count' in label or 'Klaviyo (Purchases' in label:
                        print(f"  {label:<45} {int(num_value):,}")
                    else:
                        print(f"  {label:<45} {num_value}")
                except:
                    print(f"  {label:<45} {cell_value}")
            else:
                print(f"  {label:<45} (empty)")

        print("\n" + "=" * 80)

        if has_data:
            print(f"\n[SUCCESS] Found data for {date_str}!")
        else:
            print(f"\n[WARNING] No data found for {date_str} - all cells are empty")

        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\n[ERROR] Error checking sheet: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main execution."""
    # Check for November 1st, 2025
    date_obj = datetime(2025, 11, 1)
    check_date_data(date_obj)

    # Optional: Allow user to check other dates
    print("\n\nWould you like to check another date? (press Enter to skip)")
    user_input = input("Enter date (DD-MMM-YYYY, e.g., 01-Nov-2025): ").strip()

    if user_input:
        try:
            date_obj = datetime.strptime(user_input, '%d-%b-%Y')
            check_date_data(date_obj)
        except Exception as e:
            print(f"Invalid date format: {e}")


if __name__ == "__main__":
    main()
