"""
Helper functions for writing marketing data to Google Sheets.
These helpers convert your extracted data into the correct format for the sheets.
"""

import os
from typing import Dict
from datetime import datetime
from services.sheets.io import append_rows
import gspread
from google.oauth2.service_account import Credentials
from config_store import get_setting


def write_marketing_data(date_obj, overall_data: Dict, facebook_data: Dict, google_data: Dict):
    """
    Write marketing data to Google Sheets.

    Args:
        date_obj: datetime object or date string for the day
        overall_data: Dictionary with overall metrics (keys: Conversions, Spend, ROAS, AOV, Conversion %)
        facebook_data: Dictionary with Facebook metrics (keys: Conversions, Spend, ROAS, AOV, Conversion %)
        google_data: Dictionary with Google Ads metrics (keys: Conversions, Spend, ROAS, AOV, Conversion %)

    Example:
        write_marketing_data(
            date_obj=datetime(2025, 1, 9),
            overall_data={
                "Conversions": 10,
                "Spend": 500.50,
                "ROAS": 2.5,
                "AOV": 75.25,
                "Conversion %": 1.5
            },
            facebook_data={...},
            google_data={...}
        )
    """
    # Extract day number
    if isinstance(date_obj, datetime):
        day = date_obj.day
    else:
        # If it's a string, try to parse it
        try:
            day = datetime.strptime(str(date_obj), "%Y-%m-%d").day
        except:
            day = date_obj  # Assume it's already a day number

    # Map data to column structure using EXACT column names from columns.py
    row = {
        "": "",  # Columns A, B, C (empty placeholders)
        "Days": day,  # Column D

        # Overall metrics (Columns E-I)
        "Conversion (<4 = red, 4-5 = orange, 5 > green)": round(overall_data.get("Conversion %", 0), 2),  # Column E
        "Total spend per day": round(overall_data.get("Spend", 0), 2),  # Column F
        "ROAS total": round(overall_data.get("ROAS", 0), 2),  # Column G
        "AOV total": round(overall_data.get("AOV", 0), 2),  # Column H
        "Conversies total per day": int(overall_data.get("Conversions", 0)),  # Column I

        # Facebook metrics (Columns J-N)
        "Conversion meta (<3 = red, 3-4 = orange, 4 > green)": round(facebook_data.get("Conversion %", 0), 2),  # Column J
        "Total spend per day in Meta excl. lead gen ad": round(facebook_data.get("Spend", 0), 2),  # Column K
        "Roas Meta": round(facebook_data.get("ROAS", 0), 2),  # Column L
        "AOV meta": round(facebook_data.get("AOV", 0), 2),  # Column M
        "Conversions Meta per day": int(facebook_data.get("Conversions", 0)),  # Column N

        # Google metrics (Columns O-S)
        "Conversions (<4 = red, 4-5 = orange, 5 > green)": round(google_data.get("Conversion %", 0), 2),  # Column O (now unique!)
        "Total spend per day in Google": round(google_data.get("Spend", 0), 2),  # Column P
        "Roas Google": round(google_data.get("ROAS", 0), 2),  # Column Q
        "AOV": round(google_data.get("AOV", 0), 2),  # Column R
        "Conversions Google per day": int(google_data.get("Conversions", 0)),  # Column S
    }

    # Write to sheet
    append_rows([row])

    print(f"  ✓ Data written to Google Sheets!")
    print(f"    Day: {day}")
    print(f"    Overall: Conv%={overall_data.get('Conversion %', 0):.2f}%, "
          f"Spend=€{overall_data.get('Spend', 0):.2f}, "
          f"Conv={int(overall_data.get('Conversions', 0))}")
    print(f"    Facebook: Conv%={facebook_data.get('Conversion %', 0):.2f}%, "
          f"Spend=€{facebook_data.get('Spend', 0):.2f}, "
          f"Conv={int(facebook_data.get('Conversions', 0))}")
    print(f"    Google: Conv%={google_data.get('Conversion %', 0):.2f}%, "
          f"Spend=€{google_data.get('Spend', 0):.2f}, "
          f"Conv={int(google_data.get('Conversions', 0))}")


def get_order_type_worksheet():
    """
    Get the Order Type Google Sheets worksheet.
    Reads full Google Sheets URL from the local config store and extracts IDs automatically.

    Stored value required:
        ORDER_TYPE_SHEET_URL - Full Google Sheets URL
            Example: https://docs.google.com/spreadsheets/d/1d8KEO_R3PUsEdnTvt1MmSrYd0MbHxPIQOt1hv0m0zVs/edit?gid=1449465974

    Returns:
        gspread.Worksheet: The worksheet object
    """
    import json
    import re

    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # Get credentials
    env_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if env_json:
        info = json.loads(env_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("auth.json", scopes=SCOPES)

    # Authorize and open the specific spreadsheet by ID
    gc = gspread.authorize(creds)

    # Get full URL from local config
    sheet_url = get_setting("ORDER_TYPE_SHEET_URL")

    if not sheet_url:
        raise ValueError("ORDER_TYPE_SHEET_URL is not set. Please add it via the GUI settings or config.db.")

    # Parse spreadsheet ID from URL
    # Pattern: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit...
    spreadsheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not spreadsheet_match:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {sheet_url}")

    spreadsheet_id = spreadsheet_match.group(1)

    # Parse worksheet ID (gid) from URL
    # Pattern: ?gid={WORKSHEET_ID} or #gid={WORKSHEET_ID}
    worksheet_match = re.search(r'[?#&]gid=(\d+)', sheet_url)
    if not worksheet_match:
        raise ValueError(f"Could not extract worksheet ID (gid) from URL: {sheet_url}")

    worksheet_id = worksheet_match.group(1)

    # Open spreadsheet and get worksheet
    ss = gc.open_by_key(spreadsheet_id)
    worksheet = ss.get_worksheet_by_id(int(worksheet_id))

    return worksheet


def write_order_type_data(date_obj, order_type_metrics: Dict, marketing_spend: float, klaviyo_metrics: Dict, conversion_rate: float = 0.0):
    """
    Write Order Type, Marketing Spend, Klaviyo data, and Conversion Rate to Google Sheets.

    This function:
    1. Finds the row matching the date (searches column with date values)
    2. Updates specific cells in that row with the extracted data

    Args:
        date_obj: datetime object for the day
        order_type_metrics: Dictionary with order type data
            Expected keys: first_subscription, first_single, repeat_subscription, repeat_single
            Each has: {'Net Revenue': float, 'Count': int}
        marketing_spend: Float value for marketing spend
        klaviyo_metrics: Dictionary with Klaviyo data
            Expected keys: purchases, nc, revenue, nc_revenue
        conversion_rate: Float value for conversion rate from Converge (default: 0.0)

    Column Mapping (Column O):
        O8  = first_subscription > Net Revenue (0 if empty)
        O9  = first_single > Net Revenue
        O11 = repeat_subscription > Net Revenue
        O12 = repeat_single > Net Revenue
        O16 = first_subscription > Count (0 if empty)
        O17 = first_single > Count
        O19 = repeat_subscription > Count
        O20 = repeat_single > Count
        O33 = Marketing Spend
        O46 = Klaviyo Purchases - NC
        O47 = (Klaviyo Revenue - NC Revenue) / 1.21
        O81 = Conversion Rate from Converge
    """
    try:
        ws = get_order_type_worksheet()

        # Format date as shown in screenshot: "Oct 14" or "Nov 1"
        # Note: Some sheets use "Nov 1" (no leading zero), others use "Nov 01"
        date_str = date_obj.strftime('%b %d')  # With leading zero: "Nov 01"
        # Alternative format without leading zero: "Nov 1"
        date_str_alt = date_obj.strftime('%b %d').replace(' 0', ' ')  # "Nov 1"

        print(f"\n  Looking for date '{date_str}' or '{date_str_alt}' in Google Sheets...")

        # Find the row with matching date
        # The screenshot shows dates in a specific format, let's search for it
        target_row = None
        target_col = None

        # Get all values to search
        all_values = ws.get_all_values()

        # Search for the date in the sheet (exact match)
        # Based on screenshot, dates appear to be in row headers on the right side
        for row_idx, row in enumerate(all_values, start=1):
            for col_idx, cell_value in enumerate(row, start=1):
                cell_str = str(cell_value).strip()
                # Exact match or match with year
                if cell_str == date_str or cell_str == date_str_alt or \
                   cell_str.startswith(date_str + ' ') or cell_str.startswith(date_str_alt + ' '):
                    target_row = row_idx
                    target_col = col_idx  # Already 1-based from enumerate
                    print(f"  ✓ Found date '{cell_value}' at row {target_row}, column {target_col}")
                    break
            if target_row:
                break

        if not target_row:
            print(f"  ✗ Could not find date '{date_str}' or '{date_str_alt}' in the sheet")
            print(f"  Please ensure the date row exists in the spreadsheet")
            print(f"\n  Tip: Check the first few rows to see the date format:")
            for i, row in enumerate(all_values[:5], start=1):
                print(f"    Row {i}: {row[:10] if len(row) > 10 else row}")
            return

        # Convert column index to letter (1=A, 2=B, ..., 15=O, 16=P, etc.)
        def col_index_to_letter(col_idx):
            """Convert column index (1-based) to letter (A, B, ..., Z, AA, AB, ...)"""
            result = ''
            while col_idx > 0:
                col_idx -= 1
                result = chr(65 + (col_idx % 26)) + result
                col_idx //= 26
            return result

        col_letter = col_index_to_letter(target_col)
        print(f"  Using column '{col_letter}' for updates")

        # Helper function to safely get values
        def get_net_revenue(order_type_name):
            return order_type_metrics.get(order_type_name, {}).get('Net Revenue', 0)

        def get_count(order_type_name):
            return int(order_type_metrics.get(order_type_name, {}).get('Count', 0))

        # Prepare updates as a batch
        updates = []

        # O8 = first_subscription > Net Revenue
        updates.append({
            'range': f'{col_letter}8',
            'values': [[get_net_revenue('first_subscription')]]
        })

        # O9 = first_single > Net Revenue
        updates.append({
            'range': f'{col_letter}9',
            'values': [[get_net_revenue('first_single')]]
        })

        # O11 = repeat_subscription > Net Revenue
        updates.append({
            'range': f'{col_letter}11',
            'values': [[get_net_revenue('repeat_subscription')]]
        })

        # O12 = repeat_single > Net Revenue
        updates.append({
            'range': f'{col_letter}12',
            'values': [[get_net_revenue('repeat_single')]]
        })

        # O16 = first_subscription > Count
        updates.append({
            'range': f'{col_letter}16',
            'values': [[get_count('first_subscription')]]
        })

        # O17 = first_single > Count
        updates.append({
            'range': f'{col_letter}17',
            'values': [[get_count('first_single')]]
        })

        # O19 = repeat_subscription > Count
        updates.append({
            'range': f'{col_letter}19',
            'values': [[get_count('repeat_subscription')]]
        })

        # O20 = repeat_single > Count
        updates.append({
            'range': f'{col_letter}20',
            'values': [[get_count('repeat_single')]]
        })

        # O33 = Marketing Spend
        updates.append({
            'range': f'{col_letter}33',
            'values': [[marketing_spend]]
        })

        # O46 = Klaviyo Purchases - NC
        klaviyo_diff = klaviyo_metrics.get('purchases', 0) - klaviyo_metrics.get('nc', 0)
        updates.append({
            'range': f'{col_letter}46',
            'values': [[klaviyo_diff]]
        })

        # O47 = (Revenue - NC Revenue) / 1.21
        revenue = klaviyo_metrics.get('revenue', 0)
        nc_revenue = klaviyo_metrics.get('nc_revenue', 0)
        klaviyo_calc = (revenue - nc_revenue) / 1.21 if (revenue - nc_revenue) != 0 else 0
        updates.append({
            'range': f'{col_letter}47',
            'values': [[klaviyo_calc]]
        })

        # O81 = Conversion Rate from Converge
        # Divide by 100 because the cell is formatted as percentage in Google Sheets
        # So 3.81 becomes 0.0381, which displays as 3.81% in a percentage-formatted cell
        conversion_rate_decimal = conversion_rate / 100 if conversion_rate != 0 else 0
        updates.append({
            'range': f'{col_letter}81',
            'values': [[conversion_rate_decimal]]
        })

        # Execute batch update
        ws.batch_update(updates, value_input_option='USER_ENTERED')

        print(f"  ✓ Successfully wrote data to Google Sheets!")
        print(f"    Date: {date_str}")
        print(f"    Order Type Revenues:")
        print(f"      first_subscription: €{get_net_revenue('first_subscription'):,.2f} (Count: {get_count('first_subscription')})")
        print(f"      first_single: €{get_net_revenue('first_single'):,.2f} (Count: {get_count('first_single')})")
        print(f"      repeat_subscription: €{get_net_revenue('repeat_subscription'):,.2f} (Count: {get_count('repeat_subscription')})")
        print(f"      repeat_single: €{get_net_revenue('repeat_single'):,.2f} (Count: {get_count('repeat_single')})")
        print(f"    Marketing Spend: €{marketing_spend:,.2f}")
        print(f"    Klaviyo (Purchases - NC): {klaviyo_diff}")
        print(f"    Klaviyo ((Rev - NC Rev) / 1.21): €{klaviyo_calc:,.2f}")
        print(f"    Conversion Rate: {conversion_rate}%")

    except Exception as e:
        print(f"\n  ✗ Failed to write to Google Sheets: {e}")
        print(f"\nTroubleshooting:")
        print(f"  1. Check that auth.json exists in project root")
        print(f"  2. Check that Google Sheet is shared with service account")
        print(f"  3. Verify the spreadsheet ID and worksheet ID are correct")
        import traceback
        traceback.print_exc()
