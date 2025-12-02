"""
Clean Daily Report Extraction - Minimal output, maximum automation
"""

from browser_manager import BrowserManager
from looker_data_extractor import LookerDataExtractor
from datetime import datetime, timedelta
import time

# Configuration: Specify the Google Sheets row to write to
# Set to None for auto-detection, or specify a row number (e.g., 200)
TARGET_SHEET_ROW = None  # Change this to the row number you want


def get_date_input():
    """Get date from user or use previous day."""
    print("\nEnter date to extract (DD-MMM-YYYY, e.g., 09-Oct-2025)")
    print("Or press Enter for previous day: ", end='')

    date_input = input().strip()

    if not date_input:
        # Use previous day
        date_obj = datetime.now() - timedelta(days=1)
        date_str = date_obj.strftime('%d-%b-%Y')
        print(f"Using previous day: {date_str}")
    else:
        try:
            date_obj = datetime.strptime(date_input, '%d-%b-%Y')
            date_str = date_input
        except:
            try:
                date_obj = datetime.strptime(date_input, '%d-%B-%Y')
                date_str = date_obj.strftime('%d-%b-%Y')
            except:
                print("Invalid date format. Using previous day instead.")
                date_obj = datetime.now() - timedelta(days=1)
                date_str = date_obj.strftime('%d-%b-%Y')

    return date_obj, date_str


def get_sheet_row_input():
    """Get Google Sheets row number from user."""
    print("\nEnter Google Sheets row number to write to (e.g., 200)")
    print("Or press Enter to auto-detect: ", end='')

    row_input = input().strip()

    if not row_input:
        print("Will auto-detect next empty row")
        return None

    try:
        row_num = int(row_input)
        if row_num < 2:
            print("Row number must be 2 or greater. Using auto-detect.")
            return None
        print(f"Will write to row {row_num}")
        return row_num
    except ValueError:
        print("Invalid row number. Using auto-detect.")
        return None


def setup_browser(report_url):
    """Setup browser and navigate to report."""
    print("\n[1/9] Opening browser...")
    manager = BrowserManager(use_existing_chrome=False)
    driver = manager.start_browser()

    print("[2/9] Navigating to report...")
    driver.get(report_url)
    time.sleep(5)

    # Check if login needed
    if "accounts.google.com" in driver.current_url:
        print("\n⚠️  Please login in the browser window...")
        while "accounts.google.com" in driver.current_url:
            time.sleep(2)
        print("✓ Login successful")

    # time.sleep(5)
    return manager, driver


def extract_data(extractor, date_obj):
    """Extract all data for the given date."""
    results = {
        'Overall': {},
        'Facebook': {},
        'Google Ads': {}
    }

    # Set date
    print(f"[3/9] Setting date range...")
    extractor.set_date_range(date_obj, date_obj)
    

    # Extract overall
    print(f"[4/9] Extracting overall metrics...")
    results['Overall'] = extractor.extract_metrics()

    print(f"[5/9] Taking overall screenshot...")
    # extractor.take_screenshot(f"screenshots/overall_{date_obj.strftime('%Y%m%d')}")

    # Facebook
    print(f"[6/9] Filtering to Facebook...")
    extractor.select_medium('Facebook')
    

    print(f"[7/9] Extracting Facebook metrics...")
    results['Facebook'] = extractor.extract_metrics()
    # extractor.take_screenshot(f"screenshots/facebook_{date_obj.strftime('%Y%m%d')}")

    # Google Ads
    print(f"[8/9] Filtering to Google Ads...")
    extractor.select_medium('Google Ads')
    

    print(f"[9/9] Extracting Google Ads metrics...")
    results['Google Ads'] = extractor.extract_metrics()
    # extractor.take_screenshot(f"screenshots/google_ads_{date_obj.strftime('%Y%m%d')}")

    return results


def display_results(date_str, results):
    """Display only the requested metrics in clean format."""
    print("\n" + "="*80)
    print(f"REPORT DATA FOR {date_str}".center(80))
    print("="*80)

    for source in ['Overall', 'Facebook', 'Google Ads']:
        metrics = results[source]

        print(f"\n{source.upper()}")
        print("-" * 40)
        print(f"Impressions:      {metrics.get('Impressions', 0):>15,.0f}")
        print(f"CTR:              {metrics.get('CTR', 0):>14.2f}%")
        print(f"Clicks:           {metrics.get('Clicks', 0):>15,.0f}")
        print(f"Conversions:      {metrics.get('Conversions', 0):>15,.0f}")
        print(f"Conversion %:     {metrics.get('Conversion %', 0):>14.2f}%")
        print(f"Online Revenue:   €{metrics.get('Online Revenue', 0):>14,.2f}")
        print(f"Spend:            €{metrics.get('Spend', 0):>14,.2f}")
        print(f"AOV:              €{metrics.get('AOV', 0):>14,.2f}")
        print(f"CPO:              €{metrics.get('CPO', 0):>14,.2f}")
        print(f"ROAS:             {metrics.get('ROAS', 0):>15.2f}")

    print("\n" + "="*80)




def save_to_google_sheets(date_obj, results):
    """Save results to Google Sheets using service account OAuth."""
    try:
        from dotenv import load_dotenv
        from services.sheets.helpers import write_marketing_data
        

        # Load environment variables
        load_dotenv()

        print("\n[10/10] Saving to Google Sheets...")

        # Write data using new service account integration
        write_marketing_data(
            date_obj,
            results['Overall'],
            results['Facebook'],
            results['Google Ads']
        )

        print("  ✓ Successfully saved to Google Sheets!")


    except Exception as e:
        print(f"\n  ✗ Failed to save to Google Sheets: {e}")
        print("\nTroubleshooting:")
        print("  1. Check that auth.json exists in project root")
        print("  2. Check that .env has SPREAD_SHEET_NAME and WORK_SHEET_NAME")
        print("  3. Check that Google Sheet is shared with service account")
        print("  4. Run: pip install -r requirements.txt")
        import traceback
        traceback.print_exc()


def main():
    """Main execution."""
    REPORT_URL = "https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/M05qB"

    print("="*80)
    print("LOOKER STUDIO DAILY REPORT EXTRACTION".center(80))
    print("="*80)

    # Get date
    date_obj, date_str = get_date_input()

    # Setup
    manager, driver = setup_browser(REPORT_URL)
    extractor = LookerDataExtractor(driver)

    try:
        # Extract
        results = extract_data(extractor, date_obj)

        # Display
        display_results(date_str, results)

        # Save to Google Sheets (no browser needed - uses service account!)
        save_to_google_sheets(date_obj, results)

        print("\nExtraction complete!")
        print("\nPress Enter to close browser...")
        input()

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

    finally:
        manager.close()


if __name__ == "__main__":
    main()
