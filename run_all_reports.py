"""
Parent Runner - Execute all report extractions in one go
Runs both daily report and order type report extractions sequentially

This script runs both reports one after another to avoid browser conflicts.
Each report uses the same browser profile, so login is only needed once.
"""

from datetime import datetime, timedelta
import sys
import os


def get_date_input():
    """Get date from user or use previous day."""
    print("\n" + "="*80)
    print("RUN ALL REPORTS - PARENT RUNNER".center(80))
    print("="*80)
    print("\nThis will run BOTH report extractions:")
    print("  1. Daily Report (Overall, Facebook, Google Ads)")
    print("  2. Order Type Report (Order Types, Marketing Spend, Klaviyo)")
    print("\n" + "="*80)

    print("\nEnter date to extract (DD-MMM-YYYY, e.g., 13-Oct-2025)")
    print("Or press Enter for previous day: ", end='')

    date_input = input().strip()

    if not date_input:
        # Use previous day
        date_obj = datetime.now() - timedelta(days=1)
        date_str = date_obj.strftime('%d-%b-%Y')
        print(f"\nUsing previous day: {date_str}")
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


def run_daily_report(date_obj, date_str):
    """Run the daily report extraction."""
    print("\n\n")
    print("█" * 80)
    print("REPORT 1 OF 2: DAILY REPORT".center(80))
    print("█" * 80 + "\n")

    try:
        # Import the daily report modules
        from browser_manager import BrowserManager
        from looker_data_extractor import LookerDataExtractor
        from services.sheets.helpers import write_marketing_data
        from dotenv import load_dotenv
        import time

        load_dotenv()

        REPORT_URL = "https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/M05qB"

        print("[1/9] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        print("[2/9] Navigating to report...")
        driver.get(REPORT_URL)
        time.sleep(5)

        # Check if login needed
        if "accounts.google.com" in driver.current_url:
            print("\n⚠️  Please login in the browser window...")
            while "accounts.google.com" in driver.current_url:
                time.sleep(2)
            print("✓ Login successful")

        extractor = LookerDataExtractor(driver)

        # Extract data
        print(f"[3/9] Setting date range...")
        extractor.set_date_range(date_obj, date_obj)

        print(f"[4/9] Extracting overall metrics...")
        overall = extractor.extract_metrics()

        print(f"[6/9] Filtering to Facebook...")
        extractor.select_medium('Facebook')
        print(f"[7/9] Extracting Facebook metrics...")
        facebook = extractor.extract_metrics()

        print(f"[8/9] Filtering to Google Ads...")
        extractor.select_medium('Google Ads')
        print(f"[9/9] Extracting Google Ads metrics...")
        google_ads = extractor.extract_metrics()

        # Display results
        print("\n" + "="*80)
        print(f"DAILY REPORT DATA FOR {date_str}".center(80))
        print("="*80)

        for source, metrics in [('Overall', overall), ('Facebook', facebook), ('Google Ads', google_ads)]:
            print(f"\n{source.upper()}")
            print("-" * 40)
            print(f"Conversions:      {metrics.get('Conversions', 0):>15,.0f}")
            print(f"Spend:            €{metrics.get('Spend', 0):>14,.2f}")
            print(f"ROAS:             {metrics.get('ROAS', 0):>15.2f}")

        # Save to Google Sheets
        print("\n[10/10] Saving to Google Sheets...")
        write_marketing_data(date_obj, overall, facebook, google_ads)
        print("  ✓ Successfully saved to Google Sheets!")

        # Close browser
        manager.close()

        print("\n✓ Daily Report completed successfully!")
        return True

    except Exception as e:
        print(f"\n✗ Daily Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_order_type_report(date_obj, date_str):
    """Run the order type report extraction."""
    print("\n\n")
    print("█" * 80)
    print("REPORT 2 OF 2: ORDER TYPE REPORT".center(80))
    print("█" * 80 + "\n")

    try:
        # Import the order type report modules
        from browser_manager import BrowserManager
        from selenium.webdriver.common.by import By
        from datetime import datetime
        from services.sheets.helpers import write_order_type_data
        from dotenv import load_dotenv
        import time

        # Import the OrderTypeDataExtractor and helper functions
        # We need to load them from the script
        import extract_order_type_report as order_script

        load_dotenv()

        ORDER_TYPE_URL = "https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/h0vQC"
        MARKETING_URL = "https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/M05qB"

        print("[1/10] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        print("[2/10] Navigating to Order Type report...")
        driver.get(ORDER_TYPE_URL)
        time.sleep(5)

        # Check if login needed
        if "accounts.google.com" in driver.current_url:
            print("\n⚠️  Please login in the browser window...")
            while "accounts.google.com" in driver.current_url:
                time.sleep(2)
            print("✓ Login successful")

        extractor = order_script.OrderTypeDataExtractor(driver)

        # Extract Order Type data
        print("[3/10] Setting date range...")
        extractor.set_date_range(date_obj, date_obj)

        print("[4/10] Extracting Order Type data...")
        order_type_metrics = extractor.extract_order_type_metrics()

        # Navigate to Marketing page
        print("\n[5/10] Navigating to Marketing Deepdive page...")
        driver.get(MARKETING_URL)
        time.sleep(5)

        print("[6/10] Setting date range...")
        extractor.set_date_range(date_obj, date_obj)

        print("[7/10] Extracting Marketing Spend...")
        marketing_spend = order_script.extract_marketing_spend(extractor, date_obj)

        # Navigate to Converge
        date_str_url = date_obj.strftime('%Y-%m-%d')
        CONVERGE_URL = f"https://app.runconverge.com/smyle-7267/attribution/channels#since={date_str_url}&until={date_str_url}"

        print(f"\n[8/10] Navigating to Converge Attribution page...")
        driver.get(CONVERGE_URL)
        time.sleep(5)

        # Check if login needed for Converge
        if "login" in driver.current_url.lower() or "sign" in driver.current_url.lower():
            print("\n⚠️  Please login to Converge in the browser window...")
            while "login" in driver.current_url.lower() or "sign" in driver.current_url.lower():
                time.sleep(2)
            print("✓ Login successful")

        # Reload page twice
        print("[9/10] Loading page first time...")
        driver.get(CONVERGE_URL)
        time.sleep(5)

        print("Reloading page to get fresh data...")
        driver.get(CONVERGE_URL)
        time.sleep(8)

        print("[10/10] Extracting Klaviyo metrics...")
        klaviyo_metrics = order_script.extract_klaviyo_metrics(driver, extractor.parse_number)

        # Display results
        print("\n" + "="*80)
        print(f"ORDER TYPE REPORT DATA FOR {date_str}".center(80))
        print("="*80)
        print(f"\nOrder Types: {len(order_type_metrics)} extracted")
        print(f"Marketing Spend: €{marketing_spend:,.2f}")
        print(f"Klaviyo Purchases: {klaviyo_metrics.get('purchases', 0)}")

        # Save to Google Sheets
        print("\nSaving to Google Sheets...")
        write_order_type_data(date_obj, order_type_metrics, marketing_spend, klaviyo_metrics)

        # Close browser
        manager.close()

        print("\n✓ Order Type Report completed successfully!")
        return True

    except Exception as e:
        print(f"\n✗ Order Type Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main execution - run all reports."""

    # Get date once for both scripts
    date_obj, date_str = get_date_input()

    print("\n" + "="*80)
    print("STARTING AUTOMATED EXTRACTION".center(80))
    print("="*80)
    print(f"\nDate: {date_str}")
    print("Both reports will run sequentially to avoid browser conflicts.")
    print("Each browser session will close before the next one starts.")
    print("\n" + "="*80)

    # Track results
    results = {
        'Daily Report': False,
        'Order Type Report': False
    }

    # Run Report 1: Daily Report
    results['Daily Report'] = run_daily_report(date_obj, date_str)

    # Run Report 2: Order Type Report
    results['Order Type Report'] = run_order_type_report(date_obj, date_str)

    # Summary
    print("\n\n")
    print("="*80)
    print("EXECUTION SUMMARY".center(80))
    print("="*80)
    print(f"\nDate Extracted: {date_str}\n")

    for report_name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"  {report_name:<30} {status}")

    print("\n" + "="*80)

    all_success = all(results.values())
    if all_success:
        print("\n🎉 All reports completed successfully!")
        print("Data has been saved to Google Sheets.")
    else:
        print("\n⚠️  Some reports failed. Please check the output above.")

    print("\nPress Enter to exit...")
    input()


if __name__ == "__main__":
    main()
