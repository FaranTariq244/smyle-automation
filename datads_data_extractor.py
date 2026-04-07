"""
DataAds Data Extractor
Extracts creative reporting data from DataAds (app.datads.io)
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime


class DataAdsDataExtractor:
    """Extract creative reporting data from DataAds."""

    # Base report URL (without date params)
    BASE_URL = "https://app.datads.io/creative-reporting/detail/c9f95694-cfd5-4255-8414-501e5fb11369"

    # Grid metrics to request in URL
    GRID_METRICS = [
        "cpm", "landing_page_view", "cost_per_landing_page_view",
        "cpc", "ctr", "add_to_cart_per_clicks", "add_to_cart",
        "initiate_checkout", "purchase_per_add_to_cart", "purchases",
        "purchase_per_clicks", "purchase_roas", "cost_per_purchase",
        "aov", "spend"
    ]

    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    def build_report_url(self, date_obj, end_date_obj=None):
        """
        Build the full report URL with date parameters.

        Args:
            date_obj: datetime object for the start date
            end_date_obj: optional datetime for end date (defaults to same as start)
        Returns:
            str: Full URL with query params
        """
        # Format date as YYYY-M-D (no zero padding, matching the original URL format)
        date_str = f"{date_obj.year}-{date_obj.month}-{date_obj.day}"
        if end_date_obj is None:
            end_date_obj = date_obj

        # Build gridMetrics JSON array
        import urllib.parse
        metrics_json = "[" + ",".join(f'"{m}"' for m in self.GRID_METRICS) + "]"

        end_date_str = f"{end_date_obj.year}-{end_date_obj.month}-{end_date_obj.day}"

        params = {
            "pageSize": "80",
            "pageNumber": "1",
            "tablePageNumber": "1",
            "primaryMetric": "conversion_rate",
            "sort": "DESC",
            "attributionWindow": "DEFAULT",
            "sortBy": "PRIMARY_METRIC",
            "timeframe": "",
            "startDate": date_str,
            "endDate": end_date_str,
            "viewType": "grid",
            "gridMetrics": metrics_json,
        }

        query = "&".join(f"{k}={urllib.parse.quote(str(v), safe='[]\",')}" for k, v in params.items())
        return f"{self.BASE_URL}?{query}"

    def navigate_to_report(self, date_obj, end_date_obj=None):
        """Navigate to the DataAds report page for the given date (range)."""
        url = self.build_report_url(date_obj, end_date_obj)
        print(f"[NAV] Navigating to DataAds report...")
        print(f"[NAV] URL: {url[:120]}...")
        self.driver.get(url)
        time.sleep(5)

    def check_and_wait_for_login(self):
        """Check if login is required and wait for user to login."""
        current_url = self.driver.current_url.lower()

        if "login" in current_url or "sign" in current_url or "auth" in current_url:
            print("\n" + "=" * 60)
            print("LOGIN REQUIRED")
            print("=" * 60)
            print("Please login to DataAds in the browser window...")
            print("=" * 60 + "\n")

            max_wait = 300  # 5 minutes
            start_time = time.time()

            while time.time() - start_time < max_wait:
                current_url = self.driver.current_url.lower()
                if "login" not in current_url and "sign" not in current_url and "auth" not in current_url:
                    print("Login successful!")
                    time.sleep(3)
                    return True
                time.sleep(2)

            print("Login timeout!")
            return False

        return True

    def wait_for_data_load(self, timeout=30):
        """Wait for the report data to finish loading."""
        print("[WAIT] Waiting for data to load...")
        start = time.time()

        # Wait for common loading indicators to disappear
        loading_selectors = [
            ".ant-spin",
            ".loading",
            "[class*='spinner']",
            "[class*='Spinner']",
            "[class*='loading']",
            "[class*='Loading']",
            "[class*='skeleton']",
            "[class*='Skeleton']",
        ]

        # First wait a bit for loading to start
        time.sleep(3)

        # Then wait for loading indicators to disappear
        while time.time() - start < timeout:
            loading_found = False
            for selector in loading_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for el in elements:
                        if el.is_displayed():
                            loading_found = True
                            break
                except Exception:
                    pass
                if loading_found:
                    break

            # Also check for "Loading" text
            try:
                loading_texts = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Loading')]")
                for el in loading_texts:
                    if el.is_displayed():
                        loading_found = True
                        break
            except Exception:
                pass

            if not loading_found:
                elapsed = time.time() - start
                print(f"[WAIT] Data loaded in {elapsed:.1f}s")
                return True

            time.sleep(1)

        print(f"[WAIT] Timeout after {timeout}s - proceeding anyway")
        return False

    def extract_table_data(self):
        """
        Extract data from the DataAds report page using card-based extraction.
        DataAds uses a grid of cards, each representing a landing page with metrics.

        Returns:
            list[dict]: List of row dictionaries with "Landing page" + metric names as keys
        """
        print("[EXTRACT] Starting card-based data extraction...")

        # DataAds renders data as cards under: .col-span-1.relative
        cards = self.driver.find_elements(By.CSS_SELECTOR, ".col-span-1.relative")

        if not cards:
            print("[EXTRACT] No cards found with .col-span-1.relative selector")
            # Try broader search
            cards = self.driver.find_elements(By.CSS_SELECTOR, "[data-cy='creative-group-card']")
            if not cards:
                print("[EXTRACT] No cards found at all. Page may not have loaded.")
                return []

        print(f"[EXTRACT] Found {len(cards)} cards")

        data = []
        for card_idx, card in enumerate(cards):
            try:
                row_data = self._extract_card_metrics(card, card_idx)
                if row_data:
                    data.append(row_data)
            except Exception as e:
                print(f"[EXTRACT] Error extracting card {card_idx}: {e}")

        print(f"[EXTRACT] Successfully extracted {len(data)} rows")
        return data

    def _extract_card_metrics(self, card, card_idx):
        """
        Extract landing page URL and all metrics from a single DataAds card.

        Card structure:
        - Landing page URL in <h5> or <p> element
        - Metric rows as divs with class "flex items-center flex items-end my-2"
          - Label: span with class containing "w-[40%]"
          - Value: span with class "text-xs" inside div with class containing "whitespace-nowrap"

        Returns:
            dict: Row data with "Landing page" key + metric keys
        """
        row_data = {}

        # Extract landing page URL from h5 or p element
        landing_page = ""
        try:
            h5 = card.find_element(By.CSS_SELECTOR, "h5.text-sm.font-medium")
            landing_page = h5.text.strip()
        except Exception:
            pass

        if not landing_page:
            try:
                p = card.find_element(By.CSS_SELECTOR, "p.whitespace-pre-wrap")
                landing_page = p.text.strip()
            except Exception:
                pass

        if not landing_page:
            print(f"[CARD {card_idx}] No landing page URL found, skipping")
            return None

        row_data["Landing page"] = landing_page
        print(f"[CARD {card_idx}] Landing page: {landing_page}")

        # Extract metrics from metric rows
        # Each metric row is: div.flex.items-center containing label span + value span
        metric_rows = card.find_elements(By.CSS_SELECTOR, "div.my-2")

        for metric_row in metric_rows:
            try:
                # Get label - span with w-[40%] class (the metric name)
                label_el = metric_row.find_elements(By.CSS_SELECTOR, "span[class*='w-']")
                if not label_el:
                    continue
                label = label_el[0].text.strip()
                if not label:
                    continue

                # Get value - span.text-xs inside the whitespace-nowrap div
                value = ""
                value_container = metric_row.find_elements(By.CSS_SELECTOR, "div[class*='whitespace-nowrap'] > span.text-xs")
                if value_container:
                    value = value_container[0].text.strip()
                else:
                    # Fallback: get any span.text-xs that's not the label
                    all_value_spans = metric_row.find_elements(By.CSS_SELECTOR, "span.text-xs")
                    for vs in all_value_spans:
                        vs_text = vs.text.strip()
                        if vs_text and vs_text != label:
                            value = vs_text
                            break

                if label and value:
                    row_data[label] = value

            except Exception:
                continue

        metric_count = len(row_data) - 1  # Exclude "Landing page"
        print(f"[CARD {card_idx}] Extracted {metric_count} metrics")

        return row_data if metric_count > 0 else None

    def display_data(self, data):
        """Display extracted data in a readable format."""
        if not data:
            print("\nNo data to display.")
            return

        print("\n" + "=" * 100)
        print(f"EXTRACTED DATA ({len(data)} rows)")
        print("=" * 100)

        for i, row in enumerate(data):
            print(f"\n--- Row {i + 1} ---")
            for key, value in row.items():
                print(f"  {key}: {value}")

        print("\n" + "=" * 100)
        print(f"Total rows: {len(data)}")
        print("=" * 100)


def run_datads_report(date_obj, date_str):
    """
    Run the DataAds report extraction.

    Args:
        date_obj: datetime object for the date to extract
        date_str: String representation of the date

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n\n")
    print("=" * 80)
    print("DATADS REPORT".center(80))
    print("=" * 80 + "\n")

    try:
        from browser_manager import BrowserManager
        import time

        print("[1/5] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        extractor = DataAdsDataExtractor(driver)

        # Navigate to report with date in URL
        print("[2/5] Navigating to DataAds report...")
        extractor.navigate_to_report(date_obj)

        # Check if login is needed - restart browser in visible mode if so
        current_url = driver.current_url.lower()
        if "login" in current_url or "sign" in current_url or "auth" in current_url:
            print("\n  DataAds login required - restarting browser in visible mode...")
            manager.close()
            time.sleep(2)
            manager = BrowserManager(use_existing_chrome=False)
            driver = manager.start_browser(headless=False)
            extractor = DataAdsDataExtractor(driver)
            extractor.navigate_to_report(date_obj)
            time.sleep(3)
            if not extractor.check_and_wait_for_login():
                print("Login failed or timed out")
                manager.close()
                return False
            # After login, navigate back to report with correct date
            extractor.navigate_to_report(date_obj)
        elif not extractor.check_and_wait_for_login():
            print("Login failed or timed out")
            manager.close()
            return False

        print("[3/5] Waiting for data to load...")
        extractor.wait_for_data_load()

        # Give extra time for dynamic content to render
        time.sleep(3)

        print("[4/5] Extracting data...")
        data = extractor.extract_table_data()

        # Display data on console for verification
        print("\n*** DATADS REPORT DATA ***")
        extractor.display_data(data)

        # Summary
        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"DataAds Report: {len(data)} rows extracted")
        print("=" * 80)

        # Close browser
        print("[5/5] Closing browser...")
        manager.close()

        # Write to Google Sheets
        print("\n[6/6] Writing to Google Sheets...")
        try:
            from services.sheets.datads_helpers import write_datads_data_to_sheets
            write_datads_data_to_sheets(date_obj, data)
            print("  Successfully wrote to Google Sheets!")
        except Exception as e:
            print(f"  Warning: Could not write to Google Sheets: {e}")
            print("  (Data was extracted successfully, but sheet update failed)")
            import traceback
            traceback.print_exc()

        print("\n" + "=" * 80)
        print("DATADS REPORT COMPLETED SUCCESSFULLY")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\nDataAds Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_datads_weekly_report(start_date_obj, end_date_obj, start_date_str, end_date_str):
    """
    Run the DataAds weekly report extraction with a date range.

    Args:
        start_date_obj: datetime for start of week
        end_date_obj: datetime for end of week
        start_date_str: String representation of start date
        end_date_str: String representation of end date

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n\n")
    print("=" * 80)
    print("DATADS WEEKLY REPORT".center(80))
    print(f"Date range: {start_date_str} to {end_date_str}".center(80))
    print("=" * 80 + "\n")

    try:
        from browser_manager import BrowserManager
        import time

        print("[1/5] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        extractor = DataAdsDataExtractor(driver)

        # Navigate to report with date range in URL
        print("[2/5] Navigating to DataAds report (weekly)...")
        extractor.navigate_to_report(start_date_obj, end_date_obj)

        # Check if login is needed
        current_url = driver.current_url.lower()
        if "login" in current_url or "sign" in current_url or "auth" in current_url:
            print("\n  DataAds login required - restarting browser in visible mode...")
            manager.close()
            time.sleep(2)
            manager = BrowserManager(use_existing_chrome=False)
            driver = manager.start_browser(headless=False)
            extractor = DataAdsDataExtractor(driver)
            extractor.navigate_to_report(start_date_obj, end_date_obj)
            time.sleep(3)
            if not extractor.check_and_wait_for_login():
                print("Login failed or timed out")
                manager.close()
                return False
            extractor.navigate_to_report(start_date_obj, end_date_obj)
        elif not extractor.check_and_wait_for_login():
            print("Login failed or timed out")
            manager.close()
            return False

        print("[3/5] Waiting for data to load...")
        extractor.wait_for_data_load()
        time.sleep(3)

        print("[4/5] Extracting data...")
        data = extractor.extract_table_data()

        print("\n*** DATADS WEEKLY REPORT DATA ***")
        extractor.display_data(data)

        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"DataAds Weekly Report: {len(data)} rows extracted")
        print("=" * 80)

        print("[5/5] Closing browser...")
        manager.close()

        # Write to Google Sheets (weekly mode)
        print("\n[6/6] Writing weekly data to Google Sheets...")
        try:
            from services.sheets.datads_helpers import write_datads_weekly_data_to_sheets
            write_datads_weekly_data_to_sheets(start_date_obj, end_date_obj, data)
            print("  Successfully wrote weekly data to Google Sheets!")
        except Exception as e:
            print(f"  Warning: Could not write to Google Sheets: {e}")
            print("  (Data was extracted successfully, but sheet update failed)")
            import traceback
            traceback.print_exc()

        print("\n" + "=" * 80)
        print("DATADS WEEKLY REPORT COMPLETED SUCCESSFULLY")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\nDataAds Weekly Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    """Test the extractor standalone."""
    from datetime import datetime, timedelta

    # Use previous day by default
    date_obj = datetime.now() - timedelta(days=1)
    date_str = date_obj.strftime('%d-%b-%Y')

    print(f"Testing DataAds Report for {date_str}")

    success = run_datads_report(date_obj, date_str)

    if success:
        print("\nTest completed successfully!")
    else:
        print("\nTest failed!")

    input("\nPress Enter to exit...")
