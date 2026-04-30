"""
DataAds Data Extractor
Extracts creative reporting data from DataAds (app.datads.io)
Uses UI-based date selection (date picker) instead of URL parameters.
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
from datetime import datetime


class DataAdsDataExtractor:
    """Extract creative reporting data from DataAds."""

    # Base report URL (no date/query params — dates are selected via UI)
    BASE_URL = "https://app.datads.io/creative-reporting/detail/c9f95694-cfd5-4255-8414-501e5fb11369"

    PRESET_NAME = "FaranDaily"

    # Month abbreviations used in the date picker button text (e.g. "Apr 09, 26")
    MONTH_ABBR = [
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ]
    # Full month names used in the calendar header (e.g. "April 2026")
    MONTH_FULL = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def navigate_to_report(self, date_obj, end_date_obj=None):
        """
        Navigate to the DataAds report page and select the date via UI.

        Args:
            date_obj: datetime for the start (or only) date
            end_date_obj: optional datetime for the end date (defaults to same as start)
        """
        if end_date_obj is None:
            end_date_obj = date_obj

        print(f"[NAV] Navigating to DataAds report page...")
        print(f"[NAV] URL: {self.BASE_URL}")
        self.driver.get(self.BASE_URL)
        time.sleep(5)

    def setup_report(self, date_obj, end_date_obj=None):
        """
        After navigation and login, ensure the preset is selected and pick dates.
        Call this AFTER check_and_wait_for_login succeeds.

        Returns True if dates were successfully selected.
        """
        if end_date_obj is None:
            end_date_obj = date_obj

        # 1. Ensure correct preset is selected
        self.ensure_preset_selected()

        # 2. Select dates via date picker (with retry)
        if not self.select_date_range(date_obj, end_date_obj):
            return False

        # 3. Set page size to 80 so all results are visible
        self.set_page_size(80)

        return True

    # ------------------------------------------------------------------
    # Preset handling
    # ------------------------------------------------------------------

    def ensure_preset_selected(self):
        """Check that the 'FaranDaily' preset is selected; select it if not."""
        print(f"[PRESET] Checking if preset '{self.PRESET_NAME}' is selected...")
        time.sleep(2)

        try:
            # Look for preset button/dropdown — it shows "Preset FaranDaily" when selected
            preset_buttons = self.driver.find_elements(
                By.XPATH,
                "//*[contains(text(), 'Preset')]"
            )

            preset_selected = False
            for btn in preset_buttons:
                if self.PRESET_NAME in btn.text:
                    print(f"[PRESET] '{self.PRESET_NAME}' is already selected.")
                    preset_selected = True
                    break

            if not preset_selected:
                print(f"[PRESET] '{self.PRESET_NAME}' not selected — attempting to select it...")
                self._select_preset()

        except Exception as e:
            print(f"[PRESET] Warning: Could not verify preset: {e}")

    def _select_preset(self):
        """Open the preset dropdown and select FaranDaily."""
        try:
            # Click the Preset dropdown button
            preset_btn = self.driver.find_element(
                By.XPATH,
                "//*[contains(text(), 'Preset')]"
            )
            preset_btn.click()
            time.sleep(2)

            # Look for FaranDaily option in the dropdown
            option = self.driver.find_element(
                By.XPATH,
                f"//*[contains(text(), '{self.PRESET_NAME}')]"
            )
            option.click()
            time.sleep(3)
            print(f"[PRESET] Selected '{self.PRESET_NAME}' preset.")
        except Exception as e:
            print(f"[PRESET] Warning: Could not select preset: {e}")

    # ------------------------------------------------------------------
    # Date picker
    # ------------------------------------------------------------------

    def select_date_range(self, start_date, end_date, max_retries=2):
        """
        Select a date range using the UI date picker.
        For a single day, click the same date twice (start = end).
        Retries up to max_retries times if verification fails.

        Returns True if dates were successfully selected and verified.
        """
        # First check if dates are already correct — skip selection if so
        if self._verify_selected_dates(start_date, end_date):
            print(f"[DATE] Dates already set correctly, no change needed.")
            return True

        for attempt in range(1, max_retries + 1):
            print(f"[DATE] Attempt {attempt}/{max_retries}: Selecting date range "
                  f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

            try:
                # Open the date picker
                self._open_date_picker()
                time.sleep(2)

                # Navigate to the correct month and click start date
                self._navigate_and_click_date(start_date)
                time.sleep(1)

                # Click end date (same date for single-day; different for range)
                self._navigate_and_click_date(end_date)
                time.sleep(1)

                # Click Apply
                self._click_apply()
                time.sleep(3)

                # Verify the selected dates
                if self._verify_selected_dates(start_date, end_date):
                    print(f"[DATE] Date selection verified successfully!")
                    return True
                else:
                    print(f"[DATE] Date verification failed on attempt {attempt}")

            except Exception as e:
                print(f"[DATE] Error on attempt {attempt}: {e}")
                import traceback
                traceback.print_exc()
                # Close date picker if still open
                try:
                    cancel_btns = self.driver.find_elements(By.XPATH, "//button[normalize-space()='Cancel']")
                    if not cancel_btns:
                        cancel_btns = self.driver.find_elements(By.XPATH, "//button[.//text()[contains(., 'Cancel')]]")
                    for btn in cancel_btns:
                        if btn.is_displayed():
                            btn.click()
                            time.sleep(1)
                            break
                except Exception:
                    pass
                # Also try pressing Escape to close any popup
                try:
                    from selenium.webdriver.common.keys import Keys
                    self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                    time.sleep(1)
                except Exception:
                    pass

        print(f"[DATE] FAILED to select correct dates after {max_retries} attempts")
        return False

    def _find_date_picker_button(self):
        """Find the date picker button element on the page."""
        # Strategy 1: find a button containing date range text like "Apr 09, 26 - Apr 09, 26"
        all_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button")
        for btn in all_buttons:
            try:
                text = btn.text.strip()
                if " - " in text and any(m in text for m in self.MONTH_ABBR[1:]):
                    return btn
            except Exception:
                continue

        # Strategy 2: find any visible element with date range text, then get its closest button ancestor
        month_xpath = " or ".join(f"contains(., '{m}')" for m in self.MONTH_ABBR[1:])
        candidates = self.driver.find_elements(
            By.XPATH,
            f"//*[contains(., ' - ') and ({month_xpath})]"
        )
        for c in candidates:
            if c.is_displayed() and len(c.text.strip()) < 60:
                # Try to find a clickable parent button
                try:
                    btn = c.find_element(By.XPATH, "ancestor-or-self::button")
                    return btn
                except Exception:
                    return c

        return None

    def _is_calendar_open(self):
        """Check if the date picker calendar popup is currently open."""
        # Look for the Apply/Cancel buttons or calendar grid which only appear when open
        indicators = [
            "//button[normalize-space()='Apply']",
            "//button[normalize-space()='Cancel']",
            "//table[contains(@class, 'rdp') or @role='grid']",
        ]
        for xpath in indicators:
            try:
                els = self.driver.find_elements(By.XPATH, xpath)
                for el in els:
                    if el.is_displayed():
                        return True
            except Exception:
                pass
        # Also check for month name headers in a calendar context
        for m_name in self.MONTH_FULL[1:]:
            try:
                headers = self.driver.find_elements(
                    By.XPATH,
                    f"//*[text()='{m_name} 2025' or text()='{m_name} 2026' or text()='{m_name} 2027']"
                )
                for h in headers:
                    if h.is_displayed():
                        return True
            except Exception:
                pass
        return False

    def _open_date_picker(self):
        """Click the date picker button to open the calendar popup."""
        print("[DATE] Opening date picker...")

        # If calendar is already open, no need to click
        if self._is_calendar_open():
            print("[DATE] Calendar is already open.")
            return

        date_btn = self._find_date_picker_button()

        if not date_btn:
            raise Exception("Could not find date picker button")

        btn_text = date_btn.text.strip()
        print(f"[DATE] Found date picker button: '{btn_text}'")
        date_btn.click()
        time.sleep(2)

        # Verify the calendar actually opened
        if not self._is_calendar_open():
            print("[DATE] Calendar did not open on first click, trying again...")
            # Sometimes the text span captures the click but not the button — try JS click
            try:
                self.driver.execute_script("arguments[0].click();", date_btn)
                time.sleep(2)
            except Exception:
                pass

            if not self._is_calendar_open():
                raise Exception("Date picker calendar did not open after clicking")

    # JavaScript to click a day using <time datetime="YYYY-M-D"> elements.
    # The calendar uses 0-indexed months in datetime attrs (JS-style: 0=Jan, 11=Dec).
    # We also filter by X-position to ensure we click in the correct month panel,
    # not an overflow day from an adjacent month.
    _JS_CLICK_DAY = """
    var year = arguments[0];
    var jsMonth = arguments[1];
    var day = arguments[2];
    var targetMonthHeader = arguments[3];
    var dateAttr = year + '-' + jsMonth + '-' + day;

    var monthHeaders = [];
    document.querySelectorAll('h2').forEach(function(h) {
        var text = h.textContent.trim();
        if (/^[A-Z][a-z]+ \\d{4}$/.test(text) && h.offsetParent !== null) {
            var rect = h.getBoundingClientRect();
            monthHeaders.push({text: text, centerX: rect.x + rect.width / 2});
        }
    });
    monthHeaders.sort(function(a, b) { return a.centerX - b.centerX; });

    var targetIdx = -1;
    for (var i = 0; i < monthHeaders.length; i++) {
        if (monthHeaders[i].text === targetMonthHeader) { targetIdx = i; break; }
    }
    var xMin = 0, xMax = 99999;
    if (targetIdx >= 0) {
        if (targetIdx > 0)
            xMin = (monthHeaders[targetIdx - 1].centerX + monthHeaders[targetIdx].centerX) / 2;
        if (targetIdx + 1 < monthHeaders.length)
            xMax = (monthHeaders[targetIdx].centerX + monthHeaders[targetIdx + 1].centerX) / 2;
    }

    var allMatches = [];
    document.querySelectorAll('time[datetime="' + dateAttr + '"]').forEach(function(t) {
        if (t.offsetParent !== null) {
            var btn = t.closest('button') || t.parentElement;
            var rect = btn.getBoundingClientRect();
            allMatches.push({element: btn, centerX: rect.x + rect.width / 2});
        }
    });

    if (allMatches.length === 0) {
        var allTimes = [];
        document.querySelectorAll('time[datetime]').forEach(function(t) {
            if (t.offsetParent !== null) allTimes.push(t.getAttribute('datetime'));
        });
        return {error: 'No <time> for ' + dateAttr, visibleDates: allTimes.slice(0, 20)};
    }

    var inRange = allMatches.filter(function(m) { return m.centerX >= xMin && m.centerX <= xMax; });
    if (inRange.length === 0 && targetIdx >= 0) {
        var tgtX = monthHeaders[targetIdx].centerX;
        inRange = [allMatches[0]];
        for (var m of allMatches) {
            if (Math.abs(m.centerX - tgtX) < Math.abs(inRange[0].centerX - tgtX)) inRange = [m];
        }
    }
    if (inRange.length === 0) inRange = allMatches;

    inRange[0].element.click();
    return {success: true, clicked: dateAttr, totalMatches: allMatches.length, inRange: inRange.length};
    """

    def _navigate_and_click_date(self, target_date):
        """Navigate the calendar to the correct month and click the target day via JS."""
        target_month_name = self.MONTH_FULL[target_date.month]
        target_year = target_date.year
        target_header = f"{target_month_name} {target_year}"

        print(f"[DATE] Looking for month '{target_header}' to click day {target_date.day}")

        # Navigate to the correct month
        max_nav_clicks = 24
        for i in range(max_nav_clicks):
            visible = self._get_visible_month_headers()
            if target_header in visible:
                break

            if not visible:
                if i == 0 and not self._is_calendar_open():
                    raise Exception("Calendar is not open — cannot navigate months")
                self._click_calendar_nav("next")
                time.sleep(0.5)
                continue

            # Determine direction from first visible header
            first = visible[0]
            match = re.match(r'^(\w+)\s+(\d{4})$', first)
            if match:
                vis_month = self.MONTH_FULL.index(match.group(1))
                vis_year = int(match.group(2))
                direction = "prev" if (target_year, target_date.month) < (vis_year, vis_month) else "next"
            else:
                direction = "next"

            print(f"[DATE] Visible: {visible}, need '{target_header}' — clicking {direction}")
            if not self._click_calendar_nav(direction):
                time.sleep(0.5)
                self._click_calendar_nav(direction)
            time.sleep(1)
        else:
            raise Exception(f"Could not navigate calendar to {target_header}")

        # Click the day using JS with <time datetime> and X-position filtering.
        # The datetime attribute uses 0-indexed months (JS-style), so subtract 1.
        js_month = target_date.month - 1
        result = self.driver.execute_script(
            self._JS_CLICK_DAY, target_year, js_month, target_date.day, target_header
        )

        if result and result.get('success'):
            print(f"[DATE] Clicked day {target_date.day}")
        else:
            raise Exception(f"Could not click day {target_date.day}: {result}")

    def _get_visible_month_headers(self):
        """Get visible month/year header texts from the calendar h2 elements."""
        result = self.driver.execute_script("""
            var headers = [];
            document.querySelectorAll('h2').forEach(function(h) {
                if (h.offsetParent !== null) {
                    var text = h.textContent.trim();
                    if (/^[A-Z][a-z]+ \\d{4}$/.test(text)) {
                        headers.push(text);
                    }
                }
            });
            return headers;
        """)
        return result or []

    def _click_calendar_nav(self, direction):
        """Click the previous or next month navigation arrow. Returns True if clicked."""
        sr_text = "Previous month" if direction == "prev" else "Next month"

        # Use JS for reliability — find button with sr-only span
        clicked = self.driver.execute_script("""
            var srText = arguments[0];
            var btns = document.querySelectorAll('button');
            for (var btn of btns) {
                var span = btn.querySelector('span.sr-only');
                if (span && span.textContent.trim() === srText && btn.offsetParent !== null) {
                    btn.click();
                    return true;
                }
            }
            return false;
        """, sr_text)

        if clicked:
            return True

        # Fallback strategies for other calendar implementations
        is_prev = direction == "prev"
        selectors = [
            f"//button[.//span[contains(@class, 'sr-only') and normalize-space(text())='{sr_text}']]",
            "//button[contains(@class, 'prev')]" if is_prev else "//button[contains(@class, 'next')]",
            f"//button[@aria-label='{sr_text}']",
        ]
        for xpath in selectors:
            try:
                for btn in self.driver.find_elements(By.XPATH, xpath):
                    if btn.is_displayed():
                        btn.click()
                        return True
            except Exception:
                continue

        print(f"[DATE] WARNING: Could not find {direction} navigation button!")
        return False

    def _click_apply(self):
        """Click the Apply button in the date picker."""
        print("[DATE] Clicking Apply...")

        # Try multiple selectors — the button text may be in a child span
        selectors = [
            "//button[normalize-space()='Apply']",
            "//button[.//text()[contains(., 'Apply')]]",
            "//button[contains(@class, 'apply')]",
            "//button[contains(@class, 'primary') or contains(@class, 'confirm')]",
        ]

        for xpath in selectors:
            try:
                btns = self.driver.find_elements(By.XPATH, xpath)
                for btn in btns:
                    if btn.is_displayed():
                        btn.click()
                        print("[DATE] Apply clicked.")
                        return
            except Exception:
                continue

        # Last resort: find all visible buttons and look for one with "Apply" text
        try:
            all_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button")
            for btn in all_buttons:
                if btn.is_displayed() and "apply" in btn.text.strip().lower():
                    btn.click()
                    print("[DATE] Apply clicked (fallback).")
                    return
        except Exception:
            pass

        raise Exception("Could not find/click Apply button — no matching button visible")

    def _verify_selected_dates(self, start_date, end_date):
        """
        Verify the dates shown in the date picker button match what we selected.
        The button text format is like: "Apr 09, 26 - Apr 09, 26" or "Today Apr 12, 26 - Apr 12, 26"
        """
        print("[DATE] Verifying selected dates...")

        # Build expected date strings
        start_str = f"{self.MONTH_ABBR[start_date.month]} {start_date.day:02d}, {start_date.year % 100}"
        end_str = f"{self.MONTH_ABBR[end_date.month]} {end_date.day:02d}, {end_date.year % 100}"

        try:
            # Find the date picker button text
            candidates = self.driver.find_elements(
                By.XPATH,
                "//*[contains(text(), ' - ') and (contains(text(), 'Jan') or contains(text(), 'Feb') "
                "or contains(text(), 'Mar') or contains(text(), 'Apr') or contains(text(), 'May') "
                "or contains(text(), 'Jun') or contains(text(), 'Jul') or contains(text(), 'Aug') "
                "or contains(text(), 'Sep') or contains(text(), 'Oct') or contains(text(), 'Nov') "
                "or contains(text(), 'Dec'))]"
            )

            for c in candidates:
                if c.is_displayed():
                    btn_text = c.text.strip()
                    print(f"[DATE] Date picker shows: '{btn_text}'")

                    if start_str in btn_text and end_str in btn_text:
                        return True
                    else:
                        print(f"[DATE] Expected: '{start_str} - {end_str}' but got: '{btn_text}'")
                        return False

        except Exception as e:
            print(f"[DATE] Warning: Could not verify dates: {e}")

        return False

    # ------------------------------------------------------------------
    # Page size
    # ------------------------------------------------------------------

    def set_page_size(self, target_size=80):
        """
        Scroll down and set the page size dropdown to the target value (e.g. 80).
        The dropdown shows "8 per page" by default and has options like 8, 20, 40, 80.
        """
        print(f"[PAGESIZE] Setting page size to {target_size}...")

        try:
            # Scroll to bottom of page to find the page size dropdown
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Find the page size dropdown — it contains text like "8 per page"
            page_size_btn = None
            candidates = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'per page')]")
            for c in candidates:
                if c.is_displayed():
                    page_size_btn = c
                    break

            if not page_size_btn:
                # Try finding by button with "per page" text
                all_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button")
                for btn in all_buttons:
                    if "per page" in btn.text.lower() and btn.is_displayed():
                        page_size_btn = btn
                        break

            if not page_size_btn:
                print(f"[PAGESIZE] Warning: Could not find page size dropdown")
                return

            current_text = page_size_btn.text.strip()
            print(f"[PAGESIZE] Current page size: '{current_text}'")

            # Check if already set to target
            if str(target_size) in current_text:
                print(f"[PAGESIZE] Already set to {target_size}.")
                return

            # Click to open dropdown
            page_size_btn.click()
            time.sleep(1)

            # Find and click the target option (e.g. "80 per p...")
            target_option = None
            options = self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{target_size}')]")
            for opt in options:
                if opt.is_displayed() and "per p" in opt.text.lower():
                    target_option = opt
                    break

            if not target_option:
                # Try broader: any visible element starting with target number
                options = self.driver.find_elements(By.XPATH, f"//*[starts-with(normalize-space(text()), '{target_size}')]")
                for opt in options:
                    if opt.is_displayed():
                        target_option = opt
                        break

            if target_option:
                target_option.click()
                print(f"[PAGESIZE] Selected {target_size} per page.")
                time.sleep(3)  # Wait for page to reload with more results
            else:
                print(f"[PAGESIZE] Warning: Could not find '{target_size}' option in dropdown")

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

        except Exception as e:
            print(f"[PAGESIZE] Warning: Could not set page size: {e}")

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

        print("[1/6] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        extractor = DataAdsDataExtractor(driver)

        # Navigate to report page (no date in URL — will use date picker)
        print("[2/6] Navigating to DataAds report page...")
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
            # After login, navigate back to report page
            extractor.navigate_to_report(date_obj)
        elif not extractor.check_and_wait_for_login():
            print("Login failed or timed out")
            manager.close()
            return False

        # Setup: verify preset and select date via UI
        print("[3/6] Setting up report (preset + date selection)...")
        if not extractor.setup_report(date_obj):
            print("Failed to select correct date on DataAds")
            manager.close()
            return False

        print("[4/6] Waiting for data to load...")
        extractor.wait_for_data_load()

        # Give extra time for dynamic content to render
        time.sleep(3)

        print("[5/6] Extracting data...")
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
        print("[6/6] Closing browser...")
        manager.close()

        # Write to Google Sheets
        print("\n[SHEETS] Writing to Google Sheets...")
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

        print("[1/6] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        extractor = DataAdsDataExtractor(driver)

        # Navigate to report page (no date in URL — will use date picker)
        print("[2/6] Navigating to DataAds report page (weekly)...")
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

        # Setup: verify preset and select date range via UI
        print("[3/6] Setting up report (preset + date selection)...")
        if not extractor.setup_report(start_date_obj, end_date_obj):
            print("Failed to select correct date range on DataAds")
            manager.close()
            return False

        print("[4/6] Waiting for data to load...")
        extractor.wait_for_data_load()
        time.sleep(3)

        print("[5/6] Extracting data...")
        data = extractor.extract_table_data()

        print("\n*** DATADS WEEKLY REPORT DATA ***")
        extractor.display_data(data)

        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"DataAds Weekly Report: {len(data)} rows extracted")
        print("=" * 80)

        print("[6/6] Closing browser...")
        manager.close()

        # Write to Google Sheets (weekly mode)
        print("\n[SHEETS] Writing weekly data to Google Sheets...")
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
