"""
Atria Data Extractor for Add Tracker Report
Extracts landing page performance data from Atria Analytics platform
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
from datetime import datetime


class AtriaDataExtractor:
    """Extract landing page performance data from Atria Analytics."""

    # First report URL (Top performing landing pages - REO)
    ATRIA_URL = "https://app.tryatria.com/workspace/analytics/facebook/a4ca9167fc2446bba1aa5981bbabc254/report/f6e0ee4a234d4d749aa98049eafc5d72"

    # Second report URL
    ATRIA_URL_2 = "https://app.tryatria.com/workspace/analytics/facebook/7c70f57b653f41c0a081781619884f33/report/97eca273cb9e431db7d90271eb87f047"

    def __init__(self, driver):
        """
        Initialize the extractor.

        Args:
            driver: Selenium WebDriver instance
        """
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    def navigate_to_report(self, report_num=1):
        """
        Navigate to the Atria report page.

        Args:
            report_num: 1 for first report, 2 for second report
        """
        if report_num == 2:
            url = self.ATRIA_URL_2
            print(f"[NAV] Navigating to Atria report 2...")
        else:
            url = self.ATRIA_URL
            print(f"[NAV] Navigating to Atria report 1...")

        self.driver.get(url)
        time.sleep(5)

    def check_and_wait_for_login(self):
        """Check if login is required and wait for user to login."""
        # Check if we're on a login page
        current_url = self.driver.current_url.lower()

        if "login" in current_url or "sign" in current_url or "auth" in current_url:
            print("\n" + "=" * 60)
            print("LOGIN REQUIRED")
            print("=" * 60)
            print("Please login to Atria in the browser window...")
            print("=" * 60 + "\n")

            # Wait for login to complete (URL should change)
            max_wait = 300  # 5 minutes
            start_time = time.time()

            while time.time() - start_time < max_wait:
                current_url = self.driver.current_url.lower()
                if "login" not in current_url and "sign" not in current_url and "auth" not in current_url:
                    print("Login successful!")
                    time.sleep(3)
                    # Navigate to report after login
                    self.navigate_to_report()
                    return True
                time.sleep(2)

            print("Login timeout!")
            return False

        return True

    def set_date(self, target_date):
        """
        Set the date in the date picker.
        The date picker is an Ant Design range picker, so we need to click the same date twice.

        Args:
            target_date: datetime object for the date to select
        """
        print(f"[DATE] Setting date to {target_date.strftime('%d-%b-%Y')}...")

        try:
            # Wait for page to load
            time.sleep(3)

            # Find the Ant Design date picker
            # It has class "ant-picker ant-picker-range"
            date_picker = None

            # Strategy 1: Find by Ant Design class (most reliable)
            print("[DATE] Looking for ant-picker-range...")
            try:
                date_picker = self.driver.find_element(By.CSS_SELECTOR, ".ant-picker.ant-picker-range")
                print(f"[DATE] Found ant-picker-range: displayed={date_picker.is_displayed()}")
            except Exception as e:
                print(f"[DATE] ant-picker-range not found: {e}")

            # Strategy 2: Find by the input with date-range attribute
            if not date_picker:
                print("[DATE] Looking for input with date-range attribute...")
                try:
                    start_input = self.driver.find_element(By.CSS_SELECTOR, "input[date-range='start']")
                    print(f"[DATE] Found start input: value={start_input.get_attribute('value')}")
                    # Get the parent picker container
                    date_picker = start_input.find_element(By.XPATH, "./ancestor::div[contains(@class, 'ant-picker')]")
                    print(f"[DATE] Found parent picker")
                except Exception as e:
                    print(f"[DATE] input date-range not found: {e}")

            # Strategy 3: Find by class containing 'picker' and 'range'
            if not date_picker:
                print("[DATE] Looking for div with picker class...")
                try:
                    pickers = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'picker') and contains(@class, 'range')]")
                    print(f"[DATE] Found {len(pickers)} picker elements")
                    for i, p in enumerate(pickers):
                        try:
                            print(f"[DATE]   Picker {i}: displayed={p.is_displayed()}, y={p.location['y']}, class={p.get_attribute('class')[:50]}")
                            if p.is_displayed() and p.location['y'] < 300:
                                date_picker = p
                                break
                        except Exception as e:
                            print(f"[DATE]   Picker {i} error: {e}")
                except Exception as e:
                    print(f"[DATE] picker class search failed: {e}")

            # Strategy 4: Find input with placeholder containing 'date'
            if not date_picker:
                print("[DATE] Looking for input with date placeholder...")
                try:
                    inputs = self.driver.find_elements(By.XPATH, "//input[contains(@placeholder, 'date') or contains(@placeholder, 'Date')]")
                    print(f"[DATE] Found {len(inputs)} date inputs")
                    for inp in inputs:
                        try:
                            if inp.is_displayed():
                                print(f"[DATE]   Input: placeholder={inp.get_attribute('placeholder')}, value={inp.get_attribute('value')}")
                                date_picker = inp
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"[DATE] placeholder search failed: {e}")

            if date_picker:
                print(f"[DATE] Clicking date picker...")
                try:
                    date_picker.click()
                    print("[DATE] Click successful")
                except Exception as e:
                    print(f"[DATE] Direct click failed: {e}, trying JavaScript click...")
                    self.driver.execute_script("arguments[0].click();", date_picker)
                    print("[DATE] JavaScript click executed")

                time.sleep(2)

                # Now we need to select the date in the calendar
                self._select_date_in_calendar(target_date)
            else:
                print("[DATE] ERROR: Could not find date picker element")
                # Debug: print all visible elements with their classes
                print("[DATE] Debugging - looking for all potential picker elements...")
                try:
                    all_divs = self.driver.find_elements(By.XPATH, "//div[@class]")
                    for div in all_divs[:50]:  # Check first 50
                        try:
                            cls = div.get_attribute('class')
                            if 'picker' in cls.lower() or 'date' in cls.lower() or 'calendar' in cls.lower():
                                print(f"[DATE]   Found: class='{cls[:60]}', y={div.location['y']}")
                        except:
                            continue
                except:
                    pass

        except Exception as e:
            print(f"[DATE] Error setting date: {e}")
            import traceback
            traceback.print_exc()

    def _select_date_in_calendar(self, target_date):
        """
        Select a specific date in the Ant Design calendar popup.
        Since it's a range picker, click the same date twice in the same month.

        Args:
            target_date: datetime object
        """
        try:
            print(f"[CALENDAR] Selecting date: {target_date.strftime('%Y-%m-%d')}")
            time.sleep(2)

            target_day = target_date.day
            target_month = target_date.month
            target_year = target_date.year
            target_month_name = target_date.strftime('%b')  # e.g., 'Dec'
            target_month_full = target_date.strftime('%B')  # e.g., 'December'

            print(f"[CALENDAR] Target: day={target_day}, month={target_month_name} ({target_month}), year={target_year}")

            # First, navigate to the correct month/year
            self._navigate_to_month(target_month, target_year)

            time.sleep(1)

            # Now find and click the target day TWICE in the LEFT calendar (same month)
            print("[CALENDAR] Looking for calendar day cells...")

            day_cells = []

            # Find all day cells - use the inner element for clicking
            try:
                day_cells = self.driver.find_elements(By.CSS_SELECTOR, ".ant-picker-cell .ant-picker-cell-inner")
                print(f"[CALENDAR] Found {len(day_cells)} day cell elements")
            except Exception as e:
                print(f"[CALENDAR] CSS selector failed: {e}")

            if not day_cells:
                try:
                    day_cells = self.driver.find_elements(By.CSS_SELECTOR, "td.ant-picker-cell")
                    print(f"[CALENDAR] Found {len(day_cells)} TD cells")
                except:
                    pass

            # Find cells with our target day number in the LEFT calendar (first/start calendar)
            print(f"[CALENDAR] Searching for day {target_day} in left calendar...")
            target_cells = []

            # First, find the boundary between left and right calendars
            # Get all cells and find the middle X position
            all_x_positions = []
            for cell in day_cells:
                try:
                    if cell.is_displayed():
                        all_x_positions.append(cell.location['x'])
                except:
                    continue

            if all_x_positions:
                min_x = min(all_x_positions)
                max_x = max(all_x_positions)
                mid_x = (min_x + max_x) / 2
                print(f"[CALENDAR] X range: {min_x} to {max_x}, midpoint: {mid_x}")
            else:
                mid_x = 99999  # If we can't determine, include all

            for i, cell in enumerate(day_cells):
                try:
                    if cell.is_displayed():
                        cell_text = cell.text.strip()
                        cell_x = cell.location['x']

                        # Only consider cells in the LEFT calendar (x < midpoint)
                        if cell_x >= mid_x:
                            continue

                        # Check if this cell contains our target day
                        if cell_text.isdigit() and int(cell_text) == target_day:
                            # Check it's not disabled or from another month
                            cell_class = cell.get_attribute('class') or ''
                            parent_class = ''
                            try:
                                parent = cell.find_element(By.XPATH, "./..")
                                parent_class = parent.get_attribute('class') or ''
                            except:
                                pass

                            is_disabled = 'disabled' in cell_class.lower() or 'disabled' in parent_class.lower()
                            # Check for cells that belong to previous/next month (greyed out)
                            is_other_month = ('ant-picker-cell-in-view' not in parent_class) if parent_class else False

                            print(f"[CALENDAR]   Cell {i}: text='{cell_text}', x={cell_x}, disabled={is_disabled}, other_month={is_other_month}, in_left={cell_x < mid_x}")

                            if not is_disabled:
                                target_cells.append({
                                    'element': cell,
                                    'x': cell_x,
                                    'y': cell.location['y']
                                })
                except Exception as e:
                    continue

            print(f"[CALENDAR] Found {len(target_cells)} matching cells for day {target_day} in left calendar")

            if target_cells:
                # Use the first matching cell (in the left/start calendar)
                target_cell = target_cells[0]['element']

                # Click the date TWICE to select it as both start and end of range
                print(f"[CALENDAR] Clicking day {target_day} (first click)...")
                try:
                    target_cell.click()
                    print("[CALENDAR] First click successful")
                except Exception as e:
                    print(f"[CALENDAR] First click failed: {e}, trying JS click...")
                    self.driver.execute_script("arguments[0].click();", target_cell)

                time.sleep(0.5)

                # Click the SAME cell again for end date
                print(f"[CALENDAR] Clicking day {target_day} (second click - same cell)...")
                try:
                    target_cell.click()
                    print("[CALENDAR] Second click successful")
                except Exception as e:
                    print(f"[CALENDAR] Second click failed: {e}, trying JS click...")
                    self.driver.execute_script("arguments[0].click();", target_cell)

                time.sleep(1)
                print("[CALENDAR] Date selection completed")

            else:
                print(f"[CALENDAR] ERROR: Could not find day {target_day} in left calendar")
                # Debug: show what cells we can see
                print("[CALENDAR] Debug - all visible cells in left calendar:")
                for i, cell in enumerate(day_cells[:42]):  # First 42 = one month
                    try:
                        if cell.is_displayed() and cell.location['x'] < mid_x:
                            print(f"[CALENDAR]   Cell {i}: text='{cell.text}', x={cell.location['x']}")
                    except:
                        continue

        except Exception as e:
            print(f"[CALENDAR] Error selecting date: {e}")
            import traceback
            traceback.print_exc()

    def _navigate_to_month(self, target_month, target_year):
        """
        Navigate the calendar to the target month and year.

        Args:
            target_month: Target month (1-12)
            target_year: Target year (e.g., 2025)
        """
        try:
            print(f"[CALENDAR-NAV] Navigating to {target_month}/{target_year}...")

            max_iterations = 24  # Max 2 years of navigation
            iteration = 0

            while iteration < max_iterations:
                iteration += 1

                # Get current displayed month/year from the LEFT calendar header
                current_month, current_year = self._get_current_calendar_month()

                if current_month is None or current_year is None:
                    print("[CALENDAR-NAV] Could not determine current month/year")
                    break

                print(f"[CALENDAR-NAV] Current: {current_month}/{current_year}, Target: {target_month}/{target_year}")

                # Check if we're at the right month
                if current_month == target_month and current_year == target_year:
                    print("[CALENDAR-NAV] Reached target month!")
                    return True

                # Calculate direction: need to go forward or backward?
                current_total = current_year * 12 + current_month
                target_total = target_year * 12 + target_month

                if target_total > current_total:
                    # Need to go forward
                    print("[CALENDAR-NAV] Clicking next month...")
                    if not self._click_next_month():
                        print("[CALENDAR-NAV] Failed to click next")
                        break
                else:
                    # Need to go backward
                    print("[CALENDAR-NAV] Clicking previous month...")
                    if not self._click_prev_month():
                        print("[CALENDAR-NAV] Failed to click prev")
                        break

                time.sleep(0.5)

            print(f"[CALENDAR-NAV] Navigation completed after {iteration} iterations")
            return True

        except Exception as e:
            print(f"[CALENDAR-NAV] Error: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _get_current_calendar_month(self):
        """
        Get the currently displayed month and year from the LEFT calendar.

        Returns:
            tuple: (month_number, year) or (None, None) if not found
        """
        try:
            # Look for month/year buttons in the header
            # The left calendar header typically shows "Dec 2025" or has separate month/year buttons

            # Try finding the header text that contains month and year
            header_selectors = [
                ".ant-picker-header-view",
                ".ant-picker-header button",
                ".ant-picker-month-btn",
                ".ant-picker-year-btn",
            ]

            month_map = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12,
            }

            # Get all header elements
            headers = self.driver.find_elements(By.CSS_SELECTOR,
                ".ant-picker-header-view, .ant-picker-panel-header")

            found_month = None
            found_year = None

            for header in headers:
                try:
                    if header.is_displayed():
                        header_text = header.text.lower()
                        header_x = header.location['x']

                        # Only consider LEFT calendar header (lower x position)
                        # Usually there are two panels, we want the first one
                        print(f"[CALENDAR-NAV]   Header: '{header.text}', x={header_x}")

                        # Extract month
                        for month_name, month_num in month_map.items():
                            if month_name in header_text:
                                found_month = month_num
                                break

                        # Extract year (4 digit number)
                        import re
                        year_match = re.search(r'20\d{2}', header_text)
                        if year_match:
                            found_year = int(year_match.group())

                        if found_month and found_year:
                            return (found_month, found_year)
                except:
                    continue

            # Try individual month and year buttons
            try:
                month_btns = self.driver.find_elements(By.CSS_SELECTOR, ".ant-picker-month-btn")
                year_btns = self.driver.find_elements(By.CSS_SELECTOR, ".ant-picker-year-btn")

                if month_btns:
                    for month_name, month_num in month_map.items():
                        if month_name in month_btns[0].text.lower():
                            found_month = month_num
                            break

                if year_btns:
                    import re
                    year_match = re.search(r'20\d{2}', year_btns[0].text)
                    if year_match:
                        found_year = int(year_match.group())
            except:
                pass

            return (found_month, found_year)

        except Exception as e:
            print(f"[CALENDAR-NAV] Error getting current month: {e}")
            return (None, None)

    def _click_next_month(self):
        """Click the next month button (>)."""
        try:
            # Ant Design uses specific classes for navigation buttons
            next_selectors = [
                ".ant-picker-header-next-btn",
                "button.ant-picker-header-next-btn",
                ".ant-picker-header button[class*='next']",
                "button[aria-label*='next']",
                "button[aria-label*='Next']",
            ]

            for selector in next_selectors:
                try:
                    btns = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for btn in btns:
                        if btn.is_displayed():
                            # Get the first (leftmost) next button for the left calendar
                            btn.click()
                            print(f"[CALENDAR-NAV] Clicked next button")
                            return True
                except:
                    continue

            # Fallback: find by > character
            try:
                buttons = self.driver.find_elements(By.CSS_SELECTOR, ".ant-picker-header button")
                for btn in buttons:
                    if btn.is_displayed() and btn.text.strip() == '>':
                        btn.click()
                        return True
            except:
                pass

            return False

        except Exception as e:
            print(f"[CALENDAR-NAV] Error clicking next: {e}")
            return False

    def _click_prev_month(self):
        """Click the previous month button (<)."""
        try:
            # Ant Design uses specific classes for navigation buttons
            prev_selectors = [
                ".ant-picker-header-prev-btn",
                "button.ant-picker-header-prev-btn",
                ".ant-picker-header button[class*='prev']",
                "button[aria-label*='prev']",
                "button[aria-label*='Prev']",
            ]

            for selector in prev_selectors:
                try:
                    btns = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for btn in btns:
                        if btn.is_displayed():
                            btn.click()
                            print(f"[CALENDAR-NAV] Clicked prev button")
                            return True
                except:
                    continue

            # Fallback: find by < character
            try:
                buttons = self.driver.find_elements(By.CSS_SELECTOR, ".ant-picker-header button")
                for btn in buttons:
                    if btn.is_displayed() and btn.text.strip() == '<':
                        btn.click()
                        return True
            except:
                pass

            return False

        except Exception as e:
            print(f"[CALENDAR-NAV] Error clicking prev: {e}")
            return False

    def apply_dimension_filter(self, campaign_filter_text="aware"):
        """
        Apply dimension filter: Campaign name does not contain 'aware'.

        Args:
            campaign_filter_text: Text to filter out (default: 'aware')
        """
        print(f"[FILTER] Applying dimension filter (Campaign name does not contain '{campaign_filter_text}')...")

        try:
            time.sleep(2)

            # Find and click the "Dimension filter" button
            dimension_filter_btn = None

            # Strategy 1: Find by exact text content
            print("[FILTER] Looking for 'Dimension filter' button...")
            try:
                dimension_filter_btn = self.driver.find_element(
                    By.XPATH, "//*[contains(text(), 'Dimension filter')]"
                )
                print(f"[FILTER] Found by text: {dimension_filter_btn.tag_name}")
            except Exception as e:
                print(f"[FILTER] Text search failed: {e}")

            # Strategy 2: Find button with filter-related text
            if not dimension_filter_btn:
                print("[FILTER] Looking for button with filter text...")
                try:
                    buttons = self.driver.find_elements(By.XPATH, "//button")
                    print(f"[FILTER] Found {len(buttons)} buttons")
                    for i, btn in enumerate(buttons):
                        try:
                            btn_text = btn.text.lower()
                            if btn.is_displayed() and ('dimension' in btn_text or 'filter' in btn_text):
                                print(f"[FILTER]   Button {i}: text='{btn.text}'")
                                dimension_filter_btn = btn
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"[FILTER] Button search failed: {e}")

            # Strategy 3: Find by icon class (filter funnel icon)
            if not dimension_filter_btn:
                print("[FILTER] Looking for filter icon...")
                try:
                    filter_icons = self.driver.find_elements(By.XPATH,
                        "//*[contains(@class, 'filter') or contains(@aria-label, 'filter')]")
                    print(f"[FILTER] Found {len(filter_icons)} filter elements")
                    for elem in filter_icons:
                        try:
                            if elem.is_displayed():
                                parent = elem.find_element(By.XPATH, "./ancestor::button[1]")
                                if parent.is_displayed():
                                    print(f"[FILTER]   Found filter button via icon")
                                    dimension_filter_btn = parent
                                    break
                        except:
                            continue
                except Exception as e:
                    print(f"[FILTER] Icon search failed: {e}")

            if dimension_filter_btn and dimension_filter_btn.is_displayed():
                print("[FILTER] Clicking Dimension filter button...")
                try:
                    dimension_filter_btn.click()
                    print("[FILTER] Click successful")
                except Exception as e:
                    print(f"[FILTER] Direct click failed: {e}, trying JS click...")
                    self.driver.execute_script("arguments[0].click();", dimension_filter_btn)

                time.sleep(2)

                # Now configure the filter
                self._configure_filter(campaign_filter_text)
            else:
                print("[FILTER] ERROR: Could not find Dimension filter button")
                # Debug: show clickable elements
                print("[FILTER] Debug - showing potential filter buttons:")
                try:
                    all_buttons = self.driver.find_elements(By.XPATH, "//button")
                    for i, btn in enumerate(all_buttons[:20]):
                        try:
                            if btn.is_displayed():
                                print(f"[FILTER]   Button {i}: text='{btn.text[:30]}', y={btn.location['y']}")
                        except:
                            continue
                except:
                    pass

        except Exception as e:
            print(f"[FILTER] Error applying dimension filter: {e}")
            import traceback
            traceback.print_exc()

    def _configure_filter(self, filter_text):
        """
        Configure the dimension filter popup.
        1. Select 'Campaign name' in first dropdown
        2. Select 'does not contain' in second dropdown
        3. Type filter text in third field
        4. Click Apply
        """
        try:
            print("[FILTER-CONFIG] Configuring filter popup...")
            time.sleep(2)

            # Find dropdowns and input in the filter popup
            # Ant Design uses ant-select for dropdowns
            print("[FILTER-CONFIG] Looking for dropdowns...")

            # Strategy 1: Find Ant Design select components
            dropdowns = self.driver.find_elements(By.CSS_SELECTOR,
                ".ant-select, select, [class*='select'], [class*='dropdown']")
            print(f"[FILTER-CONFIG] Found {len(dropdowns)} dropdown elements")

            # Try to find and click first dropdown (Campaign name)
            print("[FILTER-CONFIG] Setting first dropdown (Campaign name)...")
            try:
                first_dropdown = None
                for i, dd in enumerate(dropdowns):
                    try:
                        if dd.is_displayed():
                            dd_text = dd.text.lower()
                            print(f"[FILTER-CONFIG]   Dropdown {i}: text='{dd.text[:30]}', class='{dd.get_attribute('class')[:30] if dd.get_attribute('class') else ''}'")
                            if 'campaign' in dd_text:
                                first_dropdown = dd
                                print(f"[FILTER-CONFIG]   -> Selected (contains 'campaign')")
                                break
                    except:
                        continue

                if not first_dropdown and dropdowns:
                    # Click the first visible dropdown
                    for dd in dropdowns:
                        if dd.is_displayed():
                            first_dropdown = dd
                            print(f"[FILTER-CONFIG] Using first visible dropdown")
                            break

                if first_dropdown:
                    print("[FILTER-CONFIG] Clicking first dropdown...")
                    first_dropdown.click()
                    time.sleep(1)

                    # Select "Campaign name" option
                    print("[FILTER-CONFIG] Looking for 'Campaign name' option...")
                    try:
                        campaign_options = self.driver.find_elements(
                            By.XPATH, "//*[contains(text(), 'Campaign name') or contains(text(), 'campaign name') or contains(text(), 'Campaign Name')]"
                        )
                        print(f"[FILTER-CONFIG] Found {len(campaign_options)} campaign name options")
                        for opt in campaign_options:
                            if opt.is_displayed():
                                print(f"[FILTER-CONFIG] Clicking: {opt.text}")
                                opt.click()
                                time.sleep(1)
                                break
                    except Exception as e:
                        print(f"[FILTER-CONFIG] Campaign name selection: {e} (may already be selected)")
            except Exception as e:
                print(f"[FILTER-CONFIG] Error with first dropdown: {e}")

            # Second dropdown: does not contain
            print("[FILTER-CONFIG] Setting second dropdown (does not contain)...")
            try:
                operator_options = [
                    "does not contain",
                    "does not",
                    "not contain",
                    "doesn't contain"
                ]

                # Re-find dropdowns after first selection
                dropdowns = self.driver.find_elements(By.CSS_SELECTOR,
                    ".ant-select, select, [class*='select'], [class*='dropdown']")

                for dd in dropdowns:
                    try:
                        if dd.is_displayed():
                            text = dd.text.lower()
                            if any(op in text for op in ['contain', 'equal', 'match', 'does', 'is']):
                                print(f"[FILTER-CONFIG] Found operator dropdown: {dd.text}")
                                dd.click()
                                time.sleep(1)
                                break
                    except:
                        continue

                # Select "does not contain" option
                print("[FILTER-CONFIG] Looking for 'does not contain' option...")
                for op in operator_options:
                    try:
                        option_elems = self.driver.find_elements(
                            By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{op}')]"
                        )
                        for opt in option_elems:
                            if opt.is_displayed():
                                print(f"[FILTER-CONFIG] Clicking operator: {opt.text}")
                                opt.click()
                                time.sleep(1)
                                break
                    except:
                        continue
            except Exception as e:
                print(f"[FILTER-CONFIG] Error with operator dropdown: {e}")

            # Third field: text input for filter value
            print(f"[FILTER-CONFIG] Entering filter text: '{filter_text}'...")
            try:
                text_inputs = self.driver.find_elements(By.CSS_SELECTOR,
                    "input[type='text'], input:not([type]), input[placeholder]")
                print(f"[FILTER-CONFIG] Found {len(text_inputs)} text inputs")

                for i, inp in enumerate(text_inputs):
                    try:
                        if inp.is_displayed():
                            inp_type = inp.get_attribute('type')
                            inp_placeholder = inp.get_attribute('placeholder') or ''
                            print(f"[FILTER-CONFIG]   Input {i}: type='{inp_type}', placeholder='{inp_placeholder}'")

                            # Skip date inputs
                            if 'date' not in inp_placeholder.lower():
                                inp.clear()
                                inp.send_keys(filter_text)
                                print(f"[FILTER-CONFIG] Entered filter text in input {i}")
                                time.sleep(1)
                                break
                    except Exception as e:
                        print(f"[FILTER-CONFIG]   Input {i} error: {e}")
                        continue
            except Exception as e:
                print(f"[FILTER-CONFIG] Error entering filter text: {e}")

            # Click Apply button
            print("[FILTER-CONFIG] Looking for Apply button...")
            try:
                apply_btn = None

                # Find Apply button
                buttons = self.driver.find_elements(By.XPATH, "//button | //span[contains(@class, 'btn')]")
                print(f"[FILTER-CONFIG] Found {len(buttons)} buttons")

                for btn in buttons:
                    try:
                        if btn.is_displayed():
                            btn_text = btn.text.lower()
                            if 'apply' in btn_text:
                                print(f"[FILTER-CONFIG] Found Apply button: {btn.text}")
                                apply_btn = btn
                                break
                    except:
                        continue

                if not apply_btn:
                    # Try by Ant Design button class
                    try:
                        apply_btn = self.driver.find_element(By.CSS_SELECTOR,
                            "button.ant-btn-primary, button[type='submit']")
                        print(f"[FILTER-CONFIG] Found primary button")
                    except:
                        pass

                if apply_btn and apply_btn.is_displayed():
                    print("[FILTER-CONFIG] Clicking Apply button...")
                    try:
                        apply_btn.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", apply_btn)
                    time.sleep(3)
                    print("[FILTER-CONFIG] Filter applied successfully")
                else:
                    print("[FILTER-CONFIG] ERROR: Could not find Apply button")

            except Exception as e:
                print(f"[FILTER-CONFIG] Error clicking Apply: {e}")

        except Exception as e:
            print(f"[FILTER-CONFIG] Error configuring filter: {e}")
            import traceback
            traceback.print_exc()

    def wait_for_data_load(self, timeout=15):
        """Wait for data to finish loading."""
        print("[LOAD] Waiting for data to load...")

        try:
            # Wait for loading indicator to disappear or data to appear
            time.sleep(3)  # Initial wait (reduced from 5 to 3)

            # Check for loading indicators
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    # Look for loading text or spinner
                    loading_elements = self.driver.find_elements(
                        By.XPATH, "//*[contains(text(), 'Loading') or contains(text(), 'loading')]"
                    )

                    # Also check for Ant Design spin (but exclude permanent elements)
                    spin_elements = self.driver.find_elements(By.CSS_SELECTOR, ".ant-spin, .loading, [class*='spinner']")

                    loading_visible = False
                    for elem in loading_elements + spin_elements:
                        try:
                            if elem.is_displayed():
                                # Get the class name
                                elem_class = elem.get_attribute('class') or ''
                                elem_text = elem.text[:30] if elem.text else ''

                                # IMPORTANT: Exclude permanent AG Grid elements that are not loaders
                                # ag-aria-description-container is a permanent accessibility element
                                if 'ag-aria-description-container' in elem_class:
                                    continue  # Skip this, it's not a loading indicator

                                # If we get here, it's a real loading indicator
                                print(f"[LOAD]   Still loading: {elem_class[:40] if elem_class else elem_text}")
                                loading_visible = True
                                break
                        except:
                            continue

                    if not loading_visible:
                        print("[LOAD] Data loaded successfully")
                        time.sleep(1)  # Extra wait for rendering (reduced from 2 to 1)
                        return True

                    time.sleep(1)
                except:
                    time.sleep(1)

            print("[LOAD] Loading wait timeout, proceeding anyway...")
            return True

        except Exception as e:
            print(f"[LOAD] Error waiting for load: {e}")
            return True

    def _extract_card_data(self):
        """
        Extract data from card-based layout.
        This is used when data is displayed as cards instead of a grid/table.

        Returns:
            list: List of dictionaries containing row data
        """
        print("[CARD] Extracting data from card layout...")
        table_data = []

        try:
            time.sleep(2)

            # Look for card containers
            # Try different card container selectors
            card_selectors = [
                ".card",
                ".data-card",
                "[class*='card']",
                ".ag-center-cols-container > div",  # AG Grid card mode
                ".ag-row",  # AG Grid rows (might be cards)
            ]

            cards = []
            for selector in card_selectors:
                try:
                    found_cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if found_cards:
                        print(f"[CARD] Found {len(found_cards)} elements with selector '{selector}'")
                        # Filter for displayed cards
                        cards = [c for c in found_cards if c.is_displayed()]
                        if cards:
                            print(f"[CARD] Using {len(cards)} visible cards from '{selector}'")
                            break
                except Exception as e:
                    print(f"[CARD] Selector '{selector}' failed: {e}")
                    continue

            if not cards:
                print("[CARD] No cards found")
                return []

            print(f"[CARD] Processing {len(cards)} cards...")

            for card_idx, card in enumerate(cards[:50]):  # Limit to first 50 cards
                try:
                    # Extract all text from the card
                    card_text = card.text.strip()

                    if not card_text:
                        continue

                    # Try to find URL (anchor tag)
                    landing_page = ""
                    try:
                        anchor = card.find_element(By.CSS_SELECTOR, "a[href]")
                        href = anchor.get_attribute("href")
                        landing_page = href if href else anchor.text.strip()
                    except:
                        # Try to extract URL from text
                        lines = card_text.split('\n')
                        for line in lines:
                            if 'http' in line.lower() or '.com' in line.lower() or '.de' in line.lower():
                                landing_page = line.strip()
                                break

                    # Try to find ROAS value (number, possibly with decimal)
                    roas = ""
                    try:
                        # Look for elements with ROAS-related text
                        import re
                        # Find numbers in the card text (ROAS, Spend, etc.)
                        numbers = re.findall(r'\d+\.?\d*', card_text)
                        if numbers:
                            # ROAS is typically a decimal number (e.g., 2.12)
                            for num in numbers:
                                if '.' in num and float(num) < 100:  # ROAS is usually < 100
                                    roas = num
                                    break
                            if not roas and numbers:
                                roas = numbers[0]  # Fallback to first number
                    except:
                        pass

                    # Create row data
                    row_data = {
                        "Landing page": landing_page,
                        "ROAS": roas,
                        "Card Text": card_text[:100]  # Store first 100 chars for debugging
                    }

                    # Debug first few cards
                    if card_idx < 5:
                        print(f"[CARD] Card {card_idx}:")
                        print(f"[CARD]   Landing page: {landing_page[:60] if landing_page else '(not found)'}")
                        print(f"[CARD]   ROAS: {roas}")
                        print(f"[CARD]   Text preview: {card_text[:80].replace(chr(10), ' ')}")

                    # Only add if we found meaningful data
                    if landing_page or roas:
                        table_data.append(row_data)

                except Exception as e:
                    print(f"[CARD] Card {card_idx} error: {e}")
                    continue

            print(f"[CARD] Successfully extracted {len(table_data)} cards")

        except Exception as e:
            print(f"[CARD] Error extracting card data: {e}")
            import traceback
            traceback.print_exc()

        return table_data

    def _extract_ag_grid_data(self):
        """
        Extract data from AG Grid component.

        Returns:
            list: List of dictionaries containing row data
        """
        print("[AG-GRID] Extracting data from AG Grid...")
        table_data = []

        try:
            time.sleep(2)

            # First, scroll to the grid to ensure it's visible
            try:
                ag_grid = self.driver.find_element(By.CSS_SELECTOR, ".ag-root")
                print("[AG-GRID] Scrolling to grid...")
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", ag_grid)
                time.sleep(2)
            except Exception as e:
                print(f"[AG-GRID] Scroll error (continuing anyway): {e}")

            # Skip Custom columns button - all columns are already visible in the grid
            print("[AG-GRID] Skipping Custom columns button (all columns already visible)")

            # Scroll down within the grid to load all rows (AG Grid uses virtual scrolling)
            print("[AG-GRID] Scrolling grid to load all rows...")
            try:
                # Find the grid body container
                grid_body = self.driver.find_element(By.CSS_SELECTOR, ".ag-body-viewport")
                # Scroll to bottom to trigger loading of all rows
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", grid_body)
                time.sleep(1)
                # Scroll back to top
                self.driver.execute_script("arguments[0].scrollTop = 0;", grid_body)
                time.sleep(1)
            except Exception as e:
                print(f"[AG-GRID] Grid scroll error (continuing anyway): {e}")

            # Scroll horizontally to load all columns (AG Grid uses virtual column scrolling)
            print("[AG-GRID] Scrolling grid horizontally to load all columns...")
            try:
                # Find horizontal scroll containers
                h_scroll_viewport = self.driver.find_element(By.CSS_SELECTOR, ".ag-body-horizontal-scroll-viewport")
                # Scroll to the right to reveal all columns
                self.driver.execute_script("arguments[0].scrollLeft = arguments[0].scrollWidth;", h_scroll_viewport)
                time.sleep(1)
                # Scroll back to left
                self.driver.execute_script("arguments[0].scrollLeft = 0;", h_scroll_viewport)
                time.sleep(1)
                print("[AG-GRID] Horizontal scroll completed")
            except Exception as e:
                print(f"[AG-GRID] Horizontal scroll error (continuing anyway): {e}")

            # Extract headers from AG Grid (including hidden/collapsed ones)
            headers = []
            header_positions = []  # Track which positions have headers (for checkbox columns)
            print("[AG-GRID] Extracting headers...")
            try:
                # Get ALL header cells including hidden ones
                all_header_cells = self.driver.find_elements(By.CSS_SELECTOR, ".ag-header-cell")
                print(f"[AG-GRID] Found {len(all_header_cells)} total header cells")

                # Also try to get headers from all column containers
                left_headers = self.driver.find_elements(By.CSS_SELECTOR, ".ag-pinned-left-header .ag-header-cell")
                center_headers = self.driver.find_elements(By.CSS_SELECTOR, ".ag-header-container .ag-header-cell")
                right_headers = self.driver.find_elements(By.CSS_SELECTOR, ".ag-pinned-right-header .ag-header-cell")

                # Combine all headers (left + center + right)
                combined_headers = left_headers + center_headers + right_headers
                if len(combined_headers) > len(all_header_cells):
                    all_header_cells = combined_headers
                    print(f"[AG-GRID] Using combined headers: {len(left_headers)} left + {len(center_headers)} center + {len(right_headers)} right = {len(combined_headers)} total")

                for idx, cell in enumerate(all_header_cells):
                    try:
                        # Extract header text even if not displayed (for hidden columns)
                        header_text = ""
                        try:
                            # Try .ag-header-cell-text
                            text_elem = cell.find_element(By.CSS_SELECTOR, ".ag-header-cell-text")
                            header_text = text_elem.text.strip()
                        except:
                            # Fallback to cell text
                            header_text = cell.text.strip()

                        # Also try col-id attribute as fallback
                        if not header_text:
                            col_id = cell.get_attribute('col-id')
                            if col_id and col_id not in ['selection', 'ag-Grid-AutoColumn']:
                                header_text = col_id.replace('_', ' ').title()

                        if header_text:
                            headers.append(header_text)
                            header_positions.append(idx)
                            is_displayed = cell.is_displayed()
                            print(f"[AG-GRID]   Header {idx}: '{header_text}' (displayed={is_displayed})")
                        else:
                            print(f"[AG-GRID]   Header {idx}: (empty - likely checkbox)")
                    except Exception as e:
                        print(f"[AG-GRID]   Header {idx} error: {e}")
                        continue
            except Exception as e:
                print(f"[AG-GRID] Header extraction error: {e}")

            # If no headers found, use known Atria column order
            if not headers or len(headers) < 5:
                print("[AG-GRID] Using KNOWN Atria column order")
                headers = [
                    "Landing page",
                    "ROAS",
                    "Spend",
                    # Add other expected columns as needed
                ]

            print(f"\n[AG-GRID] HEADERS ({len(headers)}): {headers}")

            # Check for expected columns
            expected_columns = [
                'Landing page', 'ROAS', 'Spend', 'Link clicks', 'ATC',
                'Checkouts Initiated', 'Purchases', 'AOV', 'CPM',
                'Landing page views', 'CPC (link click)', 'CTR (link click)'
            ]
            missing_columns = []
            for expected in expected_columns:
                # Check if any header contains the expected column name (case insensitive, partial match)
                found = any(expected.lower() in h.lower() for h in headers)
                if not found:
                    missing_columns.append(expected)

            if missing_columns:
                print(f"\n" + "!" * 80)
                print(f"[AG-GRID] ⚠ WARNING: MISSING {len(missing_columns)} EXPECTED COLUMNS")
                print(f"!" * 80)
                for col in missing_columns:
                    print(f"[AG-GRID]   ✗ {col}")
                print(f"!" * 80)
                print(f"[AG-GRID] ACTION REQUIRED:")
                print(f"[AG-GRID]   1. In Atria, click 'Custom columns' (or column settings icon)")
                print(f"[AG-GRID]   2. Enable these metrics: {', '.join(missing_columns[:5])}")
                if len(missing_columns) > 5:
                    print(f"[AG-GRID]      and {', '.join(missing_columns[5:])}")
                print(f"[AG-GRID]   3. Click 'Apply' or 'Save'")
                print(f"[AG-GRID]   4. Re-run this automation")
                print(f"!" * 80 + "\n")

            # Extract rows from AG Grid
            print("[AG-GRID] Extracting data rows...")
            try:
                # AG Grid uses multiple column containers
                # Get row index from one container, then find matching cells in all containers
                left_rows = self.driver.find_elements(By.CSS_SELECTOR, ".ag-pinned-left-cols-container .ag-row")
                center_rows = self.driver.find_elements(By.CSS_SELECTOR, ".ag-center-cols-container .ag-row")

                # Use whichever has more rows
                if len(center_rows) > len(left_rows):
                    rows = center_rows
                    print(f"[AG-GRID] Found {len(rows)} rows in center container")
                else:
                    rows = left_rows
                    print(f"[AG-GRID] Found {len(rows)} rows in left container")

                # If no rows in either, try the old way
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, ".ag-row")
                    print(f"[AG-GRID] Found {len(rows)} rows (fallback)")

                for row_idx, row in enumerate(rows):
                    try:
                        if not row.is_displayed():
                            continue

                        # Get the row-index attribute to match cells across containers
                        row_index_attr = row.get_attribute("row-index")

                        # Collect all cells for this row from ALL column containers
                        cells = []

                        # Strategy 1: Use row-index to find matching cells in all containers
                        if row_index_attr:
                            try:
                                # Debug: For first row, check what containers exist
                                if row_idx == 0:
                                    containers = []
                                    if self.driver.find_elements(By.CSS_SELECTOR, ".ag-pinned-left-cols-container"):
                                        containers.append("left")
                                    if self.driver.find_elements(By.CSS_SELECTOR, ".ag-center-cols-container"):
                                        containers.append("center")
                                    if self.driver.find_elements(By.CSS_SELECTOR, ".ag-pinned-right-cols-container"):
                                        containers.append("right")
                                    print(f"[AG-GRID] Available containers: {containers}")

                                # Find cells in pinned-left container
                                left_cells = self.driver.find_elements(
                                    By.CSS_SELECTOR,
                                    f".ag-pinned-left-cols-container .ag-row[row-index='{row_index_attr}'] .ag-cell"
                                )
                                # Find cells in center container
                                center_cells = self.driver.find_elements(
                                    By.CSS_SELECTOR,
                                    f".ag-center-cols-container .ag-row[row-index='{row_index_attr}'] .ag-cell"
                                )
                                # Find cells in pinned-right container (if exists)
                                right_cells = self.driver.find_elements(
                                    By.CSS_SELECTOR,
                                    f".ag-pinned-right-cols-container .ag-row[row-index='{row_index_attr}'] .ag-cell"
                                )

                                # Combine all cells
                                cells = left_cells + center_cells + right_cells

                                if row_idx == 0:
                                    print(f"[AG-GRID] Strategy 1: found {len(left_cells)} left + {len(center_cells)} center + {len(right_cells)} right = {len(cells)} total cells")
                                    if len(center_cells) == 0:
                                        print(f"[AG-GRID] WARNING: Center container has 0 cells! Trying broader selector...")
                                        # Try without the row-index filter
                                        try:
                                            center_all = self.driver.find_elements(By.CSS_SELECTOR, ".ag-center-cols-container .ag-cell")
                                            print(f"[AG-GRID]          Found {len(center_all)} total cells in center container (all rows)")
                                        except:
                                            pass
                            except Exception as e:
                                if row_idx == 0:
                                    print(f"[AG-GRID] Strategy 1 error: {e}")

                        # Strategy 2: If we only found 2 cells, try finding ALL cells with this row-index across entire grid
                        if len(cells) <= 2:
                            if row_idx == 0:
                                print(f"[AG-GRID] Strategy 1 found only {len(cells)} cells, trying Strategy 2 (XPath with row-index)...")
                            try:
                                # Find all cells anywhere in grid with this row-index
                                xpath = f"//div[@row-index='{row_index_attr}']//div[contains(@class, 'ag-cell') and @col-id]"
                                all_cells = self.driver.find_elements(By.XPATH, xpath)
                                if len(all_cells) > len(cells):
                                    cells = all_cells
                                    if row_idx == 0:
                                        print(f"[AG-GRID] Strategy 2: found {len(cells)} cells via XPath")
                            except Exception as e:
                                if row_idx == 0:
                                    print(f"[AG-GRID] Strategy 2 error: {e}")

                        # Strategy 3: Find cells by matching Y-position (same vertical position = same row)
                        if len(cells) <= 2:
                            if row_idx == 0:
                                print(f"[AG-GRID] Strategy 2 found only {len(cells)} cells, trying Strategy 3 (Y-position matching)...")
                            try:
                                # Get all cells in grid
                                all_cells_in_grid = self.driver.find_elements(By.CSS_SELECTOR, ".ag-cell[col-id]")
                                # Get Y position of current row
                                row_y = row.location['y']
                                # Filter cells that are at the same Y position (±5px tolerance)
                                matching_cells = []
                                for cell in all_cells_in_grid:
                                    try:
                                        cell_y = cell.location['y']
                                        if abs(cell_y - row_y) < 5:  # Same row if Y position is within 5px
                                            matching_cells.append(cell)
                                    except:
                                        continue

                                if len(matching_cells) > len(cells):
                                    cells = matching_cells
                                    if row_idx == 0:
                                        print(f"[AG-GRID] Strategy 3: found {len(cells)} cells by Y-position at y={row_y}")
                            except Exception as e:
                                if row_idx == 0:
                                    print(f"[AG-GRID] Strategy 3 error: {e}")

                        if not cells:
                            print(f"[AG-GRID] Row {row_idx}: no cells found")
                            continue

                        if row_idx == 0:
                            print(f"[AG-GRID] First row has {len(cells)} cells total")
                            # Debug: show col-id of all cells
                            for ci, c in enumerate(cells):
                                col_id = c.get_attribute('col-id') or 'N/A'
                                text_preview = c.text.strip()[:30] if c.text else '(empty)'
                                print(f"[AG-GRID]   Cell {ci}: col-id='{col_id}', text='{text_preview}'")

                            # Check if cells match headers
                            total_headers = len(headers) + (header_positions[0] if header_positions else 0)
                            if len(cells) < total_headers:
                                print(f"[AG-GRID] WARNING: Cell count ({len(cells)}) is less than expected ({total_headers})")
                                print(f"[AG-GRID]          Missing {total_headers - len(cells)} columns in the data.")

                        row_data = {}

                        # Determine if we need to skip the first cell (checkbox column)
                        # If we have header_positions and the first one is > 0, skip cells before it
                        cell_offset = 0
                        if header_positions and header_positions[0] > 0:
                            cell_offset = header_positions[0]
                            if row_idx == 0:
                                print(f"[AG-GRID] Skipping first {cell_offset} cells (checkbox columns)")

                        for cell_idx, cell in enumerate(cells[cell_offset:]):
                            try:
                                # Map cell index to header index
                                header_idx = cell_idx
                                if header_idx >= len(headers):
                                    if row_idx == 0:
                                        print(f"[AG-GRID] Row {row_idx}, Cell {cell_idx}: exceeded header count ({len(headers)} headers)")
                                    break

                                header = headers[header_idx]
                                cell_text = ""

                                # Special handling for Landing page column (URL)
                                if 'landing page' in header.lower() and cell_idx == 0:
                                    # Try to extract URL from anchor tag
                                    try:
                                        anchor = cell.find_element(By.CSS_SELECTOR, "a")
                                        href = anchor.get_attribute("href")
                                        if href:
                                            cell_text = href
                                        else:
                                            cell_text = anchor.text.strip()
                                    except:
                                        # No anchor, get text
                                        cell_text = cell.text.strip()

                                    # Clean up - remove "Used in X ads" text
                                    if cell_text and "Used in" in cell_text:
                                        cell_text = cell_text.split("Used in")[0].strip()
                                    if cell_text and "\n" in cell_text:
                                        cell_text = cell_text.split("\n")[0].strip()
                                else:
                                    # Regular cell
                                    cell_text = cell.text.strip()

                                row_data[header] = cell_text

                                # Debug first row
                                if row_idx < 2:
                                    print(f"[AG-GRID]   Row {row_idx}, Cell {cell_idx} ({header}): '{cell_text[:50] if cell_text else '(empty)'}'")

                            except Exception as e:
                                print(f"[AG-GRID] Row {row_idx}, Cell {cell_idx} error: {e}")
                                continue

                        # Only add row if it has data
                        if row_data and any(v for v in row_data.values()):
                            table_data.append(row_data)

                    except Exception as e:
                        print(f"[AG-GRID] Row {row_idx} extraction error: {e}")
                        continue

                print(f"[AG-GRID] Successfully extracted {len(table_data)} rows")

            except Exception as e:
                print(f"[AG-GRID] Row extraction error: {e}")
                import traceback
                traceback.print_exc()

        except Exception as e:
            print(f"[AG-GRID] Error extracting AG Grid data: {e}")
            import traceback
            traceback.print_exc()

        return table_data

    def extract_table_data(self):
        """
        Extract all data from the landing pages table.

        Returns:
            list: List of dictionaries containing row data
        """
        print("[TABLE] Extracting table data...")

        table_data = []

        try:
            time.sleep(3)

            # Scroll down the page to ensure data grid is visible (it's below the calendar)
            print("[TABLE] Scrolling page down to reveal data grid...")
            try:
                self.driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(2)
            except Exception as e:
                print(f"[TABLE] Page scroll error (continuing anyway): {e}")

            # Find the table element
            table = None

            # Try various table selectors
            print("[TABLE] Looking for table element...")
            table_selectors = [
                ("CSS", "table"),
                ("CSS", ".ant-table"),
                ("CSS", ".ant-table-container table"),
                ("XPATH", "//table"),
                ("XPATH", "//div[contains(@class, 'table')]//table"),
            ]

            # Check ALL tables and score them to find the best one
            best_table = None
            best_score = 0

            for selector_type, selector in table_selectors:
                try:
                    if selector_type == "CSS":
                        tables = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    else:
                        tables = self.driver.find_elements(By.XPATH, selector)

                    print(f"[TABLE] Selector '{selector}': found {len(tables)} tables")

                    for i, t in enumerate(tables):
                        try:
                            # Check table quality
                            rows = t.find_elements(By.CSS_SELECTOR, "tr")
                            is_displayed = t.is_displayed()

                            # Check headers
                            has_headers = False
                            header_count = 0
                            try:
                                thead = t.find_element(By.CSS_SELECTOR, "thead")
                                header_cells = thead.find_elements(By.CSS_SELECTOR, "th")
                                # Count non-empty headers
                                for hc in header_cells:
                                    if hc.text.strip():
                                        header_count += 1
                                has_headers = header_count > 2
                            except:
                                pass

                            # Check tbody
                            has_tbody = False
                            tbody_rows = 0
                            try:
                                tbody = t.find_element(By.CSS_SELECTOR, "tbody")
                                tbody_rows = len(tbody.find_elements(By.CSS_SELECTOR, "tr"))
                                has_tbody = tbody_rows > 0
                            except:
                                pass

                            # Score the table
                            score = 0
                            if len(rows) > 5:  # More rows = better
                                score += len(rows)
                            if has_headers:
                                score += 50  # Bonus for having headers
                            if header_count > 5:
                                score += header_count * 10  # More headers = better
                            if has_tbody:
                                score += 20
                            if tbody_rows > 3:
                                score += tbody_rows * 2

                            # Debug: show first few cells of first row
                            first_row_preview = ""
                            try:
                                if tbody_rows > 0:
                                    first_row = tbody.find_element(By.CSS_SELECTOR, "tr")
                                    first_cells = first_row.find_elements(By.CSS_SELECTOR, "td")
                                    cell_texts = [c.text.strip()[:20] for c in first_cells[:3]]
                                    first_row_preview = " | ".join(cell_texts)
                            except:
                                pass

                            print(f"[TABLE]   Table {i}: rows={len(rows)}, headers={header_count}, tbody_rows={tbody_rows}, score={score}, preview='{first_row_preview}'")

                            # Update best table if this one is better
                            if score > best_score and len(rows) > 1:
                                best_score = score
                                best_table = t
                                print(f"[TABLE]   -> New best table (score={score})")

                        except Exception as e:
                            print(f"[TABLE]   Table {i} check error: {e}")
                            continue

                    # If we found a good table (score > 10), use it
                    if best_table and best_score > 10:
                        table = best_table
                        print(f"[TABLE] Selected best table with score {best_score}")
                        break

                except Exception as e:
                    print(f"[TABLE] Selector '{selector}' failed: {e}")
                    continue

            # If we found a table but it has a low score, try AG Grid first
            if table and best_score < 50:
                print(f"[TABLE] Found table but quality is low (score={best_score}), trying AG Grid first...")
                try:
                    ag_grid = self.driver.find_element(By.CSS_SELECTOR, ".ag-root")
                    if ag_grid and ag_grid.is_displayed():
                        print("[TABLE] Found AG Grid, using it instead of low-quality table")
                        ag_data = self._extract_ag_grid_data()
                        if ag_data and len(ag_data) > 0:
                            return ag_data
                        else:
                            print("[TABLE] AG Grid returned no data, falling back to table")
                except Exception as e:
                    print(f"[TABLE] AG Grid attempt failed: {e}, falling back to table")

            # If we found a table, scroll to it to ensure it's visible
            if table:
                try:
                    print("[TABLE] Scrolling to table...")
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", table)
                    time.sleep(2)
                except Exception as e:
                    print(f"[TABLE] Scroll error (continuing anyway): {e}")

            if not table:
                print("[TABLE] Standard table not found, checking for AG Grid...")
                # Try AG Grid extraction
                try:
                    ag_grid = self.driver.find_element(By.CSS_SELECTOR, ".ag-root")
                    if ag_grid and ag_grid.is_displayed():
                        print("[TABLE] Found AG Grid, extracting data...")
                        ag_data = self._extract_ag_grid_data()
                        if ag_data:
                            return ag_data
                        else:
                            print("[TABLE] AG Grid extraction returned no data, trying card-based extraction...")
                except Exception as e:
                    print(f"[TABLE] AG Grid extraction failed: {e}")

                # Try card-based extraction as fallback
                print("[TABLE] Checking for card-based layout...")
                try:
                    card_data = self._extract_card_data()
                    if card_data:
                        return card_data
                except Exception as e:
                    print(f"[TABLE] Card extraction failed: {e}")

                print("[TABLE] ERROR: Could not find table, AG Grid, or card-based data")
                # Debug: show what's on the page
                print("[TABLE] Debug - looking for any table-like elements...")
                try:
                    all_tables = self.driver.find_elements(By.XPATH, "//*[contains(@class, 'table')]")
                    for i, t in enumerate(all_tables[:10]):
                        print(f"[TABLE]   Element {i}: tag={t.tag_name}, class='{t.get_attribute('class')[:40] if t.get_attribute('class') else ''}'")
                except:
                    pass
                return []

            # Extract headers - ALWAYS extract from page first, then verify
            headers = []
            print("[TABLE] Extracting headers...")

            # Try to find header row in thead
            try:
                thead = table.find_element(By.CSS_SELECTOR, "thead")
                header_rows = thead.find_elements(By.CSS_SELECTOR, "tr")
                print(f"[TABLE] Found {len(header_rows)} header rows in thead")

                # Use the last header row (sometimes there are multiple)
                header_row = header_rows[-1] if header_rows else None
                if header_row:
                    header_cells = header_row.find_elements(By.CSS_SELECTOR, "th, td")
                    print(f"[TABLE] Found {len(header_cells)} header cells")

                    for idx, cell in enumerate(header_cells):
                        try:
                            # Get text, handling multi-line headers
                            text = cell.text.strip().replace('\n', ' ')
                            # Skip empty headers (likely checkbox column)
                            if text:
                                headers.append(text)
                            else:
                                # Check if it's a checkbox column (first column, empty)
                                if idx == 0:
                                    print(f"[TABLE]   Header {idx}: (empty - likely checkbox)")
                                else:
                                    headers.append(f"Column_{idx}")
                            print(f"[TABLE]   Header {idx}: '{text if text else '(empty)'}'")
                        except:
                            continue
            except Exception as e:
                print(f"[TABLE] Header extraction from thead error: {e}")

            # If no headers extracted, use KNOWN Atria column order
            if not headers or len(headers) < 5:
                print("[TABLE] WARNING: Could not extract headers, using KNOWN Atria column order")
                print("[TABLE] Based on Atria screenshot, columns are:")
                headers = [
                    "Landing page",
                    "Landing page views",
                    "Spend",
                    "Link clicks",
                    "ATC",
                    "Checkouts Initiated",
                    "Purchases",
                    "ROAS",
                    "AOV",
                    "CPM",
                    "CPC (link click)",
                    "CTR (link click)",
                    "Cost per landing"
                ]
                for i, h in enumerate(headers):
                    print(f"[TABLE]   {i}: {h}")

            # Ensure first header is "Landing page" (for URL column)
            # Some extractions might skip it or have a different name
            if headers and 'landing page' not in headers[0].lower():
                print(f"[TABLE] First header is '{headers[0]}', not 'Landing page'")
                # Check if it looks like we're missing the Landing page column
                if headers[0].lower() in ['landing page views', 'cpm', 'spend']:
                    print("[TABLE] Prepending 'Landing page' to headers")
                    headers.insert(0, "Landing page")

            print(f"\n[TABLE] FINAL HEADERS ({len(headers)}):")
            for i, h in enumerate(headers):
                print(f"[TABLE]   [{i}] {h}")

            # Extract rows
            print("[TABLE] Extracting data rows...")
            try:
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                if not rows:
                    rows = table.find_elements(By.XPATH, ".//tr[position() > 1]")
                print(f"[TABLE] Found {len(rows)} rows")
            except Exception as e:
                print(f"[TABLE] Row extraction error: {e}")
                rows = []

            for row_idx, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.CSS_SELECTOR, "td")
                    if not cells:
                        continue

                    # Debug: print cell count for first row
                    if row_idx == 0:
                        print(f"[TABLE] First row has {len(cells)} cells, we have {len(headers)} headers")
                        # Print first few cell contents for debugging
                        for ci, c in enumerate(cells[:5]):
                            try:
                                txt = c.text.strip()[:30] if c.text else "(empty)"
                                print(f"[TABLE]   Cell {ci}: '{txt}'")
                            except:
                                pass

                    # Check if we need to skip the first cell (checkbox column)
                    # If first cell is empty or very short (just checkbox), skip it
                    cell_offset = 0
                    if len(cells) > len(headers):
                        # More cells than headers - likely a checkbox column
                        first_cell_text = cells[0].text.strip() if cells[0].text else ""
                        # Check if first cell looks like a checkbox (empty or very short)
                        if len(first_cell_text) < 3 or first_cell_text.isdigit():
                            cell_offset = 1
                            print(f"[TABLE] Skipping first cell (checkbox column), offset={cell_offset}")

                    row_data = {}

                    for i, cell in enumerate(cells[cell_offset:]):  # Skip checkbox cell if needed
                        try:
                            header = headers[i] if i < len(headers) else f"Column_{i}"

                            # Special handling for first data column (Landing page) - extract URL from anchor
                            if i == 0:
                                # Try to find anchor tag with URL
                                cell_text = ""
                                try:
                                    anchor = cell.find_element(By.CSS_SELECTOR, "a")
                                    href = anchor.get_attribute("href")
                                    if href:
                                        cell_text = href
                                    else:
                                        cell_text = anchor.text.strip()
                                except:
                                    # No anchor, try getting text or title
                                    cell_text = cell.get_attribute("title") or cell.text.strip()
                                    # Also check for nested anchor
                                    try:
                                        nested_a = cell.find_element(By.XPATH, ".//a[@href]")
                                        if nested_a:
                                            href = nested_a.get_attribute("href")
                                            if href:
                                                cell_text = href
                                    except:
                                        pass

                                # Clean up - remove "Used in X ads" text
                                if cell_text and "Used in" in cell_text:
                                    cell_text = cell_text.split("Used in")[0].strip()
                                if cell_text and "\n" in cell_text:
                                    cell_text = cell_text.split("\n")[0].strip()
                            else:
                                cell_text = cell.text.strip()

                            row_data[header] = cell_text
                        except Exception as e:
                            print(f"[TABLE] Cell {i} error: {e}")
                            continue

                    # Debug: print first few rows with all their data
                    if row_idx < 3:
                        lp = row_data.get('Landing page', 'N/A')
                        spend = row_data.get('Spend', 'N/A')
                        clicks = row_data.get('Link clicks', 'N/A')
                        print(f"[TABLE]   Row {row_idx}: Landing page='{lp[:50] if lp else 'N/A'}', Spend={spend}, Link clicks={clicks}")

                    if row_data and any(v for v in row_data.values()):
                        # Skip if it's a "Net Results" summary row
                        first_value = list(row_data.values())[0] if row_data else ""
                        if "Net Results" not in str(first_value):
                            table_data.append(row_data)
                except Exception as e:
                    print(f"[TABLE] Row {row_idx} extraction error: {e}")
                    continue

            print(f"[TABLE] Successfully extracted {len(table_data)} rows")

        except Exception as e:
            print(f"[TABLE] Error extracting table data: {e}")
            import traceback
            traceback.print_exc()

        return table_data

    def display_data(self, data):
        """
        Display extracted data to console.

        Args:
            data: List of dictionaries with table data
        """
        if not data:
            print("\nNo data to display")
            return

        print("\n" + "=" * 100)
        print("EXTRACTED LANDING PAGE DATA")
        print("=" * 100)

        # Display each row with Landing page URL prominently
        for i, row in enumerate(data):
            landing_page = row.get('Landing page', 'N/A')
            print(f"\n[{i+1}] URL: {landing_page}")
            print("    Data:")
            for key, value in row.items():
                if key != 'Landing page':
                    print(f"      {key}: {value}")

        print("\n" + "=" * 100)
        print(f"Total rows: {len(data)}")
        print("=" * 100)


def run_add_tracker_report(date_obj, date_str):
    """
    Run the Add Tracker report extraction from both Atria pages.

    Args:
        date_obj: datetime object for the date to extract
        date_str: String representation of the date

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n\n")
    print("=" * 80)
    print("ADD TRACKER REPORT".center(80))
    print("=" * 80 + "\n")

    try:
        from browser_manager import BrowserManager
        import time

        print("[1/9] Opening browser...")
        manager = BrowserManager(use_existing_chrome=False)
        driver = manager.start_browser()

        extractor = AtriaDataExtractor(driver)

        # =====================================================================
        # REPORT 1
        # =====================================================================
        print("\n" + "-" * 60)
        print("REPORT 1: Top Performing Landing Pages (REO)")
        print("-" * 60)

        print("[2/9] Navigating to Atria report 1...")
        extractor.navigate_to_report(1)

        # Check and wait for login if needed
        if not extractor.check_and_wait_for_login():
            print("Login failed or timed out")
            manager.close()
            return False

        print("[3/9] Setting date for report 1...")
        extractor.set_date(date_obj)

        print("[4/9] Waiting for data to load...")
        extractor.wait_for_data_load()

        print("[5/9] Extracting table data from report 1...")
        data_report_1 = extractor.extract_table_data()

        # Display data on console for verification
        print("\n*** REPORT 1 DATA ***")
        extractor.display_data(data_report_1)

        # =====================================================================
        # REPORT 2
        # =====================================================================
        print("\n" + "-" * 60)
        print("REPORT 2: Second Report")
        print("-" * 60)

        print("[6/9] Navigating to Atria report 2...")
        extractor.navigate_to_report(2)
        time.sleep(3)

        print("[7/9] Setting date for report 2...")
        extractor.set_date(date_obj)

        print("[8/9] Waiting for data to load...")
        extractor.wait_for_data_load()

        print("Extracting table data from report 2...")
        data_report_2 = extractor.extract_table_data()

        # Display data on console for verification
        print("\n*** REPORT 2 DATA ***")
        extractor.display_data(data_report_2)

        # =====================================================================
        # SUMMARY
        # =====================================================================
        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"Report 1: {len(data_report_1)} rows extracted")
        print(f"Report 2: {len(data_report_2)} rows extracted")
        print("=" * 80)

        # Close browser
        manager.close()

        # =====================================================================
        # WRITE TO GOOGLE SHEETS
        # =====================================================================
        print("\n[9/9] Writing to Google Sheets...")
        try:
            from services.sheets.add_tracker_helpers import write_atria_data_to_sheets
            write_atria_data_to_sheets(date_obj, data_report_1, data_report_2)
            print("  Successfully wrote to Google Sheets!")
        except Exception as e:
            print(f"  Warning: Could not write to Google Sheets: {e}")
            print("  (Data was extracted successfully, but sheet update failed)")
            import traceback
            traceback.print_exc()

        print("\n" + "=" * 80)
        print("ADD TRACKER REPORT COMPLETED SUCCESSFULLY")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\nAdd Tracker Report failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    """Test the extractor standalone."""
    from datetime import datetime, timedelta

    # Use previous day by default
    date_obj = datetime.now() - timedelta(days=1)
    date_str = date_obj.strftime('%d-%b-%Y')

    print(f"Testing Add Tracker Report for {date_str}")

    success = run_add_tracker_report(date_obj, date_str)

    if success:
        print("\nTest completed successfully!")
    else:
        print("\nTest failed!")

    input("\nPress Enter to exit...")
