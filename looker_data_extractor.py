"""
Enhanced Looker Studio Data Extractor
Handles date selection, medium filtering, and proper data formatting
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import re
from datetime import datetime, timedelta


class LookerDataExtractor:
    """Extract specific metrics from Looker Studio with date and filter control."""

    def __init__(self, driver):
        """
        Initialize the extractor.

        Args:
            driver: Selenium WebDriver instance
        """
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    def parse_number(self, value_str):
        """
        Parse numbers with K (thousands) and M (millions) suffixes.

        Args:
            value_str: String like '3.51M', '29.77K', '819', '0.85%'

        Returns:
            float: The actual number
        """
        if not value_str or value_str == '-':
            return 0.0

        # Remove € and % symbols, and whitespace
        cleaned = value_str.replace('€', '').replace('%', '').strip()

        # Handle K (thousands)
        if 'K' in cleaned:
            number = float(cleaned.replace('K', ''))
            return number * 1000

        # Handle M (millions)
        if 'M' in cleaned:
            number = float(cleaned.replace('M', ''))
            return number * 1000000

        # Regular number
        try:
            return float(cleaned.replace(',', ''))
        except ValueError:
            return 0.0

    def wait_for_data_load(self, seconds=5):
        """Wait for data to load after interactions."""
        time.sleep(seconds)

    def take_screenshot(self, filename):
        """Take a screenshot of the current page."""
        filepath = f"{filename}.png"
        self.driver.save_screenshot(filepath)
        return filepath

    def set_date_range(self, start_date, end_date):
        """
        Set the date range in Looker Studio.

        Args:
            start_date: datetime object or string 'YYYY-MM-DD'
            end_date: datetime object or string 'YYYY-MM-DD'
        """

        # Convert strings to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        try:
            # Find and click the date range button using the class "date-text"
            # This element shows current date range like "Oct 1, 2025 - Oct 9, 2025"

            date_button = None

            # First try by class name (most reliable based on debug output)
            try:
                date_button = self.driver.find_element(By.CLASS_NAME, "date-text")
            except:
                # Fallback: try by xpath
                try:
                    date_button = self.driver.find_element(By.XPATH, "//div[contains(@class, 'date-text')]")
                except:
                    # Last resort: find by date pattern
                    elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '2025')]")
                    for elem in elements:
                        try:
                            if elem.is_displayed() and elem.location['y'] < 400:
                                date_button = elem
                                break
                        except:
                            continue

            if date_button and date_button.is_displayed():
                # Click to open date picker
                date_button.click()
                time.sleep(2)

                # Now set the dates in the calendar
                self._set_calendar_dates(start_date, end_date)

        except Exception as e:
            pass

    def _set_calendar_dates(self, start_date, end_date):
        """Internal method to set specific dates by clicking on calendar."""
        try:
            # Wait for calendar to appear
            time.sleep(3)

            # The calendar has TWO calendars side by side: Start Date (left) and End Date (right)
            # Each date appears TWICE - once in each calendar
            # We need to click the date in the FIRST calendar, then in the SECOND calendar

            start_day = start_date.day
            end_day = end_date.day

            # Find all TD elements that are date cells
            all_date_cells = self.driver.find_elements(By.XPATH, "//td")

            # Filter to get only visible date cells with our target numbers
            visible_cells = []
            for cell in all_date_cells:
                try:
                    if cell.is_displayed() and cell.text.strip().isdigit():
                        cell_number = int(cell.text.strip())
                        if 1 <= cell_number <= 31:
                            visible_cells.append({
                                'element': cell,
                                'number': cell_number,
                                'x': cell.location['x']
                            })
                except:
                    pass

            # Sort by X position (left to right) to identify which calendar they belong to
            visible_cells.sort(key=lambda c: c['x'])

            # Find start date cells
            start_cells = [c for c in visible_cells if c['number'] == start_day]

            # Find end date cells
            end_cells = [c for c in visible_cells if c['number'] == end_day]

            # Click the FIRST occurrence (left calendar = start date)
            if len(start_cells) > 0:
                start_cells[0]['element'].click()
                time.sleep(1)

            # Click the SECOND occurrence (right calendar = end date)
            # If start and end are the same day, click the 2nd instance
            # If different, click the first instance of end_day in the right calendar
            if start_day == end_day:
                # Same date for start and end - click the 2nd instance
                if len(end_cells) >= 2:
                    end_cells[1]['element'].click()
                    time.sleep(1)
            else:
                # Different dates - find end_day in right calendar
                # The right calendar cells have higher X positions
                if len(end_cells) > 0:
                    # If we have 2 instances, use the 2nd (right calendar)
                    # Otherwise use the 1st
                    if len(end_cells) >= 2:
                        end_cells[1]['element'].click()
                    else:
                        end_cells[0]['element'].click()
                    time.sleep(1)

            # Now find and click the Apply button
            time.sleep(1)

            # Use the class we discovered: "apply-button"
            try:
                apply_button = self.driver.find_element(By.CLASS_NAME, "apply-button")
                if apply_button.is_displayed():
                    apply_button.click()
                    time.sleep(8)  # Wait for data to reload
                    return
            except:
                pass

            # Fallback: try other selectors
            apply_selectors = [
                "//button[contains(@class, 'apply-button')]",
                "//button[contains(text(), 'Apply')]",
                "//button[text()='Apply']"
            ]

            for selector in apply_selectors:
                try:
                    apply_button = self.driver.find_element(By.XPATH, selector)
                    if apply_button and apply_button.is_displayed():
                        apply_button.click()
                        time.sleep(8)
                        return
                except:
                    continue

        except Exception as e:
            pass

    def select_medium(self, medium_name):
        """
        Select a specific medium filter (Facebook or Google Ads).

        Strategy:
        1. Open the Medium dropdown
        2. Ensure the target medium is checked
        3. Ensure the other medium is unchecked
        4. Close the dropdown to apply the filter

        Args:
            medium_name: 'Facebook' or 'Google Ads'
        """
        try:
            # Determine which medium to exclude
            other_medium = 'Google Ads' if medium_name == 'Facebook' else 'Facebook'

            # Find the Medium dropdown - it's in the top filter bar
            medium_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Medium')]")

            medium_button = None
            for elem in medium_elements:
                try:
                    if elem.is_displayed() and elem.location['y'] < 300:
                        medium_button = elem
                        break
                except:
                    continue

            if not medium_button:
                return

            # Click to open dropdown
            medium_button.click()
            time.sleep(3)

            # Find all checkboxes with role="checkbox"
            # We need to manage BOTH checkboxes to ensure correct state
            try:
                checkboxes = self.driver.find_elements(By.XPATH, "//*[@role='checkbox']")

                target_checkbox = None
                other_checkbox = None

                # Find both checkboxes
                for checkbox in checkboxes:
                    try:
                        if not checkbox.is_displayed():
                            continue

                        # Get the parent container
                        parent = checkbox.find_element(By.XPATH, "./ancestor::div[contains(@class, 'item')][1]")

                        # Check which medium this checkbox belongs to
                        if medium_name in parent.text:
                            target_checkbox = checkbox
                        elif other_medium in parent.text:
                            other_checkbox = checkbox

                    except:
                        continue

                # Now manage the checkbox states
                # Target should be checked, other should be unchecked

                if target_checkbox:
                    is_checked = target_checkbox.get_attribute('aria-checked') == 'true'
                    if not is_checked:
                        # Check it
                        target_checkbox.click()
                        time.sleep(1.5)

                if other_checkbox:
                    is_checked = other_checkbox.get_attribute('aria-checked') == 'true'
                    if is_checked:
                        # Uncheck it
                        other_checkbox.click()
                        time.sleep(1.5)

            except Exception as e:
                pass

            # Close the dropdown to apply the filter
            time.sleep(1)

            # Press ESC key to close dropdown
            try:
                actions = ActionChains(self.driver)
                actions.send_keys(Keys.ESCAPE).perform()
                time.sleep(2)
            except:
                pass

            # Wait for the filter to apply and data to reload
            time.sleep(8)

        except Exception as e:
            pass

    def extract_metrics(self):
        """
        Extract the main KPI metrics from the page.

        Returns:
            dict: Dictionary with metric names and values
        """

        metrics = {
            'Impressions': 0,
            'CTR': 0,
            'Clicks': 0,
            'Conversions': 0,
            'Conversion %': 0,
            'Online Revenue': 0,
            'Spend': 0,
            'AOV': 0,
            'CPO': 0,
            'ROAS': 0
        }

        time.sleep(3)  # Wait for data to render

        try:
            # NEW STRATEGY: Extract from specific Y coordinates where KPI values are located
            # Based on testing, the values are around Y=259
            script = """
                let allElements = document.querySelectorAll('*');
                let labels = [];
                let values = [];

                // First pass: find labels (around Y=200-240)
                // Second pass: find values (around Y=250-280)
                allElements.forEach(el => {
                    let rect = el.getBoundingClientRect();
                    let text = el.textContent.trim();

                    if (el.children.length === 0 && text && text.length < 50) {
                        // Labels are higher up
                        if (rect.y >= 180 && rect.y < 250) {
                            let labelMatch = ['Impressions', 'CTR', 'Clicks', 'Conversions', 'Conversion',
                                            'Online Revenue', 'Spend', 'AOV', 'CPO', 'ROAS'];
                            for (let label of labelMatch) {
                                if (text === label || text.includes(label)) {
                                    labels.push({
                                        text: label,
                                        x: Math.round(rect.x),
                                        y: Math.round(rect.y)
                                    });
                                }
                            }
                        }

                        // Values are lower (Y ~ 250-280) and contain numbers
                        if (rect.y >= 245 && rect.y < 285 && text.match(/[0-9]/)) {
                            values.push({
                                text: text,
                                x: Math.round(rect.x),
                                y: Math.round(rect.y)
                            });
                        }
                    }
                });

                // Sort by X position (left to right)
                labels.sort((a, b) => a.x - b.x);
                values.sort((a, b) => a.x - b.x);

                // Match labels to values by X position
                let results = {};
                for (let i = 0; i < labels.length; i++) {
                    let label = labels[i];
                    // Find value with similar X position
                    for (let value of values) {
                        if (Math.abs(value.x - label.x) < 100) {
                            results[label.text] = value.text;
                            break;
                        }
                    }
                }

                return results;
            """

            found_metrics = self.driver.execute_script(script)

            # Parse the found values
            label_map = {
                'Impressions': 'Impressions',
                'CTR': 'CTR',
                'Clicks': 'Clicks',
                'Conversions': 'Conversions',
                'Conversion %': 'Conversion %',
                'Conversion': 'Conversion %',
                'Online Revenue': 'Online Revenue',
                'Spend': 'Spend',
                'AOV': 'AOV',
                'CPO': 'CPO',
                'ROAS': 'ROAS'
            }

            for label, value in found_metrics.items():
                clean_label = label.replace('*', '').strip()
                if clean_label in label_map:
                    target_metric = label_map[clean_label]
                    parsed_value = self.parse_number(value)
                    metrics[target_metric] = parsed_value

        except Exception as e:
            pass

        return metrics

    def _extract_metrics_alternative(self):
        """Alternative method to extract metrics using different approach."""
        metrics = {
            'Impressions': 0,
            'CTR': 0,
            'Clicks': 0,
            'Conversions': 0,
            'Conversion %': 0,
            'Online Revenue': 0,
            'Spend': 0,
            'AOV': 0,
            'CPO': 0,
            'ROAS': 0
        }

        try:
            # Look in the top section (y < 300) for KPI scorecards
            script = """
                let topElements = [];
                let allElements = document.querySelectorAll('*');

                allElements.forEach(el => {
                    let rect = el.getBoundingClientRect();
                    if (rect.y < 300 && rect.width > 0 && el.children.length === 0) {
                        let text = el.textContent.trim();
                        if (text && text.length < 50) {
                            topElements.push({
                                text: text,
                                x: Math.round(rect.x),
                                y: Math.round(rect.y)
                            });
                        }
                    }
                });

                return topElements;
            """

            elements = self.driver.execute_script(script)

            # Group by Y position
            from collections import defaultdict
            rows = defaultdict(list)
            for elem in elements:
                y_group = round(elem['y'] / 15) * 15
                rows[y_group].append(elem)

            # Process each row
            for y_pos in sorted(rows.keys()):
                row = sorted(rows[y_pos], key=lambda x: x['x'])
                row_texts = [elem['text'] for elem in row]

                # Look for metric patterns
                for i, text in enumerate(row_texts):
                    if text in metrics.keys() or text.replace('*', '').strip() in metrics.keys():
                        # Next element might be the value
                        if i + 1 < len(row_texts):
                            value_text = row_texts[i + 1]
                            clean_label = text.replace('*', '').strip()
                            if clean_label in metrics:
                                metrics[clean_label] = self.parse_number(value_text)

        except Exception as e:
            print(f"  ⚠️  Alternative extraction error: {e}")

        return metrics

    def get_previous_day_date(self, days_back=1):
        """
        Get the date for N days back.

        Args:
            days_back: Number of days to go back (default: 1 for yesterday)

        Returns:
            datetime object
        """
        return datetime.now() - timedelta(days=days_back)
