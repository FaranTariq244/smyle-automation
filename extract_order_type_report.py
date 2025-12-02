"""
Order Type Report Extraction - Extract subscription and order metrics from Looker Studio
"""

from browser_manager import BrowserManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta
import time
import re
import json


class OrderTypeDataExtractor:
    """Extract Order Type metrics from Looker Studio."""

    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    def parse_number(self, value_str):
        """Parse numbers with K, M suffixes and currency/percentage symbols."""
        if not value_str or value_str == '-':
            return 0.0

        # Remove symbols and whitespace
        cleaned = value_str.replace('€', '').replace('%', '').replace(',', '').strip()

        # Handle K (thousands)
        if 'K' in cleaned:
            number = float(cleaned.replace('K', ''))
            return number * 1000

        # Handle M (millions)
        if 'M' in cleaned:
            number = float(cleaned.replace('M', ''))
            return number * 1000000

        try:
            return float(cleaned)
        except:
            return 0.0

    def set_date_range(self, start_date, end_date):
        """Set the date range in Looker Studio using calendar date clicking."""
        print(f"Setting date range to {start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}")

        try:
            # Find and click the date range button (same approach as working script)
            date_button = None

            # First try by class name
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

            if not date_button or not date_button.is_displayed():
                print("Could not find date selector button")
                return False

            # Click to open date picker
            print("Opening date picker...")
            date_button.click()
            time.sleep(2)

            # Now set the dates in the calendar
            self._set_calendar_dates(start_date, end_date)

            return True

        except Exception as e:
            print(f"Error setting date range: {e}")
            return False

    def _set_calendar_dates(self, start_date, end_date):
        """Internal method to set specific dates by clicking on calendar."""
        try:
            # Wait for calendar to appear
            time.sleep(3)

            start_day = start_date.day
            end_day = end_date.day

            print(f"Selecting dates: Start day={start_day}, End day={end_day}")

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
                print(f"Clicking start date: {start_day}")
                start_cells[0]['element'].click()
                time.sleep(1)

            # Click the SECOND occurrence (right calendar = end date)
            if start_day == end_day:
                # Same date for start and end - click the 2nd instance
                if len(end_cells) >= 2:
                    print(f"Clicking end date: {end_day} (same as start)")
                    end_cells[1]['element'].click()
                    time.sleep(1)
            else:
                # Different dates
                if len(end_cells) > 0:
                    # If we have 2 instances, use the 2nd (right calendar)
                    if len(end_cells) >= 2:
                        print(f"Clicking end date: {end_day}")
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
                    print("Clicking Apply button...")
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
                        print("Clicking Apply button...")
                        apply_button.click()
                        time.sleep(8)
                        return
                except:
                    continue

        except Exception as e:
            print(f"Error setting calendar dates: {e}")

    def extract_order_type_metrics(self):
        """Extract the order type metrics table - Net Revenue & AOV per Order Type."""
        print("Extracting order type metrics table...")

        try:
            # Wait longer for the data table to load after date change
            print("Waiting for data to load...")
            time.sleep(5)

            # Only extract these specific order types
            ALLOWED_ORDER_TYPES = [
                'repeat_subscription',
                'repeat_single',
                'first_subscription',
                'first_sub_after_sub',
                'first_sub_after_single',
                'first_single'
            ]

            # Looker Studio uses custom div-based tables, not HTML tables
            # Look for div elements with class "row" that contain "cell" divs
            print("Looking for Looker Studio custom table rows...")

            # Find all div.row elements
            row_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'row')]")
            print(f"Found {len(row_elements)} row element(s)")

            if len(row_elements) == 0:
                print(" No row elements found")
                return {}

            metrics = {}

            # Process each row
            for row_idx, row_element in enumerate(row_elements, 1):
                try:
                    # Find all cell divs within this row
                    cells = row_element.find_elements(By.XPATH, ".//div[contains(@class, 'cell')]")

                    if len(cells) < 4:
                        # Skip rows that don't have at least 4 cells
                        continue

                    # Extract cell values using span.cell-value
                    cell_values = []
                    for cell in cells:
                        try:
                            # Try to find span.cell-value within the cell
                            value_span = cell.find_element(By.XPATH, ".//span[contains(@class, 'cell-value')]")
                            cell_text = value_span.text.strip()
                            cell_values.append(cell_text)
                        except:
                            # If no span found, try getting text from the cell div directly
                            cell_text = cell.text.strip()
                            cell_values.append(cell_text)

                    # Need at least 4 values: Order Type, Net Revenue, AOV, Count
                    if len(cell_values) < 4:
                        continue

                    order_type = cell_values[0]
                    net_revenue_text = cell_values[1]
                    aov_text = cell_values[2]
                    count_text = cell_values[3]

                    # Skip if order type is empty or looks like a header
                    if not order_type or order_type in ['Order Type', 'Net Revenue', 'AOV', '#']:
                        continue

                    # Filter: Only process allowed order types
                    if order_type not in ALLOWED_ORDER_TYPES:
                        print(f"Row {row_idx}: [{order_type}] - Skipped (not in allowed list)")
                        continue

                    print(f"Row {row_idx}: [{order_type}] [{net_revenue_text}] [{aov_text}] [{count_text}]")

                    # Parse the values
                    net_revenue = self.parse_number(net_revenue_text)
                    aov = self.parse_number(aov_text)
                    count = self.parse_number(count_text)

                    # Only add if we got actual numbers (not all zeros)
                    if net_revenue > 0 or aov > 0 or count > 0:
                        metrics[order_type] = {
                            'Net Revenue': net_revenue,
                            'AOV': aov,
                            'Count': count
                        }
                        print(f" Added: {order_type} - Revenue=�{net_revenue:.2f}, AOV=�{aov:.2f}, Count={count:.0f}")
                    else:
                        print(f"  Skipped (all zeros): {order_type}")

                except Exception as e:
                    print(f"Error processing row {row_idx}: {e}")
                    continue

            if len(metrics) > 0:
                print(f"\n Successfully extracted {len(metrics)} order type(s)")
            else:
                print("\n No valid data extracted")

            return metrics

        except Exception as e:
            print(f"Error extracting order type metrics: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def extract_summary_metrics(self):
        """Extract the summary metrics from the top of the page."""
        print("Extracting summary metrics...")

        try:
            # Look for summary cards/metrics at the top
            summary_selectors = [
                "//div[contains(@class, 'scorecard')]",
                "//div[contains(@class, 'metric')]",
                "//div[contains(@class, 'kpi')]"
            ]

            summary_metrics = {}

            # Try to find metric cards
            for selector in summary_selectors:
                try:
                    metric_cards = self.driver.find_elements(By.XPATH, selector)

                    for card in metric_cards:
                        try:
                            # Extract metric name and value
                            metric_text = card.text.strip()
                            lines = metric_text.split('\n')

                            if len(lines) >= 2:
                                metric_name = lines[0]
                                metric_value = self.parse_number(lines[1])
                                summary_metrics[metric_name] = metric_value
                                print(f"Summary metric: {metric_name} = {metric_value}")

                        except:
                            continue

                except:
                    continue

            return summary_metrics

        except Exception as e:
            print(f"Error extracting summary metrics: {e}")
            return {}


def get_date_input():
    """Get date from user or use previous day."""
    print("\nEnter date to extract (DD-MMM-YYYY, e.g., 13-Oct-2025)")
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


def setup_browser(report_url):
    """Setup browser and navigate to report."""
    print("\n[1/4] Opening browser...")
    # Use the same Chrome profile as the daily report script
    manager = BrowserManager(use_existing_chrome=False)
    driver = manager.start_browser()

    print("[2/4] Navigating to Order Type report...")
    driver.get(report_url)
    time.sleep(5)

    # Check if login needed
    if "accounts.google.com" in driver.current_url:
        print("\n�  Please login in the browser window...")
        while "accounts.google.com" in driver.current_url:
            time.sleep(2)
        print(" Login successful")

    return manager, driver


def display_results(date_str, order_type_metrics):
    """Display extracted data in a clean format."""
    print("\n" + "="*80)
    print(f"NET REVENUE & AOV PER ORDER TYPE - {date_str}".center(80))
    print("="*80)

    if order_type_metrics:
        print(f"\n{'Order Type':<25} {'Net Revenue':<15} {'AOV':<12} {'Count':<10}")
        print("-" * 65)

        for order_type, data in order_type_metrics.items():
            print(f"{order_type:<25} �{data['Net Revenue']:<14,.2f} �{data['AOV']:<11.2f} {data['Count']:<10,.0f}")
    else:
        print("\nNo data extracted.")

    print("\n" + "="*80)


def extract_marketing_spend(extractor, date_obj):
    """Extract Spend value from Marketing Deepdive page."""
    print("\nExtracting Marketing Spend...")

    try:
        # Wait for page to load
        time.sleep(3)

        # Look for the Spend metric card in the top section
        # Based on the screenshot, "Spend" is one of the metric cards
        print("Looking for Spend metric...")

        # Try to find elements containing "Spend" text
        spend_elements = extractor.driver.find_elements(By.XPATH, "//*[contains(text(), 'Spend')]")

        for elem in spend_elements:
            try:
                # Check if this is in the top metrics area (Y < 300)
                if elem.location['y'] < 300 and elem.is_displayed():
                    print(f"Found 'Spend' label at Y={elem.location['y']}")

                    # Get the parent container to find the value
                    parent = elem.find_element(By.XPATH, "./ancestor::*[contains(@class, 'scorecard') or contains(@class, 'metric')][1]")

                    # Get all text from the parent
                    parent_text = parent.text.strip()
                    lines = parent_text.split('\n')

                    print(f"Metric card text: {lines}")

                    # The value should be on the next line after "Spend"
                    for i, line in enumerate(lines):
                        if 'Spend' in line and i + 1 < len(lines):
                            spend_text = lines[i + 1]
                            spend_value = extractor.parse_number(spend_text)
                            print(f" Found Spend: {spend_text} = �{spend_value:,.2f}")
                            return spend_value

            except Exception as e:
                continue

        # Alternative method: Use JavaScript to find the Spend value
        print("Trying JavaScript extraction...")
        script = """
            let allElements = document.querySelectorAll('*');
            let spendLabel = null;
            let spendValue = null;

            // Find "Spend" label in top area
            for (let el of allElements) {
                let rect = el.getBoundingClientRect();
                let text = el.textContent.trim();

                if (rect.y < 300 && text === 'Spend' && el.children.length === 0) {
                    spendLabel = {text: text, x: rect.x, y: rect.y};

                    // Look for value near this label (similar X, slightly higher Y)
                    for (let val of allElements) {
                        let valRect = val.getBoundingClientRect();
                        let valText = val.textContent.trim();

                        if (Math.abs(valRect.x - rect.x) < 50 &&
                            valRect.y > rect.y &&
                            valRect.y < rect.y + 100 &&
                            valText.match(/[0-9]/) &&
                            val.children.length === 0) {
                            spendValue = valText;
                            break;
                        }
                    }
                    break;
                }
            }

            return {label: spendLabel, value: spendValue};
        """

        result = extractor.driver.execute_script(script)

        if result and result.get('value'):
            spend_text = result['value']
            spend_value = extractor.parse_number(spend_text)
            print(f" Found Spend via JavaScript: {spend_text} = �{spend_value:,.2f}")
            return spend_value

        print(" Could not find Spend metric")
        return 0.0

    except Exception as e:
        print(f"Error extracting Marketing Spend: {e}")
        import traceback
        traceback.print_exc()
        return 0.0


def extract_klaviyo_metrics(driver, parse_number_func):
    """Extract Klaviyo metrics from Converge attribution page (AG Grid) - Fixed virtualization issue."""
    print("\nExtracting Klaviyo metrics...")

    try:
        # Step 1: Build dynamic column mapping from headers
        print("Building column header mapping...")
        col_map = {}

        try:
            # Find all header cells
            headers = driver.find_elements(By.CSS_SELECTOR, ".ag-header-cell")
            print(f"Found {len(headers)} header columns")

            for header in headers:
                try:
                    col_id = header.get_attribute("col-id")
                    # Try multiple ways to get header text
                    header_text = header.text.strip().lower()

                    if not header_text:
                        # Try aria-label
                        header_text = header.get_attribute("aria-label")
                        if header_text:
                            header_text = header_text.strip().lower()

                    if col_id and header_text and header_text not in col_map:
                        col_map[header_text] = col_id
                        print(f"  Column: '{header_text}' -> col-id='{col_id}'")
                except Exception as e:
                    continue

            print(f" Built column map with {len(col_map)} columns")

        except Exception as e:
            print(f"  Could not build column map: {e}")
            col_map = {}

        # Step 2: Find Klaviyo row
        print("Looking for Klaviyo row in AG Grid table...")

        wait = WebDriverWait(driver, 10)
        row_id = None
        row_index = None

        try:
            pinned_row = wait.until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//div[contains(@class,'ag-pinned-left-cols-container')]"
                    "//div[@row-id and contains(translate(@row-id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'klaviyo')]"
                ))
            )
            row_id = pinned_row.get_attribute('row-id')
            row_index = pinned_row.get_attribute('row-index')
            print(f" Found Klaviyo row in pinned columns: row-id='{row_id}', row-index='{row_index}'")
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", pinned_row)
            except Exception:
                pass
        except Exception as e:
            print(f"  Note: could not find Klaviyo row in pinned columns: {e}")

        def ensure_row_visible():
            script = (
                "const rowId = arguments[0];\n"
                "const rowIndex = arguments[1];\n"
                "const root = document.querySelector('.ag-root');\n"
                "if (!root) { return; }\n"
                "const apis = [];\n"
                "const pushIf = api => { if (api && apis.indexOf(api) === -1) { apis.push(api); } };\n"
                "if (root.__agComponent && root.__agComponent.gridOptions && root.__agComponent.gridOptions.api) {\n"
                "    pushIf(root.__agComponent.gridOptions.api);\n"
                "}\n"
                "if (root.__agComponent && root.__agComponent.api) { pushIf(root.__agComponent.api); }\n"
                "if (root.gridOptions && root.gridOptions.api) { pushIf(root.gridOptions.api); }\n"
                "if (root.api) { pushIf(root.api); }\n"
                "if (window.gridOptions && window.gridOptions.api) { pushIf(window.gridOptions.api); }\n"
                "for (const api of apis) {\n"
                "    try {\n"
                "        if (rowId && typeof api.getRowNode === 'function') {\n"
                "            const node = api.getRowNode(rowId);\n"
                "            if (node) {\n"
                "                if (typeof api.ensureNodeVisible === 'function') {\n"
                "                    api.ensureNodeVisible(node, 'middle');\n"
                "                    return;\n"
                "                }\n"
                "                if (typeof api.ensureIndexVisible === 'function') {\n"
                "                    api.ensureIndexVisible(node.rowIndex, 'middle');\n"
                "                    return;\n"
                "                }\n"
                "            }\n"
                "        }\n"
                "        if (rowIndex !== null && typeof api.ensureIndexVisible === 'function') {\n"
                "            api.ensureIndexVisible(Number(rowIndex), 'middle');\n"
                "            return;\n"
                "        }\n"
                "    } catch (err) {\n"
                "        continue;\n"
                "    }\n"
                "}\n"
            )
            try:
                driver.execute_script(script, row_id, row_index)
            except Exception:
                pass

        ensure_row_visible()

        def get_center_row():
            try:
                return driver.find_element(
                    By.XPATH,
                    "//div[contains(@class,'ag-center-cols-container')]"
                    "//div[@row-id and contains(translate(@row-id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'klaviyo')]"
                )
            except Exception:
                return None

        klaviyo_row = get_center_row()
        if klaviyo_row is None:
            time.sleep(0.5)
            ensure_row_visible()
            time.sleep(0.5)
            klaviyo_row = get_center_row()

        if klaviyo_row is not None:
            if row_id is None:
                row_id = klaviyo_row.get_attribute('row-id')
            if row_index is None:
                row_index = klaviyo_row.get_attribute('row-index')

        if klaviyo_row is None:
            print("  Could not locate Klaviyo row in center columns")
            return {
                'purchases': 0.0,
                'nc': 0.0,
                'revenue': 0.0,
                'nc_revenue': 0.0
            }

        try:
            viewport = driver.find_element(By.CSS_SELECTOR, ".ag-center-cols-viewport")
            driver.execute_script("arguments[0].scrollLeft = arguments[0].scrollWidth;", viewport)
            time.sleep(0.5)
            driver.execute_script("arguments[0].scrollLeft = 0;", viewport)
            time.sleep(0.5)
        except Exception:
            pass

        def ensure_column_visible(col_id):
            script = (
                "const colId = arguments[0];\n"
                "const root = document.querySelector('.ag-root');\n"
                "if (!root) { return; }\n"
                "const apis = [];\n"
                "const pushIf = api => { if (api && apis.indexOf(api) === -1) { apis.push(api); } };\n"
                "if (root.__agComponent && root.__agComponent.gridOptions && root.__agComponent.gridOptions.api) { pushIf(root.__agComponent.gridOptions.api); }\n"
                "if (root.__agComponent && root.__agComponent.api) { pushIf(root.__agComponent.api); }\n"
                "if (root.gridOptions && root.gridOptions.api) { pushIf(root.gridOptions.api); }\n"
                "if (root.api) { pushIf(root.api); }\n"
                "if (window.gridOptions && window.gridOptions.api) { pushIf(window.gridOptions.api); }\n"
                "for (const api of apis) {\n"
                "    try {\n"
                "        if (typeof api.ensureColumnVisible === 'function') {\n"
                "            api.ensureColumnVisible(colId);\n"
                "            return;\n"
                "        }\n"
                "    } catch (err) {\n"
                "        continue;\n"
                "    }\n"
                "}\n"
            )
            try:
                driver.execute_script(script, col_id)
            except Exception:
                pass

        # Step 3: Helper function to find metric by header keywords
        def get_metric_value(keywords, metric_name):
            """Find cell value by matching header keywords."""

            for header_text, col_id in col_map.items():
                if any(keyword in header_text for keyword in keywords):
                    try:
                        ensure_row_visible()
                        ensure_column_visible(col_id)
                        time.sleep(0.2)
                        current_row = get_center_row()
                        if current_row is None:
                            continue

                        cell = current_row.find_element(By.CSS_SELECTOR, f"div[col-id='{col_id}']")
                        driver.execute_script("arguments[0].scrollIntoView({inline: 'center', block: 'center'});", cell)
                        time.sleep(0.2)

                        value_text = cell.text.strip()
                        if value_text and value_text != '-':
                            parsed = parse_number_func(value_text)
                            print(f" {metric_name} (header='{header_text}'): {value_text} = {parsed}")
                            return parsed
                    except Exception as e:
                        print(f"  Could not extract {metric_name} from col-id '{col_id}': {e}")
                        continue

            print(f"  Trying JavaScript direct extraction for {metric_name}...")
            try:
                script = (
                    "const rowId = arguments[0];\n"
                    "const colId = arguments[1];\n"
                    "const root = document.querySelector('.ag-root');\n"
                    "if (!root) { return null; }\n"
                    "const apis = [];\n"
                    "const pushIf = api => { if (api && apis.indexOf(api) === -1) { apis.push(api); } };\n"
                    "if (root.__agComponent && root.__agComponent.gridOptions && root.__agComponent.gridOptions.api) { pushIf(root.__agComponent.gridOptions.api); }\n"
                    "if (root.__agComponent && root.__agComponent.api) { pushIf(root.__agComponent.api); }\n"
                    "if (root.gridOptions && root.gridOptions.api) { pushIf(root.gridOptions.api); }\n"
                    "if (root.api) { pushIf(root.api); }\n"
                    "if (window.gridOptions && window.gridOptions.api) { pushIf(window.gridOptions.api); }\n"
                    "for (const api of apis) {\n"
                    "    try {\n"
                    "        if (typeof api.getRowNode === 'function') {\n"
                    "            const node = api.getRowNode(rowId);\n"
                    "            if (node && node.data && colId in node.data) {\n"
                    "                return node.data[colId];\n"
                    "            }\n"
                    "        }\n"
                    "    } catch (err) {\n"
                    "        continue;\n"
                    "    }\n"
                    "}\n"
                    "return null;\n"
                )

                for header_text, col_id in col_map.items():
                    if any(keyword in header_text for keyword in keywords):
                        try:
                            value = driver.execute_script(script, row_id, col_id)
                            if value is not None:
                                parsed = parse_number_func(str(value))
                                print(f" {metric_name} (via JS API, header='{header_text}'): {value} = {parsed}")
                                return parsed
                        except Exception:
                            continue
            except Exception as e:
                print(f"  JS API extraction failed: {e}")

            try:
                ensure_row_visible()
                current_row = get_center_row()
                if current_row is None:
                    raise RuntimeError('Row not visible')
                all_cells = current_row.find_elements(By.XPATH, ".//div[@role='gridcell']")
                print(f"  Scanning {len(all_cells)} visible cells for {metric_name}...")

                for idx, cell in enumerate(all_cells):
                    col_id = cell.get_attribute('col-id')
                    cell_text = cell.text.strip()
                    if col_id and cell_text and cell_text != '-':
                        print(f"    Cell {idx}: col-id='{col_id}', value='{cell_text}'")
            except Exception as e:
                print(f"  Error scanning cells for {metric_name}: {e}")

            print(f"    Could not find {metric_name}")
            return 0.0


        # Step 4: Extract all metrics using dynamic detection
        purchases = get_metric_value(['purchase', 'purchases', 'orders'], 'Purchases')
        nc = get_metric_value(['nc', 'new customer', 'new_customer'], 'NC')
        revenue = get_metric_value(['revenue', 'total revenue'], 'Revenue')
        nc_revenue = get_metric_value([
            'nc revenue',
            'new customer revenue',
            'new customers revenue',
            'nc_revenue'
        ], 'NC Revenue')

        def to_float(value):
            try:
                return round(float(value), 2)
            except Exception:
                return 0.0

        return {
            'purchases': to_float(purchases),
            'nc': to_float(nc),
            'revenue': to_float(revenue),
            'nc_revenue': to_float(nc_revenue)
        }


    except Exception as e:
        print(f"Error extracting Klaviyo metrics: {e}")
        import traceback
        traceback.print_exc()
        return {
            'purchases': 0.0,
            'nc': 0.0,
            'revenue': 0.0,
            'nc_revenue': 0.0
        }


def main():
    """Main execution."""
    ORDER_TYPE_URL = "https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/h0vQC"
    MARKETING_URL = "https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/M05qB"

    print("="*80)
    print("LOOKER STUDIO & CONVERGE DATA EXTRACTION".center(80))
    print("="*80)

    # Get date
    date_obj, date_str = get_date_input()

    # Store extracted data
    extracted_data = {
        'date': date_str,
        'order_type_metrics': {},
        'marketing_spend': 0.0,
        'klaviyo_metrics': {}
    }

    # ============================================================================
    # STEP 1: ORDER TYPE REPORT
    # ============================================================================

    # Setup browser - start with Order Type page
    print("\n" + "="*80)
    print("STEP 1: ORDER TYPE REPORT".center(80))
    print("="*80)

    print("\n[1/10] Opening browser...")
    manager = BrowserManager(use_existing_chrome=False)
    driver = manager.start_browser()

    print("[2/10] Navigating to Order Type report...")
    driver.get(ORDER_TYPE_URL)
    time.sleep(5)

    # Check if login needed
    if "accounts.google.com" in driver.current_url:
        print("\n  Please login in the browser window...")
        while "accounts.google.com" in driver.current_url:
            time.sleep(2)
        print(" Login successful")

    extractor = OrderTypeDataExtractor(driver)

    try:
        # Extract Order Type data
        print("[3/10] Setting date range...")
        extractor.set_date_range(date_obj, date_obj)

        print("[4/10] Extracting Order Type data...")
        order_type_metrics = extractor.extract_order_type_metrics()
        extracted_data['order_type_metrics'] = order_type_metrics

        # Display Order Type results
        display_results(date_str, order_type_metrics)

        # ============================================================================
        # STEP 2: MARKETING DEEPDIVE - SPEND
        # ============================================================================

        print("\n" + "="*80)
        print("STEP 2: MARKETING DEEPDIVE - SPEND".center(80))
        print("="*80)

        print("\n[5/10] Navigating to Marketing Deepdive page...")
        driver.get(MARKETING_URL)
        time.sleep(5)

        print("[6/10] Setting date range...")
        extractor.set_date_range(date_obj, date_obj)

        print("[7/10] Extracting Marketing Spend...")
        marketing_spend = extract_marketing_spend(extractor, date_obj)
        extracted_data['marketing_spend'] = marketing_spend

        print(f"\n Marketing Spend: �{marketing_spend:,.2f}")

        # ============================================================================
        # STEP 3: CONVERGE ATTRIBUTION - KLAVIYO METRICS
        # ============================================================================

        print("\n" + "="*80)
        print("STEP 3: CONVERGE ATTRIBUTION - KLAVIYO METRICS".center(80))
        print("="*80)

        # Build dynamic Converge URL with date
        date_str_url = date_obj.strftime('%Y-%m-%d')  # Format: 2025-10-14
        CONVERGE_URL = f"https://app.runconverge.com/smyle-7267/attribution/channels#since={date_str_url}&until={date_str_url}"

        print(f"\n[8/10] Navigating to Converge Attribution page...")
        print(f"URL: {CONVERGE_URL}")
        driver.get(CONVERGE_URL)
        time.sleep(5)

        # Check if login needed for Converge
        if "login" in driver.current_url.lower() or "sign" in driver.current_url.lower():
            print("\n  Please login to Converge in the browser window...")
            print("Waiting for login...")
            # Wait for user to login
            while "login" in driver.current_url.lower() or "sign" in driver.current_url.lower():
                time.sleep(2)
            print(" Login successful")

        # IMPORTANT: Open URL twice to get correct data
        print("[9/10] Loading page first time...")
        driver.get(CONVERGE_URL)
        time.sleep(5)

        print("Reloading page to get fresh data...")
        driver.get(CONVERGE_URL)
        time.sleep(8)  # Wait longer for data to fully load

        print("[10/10] Extracting Klaviyo metrics...")
        klaviyo_metrics = extract_klaviyo_metrics(driver, extractor.parse_number)
        extracted_data['klaviyo_metrics'] = klaviyo_metrics

        # ============================================================================
        # FINAL RESULTS SUMMARY
        # ============================================================================

        print("\n" + "="*80)
        print("COMPLETE EXTRACTION RESULTS".center(80))
        print("="*80)
        print(f"\nDate: {date_str}\n")

        print("ORDER TYPE METRICS:")
        if order_type_metrics:
            for order_type, data in order_type_metrics.items():
                print(f"   {order_type}:")
                print(f"      Net Revenue: �{data['Net Revenue']:,.2f}")
                print(f"      AOV: �{data['AOV']:,.2f}")
                print(f"      Count: {data['Count']:,.0f}")
        else:
            print("  No data extracted")

        print(f"\nMARKETING SPEND:")
        print(f"   Spend: {marketing_spend:,.2f}")

        print(f"\nKLAVIYO METRICS:")
        print(f"   Purchases: {klaviyo_metrics.get('purchases', 0)}")
        print(f"   NC (New Customers): {klaviyo_metrics.get('nc', 0)}")
        print(f"   Revenue: {klaviyo_metrics.get('revenue', 0):,.2f}")
        print(f"   NC Revenue: {klaviyo_metrics.get('nc_revenue', 0):,.2f}")

        print("\n" + "="*80)

        # ============================================================================
        # SAVE TO GOOGLE SHEETS
        # ============================================================================

        print("\n" + "="*80)
        print("SAVING TO GOOGLE SHEETS".center(80))
        print("="*80)

        try:
            from dotenv import load_dotenv
            from services.sheets.helpers import write_order_type_data

            # Load environment variables
            load_dotenv()

            write_order_type_data(
                date_obj,
                order_type_metrics,
                marketing_spend,
                klaviyo_metrics
            )

        except Exception as e:
            print(f"\n Failed to save to Google Sheets: {e}")
            print("\nTroubleshooting:")
            print("  1. Check that auth.json exists in project root")
            print("  2. Check that Google Sheet is shared with service account")
            print("  3. Verify the spreadsheet URL and worksheet ID are correct")
            import traceback
            traceback.print_exc()

        print("\n" + "="*80)

        print("\nExtraction complete!")
        print("\nPress Enter to close browser...")
        input()

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

    finally:
        manager.close()

    return extracted_data


if __name__ == "__main__":
    main()
