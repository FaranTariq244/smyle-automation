# Add Tracker — Atria Integration Documentation

## Overview

The "Add Tracker" feature automates the extraction of Facebook ad campaign landing page metrics from **Atria** (`app.tryatria.com`) and writes them into **Google Sheets** (DAILY worksheets).

## End-to-End Flow

```
Atria Analytics Reports (2 reports)
    ↓  Selenium browser automation
Extract landing page metrics (AG Grid / HTML table)
    ↓  URL matching + column mapping
Google Sheets (DAILY worksheets — one tab per landing page)
```

**Entry point:** User clicks "Add Tracker" button in the web UI, or it runs on a schedule. This triggers `run_add_tracker_report()` in `atria_data_extractor.py`.

---

## What is Atria?

Atria is a web-based analytics platform that reports on Facebook ad campaign landing page performance. It aggregates metrics from Facebook's ad platform into dashboards/reports.

### Atria Report URLs

- **Report 1 (Top Performing Landing Pages - REO):**
  `https://app.tryatria.com/workspace/analytics/facebook/a4ca9167fc2446bba1aa5981bbabc254/report/f6e0ee4a234d4d749aa98049eafc5d72`

- **Report 2 (Secondary Report):**
  `https://app.tryatria.com/workspace/analytics/facebook/7c70f57b653f41c0a081781619884f33/report/97eca273cb9e431db7d90271eb87f047`

---

## Step-by-Step Process

### Step 1: Launch Browser

- Opens headless Chrome with a saved user profile (cached Atria credentials).
- Managed by `browser_manager.py`.

### Step 2: Navigate to Atria Report

- Goes to one of the two hardcoded report URLs.
- **File:** `atria_data_extractor.py`, lines 34–49.

### Step 3: Login Check

- Checks if the Atria login page is displayed.
- If login is needed, restarts browser in **visible mode** so the user can log in manually.
- Waits up to **5 minutes** for login to complete.
- **File:** `atria_data_extractor.py`, lines 51–80.

### Step 4: Set Date Range

- Uses the **Ant Design date picker** (`.ant-picker.ant-picker-range`).
- Clicks the same date **twice** to set a single-day range.
- Navigates months using next/prev buttons if needed.
- **File:** `atria_data_extractor.py`, lines 82–335.

### Step 5: Apply Campaign Filter

- Opens the dimension filter.
- Configures: **"Campaign name does not contain 'aware'"**.
- Clicks the Apply button.
- **File:** `atria_data_extractor.py`, lines 568–849.

### Step 6: Wait for Data to Load

- Polls for loading indicators (`.ant-spin`, "Loading" text) to disappear.
- Timeout: **15 seconds**.
- **File:** `atria_data_extractor.py`, lines 851–905.

### Step 7: Extract Data

Three fallback extraction strategies:

1. **AG Grid extraction (primary)** — Detects `.ag-root` component, handles virtual scrolling by scrolling the grid to load all rows, collects headers at multiple scroll positions (left, middle, right), maps cells by `col-id` attribute, handles pinned columns (left/center/right containers). **Lines 1023–1405.**

2. **HTML table extraction (fallback)** — Searches for `<table>` elements, extracts `<thead>` for headers and `<tbody>` for rows, scores tables by quality (row count, header count, visibility), handles URL extraction from `<a>` tags. **Lines 1407–1765.**

3. **Card-based extraction (last resort)** — For non-table/grid layouts displayed as cards, extracts URLs from anchor tags, finds ROAS values via regex. **Lines 907–1021.**

### Step 8: Write Data to Google Sheets

- Handled by `services/sheets/add_tracker_helpers.py`.
- See "Data Writing to Google Sheets" section below.

### Step 9: Close Browser

- Quits the Selenium browser and cleans up resources.

---

## Data Extracted from Atria

| Atria Column           | Google Sheet Column | Example Value |
|------------------------|---------------------|---------------|
| Landing page (URL)     | *(used for matching sheet tabs)* | `wesmyle.com/nl/pages/starter-kit` |
| Landing page views     | Sessions            | `1,234` |
| Spend                  | Spend               | `€947.52` |
| Link clicks            | Clicks              | `667` |
| ATC                    | Add to cart          | `89` |
| Checkouts Initiated    | Started checkout     | `45` |
| Purchases              | Purchase             | `23` |
| ROAS                   | ROAS                | `3.01` |
| AOV                    | AOV                 | `€73.01` |
| CPM                    | CPM                 | `€10.17` |
| CPC (link click)       | CPC                 | `€1.42` |
| CTR (link click)       | CTR                 | `0.71%` |
| Cost per landing       | *(not mapped)*       | `€0.77` |

### Value Parsing

Values are parsed from formatted Atria strings to numbers:

- Currency removed: `€1,234.56` → `1234.56`
- Percentage removed: `0.71%` → `0.71`
- Thousands shorthand: `€1.2K` → `1200`
- Millions shorthand: `€2.5M` → `2500000`
- Comma separators: `1,234` → `1234`

---

## Data Writing to Google Sheets

### Sheet Configuration

- **Setting Key:** `DAILY_ADD_TRACKER_SHEET_URL`
- **Configured via:** Settings page in web UI → "Daily Add Tracker Sheet" input
- **Format:** `https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid={SHEET_ID}`

### Sheet Structure

Each worksheet (tab) represents **one landing page/product**:

- **Cell A1–A10:** Contains the landing page URL/domain (e.g., `wesmyle.com/nl/pages/starter-kit-ebrush-sub`)
- **Header Row:** Around row 20+, contains column names: Clicks, Add to cart, Purchase, ROAS, AOV, CPM, CPC, CTR, Spend, Started checkout
- **Date Column:** Column A, contains dates (e.g., `30 November`, `1 December`, `05/December/2026`)
- **Data Rows:** Below headers, one row per date

### Column Mapping (Atria → Sheet)

```python
COLUMN_MAPPINGS = [
    ColumnMapping("Landing page views",    "Sessions"),
    ColumnMapping("Spend",                 "Spend"),
    ColumnMapping("Link clicks",           "Clicks"),
    ColumnMapping("ATC",                   "Add to cart"),
    ColumnMapping("Checkouts Initiated",   "Started checkout"),
    ColumnMapping("Purchases",             "Purchase"),
    ColumnMapping("ROAS",                  "ROAS"),
    ColumnMapping("AOV",                   "AOV"),
    ColumnMapping("CPM",                   "CPM"),
    ColumnMapping("CPC (link click)",      "CPC"),
    ColumnMapping("CTR (link click)",      "CTR"),
]
```

### Writing Process

1. **Open spreadsheet** via Google Sheets API using the configured URL.
2. **Iterate each worksheet** (tab) — each tab is one landing page.
3. **Read URL from cell A1–A10** of each tab.
4. **Match the sheet URL** against extracted Atria data rows using priority matching:
   - Exact URL match
   - Query parameter compatibility (e.g., same `?currency=GBP`)
   - Partial base URL match (ignoring query params)
   - Path + country match (e.g., `/pages/starter-kit` + `nl`)
5. **Find the header row** (must contain 5+ known metric column names).
6. **Find the date row** (match day + month in column A).
7. **Map columns** using `COLUMN_MAPPINGS`.
8. **Write values** via `worksheet.batch_update()` with `USER_ENTERED` mode.

### URL Matching Logic

**Country extraction:**
- From TLD: `wesmyle.de` → `de`
- From path: `wesmyle.com/nl/` → `nl`

**URL normalization:**
- Remove protocol (`https://`, `http://`)
- Remove `www.`
- Remove trailing slashes
- Strip query parameters for base matching

---

## Selenium Selectors Reference

### Date Picker
| Selector | Purpose |
|----------|---------|
| `.ant-picker.ant-picker-range` | Range date picker container |
| `input[date-range='start']` | Start date input |
| `.ant-picker-cell .ant-picker-cell-inner` | Day cells in calendar |
| `.ant-picker-header-view` | Month/year header |
| `.ant-picker-month-btn`, `.ant-picker-year-btn` | Month/year buttons |
| `.ant-picker-header-next-btn` | Next month button (>) |
| `.ant-picker-header-prev-btn` | Previous month button (<) |

### Filter Controls
| Selector | Purpose |
|----------|---------|
| `//button` | Generic button search |
| `//*[contains(@class, 'filter')]` | Filter icon/button |
| `.ant-select` | Ant Design dropdown |
| `//button[contains(text(), 'Apply')]` | Apply button |
| `input[type='text']` | Text input for filter value |

### Data Grid (AG Grid)
| Selector | Purpose |
|----------|---------|
| `.ag-root` | AG Grid container |
| `.ag-center-cols-container .ag-row` | Data rows |
| `.ag-header-cell` | Column headers |
| `.ag-cell[col-id]` | Individual cells with column ID |
| `.ag-body-viewport` | Scrollable area |
| `.ag-pinned-left-cols-container` | Pinned left columns |
| `.ag-pinned-right-cols-container` | Pinned right columns |

### Table (Fallback)
| Selector | Purpose |
|----------|---------|
| `table`, `.ant-table`, `//table` | Table elements |
| `thead`, `tbody` | Table sections |
| `tr`, `th`, `td` | Rows and cells |
| `a[href]` | Anchor tags for URLs |

### Wait / Loading
| Selector | Purpose |
|----------|---------|
| `//*[contains(text(), 'Loading')]` | Loading text |
| `.ant-spin` | Ant Design spinner |
| `.loading` | Generic loading class |

---

## Key Files

| File | Purpose |
|------|---------|
| `atria_data_extractor.py` | All Selenium automation — navigation, date picking, filtering, data extraction |
| `services/sheets/add_tracker_helpers.py` | Google Sheets writing — URL matching, column mapping, date/header finding |
| `web_app.py` | Flask route `/api/run` with task `"addtracker"` |
| `templates/index.html` | UI button (line 1121) + settings input (line 1371) for sheet URL |
| `run_all_reports.py` | Orchestration wrapper |
| `browser_manager.py` | Chrome browser lifecycle management |

---

## Execution Trace

| Step | Function | File | Action |
|------|----------|------|--------|
| 1 | `run_add_tracker_report()` | `atria_data_extractor.py` | Main orchestrator |
| 2 | `BrowserManager.start_browser()` | `browser_manager.py` | Launch headless Chrome |
| 3 | `AtriaDataExtractor.__init__()` | `atria_data_extractor.py` | Initialize extractor |
| 4 | `navigate_to_report(1)` | `atria_data_extractor.py` | Go to Report 1 URL |
| 5 | `check_and_wait_for_login()` | `atria_data_extractor.py` | Poll login page |
| 6 | `set_date()` | `atria_data_extractor.py` | Set date in Ant picker |
| 7 | `_select_date_in_calendar()` | `atria_data_extractor.py` | Click date twice |
| 8 | `apply_dimension_filter()` | `atria_data_extractor.py` | Click filter button |
| 9 | `_configure_filter()` | `atria_data_extractor.py` | Set dropdowns/text |
| 10 | `wait_for_data_load()` | `atria_data_extractor.py` | Poll loading indicators |
| 11 | `extract_table_data()` | `atria_data_extractor.py` | Scrape data grid |
| 12 | `_extract_ag_grid_data()` | `atria_data_extractor.py` | Parse AG Grid rows/cells |
| 13 | `display_data()` | `atria_data_extractor.py` | Log extracted data |
| 14 | `write_atria_data_to_sheets()` | `add_tracker_helpers.py` | Write to Google Sheets |
| 15 | `get_daily_add_tracker_spreadsheet()` | `add_tracker_helpers.py` | Open Sheets via API |
| 16 | `get_daily_worksheets()` | `add_tracker_helpers.py` | Find DAILY tabs |
| 17 | `find_url_in_sheet()` | `add_tracker_helpers.py` | Read URL from A1–A10 |
| 18 | `match_atria_data_to_url()` | `add_tracker_helpers.py` | Find matching data row |
| 19 | `find_header_row()` | `add_tracker_helpers.py` | Locate headers in sheet |
| 20 | `find_date_row()` | `add_tracker_helpers.py` | Locate date row in sheet |
| 21 | `map_atria_to_sheet_columns()` | `add_tracker_helpers.py` | Apply column mappings |
| 22 | `write_data_to_daily_sheet()` | `add_tracker_helpers.py` | Update cells via API |
| 23 | `BrowserManager.close()` | `browser_manager.py` | Quit browser |

---

## Hardcoded Configuration

| Setting | Value |
|---------|-------|
| Report 1 URL | `https://app.tryatria.com/workspace/analytics/facebook/a4ca9167fc2446bba1aa5981bbabc254/report/f6e0ee4a234d4d749aa98049eafc5d72` |
| Report 2 URL | `https://app.tryatria.com/workspace/analytics/facebook/7c70f57b653f41c0a081781619884f33/report/97eca273cb9e431db7d90271eb87f047` |
| Campaign filter | Excludes campaigns containing `"aware"` |
| Login timeout | 5 minutes |
| Data load timeout | 15 seconds |
