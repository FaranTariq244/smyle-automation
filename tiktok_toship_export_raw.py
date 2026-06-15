"""
RAW proof-of-concept: TikTok Seller Center "To ship" order export.

Workflow (first candidate for the new "Workflows" dashboard section):
    1. Open seller-nl.tiktok.com Manage orders > To ship
    2. Click Export
    3. In the Export orders dialog: select "All orders awaiting shipping",
       format CSV, click Export
    4. Poll Export History until the new report appears (TikTok takes ~2 min)
    5. Click Download on the new report
    6. Read the downloaded CSV and print the extracted data to the log

Uses the same persistent chrome_profile as the other extractors. First run
requires a one-time manual login to TikTok Seller Center in the opened
browser window; the session persists afterwards.

Usage:
    python tiktok_toship_export_raw.py              # visible browser
    python tiktok_toship_export_raw.py --headless   # after login is saved
"""

import csv
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By

from browser_manager import BrowserManager

PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOAD_DIR = PROJECT_ROOT / "reports" / "tiktok_exports"
LOG_DIR = PROJECT_ROOT / "logs"

ORDERS_URL = "https://seller-nl.tiktok.com/order?order_status[]=1&selected_sort=1&tab=to_ship"
FULFILMENT_ORDERS_URL = "https://www.my-fulfilment.com/orders/"
SHIP_UPLOAD_URL = "https://seller-nl.tiktok.com/order/seller-shipping/upload?shop_region=NL"

# Fulfilment "Shipper" -> TikTok template "Shipping provider name" (must
# exactly match the template's shipping_provider_name_drop_lis sheet).
CARRIER_MAP = {
    "POSTNL": "PostNL Netherlands",
    "GLS": "GLS Netherlands",
    "DHL": "DHL_Netherlands",
    "DPD": "DPD Netherlands",
    "UPS": "UPS Netherlands",
    "PEDDLER": "Peddler Netherlands",
}


def tiktok_provider(shipper):
    """Map a my-fulfilment.com "Shipper" (e.g. 'PostNL') to TikTok's provider
    name (e.g. 'PostNL Netherlands'). Matches on the leading word."""
    key = (shipper or "").strip().upper().split()[0] if shipper else ""
    return CARRIER_MAP.get(key, shipper)

LOGIN_TIMEOUT = 600          # max seconds to wait for manual login
EXPORT_READY_TIMEOUT = 360   # max seconds to wait for TikTok to generate the report
DOWNLOAD_TIMEOUT = 120       # max seconds to wait for the file to land on disk

log = logging.getLogger("tiktok_toship")


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / "tiktok_toship_export.log", encoding="utf-8"),
        ],
    )


def log_phase(title):
    """Print a clear, scannable phase banner to the log."""
    log.info("")
    log.info("==================== %s ====================", title)


def screenshot(driver, name):
    path = LOG_DIR / f"tiktok_{name}_{datetime.now():%H%M%S}.png"
    try:
        driver.save_screenshot(str(path))
        log.info("Screenshot saved: %s", path)
    except WebDriverException:
        pass


def find_visible(driver, xpath):
    """Return the first displayed element matching xpath, else None."""
    for el in driver.find_elements(By.XPATH, xpath):
        try:
            if el.is_displayed():
                return el
        except WebDriverException:
            continue
    return None


def wait_for(driver, xpath, timeout, what):
    log.info("Waiting for %s ...", what)
    deadline = time.time() + timeout
    while time.time() < deadline:
        el = find_visible(driver, xpath)
        if el:
            return el
        time.sleep(2)
    screenshot(driver, f"timeout_{what.replace(' ', '_')}")
    raise TimeoutError(f"Timed out waiting for {what}")


def js_click(driver, el):
    """Click via JS — immune to overlay/scroll interception in the drawer."""
    driver.execute_script("arguments[0].scrollIntoView({block:'center'}); arguments[0].click();", el)


def ensure_logged_in(driver, headless):
    driver.get(ORDERS_URL)
    time.sleep(10)
    if "/account/login" not in driver.current_url:
        log.info("Already logged in to Seller Center.")
        return
    if headless:
        raise RuntimeError(
            "Not logged in to TikTok Seller Center and running headless — "
            "run once WITHOUT --headless to log in manually; the session is "
            "then saved to chrome_profile and headless runs will work."
        )
    log.warning("=" * 60)
    log.warning("MANUAL LOGIN REQUIRED — please log in to TikTok Seller")
    log.warning("Center in the opened Chrome window (waiting up to %d min).", LOGIN_TIMEOUT // 60)
    log.warning("=" * 60)
    deadline = time.time() + LOGIN_TIMEOUT
    while time.time() < deadline:
        if "/order" in driver.current_url and "/account/login" not in driver.current_url:
            log.info("Login detected, session saved to chrome_profile.")
            time.sleep(5)
            return
        time.sleep(3)
    raise TimeoutError("Manual login was not completed in time.")


def first_history_entry(driver):
    """Return the report name of the top Export History row (or None)."""
    el = find_visible(
        driver,
        "//*[contains(normalize-space(.), 'Export History')]"
        "/following::*[contains(normalize-space(text()), '.csv') or contains(normalize-space(text()), '.xlsx')][1]",
    )
    return el.text.strip() if el else None


def run_export(driver):
    """Steps 2-3: open the Export dialog, pick options, start the export.

    Returns the report name that was at the top of Export History before
    the new export, so the caller can detect the new entry.
    """
    export_btn = wait_for(
        driver,
        "//button[.//text()[normalize-space()='Export']]",
        60,
        "Export button on orders page",
    )
    js_click(driver, export_btn)
    log.info("Clicked Export — waiting for the Export orders dialog.")

    wait_for(
        driver,
        "//*[contains(normalize-space(text()), 'Export orders')]",
        30,
        "Export orders dialog",
    )
    time.sleep(2)
    previous_top = first_history_entry(driver)
    log.info("Top of Export History before export: %s", previous_top)

    awaiting = wait_for(
        driver,
        "//*[contains(normalize-space(text()), 'All orders awaiting shipping')]",
        20,
        "'All orders awaiting shipping' option",
    )
    js_click(driver, awaiting)
    log.info("Selected: All orders awaiting shipping")

    csv_opt = wait_for(
        driver,
        "//*[normalize-space(text())='CSV']",
        20,
        "CSV format option",
    )
    js_click(driver, csv_opt)
    log.info("Selected format: CSV")
    time.sleep(1)

    # The dialog's Export button is the last visible one (the page header
    # also has an Export button behind the drawer overlay).
    buttons = [
        el for el in driver.find_elements(
            By.XPATH, "//button[.//text()[normalize-space()='Export']]")
        if el.is_displayed()
    ]
    if not buttons:
        screenshot(driver, "no_dialog_export_btn")
        raise RuntimeError("Export button inside the dialog not found")
    js_click(driver, buttons[-1])
    log.info("Clicked Export in the dialog — TikTok is generating the report.")
    return previous_top


def wait_for_new_report(driver, previous_top):
    """Step 4: poll Export History until a new entry appears at the top."""
    deadline = time.time() + EXPORT_READY_TIMEOUT
    while time.time() < deadline:
        time.sleep(10)
        top = first_history_entry(driver)
        log.info("Export History top entry: %s", top)
        if top and top != previous_top:
            log.info("New report detected: %s", top)
            return top
    screenshot(driver, "export_never_appeared")
    raise TimeoutError("New export never appeared in Export History")


def download_report(driver, report_name):
    """Step 5: click Download on the row of the given report; return the file path."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    driver.execute_cdp_cmd(
        "Browser.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(DOWNLOAD_DIR)},
    )
    before = {p.name for p in DOWNLOAD_DIR.glob("*")}

    # The Download button only appears in the report's row once TikTok
    # finishes generating the file. Climb the DOM from the report-name cell
    # to its own row and accept only a button inside that row — a sibling
    # row's button (e.g. the previous, already-finished report) must never
    # match, or we'd silently download stale data.
    find_row_button_js = """
        const name = arguments[0];
        const leaves = [...document.querySelectorAll('*')]
            .filter(e => e.children.length === 0 && e.textContent.trim() === name);
        for (const leaf of leaves) {
            let node = leaf;
            while (node && node !== document.body) {
                const btns = [...node.querySelectorAll('button')]
                    .filter(b => b.textContent.includes('Download'));
                const reports = (node.textContent.match(/\\.csv|\\.xlsx/g) || []).length;
                if (reports > 1 || btns.length > 1) break;  // climbed past the row
                if (btns.length === 1) return btns[0];
                node = node.parentElement;
            }
        }
        return null;
    """
    log.info("Waiting for Download button in the row of '%s' ...", report_name)
    deadline = time.time() + EXPORT_READY_TIMEOUT
    download_btn = None
    while time.time() < deadline:
        download_btn = driver.execute_script(find_row_button_js, report_name)
        if download_btn:
            break
        time.sleep(5)
    if not download_btn:
        screenshot(driver, "no_row_download_btn")
        raise TimeoutError(f"Download button for '{report_name}' never appeared")
    js_click(driver, download_btn)
    log.info("Clicked Download — waiting for the file.")

    deadline = time.time() + DOWNLOAD_TIMEOUT
    while time.time() < deadline:
        new_files = [
            p for p in DOWNLOAD_DIR.glob("*")
            if p.name not in before and not p.name.endswith(".crdownload")
        ]
        if new_files and not list(DOWNLOAD_DIR.glob("*.crdownload")):
            path = max(new_files, key=lambda p: p.stat().st_mtime)
            log.info("Downloaded: %s (%d bytes)", path, path.stat().st_size)
            return path
        time.sleep(2)
    raise TimeoutError("Download did not complete in time")


def extract_orders(path):
    """Step 6: read the CSV and return the order records."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        log.warning("CSV is empty: %s", path)
        return []

    header, data = rows[0], rows[1:]
    records = [
        # TikTok pads IDs with a trailing tab (anti-Excel-mangling) — strip it
        {k.strip(): v.strip() for k, v in zip(header, row) if v.strip()}
        for row in data
    ]
    order_ids = list(dict.fromkeys(r["Order ID"] for r in records if r.get("Order ID")))
    log.info("Export OK: %d row(s) -> %d unique order(s) [%s]",
             len(records), len(order_ids), path.name)
    return records


def match_orders_in_shopify(records):
    """Phase 2: find each TikTok order in Shopify via its TikTokOrderID tag."""
    from services.shopify import client as shopify

    order_ids = list(dict.fromkeys(r["Order ID"] for r in records if r.get("Order ID")))
    log_phase("PHASE 2/3: Match orders in Shopify")
    log.info("Looking up %d TikTok order ID(s) in Shopify by TikTokOrderID tag ...",
             len(order_ids))

    found, missing = [], []
    for oid in order_ids:
        try:
            order = shopify.find_order_by_tiktok_id(oid)
        except Exception as exc:
            log.error("  %s -> Shopify lookup failed: %s", oid, exc)
            missing.append(oid)
            continue
        if order:
            found.append({"tiktok_id": oid, "shopify_order": order})
            log.info("  %s -> %s (%s / %s)", oid, order["name"],
                     order["displayFinancialStatus"], order["displayFulfillmentStatus"])
        else:
            missing.append(oid)
            log.info("  %s -> NOT FOUND in Shopify", oid)

    log.info("Result: %d matched in Shopify, %d not found (of %d).",
             len(found), len(missing), len(order_ids))
    if missing:
        log.info("Not in Shopify (likely not synced yet): %s", ", ".join(missing))
    return found, missing


FULFILMENT_LOGIN_URL = "https://www.my-fulfilment.com/login"


def _load_fulfilment_creds():
    """Read my-fulfilment.com login from env vars or fulfilment.env."""
    email = os.getenv("MYFULFILMENT_EMAIL")
    password = os.getenv("MYFULFILMENT_PASSWORD")
    env_file = PROJECT_ROOT / "fulfilment.env"
    if (not email or not password) and env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == "MYFULFILMENT_EMAIL" and not email:
                    email = v.strip()
                elif k.strip() == "MYFULFILMENT_PASSWORD" and not password:
                    password = v.strip()
    return email, password


def _fulfilment_session_ok(driver):
    """True if the orders page is loaded with a valid session."""
    try:
        body = driver.find_element(By.TAG_NAME, "body").text
    except WebDriverException:
        body = ""
    if "HTTP ERROR 5" in body or "isn't working" in body:
        return False  # stale session is served as a 500 by the portal
    if "/login" in driver.current_url.lower():
        return False
    if find_visible(driver, "//input[@type='password']") is not None:
        return False
    return True


def _fulfilment_auto_login(driver):
    """Fill the login form from creds, tick 'Remember me', submit."""
    from selenium.webdriver.common.keys import Keys

    email, password = _load_fulfilment_creds()
    if not email or not password:
        raise RuntimeError(
            "my-fulfilment.com session expired and no credentials available. "
            "Set MYFULFILMENT_EMAIL / MYFULFILMENT_PASSWORD in fulfilment.env."
        )

    _load_fulfilment_page(driver, FULFILMENT_LOGIN_URL)
    time.sleep(3)

    email_in = find_visible(driver, "//input[@type='email']") or \
        find_visible(driver, "//input[@type='text']")
    pass_in = find_visible(driver, "//input[@type='password']")
    if not email_in or not pass_in:
        screenshot(driver, "fulfilment_login_no_fields")
        raise RuntimeError("Could not locate my-fulfilment.com login fields")

    # JS-clear first: the browser autofills these, and typing on top would
    # duplicate the value (e.g. 'a@b.coma@b.com').
    for el, val in ((email_in, email), (pass_in, password)):
        driver.execute_script("arguments[0].value='';", el)
        el.click()
        el.send_keys(Keys.CONTROL, "a")
        el.send_keys(Keys.DELETE)
        el.send_keys(val)

    # Tick "Remember me" so the session persists and we avoid re-logging in.
    remember = find_visible(driver, "//input[@type='checkbox']")
    if remember and not remember.is_selected():
        try:
            js_click(driver, remember)
        except WebDriverException:
            pass
    if remember and remember.is_selected():
        log.info("'Remember me' checked.")

    submit = find_visible(driver, "//button[@type='submit']") or \
        find_visible(driver, "//button[contains(translate(., 'LOGIN', 'login'), 'login')]") or \
        find_visible(driver, "//input[@type='submit']")
    if submit:
        js_click(driver, submit)
    else:
        pass_in.send_keys(Keys.RETURN)
    log.info("Submitted my-fulfilment.com login for %s", email)
    time.sleep(6)


def ensure_fulfilment_logged_in(driver, headless):
    """Ensure a valid my-fulfilment.com session, auto-logging in if needed.

    The portal serves a stale session as an HTTP 500 on /orders/ rather than
    a clean login redirect, so we detect that and re-authenticate from creds.
    """
    _load_fulfilment_page(driver, FULFILMENT_ORDERS_URL)
    time.sleep(5)

    if _fulfilment_session_ok(driver):
        log.info("Already logged in to my-fulfilment.com.")
        return

    log.info("my-fulfilment.com session invalid/expired — logging in automatically.")
    _fulfilment_auto_login(driver)

    _load_fulfilment_page(driver, FULFILMENT_ORDERS_URL)
    time.sleep(4)
    if _fulfilment_session_ok(driver):
        log.info("my-fulfilment.com login successful, session saved to chrome_profile.")
        return
    screenshot(driver, "fulfilment_login_failed")
    raise RuntimeError("my-fulfilment.com login failed (check credentials or portal status)")


def _read_table(driver, heading):
    """Parse the first table following a section heading into row dicts."""
    table = find_visible(driver, f"//*[normalize-space(text())='{heading}']/following::table[1]")
    if not table:
        return [], []
    headers = [th.text.strip() for th in table.find_elements(By.XPATH, ".//th")]
    rows = []
    for tr in table.find_elements(By.XPATH, ".//tbody/tr"):
        cells = tr.find_elements(By.XPATH, "./td")
        if cells:
            rows.append({h: c.text.strip() for h, c in zip(headers, cells)})
    links = [{"text": a.text.strip(), "url": a.get_attribute("href")}
             for a in table.find_elements(By.XPATH, ".//a")
             if a.text.strip()]
    return rows, links


def _load_fulfilment_page(driver, url, attempts=4):
    """Navigate to a my-fulfilment.com page, retrying on a transient HTTP 500.

    The portal intermittently returns a "This page isn't working / HTTP ERROR
    500" page; a reload after a short wait usually fixes it.
    """
    for attempt in range(1, attempts + 1):
        driver.get(url)
        time.sleep(3)
        body = ""
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
        except WebDriverException:
            pass
        if "isn't working" not in body and "HTTP ERROR 5" not in body \
                and "unable to handle this request" not in body:
            return True
        log.warning("my-fulfilment.com returned a server error (attempt %d/%d) — "
                    "retrying in %ds", attempt, attempts, 5 * attempt)
        time.sleep(5 * attempt)
    log.error("my-fulfilment.com still erroring after %d attempts: %s", attempts, url)
    return False


def fulfilment_lookup(driver, reference):
    """Filter my-fulfilment.com orders by reference and pull full details.

    Returns a dict with the result row, order lines, packages and tracking
    link(s), or None if no order matches the reference.
    """
    ref = reference.lstrip("#")
    if not _load_fulfilment_page(driver, FULFILMENT_ORDERS_URL):
        raise RuntimeError("my-fulfilment.com orders page unavailable (HTTP 500)")

    # The Reference input lives in the collapsible Filters panel.
    ref_input = find_visible(driver, "//input[@placeholder='Reference']")
    if not ref_input:
        toggle = find_visible(driver, "//*[normalize-space(text())='Filters']")
        if toggle:
            js_click(driver, toggle)
        ref_input = wait_for(driver, "//input[@placeholder='Reference']",
                             20, "Reference filter input")
    ref_input.clear()
    ref_input.send_keys(ref)
    apply_btn = wait_for(
        driver,
        "//button[normalize-space()='Apply filters'] | //input[@value='Apply filters']",
        10, "Apply filters button",
    )
    js_click(driver, apply_btn)
    time.sleep(4)

    row = find_visible(driver, f"//tr[td[contains(normalize-space(), '{ref}')]]")
    if not row:
        log.warning("No fulfilment order found for reference %s", ref)
        return None

    header_cells = [th.text.strip() for th in driver.find_elements(
        By.XPATH, f"//tr[td[contains(normalize-space(), '{ref}')]]/ancestor::table[1]//th")]
    cells = [td.text.strip() for td in row.find_elements(By.XPATH, "./td")]
    summary = {
        "reference": reference,
        "order_row": {h: c for h, c in zip(header_cells, cells) if h},
    }

    # The grid row carries its detail page in data-href (no <a> elements).
    detail_url = row.get_attribute("data-href")
    if not detail_url:
        for a in row.find_elements(By.XPATH, ".//a[contains(@href, '/orders/')]"):
            detail_url = a.get_attribute("href")
            break
    if not detail_url:
        log.warning("Fulfilment order row found for %s but no detail link", ref)
        return summary

    detail_url = detail_url.replace("/edit", "/show")
    if detail_url.startswith("/"):
        detail_url = "https://www.my-fulfilment.com" + detail_url
    _load_fulfilment_page(driver, detail_url)
    summary["detail_url"] = detail_url
    summary["order_lines"], _ = _read_table(driver, "Order lines")
    summary["packages"], package_links = _read_table(driver, "Packages")
    # Keep only carrier links (the table headers are sort links back into
    # the portal — only external URLs are actual tracking links).
    summary["tracking"] = [
        l for l in package_links
        if l["url"] and "my-fulfilment.com" not in l["url"]
    ]

    # Extract the shipped package's carrier + tracking number (the inputs
    # phase 4 needs). A "Confirmed"/unshipped order has no package yet.
    summary["carrier"] = ""
    summary["tracking_number"] = ""
    for pkg in summary["packages"]:
        tnt = (pkg.get("TNT-Code") or "").strip()
        if tnt:
            summary["carrier"] = (pkg.get("Shipper") or "").strip()
            summary["tracking_number"] = tnt
            break
    summary["shipped"] = bool(summary["tracking_number"])
    return summary


def log_fulfilment_result(tiktok_id, shopify_name, info):
    """Log one consolidated end-summary block for an order."""
    log.info("-" * 60)
    log.info("TikTok order : %s", tiktok_id)
    log.info("Shopify order: %s", shopify_name)
    if not info:
        log.info("Fulfilment   : NOT FOUND for reference %s", shopify_name)
        return
    row = info.get("order_row", {})
    log.info("Fulfilment   : Nic.Oud ref %s | status %s | %s, %s, %s",
             row.get("Nic. Oud reference", "?"), row.get("Status", "?"),
             row.get("Name", "?"), row.get("City", "?"), row.get("Country", "?"))
    for line in info.get("order_lines", []):
        log.info("Order line   : %s x%s (%s) shipped=%s open=%s",
                 line.get("Article code", "?"), line.get("Ordered", "?"),
                 line.get("Article description", ""), line.get("Shipped", "?"),
                 line.get("Open", "?"))
    if info.get("shipped"):
        for pkg in info.get("packages", []):
            if (pkg.get("TNT-Code") or "").strip():
                log.info("Package      : shipped %s via %s (%s), weight %s, TNT-code %s",
                         pkg.get("Shipping date", "?"), pkg.get("Shipper", "?"),
                         pkg.get("Shipping method", "?"), pkg.get("Weight", "?"),
                         pkg.get("TNT-Code", "?"))
        for t in info.get("tracking", []):
            log.info("Tracking     : %s -> %s", t["text"], t["url"])
    else:
        log.info("Package      : NOT SHIPPED YET — no tracking number "
                 "(status %s); will be skipped for TikTok upload.",
                 info.get("order_row", {}).get("Status", "?"))
    if info.get("detail_url"):
        log.info("Detail page  : %s", info["detail_url"])


def run_fulfilment_phase(driver, headless, found):
    """Phase 3: look up each Shopify-matched order in my-fulfilment.com."""
    log_phase("PHASE 3/3: Look up tracking in my-fulfilment.com")
    log.info("Checking %d matched order(s) for tracking ...", len(found))
    ensure_fulfilment_logged_in(driver, headless)
    results = []
    for item in found:
        name = item["shopify_order"]["name"]
        try:
            info = fulfilment_lookup(driver, name)
        except Exception as exc:
            log.error("Fulfilment lookup failed for %s: %s", name, exc)
            screenshot(driver, f"fulfilment_fail_{name.lstrip('#')}")
            info = None
        results.append((item["tiktok_id"], name, info))

    shipped = sum(1 for _, _, info in results if info and info.get("shipped"))
    log_phase("FINAL SUMMARY")
    log.info("%d order(s) processed: %d shipped (tracking found), %d not yet shipped/found.",
             len(results), shipped, len(results) - shipped)
    for tiktok_id, name, info in results:
        log_fulfilment_result(tiktok_id, name, info)
    log.info("=" * 70)
    return results


def download_ship_template(driver):
    """Phase 4a: download the shipping-upload template from Seller Center."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    driver.execute_cdp_cmd(
        "Browser.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(DOWNLOAD_DIR)},
    )
    before = {p.name for p in DOWNLOAD_DIR.glob("*.xlsx")}

    driver.get(SHIP_UPLOAD_URL)
    btn = wait_for(driver, "//button[.//text()[normalize-space()='Download template']]",
                   60, "Download template button")
    js_click(driver, btn)
    log.info("Clicked Download template — waiting for the xlsx file.")

    deadline = time.time() + DOWNLOAD_TIMEOUT
    while time.time() < deadline:
        new = [p for p in DOWNLOAD_DIR.glob("*.xlsx") if p.name not in before]
        if new and not list(DOWNLOAD_DIR.glob("*.crdownload")):
            path = max(new, key=lambda p: p.stat().st_mtime)
            log.info("Template downloaded: %s", path.name)
            return path
        time.sleep(2)
    raise TimeoutError("Shipping template download did not complete")


def fill_ship_template(template_path, shipments):
    """Phase 4b: fill the template's mandatory columns, preserving format.

    shipments: list of {"order_id", "provider", "tracking"}.
    Only the 'Shipping info' sheet gets data rows (from row 4); all other
    sheets (provider list, meta_info_sheet) are left untouched.
    """
    import openpyxl

    wb = openpyxl.load_workbook(template_path)
    ws = wb["Shipping info"]
    headers = {ws.cell(row=2, column=c).value: c for c in range(1, ws.max_column + 1)}
    col_order = headers["Order ID"]
    col_provider = headers["Shipping provider name"]
    col_tracking = headers["Tracking ID"]

    for i, s in enumerate(shipments):
        r = 4 + i
        ws.cell(row=r, column=col_order, value=str(s["order_id"]))
        ws.cell(row=r, column=col_provider, value=s["provider"])
        ws.cell(row=r, column=col_tracking, value=str(s["tracking"]))

    filled = template_path.with_name(template_path.stem + "_filled.xlsx")
    wb.save(filled)
    log.info("Template filled with %d shipment(s): %s", len(shipments), filled.name)
    return filled


def upload_ship_file(driver, file_path):
    """Phase 4c: attach the filled file, click Upload shipping info, read result."""
    if SHIP_UPLOAD_URL.split("?")[0] not in driver.current_url:
        driver.get(SHIP_UPLOAD_URL)
        time.sleep(5)

    file_input = None
    deadline = time.time() + 30
    while time.time() < deadline and file_input is None:
        inputs = driver.find_elements(By.XPATH, "//input[@type='file']")
        file_input = inputs[0] if inputs else None
        if file_input is None:
            time.sleep(2)
    if file_input is None:
        screenshot(driver, "no_file_input")
        raise RuntimeError("File input for the upload dropzone not found")

    file_input.send_keys(str(file_path))
    log.info("File attached: %s", file_path.name)
    time.sleep(3)

    upload_btn = wait_for(
        driver,
        "//button[.//text()[normalize-space()='Upload shipping info']][not(@disabled)]",
        30, "enabled 'Upload shipping info' button",
    )
    js_click(driver, upload_btn)
    log.info("Clicked Upload shipping info — waiting for the review page.")

    # TikTok navigates to an "Add tracking info" review page that lists the
    # orders identified in the file (already-shipped / unknown orders are
    # filtered out). A file-format problem surfaces as an error toast instead.
    review = None
    deadline = time.time() + 60
    while time.time() < deadline and review is None:
        time.sleep(3)
        review = find_visible(driver, "//*[normalize-space(text())='Add tracking info']")
        err = find_visible(
            driver, "//*[contains(translate(text(), 'EF', 'ef'), 'error') or "
                    "contains(translate(text(), 'EF', 'ef'), 'failed')]")
        if err:
            screenshot(driver, "upload_error")
            verdict = err.text.strip()
            log.info("UPLOAD RESULT (FAILED): %s", verdict)
            return False, verdict
    if review is None:
        screenshot(driver, "upload_no_review_page")
        log.warning("UPLOAD RESULT: review page never appeared")
        return False, "review page never appeared"

    time.sleep(3)
    body = driver.find_element(By.TAG_NAME, "body").text
    if "No orders identified" in body:
        screenshot(driver, "upload_result")
        log.info("UPLOAD RESULT (REJECTED): file parsed OK but no eligible "
                 "orders identified (already shipped / not awaiting shipment).")
        return False, "No orders identified"

    # Orders were identified — confirm with the final submit button.
    rows = driver.find_elements(
        By.XPATH, "//table//tr[td]")
    log.info("Review page lists %d order row(s) — confirming submission.", len(rows))
    submit = None
    for label in ("Submit", "Confirm", "Save", "Add tracking info", "Upload"):
        submit = find_visible(
            driver, f"//button[.//text()[normalize-space()='{label}']][not(@disabled)]")
        if submit:
            break
    if not submit:
        screenshot(driver, "upload_no_submit_btn")
        log.warning("UPLOAD RESULT: orders identified but no submit button found")
        return False, "no submit button on review page"
    js_click(driver, submit)
    log.info("Clicked final '%s' — waiting for confirmation.", submit.text.strip())

    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(3)
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        if "success" in body or "uploaded" in body or "submitted" in body:
            screenshot(driver, "upload_success")
            log.info("UPLOAD RESULT (SUCCESS): tracking info submitted for %d order(s)",
                     len(rows))
            return True, f"submitted {len(rows)} order(s)"
        if "error" in body or "failed" in body:
            screenshot(driver, "upload_failed")
            log.info("UPLOAD RESULT (FAILED): error after final submit")
            return False, "error after final submit"
    screenshot(driver, "upload_result")
    log.warning("UPLOAD RESULT: no confirmation detected after final submit")
    return False, "no confirmation after final submit"


def upload_test(headless):
    """Test the shipping-upload mechanics with an already-shipped order.

    Uses the completed order 576907531018934637 with its REAL PostNL
    tracking — TikTok should reject it (already shipped), which safely
    proves download/fill/attach/upload/verdict end to end.
    """
    manager = BrowserManager(profile_dir=str(PROJECT_ROOT / "chrome_profile"))
    driver = manager.start_browser(headless=headless)
    try:
        ensure_logged_in(driver, headless)
        template = download_ship_template(driver)
        filled = fill_ship_template(template, [{
            "order_id": "576907531018934637",
            "provider": tiktok_provider("PostNL"),
            "tracking": "3SOSVJ0979112",
        }])
        ok, verdict = upload_ship_file(driver, filled)
        log.info("Upload test finished — possible in headless: YES. Verdict: %s", verdict)
    except Exception:
        log.exception("Upload test failed")
        screenshot(driver, "upload_test_failure")
        raise
    finally:
        manager.close()


def fulfilment_test(tiktok_id, headless):
    """Test mode: Shopify lookup + fulfilment lookup for one TikTok order ID."""
    from services.shopify import client as shopify

    log.info("TEST MODE: fulfilment lookup for TikTok order %s", tiktok_id)
    order = shopify.find_order_by_tiktok_id(tiktok_id)
    if not order:
        log.error("TikTok order %s not found in Shopify — cannot continue.", tiktok_id)
        sys.exit(1)
    log.info("Shopify match: %s (%s / %s)", order["name"],
             order["displayFinancialStatus"], order["displayFulfillmentStatus"])

    manager = BrowserManager(profile_dir=str(PROJECT_ROOT / "chrome_profile"))
    driver = manager.start_browser(headless=headless)
    try:
        run_fulfilment_phase(driver, headless,
                             [{"tiktok_id": tiktok_id, "shopify_order": order}])
        log.info("Fulfilment test completed successfully.")
    except Exception:
        log.exception("Fulfilment test failed")
        screenshot(driver, "fulfilment_test_failure")
        raise
    finally:
        manager.close()


def main():
    setup_logging()
    # headless via --headless flag or HEADLESS_MODE=1 (project convention);
    # default is visible so you can watch what's going on.
    headless = "--headless" in sys.argv or os.environ.get("HEADLESS_MODE") == "1"
    log.info("Browser mode: %s", "headless" if headless else "visible")

    # Test mode: --upload-test exercises the shipping-info upload mechanics
    # with an already-shipped order (safe — TikTok rejects duplicates).
    if "--upload-test" in sys.argv:
        upload_test(headless)
        return

    # Test mode: --fulfilment-test <tiktok_order_id> skips the TikTok export
    # and runs only the Shopify + fulfilment lookups for one order.
    if "--fulfilment-test" in sys.argv:
        idx = sys.argv.index("--fulfilment-test")
        if idx + 1 >= len(sys.argv):
            print("Usage: python tiktok_toship_export_raw.py --fulfilment-test <tiktok_order_id>")
            sys.exit(2)
        fulfilment_test(sys.argv[idx + 1], headless)
        return

    manager = BrowserManager(profile_dir=str(PROJECT_ROOT / "chrome_profile"))
    driver = manager.start_browser(headless=headless)
    try:
        log_phase("PHASE 1/3: Export awaiting-shipment orders from TikTok")
        ensure_logged_in(driver, headless)
        log.info("On TikTok orders page.")
        previous_top = run_export(driver)
        report_name = wait_for_new_report(driver, previous_top)
        csv_path = download_report(driver, report_name)
        records = extract_orders(csv_path)
        found, _missing = match_orders_in_shopify(records)
        if found:
            run_fulfilment_phase(driver, headless, found)
        else:
            log_phase("FINAL SUMMARY")
            log.info("No orders matched in Shopify — nothing to look up in fulfilment.")
        log.info("")
        log.info("Workflow completed successfully.")
    except Exception:
        log.exception("Workflow FAILED — see error above and screenshot in logs/")
        screenshot(driver, "failure")
        raise
    finally:
        manager.close()


if __name__ == "__main__":
    main()
