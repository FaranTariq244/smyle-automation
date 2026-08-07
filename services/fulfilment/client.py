"""
my-fulfilment.com portal client (Nic. Oud "User portal").

The 3PL fulfilment backend used by Smyle BV. The portal has no public API —
it's a server-rendered Symfony app, so this client logs in with a real form
POST and scrapes the HTML. Everything here was derived from live requests; the
non-obvious behaviours are marked with GOTCHA comments so they don't have to be
rediscovered.

Credentials are read from (in priority order):
    1. environment variables
    2. fulfilment.env at the project root (gitignored)
    3. config_store

Required settings:
    MYFULFILMENT_EMAIL      e.g. "partnerships@wesmyle.com"
    MYFULFILMENT_PASSWORD
Legacy aliases MYF_USERNAME / MYF_PASSWORD are also accepted.

The session cookie is cached in fulfilment_session.cookies at the project root
so repeated calls don't re-login. That file is a live credential — it is
gitignored, don't commit or share it. Delete it to force a fresh login.

This module is read-only against the portal: it lists and reads orders, it
never creates or edits them.

Usage:
    from services.fulfilment import client as myf

    with myf.MyFulfilmentClient() as c:
        c.login()
        for row in c.iter_orders(status="completed", created_from="2026-08-01"):
            print(row.reference, row.status)

        detail = c.get_order_detail(c.find_order("#SMYLE119123"))
        print(detail.status, detail.tnt_codes)   # 'Completed', ['3SOSVJ0995727']
"""

from __future__ import annotations

import http.cookiejar
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, Optional

import requests
from bs4 import BeautifulSoup

from config_store import get_setting

log = logging.getLogger(__name__)

BASE_URL = "https://www.my-fulfilment.com"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / "fulfilment.env"
COOKIE_FILE = str(PROJECT_ROOT / "fulfilment_session.cookies")

# The portal rejects requests that don't look like a browser, and the login
# flow in particular is picky about Origin/Referer. Reuse one realistic UA.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

# GOTCHA: the grid injects U+2060 WORD JOINER into cell text (e.g. inside
# street names) purely for line-breaking. Strip it or string comparisons and
# CSV exports get invisible junk.
WORD_JOINER = "⁠"

# ---------------------------------------------------------------------------
# Status vocabulary
# ---------------------------------------------------------------------------
# These are the exact <option value="..."> strings from the OrderFilters status
# dropdown. Passing anything else is silently ignored by the backend.
STATUSES = {
    "created": "Created",
    "soft_error": "Soft error",
    "ready_for_wms": "Ready for WMS",
    "to_wms_error": "Failed to send to WMS",
    "to_wms": "Send to WMS",
    "wms_error": "WMS error",
    "confirmed": "Confirmed",
    "processing": "Processing",
    "partially_shipped": "Partially shipped",
    "completed": "Completed",
    "on_hold": "On hold",
}

# Pseudo-statuses accepted only by statusCode_from (not by statusCode_to).
META_STATUSES = {
    "allStatuses": "All statuses",
    "backOrders": "Back orders",
    "shippedToday": "Shipped today",
}

# "Open" = reached neither a shipped nor a terminal state.
OPEN_STATUS_LABELS = {
    "Created", "Soft error", "Ready for WMS", "Failed to send to WMS",
    "Send to WMS", "WMS error", "Confirmed", "Processing",
    "Partially shipped", "On hold",
}

# Sortable columns, as accepted by kitdg_grid_grid_sort_field.
SORT_FIELDS = {
    "wms_ref": "o.wicsOrderId",
    "reference": "o.reference",
    "status": "o.middlewareStatusCode",
    "name": "da.name",
    "address": "da.address1",
    "city": "da.city",
    "country": "da.country",
    "lines": "o.totalOrderLines",
    "created": "o.createdAt",
    "modified": "o.modifiedAt",
}


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

_env_file_cache: Optional[Dict[str, str]] = None


def _load_env_file() -> Dict[str, str]:
    """Parse fulfilment.env (minimal .env format, supports inline # comments)."""
    global _env_file_cache
    if _env_file_cache is None:
        env: Dict[str, str] = {}
        if ENV_FILE.exists():
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                env[key.strip()] = val.split("#", 1)[0].strip()
        _env_file_cache = env
    return _env_file_cache


def _get_config(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key) or _load_env_file().get(key) or get_setting(key, default)


def get_credentials() -> tuple:
    """Return (email, password) for the portal, or raise if not configured."""
    email = _get_config("MYFULFILMENT_EMAIL") or _get_config("MYF_USERNAME")
    password = _get_config("MYFULFILMENT_PASSWORD") or _get_config("MYF_PASSWORD")
    if not email or not password:
        raise LoginError(
            "my-fulfilment.com credentials not configured. Set "
            "MYFULFILMENT_EMAIL / MYFULFILMENT_PASSWORD in fulfilment.env at the "
            "project root or as environment variables."
        )
    return email, password


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class OrderRow:
    """One row from the /orders/ grid (the list view)."""

    wms_reference: str   # "Nic. Oud reference" — blank/"-" until accepted by the WMS
    reference: str       # shop reference, e.g. "#SMYLE120123"
    status: str          # human label, e.g. "Completed"
    name: str
    address: str
    city: str
    country: str
    lines: str
    created_at: str
    modified_at: str
    uuid: str = ""         # portal's internal order id
    detail_url: str = ""   # full URL to /show or /edit

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_STATUS_LABELS

    @property
    def is_in_wms(self) -> bool:
        """False while the order is still stuck in the middleware."""
        return bool(self.wms_reference) and self.wms_reference != "-"


@dataclass
class OrderLine:
    article_code: str
    description: str
    value: str
    serial_numbers: str
    ordered: str
    shipped: str
    open: str


@dataclass
class Package:
    shipping_date: str
    shipper: str           # e.g. "PostNL"
    shipping_method: str
    weight: str
    tnt_code: str          # track & trace, e.g. "3SOSVJ0995727"
    # The portal renders the T&T code as a real carrier link, e.g.
    # http://postnl.nl/tracktrace/?L=NL&B=<code>&P=<zip>&D=<country>&T=C
    # Prefer this over building a URL yourself — it already carries the
    # postcode/country params PostNL needs for the lookup to resolve.
    tnt_url: str = ""

    @property
    def is_tracked(self) -> bool:
        """Non-tracked delivery modes ship with an empty T&T column."""
        return bool(self.tnt_code and self.tnt_code.strip("- "))


@dataclass
class OrderDetail:
    """Everything on a single order's /show or /edit page."""

    url: str
    fields: dict = field(default_factory=dict)   # label -> value
    lines: list = field(default_factory=list)    # list[OrderLine]
    packages: list = field(default_factory=list)  # list[Package]
    error_message: str = ""                      # populated for Soft error orders

    @property
    def reference(self) -> str:
        return self.fields.get("Reference", "")

    @property
    def status(self) -> str:
        return self.fields.get("Status", "")

    @property
    def tnt_codes(self) -> list:
        return [p.tnt_code for p in self.packages if p.tnt_code and p.tnt_code != "-"]

    @property
    def tracked_packages(self) -> list:
        return [p for p in self.packages if p.is_tracked]


class LoginError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------


def _decode(response: requests.Response) -> str:
    """
    Decode a response body to text.

    GOTCHA: the portal sends `Content-Type: text/html; charset=UTF-8` but some
    pages contain raw cp1252 bytes (accented names like "buzon" in Spanish
    addresses). Strict UTF-8 decoding throws or mangles them, so fall back to
    cp1252 when UTF-8 fails.
    """
    raw = response.content
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
    return text.replace(WORD_JOINER, "")


def _clean(node) -> str:
    """Collapse an element's text to a single tidy line."""
    if node is None:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).replace(WORD_JOINER, "").strip()


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class MyFulfilmentClient:
    def __init__(self, base_url: str = BASE_URL, cookie_file: str = COOKIE_FILE,
                 timeout: int = 180):
        self.base_url = base_url.rstrip("/")
        # GOTCHA: some filtered queries take >2 minutes on a cold cache
        # (a reference substring search over a month took ~130s the first time,
        # ~8s afterwards). Keep the timeout generous.
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "image/avif,image/webp,image/apng,*/*;q=0.8"),
            "Accept-Language": "en-US,en;q=0.9",
        })

        # Persist PHPSESSID between runs so we don't re-login every call.
        self.session.cookies = http.cookiejar.LWPCookieJar(cookie_file)
        if os.path.exists(cookie_file):
            try:
                self.session.cookies.load(ignore_discard=True)
            except Exception:
                pass  # corrupt/empty jar — just log in again

    # -- context manager -----------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.save_session()
        self.session.close()

    def save_session(self):
        try:
            self.session.cookies.save(ignore_discard=True)
        except Exception:
            pass

    # -- auth ----------------------------------------------------------------

    def is_logged_in(self) -> bool:
        """Cheap check: the dashboard renders a logout link only when authenticated."""
        r = self.session.get(f"{self.base_url}/", timeout=self.timeout, allow_redirects=True)
        return "/login" not in r.url and "logout" in _decode(r).lower()

    def login(self, username: Optional[str] = None, password: Optional[str] = None,
              force: bool = False) -> None:
        """
        Log in to the portal.

        Two-step flow, and both steps matter:

          1. GET /login          -> sets PHPSESSID and renders a _csrf_token
          2. POST /login_check   -> form-encoded credentials + that token

        GOTCHA: the _csrf_token is bound to the PHPSESSID that issued it. You
        cannot reuse a token copied from a browser session (or from an older
        run) — Symfony will reject it as invalid. Always scrape a fresh one
        from step 1 using the same cookie jar you'll POST with.

        Success is signalled by a 302 to "/". A failed login also returns 302,
        but back to "/login" — so check the redirect target, not the status code.
        """
        if not force and self.is_logged_in():
            log.debug("Reusing cached my-fulfilment.com session.")
            return

        if not username or not password:
            username, password = get_credentials()

        # Step 1 — fresh session + CSRF token.
        page = self.session.get(f"{self.base_url}/login", timeout=self.timeout)
        page.raise_for_status()
        soup = BeautifulSoup(_decode(page), "html.parser")
        token_el = soup.find("input", {"name": "_csrf_token"})
        if not token_el or not token_el.get("value"):
            raise LoginError("Could not find _csrf_token on /login — page layout changed?")
        token = token_el["value"]

        # Step 2 — the actual credential POST.
        resp = self.session.post(
            f"{self.base_url}/login_check",
            data={
                "_csrf_token": token,
                "_username": username,
                "_password": password,
                "_submit": "",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/login",
            },
            allow_redirects=False,  # we want to inspect the redirect target
            timeout=self.timeout,
        )

        target = resp.headers.get("Location", "")
        if resp.status_code != 302 or target.rstrip("/").endswith("/login"):
            raise LoginError(
                f"Login failed (HTTP {resp.status_code}, redirect -> {target or 'none'}). "
                "Check MYFULFILMENT_EMAIL / MYFULFILMENT_PASSWORD in fulfilment.env."
            )
        self.save_session()
        log.info("Logged in to my-fulfilment.com as %s", username)

    # -- order list ----------------------------------------------------------

    def _order_list_params(self, *, page=1, status=None, reference=None,
                           created_from=None, created_to=None,
                           modified_from=None, modified_to=None,
                           created_by=None, sort_by=None, sort_order="DESC") -> dict:
        """Build the query string for /orders/."""
        params = {
            "OrderFilters[createdAt_from]": created_from or "",
            "OrderFilters[createdAt_to]": created_to or "",
            "OrderFilters[modifiedAt_from]": modified_from or "",
            "OrderFilters[modifiedAt_to]": modified_to or "",
            "OrderFilters[reference]": reference or "",  # substring match, not exact
            "OrderFilters[created_by]": created_by or "",
            "OrderFilters[submit]": "",
        }

        # GOTCHA — the big one. The status filter is a RANGE (statusCode_from +
        # statusCode_to). If you send only statusCode_from, the backend accepts
        # the parameter and even re-renders the dropdown with your choice
        # selected, but it applies NO status filter at all — you silently get
        # every status back. To filter on a single status you must send the
        # same value for both ends.
        if status:
            if status in META_STATUSES:
                params["OrderFilters[statusCode_from]"] = status
            else:
                if status not in STATUSES:
                    raise ValueError(
                        f"Unknown status {status!r}. Valid: "
                        f"{', '.join(sorted(STATUSES) + sorted(META_STATUSES))}"
                    )
                params["OrderFilters[statusCode_from]"] = status
                params["OrderFilters[statusCode_to]"] = status
        else:
            params["OrderFilters[statusCode_from]"] = "allStatuses"

        if page and page > 1:
            params["kitdg_paginator_grid_currentPage"] = page
        if sort_by:
            params["kitdg_grid_grid_sort_field"] = SORT_FIELDS.get(sort_by, sort_by)
            params["kitdg_grid_grid_sort_order"] = sort_order
        return params

    def list_orders(self, **kwargs) -> list:
        """
        Fetch one page of the order grid. Returns list[OrderRow].

        All filters are plain GET query parameters — no CSRF token needed for
        reads, so these URLs can be bookmarked or replayed freely.
        """
        params = self._order_list_params(**kwargs)
        resp = self.session.get(f"{self.base_url}/orders/", params=params,
                                headers={"Referer": f"{self.base_url}/orders/"},
                                timeout=self.timeout)
        resp.raise_for_status()
        if "/login" in resp.url:
            raise LoginError("Session expired — call login(force=True).")
        return self._parse_order_rows(_decode(resp))

    def _parse_order_rows(self, html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if table is None:
            return []

        rows = []
        for tr in table.find_all("tr"):
            cells = [_clean(td) for td in tr.find_all("td")]
            cells = [c for c in cells if c != ""] or cells
            if len(cells) < 10:
                continue  # header / spacer / "no records found" row

            # GOTCHA: the grid does NOT wrap rows in <a> tags. The target sits
            # in a `data-href` attribute on the <tr> itself and is turned into a
            # click handler by JavaScript. Searching for anchors finds nothing.
            href = tr.get("data-href") or ""
            if not href:
                link = tr.find("a", href=re.compile(r"/orders/[0-9a-f-]{36}/(show|edit)"))
                href = link["href"] if link else ""

            uuid, detail_url = "", ""
            if href:
                detail_url = self.base_url + href
                m = re.search(r"/orders/([0-9a-f-]{36})/", href)
                uuid = m.group(1) if m else ""

            rows.append(OrderRow(
                wms_reference=cells[0], reference=cells[1], status=cells[2],
                name=cells[3], address=cells[4], city=cells[5], country=cells[6],
                lines=cells[7], created_at=cells[8], modified_at=cells[9],
                uuid=uuid, detail_url=detail_url,
            ))
        return rows

    def iter_orders(self, max_pages: int = 1, **kwargs) -> Iterator[OrderRow]:
        """
        Walk the grid page by page.

        Pagination is `kitdg_paginator_grid_currentPage=N`. The portal doesn't
        publish a total count, so we stop when a page comes back empty or
        repeats the previous page's first reference.
        """
        seen_first = None
        for page in range(1, max_pages + 1):
            batch = self.list_orders(page=page, **kwargs)
            if not batch:
                return
            if seen_first is not None and batch[0].reference == seen_first:
                return  # pager clamped to the last page; stop looping
            seen_first = batch[0].reference
            for row in batch:
                yield row

    # -- convenience queries -------------------------------------------------

    def open_orders(self, max_pages: int = 5, **kwargs) -> list:
        """
        Orders that still need something to happen (anything but Completed).

        Filtered client-side: the server's status range filter can only express
        one status at a time reliably, and we want the whole open set.
        """
        return [r for r in self.iter_orders(max_pages=max_pages, **kwargs) if r.is_open]

    def soft_error_orders(self, max_pages: int = 5, **kwargs) -> list:
        """Orders rejected by the middleware — these need a human fix."""
        return list(self.iter_orders(max_pages=max_pages, status="soft_error", **kwargs))

    def find_order(self, reference: str, **kwargs) -> Optional[OrderRow]:
        """
        Look up a single order by reference.

        NOTE: OrderFilters[reference] is a SUBSTRING match. Searching "123"
        returns #SMYLE120123, #SMYLE119123, #com79123 ... so we filter for an
        exact match on the way out.
        """
        needle = reference.lstrip("#")
        for row in self.iter_orders(max_pages=3, reference=needle, **kwargs):
            if row.reference.lstrip("#") == needle:
                return row
        return None

    def iter_shipped_with_tracking(self, max_pages: int = 5, **kwargs) -> Iterator[tuple]:
        """
        Yield (OrderRow, Package) for every Completed order carrying a T&T code.

        Tracking lives only on the detail page, so this opens each order in the
        selection — it is one HTTP request per order. Keep max_pages sane.

        Defaults to newest-first: the grid's natural order is NOT by creation
        date, so an unsorted page 1 returns an arbitrary slice of old orders.
        """
        kwargs.setdefault("status", "completed")
        kwargs.setdefault("sort_by", "created")
        kwargs.setdefault("sort_order", "DESC")
        for row in self.iter_orders(max_pages=max_pages, **kwargs):
            for pkg in self.get_order_detail(row).tracked_packages:
                yield row, pkg

    # -- order detail --------------------------------------------------------

    def get_order_detail(self, url_or_row) -> OrderDetail:
        """
        Fetch and parse a single order page.

        GOTCHA: the route depends on the order's state.
          * /orders/<uuid>/edit  — editable form; used while the order is still
                                   in Soft error / not yet accepted by the WMS
          * /orders/<uuid>/show  — read-only; used once Confirmed/Completed
        Guessing wrong gives a 404 or a redirect, so always use the href from
        the grid row (OrderRow.detail_url), which is what this method expects.
        """
        url = url_or_row.detail_url if isinstance(url_or_row, OrderRow) else url_or_row
        if not url:
            raise ValueError("No detail URL — the grid row had no link.")

        resp = self.session.get(url, headers={"Referer": f"{self.base_url}/orders/"},
                                timeout=self.timeout)
        resp.raise_for_status()
        html = _decode(resp)
        soup = BeautifulSoup(html, "html.parser")

        detail = OrderDetail(url=url)

        # -- header fields ---------------------------------------------------
        # Both /show and /edit render every field as <label for="x"> + an
        # <input id="x" value="...">. On /show the inputs are readonly. Pairing
        # by the for/id attribute is far more robust than walking the divs.
        #
        # NOTE: reading the page top-to-bottom (e.g. with a plain tag-strip
        # regex) shifts labels against values and makes fields like "Partial
        # delivery type" show the delivery date instead.
        labels = {lb["for"]: _clean(lb)
                  for lb in soup.find_all("label") if lb.get("for")}
        for inp in soup.find_all("input"):
            key = inp.get("id")
            if key in labels and labels[key] and key not in detail.fields:
                detail.fields[labels[key]] = (inp.get("value") or "").strip()
        # Dropdowns (country, shipper, shipping method...) on the /edit form.
        for sel in soup.find_all("select"):
            key = sel.get("id")
            if key in labels and labels[key] and labels[key] not in detail.fields:
                chosen = sel.find("option", selected=True)
                if chosen is not None:
                    detail.fields[labels[key]] = _clean(chosen)

        # -- soft error reason -----------------------------------------------
        m = re.search(r"The following error occured while processing the order:\s*"
                      r"(?:</?[^>]+>\s*)*([^<]{3,300})", html)
        if m:
            detail.error_message = re.sub(r"\s+", " ", m.group(1)).strip()

        # -- tables ----------------------------------------------------------
        # The page has several tables (order lines, packages, package history).
        # Identify them by their header text rather than by position, because
        # the count varies between /show and /edit.
        for table in soup.find_all("table"):
            headers = [_clean(th) for th in table.find_all("th")]
            header_blob = " ".join(headers).lower()
            body_rows = []
            for tr in table.find_all("tr"):
                cells = [_clean(td) for td in tr.find_all("td")]
                cells = [c for c in cells if c != ""]
                if cells:
                    body_rows.append(cells)

            if "ordered" in header_blob and "shipped" in header_blob:
                # GOTCHA: this table carries a leading group of columns
                # (Article code / description / group / ol.id / Shipment NO)
                # that is blank on most rows, then the seven that matter:
                #   code, description, value, serials, ordered, shipped, open
                # Filtering blanks out and indexing from the START silently
                # drops every line whose "Shipment NO" is empty — i.e. every
                # unshipped or cancelled line. Index from the END instead, off
                # the raw cells, so the row length can't shift the mapping.
                # Verified on #SMYLE77964, whose only line is cancelled.
                for tr in table.find_all("tr"):
                    raw = [_clean(td) for td in tr.find_all("td")]
                    if len(raw) < 7:
                        continue  # header, spacer, or the "Canceled orderlines" label
                    code, desc, value, serials, ordered, shipped, still_open = raw[-7:]
                    if not code:
                        continue
                    detail.lines.append(OrderLine(
                        article_code=code, description=desc, value=value,
                        serial_numbers=serials, ordered=ordered,
                        shipped=shipped, open=still_open,
                    ))
            elif "shipping date" in header_blob:
                # Packages table; the T&T column only appears once shipped.
                # Re-walk the rows here (not body_rows) so we can pull the
                # carrier hyperlink off the T&T cell as well as its text.
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    cells = [_clean(td) for td in tds]
                    cells = [c for c in cells if c != ""]
                    if len(cells) < 4:
                        continue
                    link = tr.find("a", href=True)
                    detail.packages.append(Package(
                        shipping_date=cells[0], shipper=cells[1],
                        shipping_method=cells[2], weight=cells[3],
                        tnt_code=cells[4] if len(cells) > 4 else "",
                        tnt_url=link["href"].replace("&amp;", "&") if link else "",
                    ))

        # GOTCHA: on /edit pages the order lines are an editable FORM, not a
        # table — there is no Ordered/Shipped grid to scrape. Rebuild them from
        # the order[orderLines][N][...] inputs instead. Nothing has shipped on
        # these orders by definition, so shipped=0 and open=ordered.
        if not detail.lines:
            by_index = {}
            for inp in soup.find_all("input"):
                m = re.match(r"order\[orderLines\]\[(\d+)\]\[(\w+)\]",
                             inp.get("name") or "")
                if m:
                    by_index.setdefault(m.group(1), {})[m.group(2)] = \
                        (inp.get("value") or "").strip()
            for idx in sorted(by_index, key=int):
                line = by_index[idx]
                if not line.get("articleCode"):
                    continue
                qty = line.get("quantityOrdered", "")
                detail.lines.append(OrderLine(
                    article_code=line.get("articleCode", ""),
                    description=line.get("articleDescription", ""),
                    value=line.get("unitValue", ""),
                    serial_numbers="-",
                    ordered=qty, shipped="0", open=qty,
                ))
        return detail


def test_connection() -> bool:
    """Connectivity check; logs in and reads the first page of the order grid."""
    with MyFulfilmentClient() as c:
        c.login()
        rows = c.list_orders()
        log.info("my-fulfilment.com OK — %d orders on page 1", len(rows))
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    with MyFulfilmentClient() as _c:
        _c.login()
        print("Login OK — session cached in", COOKIE_FILE)
        for _r in _c.list_orders():
            print(f"  {_r.reference:<16} {_r.status:<18} {_r.name[:24]:<24} {_r.created_at}")
    sys.exit(0)
