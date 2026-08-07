# Tracking Sync — Research & Understanding

How fulfilment and tracking actually work on both sides of the Smyle stack, and
how the two join up. Written from live investigation on **2026-08-07**.

**Nothing described here has been written to Shopify.** Every finding below
came from read-only queries and a dry run.

---

## The problem in one line

The 3PL ships every order and records a PostNL barcode against it, but nothing
carries that barcode back into Shopify — so customers get a "shipped" email
with no trackable link.

```
my-fulfilment.com (Nic. Oud 3PL)          Shopify
  Completed order                           order #SMYLE120123
  Package.tnt_code = 3SOSVJ0997151   -X->   fulfilment with NO trackingInfo
                                     gap
```

---

# Part 1 — Shopify: how fulfilment is set today

## 1.1 The object model

Three objects, and the distinction between the middle two is the whole game:

| Object | What it is |
|---|---|
| **Order** | The customer's purchase. `#SMYLE120579` |
| **FulfillmentOrder** | A *work request* — "ship these items from this location". Created automatically by Shopify. This is what you act on |
| **Fulfillment** | The *record of a shipment* — what actually went out, and what tracking it carries |

You never write tracking to an Order. You either create a **Fulfillment**
against a FulfillmentOrder, or you edit the tracking on a Fulfillment that
already exists.

## 1.2 `supportedActions` is the gate

Every FulfillmentOrder advertises what you're allowed to do with it, and the
list is **filtered by your token's scopes**. This is the single most useful
diagnostic on this whole integration:

```
Before the write scopes were granted:
  supportedActions: ['HOLD']

After:
  supportedActions: ['CREATE_FULFILLMENT', 'HOLD']
```

If `CREATE_FULFILLMENT` is absent, don't debug the mutation — check the scopes.

## 1.3 Locations

| Location | ID | Merchant-managed | Fulfils online orders |
|---|---|---|---|
| Smyle | 107029856591 | yes | yes |
| Smyle BV - Location UK | 117354266959 | yes | **no** |

Everything observed fulfils from **Smyle**. Because it's merchant-managed, the
scope needed is `write_merchant_managed_fulfillment_orders` — not
`write_third_party_fulfillment_orders`, despite an actual third party (Nic.
Oud) doing the physical shipping. Shopify doesn't know about the 3PL; as far as
it's concerned Smyle ships its own orders.

There are **no fulfilment services registered** (`GET /fulfillment_services.json`
returns empty), and 100% of fulfilments observed have
`service: { handle: "manual", type: "MANUAL" }`.

## 1.4 How much tracking actually exists — measured

Two independent sweeps:

**Sweep A — newest-first, 6,000 shipped orders**

| | |
|---|---|
| Fulfilments scanned | 1,004 in the first 1,000 orders |
| Service type | `manual` — 100% |
| With tracking, whole 6,000 | **11** |
| ↳ 10 × source `tiktok`, all 3–4 Aug 2026 |
| ↳ 1 × source `web` — a staff order |

**Sweep B — 250 most recent shipped orders sampled per month**

| Period | Tracked |
|---|---|
| Feb, Apr–Aug 2026 | 0 |
| Mar/Apr 2026 boundary | 1 (`kaufland`) |
| Jan 2026 → Apr 2025 | **0 in every single month** |
| Before Apr 2025 | no orders exist — store history starts here |

**Conclusion: Smyle has never populated tracking in Shopify.** The only
tracking present arrived automatically from marketplace connectors (TikTok,
Kaufland) pushing their own shipment data in. Direct web orders — the large
majority — have none. There is no internal convention to copy; this integration
establishes it.

## 1.5 The two real examples

Everything about the target shape comes from these.

**`#SMYLE77964` — complete (carrier + number + URL)**
```
source: kaufland    ship-to: DE
Fulfillment gid://shopify/Fulfillment/7417468289359   #SMYLE77964-F1
  status SUCCESS   service Manual   location Smyle
  createdAt 2026-04-08T07:47:03Z
  updatedAt 2026-04-08T07:47:03Z          <-- identical
  trackingInfo: [{ company: "PostNL Domestic",
                   number:  "LA036232528NL",
                   url:     "https://jouw.postnl.nl/track-and-trace/" }]
```

> **This tracking did not come from the 3PL portal.** Looked up afterwards,
> `#SMYLE77964` on my-fulfilment.com has **no packages and no T&T record at
> all** — both tables read "No records found", and its single order line sits
> under *Canceled orderlines* (`ordered=1, shipped=0, open=0`). The barcode in
> Shopify came from the **Kaufland connector**, not from Nic. Oud. It is still
> a valid example of the Shopify-side *shape*, but it is not evidence of the
> portal → Shopify flow. See Part 5 §1 — it also turns out to be mislabelled.

**`#SMYLE119086` — minimal (number only)**
```
source: tiktok      ship-to: NL
Fulfillment gid://shopify/Fulfillment/7755564712271   #SMYLE119086-F1
  status SUCCESS   service Manual   location Smyle
  createdAt 2026-08-04T12:15:58Z
  updatedAt 2026-08-04T12:15:58Z          <-- identical
  trackingInfo: [{ company: null, number: "3SOSVJ0630409", url: null }]
```

Two things this tells us:

1. **`createdAt == updatedAt` on both.** Tracking was supplied in the same call
   that created the fulfilment — not bolted on afterwards. That's the pattern to
   follow. (Contrast: the staff `web` order had a ~20h gap, i.e. tracking added
   later via a separate update.)
2. **`company: null` produces `url: null`.** The TikTok connector sends a bare
   number, so those customers get an untrackable string. Always send a carrier.

## 1.6 The two mutations

**Order not yet fulfilled** — one call does both:
```graphql
fulfillmentCreate(fulfillment: {
  lineItemsByFulfillmentOrder: [{ fulfillmentOrderId: "gid://shopify/FulfillmentOrder/..." }],
  trackingInfo: { company: "PostNL Domestic", number: "3SOSVJ0997151" },
  notifyCustomer: false
}) { fulfillment { id name trackingInfo { company number url } }
     userErrors { field message } }
```
Omitting line items fulfils everything remaining — correct for whole-order
shipments. Partial shipments need
`fulfillmentOrderLineItems: [{ id, quantity }]`.

**Order already fulfilled without tracking** — the common case here:
```graphql
fulfillmentTrackingInfoUpdateV2(
  fulfillmentId: "gid://shopify/Fulfillment/...",
  trackingInfoInput: { company: "...", number: "..." },
  notifyCustomer: false
)
```

## 1.7 Shopify gotchas

1. **`userErrors` does not raise.** A rejected mutation still returns HTTP 200
   with a populated `userErrors` array. Unchecked, a bulk run reports success
   while writing nothing. `_check_user_errors()` in
   `services/shopify/fulfillment.py` exists for this.
2. **Carrier strings must match exactly.** `"PostNL"` alone does not resolve;
   `PostNL Domestic` and `PostNL International` are distinct entries. A wrong
   string silently kills the auto-generated URL.
3. **Changing app scopes reissues the access token.** The old `shpat_…` stops
   working and `shopify.env` must be updated.
4. **`read_orders` vs `read_all_orders`.** The token has `read_all_orders`, so
   the ~60-day order window that `services/shopify/client.py:17-19` still warns
   about **does not apply** — full history is readable. That docstring is stale.

## 1.8 Token scopes (verified 2026-08-07)

`GET /admin/oauth/access_scopes.json`

Writes: `write_assigned_fulfillment_orders`, `write_discounts`,
`write_fulfillments`, `write_merchant_managed_fulfillment_orders`,
`write_price_rules`, `write_third_party_fulfillment_orders`

Plus broad read access including `read_all_orders`, `read_fulfillments`,
`read_merchant_managed_fulfillment_orders`.

---

# Part 2 — my-fulfilment.com: where the tracking comes from

The Nic. Oud "User portal". A server-rendered Symfony app with **no public
API**, so the client logs in with a real form POST and scrapes HTML.

## 2.1 Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/login` | GET | issues `PHPSESSID`, renders `_csrf_token` |
| `/login_check` | POST | `_csrf_token`, `_username`, `_password`, `_submit` |
| `/orders/` | GET | order grid — filters, paging, sorting all via query params |
| `/orders/<uuid>/show` | GET | read-only detail (Confirmed / Completed) |
| `/orders/<uuid>/edit` | GET | editable form (Soft error / not yet in WMS) |

**Login:** GET `/login` → scrape the CSRF token → POST `/login_check`. Success
is a `302 → /`; **failure is also a 302, but back to `/login`** — check the
redirect target, not the status code.

## 2.2 Where the tracking lives

Only on the **detail page**, in the Packages table. Not in the grid. So reading
tracking costs one HTTP request per order.

```
Package:
  shipping_date    2026-08-07
  shipper          PostNL
  shipping_method  Brievenbuspakje+ 24h
  weight
  tnt_code         3SOSVJ0997151
  tnt_url          http://postnl.nl/tracktrace/?L=NL&B=3SOSVJ0997151&P=6523NK&D=NL&T=C
```

**The portal's `tnt_url` is better than anything we'd build.** It already
carries the postcode and destination country that PostNL needs for the lookup
to resolve. Prefer passing it through over letting Shopify generate a bare URL.

## 2.3 Not everything is trackable

Sampled 4 pages of Completed orders — 40 packages:

| Count | Tracked | Shipper | Method |
|---|---|---|---|
| 22 | yes | PostNL | Brievenbuspakje+ 24h |
| 15 | yes | PostNL | EU/ROW Boxable tracked max 23*35*3 2kg |
| 2 | **no** | PostNL | EU/ROW Boxable **non tracked** max 23*35*3 2kg |
| 1 | yes | PostNL | Pakket - AVG |

**38 of 40 tracked.** The untracked ones ship on a deliberately non-tracked
method and have an empty T&T column — `Package.is_tracked` filters them out
rather than writing blanks into Shopify.

Two barcode formats, and they line up with the shipping method:

| Prefix | Example | Method | Meaning |
|---|---|---|---|
| `3S…` | `3SOSVJ0997151` | Brievenbuspakje+ 24h | NL domestic letterbox |
| `LA…NL` | `LA898365296NL` | EU/ROW Boxable tracked | cross-border registered |

## 2.4 Order statuses

`created`, `soft_error`, `ready_for_wms`, `to_wms_error`, `to_wms`, `wms_error`,
`confirmed`, `processing`, `partially_shipped`, `completed`, `on_hold`

Only **Completed** orders have packages. Reading order state:

| Signal | Meaning |
|---|---|
| `Nic. Oud reference` is `-` | not yet accepted into the WMS — stuck in middleware |
| Status `Soft error` | validation rejected it; see `error_message` |
| Status `Confirmed` | accepted, not shipped |
| Status `Completed` | shipped; Packages holds the T&T code |
| Packages table empty | nothing physically shipped yet |

## 2.5 Portal gotchas

1. **The status filter needs BOTH ends.** Sending only
   `statusCode_from=completed` is *silently ignored* — the page re-renders with
   your choice selected so it looks applied, but you get every status back. Send
   `statusCode_from` **and** `statusCode_to`.
2. **The grid's natural order is not by date.** An unsorted page 1 returns an
   arbitrary slice of old orders — the first dry run found 0 tracked shipments
   for exactly this reason. Always sort: `sort_by="created", sort_order="DESC"`.
3. **CSRF tokens are bound to the session that issued them.** You can't reuse
   one from a browser or an earlier run.
4. **Grid rows have no `<a>` tags.** The detail link is in a `data-href`
   attribute on the `<tr>`, wired up by JavaScript.
5. **`/show` vs `/edit` depends on order state.** Don't guess the route — take
   it from the grid row.
6. **Encoding is inconsistent.** Header says UTF-8, some pages carry raw cp1252
   (accented ES/FR addresses). Also strips `U+2060` WORD JOINER, which the grid
   injects into street names and which otherwise poisons string comparison.
7. **`OrderFilters[reference]` is a SUBSTRING match.** Searching `123` returns
   `#SMYLE120123`, `#SMYLE119123`, `#com79123`… `find_order()` re-filters for an
   exact match.
8. **Some queries are slow.** A reference substring search over a month took
   ~130 s cold, ~8 s warm. Default timeout is 180 s.
9. **No total count is exposed.** Pagination stops when a page is empty or
   repeats the previous page's first reference.
10. **The order-lines table has a blank leading column group.** Rows carry
    `Article code / description / group / ol.id / Shipment NO` before the seven
    columns that matter. "Shipment NO" is empty on unshipped and cancelled
    lines, so dropping blanks and indexing from the start silently discards
    exactly those rows. Index from the **end** off the raw cells. Found on
    `#SMYLE77964`, whose only line is cancelled and was being skipped entirely.

---

# Part 3 — The join

## 3.1 The key

`OrderRow.reference` on the portal is literally the Shopify order `name`:

```
portal  "#SMYLE120123"   ==   Shopify  order.name  "#SMYLE120123"
```

Matched with `orders(query: "name:SMYLE120123")`.

Corroboration that this loop already works elsewhere: the TikTok tracking
numbers sitting in Shopify (`3SOSVJ0630409`) are the exact same PostNL barcode
format the portal produces (`3SOSVJ0997151`). TikTok's connector already does
this round-trip for TikTok orders. Web orders are simply unserved.

## 3.2 Decision tree

Per shipment, `_decide()` in `sync_tracking_to_shopify.py`:

| Shopify state | Action |
|---|---|
| Order not found | `missing` — report, do nothing |
| A fulfilment already carries this exact number | `skip` — already done, idempotent |
| A fulfilment carries a *different* number | `conflict` — report, never overwrite |
| FulfillmentOrder supports `CREATE_FULFILLMENT` | `create` |
| Already fulfilled, no tracking | `update` |
| Neither | `blocked` — report the status |

The `skip` branch makes re-runs safe. The `conflict` branch means the sync will
never silently clobber a tracking number someone else set.

## 3.3 Verified dry run

```
8 shipment(s) resolved from the portal, all matched in Shopify:

#SMYLE120606  3SOSVJ0997151  PostNL Domestic       UNFULFILLED  create
#SMYLE120594  3SOSVJ0997148  PostNL Domestic       UNFULFILLED  create
#SMYLE120596  3SOSVJ0997146  PostNL Domestic       UNFULFILLED  create
#SMYLE120581  3SOSVJ0997155  PostNL Domestic       UNFULFILLED  create
#SMYLE120582  LA898365296NL  PostNL International  UNFULFILLED  create
#SMYLE120584  3SOSVJ0997152  PostNL Domestic       UNFULFILLED  create
#SMYLE120586  LA582790071NL  PostNL International  UNFULFILLED  create
#SMYLE120587  3SOSVJ0997142  PostNL Domestic       UNFULFILLED  create

DRY RUN — nothing was written.
```

---

# Part 4 — What's built

| File | Role |
|---|---|
| `services/fulfilment/client.py` | Portal client. Read-only against the portal — lists and reads orders, never creates or edits |
| `services/shopify/fulfillment.py` | **All Shopify writes.** Deliberately separate from `client.py`, which is read-only and used by live workflows |
| `sync_tracking_to_shopify.py` | The workflow. **Dry run by default**; writes only with `--apply` |

Credentials follow the existing project chain — env → `<vendor>.env` →
`config_store` — reading `MYFULFILMENT_EMAIL` / `MYFULFILMENT_PASSWORD` from
`fulfilment.env`. The cached session cookie
(`fulfilment_session.cookies`) is gitignored; it's a live credential.

**No existing workflow was modified.** `tiktok_toship_export_raw.py` still does
its own Selenium scraping of the same portal (lines ~353–640) — that's ~200
lines the new client could replace, but it's a live supervised job and was left
alone deliberately.

---

# Part 5 — Open questions

1. ~~**Carrier mapping.**~~ **RESOLVED.** The apparent contradiction was bad
   source data, not a bad heuristic. `#SMYLE77964` is labelled `PostNL Domestic`
   in Shopify despite shipping to **DE** — but the portal records its delivery
   mode as `EU/ROW Boxable tracked` (cross-border) and its barcode is
   `LA036232528NL`, the international format. **Kaufland's connector mislabelled
   it.** Country-based and method-based mapping both correctly say
   *International* here, so the current mapping stands. Switching to
   `shipping_method` is still marginally better — it reads the carrier product
   directly rather than inferring from destination — but it is no longer a
   correctness fix.
2. **Pass the portal's `tnt_url` through?** It resolves properly at PostNL
   (carries postcode + country); a Shopify-generated URL doesn't. Currently the
   code sends carrier + number only.
3. **`notifyCustomer`.** Defaults to `false` everywhere. Creating a fulfilment
   with `true` sends Shopify's shipping email — needs a deliberate decision
   before any bulk run, especially for orders shipped days ago.
4. **Backfill scope.** Thousands of historical shipped orders have no tracking.
   Whether to backfill, and how far, is unresolved — and interacts directly with
   the `notifyCustomer` question.
