# Smyle BigQuery Knowledge Base

Reference for querying Smyle data from BigQuery. Use this to build automations,
answer data questions, or replicate any Looker Studio dashboard.

---

## 1. Access Configuration

| Setting | Value |
|---|---|
| **Billing project** | `gen-lang-client-0136431006` (runs/bills queries) |
| **Data project** | `pelagic-core-307421` (owns the data) |
| **Dataset** | `smyle_dbt_prod` (production dbt models) |
| **Auth** | OAuth user credentials in `bigquery_auth.json` |
| **Python client** | `services/bigquery/client.py` |

**Key functions:**
```python
from services.bigquery.client import run_query, fqn, get_bigquery_client

# Run a query, get list of dicts
rows = run_query("SELECT * FROM `pelagic-core-307421.smyle_dbt_prod.ads_spend` LIMIT 10")

# Build fully-qualified table name
fqn("ads_spend")  # -> `pelagic-core-307421.smyle_dbt_prod.ads_spend`
```

**Fully-qualified table reference pattern:**
```sql
`pelagic-core-307421.smyle_dbt_prod.<table_name>`
```

---

## 2. Looker Studio Data Source -> BigQuery Table Mapping

| Looker Studio Data Source | BigQuery Table | Charts | Type |
|---|---|---|---|
| smyle_online_marketing | **ads_online** | 18 | Reusable |
| smyle_ads_spend | **ads_spend** | 8 | Reusable |
| smyle_aggregated_spend | **orders_enriched_agg_spend** | 26 | Reusable |
| smyle_enriched_aggregated | **orders_enriched_aggregated** | 34 | Reusable |
| Smyle_orders_enriched | **orders_enriched** | 46 | Reusable |
| smyle_subscriptions_total | **subscriptions_total** | - | Reusable |
| smyle_subscriptions_new | **subscriptions_new** | - | Reusable |
| funnel_all_campaigns | **funnel_all_campaigns** | 2 | Embedded |
| smyle_funnel | **funnel** | 12 | Reusable |
| orders_enriched_customer_dim | **orders_enriched_customer_dim** | 7 | Embedded |
| smyle_orders_agg_CLV | **orders_agg_CLV** | 17 | Reusable |
| orders_agg_CLV_dim | **orders_agg_CLV_dim** | 3 | Embedded |
| overiw_magali_country_dynamic_metrics | **overiw_magali_country_dynamic_metrics** | 14 | Embedded |
| clv_running_dim | **clv_running_dim** | 24 | Embedded |
| customers_enriched | **customers_enriched** | 29 | Embedded |
| smyle_clv_running | **clv_running** | 5 | Reusable |
| Smyle_clv_runningdatesub | **clv_runningdatesub** | 1 | Reusable |
| Smyle_clv_runningonlydate | **clv_runningonlydate** | 1 | Reusable |
| smyle_clv_cac_date_country | **clv_cac_date_country** | 2 | Reusable |
| SMYLE_NPS_total | **NPS_total** | 5 | Reusable |
| smyle_NPS_enriched | **NPS_enriched** | 4 | Reusable |
| Smyle_orders_products | **orders_products** | 13 | Reusable |
| profitability | **profitability** | 5 | Reusable |
| overview_magali | **overview_magali** | 2 | Embedded |
| overview_magali_productlevel | **overview_magali_productlevel** | 1 | Embedded |
| overview_magali_country | **overview_magali_country** | 2 | Embedded |
| subscriptions_running | **subscriptions_running** | 6 | Embedded |
| subscriptions_running_endtable | **subscriptions_running_endtable** | 2 | Embedded |
| subscriptions_productcat_total | **subscriptions_productcat_total** | 1 | Embedded |
| shopify_woo_transactions | **shopify_woo_transactions** | 1 | Reusable |
| smyle_orders_return_windows | **orders_return_windows** | 3 | Reusable |
| recharge_brush | **recharge_brush** | 12 | Embedded |

---

## 3. Dashboard Tabs Overview

| Tab | Primary Data Sources | Status |
|---|---|---|
| **KPI's** | orders_enriched_agg_spend, ads_spend, overiw_magali_country_dynamic_metrics | **Mapped** |
| **Funnel** | funnel, funnel_all_campaigns | **Mapped** |
| **Marketing Deepdive** | ads_online | **Fully mapped** |
| **Subscriptions** | subscriptions_running, subscriptions_new, subscriptions_productcat_total | **Mapped** |
| **Subscriptions Churn** | subscriptions_running_endtable | **Mapped** |
| **Customer Lifetime Value** | clv_running, clv_running_dim, clv_runningdatesub | **Mapped** |
| **Discount / Influencer** | orders_enriched, customers_enriched | **Mapped** |
| **CLV Development Graph** | clv_runningonlydate | **Mapped** |
| **CAC / CLV** | clv_cac_date_country | **Mapped** |
| **Store Deepdive** | orders_enriched_aggregated | **Mapped** |
| **Order Type** | orders_enriched, orders_enriched_customer_dim | **Mapped** |
| **Return Window & Buying Pattern** | orders_return_windows | **Mapped** |

---

## 4. Marketing Deepdive

**Table**: `ads_online` (11 cols, DATE-based)

### Schema

| Column | Type | Description |
|---|---|---|
| account | STRING | Ad account ("SMYLE - NL", "Smyle - 2024 - NL/BE", "Smyle - 2024 - REO") |
| date | DATE | Date |
| medium | STRING | "Facebook" or "Google Ads" |
| billing_country | STRING | **Tier label** -- NOT actual country. Values: Other, Tier 1-6 |
| country | STRING | Actual country name |
| campaign_name | STRING | Campaign name |
| impressions | FLOAT64 | Ad impressions |
| clicks | FLOAT64 | Ad clicks |
| transactions | FLOAT64 | Conversions |
| conversion_value | FLOAT64 | Revenue (EUR) |
| cost | FLOAT64 | Ad spend (EUR) |

### IMPORTANT: billing_country = Tiers

In `ads_online`, `billing_country` contains tier labels, NOT real countries.
The Looker Studio "Tiers" filter maps to this column. The `country` column has actual names.

| Tier | Primary Countries (current) |
|---|---|
| Other | All countries (Facebook campaigns + catch-all Google) |
| Tier 1 | Netherlands (Google Ads) |
| Tier 2 | Germany (Google Ads) |
| Tier 4 | Belgium, Austria, Switzerland, Spain, Italy, France, Ireland, Portugal, Sweden, Denmark, Norway, Finland + broader EU (Google Ads) |
| Tier 6 | United Kingdom (Google Ads) |

### Filters -> SQL

| Filter | Column | Example |
|---|---|---|
| Country | `country` | `WHERE country = 'Netherlands'` |
| Medium | `medium` | `WHERE medium = 'Facebook'` |
| Tiers | `billing_country` | `WHERE billing_country = 'Tier 1'` |
| Date | `date` | `WHERE date BETWEEN '2026-04-01' AND '2026-04-22'` |

### All 10 KPI Formulas

```sql
SELECT
  SUM(impressions)                                      AS impressions,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions))            AS ctr,
  SUM(clicks)                                           AS clicks,
  SUM(transactions)                                     AS conversions,
  SAFE_DIVIDE(SUM(transactions), SUM(clicks))           AS conversion_pct,
  SUM(conversion_value)                                 AS online_revenue,
  SUM(cost)                                             AS spend,
  SAFE_DIVIDE(SUM(conversion_value), SUM(transactions)) AS aov,
  SAFE_DIVIDE(SUM(cost), SUM(transactions))             AS cpo,
  SAFE_DIVIDE(SUM(conversion_value), SUM(cost))         AS roas
FROM `pelagic-core-307421.smyle_dbt_prod.ads_online`
WHERE date BETWEEN @start_date AND @end_date
```

### Campaign Breakdown

```sql
SELECT
  campaign_name,
  SUM(impressions)                                      AS impressions,
  SUM(clicks)                                           AS clicks,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions))            AS ctr,
  SUM(transactions)                                     AS conversions,
  SAFE_DIVIDE(SUM(transactions), SUM(clicks))           AS conv_rate,
  SUM(cost)                                             AS spend,
  SAFE_DIVIDE(SUM(cost), SUM(impressions)) * 1000       AS cpm,
  SAFE_DIVIDE(SUM(cost), SUM(clicks))                   AS cpc,
  SAFE_DIVIDE(SUM(cost), SUM(transactions))             AS cpo,
  SUM(conversion_value)                                 AS revenue,
  SAFE_DIVIDE(SUM(conversion_value), SUM(cost))         AS roas
FROM `pelagic-core-307421.smyle_dbt_prod.ads_online`
WHERE date BETWEEN @start_date AND @end_date
GROUP BY campaign_name
ORDER BY spend DESC
```

### ads_spend vs ads_online

| Feature | ads_spend | ads_online |
|---|---|---|
| billing_country | Actual country names | Tier labels |
| country column | Not present | Actual country names |
| Tiers support | No | Yes |
| Use for | Simple per-country queries | Marketing Deepdive / tier filtering |

---

## 5. KPI's Tab

**Primary tables**: `orders_enriched_agg_spend`, `ads_spend`, `overiw_magali_country_dynamic_metrics`

### orders_enriched_agg_spend (26 charts)

Daily aggregated revenue + spend per country. Main KPI overview table.

| Column | Type | Description |
|---|---|---|
| Account | STRING | Always "smyle" |
| date | DATE | |
| billing_country | STRING | Mixed format: "Netherlands", "NL", "AT", etc. |
| net_revenue | FLOAT64 | Total net revenue |
| spend | FLOAT64 | Total ad spend |
| orders | INT64 | Total orders |
| net_refund | FLOAT64 | Refund amount |
| orders_first_time | INT64 | First-time orders |
| orders_returning | INT64 | Returning customer orders |
| netrevenue_first_time | FLOAT64 | Revenue from new customers |
| netrevenue_returning | FLOAT64 | Revenue from returning customers |
| netrevenue_excl_repeat | FLOAT64 | Revenue excluding repeat subs |
| customer_first_time | INT64 | New customers |
| facebook_spend | FLOAT64 | Facebook ad spend |
| facebook_orders | FLOAT64 | Facebook-attributed orders |
| paid_cost | FLOAT64 | Total paid ad cost |
| paid_orders | FLOAT64 | Paid-attributed orders |
| paid_customers | INT64 | Paid-acquired customers |

### KPI Summary Query

```sql
SELECT
  SUM(net_revenue)           AS total_revenue,
  SUM(orders)                AS total_orders,
  SUM(spend)                 AS total_spend,
  SUM(orders_first_time)     AS new_orders,
  SUM(orders_returning)      AS returning_orders,
  SUM(netrevenue_first_time) AS new_revenue,
  SUM(netrevenue_returning)  AS returning_revenue,
  SUM(net_refund)            AS refunds,
  SAFE_DIVIDE(SUM(net_revenue), SUM(orders)) AS aov,
  SAFE_DIVIDE(SUM(spend), SUM(orders_first_time)) AS cac
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched_agg_spend`
WHERE date BETWEEN @start_date AND @end_date
```

### KPI by Country

```sql
SELECT
  billing_country,
  SUM(net_revenue) AS revenue,
  SUM(orders) AS orders,
  SUM(spend) AS spend,
  SAFE_DIVIDE(SUM(net_revenue), SUM(orders)) AS aov
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched_agg_spend`
WHERE date BETWEEN @start_date AND @end_date
GROUP BY billing_country
ORDER BY revenue DESC
```

**Note**: billing_country has mixed formats ("Netherlands" vs "NL"). Normalize
when comparing across tables.

---

## 6. Funnel Tab

**Tables**: `funnel` (12 charts), `funnel_all_campaigns` (2 charts)

Both have identical schemas but different data scope.

### CRITICAL: Multi-Row Architecture

These tables use **segregated rows by channel**. Different channels hold different metrics:
- `channel = 'Facebook'` / `'Google Ads'` rows: `spend`, `impressions`, `clicks` (no orders/revenue)
- `channel = 'Shop'` rows: `orders`, `first_time_orders`, `repurchase_orders`, `net_revenue` (no spend)
- `channel = 'Google Analytics'` rows (historical only): `sessions`, `bounces`, `add_to_carts_ga`, `checkouts_ga`

**You cannot simply SUM(*) across all channels.** Spend and revenue are in separate rows.

### CRITICAL: GA Data Gap

GA funnel columns (`sessions`, `bounces`, `add_to_carts_ga`, `checkouts_ga`, `transactions_ga`)
are **NULL for all dates after 2023-09-11**. The session-based funnel pipeline is historical only.

### funnel vs funnel_all_campaigns

- `funnel` (12 charts): Filtered subset -- specific Facebook + limited non-NL Google Ads campaigns.
  No Shop rows in current data.
- `funnel_all_campaigns` (2 charts): All campaigns including Google Ads NL + Shop rows.
  This is the complete picture.

### Schema (both tables)

| Column | Type | Description |
|---|---|---|
| date | DATE | |
| shipping_country | STRING | Country (mixed: "NL", "Netherlands", "Other") |
| channel | STRING | "Facebook", "Google Ads", "Shop" (determines which metrics are populated) |
| campaign | STRING | Campaign name (lowercase) |
| campaign_phase | STRING | "Do", "Other" |
| tiers | STRING | Only "Other" (not useful) |
| spend | FLOAT64 | Ad spend (Facebook/Google Ads rows only) |
| impressions | FLOAT64 | Ad impressions (Facebook/Google Ads rows only) |
| clicks | FLOAT64 | Ad clicks (Facebook/Google Ads rows only) |
| sessions | INT64 | GA sessions (NULL after 2023-09) |
| bounces | INT64 | GA bounces (NULL after 2023-09) |
| add_to_carts_ga | INT64 | GA add-to-cart (NULL after 2023-09) |
| checkouts_ga | INT64 | GA checkout starts (NULL after 2023-09) |
| transactions_ga | INT64 | GA transactions (NULL after 2023-09) |
| transaction_revenue_ga | FLOAT64 | GA revenue (NULL after 2023-09) |
| orders | INT64 | Orders (Shop rows only) |
| first_time_orders | INT64 | New customer orders (Shop rows only) |
| revenue_first_time | FLOAT64 | New customer revenue (Shop rows only) |
| repurchase_orders | INT64 | Returning orders (Shop rows only) |
| revenue_repurchase | FLOAT64 | Returning revenue (Shop rows only) |
| net_revenue | FLOAT64 | Net revenue (Shop rows only) |

### Ad Spend Summary (current data)

```sql
SELECT
  channel,
  SUM(spend) AS spend,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks
FROM `pelagic-core-307421.smyle_dbt_prod.funnel_all_campaigns`
WHERE date BETWEEN @start_date AND @end_date
  AND channel IN ('Facebook', 'Google Ads')
GROUP BY channel
```

### Orders/Revenue Summary (current data)

```sql
SELECT
  shipping_country,
  SUM(orders) AS orders,
  SUM(first_time_orders) AS new_orders,
  SUM(repurchase_orders) AS repeat_orders,
  SUM(net_revenue) AS revenue,
  SUM(revenue_first_time) AS new_revenue,
  SUM(revenue_repurchase) AS repeat_revenue
FROM `pelagic-core-307421.smyle_dbt_prod.funnel_all_campaigns`
WHERE date BETWEEN @start_date AND @end_date
  AND channel = 'Shop'
GROUP BY shipping_country
ORDER BY revenue DESC
```

### Combined Spend + Revenue View (requires separate subqueries)

```sql
WITH ad_spend AS (
  SELECT
    date,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
  FROM `pelagic-core-307421.smyle_dbt_prod.funnel_all_campaigns`
  WHERE date BETWEEN @start_date AND @end_date
    AND channel IN ('Facebook', 'Google Ads')
  GROUP BY date
),
shop_orders AS (
  SELECT
    date,
    SUM(orders) AS orders,
    SUM(net_revenue) AS revenue,
    SUM(first_time_orders) AS new_orders
  FROM `pelagic-core-307421.smyle_dbt_prod.funnel_all_campaigns`
  WHERE date BETWEEN @start_date AND @end_date
    AND channel = 'Shop'
  GROUP BY date
)
SELECT
  COALESCE(a.date, s.date) AS date,
  a.spend,
  a.impressions,
  a.clicks,
  s.orders,
  s.revenue,
  s.new_orders,
  SAFE_DIVIDE(s.revenue, a.spend) AS roas
FROM ad_spend a
FULL OUTER JOIN shop_orders s ON a.date = s.date
ORDER BY date
```

### Historical Funnel Conversion Rates (pre-Sept 2023 only)

```sql
SELECT
  SUM(sessions) AS sessions,
  SUM(bounces) AS bounces,
  SUM(add_to_carts_ga) AS add_to_carts,
  SUM(checkouts_ga) AS checkouts,
  SUM(transactions_ga) AS transactions,
  SAFE_DIVIDE(SUM(add_to_carts_ga), SUM(sessions))    AS session_to_atc_rate,
  SAFE_DIVIDE(SUM(checkouts_ga), SUM(add_to_carts_ga)) AS atc_to_checkout_rate,
  SAFE_DIVIDE(SUM(transactions_ga), SUM(checkouts_ga))  AS checkout_to_purchase_rate,
  SAFE_DIVIDE(SUM(transactions_ga), SUM(sessions))      AS overall_conv_rate
FROM `pelagic-core-307421.smyle_dbt_prod.funnel`
WHERE date BETWEEN '2023-01-01' AND '2023-09-11'
```

### Campaign-Level Spend

```sql
SELECT
  campaign,
  channel,
  SUM(spend) AS spend,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
  SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc
FROM `pelagic-core-307421.smyle_dbt_prod.funnel_all_campaigns`
WHERE date BETWEEN @start_date AND @end_date
  AND channel IN ('Facebook', 'Google Ads')
GROUP BY campaign, channel
ORDER BY spend DESC
```

---

## 7. Subscriptions Tab

**Tables**: `subscriptions_running` (6 charts), `subscriptions_new`, `subscriptions_productcat_total`

### subscriptions_running -- Cohort Retention

Monthly cohort survival table tracking active subscriptions over time by product.

| Column | Type | Description |
|---|---|---|
| first_order_month | DATE | Cohort start month |
| sku | STRING | Product SKU |
| product_name | STRING | Product name |
| product_category | STRING | Category (Toothpaste refills, Starter kits, Bamboo brushheads, etc.) |
| subscriptions_started_in_month | INT64 | New subs started in cohort month |
| last_day_of_month | DATE | Observation month (end of) |
| months_since_fo | INT64 | Months since first order (0-72) |
| active_subscriptions_end_of_month | INT64 | Active subs at end of observation month |

**~78K rows, date range**: 2020-04 to 2026-04

### Subscription Retention Curve

```sql
SELECT
  months_since_fo,
  SUM(subscriptions_started_in_month) AS cohort_size,
  SUM(active_subscriptions_end_of_month) AS still_active,
  SAFE_DIVIDE(
    SUM(active_subscriptions_end_of_month),
    SUM(subscriptions_started_in_month)
  ) AS retention_rate
FROM `pelagic-core-307421.smyle_dbt_prod.subscriptions_running`
WHERE first_order_month >= '2024-01-01'
GROUP BY months_since_fo
ORDER BY months_since_fo
```

### Retention by Product Category

```sql
SELECT
  product_category,
  months_since_fo,
  SUM(active_subscriptions_end_of_month) AS active_subs
FROM `pelagic-core-307421.smyle_dbt_prod.subscriptions_running`
WHERE last_day_of_month = '2026-04-30'
GROUP BY product_category, months_since_fo
ORDER BY product_category, months_since_fo
```

### subscriptions_new -- New Acquisition Tracking

| Column | Type | Description |
|---|---|---|
| date | DATE | Subscription start date |
| billing_country | STRING | Country |
| store | STRING | "nl", "com", "com_2v", "de" |
| order_unique_id | STRING | Order ID |

**94K rows, fully current (up to 2026-04-23)**

```sql
SELECT
  DATE_TRUNC(date, MONTH) AS month,
  billing_country,
  COUNT(*) AS new_subscriptions
FROM `pelagic-core-307421.smyle_dbt_prod.subscriptions_new`
WHERE date BETWEEN @start_date AND @end_date
GROUP BY month, billing_country
ORDER BY month DESC, new_subscriptions DESC
```

### subscriptions_productcat_total -- Product Category Snapshot

Static aggregate (no date column). One row per product_category + status.

| Column | Type |
|---|---|
| sub_product_category | STRING |
| status | STRING |
| customers | INT64 |
| active_subscription | INT64 |

```sql
SELECT
  sub_product_category,
  SUM(customers) AS total_customers,
  SUM(active_subscription) AS active_subs
FROM `pelagic-core-307421.smyle_dbt_prod.subscriptions_productcat_total`
GROUP BY sub_product_category
ORDER BY total_customers DESC
```

---

## 8. Subscriptions Churn Tab

**Table**: `subscriptions_running_endtable` (2 charts)

Same structure as `subscriptions_running` but includes `months_since_fo = -1` rows
for calculating churn as delta between period start and end.

### Churn Calculation

```sql
WITH monthly AS (
  SELECT
    first_order_month,
    months_since_fo,
    SUM(active_subscriptions_end_of_month) AS active_subs
  FROM `pelagic-core-307421.smyle_dbt_prod.subscriptions_running_endtable`
  GROUP BY first_order_month, months_since_fo
)
SELECT
  a.first_order_month,
  a.months_since_fo,
  b.active_subs AS subs_start_of_period,
  a.active_subs AS subs_end_of_period,
  b.active_subs - a.active_subs AS churned,
  SAFE_DIVIDE(b.active_subs - a.active_subs, b.active_subs) AS churn_rate
FROM monthly a
JOIN monthly b
  ON a.first_order_month = b.first_order_month
  AND b.months_since_fo = a.months_since_fo - 1
WHERE a.months_since_fo > 0
ORDER BY a.first_order_month DESC, a.months_since_fo
```

---

## 9. Customer Lifetime Value Tab

**Tables**: `clv_running` (5 charts), `clv_running_dim` (24 charts), `clv_runningdatesub` (1 chart)

### clv_running -- Core CLV Table

One row per customer per observation date. Filterable by many dimensions.

| Column | Type | Description |
|---|---|---|
| date | DATE | Observation date |
| first_order_date | DATE | Customer's first order |
| months_since_first_order | INT64 | Months since acquisition |
| billing_country | STRING | Country |
| store | STRING | nl, com, com_2v, de |
| source_name | STRING | Acquisition source |
| source_medium | STRING | Facebook paid, Google CPC, direct, email, TikTok, organic |
| campaign | STRING | Campaign |
| device | STRING | Device type |
| browser | STRING | Browser |
| operating_system | STRING | OS |
| trial_buyer | STRING | "TRUE"/"FALSE" |
| active_sub | STRING | "TRUE"/"FALSE" |
| subscriber | STRING | "TRUE"/"FALSE" |
| net_revenue | FLOAT64 | Revenue in period |
| first_customers | INT64 | New customers |
| active_subscribers | INT64 | Active subscribers |
| running_sum_revenue | FLOAT64 | **Cumulative lifetime revenue** |

**377K rows, date range**: 2020-01 to 2026-04

### CLV Cohort Curve

```sql
SELECT
  months_since_first_order,
  SUM(first_customers) AS cohort_size,
  SUM(net_revenue) AS period_revenue,
  SUM(running_sum_revenue) AS cumulative_revenue,
  SAFE_DIVIDE(SUM(running_sum_revenue), SUM(first_customers)) AS clv_per_customer
FROM `pelagic-core-307421.smyle_dbt_prod.clv_running`
WHERE first_order_date >= '2024-01-01'
GROUP BY months_since_first_order
ORDER BY months_since_first_order
```

### CLV by Channel

```sql
SELECT
  source_medium,
  SUM(first_customers) AS customers,
  SUM(running_sum_revenue) AS lifetime_revenue,
  SAFE_DIVIDE(SUM(running_sum_revenue), SUM(first_customers)) AS clv
FROM `pelagic-core-307421.smyle_dbt_prod.clv_running`
WHERE date = (SELECT MAX(date) FROM `pelagic-core-307421.smyle_dbt_prod.clv_running`)
GROUP BY source_medium
ORDER BY lifetime_revenue DESC
```

### clv_running_dim -- Customer-Level CLV with Segments (24 charts)

Most detailed customer profile table: 40 columns including NPS scores, product ownership,
subscription status, and behavioral segments.

Key segmentation columns:

| Column | Type | Values |
|---|---|---|
| first_order_cat | STRING | "order" (63%), "trial" (30%), "subscription" (7%) |
| no_sub_anymore | STRING | "Active subscriber", "No subscriber anymore", "Never sub" |
| ebrush_owner | BOOL | E-brush owner flag |
| ebrush_order | STRING | "E-brush in first order", "E-brush not in first order", "Not a E-brush owner" |
| multi_tab_buyer | BOOL | Multi-tab buyer flag |
| subscriber | BOOL | Ever subscribed |
| active_sub | BOOL | Currently subscribed |
| recent_nps_category | STRING | "Promotor", "Passive", "Detractor" |
| recent_nps_score | FLOAT64 | Latest NPS score |

**350K rows, date range**: 2020-01 to 2026-04

### CLV by Customer Segment

```sql
SELECT
  first_order_cat,
  no_sub_anymore,
  SUM(first_customers) AS customers,
  SUM(running_sum_revenue) AS lifetime_revenue,
  SAFE_DIVIDE(SUM(running_sum_revenue), SUM(first_customers)) AS clv
FROM `pelagic-core-307421.smyle_dbt_prod.clv_running_dim`
WHERE date = (SELECT MAX(date) FROM `pelagic-core-307421.smyle_dbt_prod.clv_running_dim`)
GROUP BY first_order_cat, no_sub_anymore
ORDER BY lifetime_revenue DESC
```

### clv_runningdatesub -- CLV by Subscriber Status

Monthly cohort CLV split by subscriber TRUE/FALSE, country, and store.

| Column | Type |
|---|---|
| date_month | DATE |
| fo_month | DATE |
| subscriber | BOOL |
| billing_country | STRING |
| store | STRING |
| months_since_first_order | INT64 |
| net_revenue | FLOAT64 |
| first_customers | INT64 |
| running_sum_revenue | FLOAT64 |

```sql
SELECT
  subscriber,
  months_since_first_order,
  SUM(first_customers) AS cohort_size,
  SAFE_DIVIDE(SUM(running_sum_revenue), SUM(first_customers)) AS clv
FROM `pelagic-core-307421.smyle_dbt_prod.clv_runningdatesub`
WHERE fo_month >= '2024-01-01'
GROUP BY subscriber, months_since_first_order
ORDER BY subscriber, months_since_first_order
```

---

## 10. CLV Development Graph Tab

**Table**: `clv_runningonlydate` (1 chart)

Simplest CLV cohort table -- fully aggregated, just cohort month x observation month.
**2,706 rows**. Powers the triangular cohort matrix.

| Column | Type |
|---|---|
| date_month | DATE |
| fo_month | DATE |
| months_since_first_order | INT64 |
| net_revenue | FLOAT64 |
| first_customers | INT64 |
| running_sum_revenue | FLOAT64 |

```sql
SELECT
  fo_month,
  months_since_first_order,
  first_customers AS cohort_size,
  net_revenue AS period_revenue,
  running_sum_revenue AS cumulative_revenue,
  SAFE_DIVIDE(running_sum_revenue, first_customers) AS clv_per_customer
FROM `pelagic-core-307421.smyle_dbt_prod.clv_runningonlydate`
WHERE fo_month >= '2024-01-01'
ORDER BY fo_month, months_since_first_order
```

---

## 11. CAC / CLV Tab

**Table**: `clv_cac_date_country` (2 charts)

Daily CAC/CLV comparison with ad spend by country. The only table joining CLV metrics
with ad spend for CAC calculation.

| Column | Type | Description |
|---|---|---|
| date | DATE | |
| billing_country | STRING | Country |
| first_customers | INT64 | New acquisitions |
| rp_customers | INT64 | Repeat purchasers |
| sum_revenue | FLOAT64 | Total revenue |
| spend | FLOAT64 | Ad spend |

**60K rows, 200+ countries, date range**: 2020-01 to 2026-04

### CAC by Country

```sql
SELECT
  billing_country,
  SUM(first_customers) AS new_customers,
  SUM(spend) AS total_spend,
  SUM(sum_revenue) AS total_revenue,
  SAFE_DIVIDE(SUM(spend), SUM(first_customers)) AS cac,
  SAFE_DIVIDE(SUM(sum_revenue), SUM(first_customers)) AS revenue_per_customer
FROM `pelagic-core-307421.smyle_dbt_prod.clv_cac_date_country`
WHERE date BETWEEN @start_date AND @end_date
GROUP BY billing_country
HAVING SUM(first_customers) > 0
ORDER BY total_spend DESC
```

### CAC Trend Over Time

```sql
SELECT
  DATE_TRUNC(date, MONTH) AS month,
  SUM(first_customers) AS new_customers,
  SUM(spend) AS spend,
  SAFE_DIVIDE(SUM(spend), SUM(first_customers)) AS cac
FROM `pelagic-core-307421.smyle_dbt_prod.clv_cac_date_country`
WHERE date BETWEEN @start_date AND @end_date
GROUP BY month
ORDER BY month
```

---

## 12. Order Type Tab

**Tables**: `orders_enriched` (46 charts), `orders_enriched_customer_dim` (7 charts)

### orders_enriched -- Main Order Table

One row per order. 44 columns. **Date column is DATETIME** -- always cast.

Key dimension values:
- **order_type**: first_single, first_subscription, first_sub_after_single, first_sub_after_sub, repeat_single, repeat_subscription
- **store**: com, com_2v, de
- **source_name**: web, 9548333057 (Shopify POS), subscription_contract_checkout_one, subscription_contract, Recharge, bol, kaufland
- **active_customer_base**: Active Customer, Active Subscriber, Active Subcustomer
- **financial_status**: paid, partially_paid, partially_refunded, pending

### Order Type Breakdown

```sql
SELECT
  order_type,
  COUNT(*) AS orders,
  SUM(net_revenue) AS revenue,
  SAFE_DIVIDE(SUM(net_revenue), COUNT(*)) AS aov
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
GROUP BY order_type
ORDER BY revenue DESC
```

### Revenue by Country

```sql
SELECT
  billing_country,
  COUNT(*) AS orders,
  SUM(net_revenue) AS revenue
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
GROUP BY billing_country
ORDER BY revenue DESC
LIMIT 15
```

### Subscriber vs Non-Subscriber

```sql
SELECT
  active_customer_base,
  COUNT(*) AS orders,
  SUM(net_revenue) AS revenue
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
GROUP BY active_customer_base
```

### orders_enriched_customer_dim -- Customer Dimension Table

Extended version of orders_enriched with 59 columns including customer-level flags:
- `first_order_cat` (STRING): "order", "trial", "subscription"
- `ebrush_owner` (BOOL)
- `multi_tab_buyer` (BOOL)
- `subscriber` (BOOL), `active_sub` (BOOL)
- `no_sub_anymore` (STRING): "Active subscriber", "No subscriber anymore", "Never sub"
- NPS fields: `recent_nps_category`, `recent_nps_score`, `avg_nps_score`

**Note**: Data range is 2020-06 to 2023-07 only. Use `ga_date` for filtering.
May not have current data.

---

## 13. Store Deepdive Tab

**Table**: `orders_enriched_aggregated` (34 charts)

Pre-aggregated order data with very granular dimensions. 42 columns including
detailed repeat-purchase breakdowns (2nd, 3rd, 4th... 10th order).

| Column | Type | Description |
|---|---|---|
| date | DATE | |
| source_name | STRING | "web", "3890849" (POS), etc. |
| store | STRING | "com_2v", "com", "de" |
| billing_country | STRING | Country |
| city | STRING | City |
| source_medium | STRING | |
| device | STRING | |
| campaign | STRING | |
| product_combi | STRING | "Finished products", "Components", "Packets", etc. |
| order_type | STRING | Same as orders_enriched |
| orders | INT64 | Order count |
| net_revenue | FLOAT64 | Revenue |
| customers_first_time | INT64 | New customers |
| orders_first_time | INT64 | First orders |
| first_subscription_order | INT64 | |
| first_single_order | INT64 | |
| repeat_subscription_order | INT64 | |
| repeat_single_order | INT64 | |
| netrevenue_first_time | FLOAT64 | |
| netrevenue_returning | FLOAT64 | |
| orders_2nd_time ... orders_10th_time | INT64 | Nth-order counts |

**378K rows, date range**: 2020-01 to 2026-04

### Store Performance

```sql
SELECT
  store,
  SUM(orders) AS total_orders,
  SUM(net_revenue) AS revenue,
  SUM(orders_first_time) AS new_orders,
  SUM(orders_returning) AS returning_orders,
  SAFE_DIVIDE(SUM(netrevenue_first_time), SUM(orders_first_time)) AS new_aov,
  SAFE_DIVIDE(SUM(netrevenue_returning), SUM(orders_returning)) AS returning_aov
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched_aggregated`
WHERE date BETWEEN @start_date AND @end_date
GROUP BY store
```

### Repeat Purchase Analysis

```sql
SELECT
  SUM(orders_first_time) AS first_orders,
  SUM(orders_2nd_time) AS second_orders,
  SUM(orders_3rd_time) AS third_orders,
  SUM(orders_4th_time) AS fourth_orders,
  SUM(orders_5th_time) AS fifth_orders,
  SUM(orders_6th_time) AS sixth_plus_orders
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched_aggregated`
WHERE date BETWEEN @start_date AND @end_date
```

---

## 14. Return Window & Buying Pattern Tab

**Table**: `orders_return_windows` (3 charts)

One row per order with return window and maturity information.

| Column | Type | Description |
|---|---|---|
| date | DATETIME | Order date (cast to DATE) |
| order_unique_id | STRING | |
| customer_unique_id | STRING | |
| first_order_date | DATETIME | |
| billing_country | STRING | |
| store | STRING | |
| source_medium | STRING | |
| trial_buyer | STRING | "TRUE"/"FALSE" |
| return_window_group | STRING | "A0-0" (same day), "B01-30" (1-30 days), NULL (first order) |
| return_window_group2 | STRING | "A0-0", "B01-60", NULL |
| return_from_first | INT64 | Days since first order |
| return_window_days | INT64 | Days since previous order (NULL for first) |
| maturity | INT64 | Order number (1=first, 2=second, etc.) |
| item_quantity | NUMERIC | Items in order |

**387K rows, 152K distinct customers**

### Return Window Distribution

```sql
SELECT
  return_window_group,
  COUNT(*) AS orders,
  AVG(return_window_days) AS avg_days_between_orders
FROM `pelagic-core-307421.smyle_dbt_prod.orders_return_windows`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
  AND return_window_group IS NOT NULL
GROUP BY return_window_group
ORDER BY orders DESC
```

### Maturity Distribution (Order Number)

```sql
SELECT
  maturity AS order_number,
  COUNT(*) AS orders
FROM `pelagic-core-307421.smyle_dbt_prod.orders_return_windows`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
GROUP BY maturity
ORDER BY maturity
```

---

## 15. Discount / Influencer Tab

**Tables**: `orders_enriched`, `customers_enriched` (29 charts)

### customers_enriched -- Customer Master Table

One row per customer. 46 columns with full profile including NPS, subscription history,
and product ownership.

| Column | Type | Description |
|---|---|---|
| customer_email | STRING | |
| subscriber | BOOL | Ever subscribed |
| active_sub | BOOL | Currently subscribed |
| first_order_cat | STRING | "order", "trial", "subscription" |
| ebrush_owner | BOOL | |
| ebrush_order | STRING | "E-brush in first order", "E-brush not in first order", "Not a E-brush owner" |
| no_sub_anymore | STRING | "Active subscriber" (12,785), "No subscriber anymore" (55,792), "Never sub" (84,070) |
| country | STRING | Lowercase country name |
| last_order_store | STRING | "shopify" or "woo" |
| first_source_name | STRING | Acquisition source |
| first_campaign | STRING | |
| max_maturity | INT64 | Total orders placed |
| total_net_revenue | FLOAT64 | Lifetime revenue |
| total_item_quantity | NUMERIC | Lifetime items |
| recent_nps_category | STRING | "Promotor", "Passive", "Detractor" |
| recent_nps_score | FLOAT64 | |
| avg_nps_score | FLOAT64 | |

**152,647 total customers**

### Discount Code Analysis (from orders_enriched)

```sql
SELECT
  discount_code,
  COUNT(*) AS orders,
  SUM(net_revenue) AS revenue,
  SAFE_DIVIDE(SUM(net_revenue), COUNT(*)) AS aov
FROM `pelagic-core-307421.smyle_dbt_prod.orders_enriched`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
  AND discount_code IS NOT NULL
  AND discount_code != ''
GROUP BY discount_code
ORDER BY orders DESC
LIMIT 20
```

### Customer Segment Summary

```sql
SELECT
  no_sub_anymore AS segment,
  COUNT(*) AS customers,
  SUM(total_net_revenue) AS lifetime_revenue,
  SAFE_DIVIDE(SUM(total_net_revenue), COUNT(*)) AS avg_clv
FROM `pelagic-core-307421.smyle_dbt_prod.customers_enriched`
GROUP BY no_sub_anymore
ORDER BY lifetime_revenue DESC
```

### NPS Summary

```sql
SELECT
  recent_nps_category,
  COUNT(*) AS customers,
  AVG(recent_nps_score) AS avg_score
FROM `pelagic-core-307421.smyle_dbt_prod.customers_enriched`
WHERE recent_nps_category IS NOT NULL
GROUP BY recent_nps_category
ORDER BY avg_score DESC
```

---

## 16. Product Analysis (orders_products)

**Table**: `orders_products` (13 charts)

Order-line level table -- one row per product per order. 42 columns.

| Column | Type | Description |
|---|---|---|
| date | DATETIME | Order date |
| item_sku | STRING | Product SKU |
| item_name | STRING | Product name |
| product_category | STRING | "Bamboo brushheads", "Sonic electric toothbrushes", "Toothpaste refills", etc. |
| main_product_category | STRING | "Finished products", "Components", "Packets" |
| sub_product_category | STRING | |
| specific_product_category | STRING | |
| single_vs_sub | STRING | "single sales" or "subscription" |
| fo_vs_ro | STRING | "first order" or "repurchase order" |
| item_quantity | NUMERIC | Quantity |
| item_price | FLOAT64 | Price per item |
| item_cogs | FLOAT64 | Cost of goods |
| item_refund | FLOAT64 | Refund amount |

**525K rows across 387K orders**

### Product Category Revenue

```sql
SELECT
  product_category,
  SUM(item_quantity) AS units_sold,
  SUM(item_price * item_quantity) AS gross_revenue,
  SUM(item_cogs * item_quantity) AS total_cogs
FROM `pelagic-core-307421.smyle_dbt_prod.orders_products`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
GROUP BY product_category
ORDER BY gross_revenue DESC
```

### Single vs Subscription Revenue

```sql
SELECT
  single_vs_sub,
  fo_vs_ro,
  COUNT(DISTINCT order_unique_id) AS orders,
  SUM(item_quantity) AS items
FROM `pelagic-core-307421.smyle_dbt_prod.orders_products`
WHERE CAST(date AS DATE) BETWEEN @start_date AND @end_date
GROUP BY single_vs_sub, fo_vs_ro
ORDER BY orders DESC
```

---

## 17. Profitability (P&L)

**Table**: `profitability` (5 charts)

Transaction-level P&L table combining sales revenue, COGS, refunds, and ad spend.

| Column | Type | Description |
|---|---|---|
| date_paid | DATE | Payment/spend date |
| kind | STRING | "" = ad spend row, "sale" = revenue, "refund" = refund, "capture" = capture |
| payment_method | STRING | shopify_payments, paypal, manual, Mollie - Bancontact, Mollie - iDeal, stripe, braintree |
| store | STRING | "Shopify", "Woo", "" (ad spend rows) |
| spend | FLOAT64 | Ad spend (only on kind="" rows) |
| orders | INT64 | Order count |
| item_quantity | NUMERIC | Items |
| transaction_amount | FLOAT64 | Gross transaction amount (before payment fees) |
| net_transaction_amount | FLOAT64 | Net after payment processing fees |
| cogs | FLOAT64 | Cost of goods sold |

**Key insight**: Ad spend rows have `kind=""` with no store. Sales rows have `kind="sale"`.
Refund rows have `kind="refund"` with negative `transaction_amount`.

### Full P&L Query

```sql
SELECT
  SUM(CASE WHEN kind = 'sale' THEN transaction_amount END)         AS gross_revenue,
  SUM(CASE WHEN kind = 'sale' THEN net_transaction_amount END)     AS net_revenue_after_fees,
  SUM(CASE WHEN kind = 'sale' THEN cogs END)                      AS cogs,
  SUM(CASE WHEN kind = 'sale' THEN transaction_amount - cogs END)  AS gross_profit,
  SUM(CASE WHEN kind = 'refund' THEN transaction_amount END)       AS refunds,
  SUM(CASE WHEN kind = '' THEN spend END)                          AS ad_spend,
  -- Net profit = gross revenue - COGS - ad spend + refunds (negative)
  SUM(CASE WHEN kind = 'sale' THEN transaction_amount - cogs
           WHEN kind = 'refund' THEN transaction_amount
           WHEN kind = '' THEN -spend
           ELSE 0 END)                                             AS net_profit
FROM `pelagic-core-307421.smyle_dbt_prod.profitability`
WHERE date_paid BETWEEN @start_date AND @end_date
```

### Revenue by Payment Method

```sql
SELECT
  payment_method,
  SUM(net_transaction_amount) AS net_revenue,
  COUNT(*) AS transaction_days
FROM `pelagic-core-307421.smyle_dbt_prod.profitability`
WHERE date_paid BETWEEN @start_date AND @end_date
  AND kind = 'sale'
GROUP BY payment_method
ORDER BY net_revenue DESC
```

---

## 18. Important Notes

- **Currency**: All monetary values are in EUR.
- **SAFE_DIVIDE**: Always use `SAFE_DIVIDE(a, b)` instead of `a/b` to avoid division-by-zero.
- **DATETIME tables**: `orders_enriched`, `orders_return_windows`, `orders_products` use DATETIME.
  Always cast: `WHERE CAST(date AS DATE) BETWEEN ...`
- **DATE tables**: `ads_online`, `ads_spend`, `funnel`, `orders_enriched_agg_spend`,
  `orders_enriched_aggregated`, `clv_*` tables use DATE directly.
- **billing_country inconsistency**: Mixed formats across tables:
  - `ads_online`: Tier labels (Other, Tier 1, etc.)
  - `ads_spend`: Full country names
  - `orders_enriched`: Mixed (Netherlands, NL, AT, etc.)
  - `customers_enriched`: Lowercase (netherlands, germany, etc.)
- **subscriptions_total**: Data only up to 2025-09. Not current.
- **orders_enriched_customer_dim**: Data only up to 2023-07. Historical only.
- **funnel.tiers**: Only contains "Other". Use `ads_online` for tier-based queries.
- **funnel multi-row architecture**: Spend is on Facebook/Google Ads rows, revenue is on
  Shop rows. Never SUM across all channels -- use separate subqueries and JOIN.
- **funnel GA data gap**: sessions/bounces/add_to_carts_ga/checkouts_ga/transactions_ga
  are NULL after 2023-09-11. Session-based funnel is historical only.
- **profitability.date_paid**: This table uses `date_paid` (DATE), not `date`.
  Ad spend rows have `kind=""` with empty store. Sales have `kind="sale"`.
- **Parameter placeholders**: Queries use `@start_date` and `@end_date`. Replace with
  actual date strings like `'2026-04-01'` when using `run_query()`.
