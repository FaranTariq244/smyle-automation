# WEEKLY Sheet Layout Requirements

Everything the `SMYLE_ONLINE_STRATEGY_RN_FC1_WEEKLY` automation depends on in the Google Sheet. If any of these are renamed, removed, or moved, the automation will fail or skip that section.

---

## Tab Name

Must be named `WEEKLY` (case-insensitive), or the URL in settings must include the correct `gid=`.

---

## Row 3 — Month Headers

Must be pre-placed. The automation cannot create weeks under a month that isn't here.

```
Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec
```

Full names also accepted: `January`, `February`, etc. Match is case-insensitive.

---

## Column A — All Required Labels

### Weekly Row

Scanned in column A up to row 500. Must contain exactly:

```
Weekly
```

Week labels (e.g. `wk 18 27-3 April`) are auto-created by the automation in this row.

---

### Totals (below Weekly row)

```
Total
Recurring
New
```

---

### META (Facebook all-countries)

Section header:
```
META
```

Row labels:
```
Impressions
Clicks
CTR
ORDERS
CR
TURNOVER
SPEND
AOV
CPO
ROAS
cpc
DAILY SPEND
DAILY ORDERS
```

---

### GOOGLE (all-countries)

Section header:
```
GOOGLE
```

Row labels:
```
Google Turnover
Spend
ORDERS
CPO
AOV
ROAS
CR
CTR
DAILY SPEND
DAILY ORDERS
```

---

### Per-Country META Sections

Section headers (each one is a separate section):
```
META NL
META BE
META DE/AU/SW
REO
META UK
```

Each must have these row labels:
```
impressions
clicks
CTR
ORDERS
CR
TURNOVER
SPEND
AOV
CPA
ROAS
cpc
ORDERS PER DAY
```

---

### Per-Country GOOGLE Sections

Section headers (each one is a separate section):
```
GOOGLE NL/BE
GOOGLE DE/AU/SW
GOOGLE REO
GOOGLE UK
```

Each must have these row labels:
```
Google Turnover
Spend
clicks
ORDERS
CPA
AOV
ROAS
CR
CTR
CPC
ORDERS PER DAY
```

---

### EMAIL (Klaviyo)

Section header:
```
EMAIL
```

Row labels:
```
Email turnover
% flows
% campaigns
Listgrowth rate
List size
Open rate
Click rate
Unsub rate
Spam complaint
Placed order rate
```

---

## Constraints

- Row labels must be within **40 rows** of their section header
- Labels are matched **case-insensitive** and whitespace-normalized
- Week labels are auto-created (format: `wk N DD-DD Month` or `wk N DD Mon - DD Mon`)
- EMAIL List growth reads the previous week's `List size` from the column to the left
- Month headers in Row 3 define zones — weeks are placed in the columns between two month headers
- Formulas (`cpc`, `CPC`, `DAILY SPEND`, `DAILY ORDERS`, `ORDERS PER DAY`) are auto-generated referencing SPEND/Clicks/ORDERS rows in the same column
