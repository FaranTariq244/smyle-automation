"""
Column mapping for Google Sheets integration.
IMPORTANT: This is the single source of truth for column headers.
Do not modify the order or labels without updating this file.

Column mapping:
- Column D = Day
- Column E = Conversion % of overall
- Column F = Spend of overall
- Column G = ROAS of overall
- Column H = AOV of overall
- Column I = Conversions of overall
- Column J = Conversion % of Facebook
- Column K = Spend of Facebook
- Column L = ROAS of Facebook
- Column M = AOV of Facebook
- Column N = Conversions of Facebook
- Column O = Conversion % of Google
- Column P = Spend of Google
- Column Q = ROAS of Google
- Column R = AOV of Google
- Column S = Conversions of Google
"""

# Columns A, B, C are assumed to exist before column D
# Adjust if your sheet has different columns before "Day"
COLUMNS = [
    "",  # Column A (placeholder - adjust if needed)
    "",  # Column B (placeholder - adjust if needed)
    "",  # Column C (placeholder - adjust if needed)
    "Days",  # Column D
    "Conversion (<4 = red, 4-5 = orange, 5 > green)",  # Column E
    "Total spend per day",  # Column F
    "ROAS total",  # Column G
    "AOV total",  # Column H
    "Conversies total per day",  # Column I
    "Conversion meta (<3 = red, 3-4 = orange, 4 > green)",  # Column J
    "Total spend per day in Meta excl. lead gen ad",  # Column K
    "Roas Meta",  # Column L
    "AOV meta",  # Column M
    "Conversions Meta per day",  # Column N
    "Conversions (<4 = red, 4-5 = orange, 5 > green)",  # Column O
    "Total spend per day in Google",  # Column P
    "Roas Google",  # Column Q
    "AOV",  # Column R
    "Conversions Google per day",  # Column S
]
