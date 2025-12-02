"""
Test to verify the date matching logic works correctly
"""
from datetime import datetime

# Simulate the fixed logic
date_obj = datetime(2025, 11, 1)

# Format date both ways
date_str = date_obj.strftime('%b %d')  # "Nov 01"
date_str_alt = date_obj.strftime('%b %d').replace(' 0', ' ')  # "Nov 1"

print(f"Testing date matching for: {date_obj.strftime('%B %d, %Y')}")
print(f"  Format 1 (with leading zero): '{date_str}'")
print(f"  Format 2 (without leading zero): '{date_str_alt}'")
print()

# Simulate cell values from the sheet
test_cells = [
    "Nov 1",      # Should match
    "Nov 01",     # Should match
    "Nov 1 2025", # Should match
    "Nov 01 2025",# Should match
    "Nov 10",     # Should NOT match
    "Nov 19",     # Should NOT match
    "Oct 31",     # Should NOT match
]

print("Testing matches:")
for cell_value in test_cells:
    cell_str = str(cell_value).strip()

    # Use the same logic as the fix
    matches = (cell_str == date_str or cell_str == date_str_alt or
               cell_str.startswith(date_str + ' ') or cell_str.startswith(date_str_alt + ' '))

    result = "[MATCH]" if matches else "[NO MATCH]"
    print(f"  '{cell_value}' -> {result}")
