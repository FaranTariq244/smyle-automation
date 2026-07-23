"""Dummy FAILING workflow — validates the supervisor's FAILED Slack report."""
import sys

print("[INFO] Dummy failing workflow started (validates failure reporting).", flush=True)
print("[ERROR] Simulated crash: something went wrong on purpose.", flush=True)
sys.exit(1)
