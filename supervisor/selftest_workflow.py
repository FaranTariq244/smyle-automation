"""Dummy workflow to validate the supervisor pipeline end-to-end.

Prints a few log lines and the success marker so the supervisor agent can
exercise: run -> watch -> detect success -> build (none) -> Slack. ~5 seconds.
"""
import time

print("==================== SELF TEST: Supervisor pipeline check ====================", flush=True)
print("[INFO] Dummy workflow started (validates the supervisor, touches nothing real).", flush=True)
for i in range(1, 4):
    print(f"[INFO] Fake step {i}/3 ...", flush=True)
    time.sleep(1)
print("[INFO] Processed 3 fake orders: 2 ok, 1 skipped.", flush=True)
print("Workflow completed successfully.", flush=True)
