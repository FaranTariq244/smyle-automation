"""Dummy workflow simulating an expired TikTok session (login-required path)."""
import sys

print("[INFO] Dummy workflow simulating an expired TikTok session.", flush=True)
print("[ERROR] Not logged in to TikTok Seller Center and running headless — "
      "run once WITHOUT --headless to log in manually.", flush=True)
sys.exit(1)
