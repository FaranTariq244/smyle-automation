"""Post a message to the Smyle automation Slack Incoming Webhook.

The webhook URL is read from supervisor/slack.env (git-ignored) or the
SLACK_WEBHOOK_URL environment variable — it is never hard-coded or logged.

Usage:
    python slack_notify.py "message text"          # normal message
    python slack_notify.py --channel "text"        # prefixes <!channel> ping
    echo "text" | python slack_notify.py -         # read message from stdin
"""
import json
import os
import sys
import urllib.request
from pathlib import Path

ENV_FILE = Path(__file__).resolve().parent / "slack.env"


def load_webhook_url():
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url and ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("SLACK_WEBHOOK_URL=") and "=" in line:
                url = line.split("=", 1)[1].strip()
                break
    if not url:
        raise RuntimeError(
            "No Slack webhook URL found (set SLACK_WEBHOOK_URL or supervisor/slack.env)."
        )
    return url


def send(text, notify_channel=False):
    """Post `text` to Slack. Returns True on HTTP 200."""
    if notify_channel:
        text = "<!channel> " + text
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        load_webhook_url(), data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", "replace")
        ok = resp.status == 200 and body == "ok"
        print(f"Slack responded: HTTP {resp.status} '{body}'")
        return ok


def main():
    args = sys.argv[1:]
    notify_channel = False
    if args and args[0] == "--channel":
        notify_channel = True
        args = args[1:]
    if not args:
        print("Usage: python slack_notify.py [--channel] \"message\"")
        sys.exit(2)
    text = sys.stdin.read() if args[0] == "-" else " ".join(args)
    ok = send(text, notify_channel=notify_channel)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
