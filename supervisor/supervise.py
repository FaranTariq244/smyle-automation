"""Deterministic workflow supervisor.

Runs one workflow to completion (blocking — so it can never be killed by a
headless agent's session ending), self-heals a genuine code crash via a
bounded Claude fix step, and ALWAYS posts a Slack report (success, fixed, or
failed). Designed for unattended use (scheduler) and the dashboard "Run with
AI" button alike.

Usage:  python supervise.py <workflow_key> [run_log_path]

Exit code: 0 if the workflow ultimately succeeded, 1 otherwise (so the
dashboard/scheduler get an accurate result).
"""
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

# Be robust to non-ASCII in logs (em-dashes etc.) even when the Windows console
# is cp1252 — otherwise a stray character would crash our own logging.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import slack_notify  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SUP = ROOT / "supervisor"
PY = ROOT / "venv" / "Scripts" / "python.exe"
CLAUDE = Path(r"C:\Users\Administrator\AppData\Roaming\npm\claude.cmd")
BACKUP_DIR = SUP / "backups"


def log(msg):
    print(f"[supervisor] {msg}", flush=True)


def now_hm():
    return datetime.datetime.now().strftime("%H:%M")


def kill_chrome():
    """Kill leftover chrome/chromedriver so the next attempt has a clean profile."""
    for img in ("chrome.exe", "chromedriver.exe"):
        subprocess.run(["taskkill", "/F", "/IM", img, "/T"],
                       capture_output=True)


def run_workflow(wf, run_log, attempt):
    """Run the workflow to completion, streaming to run_log. Returns (rc, timed_out)."""
    cmd = [str(PY), "-u", str(ROOT / wf["script"])] + list(wf.get("args", []))
    timeout = int(wf.get("timeout_minutes", 45)) * 60
    log(f"Attempt {attempt}: {wf['script']} {' '.join(wf.get('args', []))} "
        f"(timeout {timeout // 60}m)")
    with open(run_log, "a", encoding="utf-8") as lf:
        lf.write(f"\n===== supervisor attempt {attempt} @ "
                 f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} =====\n")
        lf.flush()
        # Force the child to emit UTF-8 so the log we read back is clean.
        child_env = os.environ.copy()
        child_env["PYTHONUTF8"] = "1"
        child_env["PYTHONIOENCODING"] = "utf-8"
        proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=lf,
                                stderr=subprocess.STDOUT, env=child_env)
        try:
            proc.wait(timeout=timeout)
            return proc.returncode, False
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            lf.write(f"\n[supervisor] TIMED OUT after {timeout // 60} min — killed.\n")
            return -1, True


def succeeded(wf, run_log, rc, timed_out):
    if timed_out or rc != 0:
        return False
    text = Path(run_log).read_text(encoding="utf-8", errors="replace")
    return any(m in text for m in wf.get("success_markers", []))


def log_tail(run_log, n=45):
    lines = Path(run_log).read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])


def failure_reason(run_log, rc, timed_out, wf):
    if timed_out:
        return f"Timed out after {wf.get('timeout_minutes', 45)} min (workflow never finished)."
    lines = [l for l in Path(run_log).read_text(encoding="utf-8", errors="replace").splitlines() if l.strip()]
    # Prefer an explicit error/traceback line for the one-line reason.
    for l in reversed(lines):
        low = l.lower()
        if "traceback" in low or "error" in low or "exception" in low or "failed" in low:
            return l.strip()[:200]
    tail = lines[-1].strip()[:200] if lines else "no output"
    return f"Exited with code {rc}. Last line: {tail}"


def run_fix_agent(wf, run_log):
    """Ask a bounded headless Claude to fix the crash. Returns a one-line note,
    or None if the fix could not be attempted. Backs the script up first."""
    script_path = ROOT / wf["script"]
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUP_DIR / f"{Path(wf['script']).name}.{stamp}.bak"
    shutil.copy2(script_path, backup)

    known = "\n".join(f"- {x}" for x in wf.get("known_issues", [])) or "- (none)"
    prompt = f"""You are fixing a FAILED automation script. Work fast and minimally. Do NOT run the workflow or any long command.

Script that failed: {script_path}

Tail of its run log:
---
{log_tail(run_log)}
---

Known issues / gotchas for this workflow:
{known}

Your task:
1. Read the relevant part of the script.
2. Identify the root cause of THIS failure.
3. Make the MINIMAL edit to the script to fix it (edit the file in place).
4. Reply with ONE short line: what you changed and why. Nothing else.

Rules: edit only {script_path}. Do not run anything. Keep the change minimal."""

    log("Crash detected — invoking Claude to attempt a fix...")
    try:
        r = subprocess.run(
            ["cmd", "/c", str(CLAUDE), "-p", "--permission-mode", "bypassPermissions",
             "--allowedTools", "Read", "Edit", "Grep", "--model", "opus"],
            input=prompt, text=True, encoding="utf-8", errors="replace",
            capture_output=True, timeout=420, cwd=str(ROOT),
        )
    except Exception as exc:
        log(f"Fix agent could not run: {exc}")
        return None, backup
    note = (r.stdout or "").strip().splitlines()
    note = note[-1].strip() if note else "(no description returned)"
    changed = script_path.read_bytes() != backup.read_bytes()
    if not changed:
        log("Fix agent made no change to the script.")
        return None, backup
    log(f"Fix applied: {note}")
    return note, backup


def build_report(wf, run_log):
    """Run the workflow's report builder; return (report_filename, summary_text).

    The report builder is expected to print a machine-readable line:
      SUMMARY: processed=.. uploaded=.. pending=.. unmatched=..
    which we turn into a friendly one-liner for Slack. Falls back to the
    builder's 'Wrote ...' line for workflows that don't emit SUMMARY."""
    rb = wf.get("report_builder")
    if not rb:
        return None, None
    date = datetime.date.today().isoformat()
    out = ROOT / wf["report_out"].format(date=date)
    try:
        r = subprocess.run([str(PY), str(ROOT / rb), str(run_log), str(out)],
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace", timeout=180, cwd=str(ROOT))
    except Exception as exc:
        log(f"Report builder failed: {exc}")
        return None, None
    lines = (r.stdout or "").strip().splitlines()

    raw = next((l for l in lines if l.startswith("SUMMARY:")), None)
    if raw:
        c = {}
        for tok in raw[len("SUMMARY:"):].split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                if v.isdigit():
                    c[k] = v
        parts = []
        if "processed" in c:
            parts.append(f"Processed *{c['processed']}*")
        if "uploaded" in c:
            parts.append(f":white_check_mark: {c['uploaded']} uploaded")
        if "pending" in c:
            parts.append(f":hourglass_flowing_sand: {c['pending']} not shipped yet")
        if "unmatched" in c:
            parts.append(f":grey_question: {c['unmatched']} unmatched")
        if parts:
            return out.name, " · ".join(parts)

    wrote = next((l for l in lines if l.startswith("Wrote")), lines[-1] if lines else None)
    return out.name, wrote


def count_errors(run_log):
    """Count genuine [ERROR] lines (the benign my-fulfilment 500 retries are
    logged at WARNING, so they are not counted)."""
    text = Path(run_log).read_text(encoding="utf-8", errors="replace")
    return sum(1 for l in text.splitlines() if "[ERROR]" in l)


def _issue_signature(line):
    """Collapse a log line to a repeatable 'kind of problem' by stripping the
    per-item noise (order refs, ids, retry counters) so identical problems
    across many orders group into one counted category."""
    msg = line
    for tag in ("[ERROR]", "[WARNING]"):
        if tag in msg:
            msg = msg.split(tag, 1)[1]
            break
    msg = msg.strip()
    msg = re.sub(r"#?SMYLE\d+", "#order", msg)               # Shopify order refs
    msg = re.sub(r"\b\d{9,}\b", "<id>", msg)                  # long TikTok ids
    msg = re.sub(r"\s*\(attempt \d+/\d+\)", "", msg)          # retry counters
    msg = re.sub(r"\s*[—-]?\s*retrying in \d+s", "", msg)     # retry backoff
    return re.sub(r"\s{2,}", " ", msg).strip()


def summarize_issues(run_log, max_types=3):
    """Return a short, human-readable breakdown of the errors and warnings in a
    run log so the Slack report shows WHAT went wrong, not just a count.

    Returns (error_bullets, warn_total, warn_bullets) where *_bullets are
    Slack-ready lines like '   • 26× my-fulfilment.com unavailable (HTTP 500)'."""
    text = Path(run_log).read_text(encoding="utf-8", errors="replace")
    errs, warns = Counter(), Counter()
    for l in text.splitlines():
        if "[ERROR]" in l:
            errs[_issue_signature(l)] += 1
        elif "[WARNING]" in l:
            warns[_issue_signature(l)] += 1

    def bullets(counter, n):
        out = []
        for sig, cnt in counter.most_common(n):
            if sig and len(sig) > 120:
                sig = sig[:117] + "..."
            if sig:
                out.append(f"   • {cnt}× {sig}")
        return out

    return bullets(errs, max_types), sum(warns.values()), bullets(warns, max_types)


def login_required(wf, run_log):
    """If the failure is an expired/missing login (not a code bug), return the
    help text to show the human; else None."""
    markers = wf.get("login_required_markers", [])
    if not markers:
        return None
    text = Path(run_log).read_text(encoding="utf-8", errors="replace")
    if any(m in text for m in markers):
        return wf.get("login_help", "A manual login is required on the server.")
    return None


def send(msg, ping=False):
    try:
        slack_notify.send(msg, notify_channel=ping)
        log("Slack report sent.")
    except Exception as exc:
        log(f"Slack send FAILED: {exc}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python supervise.py <workflow_key> [run_log]")
        sys.exit(2)
    key = sys.argv[1]
    registry = json.loads((SUP / "workflows.json").read_text(encoding="utf-8"))
    if key not in registry:
        print(f"Unknown workflow '{key}'")
        sys.exit(2)
    wf = registry[key]
    name = wf["name"]

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if len(sys.argv) > 2 and sys.argv[2].strip():
        run_log = Path(sys.argv[2].strip())
    else:
        run_log = ROOT / "logs" / f"supervisor_{key}_{stamp}.log"
    run_log.parent.mkdir(parents=True, exist_ok=True)

    max_fix = int(wf.get("max_fix_attempts", 1))
    start = now_hm()
    fix_note = None
    backup = None
    rc = timed_out = None

    attempt = 1
    while True:
        rc, timed_out = run_workflow(wf, run_log, attempt)
        if succeeded(wf, run_log, rc, timed_out):
            # SUCCESS -------------------------------------------------------
            report_file, summary = build_report(wf, run_log)
            end = now_hm()
            if fix_note:  # recovered after a fix
                lines = [
                    f"*{name}*",
                    ":wrench: Fixed & Recovered",
                    f":hammer_and_wrench: Fix: {fix_note}",
                    f":white_check_mark: {summary or 'Completed successfully'}",
                ]
                if report_file:
                    lines.append(f":page_facing_up: Report: {report_file}")
                if backup:
                    lines.append(f":floppy_disk: Backup of original: `{backup.name}`")
                lines.append(":eyes: Please review the change when you can")
                send("\n".join(lines), ping=True)
            else:
                errs = count_errors(run_log)
                lines = [f"*{name}*", ":white_check_mark: Success"]
                if summary:
                    lines.append(f":package: {summary}")
                if report_file:
                    lines.append(f":page_facing_up: Report saved: {report_file}")
                if errs:
                    err_bullets, warn_total, warn_bullets = summarize_issues(run_log)
                    lines.append(f":warning: {errs} error line(s) — top issues:")
                    lines.extend(err_bullets)
                    if warn_total:
                        lines.append(f":grey_exclamation: {warn_total} warning(s), most common:")
                        lines.extend(warn_bullets[:2])
                lines.append(f"_Ran {start}–{end} · {'clean run' if not errs else 'completed with minor errors'}_")
                send("\n".join(lines), ping=False)
            sys.exit(0)

        # FAILURE ----------------------------------------------------------
        reason = failure_reason(run_log, rc, timed_out, wf)
        log(f"Attempt {attempt} failed: {reason}")
        kill_chrome()  # clean slate before any retry

        # Login required? Not a code bug — never auto-fix, escalate immediately.
        help_text = login_required(wf, run_log)
        if help_text:
            log("Login required — escalating to Slack (no auto-fix).")
            send("\n".join([
                f"*{name}*",
                ":lock: Login required — needs attention",
                ":key: The saved session expired — a human needs to log in on the server.",
                help_text,
                f":paperclip: Log: `{run_log}`",
            ]), ping=True)
            sys.exit(1)

        # Only auto-fix genuine crashes (not timeouts), within the budget.
        if not timed_out and attempt <= max_fix:
            fix_note, backup = run_fix_agent(wf, run_log)
            if fix_note:
                attempt += 1
                continue  # rerun with the fix applied

        # Give up → FAILED report
        end = now_hm()
        lines = [
            f"*{name}*",
            ":rotating_light: FAILED — needs attention"
            + (f" (after {attempt} attempts)" if attempt > 1 else ""),
            f":x: {reason}",
        ]
        if fix_note:
            lines.append(f":hammer_and_wrench: I tried a fix ({fix_note}) but it still failed.")
        else:
            lines.append(":mag: Could not auto-fix — needs a human.")
        lines.append(f":paperclip: Log: `{run_log}`")
        lines.append(f"_Ran {start}–{end}_")
        send("\n".join(lines), ping=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
