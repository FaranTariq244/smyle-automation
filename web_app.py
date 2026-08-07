"""
Smyle Automation Suite - Web Application
Flask-based web interface replacing the Tkinter desktop GUI.

Run with: python web_app.py
Then open: http://localhost:5002
"""

from __future__ import annotations

import os
import sys
import json
import time
import queue
import threading
import subprocess
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

import requests
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

# Ensure imports work
PROJECT_ROOT = Path(__file__).resolve().parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config_store import get_setting, get_settings, set_settings, set_setting
from scheduler import RECURRENCE_CHOICES, ScheduleStore, SchedulerService

app = Flask(__name__)
app.config['SECRET_KEY'] = 'smyle-automation-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global state
class AppState:
    def __init__(self):
        self.running = False
        self.current_task: Optional[str] = None
        self.current_date_str: str = ""
        self.current_process: Optional[subprocess.Popen] = None
        self.current_pid: Optional[int] = None
        self.stop_requested = False
        self.current_run_origin: str = "manual"
        self.current_schedule_id: Optional[int] = None
        self.current_log_path: Optional[Path] = None
        self.last_log_path: Optional[Path] = None
        self.message_queue: queue.Queue[str] = queue.Queue()
        self.scheduler_store = ScheduleStore()
        self.scheduler_service: Optional[SchedulerService] = None
        self.log_dir = PROJECT_ROOT / "logs"
        self.log_dir.mkdir(exist_ok=True)
        self._completion_in_progress = False

state = AppState()

# Settings keys
SETTINGS_KEYS = [
    "SPREAD_SHEET_NAME",
    "WORK_SHEET_NAME",
    "ORDER_TYPE_SHEET_URL",
    "DAILY_ADD_TRACKER_SHEET_URL",
    "DATADS_SHEET_URL",
    "SMYLE_ONLINE_STRATEGY_RN_FC1_WEEKLY_SHEET_URL",
    "ORDER_EXPORT_SHEET_URL",
]

# API keys stored with a "secret:" prefix so GET never returns the raw value
API_KEY_SETTINGS = [
    "KLAVIYO_API_KEY",
]

# Workflows: standalone pipeline scripts runnable from the Workflows page.
# Each script must be self-contained, stream progress to stdout, and exit
# 0 on success. Browser mode is passed via the HEADLESS_MODE env var.
WORKFLOWS = {
    "tiktok_toship": {
        "name": "TikTok To-Ship Tracking Sync",
        "description": "Exports all 'awaiting shipment' orders from TikTok Seller "
                       "Center, matches each to its Shopify order by TikTok Order ID, "
                       "looks up the shipped tracking number in my-fulfilment.com "
                       "(Nic. Oud), and reports it. Shipped orders are prepared for "
                       "TikTok tracking upload.",
        "source": "TikTok Seller Center",
        # Systems this workflow touches — shown as chips on the card.
        "systems": ["TikTok Seller Center", "Shopify", "my-fulfilment.com"],
        "output": "reports/tiktok_exports",
        "script": "tiktok_toship_export_raw.py",
        # Supervised by the headless Claude Code agent (supervisor/) — runs on a
        # schedule, self-heals on failure, and reports to Slack. Drives the "AI"
        # badge on the workflow card.
        "ai_supervised": True,
    },
}

DEFAULT_MAX_LOG_FILES = 100


# ============================================================================
# Utility Functions
# ============================================================================

def parse_date(date_str: str) -> tuple[datetime, str] | None:
    """Parse date string to datetime object."""
    raw_value = date_str.strip() if date_str else ""
    if not raw_value:
        date_obj = datetime.now() - timedelta(days=1)
        return date_obj, date_obj.strftime("%d-%b-%Y")

    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            date_obj = datetime.strptime(raw_value, fmt)
            return date_obj, date_obj.strftime("%d-%b-%Y")
        except ValueError:
            continue
    return None


def broadcast_log(message: str):
    """Send log message to all connected clients."""
    socketio.emit('log_message', {'message': message})


def broadcast_status(status: str, running: bool):
    """Send status update to all connected clients."""
    socketio.emit('status_update', {'status': status, 'running': running})


def build_subprocess_code(task: str, date_str: str, end_date_str: str = "") -> str:
    """Build inline Python code for the child process to execute.

    ``task`` may be a single key ("daily"), comma-separated ("daily,order"),
    or "all" to run every report.  "datads_weekly" triggers the weekly DataAds
    pipeline with a date range.
    """
    return f"""
import sys
from datetime import datetime
from run_all_reports import run_daily_report, run_order_type_report, run_add_tracker_report, run_datads_report, run_datads_weekly_report, run_smyle_online_strategy_rn_fc1_weekly_report
date_str = "{date_str}"
date_obj = datetime.strptime(date_str, "%d-%b-%Y")
end_date_str = "{end_date_str}"
end_date_obj = datetime.strptime(end_date_str, "%d-%b-%Y") if end_date_str else None

tasks = "{task}".split(",")
run_all = "all" in tasks

try:
    results = []
    if run_all or "daily" in tasks:
        results.append(run_daily_report(date_obj, date_str))
    if run_all or "order" in tasks:
        results.append(run_order_type_report(date_obj, date_str))
    if run_all or "addtracker" in tasks:
        results.append(run_add_tracker_report(date_obj, date_str))
    if run_all or "datads" in tasks:
        results.append(run_datads_report(date_obj, date_str))
    if "datads_weekly" in tasks and end_date_obj:
        results.append(run_datads_weekly_report(date_obj, end_date_obj, date_str, end_date_str))
    if "weekly" in tasks and end_date_obj:
        results.append(run_smyle_online_strategy_rn_fc1_weekly_report(date_obj, end_date_obj, date_str, end_date_str))
    ok = all(results) if results else False
    sys.exit(0 if ok else 1)
except Exception as exc:
    print(f"Unexpected error: {{exc}}")
    sys.exit(1)
"""


def build_subprocess_env() -> dict:
    """Ensure child process emits UTF-8."""
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def start_log_file(task_name: str, date_str: str, origin: str) -> Path | None:
    """Start a log file for the current run."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_task = task_name.replace(" ", "_").lower()
        filename = f"{origin}_{safe_task}_{timestamp}.log"
        path = state.log_dir / filename
        header = (
            f"{task_name} for {date_str} ({origin})\n"
            f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'-' * 60}\n"
        )
        path.write_text(header, encoding="utf-8")
        return path
    except Exception:
        return None


def cleanup_old_logs():
    """Delete oldest log files when count exceeds MAX_LOG_FILES setting."""
    try:
        max_logs = int(get_setting("MAX_LOG_FILES") or DEFAULT_MAX_LOG_FILES)
        if max_logs <= 0:
            return
        log_files = sorted(state.log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime)
        excess = len(log_files) - max_logs
        if excess > 0:
            for f in log_files[:excess]:
                try:
                    f.unlink()
                except Exception:
                    pass
    except Exception:
        pass


def watch_process_output(proc: subprocess.Popen, task: str, date_str: str):
    """Watch process output and stream to websocket."""
    try:
        if proc.stdout:
            for line in proc.stdout:
                broadcast_log(line)
                if state.current_log_path:
                    try:
                        with state.current_log_path.open("a", encoding="utf-8") as f:
                            f.write(line)
                    except Exception:
                        pass
        proc.wait()
    finally:
        success = proc.returncode == 0 and not state.stop_requested
        on_task_complete(task, date_str, success)


def on_task_complete(task: str, date_str: str, success: bool):
    """Handle task completion."""
    if state._completion_in_progress:
        return
    state._completion_in_progress = True

    task_name = _task_display_name(task)

    status = "completed successfully"
    if state.stop_requested:
        status = "stopped by user"
    elif not success:
        status = "finished with issues"

    broadcast_log(f"\n{task_name} {status} for {date_str}\n")
    broadcast_status(f"{task_name} {status} for {date_str}", False)

    state.current_process = None
    state.current_pid = None
    state.current_task = None
    state.running = False

    success_flag = success and not state.stop_requested
    if state.current_schedule_id and state.scheduler_service:
        state.scheduler_service.mark_run_complete(
            state.current_schedule_id,
            success_flag,
            message=status,
            log_path=str(state.current_log_path) if state.current_log_path else None,
        )

    state.last_log_path = state.current_log_path
    state.current_log_path = None
    state.current_run_origin = "manual"
    state.current_schedule_id = None
    state._completion_in_progress = False

    cleanup_old_logs()


def kill_process_tree(pid: int | None):
    """Best-effort kill of a process tree (Windows-friendly)."""
    if not pid:
        return
    try:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def kill_profile_processes():
    """Kill chromedriver process only (keep Chrome open for reuse)."""
    # Only kill chromedriver - Chrome stays open for next run
    try:
        subprocess.run(
            ["taskkill", "/F", "/IM", "chromedriver.exe"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


# ============================================================================
# Scheduler Integration
# ============================================================================

def on_schedule_due(schedule: dict) -> bool:
    """Callback from the background scheduler thread."""
    if state.running:
        broadcast_log("Scheduled job is due but another run is active. Will retry soon.\n")
        return False

    schedule_id = schedule.get("id")
    task = schedule.get("task", "all")

    # Workflows are scheduled with a "workflow:<key>" task and run via their
    # own runner (no report date logic applies).
    if task.startswith("workflow:"):
        wf_key = task.split(":", 1)[1]
        if wf_key not in WORKFLOWS:
            broadcast_log(f"Scheduled workflow '{wf_key}' is unknown — skipping.\n")
            return False
        if schedule_id:
            state.scheduler_store.mark_running(schedule_id, "Triggered automatically")
        # AI mode runs through the Claude supervisor (self-heals, builds the
        # Excel report, posts to Slack); normal mode runs the plain script.
        run_mode = (schedule.get("run_mode") or "normal").lower()
        if run_mode == "ai" and WORKFLOWS[wf_key].get("ai_supervised"):
            start_supervised_workflow(wf_key, origin="scheduled", schedule_id=schedule_id)
        else:
            start_workflow(wf_key, headless=True, origin="scheduled", schedule_id=schedule_id)
        return True

    # The tracking sync derives its own rolling window from the saved config,
    # so no report date logic applies here either.
    if task.startswith("tracking_sync"):
        sync_id = task.split(":", 1)[1] if ":" in task else "default"
        cfg = get_sync_schedule(sync_id)
        if cfg is None:
            broadcast_log(f"Tracking sync '{sync_id}' no longer exists — skipping.\n")
            return False
        if schedule_id:
            state.scheduler_store.mark_running(schedule_id, "Triggered automatically")
        start_tracking_sync(cfg, origin="scheduled", schedule_id=schedule_id)
        return True

    # Start the scheduled job
    days_ago = max(0, int(schedule.get("run_for_days_ago") or 1))
    target_date = datetime.combine(
        (datetime.now() - timedelta(days=days_ago)).date(), datetime.min.time()
    )
    date_str = target_date.strftime("%d-%b-%Y")

    if schedule_id:
        state.scheduler_store.mark_running(schedule_id, "Triggered automatically")

    state.current_schedule_id = schedule_id
    state.current_run_origin = "scheduled"

    # Check if datads_weekly or weekly is in the task list - calculate weekly date range
    end_date_str = ""
    if "datads_weekly" in task or "weekly" in task.split(","):
        # For weekly tasks: end date = days_ago, start date = days_ago + 6
        # This gives a 7-day range ending on the target date
        weekly_start = target_date - timedelta(days=6)
        end_date_str = date_str  # end = target date
        date_str = weekly_start.strftime("%d-%b-%Y")  # start = 6 days before

    start_task_with_date(
        task,
        target_date,
        date_str,
        origin="scheduled",
        end_date_str=end_date_str,
    )
    return True


def _task_display_name(task: str) -> str:
    """Return a human-readable name for a task string (single or comma-separated)."""
    if task.startswith("workflow:"):
        wf = WORKFLOWS.get(task.split(":", 1)[1])
        return wf["name"] if wf else task
    labels = {
        "tracking_sync": "Tracking Sync",
        "all": "All reports",
        "daily": "Daily Report",
        "order": "Order Type Report",
        "addtracker": "Daily Add Tracker",
        "datads": "DataAds Daily",
        "datads_weekly": "DataAds Weekly",
        "weekly": "SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly)",
    }
    if task in labels:
        return labels[task]
    parts = [t.strip() for t in task.split(",") if t.strip()]
    if len(parts) == 3:
        return "All reports"
    named = [labels.get(p, p) for p in parts]
    return " + ".join(named) if named else "Automation"


def start_task_with_date(task: str, date_obj: datetime, date_str: str, origin: str,
                         end_date_str: str = "", headless: bool = True):
    """Start a task with a specific date (and optional end date for weekly)."""
    task_name = _task_display_name(task)
    display_date = f"{date_str} to {end_date_str}" if end_date_str else date_str

    origin_label = "Scheduled run" if origin == "scheduled" else "Manual run"
    state.running = True
    state.stop_requested = False
    state.current_task = task
    state.current_date_str = date_str
    state.current_run_origin = origin
    state._completion_in_progress = False
    state.last_log_path = None
    state.current_log_path = start_log_file(task_name, display_date, origin)

    broadcast_status(f"{origin_label}: Running {task_name} for {display_date}...", True)
    broadcast_log(f"\n{'=' * 80}\n{origin_label} - {task_name} for {display_date}\n{'=' * 80}\n")

    # Launch subprocess
    code = build_subprocess_code(task, date_str, end_date_str=end_date_str)
    cmd = [sys.executable, "-u", "-c", code]
    env = build_subprocess_env()
    env["HEADLESS_MODE"] = "1" if headless else "0"
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        state.current_process = proc
        state.current_pid = proc.pid
    except Exception as exc:
        broadcast_log(f"\nFailed to start process: {exc}\n")
        broadcast_status("Failed to start process", False)
        state.running = False
        return

    thread = threading.Thread(
        target=watch_process_output, args=(proc, task, date_str), daemon=True
    )
    thread.start()


def start_workflow(key: str, headless: bool, origin: str = "manual",
                   schedule_id: Optional[int] = None) -> None:
    """Start a workflow script as a subprocess, streaming output like reports."""
    wf = WORKFLOWS[key]
    started_str = datetime.now().strftime("%d-%b-%Y %H:%M")

    state.running = True
    state.stop_requested = False
    state.current_task = f"workflow:{key}"
    state.current_date_str = started_str
    state.current_run_origin = origin
    state.current_schedule_id = schedule_id
    state._completion_in_progress = False
    state.last_log_path = None
    state.current_log_path = start_log_file(wf["name"], started_str, origin)

    mode = "headless" if headless else "visible"
    origin_label = "Scheduled run" if origin == "scheduled" else "Manual run"
    broadcast_status(f"{origin_label}: {wf['name']} ({mode})...", True)
    broadcast_log(f"\n{'=' * 80}\n{origin_label} - Workflow: {wf['name']} ({mode} browser)\n{'=' * 80}\n")

    # Workflows may declare fixed CLI args; omitting the key keeps the old
    # bare-script behaviour.
    cmd = [sys.executable, "-u", str(PROJECT_ROOT / wf["script"])] + list(wf.get("args", []))
    env = build_subprocess_env()
    env["HEADLESS_MODE"] = "1" if headless else "0"
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        state.current_process = proc
        state.current_pid = proc.pid
    except Exception as exc:
        broadcast_log(f"\nFailed to start workflow: {exc}\n")
        broadcast_status("Failed to start workflow", False)
        state.running = False
        return

    thread = threading.Thread(
        target=watch_process_output, args=(proc, f"workflow:{key}", started_str), daemon=True
    )
    thread.start()


def _tail_file_to_dashboard(path: Path, stop_event: threading.Event) -> None:
    """Stream new content appended to `path` to the dashboard until stopped."""
    import time
    last = 0
    while not stop_event.is_set():
        try:
            if path.exists():
                with path.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(last)
                    chunk = f.read()
                    last = f.tell()
                if chunk:
                    broadcast_log(chunk)
        except Exception:
            pass
        time.sleep(1)
    try:  # final flush of anything written just before exit
        if path.exists():
            with path.open("r", encoding="utf-8", errors="replace") as f:
                f.seek(last)
                chunk = f.read()
            if chunk:
                broadcast_log(chunk)
    except Exception:
        pass


def watch_supervised_output(proc: subprocess.Popen, task: str, date_str: str,
                            run_log: Path) -> None:
    """Stream a supervised (AI) run: tail the workflow log live + the agent's
    final summary, then complete the task."""
    import time
    stop_event = threading.Event()
    tail = threading.Thread(target=_tail_file_to_dashboard,
                            args=(run_log, stop_event), daemon=True)
    tail.start()
    try:
        if proc.stdout:
            for line in proc.stdout:
                broadcast_log(line)  # the agent's own output (e.g. final summary)
                if state.current_log_path:
                    try:
                        with state.current_log_path.open("a", encoding="utf-8") as f:
                            f.write(line)
                    except Exception:
                        pass
        proc.wait()
    finally:
        stop_event.set()
        time.sleep(1.2)
        success = proc.returncode == 0 and not state.stop_requested
        on_task_complete(task, date_str, success)


def start_supervised_workflow(key: str, origin: str = "manual-ai",
                              schedule_id: Optional[int] = None) -> None:
    """Run a workflow through the headless Claude supervisor (self-heals, builds
    the Excel report, and posts to Slack), streaming live progress to the dashboard."""
    wf = WORKFLOWS[key]
    started_str = datetime.now().strftime("%d-%b-%Y %H:%M")

    state.running = True
    state.stop_requested = False
    state.current_task = f"workflow:{key}"
    state.current_date_str = started_str
    state.current_run_origin = origin
    state.current_schedule_id = schedule_id
    state._completion_in_progress = False
    state.last_log_path = None
    log_origin = "scheduled" if origin == "scheduled" else "manual"
    state.current_log_path = start_log_file(f"{wf['name']} (AI)", started_str, log_origin)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_log = PROJECT_ROOT / "logs" / f"supervisor_{key}_{stamp}.log"

    broadcast_status(f"AI Supervisor: {wf['name']} running...", True)
    broadcast_log(
        f"\n{'=' * 80}\nAI-Supervised run - {wf['name']}\n"
        f"The AI agent is running and watching this workflow. If it hits a problem "
        f"it will try to fix it, then build the Excel report and post a summary to Slack.\n"
        f"{'=' * 80}\n")

    bat = PROJECT_ROOT / "supervisor" / "run_supervisor.bat"
    cmd = ["cmd", "/c", str(bat), key, str(run_log)]
    env = build_subprocess_env()
    try:
        proc = subprocess.Popen(
            cmd, cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1, env=env,
        )
        state.current_process = proc
        state.current_pid = proc.pid
    except Exception as exc:
        broadcast_log(f"\nFailed to start AI supervisor: {exc}\n")
        broadcast_status("Failed to start AI run", False)
        state.running = False
        return

    thread = threading.Thread(
        target=watch_supervised_output,
        args=(proc, f"workflow:{key}", started_str, run_log), daemon=True,
    )
    thread.start()


def _workflow_last_run(wf: dict) -> Optional[Dict[str, Any]]:
    """Find the newest log of this workflow and summarize its outcome."""
    safe = wf["name"].replace(" ", "_").lower()
    logs = sorted(state.log_dir.glob(f"*_{safe}_*.log"),
                  key=lambda p: p.stat().st_mtime, reverse=True)
    if not logs:
        return None
    f = logs[0]
    try:
        content = f.read_text(encoding="utf-8", errors="replace").lower()
        if "stopped by user" in content:
            status = "stopped"
        elif "completed successfully" in content:
            status = "success"
        elif "finished with issues" in content or "workflow failed" in content:
            status = "failed"
        else:
            status = "incomplete"
    except Exception:
        status = "unknown"
    return {
        "status": status,
        "started": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
        "log": f.name,
    }


def init_scheduler():
    """Initialize the scheduler service."""
    # Don't auto-create schedules - let user create them manually

    state.scheduler_service = SchedulerService(
        state.scheduler_store,
        on_job_due=on_schedule_due,
        can_start=lambda: not state.running,
        log=lambda msg: broadcast_log(f"[scheduler] {msg}\n"),
        poll_seconds=30,
    )
    state.scheduler_service.start()


# ============================================================================
# API Routes
# ============================================================================

@app.route('/')
def index():
    """Serve the main page."""
    return render_template('index.html')


@app.route('/api/status')
def get_status():
    """Get current application status."""
    schedules = state.scheduler_store.list_schedules()
    return jsonify({
        'running': state.running,
        'current_task': state.current_task,
        'current_date': state.current_date_str,
        'origin': state.current_run_origin,
        'last_log_path': str(state.last_log_path) if state.last_log_path else None,
        'schedule': schedules[0] if schedules else None,
        'schedule_count': len(schedules),
    })


@app.route('/api/settings', methods=['GET'])
def get_settings_api():
    """Get application settings."""
    stored = get_settings(SETTINGS_KEYS)
    return jsonify(stored)


@app.route('/api/settings', methods=['POST'])
def save_settings_api():
    """Save application settings."""
    data = request.json
    payload = {key: data.get(key, "").strip() for key in SETTINGS_KEYS}
    set_settings(payload)
    return jsonify({'success': True, 'message': 'Settings saved'})


@app.route('/api/api-keys', methods=['GET'])
def get_api_keys():
    """Return API key status (set / not set) — never exposes the actual value."""
    result = {}
    for key in API_KEY_SETTINGS:
        val = get_setting(key)
        if val:
            # Show masked hint: first 3 chars + dots + last 3 chars
            if len(val) > 8:
                result[key] = val[:3] + "••••••" + val[-3:]
            else:
                result[key] = "••••••"
        else:
            result[key] = ""
    return jsonify(result)


@app.route('/api/api-keys', methods=['POST'])
def save_api_keys():
    """Save API keys. Only overwrites a key if the value is non-empty and not masked."""
    data = request.json
    saved = []
    for key in API_KEY_SETTINGS:
        val = (data.get(key) or "").strip()
        # Skip if empty or still the masked placeholder
        if not val or "••" in val:
            continue
        set_setting(key, val)
        saved.append(key)
    if saved:
        return jsonify({'success': True, 'message': f'Saved: {", ".join(saved)}'})
    return jsonify({'success': True, 'message': 'No changes'})


@app.route('/api/disabled-reports', methods=['GET'])
def get_disabled_reports():
    """Get list of disabled/deprecated reports."""
    import json as _json
    stored = get_settings(["DISABLED_REPORTS"])
    raw = stored.get("DISABLED_REPORTS", "")
    try:
        disabled = _json.loads(raw) if raw else []
    except Exception:
        disabled = []
    return jsonify({'disabled': disabled})


@app.route('/api/disabled-reports', methods=['POST'])
def save_disabled_reports():
    """Save list of disabled/deprecated reports."""
    import json as _json
    data = request.json
    disabled = data.get('disabled', [])
    set_setting("DISABLED_REPORTS", _json.dumps(disabled))
    return jsonify({'success': True, 'message': 'Report availability saved'})


@app.route('/api/datads-mappings', methods=['GET'])
def get_datads_mappings():
    """Get DataAds column mappings for daily and weekly modes."""
    import json as _json
    from services.sheets.datads_helpers import get_column_mappings, DEFAULT_COLUMN_MAPPINGS
    daily = get_column_mappings('daily')
    weekly = get_column_mappings('weekly')
    defaults = [{"datads_field": m.datads_field, "sheet_column": m.sheet_column}
                for m in DEFAULT_COLUMN_MAPPINGS]
    # Spend filter settings
    sf_raw = get_setting('DATADS_WEEKLY_SPEND_FILTER')
    spend_filter = _json.loads(sf_raw) if sf_raw else {"enabled": False, "min_spend": 1000}
    return jsonify({
        'daily': daily,
        'weekly': weekly,
        'defaults': defaults,
        'spend_filter': spend_filter,
    })


@app.route('/api/datads-mappings', methods=['POST'])
def save_datads_mappings():
    """Save DataAds column mappings."""
    import json as _json
    data = request.json
    mode = data.get('mode', 'daily')
    mappings = data.get('mappings', [])

    if mode not in ('daily', 'weekly'):
        return jsonify({'success': False, 'error': 'Invalid mode'}), 400

    key = f"DATADS_{mode.upper()}_MAPPINGS"
    set_setting(key, _json.dumps(mappings))

    # Save spend filter if provided (weekly mode)
    spend_filter = data.get('spend_filter')
    if spend_filter is not None and mode == 'weekly':
        set_setting('DATADS_WEEKLY_SPEND_FILTER', _json.dumps(spend_filter))

    return jsonify({'success': True, 'message': f'{mode.title()} mappings saved'})


@app.route('/api/schedule', methods=['GET'])
def get_schedule():
    """Get scheduler configuration (returns first schedule for legacy compat)."""
    schedules = state.scheduler_store.list_schedules()
    schedule = schedules[0] if schedules else {
        'enabled': False,
        'recurrence': 'daily',
        'time_of_day': '07:00',
        'start_date': datetime.now().strftime("%Y-%m-%d"),
        'task': 'all',
        'run_for_days_ago': 1,
        'next_run': None,
        'last_status': None,
        'last_run': None,
    }
    return jsonify({
        'schedule': schedule,
        'recurrence_choices': list(RECURRENCE_CHOICES),
    })


@app.route('/api/schedules', methods=['GET'])
def get_all_schedules():
    """Get all schedules."""
    schedules = state.scheduler_store.list_schedules()
    return jsonify({
        'schedules': schedules,
        'recurrence_choices': list(RECURRENCE_CHOICES),
    })


@app.route('/api/schedules/<int:schedule_id>', methods=['GET'])
def get_schedule_by_id(schedule_id: int):
    """Get a single schedule by ID."""
    schedule = state.scheduler_store.get(schedule_id)
    if not schedule:
        return jsonify({'success': False, 'error': 'Schedule not found'}), 404
    return jsonify({'success': True, 'schedule': schedule})


@app.route('/api/schedules/<int:schedule_id>', methods=['DELETE'])
def delete_schedule(schedule_id: int):
    """Delete a schedule by ID."""
    try:
        state.scheduler_store.delete_schedule(schedule_id)
        return jsonify({'success': True, 'message': 'Schedule deleted'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/schedules/<int:schedule_id>/toggle', methods=['POST'])
def toggle_schedule(schedule_id: int):
    """Toggle a schedule's enabled state."""
    try:
        schedule = state.scheduler_store.get(schedule_id)
        if not schedule:
            return jsonify({'success': False, 'error': 'Schedule not found'}), 404

        new_state = not schedule.get('enabled', False)
        state.scheduler_store.set_enabled(schedule_id, new_state)
        return jsonify({'success': True, 'enabled': new_state})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/schedule', methods=['POST'])
def save_schedule():
    """Save or update a schedule. Supports multiple schedules with unique keys."""
    data = request.json

    name = (data.get('name') or '').strip()
    time_val = data.get('time_of_day', '07:00').strip() or '07:00'
    start_val = data.get('start_date', '').strip() or datetime.now().strftime("%Y-%m-%d")
    recurrence = data.get('recurrence', 'daily')
    task = data.get('task', 'all')
    enabled = data.get('enabled', False)
    days_ago = max(0, int(data.get('run_for_days_ago', 1)))
    run_mode = (data.get('run_mode') or 'normal').lower()
    edit_id = data.get('edit_id')  # If editing an existing schedule

    # AI mode only applies to AI-supervised workflows; ignore it otherwise.
    if run_mode == 'ai':
        wf_key = task.split(':', 1)[1] if task.startswith('workflow:') else None
        if not (wf_key and WORKFLOWS.get(wf_key, {}).get('ai_supervised')):
            run_mode = 'normal'

    if not name:
        return jsonify({'success': False, 'error': 'Schedule name is required.'}), 400

    # Validate
    try:
        datetime.strptime(time_val, "%H:%M")
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid time format. Use HH:MM.'}), 400

    try:
        datetime.strptime(start_val, "%Y-%m-%d")
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400

    if recurrence not in RECURRENCE_CHOICES:
        return jsonify({'success': False, 'error': f'Invalid recurrence. Use one of: {", ".join(RECURRENCE_CHOICES)}'}), 400

    # Check 30-minute time conflict with other schedules
    exclude_id = int(edit_id) if edit_id else None
    conflict = state.scheduler_store.check_time_conflict(
        time_of_day=time_val,
        recurrence=recurrence,
        start_date=start_val,
        exclude_id=exclude_id,
        buffer_minutes=30,
    )
    if conflict:
        conflict_name = conflict.get('name', 'Unknown')
        conflict_time = conflict.get('time_of_day', '??:??')
        return jsonify({
            'success': False,
            'error': f'Time conflict with "{conflict_name}" (runs at {conflict_time}). '
                     f'Schedules need at least 30 minutes apart.'
        }), 409

    # Generate unique key from name (or use existing key if editing)
    import re
    if edit_id:
        existing = state.scheduler_store.get(int(edit_id))
        key = existing['key'] if existing else re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
    else:
        key = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
        # Ensure unique key
        base_key = key
        counter = 1
        while state.scheduler_store.get_by_key(key):
            key = f"{base_key}_{counter}"
            counter += 1

    schedule = state.scheduler_store.upsert_schedule(
        key=key,
        name=name,
        task=task,
        recurrence=recurrence,
        time_of_day=time_val,
        start_date=start_val,
        run_for_days_ago=days_ago,
        run_mode=run_mode,
        enabled=enabled,
    )

    if state.scheduler_service:
        state.scheduler_service.refresh_next_run(schedule["id"])

    return jsonify({'success': True, 'schedule': schedule})


def _get_disabled_reports():
    """Return list of disabled report keys."""
    import json as _json
    stored = get_settings(["DISABLED_REPORTS"])
    raw = stored.get("DISABLED_REPORTS", "")
    try:
        return _json.loads(raw) if raw else []
    except Exception:
        return []


@app.route('/api/run', methods=['POST'])
def run_task():
    """Start a report task."""
    if state.running:
        return jsonify({'success': False, 'error': 'A task is already running'}), 409

    data = request.json
    task = data.get('task', 'all')
    date_str = data.get('date', '')
    end_date_str = data.get('end_date', '')
    headless = data.get('headless', True)

    # Check for disabled reports
    disabled = _get_disabled_reports()
    if disabled:
        task_parts = [t.strip() for t in task.split(',') if t.strip()]
        if task == 'all':
            task_parts = ['daily', 'order', 'addtracker', 'datads']
        blocked = [t for t in task_parts if t in disabled]
        if blocked:
            labels = {'daily': 'Daily Report', 'order': 'Order Type', 'addtracker': 'Add Tracker',
                      'datads': 'DataAds Daily', 'datads_weekly': 'DataAds Weekly',
                      'weekly': 'SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly)'}
            names = ', '.join(labels.get(b, b) for b in blocked)
            return jsonify({'success': False, 'error': f'Disabled report(s): {names}'}), 400
        # Filter out disabled from "all"
        if task == 'all':
            task_parts = [t for t in task_parts if t not in disabled]
            task = ','.join(task_parts) if task_parts else ''
            if not task:
                return jsonify({'success': False, 'error': 'All reports are disabled'}), 400

    parsed = parse_date(date_str)
    if not parsed:
        return jsonify({'success': False, 'error': 'Invalid date format. Use DD-MMM-YYYY.'}), 400

    date_obj, formatted_date = parsed
    state.current_schedule_id = None
    state.current_run_origin = "manual"

    # Handle date-range tasks (datads_weekly, weekly) with an end date.
    if task in ('datads_weekly', 'weekly') and end_date_str:
        parsed_end = parse_date(end_date_str)
        if not parsed_end:
            return jsonify({'success': False, 'error': 'Invalid end date format.'}), 400
        _, formatted_end_date = parsed_end
        start_task_with_date(task, date_obj, formatted_date, origin="manual",
                             end_date_str=formatted_end_date, headless=headless)
        label = 'SMYLE_ONLINE_STRATEGY_RN_FC1 (Weekly)' if task == 'weekly' else 'Weekly DataAds'
        return jsonify({'success': True, 'message': f'Started {label} for {formatted_date} to {formatted_end_date}'})

    start_task_with_date(task, date_obj, formatted_date, origin="manual", headless=headless)

    return jsonify({'success': True, 'message': f'Started {task} for {formatted_date}'})


# ============================================================================
# Order Explorer — interactive lookup tool (read-only)
# ============================================================================

# Fetching portal detail costs one HTTP request per order, so an unbounded
# range with "check portal" on could run for an hour. Cap it and say so.
PORTAL_ENRICH_MAX = 150

# Bulk pushes run in the background in chunks. No hard cap on the total — a
# week's backlog is a legitimate thing to push — but the work is paced so a
# large run can't exhaust Shopify's cost bucket or block the request thread.
BULK_CHUNK = 25          # orders per chunk
BULK_CHUNK_PAUSE = 1.0   # seconds between chunks once a run is large
BULK_PACE_AFTER = 100    # runs above this size get the inter-chunk pause

# In-memory progress for the running bulk push, polled by the Orders page.
bulk_push_state: Dict[str, Any] = {
    "running": False, "done": 0, "total": 0, "written": 0,
    "failed": 0, "skipped": 0, "message": "", "results": [], "finished": False,
}
_bulk_lock = threading.Lock()


@app.route('/api/orders/lookup', methods=['POST'])
def api_orders_lookup():
    """Fetch orders for the Order Explorer. Never writes to any system."""
    body = request.get_json(silent=True) or {}
    import order_lookup as ol

    try:
        if body.get('reference'):
            ref = str(body['reference']).strip()
            ref = ref if ref.startswith('#') else f'#{ref}'
            source = body.get('source') or 'both'

            shop_row = ol.shopify_order(ref) if source in ('both', 'shopify') else None
            portal_row = None
            if source in ('both', 'fulfilment'):
                portal_row = _portal_order_resilient(ref)

            if shop_row is None and portal_row is None:
                return jsonify({'success': False,
                                'error': f'{ref} not found in either system'}), 404
            merged = {**(shop_row or {'order': ref}), **(portal_row or {})}
            merged.pop('_node', None)
            return jsonify({'success': True, 'rows': [merged],
                            'summary': _orders_summary([merged]),
                            'sync': _tracking_sync_plan(ref, shop_row, portal_row)})

        date_from, date_to = body.get('date_from'), body.get('date_to')
        if not date_from or not date_to:
            return jsonify({'success': False,
                            'error': 'Pick a start and end date.'}), 400

        source = body.get('source') or 'shopify'
        pushable_only = bool(body.get('pushable_only'))
        notes = []

        if source == 'fulfilment':
            rows = ol.portal_orders_fast(date_from, date_to)

        elif source == 'both' or body.get('with_portal'):
            # Bulk on BOTH sides, then merge in memory — no per-order lookups.
            portal = ol.portal_orders_fast(date_from, date_to,
                                           only_status=ol.DETAIL_STATUS)
            by_ref = {p['order']: p for p in portal}

            if pushable_only:
                # Only orders the portal can actually contribute a number for
                # are worth asking Shopify about. This makes the filter SAVE
                # work instead of just hiding rows after the fact.
                names = [ref for ref, p in by_ref.items() if p.get('portal_tracking')]
                notes.append(f'{len(names)} of {len(portal)} completed orders carry '
                             f'a T&T code — only those were looked up in Shopify.')
                rows = _shopify_rows_by_name(names)
                # The portal also holds Amazon/Kaufland references that will
                # never match a Shopify order. Say how many dropped out rather
                # than letting the row count quietly shrink.
                unmatched = len(names) - len(rows)
                if unmatched > 0:
                    notes.append(f'{unmatched} of those have no Shopify order '
                                 f'(other sales channels, e.g. AMZ-*).')
            else:
                rows = ol.shopify_orders(date_from, date_to,
                                         limit=body.get('limit') or None)

            missing = 0
            for row in rows:
                match = by_ref.get(row['order'])
                if match:
                    row.update({k: v for k, v in match.items() if k != 'order'})
                else:
                    row['portal_status'] = 'not in my-fulfilment.com'
                    missing += 1
                _attach_row_sync(row)
            if missing:
                notes.append(f'{missing} Shopify order(s) had no completed match '
                             f'in my-fulfilment.com for this window.')
        else:
            rows = ol.shopify_orders(date_from, date_to, limit=body.get('limit') or None)

        if body.get('untracked_only'):
            rows = [r for r in rows if r.get('has_tracking', 'no') == 'no']
        if body.get('unfulfilled_only'):
            rows = [r for r in rows if r.get('fulfillment') != 'FULFILLED']
        if pushable_only:
            rows = [r for r in rows if r.get('can_push')]

        rows = [{k: v for k, v in r.items() if not k.startswith('_')} for r in rows]
        return jsonify({'success': True, 'rows': rows,
                        'summary': _orders_summary(rows), 'notes': notes})

    except Exception as exc:
        traceback.print_exc()
        return jsonify({'success': False, 'error': _friendly_error(exc)}), 500


# ---------------------------------------------------------------------------
# Tracking-sync schedules
# ---------------------------------------------------------------------------
# A LIST of independent schedules, each with its own window, times and mode —
# so you can run "today + yesterday, apply and email" hourly while a separate
# one sweeps older days that the first deliberately skips.
#
# Each schedule expands to one scheduler row per run time, keyed
# tracking_sync@<id>@<HH:MM>, with task "tracking_sync:<id>" so the dispatcher
# knows which config to load.
TRACKING_SYNC_SETTING = "TRACKING_SYNC_SCHEDULES"
LEGACY_SYNC_SETTING = "TRACKING_SYNC_CONFIG"
TRACKING_SYNC_KEY_PREFIX = "tracking_sync@"

TRACKING_SYNC_DEFAULTS = {
    "name": "Recent orders",
    "enabled": False,
    "days_back": 2,        # window size in days
    "skip_days": 0,        # recent days to exclude (0 = window ends today)
    "times": ["08:00"],
    "apply": False,        # False = dry run, report only
    "notify_customer": True,
}


def window_for(cfg: Dict[str, Any]) -> tuple:
    """(from, to) dates this config resolves to right now."""
    end = date.today() - timedelta(days=max(0, int(cfg.get("skip_days") or 0)))
    start = end - timedelta(days=max(0, int(cfg.get("days_back") or 1) - 1))
    return start.isoformat(), end.isoformat()


def get_tracking_sync_schedules() -> list:
    """All configured sync schedules, migrating the old single config if found."""
    raw = get_setting(TRACKING_SYNC_SETTING)
    if raw:
        try:
            items = json.loads(raw)
            if isinstance(items, list):
                return [{**TRACKING_SYNC_DEFAULTS, **i} for i in items]
        except (ValueError, TypeError):
            print(f"{TRACKING_SYNC_SETTING} is not valid JSON — ignoring")

    legacy = get_setting(LEGACY_SYNC_SETTING)
    if legacy:
        try:
            cfg = {**TRACKING_SYNC_DEFAULTS, **json.loads(legacy)}
            cfg.setdefault("id", "default")
            return [cfg]
        except (ValueError, TypeError):
            pass
    return []


def _validate_schedule(cfg: Dict[str, Any], index: int) -> Dict[str, Any]:
    where = cfg.get("name") or f"schedule {index + 1}"

    times = sorted({str(t).strip() for t in (cfg.get("times") or []) if str(t).strip()})
    if not times:
        raise ValueError(f"{where}: add at least one run time")
    for t in times:
        hh, _, mm = t.partition(":")
        if not (hh.isdigit() and mm.isdigit() and 0 <= int(hh) < 24 and 0 <= int(mm) < 60):
            raise ValueError(f"{where}: '{t}' is not a valid HH:MM time")

    def whole(field, lo, hi, label):
        try:
            v = int(cfg.get(field))
        except (TypeError, ValueError):
            raise ValueError(f"{where}: {label} must be a whole number")
        if not lo <= v <= hi:
            raise ValueError(f"{where}: {label} must be between {lo} and {hi}")
        return v

    days_back = whole("days_back", 1, 90, "days back")
    skip_days = whole("skip_days", 0, 90, "days to skip") if cfg.get("skip_days") else 0

    return {
        "id": str(cfg.get("id") or "").strip() or f"s{index + 1}",
        "name": (cfg.get("name") or "").strip() or f"Schedule {index + 1}",
        "enabled": bool(cfg.get("enabled")),
        "days_back": days_back,
        "skip_days": skip_days,
        "times": times,
        "apply": bool(cfg.get("apply")),
        "notify_customer": bool(cfg.get("notify_customer", True)),
    }


def save_tracking_sync_schedules(items: list) -> list:
    """Persist all schedules and rebuild the scheduler rows they expand to."""
    if not isinstance(items, list):
        raise ValueError("Expected a list of schedules")

    clean = [_validate_schedule(c, i) for i, c in enumerate(items)]

    ids = [c["id"] for c in clean]
    if len(set(ids)) != len(ids):
        raise ValueError("Two schedules share the same id")

    set_setting(TRACKING_SYNC_SETTING, json.dumps(clean))

    wanted = {f"{TRACKING_SYNC_KEY_PREFIX}{c['id']}@{t}" for c in clean for t in c["times"]}
    for row in state.scheduler_store.list_schedules():
        key = row.get("key") or ""
        if key.startswith(TRACKING_SYNC_KEY_PREFIX) and key not in wanted:
            state.scheduler_store.delete_schedule(row["id"])

    for cfg in clean:
        mode = "apply" if cfg["apply"] else "dry run"
        span = (f"{cfg['days_back']}d" if not cfg["skip_days"]
                else f"{cfg['days_back']}d skipping {cfg['skip_days']}")
        for t in cfg["times"]:
            state.scheduler_store.upsert_schedule(
                key=f"{TRACKING_SYNC_KEY_PREFIX}{cfg['id']}@{t}",
                name=f"{cfg['name']} {t} ({span}, {mode})",
                task=f"tracking_sync:{cfg['id']}",
                recurrence="daily",
                time_of_day=t,
                start_date=datetime.now().strftime("%Y-%m-%d"),
                run_for_days_ago=cfg["skip_days"] + cfg["days_back"] - 1,
                enabled=cfg["enabled"],
            )
    return clean


def get_sync_schedule(schedule_id: str) -> Optional[Dict[str, Any]]:
    for cfg in get_tracking_sync_schedules():
        if str(cfg.get("id")) == str(schedule_id):
            return cfg
    return None


def tracking_sync_command(cfg: Dict[str, Any]) -> list:
    """CLI args for a scheduled tracking-sync run."""
    args = ["--days-back", str(cfg["days_back"])]
    if cfg.get("skip_days"):
        args += ["--skip-days", str(cfg["skip_days"])]
    if cfg.get("apply"):
        args.append("--apply")
        if cfg.get("notify_customer", True):
            args.append("--notify")
    return args


def start_tracking_sync(cfg: Dict[str, Any], origin: str = "scheduled",
                        schedule_id: Optional[int] = None) -> None:
    """Run the tracking sync as a subprocess, streaming output like a workflow."""
    started_str = datetime.now().strftime("%d-%b-%Y %H:%M")
    mode = "APPLY" if cfg.get("apply") else "DRY RUN"
    win_from, win_to = window_for(cfg)
    label = cfg.get("name") or "Tracking Sync"

    state.running = True
    state.stop_requested = False
    state.current_task = "tracking_sync"
    state.current_date_str = started_str
    state.current_run_origin = origin
    state.current_schedule_id = schedule_id
    state._completion_in_progress = False
    state.last_log_path = None
    state.current_log_path = start_log_file(f"Tracking Sync {label}", started_str, origin)

    broadcast_status(f"{label} ({mode}, {win_from} to {win_to})...", True)
    broadcast_log(f"\n{'=' * 80}\nTracking Sync — {label}\n{mode} · window {win_from} "
                  f"to {win_to} ({cfg['days_back']} day(s)"
                  + (f", skipping the last {cfg['skip_days']}" if cfg.get("skip_days") else "")
                  + f")\n{'=' * 80}\n")

    cmd = [sys.executable, "-u", str(PROJECT_ROOT / "sync_tracking_to_shopify.py")]
    cmd += tracking_sync_command(cfg)
    env = build_subprocess_env()
    if origin == "scheduled":
        env["SCHEDULED_RUN"] = "1"      # tags its writes in the audit log
    try:
        proc = subprocess.Popen(
            cmd, cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
            env=env,
        )
        state.current_process = proc
        state.current_pid = proc.pid
    except Exception as exc:
        broadcast_log(f"\nFailed to start tracking sync: {exc}\n")
        broadcast_status("Failed to start tracking sync", False)
        state.running = False
        return

    threading.Thread(
        target=watch_process_output, args=(proc, "tracking_sync", started_str),
        daemon=True,
    ).start()


def _friendly_error(exc: Exception) -> str:
    """Turn an exception into something actionable for whoever clicked Fetch."""
    text = str(exc)
    low = text.lower()

    if 'throttl' in low:
        return ('Shopify rate-limited this query repeatedly. Try a shorter date '
                'range, or wait a minute and retry.')
    if 'credentials not configured' in low or 'not configured' in low:
        return text        # already tells you exactly which setting is missing
    if 'login failed' in low or 'session expired' in low:
        return ('Could not sign in to my-fulfilment.com. Check MYFULFILMENT_EMAIL / '
                'MYFULFILMENT_PASSWORD in fulfilment.env.')
    if any(code in text for code in ('500', '502', '503', '504')):
        return ('my-fulfilment.com returned a server error and did not recover '
                'after retries. It is usually transient — try again shortly.')
    if 'did not respond in time while searching' in text:
        return text        # already explains the cause and the way round it
    if 'timed out' in low or 'timeout' in low:
        return ('my-fulfilment.com timed out. Its search gets slow when their '
                'server is busy — try again shortly, or use a shorter date range.')
    if 'connection' in low:
        return f'Network problem reaching an upstream system: {text[:160]}'
    return text[:400]


# How the carrier name is written into Shopify:
#   "auto"  — the detected PostNL Domestic / International name. Shopify keeps
#             updating shipment_status (in transit / delivered) for carriers it
#             integrates with, which it will NOT do for a custom name.
#   "other" — the literal "Other". Use when the detected name would be wrong;
#             the tracking URL still makes the number clickable.
CARRIER_MODE_SETTING = "TRACKING_CARRIER_MODE"


def _carrier_for_row(portal_row, state, shop_row) -> Optional[str]:
    """Carrier string to write, honouring the configured mode."""
    if (get_setting(CARRIER_MODE_SETTING) or "auto").lower() == "other":
        return shopify_fulfil_other()
    from services.shopify import fulfillment as shopify_fulfil
    return shopify_fulfil.carrier_for(
        (portal_row or {}).get('shipper', ''),
        (state or {}).get('country_code') or (shop_row or {}).get('country'))


def shopify_fulfil_other() -> str:
    from services.shopify import fulfillment as shopify_fulfil
    return shopify_fulfil.OTHER_CARRIER


def _portal_order_resilient(ref: str, attempts: int = 2):
    """Single-order portal lookup that survives a slow reference search.

    The portal's reference filter is an unindexed substring scan — usually
    ~7s, but it can blow past a 3-minute timeout when their server is cold or
    busy. A cache hit skips it entirely; otherwise we use a shorter timeout and
    retry once rather than making the user wait 180s for a single failure.
    """
    import order_lookup as ol
    from services.fulfilment import client as myf

    last = None
    for attempt in range(attempts):
        try:
            # 75s per attempt instead of 180 — a search that slow won't get
            # faster by waiting, and a retry often lands on a warm cache.
            with myf.MyFulfilmentClient(timeout=75) as c:
                c.login()
                return ol.portal_order(c, ref)
        except requests.RequestException as exc:
            last = exc
            print(f"Portal lookup for {ref} failed ({type(exc).__name__}) — "
                  f"attempt {attempt + 1}/{attempts}")
            if attempt < attempts - 1:
                time.sleep(2)
    raise RuntimeError(
        f"my-fulfilment.com did not respond in time while searching for {ref}. "
        "Its reference search is slow when the server is busy — try again in a "
        "moment, or run a date-range lookup for that day, which uses a much "
        "faster query and caches the result."
    ) from last


def _shopify_rows_by_name(names: list) -> list:
    """Fetch specific Shopify orders in parallel batches, newest first."""
    import order_lookup as ol
    from services.shopify import bulk

    nodes = bulk.fetch_by_names(names, ol._ORDER_FIELDS)
    rows = [ol._shopify_row(n) for n in nodes.values()]
    rows.sort(key=lambda r: r['created'], reverse=True)
    return rows


def _attach_row_sync(row: Dict[str, Any]) -> None:
    """Decide, in place, whether a listed row's portal tracking can be pushed.

    Uses the order node already fetched in the listing, so this costs no extra
    Shopify calls no matter how many rows there are.
    """
    row['can_push'] = False
    row['sync_action'] = ''
    node = row.get('_node')
    tnt = (row.get('portal_tracking') or '').split(',')[0].strip()
    if not node or not tnt:
        row['sync_reason'] = ('no track & trace code in my-fulfilment.com'
                              if node else 'no Shopify order')
        return
    try:
        from services.shopify import fulfillment as shopify_fulfil
        from sync_tracking_to_shopify import _decide

        state = shopify_fulfil.state_from_node(node)
        action, reason, _target = _decide(state, tnt)
        row['can_push'] = action in ('create', 'update')
        row['sync_action'] = action
        row['sync_reason'] = reason
        row['sync_carrier'] = _carrier_for_row(row, state, row)
        row['sync_url'] = shopify_fulfil.normalize_tracking_url(row.get('tracking_url'))
    except Exception as exc:
        row['sync_reason'] = f'could not evaluate: {exc}'


@app.route('/api/orders/sync-schedule', methods=['GET'])
def api_get_sync_schedule():
    """All tracking-sync schedules, each with the rows it produced."""
    configs = get_tracking_sync_schedules()
    rows_by_id: Dict[str, list] = {}
    for r in state.scheduler_store.list_schedules():
        key = r.get('key') or ''
        if not key.startswith(TRACKING_SYNC_KEY_PREFIX):
            continue
        ident, _, time_of_day = key[len(TRACKING_SYNC_KEY_PREFIX):].partition('@')
        rows_by_id.setdefault(ident, []).append({
            'time': time_of_day, 'next_run': r.get('next_run'),
            'last_run': r.get('last_run'), 'last_status': r.get('last_status'),
            'enabled': bool(r.get('enabled')),
        })

    out = []
    for cfg in configs:
        win_from, win_to = window_for(cfg)
        out.append({**cfg,
                    'window': {'from': win_from, 'to': win_to},
                    'rows': sorted(rows_by_id.get(cfg['id'], []), key=lambda r: r['time'])})
    return jsonify({'success': True, 'schedules': out,
                    'defaults': TRACKING_SYNC_DEFAULTS})


@app.route('/api/orders/sync-schedule', methods=['POST'])
def api_save_sync_schedule():
    """Replace the full set of schedules and rebuild their scheduler rows."""
    body = request.get_json(silent=True) or {}
    items = body.get('schedules')
    if items is None:
        return jsonify({'success': False, 'error': 'No schedules supplied.'}), 400
    try:
        clean = save_tracking_sync_schedules(items)
    except ValueError as exc:
        return jsonify({'success': False, 'error': str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        return jsonify({'success': False, 'error': _friendly_error(exc)}), 500

    runs = sum(len(c['times']) for c in clean)
    live = sum(len(c['times']) for c in clean if c['enabled'])
    return jsonify({
        'success': True,
        'schedules': clean,
        'message': (f"Saved {len(clean)} schedule(s), {runs} run(s) per day "
                    f"({live} enabled)"),
    })


@app.route('/api/orders/sync-schedule/run-now', methods=['POST'])
def api_run_sync_now():
    """Run one saved schedule immediately."""
    if state.running:
        return jsonify({'success': False,
                        'error': 'Another run is already in progress.'}), 409

    body = request.get_json(silent=True) or {}
    cfg = get_sync_schedule(body.get('id')) if body.get('id') else None
    if cfg is None:
        return jsonify({'success': False,
                        'error': 'Save the schedule first, then run it.'}), 404

    # An ad-hoc run can force a dry run without touching the saved config.
    if 'apply' in body:
        cfg = {**cfg, 'apply': bool(body['apply'])}
    start_tracking_sync(cfg, origin="manual")
    win_from, win_to = window_for(cfg)
    mode = 'APPLY' if cfg['apply'] else 'DRY RUN'
    return jsonify({'success': True,
                    'message': f"Started {cfg['name']} ({mode}, {win_from} to {win_to})"})


@app.route('/api/orders/push-tracking-bulk', methods=['POST'])
def api_orders_push_tracking_bulk():
    """Start a bulk push. Returns immediately; the work runs in the background.

    There is no cap on how many orders you can push — a week's backlog is a
    legitimate thing to clear. Instead the run is chunked and paced, and it is
    detached from the HTTP request so it can't be cut short by a timeout.
    """
    body = request.get_json(silent=True) or {}
    refs = [str(r).strip() for r in (body.get('references') or []) if str(r).strip()]
    if not refs:
        return jsonify({'success': False, 'error': 'No orders selected.'}), 400

    with _bulk_lock:
        if bulk_push_state['running']:
            return jsonify({'success': False,
                            'error': 'A bulk push is already running.'}), 409
        bulk_push_state.update({
            'running': True, 'done': 0, 'total': len(refs), 'written': 0,
            'failed': 0, 'skipped': 0, 'finished': False, 'results': [],
            'message': f'Starting - {len(refs)} order(s)',
        })

    notify = True if 'notify_customer' not in body else bool(body['notify_customer'])
    threading.Thread(target=_bulk_push_worker, args=(refs, notify), daemon=True).start()

    return jsonify({
        'success': True, 'started': True, 'total': len(refs), 'notified': notify,
        'message': f'Pushing {len(refs)} order(s) in the background'
                   + (' - customers will be emailed' if notify else ' - no emails'),
    })


def _bulk_push_worker(refs: list, notify: bool) -> None:
    """Push tracking for any number of orders, in paced chunks.

    Each order is still re-derived from both systems before writing - the
    browser only ever supplies references. Before every chunk we wait for
    Shopify's cost bucket to refill if it has been drawn down, so a long run
    stays inside the rate limit instead of sprinting into a THROTTLED error.
    """
    import order_lookup as ol
    from services.fulfilment import client as myf
    from services.shopify import audit, client as shopify_client
    from services.shopify import fulfillment as shopify_fulfil

    audit.set_origin('ui-bulk')
    total = len(refs)
    paced = total > BULK_PACE_AFTER
    broadcast_log(f"\nPushing tracking for {total} order(s), notify_customer={notify}"
                  + (f" - paced in chunks of {BULK_CHUNK}\n" if paced else "\n"))

    try:
        with myf.MyFulfilmentClient() as portal:
            portal.login()

            for start in range(0, total, BULK_CHUNK):
                chunk = refs[start:start + BULK_CHUNK]

                waited = shopify_client.wait_for_capacity()
                if waited:
                    broadcast_log(f"  paused {waited:.1f}s for the Shopify rate limit\n")

                for ref in chunk:
                    ref = ref if ref.startswith('#') else f'#{ref}'
                    try:
                        shop_row = ol.shopify_order(ref)
                        portal_row = ol.portal_order(portal, ref)
                        plan = _tracking_sync_plan(ref, shop_row, portal_row)

                        if not plan or not plan.get('can_push'):
                            _bulk_record(ref, False,
                                         (plan or {}).get('reason', 'nothing to push'),
                                         skipped=True)
                            continue

                        writer = (shopify_fulfil.create_fulfillment
                                  if plan['action'] == 'create'
                                  else shopify_fulfil.update_tracking)
                        res = writer(plan['target'], plan['tracking'],
                                     tracking_company=plan['carrier'],
                                     tracking_url=plan.get('tracking_url'),
                                     notify_customer=notify)
                        _bulk_record(ref, True,
                                     f"{plan['tracking']} -> {res.get('name', '')}")
                    except Exception as exc:
                        _bulk_record(ref, False, str(exc))

                if paced and start + BULK_CHUNK < total:
                    time.sleep(BULK_CHUNK_PAUSE)

        s = bulk_push_state
        summary = (f"{s['written']} written, {s['skipped']} skipped, "
                   f"{s['failed']} failed of {total}")
        broadcast_log(f"Bulk push done - {summary}\n")
        with _bulk_lock:
            bulk_push_state.update({'running': False, 'finished': True,
                                    'message': summary})
    except Exception as exc:
        traceback.print_exc()
        broadcast_log(f"Bulk push aborted - {exc}\n")
        with _bulk_lock:
            bulk_push_state.update({'running': False, 'finished': True,
                                    'message': f'Aborted: {_friendly_error(exc)}'})


def _bulk_record(ref: str, ok: bool, message: str, skipped: bool = False) -> None:
    with _bulk_lock:
        bulk_push_state['done'] += 1
        if ok:
            bulk_push_state['written'] += 1
        elif skipped:
            bulk_push_state['skipped'] += 1
        else:
            bulk_push_state['failed'] += 1
        # Keep only the tail - a multi-thousand run shouldn't grow unbounded.
        bulk_push_state['results'] = (bulk_push_state['results']
                                      + [{'order': ref, 'ok': ok, 'message': message}])[-200:]
        s = bulk_push_state
        s['message'] = (f"{s['done']}/{s['total']} - {s['written']} written, "
                        f"{s['skipped']} skipped, {s['failed']} failed")
    label = 'OK ' if ok else ('skipped - ' if skipped else 'FAILED - ')
    broadcast_log(f"  {ref}: {label}{message}\n")


@app.route('/api/orders/write-log', methods=['GET'])
def api_write_log():
    """Every write this project has made to Shopify, newest first."""
    from services.shopify import audit
    return jsonify({
        'success': True,
        'summary': audit.summary(),
        'entries': audit.recent(
            limit=int(request.args.get('limit', 100)),
            order_name=request.args.get('order', ''),
            since=request.args.get('since', ''),
            failures_only=request.args.get('failures') == '1',
        ),
    })


@app.route('/api/orders/push-tracking-bulk/status', methods=['GET'])
def api_bulk_push_status():
    """Progress of the running (or last) bulk push."""
    with _bulk_lock:
        return jsonify({'success': True, **bulk_push_state})


def _tracking_sync_plan(reference: str, shop_row, portal_row) -> Optional[Dict[str, Any]]:
    """Work out whether this order's portal tracking can be pushed to Shopify.

    Returns None unless BOTH systems were queried and returned the order — the
    button is only meaningful when we have the two sides to compare. Otherwise
    returns the decision so the UI can show (or hide) the push button, with the
    exact values that would be written.
    """
    if shop_row is None or portal_row is None:
        return None

    tnt = (portal_row.get('portal_tracking') or '').split(',')[0].strip()
    if not tnt:
        return {'can_push': False,
                'reason': 'my-fulfilment.com has no track & trace code for this order.'}

    try:
        from services.shopify import fulfillment as shopify_fulfil
        from sync_tracking_to_shopify import _decide

        state = shopify_fulfil.get_order_state(reference)
        action, reason, target = _decide(state, tnt)
        carrier = _carrier_for_row(portal_row, state, shop_row)
        url = shopify_fulfil.normalize_tracking_url(portal_row.get('tracking_url'))

        return {
            'can_push': action in ('create', 'update'),
            'action': action,
            'reason': reason,
            'tracking': tnt,
            'carrier': carrier,
            'tracking_url': url,
            'target': target,
            # What the write would actually do, in plain words.
            'effect': ('Create a fulfilment in Shopify carrying this tracking number'
                       if action == 'create' else
                       'Add this tracking number to the existing Shopify fulfilment'
                       if action == 'update' else reason),
        }
    except Exception as exc:
        traceback.print_exc()
        return {'can_push': False, 'reason': f'Could not evaluate: {exc}'}


@app.route('/api/orders/push-tracking', methods=['POST'])
def api_orders_push_tracking():
    """Write one order's my-fulfilment.com tracking number into Shopify.

    This is the only endpoint in the tool that writes to Shopify. Everything is
    re-derived server-side from both systems — the browser only supplies the
    order reference — so a stale or tampered page can't cause a wrong write.
    """
    body = request.get_json(silent=True) or {}
    ref = str(body.get('reference') or '').strip()
    if not ref:
        return jsonify({'success': False, 'error': 'No order reference given.'}), 400
    ref = ref if ref.startswith('#') else f'#{ref}'
    # Notifying the customer is the POINT of adding tracking — a tracking number
    # nobody is told about is worthless. Default ON; only an explicit false
    # from the caller turns it off.
    notify = True if 'notify_customer' not in body else bool(body['notify_customer'])

    try:
        import order_lookup as ol
        from services.fulfilment import client as myf
        from services.shopify import audit, fulfillment as shopify_fulfil

        audit.set_origin('ui-single')
        shop_row = ol.shopify_order(ref)
        if shop_row is None:
            return jsonify({'success': False,
                            'error': f'{ref} not found in Shopify.'}), 404

        with myf.MyFulfilmentClient() as c:
            c.login()
            portal_row = ol.portal_order(c, ref)
        if portal_row is None:
            return jsonify({'success': False,
                            'error': f'{ref} not found in my-fulfilment.com.'}), 404

        plan = _tracking_sync_plan(ref, shop_row, portal_row)
        if not plan or not plan.get('can_push'):
            return jsonify({'success': False,
                            'error': (plan or {}).get('reason', 'Nothing to push.')}), 409

        if plan['action'] == 'create':
            result = shopify_fulfil.create_fulfillment(
                plan['target'], plan['tracking'],
                tracking_company=plan['carrier'], tracking_url=plan.get('tracking_url'),
                notify_customer=notify)
        else:
            result = shopify_fulfil.update_tracking(
                plan['target'], plan['tracking'],
                tracking_company=plan['carrier'], tracking_url=plan.get('tracking_url'),
                notify_customer=notify)

        broadcast_log(
            f"\nPushed tracking {plan['tracking']} ({plan['carrier'] or 'no carrier'}) "
            f"to {ref} — {plan['action']}, notify_customer={notify}\n")

        return jsonify({
            'success': True,
            'message': f"{ref}: tracking {plan['tracking']} written to Shopify "
                       f"({result.get('name', '')}) — "
                       + ("customer emailed" if notify else "NO email sent"),
            'notified': notify,
            'fulfillment': result,
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(exc)}), 500


def _orders_summary(rows: list) -> Dict[str, Any]:
    tracked = sum(1 for r in rows if r.get('has_tracking') == 'yes')
    fulfilled = sum(1 for r in rows if r.get('fulfillment') == 'FULFILLED')
    gaps = sum(1 for r in rows
               if r.get('portal_tracking') and r.get('has_tracking') == 'no')
    return {'total': len(rows), 'fulfilled': fulfilled,
            'tracked': tracked, 'gaps': gaps}


@app.route('/api/orders/export', methods=['POST'])
def api_orders_export():
    """Export the rows already on screen — as a CSV download or to Sheets."""
    body = request.get_json(silent=True) or {}
    rows = body.get('rows') or []
    if not rows:
        return jsonify({'success': False, 'error': 'Nothing to export.'}), 400

    target = body.get('target', 'csv')
    try:
        if target == 'sheet':
            from services.sheets import order_export
            tab = body.get('tab') or 'orders'
            url = order_export.export_orders(rows, tab)
            return jsonify({'success': True, 'url': url,
                            'message': f'Wrote {len(rows)} rows to tab "{tab}"'})

        # CSV: stream it back as a download.
        import csv as _csv
        import io as _io
        from flask import Response

        # Rows arrive as JSON, which loses dict ordering — so drive the column
        # order from the export schema and append anything unexpected at the end.
        from services.sheets.order_export import COLUMNS as SCHEMA
        present = {k for r in rows for k in r}
        columns = [c for c in SCHEMA if c in present]
        columns += [k for k in sorted(present) if k not in SCHEMA]
        buf = _io.StringIO()
        writer = _csv.DictWriter(buf, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

        filename = body.get('filename') or 'orders.csv'
        return Response(
            '﻿' + buf.getvalue(),   # BOM so Excel opens UTF-8 correctly
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    except Exception as exc:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.route('/api/workflows', methods=['GET'])
def list_workflows():
    """List available workflows with their last-run outcome."""
    current = None
    if state.current_task and state.current_task.startswith("workflow:"):
        current = state.current_task.split(":", 1)[1]
    return jsonify({
        'workflows': [
            {
                'key': key,
                'name': wf['name'],
                'description': wf['description'],
                'source': wf['source'],
                'systems': wf.get('systems', [wf['source']]),
                'output': wf['output'],
                'ai_supervised': wf.get('ai_supervised', False),
                'last_run': _workflow_last_run(wf),
            }
            for key, wf in WORKFLOWS.items()
        ],
        'running': state.running,
        'current': current,
    })


@app.route('/api/workflows/run', methods=['POST'])
def run_workflow():
    """Start a workflow."""
    if state.running:
        return jsonify({'success': False, 'error': 'A task is already running'}), 409

    data = request.json or {}
    key = data.get('workflow', '')
    if key not in WORKFLOWS:
        return jsonify({'success': False, 'error': f'Unknown workflow: {key}'}), 400
    headless = bool(data.get('headless', True))

    start_workflow(key, headless)
    mode = "headless" if headless else "visible browser"
    return jsonify({'success': True,
                    'message': f'Started {WORKFLOWS[key]["name"]} ({mode})'})


@app.route('/api/workflows/run-ai', methods=['POST'])
def run_workflow_ai():
    """Start a workflow through the AI supervisor (self-heal + Excel + Slack)."""
    if state.running:
        return jsonify({'success': False, 'error': 'A task is already running'}), 409

    data = request.json or {}
    key = data.get('workflow', '')
    if key not in WORKFLOWS:
        return jsonify({'success': False, 'error': f'Unknown workflow: {key}'}), 400

    start_supervised_workflow(key)
    return jsonify({'success': True,
                    'message': f'Started AI-supervised {WORKFLOWS[key]["name"]} '
                               f'- full report will be posted to Slack'})


@app.route('/api/run-schedule-now', methods=['POST'])
def run_schedule_now():
    """Manually trigger a schedule by ID (or legacy fallback)."""
    if state.running:
        return jsonify({'success': False, 'error': 'A task is already running'}), 409

    data = request.json or {}
    schedule_id = data.get('schedule_id')

    if schedule_id:
        schedule = state.scheduler_store.get(int(schedule_id))
    else:
        # Legacy fallback: run first schedule
        schedules = state.scheduler_store.list_schedules()
        schedule = schedules[0] if schedules else None

    if not schedule:
        return jsonify({'success': False, 'error': 'Schedule not found'}), 400

    on_schedule_due(schedule)
    return jsonify({'success': True, 'message': f'Schedule "{schedule.get("name", "")}" triggered'})


@app.route('/api/stop', methods=['POST'])
def stop_task():
    """Stop the current running task."""
    proc = state.current_process
    pid = state.current_pid
    task = state.current_task
    date_str = state.current_date_str

    if not proc or proc.poll() is not None:
        # Process already dead, just reset state
        if state.running:
            state.running = False
            state.current_process = None
            state.current_pid = None
            state.current_task = None
            broadcast_status("Stopped", False)
            return jsonify({'success': True, 'message': 'State reset'})
        return jsonify({'success': False, 'error': 'No task is running'}), 400

    state.stop_requested = True
    broadcast_status("Stopping run...", True)
    broadcast_log("\nStop requested - terminating process...\n")

    # Force kill immediately and reset state
    def force_kill_and_reset():
        import time

        # First try terminate
        try:
            proc.terminate()
        except Exception:
            pass

        time.sleep(1)

        # Then force kill
        try:
            proc.kill()
        except Exception:
            pass

        # Kill by PID
        kill_process_tree(pid)

        # Kill any chrome processes
        kill_profile_processes()

        time.sleep(0.5)

        # Force reset state
        broadcast_log("\nProcess stopped.\n")

        task_name = {
            "all": "All reports",
            "daily": "Daily Report",
            "order": "Order Type Report",
            "addtracker": "Daily Add Tracker",
        }.get(task or "", "Automation")

        broadcast_status(f"{task_name} stopped by user", False)

        # Reset all state
        state.current_process = None
        state.current_pid = None
        state.current_task = None
        state.running = False
        state._completion_in_progress = False

    threading.Thread(target=force_kill_and_reset, daemon=True).start()

    return jsonify({'success': True, 'message': 'Stop requested'})


@app.route('/api/previous-day')
def get_previous_day():
    """Get previous day formatted string."""
    date_obj = datetime.now() - timedelta(days=1)
    return jsonify({'date': date_obj.strftime("%d-%b-%Y")})


@app.route('/api/logs')
def list_logs():
    """List log files with parsed metadata, pagination, and server-side filtering.

    Query params:
        page (int)   - 1-based page number (default 1)
        per_page (int) - items per page (default 20, max 100)
        status (str) - filter by status (success/failed/warning/stopped)
        origin (str) - filter by origin (manual/scheduled)
        task (str)   - filter by display task name
    """
    import re as _re

    page = max(1, request.args.get("page", 1, type=int))
    per_page = min(100, max(1, request.args.get("per_page", 20, type=int)))
    filter_status = request.args.get("status", "").strip()
    filter_origin = request.args.get("origin", "").strip()
    filter_task = request.args.get("task", "").strip()

    task_labels = {
        "all_reports": "All Reports",
        "daily_report": "Daily Report",
        "order_type_report": "Order Type",
        "daily_add_tracker": "Add Tracker",
        "datads_daily": "DataAds Daily",
        "datads_weekly": "DataAds Weekly",
    }

    # Build full list (metadata only - we read just enough of each file)
    all_logs = []
    for f in sorted(state.log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True):
        stat = f.stat()
        size_kb = round(stat.st_size / 1024, 1)

        name = f.stem
        origin = "unknown"
        task = name
        timestamp_str = ""

        ts_match = _re.search(r'_(\d{8}_\d{6})$', name)
        if ts_match:
            timestamp_str = ts_match.group(1)
            prefix = name[:ts_match.start()]
            if prefix.startswith("manual_"):
                origin = "manual"
                task = prefix[7:]
            elif prefix.startswith("scheduled_"):
                origin = "scheduled"
                task = prefix[10:]
            else:
                task = prefix

        task_display = task_labels.get(task, task.replace("_", " ").title())

        # Quick filter on origin and task before reading file content
        if filter_origin and origin != filter_origin:
            continue
        if filter_task and task_display != filter_task:
            continue

        # Read file to detect status and errors
        status = "unknown"
        header_lines = []
        error_count = 0
        try:
            with f.open("r", encoding="utf-8", errors="replace") as fh:
                content = fh.read()
                header_lines = content.split("\n")[:3]
                for line in content.split("\n"):
                    ll = line.lower()
                    if any(kw in ll for kw in ["error", "failed", "exception", "traceback", "\u2717"]):
                        error_count += 1
                content_lower = content.lower()
                if "completed successfully" in content_lower or "report completed successfully" in content_lower.replace("\n", " "):
                    status = "success" if error_count == 0 else "warning"
                elif "stopped by user" in content_lower:
                    status = "stopped"
                elif "finished with issues" in content_lower or error_count > 0:
                    status = "failed"
                elif stat.st_size < 500:
                    status = "incomplete"
                else:
                    status = "success"
        except Exception:
            pass

        if filter_status and status != filter_status:
            continue

        started = ""
        if timestamp_str:
            try:
                dt = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                started = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        report_date = ""
        if header_lines:
            date_match = _re.search(r'for (\d{1,2}-\w{3}-\d{4})', header_lines[0])
            if date_match:
                report_date = date_match.group(1)

        all_logs.append({
            "filename": f.name,
            "origin": origin,
            "task": task_display,
            "started": started,
            "report_date": report_date,
            "size_kb": size_kb,
            "status": status,
            "error_count": error_count,
        })

    # Stats (computed from the filtered list)
    total = len(all_logs)
    stats = {
        "total": total,
        "success": sum(1 for l in all_logs if l["status"] == "success"),
        "failed": sum(1 for l in all_logs if l["status"] == "failed"),
        "warning": sum(1 for l in all_logs if l["status"] == "warning"),
    }

    # Paginate
    total_pages = max(1, -(-total // per_page))  # ceil division
    start = (page - 1) * per_page
    page_logs = all_logs[start : start + per_page]

    return jsonify({
        "logs": page_logs,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "stats": stats,
    })


@app.route('/api/logs/<filename>')
def get_log_content(filename: str):
    """Read a specific log file's content."""
    import re as _re
    # Sanitize filename to prevent path traversal
    safe_name = Path(filename).name
    if safe_name != filename or ".." in filename:
        return jsonify({"success": False, "error": "Invalid filename"}), 400

    log_path = state.log_dir / safe_name
    if not log_path.exists():
        return jsonify({"success": False, "error": "Log file not found"}), 404

    try:
        content = log_path.read_text(encoding="utf-8", errors="replace")

        # Find error lines with line numbers
        errors = []
        for i, line in enumerate(content.split("\n"), 1):
            ll = line.lower()
            if any(kw in ll for kw in ["error", "failed", "exception", "traceback", "✗"]):
                errors.append({"line": i, "text": line.strip()})

        return jsonify({
            "success": True,
            "content": content,
            "errors": errors,
            "size_kb": round(log_path.stat().st_size / 1024, 1),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/logs/<filename>', methods=['DELETE'])
def delete_log(filename: str):
    """Delete a specific log file."""
    safe_name = Path(filename).name
    if safe_name != filename or ".." in filename:
        return jsonify({"success": False, "error": "Invalid filename"}), 400

    log_path = state.log_dir / safe_name
    if not log_path.exists():
        return jsonify({"success": False, "error": "Log file not found"}), 404

    try:
        log_path.unlink()
        return jsonify({"success": True, "message": "Log deleted"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/log-settings', methods=['GET'])
def get_log_settings():
    """Get log retention settings."""
    max_logs = get_setting("MAX_LOG_FILES") or str(DEFAULT_MAX_LOG_FILES)
    log_count = len(list(state.log_dir.glob("*.log")))
    return jsonify({"MAX_LOG_FILES": max_logs, "log_count": log_count})


@app.route('/api/log-settings', methods=['POST'])
def save_log_settings():
    """Save log retention settings and run cleanup."""
    data = request.json
    try:
        max_logs = int(data.get("MAX_LOG_FILES", DEFAULT_MAX_LOG_FILES))
        if max_logs < 10:
            return jsonify({"success": False, "error": "Minimum is 10 logs"}), 400
        set_setting("MAX_LOG_FILES", str(max_logs))
        cleanup_old_logs()
        log_count = len(list(state.log_dir.glob("*.log")))
        return jsonify({"success": True, "message": "Log settings saved", "log_count": log_count})
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Invalid number"}), 400


@app.route('/api/setup-browser', methods=['POST'])
def setup_browser():
    """Open a visible Chrome browser with the same profile for manual login setup."""
    from browser_manager import BrowserManager
    import time as _time

    data = request.json or {}
    urls = data.get('urls', [
        'https://lookerstudio.google.com',
        'https://app.runconverge.com',
        'https://app.atriaanalytics.com',
    ])

    def _open_browser():
        try:
            socketio.emit('task_output', {'data': 'Opening visible browser for login setup...\n'})
            manager = BrowserManager(use_existing_chrome=False)
            driver = manager.start_browser(headless=False)

            # Navigate to the first URL so user can start logging in
            if urls:
                driver.get(urls[0])
                socketio.emit('task_output', {
                    'data': f'Browser opened. Navigate to these sites and login:\n'
                })
                for u in urls:
                    socketio.emit('task_output', {'data': f'  - {u}\n'})
                socketio.emit('task_output', {
                    'data': '\nSession cookies will be saved automatically.\n'
                           'Close the browser when done.\n'
                })

            # Wait until browser is closed by user
            try:
                while True:
                    _ = driver.title
                    _time.sleep(2)
            except Exception:
                pass  # Browser was closed

            socketio.emit('task_output', {'data': 'Setup browser closed. Sessions saved.\n'})
        except Exception as e:
            socketio.emit('task_output', {'data': f'Error opening browser: {e}\n'})

    threading.Thread(target=_open_browser, daemon=True).start()
    return jsonify({'success': True, 'message': 'Opening setup browser...'})


# ============================================================================
# WebSocket Events
# ============================================================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    emit('status_update', {
        'status': 'Connected to server',
        'running': state.running
    })


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("Smyle Automation Suite - Web Application")
    print("=" * 60)
    print("\nStarting server...")

    # Initialize scheduler
    init_scheduler()

    print("\nServer running at: http://localhost:5002")
    print("Press Ctrl+C to stop\n")

    socketio.run(app, host='0.0.0.0', port=5002, debug=False, allow_unsafe_werkzeug=True)
