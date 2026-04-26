"""
Smyle Automation Suite - Web Application
Flask-based web interface replacing the Tkinter desktop GUI.

Run with: python web_app.py
Then open: http://localhost:5000
"""

from __future__ import annotations

import os
import sys
import queue
import threading
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

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
]

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

    # Start the scheduled job
    days_ago = max(0, int(schedule.get("run_for_days_ago") or 1))
    target_date = datetime.combine(
        (datetime.now() - timedelta(days=days_ago)).date(), datetime.min.time()
    )
    date_str = target_date.strftime("%d-%b-%Y")
    schedule_id = schedule.get("id")

    if schedule_id:
        state.scheduler_store.mark_running(schedule_id, "Triggered automatically")

    state.current_schedule_id = schedule_id
    state.current_run_origin = "scheduled"

    # Check if datads_weekly or weekly is in the task list - calculate weekly date range
    task = schedule.get("task", "all")
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
    labels = {
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
    edit_id = data.get('edit_id')  # If editing an existing schedule

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

    print("\nServer running at: http://localhost:5000")
    print("Press Ctrl+C to stop\n")

    socketio.run(app, host='0.0.0.0', port=5002, debug=False, allow_unsafe_werkzeug=True)
