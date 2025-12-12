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

from config_store import get_settings, set_settings
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
]


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


def build_subprocess_code(task: str, date_str: str) -> str:
    """Build inline Python code for the child process to execute."""
    return f"""
import sys
from datetime import datetime
from run_all_reports import run_daily_report, run_order_type_report
date_str = "{date_str}"
date_obj = datetime.strptime(date_str, "%d-%b-%Y")
try:
    if "{task}" == "daily":
        ok = run_daily_report(date_obj, date_str)
    elif "{task}" == "order":
        ok = run_order_type_report(date_obj, date_str)
    else:
        d_ok = run_daily_report(date_obj, date_str)
        o_ok = run_order_type_report(date_obj, date_str)
        ok = d_ok and o_ok
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

    task_name = {
        "all": "All reports",
        "daily": "Daily Report",
        "order": "Order Type Report",
    }.get(task, "Automation")

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

    # Start the task
    start_task_with_date(
        schedule.get("task", "all"),
        target_date,
        date_str,
        origin="scheduled"
    )
    return True


def start_task_with_date(task: str, date_obj: datetime, date_str: str, origin: str):
    """Start a task with a specific date."""
    task_name = {
        "all": "All reports",
        "daily": "Daily Report",
        "order": "Order Type Report",
    }.get(task, "Automation")

    origin_label = "Scheduled run" if origin == "scheduled" else "Manual run"
    state.running = True
    state.stop_requested = False
    state.current_task = task
    state.current_date_str = date_str
    state.current_run_origin = origin
    state._completion_in_progress = False
    state.last_log_path = None
    state.current_log_path = start_log_file(task_name, date_str, origin)

    broadcast_status(f"{origin_label}: Running {task_name} for {date_str}...", True)
    broadcast_log(f"\n{'=' * 80}\n{origin_label} - {task_name} for {date_str}\n{'=' * 80}\n")

    # Launch subprocess
    code = build_subprocess_code(task, date_str)
    cmd = [sys.executable, "-u", "-c", code]
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
            env=build_subprocess_env(),
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
    schedule = state.scheduler_store.get_by_key("marketing_reports")
    return jsonify({
        'running': state.running,
        'current_task': state.current_task,
        'current_date': state.current_date_str,
        'origin': state.current_run_origin,
        'last_log_path': str(state.last_log_path) if state.last_log_path else None,
        'schedule': schedule,
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


@app.route('/api/schedule', methods=['GET'])
def get_schedule():
    """Get scheduler configuration."""
    schedule = state.scheduler_store.get_by_key("marketing_reports")
    # Return empty defaults if no schedule exists
    if not schedule:
        schedule = {
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
    """Save scheduler configuration."""
    data = request.json

    time_val = data.get('time_of_day', '07:00').strip() or '07:00'
    start_val = data.get('start_date', '').strip() or datetime.now().strftime("%Y-%m-%d")
    recurrence = data.get('recurrence', 'daily')
    task = data.get('task', 'all')
    enabled = data.get('enabled', False)
    days_ago = max(0, int(data.get('run_for_days_ago', 1)))

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

    schedule = state.scheduler_store.upsert_schedule(
        key="marketing_reports",
        name="Marketing Reports",
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


@app.route('/api/run', methods=['POST'])
def run_task():
    """Start a report task."""
    if state.running:
        return jsonify({'success': False, 'error': 'A task is already running'}), 409

    data = request.json
    task = data.get('task', 'all')
    date_str = data.get('date', '')

    parsed = parse_date(date_str)
    if not parsed:
        return jsonify({'success': False, 'error': 'Invalid date format. Use DD-MMM-YYYY.'}), 400

    date_obj, formatted_date = parsed
    state.current_schedule_id = None
    state.current_run_origin = "manual"

    start_task_with_date(task, date_obj, formatted_date, origin="manual")

    return jsonify({'success': True, 'message': f'Started {task} for {formatted_date}'})


@app.route('/api/run-schedule-now', methods=['POST'])
def run_schedule_now():
    """Manually trigger the saved schedule."""
    if state.running:
        return jsonify({'success': False, 'error': 'A task is already running'}), 409

    schedule = state.scheduler_store.get_by_key("marketing_reports")
    if not schedule:
        return jsonify({'success': False, 'error': 'No schedule configured'}), 400

    on_schedule_due(schedule)
    return jsonify({'success': True, 'message': 'Schedule triggered'})


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

    socketio.run(app, host='0.0.0.0', port=5001, debug=False, allow_unsafe_werkzeug=True)
