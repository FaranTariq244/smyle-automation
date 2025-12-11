"""
Smyle Automation Suite - GUI launcher for existing console scripts.

This interface keeps the underlying business logic unchanged while providing
quick buttons to run:
  - Daily Report extraction
  - Order Type Report extraction
  - Both reports back-to-back

Designed to be extendable so future automations can be added alongside these.
"""

from __future__ import annotations

import os
import queue
import sys
import threading
import tkinter as tk
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk

# Ensure imports work even if the script is launched from another directory
PROJECT_ROOT = Path(__file__).resolve().parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import existing business logic
from run_all_reports import run_daily_report, run_order_type_report  # noqa: E402
from config_store import get_settings, set_settings  # noqa: E402


class OutputRedirector:
    """Redirect stdout/stderr into a queue while still mirroring to console."""

    def __init__(self, message_queue: queue.Queue, original_stream):
        self.message_queue = message_queue
        self.original_stream = original_stream

    def write(self, message: str):
        if message:
            self.message_queue.put(message)
        if self.original_stream:
            self.original_stream.write(message)
            self.original_stream.flush()

    def flush(self):
        if self.original_stream:
            self.original_stream.flush()


class AutomationApp:
    """Tkinter application that wraps the existing automation scripts."""

    BG = "#0b1221"
    CARD_BG = "#111827"
    ACCENT = "#22d3ee"
    ACCENT_DARK = "#0ea5e9"
    TEXT_PRIMARY = "#e5e7eb"
    TEXT_SECONDARY = "#94a3b8"

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Smyle Automation Suite")
        self.root.configure(bg=self.BG)
        self.root.minsize(960, 680)

        self.date_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Idle: waiting to run a task")
        self.settings_keys = [
            "SPREAD_SHEET_NAME",
            "WORK_SHEET_NAME",
            "ORDER_TYPE_SHEET_URL",
        ]
        self.settings_vars = {key: tk.StringVar() for key in self.settings_keys}

        self.message_queue: queue.Queue[str] = queue.Queue()
        self.running = False
        self.run_buttons: list[ttk.Button] = []
        self.stop_button: ttk.Button | None = None
        self.current_process: subprocess.Popen | None = None
        self.current_pid: int | None = None
        self.stop_requested = False
        self.current_task: str | None = None
        self.current_date_str: str = ""
        self._completion_in_progress = False
        self.pages: dict[str, ttk.Frame] = {}

        self._create_styles()
        self._build_layout()
        self._load_settings_into_vars()

        # Poll log messages regularly
        self.root.after(120, self._drain_queue)

    def _create_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Main.TFrame", background=self.BG)
        style.configure(
            "Card.TFrame",
            background=self.CARD_BG,
            relief="flat",
            borderwidth=0,
            padding=18,
        )
        style.configure(
            "Header.TLabel",
            background=self.BG,
            foreground=self.TEXT_PRIMARY,
            font=("Segoe UI Semibold", 20),
        )
        style.configure(
            "SubHeader.TLabel",
            background=self.BG,
            foreground=self.TEXT_SECONDARY,
            font=("Segoe UI", 10),
        )
        style.configure(
            "CardTitle.TLabel",
            background=self.CARD_BG,
            foreground=self.TEXT_PRIMARY,
            font=("Segoe UI Semibold", 12),
        )
        style.configure(
            "CardText.TLabel",
            background=self.CARD_BG,
            foreground=self.TEXT_SECONDARY,
            font=("Segoe UI", 10),
        )
        style.configure(
            "Status.TLabel",
            background=self.CARD_BG,
            foreground=self.TEXT_PRIMARY,
            font=("Consolas", 10),
        )
        style.configure(
            "Date.TEntry",
            fieldbackground="#0d1428",
            foreground=self.TEXT_PRIMARY,
            insertcolor=self.ACCENT,
            padding=10,
            relief="flat",
            borderwidth=0,
        )
        style.configure(
            "Accent.TButton",
            background=self.ACCENT,
            foreground="#0b1221",
            padding=(14, 10),
            font=("Segoe UI Semibold", 11),
        )
        style.map(
            "Accent.TButton",
            background=[
                ("active", self.ACCENT_DARK),
                ("disabled", "#1f2937"),
            ],
            foreground=[("disabled", "#6b7280")],
        )
        style.configure(
            "Ghost.TButton",
            background=self.CARD_BG,
            foreground=self.TEXT_PRIMARY,
            padding=(12, 9),
            font=("Segoe UI", 10),
            borderwidth=1,
            relief="solid",
        )
        style.map(
            "Ghost.TButton",
            background=[
                ("active", "#0f172a"),
                ("disabled", "#1f2937"),
            ],
            foreground=[("disabled", "#6b7280")],
        )
        style.configure(
            "Badge.TLabel",
            background=self.ACCENT,
            foreground="#0b1221",
            font=("Segoe UI Semibold", 9),
            padding=(8, 2),
        )
        style.configure(
            "Danger.TButton",
            background="#f43f5e",
            foreground="#0b1221",
            padding=(14, 10),
            font=("Segoe UI Semibold", 11),
        )
        style.map(
            "Danger.TButton",
            background=[
                ("active", "#e11d48"),
                ("disabled", "#1f2937"),
            ],
            foreground=[("disabled", "#6b7280")],
        )

    def _build_layout(self):
        self.main_container = ttk.Frame(self.root, style="Main.TFrame", padding=20)
        self.main_container.pack(fill=tk.BOTH, expand=True)

        self.pages["home"] = self._build_home_page(self.main_container)
        self.pages["runner"] = self._build_runner_page(self.main_container)

        self._show_page("home")

    def _build_home_page(self, parent: ttk.Frame) -> ttk.Frame:
        frame = ttk.Frame(parent, style="Main.TFrame")

        header = ttk.Frame(frame, style="Main.TFrame")
        header.pack(fill=tk.X, pady=(0, 14))
        ttk.Label(header, text="Smyle Automation Suite", style="Header.TLabel").pack(
            anchor=tk.W
        )
        ttk.Label(
            header,
            text="Pick a tool to launch its dedicated UI. More automations coming soon.",
            style="SubHeader.TLabel",
        ).pack(anchor=tk.W, pady=(4, 0))

        cards = ttk.Frame(frame, style="Main.TFrame")
        cards.pack(fill=tk.BOTH, expand=True)
        cards.columnconfigure(0, weight=1)
        cards.columnconfigure(1, weight=1)

        tools = [
            {
                "key": "runner",
                "title": "Marketing Reports",
                "desc": "Daily + Order Type automations for Looker Studio and Converge.",
                "badge": "In use",
                "active": True,
            },
            {
                "key": "future1",
                "title": "Data Quality Checks",
                "desc": "Reserved slot for upcoming QA automation.",
                "badge": "Coming soon",
                "active": False,
            },
            {
                "key": "future2",
                "title": "Backup & Sync",
                "desc": "Reserved slot for syncing artifacts to storage.",
                "badge": "Coming soon",
                "active": False,
            },
        ]

        for idx, tool in enumerate(tools):
            card = ttk.Frame(cards, style="Card.TFrame")
            row, col = divmod(idx, 2)
            card.grid(row=row, column=col, padx=8, pady=8, sticky="nsew")
            cards.rowconfigure(row, weight=1)

            badge_text = tool["badge"]
            ttk.Label(card, text=badge_text, style="Badge.TLabel").pack(anchor=tk.W)
            ttk.Label(card, text=tool["title"], style="CardTitle.TLabel").pack(
                anchor=tk.W, pady=(6, 2)
            )
            ttk.Label(card, text=tool["desc"], style="CardText.TLabel").pack(
                anchor=tk.W, pady=(0, 10)
            )

            if tool["active"]:
                ttk.Button(
                    card,
                    text="Open",
                    style="Accent.TButton",
                    command=lambda: self._show_page("runner"),
                ).pack(anchor=tk.W, pady=(4, 0))
            else:
                btn = ttk.Button(
                    card,
                    text="Reserved",
                    style="Ghost.TButton",
                    state="disabled",
                )
                btn.pack(anchor=tk.W, pady=(4, 0))

        return frame

    def _build_runner_page(self, parent: ttk.Frame) -> ttk.Frame:
        container = ttk.Frame(parent, style="Main.TFrame")

        # Scrollable wrapper so lower settings remain reachable
        canvas = tk.Canvas(container, bg=self.BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas, style="Main.TFrame")
        scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(
                scrollregion=canvas.bbox("all"),
            ),
        )
        window = canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        # Enable scrolling via mouse wheel when focused
        scroll_frame.bind("<Enter>", lambda _: canvas.bind_all("<MouseWheel>", _on_mousewheel))
        scroll_frame.bind("<Leave>", lambda _: canvas.unbind_all("<MouseWheel>"))

        # Header with back navigation
        header_bar = ttk.Frame(scroll_frame, style="Main.TFrame")
        header_bar.pack(fill=tk.X, pady=(0, 10))

        back_btn = ttk.Button(
            header_bar,
            text="◀ Back to tools",
            style="Ghost.TButton",
            command=lambda: self._show_page("home"),
        )
        back_btn.pack(side=tk.LEFT, padx=(0, 12))

        header_text = ttk.Frame(header_bar, style="Main.TFrame")
        header_text.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(
            header_text, text="Marketing Reports", style="Header.TLabel"
        ).pack(anchor=tk.W)
        ttk.Label(
            header_text,
            text="Daily + Order Type automations, same business logic, new UI.",
            style="SubHeader.TLabel",
        ).pack(anchor=tk.W, pady=(4, 0))

        # Controls card
        controls = ttk.Frame(scroll_frame, style="Card.TFrame")
        controls.pack(fill=tk.X, pady=(8, 12))

        ttk.Label(controls, text="Run Reports", style="CardTitle.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            controls,
            text="Pick a date (blank = previous day) then choose which automation to launch.",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(2, 10))

        ttk.Label(controls, text="Extraction date", style="CardText.TLabel").grid(
            row=2, column=0, sticky="w"
        )
        date_entry = ttk.Entry(
            controls,
            width=22,
            textvariable=self.date_var,
            style="Date.TEntry",
        )
        date_entry.grid(row=3, column=0, padx=(0, 12), sticky="we")
        date_entry.insert(0, "")

        ttk.Button(
            controls,
            text="Use previous day",
            style="Ghost.TButton",
            command=self._fill_previous_day,
        ).grid(row=3, column=1, sticky="w")

        button_frame = ttk.Frame(controls, style="Card.TFrame")
        button_frame.grid(row=4, column=0, columnspan=4, pady=(12, 0), sticky="we")

        run_all_btn = ttk.Button(
            button_frame,
            text="Run All Reports",
            style="Accent.TButton",
            command=lambda: self._start_task("all"),
        )
        run_daily_btn = ttk.Button(
            button_frame,
            text="Daily Report only",
            style="Ghost.TButton",
            command=lambda: self._start_task("daily"),
        )
        run_order_btn = ttk.Button(
            button_frame,
            text="Order Type only",
            style="Ghost.TButton",
            command=lambda: self._start_task("order"),
        )

        run_all_btn.grid(row=0, column=0, padx=(0, 10), pady=2, sticky="we")
        run_daily_btn.grid(row=0, column=1, padx=(0, 10), pady=2, sticky="we")
        run_order_btn.grid(row=0, column=2, padx=(0, 10), pady=2, sticky="we")

        self.stop_button = ttk.Button(
            button_frame,
            text="Stop run",
            style="Danger.TButton",
            command=self._stop_task,
            state="disabled",
        )
        self.stop_button.grid(row=0, column=3, padx=(10, 0), pady=2, sticky="we")

        self.run_buttons = [run_all_btn, run_daily_btn, run_order_btn]

        # Status + progress
        status_frame = ttk.Frame(scroll_frame, style="Card.TFrame")
        status_frame.pack(fill=tk.X, pady=(0, 12))
        ttk.Label(status_frame, text="Live status", style="CardTitle.TLabel").pack(
            anchor=tk.W
        )
        ttk.Label(
            status_frame, textvariable=self.status_var, style="Status.TLabel"
        ).pack(anchor=tk.W, pady=(6, 8))
        self.progress = ttk.Progressbar(
            status_frame, mode="indeterminate", length=240, maximum=120
        )
        self.progress.pack(fill=tk.X)

        # Log output area
        log_card = ttk.Frame(scroll_frame, style="Card.TFrame")
        log_card.pack(fill=tk.BOTH, expand=True)
        ttk.Label(log_card, text="Run log", style="CardTitle.TLabel").pack(anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(
            log_card,
            wrap=tk.WORD,
            height=20,
            bg="#0d1428",
            fg=self.TEXT_PRIMARY,
            insertbackground=self.ACCENT,
            font=("Consolas", 10),
            relief="flat",
            borderwidth=0,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        self.log_text.insert(
            tk.END,
            "Outputs from the automation scripts will stream here. "
            "You can still watch the console if you prefer.\n\n",
        )
        self.log_text.configure(state="disabled")

        # Config card within marketing reports page
        config_card = ttk.Frame(scroll_frame, style="Card.TFrame")
        config_card.pack(fill=tk.X, pady=(10, 0))
        ttk.Label(
            config_card, text="Marketing sheet settings", style="CardTitle.TLabel"
        ).grid(row=0, column=0, sticky="w", pady=(0, 6))
        ttk.Label(
            config_card,
            text="Stored locally in config.db (seeded from .env on first load).",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 10))

        labels = {
            "SPREAD_SHEET_NAME": "SPREAD_SHEET_NAME",
            "WORK_SHEET_NAME": "WORK_SHEET_NAME",
            "ORDER_TYPE_SHEET_URL": "ORDER_TYPE_SHEET_URL",
        }
        for idx, (key, label_text) in enumerate(labels.items(), start=2):
            ttk.Label(config_card, text=label_text, style="CardText.TLabel").grid(
                row=idx, column=0, sticky="w", pady=4
            )
            entry = ttk.Entry(
                config_card,
                width=60,
                textvariable=self.settings_vars[key],
                style="Date.TEntry",
            )
            entry.grid(row=idx, column=1, sticky="we", padx=(8, 0), pady=4)

        button_bar = ttk.Frame(config_card, style="Card.TFrame")
        button_bar.grid(row=idx + 1, column=0, columnspan=2, pady=(12, 0), sticky="w")
        save_btn = ttk.Button(
            button_bar,
            text="Save settings",
            style="Accent.TButton",
            command=self._save_settings,
        )
        reload_btn = ttk.Button(
            button_bar,
            text="Reload",
            style="Ghost.TButton",
            command=self._load_settings_into_vars,
        )
        save_btn.grid(row=0, column=0, padx=(0, 8))
        reload_btn.grid(row=0, column=1)

        # Keep canvas width aligned to container resize
        def _on_frame_configure(event):
            canvas.itemconfig(window, width=event.width)

        container.bind("<Configure>", _on_frame_configure)

        return container

    def _show_page(self, name: str):
        for frame in self.pages.values():
            frame.pack_forget()
        target = self.pages.get(name)
        if target:
            target.pack(fill=tk.BOTH, expand=True)
        if name == "home":
            self._set_running(False, "Idle: waiting to run a task")
        if name == "runner":
            self._load_settings_into_vars()

    def _fill_previous_day(self):
        date_obj = datetime.now() - timedelta(days=1)
        self.date_var.set(date_obj.strftime("%d-%b-%Y"))

    def _set_running(self, is_running: bool, status_message: str | None = None):
        self.running = is_running
        if status_message:
            self.status_var.set(status_message)
        for button in self.run_buttons:
            if is_running:
                button.state(["disabled"])
            else:
                button.state(["!disabled"])
        if self.stop_button:
            if is_running:
                self.stop_button.state(["!disabled"])
            else:
                self.stop_button.state(["disabled"])
        if is_running:
            self.progress.start(8)
        else:
            self.progress.stop()

    def _start_task(self, task: str):
        if self.running:
            messagebox.showinfo(
                "Automation already running",
                "Please wait for the current run to finish.",
            )
            return

        parsed = self._parse_date()
        if not parsed:
            return
        date_obj, date_str = parsed

        task_name = {
            "all": "All reports",
            "daily": "Daily Report",
            "order": "Order Type Report",
        }.get(task, "Automation")

        self._set_running(True, f"Running {task_name} for {date_str}...")
        self._append_log(
            f"\n{'=' * 80}\nStarting {task_name} for {date_str}\n{'=' * 80}\n"
        )
        self.stop_requested = False
        self.current_task = task
        self.current_date_str = date_str
        self._completion_in_progress = False
        self._launch_subprocess_task(task, date_str)

    def _on_task_complete(self, task: str, date_str: str, success: bool):
        if self._completion_in_progress:
            return
        self._completion_in_progress = True
        task_name = {
            "all": "All reports",
            "daily": "Daily Report",
            "order": "Order Type Report",
        }.get(task, "Automation")
        status = "completed successfully"
        if self.stop_requested:
            status = "stopped by user"
        elif not success:
            status = "finished with issues"
        self._append_log(f"\n{task_name} {status} for {date_str}\n")
        self._set_running(False, f"{task_name} {status} for {date_str}")
        self.current_process = None
        self.current_pid = None
        self.current_task = None
        if not success and not self.stop_requested:
            messagebox.showwarning(
                "Check the run log",
                f"{task_name} finished with issues. Review the log for details.",
            )

    def _parse_date(self) -> tuple[datetime, str] | None:
        raw_value = self.date_var.get().strip()
        if not raw_value:
            date_obj = datetime.now() - timedelta(days=1)
            return date_obj, date_obj.strftime("%d-%b-%Y")

        for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
            try:
                date_obj = datetime.strptime(raw_value, fmt)
                return date_obj, date_obj.strftime("%d-%b-%Y")
            except ValueError:
                continue

        messagebox.showerror(
            "Invalid date",
            "Please use DD-MMM-YYYY (e.g., 09-Oct-2025). "
            "Leave the field blank to use the previous day.",
        )
        return None

    def _append_log(self, message: str):
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, message)
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _build_subprocess_code(self, task: str, date_str: str) -> str:
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

    def _launch_subprocess_task(self, task: str, date_str: str):
        """Run the selected task in a separate process so it can be stopped."""
        code = self._build_subprocess_code(task, date_str)
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
                env=self._build_subprocess_env(),
            )
            self.current_process = proc
            self.current_pid = proc.pid
        except Exception as exc:  # pragma: no cover - defensive
            self._append_log(f"\nFailed to start process: {exc}\n")
            self._set_running(False, "Failed to start process")
            return

        thread = threading.Thread(
            target=self._watch_process_output, args=(proc, task, date_str), daemon=True
        )
        thread.start()

    def _watch_process_output(
        self, proc: subprocess.Popen, task: str, date_str: str
    ):
        try:
            if proc.stdout:
                for line in proc.stdout:
                    self.message_queue.put(line)
            proc.wait()
        finally:
            success = proc.returncode == 0 and not self.stop_requested
            self.root.after(
                0, lambda: self._on_task_complete(task, date_str, success)
            )

    def _stop_task(self):
        """Terminate the running subprocess (best-effort)."""
        proc = self.current_process
        if proc and proc.poll() is None:
            self.stop_requested = True
            self.status_var.set("Stopping run...")
            try:
                proc.terminate()
            except Exception:
                pass
            # If still alive after a short delay, force kill
            self.root.after(1500, self._force_kill_if_alive)
            # Also poll until it's gone, then finalize completion
            self.root.after(200, self._check_stop_completion)
            # Try to close Chrome/Chromedriver tree on Windows to avoid orphaned browsers
            self._kill_process_tree(proc.pid)
            self._kill_profile_processes()
            self._kill_profile_processes_wmic()

    def _force_kill_if_alive(self):
        proc = self.current_process
        if proc and proc.poll() is None:
            try:
                proc.kill()
                self.message_queue.put("\nProcess force-killed.\n")
            except Exception:
                pass
            self._kill_process_tree(proc.pid)
            self._kill_profile_processes()
            self._kill_profile_processes_wmic()

    def _check_stop_completion(self):
        proc = self.current_process
        if proc and proc.poll() is None:
            self.root.after(200, self._check_stop_completion)
        else:
            # Ensure we surface completion even if the output thread is stuck
            task = self.current_task or "all"
            date_str = self.current_date_str or ""
            self._on_task_complete(task, date_str, success=False)

    def _build_subprocess_env(self) -> dict:
        """Ensure child process emits UTF-8 so special characters don't break logs."""
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        return env

    def _kill_process_tree(self, pid: int | None):
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

    def _kill_profile_processes(self):
        """Kill Chrome/chromedriver processes tied to the automation profile."""
        profile_dir = str(PROJECT_ROOT / "chrome_profile")
        # Escape backslashes for PowerShell like wildcard match
        pattern = profile_dir.replace("\\", "\\\\")
        ps_script = f"""
Get-CimInstance Win32_Process |
  Where-Object {{
    $_.CommandLine -like "*{pattern}*" -and (
      $_.Name -eq "chrome.exe" -or $_.Name -eq "chromedriver.exe" -or $_.Name -eq "msedge.exe"
    )
  }} |
  ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }}
"""
        try:
            subprocess.run(
                ["powershell", "-Command", ps_script],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def _kill_profile_processes_wmic(self):
        """WMIC-based fallback to terminate Chrome processes using our profile dir."""
        profile_dir = str(PROJECT_ROOT / "chrome_profile")
        if not profile_dir:
            return
        escaped = profile_dir.replace("\\", "\\\\")
        query = f'CommandLine like "%{escaped}%"'
        try:
            subprocess.run(
                ["wmic", "process", "where", query, "call", "terminate"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def _drain_queue(self):
        try:
            while True:
                message = self.message_queue.get_nowait()
                self._append_log(message)
        except queue.Empty:
            pass
        finally:
            self.root.after(120, self._drain_queue)

    def _load_settings_into_vars(self):
        """Load persisted settings into GUI fields."""
        stored = get_settings(self.settings_keys)
        for key, var in self.settings_vars.items():
            var.set(stored.get(key, "") or "")

    def _save_settings(self):
        """Persist GUI field values to the local config store."""
        payload = {key: var.get().strip() for key, var in self.settings_vars.items()}
        set_settings(payload)
        self.status_var.set("Saved settings to local config.db")
        messagebox.showinfo("Settings saved", "Values stored locally in config.db.")


def main():
    root = tk.Tk()
    app = AutomationApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
