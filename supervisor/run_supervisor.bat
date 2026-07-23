@echo off
REM ============================================================
REM  Smyle Automation Supervisor launcher (Task Scheduler + dashboard "Run with AI")
REM  Usage:  run_supervisor.bat [workflow_key] [run_log_path]
REM  Runs the deterministic supervisor (supervise.py): runs the workflow to
REM  completion, self-heals a crash via headless Claude, and always Slacks a report.
REM ============================================================
setlocal
set ROOT=C:\Faran\git\smyle-automation
set PY=%ROOT%\venv\Scripts\python.exe
set KEY=%~1
if "%KEY%"=="" set KEY=tiktok_toship
set RUNLOG=%~2

cd /d "%ROOT%"

"%PY%" "%ROOT%\supervisor\supervise.py" %KEY% "%RUNLOG%"
set RC=%ERRORLEVEL%

REM supervise.py always Slacks on its own. This only fires if Python itself
REM failed to start (e.g. import error) so a run can never be silent.
if %RC% GEQ 2 (
  "%PY%" "%ROOT%\supervisor\slack_notify.py" --channel "*%KEY%* automation :rotating_light: Supervisor could not start - supervise.py failed to launch (exit %RC%). Needs attention on the server."
)

exit /b %RC%
