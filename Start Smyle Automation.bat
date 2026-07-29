@echo off
title Smyle Automation Suite
cd /d "%~dp0"

echo ============================================================
echo  Smyle Automation Suite
echo  Starting web app... open http://localhost:5002
echo  Press Ctrl+C in this window to stop the server.
echo ============================================================
echo.

REM Open the app in the default browser after a short delay (background)
start "" /b cmd /c "timeout /t 3 /nobreak >nul & start http://localhost:5002"

REM Run with the project's virtual environment, unbuffered so logs stream live
"%~dp0venv\Scripts\python.exe" -u web_app.py

echo.
echo ============================================================
echo  Server stopped (exit code %errorlevel%).
echo ============================================================
pause

