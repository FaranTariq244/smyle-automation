"""
Launcher for Smyle Automation Suite.
Compiled to "Smyle Automation.exe" with PyInstaller (see Start Smyle Automation.bat
for the equivalent batch version). Runs web_app.py with the project's venv Python
so the exe never needs rebuilding when project code changes.

Rebuild with:
    venv/Scripts/pyinstaller --onefile --console --name "Smyle Automation" launcher.py
"""

import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

URL = "http://localhost:5002"


def project_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def main() -> int:
    root = project_root()
    python = root / "venv" / "Scripts" / "python.exe"
    web_app = root / "web_app.py"

    print("=" * 60)
    print(" Smyle Automation Suite")
    print(f" Starting web app... open {URL}")
    print(" Press Ctrl+C in this window to stop the server.")
    print("=" * 60)
    print()

    if not python.exists():
        print(f"ERROR: venv Python not found at {python}")
        print("This exe must live in the project root next to the venv folder.")
        input("Press Enter to close...")
        return 1
    if not web_app.exists():
        print(f"ERROR: web_app.py not found at {web_app}")
        input("Press Enter to close...")
        return 1

    threading.Thread(
        target=lambda: (time.sleep(3), webbrowser.open(URL)), daemon=True
    ).start()

    try:
        result = subprocess.run([str(python), "-u", str(web_app)], cwd=str(root))
        code = result.returncode
    except KeyboardInterrupt:
        code = 0

    print()
    print("=" * 60)
    print(f" Server stopped (exit code {code}).")
    print("=" * 60)
    input("Press Enter to close...")
    return code


if __name__ == "__main__":
    sys.exit(main())
