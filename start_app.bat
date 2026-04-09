@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "PIP_DEFAULT_TIMEOUT=180"
set "PIP_RETRIES=10"

set "PY=py -3"
where py >nul 2>nul
if errorlevel 1 (
    set "PY=python"
    where python >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] Python not found. Install Python 3.11+ and add to PATH, or use the "py" launcher.
        pause
        exit /b 1
    )
)

if not exist ".venv" (
    echo [1/4] Creating virtual environment...
    %PY% -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Could not create .venv
        pause
        exit /b 1
    )
)

echo [2/4] Upgrading pip (timeout %PIP_DEFAULT_TIMEOUT%s^)...
call ".venv\Scripts\python.exe" -m pip install --upgrade pip --retries %PIP_RETRIES% --default-timeout %PIP_DEFAULT_TIMEOUT%
if errorlevel 1 (
    echo [WARN] pip upgrade failed, continuing with existing pip.
)

echo [3/4] Installing dependencies...
call ".venv\Scripts\python.exe" -m pip install -r requirements.txt --retries %PIP_RETRIES% --default-timeout %PIP_DEFAULT_TIMEOUT%
if errorlevel 1 (
    echo [ERROR] pip install failed.
    pause
    exit /b 1
)

echo [4/4] Starting Streamlit...
set "STREAMLIT_BROWSER_GATHER_USAGE_STATS=false"
call ".venv\Scripts\python.exe" -m streamlit run streamlit_app.py --browser.gatherUsageStats false

echo App stopped.
pause
exit /b 0
