@echo off
REM =============================================================================
REM NL SQL Query Builder - Auto-Update Launcher
REM This script automatically updates the app and launches it
REM =============================================================================

setlocal enabledelayedexpansion

REM Set colors for better visibility (optional)
color 0A

echo.
echo ================================================================================
echo                    NL SQL Query Builder - Starting Up
echo ================================================================================
echo.

REM Check if we're in the right directory
if not exist "physical_schema" (
    echo [ERROR] Cannot find 'physical_schema' folder.
    echo.
    echo Make sure you're running this script from the nl-sql-builder directory.
    echo Current directory: %CD%
    echo.
    echo Press any key to exit...
    pause >nul
    exit /b 1
)

REM Check for updates from GitHub
echo [1/4] Checking for updates...
echo.
git pull
if errorlevel 1 (
    echo.
    echo [WARNING] Could not check for updates. This might be okay if you're offline.
    echo Continuing with current version...
    echo.
) else (
    echo.
    echo [SUCCESS] Update check complete!
    echo.
)

REM Install/update Python dependencies
echo [2/4] Installing/updating dependencies...
echo.
pip install -q -r physical_schema/requirements.txt
if errorlevel 1 (
    echo.
    echo [ERROR] Failed to install main dependencies.
    echo.
    echo Try running this command manually:
    echo   pip install -r physical_schema/requirements.txt
    echo.
    echo Press any key to exit...
    pause >nul
    exit /b 1
)

pip install -q -r physical_schema/ui/requirements.txt
if errorlevel 1 (
    echo.
    echo [ERROR] Failed to install UI dependencies.
    echo.
    echo Try running this command manually:
    echo   pip install -r physical_schema/ui/requirements.txt
    echo.
    echo Press any key to exit...
    pause >nul
    exit /b 1
)

echo.
echo [SUCCESS] All dependencies installed!
echo.

REM Navigate to the physical_schema directory
echo [3/4] Preparing to launch...
cd physical_schema
if errorlevel 1 (
    echo.
    echo [ERROR] Could not navigate to physical_schema directory.
    echo.
    echo Press any key to exit...
    pause >nul
    exit /b 1
)

REM Launch the Streamlit app
echo.
echo [4/4] Launching Query Builder...
echo.
echo ================================================================================
echo The app will open in your web browser in a few seconds.
echo.
echo TIPS:
echo   - Keep this window open while using the app
echo   - To stop the app, press Ctrl+C in this window or just close it
echo   - The app runs at: http://localhost:8501
echo ================================================================================
echo.

REM Start Streamlit
python -m streamlit run "ui/Query Builder.py" --server.port 8501 --server.headless true

REM If Streamlit exits with an error, keep window open so user can see the error
if errorlevel 1 (
    echo.
    echo.
    echo ================================================================================
    echo [ERROR] The app stopped with an error.
    echo ================================================================================
    echo.
    echo Common fixes:
    echo   1. Make sure Python is installed: python --version
    echo   2. Make sure dependencies are installed: pip install -r requirements.txt
    echo   3. Check if port 8501 is already in use (close other instances)
    echo   4. Contact support with the error message above
    echo.
    echo Press any key to exit...
    pause >nul
    exit /b 1
)

echo.
echo App closed normally. Press any key to exit...
pause >nul
