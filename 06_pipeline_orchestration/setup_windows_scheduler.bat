@echo off
echo ============================================================================
echo SETTING UP WINDOWS TASK SCHEDULER FOR AUTOMATED PIPELINE
echo ============================================================================
echo.

set SCRIPT_DIR=%~dp0
set PYTHON_PATH=c:\python314\python.exe
set SCHEDULER_SCRIPT=%SCRIPT_DIR%scheduler.py

echo Script directory: %SCRIPT_DIR%
echo Python path: %PYTHON_PATH%
echo Scheduler script: %SCHEDULER_SCRIPT%
echo.

echo Creating scheduled task...
schtasks /create ^
    /tn "BusinessIntelligencePipeline" ^
    /tr "%PYTHON_PATH% %SCHEDULER_SCRIPT% --now" ^
    /sc daily ^
    /st 02:00 ^
    /ru "System" ^
    /f

if %errorlevel% equ 0 (
    echo.
    echo ============================================================================
    echo SUCCESS: Scheduled task created
    echo Task name: BusinessIntelligencePipeline
    echo Runs daily at: 2:00 AM
    echo ============================================================================
) else (
    echo.
    echo ============================================================================
    echo ERROR: Failed to create scheduled task
    echo Please run as Administrator
    echo ============================================================================
)

echo.
echo To remove the task, run: schtasks /delete /tn "BusinessIntelligencePipeline" /f
echo To run manually, run: python scheduler.py --now
echo.

pause