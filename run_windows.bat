@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
cd /d "%~dp0"

where py >nul 2>&1
if errorlevel 1 goto use_python
py -3 launcher.py
goto finished

:use_python
where python >nul 2>&1
if errorlevel 1 goto no_python
python launcher.py
goto finished

:no_python
echo Python 3 was not found. Install it from https://www.python.org/downloads/windows/
set "EXIT_CODE=9009"
goto done

:finished
set "EXIT_CODE=%ERRORLEVEL%"

:done
echo.
if "%EXIT_CODE%"=="0" goto pause_and_exit
echo Republisher stopped with code %EXIT_CODE%. See the error above and logs\republisher.log.

:pause_and_exit
pause
exit /b %EXIT_CODE%
