@echo off
REM InfluMapper backend watchdog - auto-restarts the server if it exits.
cd /d "%~dp0"
set "PATH=C:\msys64\ucrt64\bin;%PATH%"

:loop
echo [InfluMapper] Backend starting...
influence_mapper.exe
echo.
echo [InfluMapper] Backend exited (code %errorlevel%). Restarting in 2s... (Ctrl+C to stop)
ping 127.0.0.1 -n 3 >nul
goto loop
