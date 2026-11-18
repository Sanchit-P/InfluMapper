@echo off
REM ============================================================
REM  InfluMapper - one-click startup
REM  Builds the backend if needed, launches it, opens the UI.
REM  The C++ server serves frontend/index.html itself - no
REM  separate frontend process required.
REM ============================================================
setlocal
cd /d "%~dp0backend"

REM --- MSYS2 toolchain PATH (ucrt64 first avoids mingw64 DLL shadowing) ---
set "PATH=C:\msys64\ucrt64\bin;%PATH%"

REM --- Decide whether a rebuild is needed ---
set "NEED_BUILD=0"
if not exist influence_mapper.exe set "NEED_BUILD=1"
goto :check_ts

:check_ts
if "%NEED_BUILD%"=="1" goto :build
powershell -NoProfile -Command "$src=(Get-ChildItem -Recurse src,include -Include *.cpp,*.hpp | Sort-Object LastWriteTime -Descending | Select-Object -First 1).LastWriteTime; if($src -gt (Get-Item influence_mapper.exe).LastWriteTime){ exit 1 } else { exit 0 }" >nul 2>&1
if not errorlevel 1 goto :run

:build
echo [InfluMapper] Building backend...
g++ -std=c++20 -fopenmp -O2 -Iinclude src\main.cpp -o influence_mapper.exe -lws2_32 -lwinhttp -fno-use-linker-plugin
if errorlevel 1 goto :fail

:run
REM --- Stop any previous instance so port 18080 is free ---
taskkill /F /IM influence_mapper.exe >nul 2>&1

REM --- Launch backend watchdog in its own window (auto-restarts on crash) ---
echo [InfluMapper] Starting backend on http://localhost:18080 ...
start "InfluMapper Backend" cmd /k "%~dp0backend\run_backend.bat"

REM --- Give the server a moment to bind, then open the dashboard ---
REM (ping trick: ~3s delay, works even when GNU timeout shadows timeout.exe)
ping 127.0.0.1 -n 4 >nul
start "" "http://localhost:18080"

echo [InfluMapper] Dashboard opening in your browser.
echo            Backend logs are in the separate console window.
echo            Press any key to close this launcher (backend keeps running)...
pause >nul
endlocal
exit /b 0

:fail
echo [InfluMapper] Build failed. Check g++ is on PATH (MSYS2 ucrt64).
pause
endlocal
exit /b 1
