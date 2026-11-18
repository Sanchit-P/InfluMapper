@echo off
REM Build script for Social Media Influence Mapper on Windows
setlocal enabledelayedexpansion

echo =========================================================
echo  Building Social Media Influence Mapper (C++20 Engine)
echo =========================================================

REM Add MSYS2 UCRT64 to PATH if present
if exist "C:\msys64\ucrt64\bin" (
    set "PATH=C:\msys64\ucrt64\bin;!PATH!"
)

if not exist build mkdir build
cd build

echo [1/3] Running CMake...
cmake -G "MinGW Makefiles" ../backend
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] CMake configuration failed.
    pause
    exit /b %ERRORLEVEL%
)

echo [2/3] Compiling C++20 Core Engine...
cmake --build . --config Release
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Compilation failed.
    pause
    exit /b %ERRORLEVEL%
)

echo [3/3] Running PRD Test Suite...
engine_tests.exe
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Unit test failure.
    pause
    exit /b %ERRORLEVEL%
)

echo =========================================================
echo  Build Successful! Run: build\influence_mapper.exe
echo =========================================================
pause
