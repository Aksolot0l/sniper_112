@echo off
setlocal EnableExtensions
cd /d "%~dp0"
title Solana Pump.fun - PAPER A/B

echo ==============================================================
echo  Solana Pump.fun - PAPER A/B - 2h30m local run
echo  paper-only: no transactions are signed or sent
echo ==============================================================
echo.

set "VPY=.venv\Scripts\python.exe"
if not exist "%VPY%" goto make_venv
goto check_deps

:make_venv
echo Creating .venv ...
where py >nul 2>nul
if %errorlevel%==0 (
    py -3 -m venv .venv
) else (
    python -m venv .venv
)
if not exist "%VPY%" (
    echo ERROR: could not create .venv. Install Python 3.10+ from python.org and rerun.
    pause
    exit /b 1
)

:check_deps
"%VPY%" -c "import aiohttp, websockets, aiosqlite" >nul 2>nul
if %errorlevel%==0 goto have_deps
echo Installing dependencies ...
"%VPY%" -m pip install --quiet --upgrade pip
"%VPY%" -m pip install --quiet -r requirements.txt
if not %errorlevel%==0 (
    echo ERROR: dependency install failed.
    pause
    exit /b 1
)

:have_deps
if not "%HELIUS_API_KEY%"=="" goto have_key
echo Paste HELIUS_API_KEY below. It is used only in memory and never written to disk.
set /p HELIUS_API_KEY=HELIUS_API_KEY: 
if "%HELIUS_API_KEY%"=="" (
    echo ERROR: empty key, nothing started.
    pause
    exit /b 1
)

:have_key
rem Run #3: the only difference between arms is the delta cap: A 2.5, B 3.0.
if "%ENTRY_DELTA_MAX_SOL%"=="" set "ENTRY_DELTA_MAX_SOL=2.5"
if "%ENTRY_DELTA_MAX_SOL_B%"=="" set "ENTRY_DELTA_MAX_SOL_B=3.0"
echo.
echo Dashboard: http://127.0.0.1:8767/  - closed with Ctrl+C
echo This window must stay open for the whole run.
"%VPY%" run_local_session.py --duration-sec 28800 --open-browser %*
set "RC=%errorlevel%"
echo.
echo Session finished with code %RC%.
pause
exit /b %RC%
