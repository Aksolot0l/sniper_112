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
rem Run #7: four arms; each experimental arm moves ONE entry boundary of A.
rem A is the reference (30.5-34.5 / 0.10-2.50); exit, slots and stake are shared.
if "%ENTRY_DELTA_MAX_SOL%"=="" set "ENTRY_DELTA_MAX_SOL=2.5"
if "%ENTRY_DELTA_MAX_SOL_B1%"=="" set "ENTRY_DELTA_MAX_SOL_B1=1.00"
if "%ENTRY_BIRTH_MAX_SOL_B2%"=="" set "ENTRY_BIRTH_MAX_SOL_B2=31.00"
if "%ENTRY_DELTA_MAX_SOL_B3%"=="" set "ENTRY_DELTA_MAX_SOL_B3=8.00"
rem Passive Jupiter datapi feature collection: zero RPC, decisions unaffected.
if "%ENTRY_ENRICH%"=="" set "ENTRY_ENRICH=1"
rem Observation schedule: 0/5/15/60 s, four reads instead of six.
if "%TELEMETRY_OBSERVATION_OFFSETS_SEC%"=="" set "TELEMETRY_OBSERVATION_OFFSETS_SEC=0,5,15,60"
echo.
echo Dashboard: http://127.0.0.1:8767/  - closed with Ctrl+C
echo This window must stay open for the whole run.
"%VPY%" run_local_session.py --duration-sec 28800 --open-browser %*
set "RC=%errorlevel%"
echo.
echo Session finished with code %RC%.
pause
exit /b %RC%
