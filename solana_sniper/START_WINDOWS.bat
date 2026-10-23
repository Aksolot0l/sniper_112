@echo off
setlocal EnableExtensions
cd /d "%~dp0"
title Solana Pump.fun - PAPER A/B

echo ==============================================================
echo  Solana Pump.fun - PAPER - books S1/S0 - fee_cover exit
echo  duration from DURATION_SEC (default 1800 s = 30 min)
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
rem Run #8: two books, no reference arm A in this run (no A comparison).
rem S1 = B1 verbatim (30.5-34.5 / 0.10-1.00); S0 = S1 without the momentum gate.
rem One exit for both books: hard stop -10%, cover +3.5%, else the 5 s clock.
if "%ARMS%"=="" set "ARMS=portfolio_s1,portfolio_s0"
if "%ALLOW_SINGLE_ARM%"=="" set "ALLOW_SINGLE_ARM=1"
if "%ENTRY_BIRTH_MIN_SOL%"=="" set "ENTRY_BIRTH_MIN_SOL=30.5"
if "%ENTRY_BIRTH_MAX_SOL%"=="" set "ENTRY_BIRTH_MAX_SOL=34.5"
if "%ENTRY_DELTA_MIN_SOL%"=="" set "ENTRY_DELTA_MIN_SOL=0.10"
if "%ENTRY_DELTA_MAX_SOL%"=="" set "ENTRY_DELTA_MAX_SOL=2.5"
if "%ENTRY_DELTA_MAX_SOL_S1%"=="" set "ENTRY_DELTA_MAX_SOL_S1=1.00"
if "%ENTRY_DELTA_MIN_SOL_S0%"=="" set "ENTRY_DELTA_MIN_SOL_S0=0.00"
if "%EXIT_MODE%"=="" set "EXIT_MODE=fee_cover"
if "%EXIT_FEE_COVER_PCT%"=="" set "EXIT_FEE_COVER_PCT=3.5"
if "%EXIT_TIME_SEC%"=="" set "EXIT_TIME_SEC=5.0"
if "%EXIT_HARD_STOP_PCT%"=="" set "EXIT_HARD_STOP_PCT=10.0"
if "%ENTRY_LATENCY_MS%"=="" set "ENTRY_LATENCY_MS=0"
if "%MARK_POLL_SEC%"=="" set "MARK_POLL_SEC=0.15"
if "%MAX_POSITIONS%"=="" set "MAX_POSITIONS=3"
if "%MAX_TRADE_RUB%"=="" set "MAX_TRADE_RUB=500"
if "%DEPOSIT_RUB%"=="" set "DEPOSIT_RUB=1500"
rem Passive Jupiter datapi feature collection: zero RPC, decisions unaffected.
if "%ENTRY_ENRICH%"=="" set "ENTRY_ENRICH=1"
rem Observation schedule in this run: 0 and 5 s only (RPS budget).
if "%TELEMETRY_OBSERVATION_OFFSETS_SEC%"=="" set "TELEMETRY_OBSERVATION_OFFSETS_SEC=0,5"
if "%DURATION_SEC%"=="" set "DURATION_SEC=1800"
echo.
echo Run: books S1+S0, duration %DURATION_SEC% s (default 30 min).
echo Dashboard: http://127.0.0.1:8767/  - closed with Ctrl+C
echo This window must stay open for the whole run.
"%VPY%" run_local_session.py --duration-sec %DURATION_SEC% --open-browser %*
set "RC=%errorlevel%"
echo.
echo Session finished with code %RC%.
pause
exit /b %RC%
