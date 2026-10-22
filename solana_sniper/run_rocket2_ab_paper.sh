#!/usr/bin/env bash
# Solana Pump.fun PAPER A/B launcher (read-only RPC, never signs or sends).
# All paths are resolved relative to this file, so the project can be cloned or
# unzipped anywhere (VPS, laptop, another sandbox) without edits.
set -euo pipefail
set +x

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-$PROJECT/.venv/bin/python3}"
if [[ ! -x "$PYTHON" ]]; then
    if [[ -x "$PROJECT/.venv/Scripts/python.exe" ]]; then
        PYTHON="$PROJECT/.venv/Scripts/python.exe"
    fi
fi
mkdir -p "$PROJECT/runs"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N4 -tx1 /dev/urandom | tr -d '[:space:]')}"
RUN_DIR="$PROJECT/runs/rocket2-ab-paper-$RUN_ID"
LOCK="$PROJECT/runs/rocket2-ab-paper.lock"

cd "$PROJECT"
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo 'ERROR: RUN_ID may contain only letters, digits, dot, underscore, and hyphen.'
    exit 8
fi
if [[ -z "${HELIUS_API_KEY:-}" ]]; then
    echo 'ERROR: HELIUS_API_KEY is not present in the process environment; no run started.'
    exit 10
fi
if [[ -e "$RUN_DIR" ]]; then
    echo 'ERROR: this RUN_ID already has an archive/state directory; refusing reuse.'
    exit 9
fi
if [[ ! -x "$PYTHON" ]]; then
    echo 'ERROR: project virtualenv missing; run START_MAC_LINUX.sh (or START_WINDOWS.bat) first.'
    exit 4
fi
if pgrep -af "[m]ain_ab.py" | grep -F "$PROJECT" >/dev/null; then
    echo 'ERROR: another A/B paper runner for this project is active; refusing a second session.'
    exit 2
fi
mkdir -p "$PROJECT/runs"
exec 9>"$LOCK"
if ! flock -n 9; then
    echo 'ERROR: A/B paper runner lock is already held.'
    exit 3
fi
mkdir -p "$RUN_DIR/exports"

# Enforce the isolated A/B-only paper profile. Secrets remain in inherited
# process environment only; no keyed URL is exported to disk or printed.
unset SOLANA_HTTP_RPC_URL SOLANA_WSS_RPC_URL
export RUN_ID RUN_DIR
export RUN_DURATION_SEC="${RUN_DURATION_SEC:-9000}"
export PAPER_ONLY='1'
export MODE='portfolio'
export SOURCES='pumpfun'
export PORTFOLIO_SCOPE='ab_only'
export PUMP_SOURCE='pumpportal'
export DEPOSIT_RUB='1500'
export MAX_POSITIONS='3'
export MAX_TRADE_RUB='500'
export MAX_HOLD_SEC='90'
export USD_RUB_RATE='90'
export ENTRY_FEE_SOL='0.00008'
export EXIT_FEE_SOL='0.00005'
export PROTOCOL_FEE_PCT='1.25'
export HONEYPOT_CHECK='off'
export ONLY_SOL_PAIRS='on'
export SHADOW_NOTIONAL_RUB='0'
export FILTER_CONCURRENCY='4'
export INDEXER_RETRIES='0'
export INDEXER_RETRY_DELAY_SEC='0.12'
export RPC_BATCH_WINDOW_SEC='0.008'
export CREATOR_RISK_POLICY='warn'
export CREATOR_HISTORY_DIR="$PROJECT/runs"
export CREATOR_BLOCKLIST_PATH="$PROJECT/runs/known_scam_sellers.txt"
# --- entry v3: live momentum instead of the anti-filter delta cap ---
export ENTRY_BIRTH_MIN_SOL="${ENTRY_BIRTH_MIN_SOL:-30.5}"
export ENTRY_BIRTH_MAX_SOL="${ENTRY_BIRTH_MAX_SOL:-50.0}"
export ENTRY_DELTA_MIN_SOL="${ENTRY_DELTA_MIN_SOL:-0.10}"
export ENTRY_DELTA_MAX_SOL="${ENTRY_DELTA_MAX_SOL:-2.5}"
# --- creator guard: B-only treatment, in-memory wallet cache keeps RPS flat ---
export CREATOR_GUARD_MIN_SIG="${CREATOR_GUARD_MIN_SIG:-25}"
export CREATOR_BURST_WINDOW_SEC="${CREATOR_BURST_WINDOW_SEC:-3600}"
export CREATOR_CACHE_TTL_SEC="${CREATOR_CACHE_TTL_SEC:-300}"
export TELEMETRY_RUN_ID="$RUN_ID"
export TELEMETRY_OBSERVATION_OFFSETS_SEC='0,5,10,15,60,120'
export WALLET_PATH="$RUN_DIR/wallet_portfolio_a.json"
export TRADE_LOG="$RUN_DIR/trades.log"
export DB_PATH="$RUN_DIR/sniper.db"
export TELEMETRY_DB_PATH="$RUN_DIR/telemetry.sqlite"
export TELEMETRY_EXPORT_DIR="$RUN_DIR/exports"

"$PYTHON" -m py_compile \
  config.py creator_watch.py creator_probe.py telemetry.py filters.py price.py \
  pumpportal_validation.py pump_curve_quote.py portfolio_ab.py main_ab.py detector.py \
  storage.py models.py sqlite_snapshots.py live_ab_dashboard.py run_overnight_ab_paper.py || exit 5
"$PYTHON" -m unittest discover -s tests -v > "$RUN_DIR/preflight_tests.log" 2>&1 || {
    echo 'ERROR: preflight tests failed; no run started.'
    tail -n 80 "$RUN_DIR/preflight_tests.log"
    exit 6
}
if [[ ! "$RUN_DURATION_SEC" =~ ^[0-9]+$ ]]; then
  echo 'ERROR: RUN_DURATION_SEC must be an integer; no run started.'
  exit 7
fi
if [[ -n "${OVERNIGHT_RUN_DEADLINE_EPOCH:-}" ]]; then
  remaining="$($PYTHON -c 'import os,time; print(max(0,int(float(os.environ["OVERNIGHT_RUN_DEADLINE_EPOCH"])-time.time())))')"
  if (( remaining < 1 )); then
    echo 'ERROR: overnight active deadline has elapsed; no run started.'
    exit 7
  fi
  if (( remaining < RUN_DURATION_SEC )); then
    RUN_DURATION_SEC="$remaining"
  fi
fi
export RUN_DURATION_SEC

"$PYTHON" - <<'PY'
import os
from urllib.parse import urlsplit
from dataclasses import asdict
from config import load_settings
from detector import PUMP_PROGRAM_ID, select_programs
from main_ab import _observation_offsets
from portfolio_ab import (
    PROFILE_A, PROFILE_B, entry_shadow_flags, profiles_from_settings,
)
from pump_curve_quote import PUMP_FEE_CONFIG_PDA, PUMP_GLOBAL_PDA
s = load_settings()
assert s.api_key
assert 'helius' in (urlsplit(s.http_rpc_url).hostname or '').lower()
assert s.sources == 'pumpfun' and len(select_programs(s.sources)) == 1
assert PUMP_PROGRAM_ID in select_programs(s.sources)
assert s.deposit_rub == 1500 and s.max_positions == 3 and s.max_trade_rub == 500
assert s.mode == 'portfolio' and s.honeypot_check is False
assert PROFILE_A.initial_balance_rub == PROFILE_B.initial_balance_rub == 1500
assert PROFILE_A.position_size_rub == PROFILE_B.position_size_rub == 500
assert PROFILE_A.max_slots == PROFILE_B.max_slots == 3
guarded = [p for p in profiles_from_settings(s).values() if not p.require_creator_gate]
assert len(guarded) == 1
assert {k: v for k, v in asdict(PROFILE_A).items() if k not in {'name', 'require_creator_gate'}} == {
    k: v for k, v in asdict(PROFILE_B).items() if k not in {'name', 'require_creator_gate'}
}
assert PROFILE_A.require_creator_gate is False and PROFILE_B.require_creator_gate is True
# Approved exit ladder must stay 1:1 with the spec.
assert PROFILE_A.tp1_pct == 30.0 and PROFILE_A.rocket_bypass_pct == 50.0
assert PROFILE_A.profit_retain_ratio == 0.85 and PROFILE_A.stop_loss_pct == 10.0
assert PROFILE_A.shelf_stagnation_sec == 4.5 and PROFILE_A.shelf_min_pnl_pct == 1.5
assert PROFILE_A.no_progress_sec == 5.0 and PROFILE_A.no_progress_best_max_pct == 4.0
# Entry v3: no dead delta==0 fills, no tight cap on real runners.
ok, reason = PROFILE_A.entry_check(31.0, 31.0)
assert not ok and reason == 'no_live_momentum'
assert PROFILE_A.delta_max_sol == 2.5
ok, reason = PROFILE_A.entry_check(33.95, 45.51)   # Turtle delta 11.56
assert not ok and reason == 'momentum_too_late'
ok, _ = PROFILE_A.entry_check(39.88, 40.41)        # PERFORMA delta 0.53
assert ok
assert entry_shadow_flags(PROFILE_A, 33.0, 36.0) == {
    'pass_if_cap_1': False, 'pass_if_cap_2.5': False,
    'pass_if_cap_4': True, 'pass_if_cap_12': True,
}
assert PUMP_GLOBAL_PDA and PUMP_FEE_CONFIG_PDA
assert s.entry_fee_sol == 0.00008 and s.exit_fee_sol == 0.00005
assert os.getenv('PAPER_ONLY') == '1'
assert os.getenv('SHADOW_NOTIONAL_RUB') == '0'
assert _observation_offsets() == (0, 5, 10, 15, 60, 120)
assert s.entry_delta_max_sol == 2.5
assert s.entry_fee_sol == 0.00008 and s.exit_fee_sol == 0.00005
assert os.getenv('SOLANA_HTTP_RPC_URL') is None and os.getenv('SOLANA_WSS_RPC_URL') is None
print('A/B preflight OK: Helius host only; read-only; paper-only; one pumpfun source; '
      'live-momentum entry; approved 4-mode exits; B creator guard only; '
      'in-memory wallet cache enabled.')
PY

# Stream logs without echoing environment variables or keyed URLs.
set +e
"$PYTHON" -u main_ab.py 2>&1 | tee -a "$RUN_DIR/run.log"
status=${PIPESTATUS[0]}
set -e
exit "$status"
