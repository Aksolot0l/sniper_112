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
# Прогон №8: 30 минут, один процесс, две книги (S1/S0), выход fee_cover.
export RUN_DURATION_SEC="${RUN_DURATION_SEC:-1800}"
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
export FILTER_CONCURRENCY='4'
export INDEXER_RETRIES='0'
export INDEXER_RETRY_DELAY_SEC='0.12'
export RPC_BATCH_WINDOW_SEC='0.008'
export CREATOR_RISK_POLICY='warn'
export CREATOR_HISTORY_DIR="$PROJECT/runs"
export CREATOR_BLOCKLIST_PATH="$PROJECT/runs/known_scam_sellers.txt"
# --- entry v3: live momentum; the arm name is the ONLY thing that differs ---
export ARMS="${ARMS:-portfolio_s1,portfolio_s0}"
export ALLOW_SINGLE_ARM="${ALLOW_SINGLE_ARM:-1}"
export ENTRY_BIRTH_MIN_SOL="${ENTRY_BIRTH_MIN_SOL:-30.5}"
# Потолок рождения 34.5 (а не 50): v_sol_birth = 29.96 + dev_buy, потолок 50
# пускал монеты с покупкой дева 4-6 SOL; 36 из 50 таких падали <= -25%.
export ENTRY_BIRTH_MAX_SOL="${ENTRY_BIRTH_MAX_SOL:-34.5}"
export ENTRY_DELTA_MIN_SOL="${ENTRY_DELTA_MIN_SOL:-0.10}"
export ENTRY_DELTA_MAX_SOL="${ENTRY_DELTA_MAX_SOL:-2.5}"
# --- the three experimental arms: one entry boundary each -------------------
export ENTRY_DELTA_MAX_SOL_B1="${ENTRY_DELTA_MAX_SOL_B1:-1.00}"
export ENTRY_BIRTH_MAX_SOL_B2="${ENTRY_BIRTH_MAX_SOL_B2:-31.00}"
export ENTRY_DELTA_MIN_SOL_B3="${ENTRY_DELTA_MIN_SOL_B3:-0.00}"
# --- run #8: S1 is B1 verbatim, S0 is S1 minus the momentum gate ------------
export ENTRY_DELTA_MAX_SOL_S1="${ENTRY_DELTA_MAX_SOL_S1:-1.00}"
export ENTRY_DELTA_MIN_SOL_S0="${ENTRY_DELTA_MIN_SOL_S0:-0.00}"
# --- run #8 exit: hard stop -> cover the round trip -> 5 s clock ------------
export EXIT_MODE="${EXIT_MODE:-fee_cover}"
export EXIT_FEE_COVER_PCT="${EXIT_FEE_COVER_PCT:-3.5}"
export EXIT_TIME_SEC="${EXIT_TIME_SEC:-5.0}"
export EXIT_HARD_STOP_PCT="${EXIT_HARD_STOP_PCT:-10.0}"
# --- run #8 fast entry and fast marks ---------------------------------------
export ENTRY_LATENCY_MS="${ENTRY_LATENCY_MS:-0}"
export MARK_POLL_SEC="${MARK_POLL_SEC:-0.15}"
# --- passive coin features (0 RPC, decisions never read them) ---------------
export ENTRY_ENRICH="${ENTRY_ENRICH:-1}"
export CREATOR_GUARD_MIN_SIG="${CREATOR_GUARD_MIN_SIG:-25}"
export CREATOR_BURST_WINDOW_SEC="${CREATOR_BURST_WINDOW_SEC:-3600}"
export CREATOR_CACHE_TTL_SEC="${CREATOR_CACHE_TTL_SEC:-300}"
export TELEMETRY_RUN_ID="$RUN_ID"
# Run #8: only 0 and 5 s — the poll cadence moved from 0.40 to 0.15 s.
export TELEMETRY_OBSERVATION_OFFSETS_SEC="${TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}"
export WALLET_PATH="$RUN_DIR/wallet_portfolio_s1.json"
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
import tempfile
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlsplit

from config import load_settings
from detector import PUMP_PROGRAM_ID, select_programs
from main_ab import _enrich_enabled, _observation_offsets
from portfolio_ab import (
    ARM_ENV_KEYS, ARM_IDS, ARM_LABELS,
    MARK_POLL_ADAPTED_SEC, MARK_POLL_RPS_LIMIT,
    STOP_DISK_FREE_BYTES, STOP_HELIUS_QUOTA_PCT, STOP_RPC_ERROR_RATE,
    STOP_RPC_LATENCY_HOLD_SEC, STOP_STALL_SEC,
    ABPaperSimulator, PortfolioBook, arm_profiles, mark_poll_sec_from_env,
    profiles_from_settings,
)
from pump_curve_quote import PUMP_FEE_CONFIG_PDA, PUMP_GLOBAL_PDA

s = load_settings()
assert s.api_key
assert 'helius' in (urlsplit(s.http_rpc_url).hostname or '').lower()
assert s.sources == 'pumpfun' and len(select_programs(s.sources)) == 1
assert PUMP_PROGRAM_ID in select_programs(s.sources)
assert s.deposit_rub == 1500 and s.max_positions == 3 and s.max_trade_rub == 500
assert s.mode == 'portfolio' and s.honeypot_check is False

# --- run #7 arms, ONE entry boundary each, one exit ladder ------------------
# Run #8 adds the S1/S0 pair (S0 vs S1, not S0 vs A), checked further down.
profiles = arm_profiles()
assert tuple(profiles) == ARM_IDS, (tuple(profiles), ARM_IDS)
assert [ARM_LABELS[name] for name in profiles] == [
    'A', 'B1', 'B2', 'B3', 'S1', 'S0'], [ARM_LABELS[name] for name in profiles]
reference = asdict(profiles['portfolio_a'])
for name in ('portfolio_b1_narrow', 'portfolio_b2_birth', 'portfolio_b3_t0'):
    arm = asdict(profiles[name])
    diff = {
        key for key in reference
        if key not in ('name', 'arm_id', 'entry_band_label')
        and reference[key] != arm[key]
    }
    expected = ARM_ENV_KEYS[name][1]
    assert diff == {expected}, (name, diff, expected)
    assert expected in ('delta_max_sol', 'v_sol_birth_max', 'delta_min_sol')
# The declared bands, spelled out: a silent env change must not slip through.
assert profiles['portfolio_a'].entry_band_label == (
    '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 2.50')
assert profiles['portfolio_b1_narrow'].entry_band_label == (
    '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 1.00')
assert profiles['portfolio_b2_birth'].entry_band_label == (
    '30.50 <= v_sol_birth <= 31.00; 0.10 <= delta <= 2.50')
assert profiles['portfolio_b3_t0'].entry_band_label == (
    '30.50 <= v_sol_birth <= 34.50; 0.00 <= delta <= 2.50')
# --- run #8 pair: S1 is B1 verbatim, S0 is S1 minus the momentum gate ------
assert (profiles['portfolio_s1'].entry_band_label
        == profiles['portfolio_b1_narrow'].entry_band_label)
assert profiles['portfolio_s0'].entry_band_label == (
    '30.50 <= v_sol_birth <= 34.50; 0.00 <= delta <= 1.00')
_s0_diff = {
    key for key, value in asdict(profiles['portfolio_s0']).items()
    if key not in ('name', 'arm_id', 'entry_band_label')
    and asdict(profiles['portfolio_s1'])[key] != value
}
assert _s0_diff == {'delta_min_sol'}, _s0_diff
# B3 is the t0 arm: it keeps A's ceilings and only drops the momentum floor.
assert profiles['portfolio_b3_t0'].delta_max_sol == 2.50
assert profiles['portfolio_b3_t0'].v_sol_birth_max == 34.50
# Shared everything else: stake, bank, slots, entry latency, exit ladder.
for name, profile in profiles.items():
    assert profile.position_size_rub == 500.0, name
    assert profile.initial_balance_rub == 1500.0, name
    assert profile.max_slots == 3, name
    assert profile.auto_top_up is True, name
assert len({asdict(p)['tp1_pct'] for p in profiles.values()}) == 1
assert len({asdict(p)['stop_loss_pct'] for p in profiles.values()}) == 1
assert len({asdict(p)['entry_latency_ms'] for p in profiles.values()}) == 1
assert profiles['portfolio_a'].tp1_pct == 30.0
assert profiles['portfolio_a'].rocket_bypass_pct == 50.0
assert profiles['portfolio_a'].stop_loss_pct == 10.0
assert profiles['portfolio_a'].shelf_stagnation_sec == 4.5
assert profiles['portfolio_a'].no_progress_sec == 5.0

# Entry: A waits for an impulse, B3 buys the first read.
assert profiles['portfolio_a'].entry_check(31.0, 31.0) == (False, 'no_live_momentum')
assert profiles['portfolio_b3_t0'].entry_check(31.0, 31.0) == (True, 'live_momentum_entry')
# Nobody buys above the 2.5 delta ceiling: the wide arm is closed for good.
for profile in profiles.values():
    assert profile.entry_check(32.0, 35.0)[0] is False
# PERFORMA (born 39.88) is rejected by the 34.5 birth ceiling.
assert profiles['portfolio_a'].entry_check(39.88, 40.41)[1] == 'birth_out_of_band'

# --- run #8: the LIVE set of books, built the way the run builds it ---------
# `arm_profiles()` above is the pure catalogue; these are the books that will
# actually trade, read through the same env this launcher exports.
live = profiles_from_settings(s)
assert tuple(live) == ('portfolio_s1', 'portfolio_s0'), tuple(live)
assert 'portfolio_a' not in live
assert os.getenv('ALLOW_SINGLE_ARM') == '1'
assert live['portfolio_s1'].entry_band_label == (
    '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 1.00')
assert live['portfolio_s0'].entry_band_label == (
    '30.50 <= v_sol_birth <= 34.50; 0.00 <= delta <= 1.00')
_live_s0_diff = {
    key for key, value in asdict(live['portfolio_s0']).items()
    if key not in ('name', 'arm_id', 'entry_band_label')
    and asdict(live['portfolio_s1'])[key] != value
}
assert _live_s0_diff == {'delta_min_sol'}, _live_s0_diff
# One exit for both books, and it is the declared fee_cover rule.
assert {p.exit_mode for p in live.values()} == {'fee_cover'}
assert {(p.fee_cover_pct, p.time_exit_sec, p.hard_stop_pct) for p in live.values()} == {
    (3.5, 5.0, 10.0)}
assert {p.entry_latency_ms for p in live.values()} == {0.0}
for name, profile in live.items():
    assert profile.position_size_rub == 500.0, name
    assert profile.initial_balance_rub == 1500.0, name
    assert profile.max_slots == 3, name
    assert profile.auto_top_up is True, name
# The three run #8 exit labels map to their own categories, never to unknown.
assert ABPaperSimulator.exit_reason_kind('Hard stop (−10.0%)') == 'hard_stop'
assert ABPaperSimulator.exit_reason_kind('Fee cover (+3.5%)') == 'fee_cover'
assert ABPaperSimulator.exit_reason_kind('Time exit (5.0s)') == 'time_exit'
assert 'fee cover' in ABPaperSimulator._exit_rule_label(live['portfolio_s1'])

# --- observation schedule, mark cadence and passive features ----------------
assert _observation_offsets() == (0, 5), _observation_offsets()
assert mark_poll_sec_from_env() == 0.15, mark_poll_sec_from_env()
assert MARK_POLL_RPS_LIMIT == 2.2 and MARK_POLL_ADAPTED_SEC == 0.25
assert _enrich_enabled() is True

# --- stop-list thresholds (the 2 % floor may not be raised) ------------------
assert STOP_RPC_ERROR_RATE == 0.02
assert STOP_RPC_LATENCY_HOLD_SEC == 300.0
assert STOP_STALL_SEC == 1800.0
assert STOP_HELIUS_QUOTA_PCT == 10.0
assert STOP_DISK_FREE_BYTES == 2 * 1024 ** 3
assert PUMP_GLOBAL_PDA and PUMP_FEE_CONFIG_PDA

# --- bank: top-up only when cash < 500, stake and slots never change --------
# Run #8: each book keeps its OWN 1500 RUB bank; nothing halves it.
_bank_dir = Path(tempfile.mkdtemp())
_book = PortfolioBook(
    live['portfolio_s1'], _bank_dir / 'wallet_bank.json', cash_balance_rub=300.0)
_event = _book.maybe_top_up(at_epoch=1_700_000_000.0)
assert _event is not None and _event['cash_before'] == 300.0
assert _event['cash_after'] == 1500.0 and _event['amount_rub'] == 1200.0
assert _book.top_ups == 1 and _book.capital_injected_rub == 1200.0
assert _book.maybe_top_up() is None

assert s.entry_fee_sol == 0.00008 and s.exit_fee_sol == 0.00005
assert os.getenv('PAPER_ONLY') == '1'
assert os.getenv('SOLANA_HTTP_RPC_URL') is None and os.getenv('SOLANA_WSS_RPC_URL') is None
print('run #8 preflight OK: Helius host only; read-only; paper-only; one pumpfun '
      'source; two books S1 (B1 itself) and S0 (S1 minus the momentum gate), no '
      'A in this run; one fee_cover exit for both (hard stop -10 %, cover +3.5 %, '
      'else 5 s, always 100 %); ENTRY_LATENCY_MS=0; MARK_POLL_SEC=0.15 with the '
      '0.25 s adaptation at 2.2 rps/10 min; offsets 0/5; enrich on; each book '
      'tops itself up to 1500 at cash<500; stop-list RPC floor 2 % not raised.')
PY

# Stream logs without echoing environment variables or keyed URLs.
set +e
"$PYTHON" -u main_ab.py 2>&1 | tee -a "$RUN_DIR/run.log"
status=${PIPESTATUS[0]}
set -e
exit "$status"
