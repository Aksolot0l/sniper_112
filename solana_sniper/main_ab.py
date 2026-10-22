"""Run isolated A/B-only Pump.fun paper portfolios; never signs/sends trades."""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from config import load_settings
from console import Color, log
from creator_probe import CreatorOnchainProbe
from creator_watch import CreatorWatch
from detector import (
    PUMP_PROGRAM_ID,
    detector_worker,
    probe_pumpportal,
    pumpportal_listener,
    select_programs,
)
from portfolio_ab import ABPaperSimulator
from price import (
    ReadOnlyRpcBatcher,
    get_sol_usd,
    reset_rpc_metrics,
    set_rpc_batcher,
)
from storage import storage_worker
from telemetry import CandidateCollector, funnel_from_db, set_collector
from ws_client import ws_listener


RUN_DURATION_SEC_DEFAULT = 3600


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(temporary, path)


def _observation_offsets() -> tuple[int, ...]:
    raw = os.getenv('TELEMETRY_OBSERVATION_OFFSETS_SEC', '0,5,10,15,60,120')
    try:
        values = sorted({int(part.strip()) for part in raw.split(',') if part.strip()})
        values = [value for value in values if value >= 0]
        return tuple(values) if values else (0, 5, 10, 15, 60, 120)
    except (TypeError, ValueError):
        return (0, 5, 10, 15, 60, 120)


def _build_commit() -> str:
    """Short code revision this process runs, without shelling out.

    Order: SNIPER_BUILD env -> pack-written BUILD file -> local git HEAD.
    The bundle ships BUILD (no .git), so a run on the user's PC still names
    the exact commit it was built from.
    """
    for env_name in ('SNIPER_BUILD', 'BUILD_COMMIT'):
        value = (os.getenv(env_name) or '').strip()
        if value:
            return value
    here = Path(__file__).resolve()
    build_file = here.with_name('BUILD')
    try:
        if build_file.is_file():
            value = build_file.read_text(encoding='utf-8').strip()
            if value:
                return value
    except OSError:
        pass
    git_dir = here.parent.parent / '.git'
    try:
        head = (git_dir / 'HEAD').read_text(encoding='utf-8').strip()
        if head.startswith('ref:'):
            ref_path = git_dir / head[4:].strip()
            if ref_path.is_file():
                head = ref_path.read_text(encoding='utf-8').strip()
        if head and not head.startswith('ref:'):
            return head[:7]
    except OSError:
        pass
    return 'unknown'


# Reference numbers from run #1 (build 9f2cd08, 30 min, 1800 s). Reported by
# the operator; kept only so SUMMARY.md can put run #2 next to run #1 without
# pretending the two were measured in one process.
BASELINE_RUN_1 = {
    'build': '9f2cd08',
    'duration_sec': 1800,
    'entry_birth_max_sol': 50.0,
    'portfolio_a': {
        'entries': 26, 'closed_trade_count': 26, 'realized_pnl_rub': -1035.56,
        'pnl_per_trade_rub': -39.83, 'conservative_100bps_pnl_rub': -1274.11,
    },
    'portfolio_b': {
        'entries': 16, 'closed_trade_count': 16, 'realized_pnl_rub': -672.77,
        'pnl_per_trade_rub': -42.05, 'conservative_100bps_pnl_rub': -818.87,
    },
    'source': 'run #1 report (30-min PAPER_ONLY run on the operator machine)',
}


def _summary_markdown(
    manifest: dict[str, Any], funnel: dict[str, Any], summary: dict[str, Any],
) -> str:
    """Render the one-page post-run report next to the raw artifacts."""
    lines: list[str] = []
    add = lines.append
    add(f"# Прогон {manifest.get('run_id')}")
    add('')
    add(f"- build: `{manifest.get('build')}` (baseline прогон №1: `{BASELINE_RUN_1['build']}`)")
    add(f"- вход: `{manifest.get('entry')}`")
    add(f"- длительность: {manifest.get('actual_duration_sec')} c "
        f"(план {manifest.get('planned_duration_sec')} c)")
    add(f"- paper-only: {manifest.get('paper_only')}; подписано транзакций: "
        f"{manifest.get('signed_transactions')}")
    stats = manifest.get('telemetry') or {}
    if stats:
        add(f"- телеметрия: dropped={stats.get('dropped_events')}, "
            f"write_errors={stats.get('write_errors')}, "
            f"writer={stats.get('writer_error') or 'ok'}")
        if stats.get('write_errors'):
            add('')
            add('> ⚠️ Часть строк телеметрии не записалась: воронка и гейт ниже '
                'построены по усечённым данным.')
    add('')
    add('## A / B')
    add('')
    add('| | portfolio_a | portfolio_b | прогон №1 A | прогон №1 B |')
    add('|---|---:|---:|---:|---:|')
    rows = (
        ('входов', 'entry_count', 'entries'),
        ('закрыто', 'closed_trade_count', 'closed_trade_count'),
        ('realized PnL, ₽', 'realized_pnl_rub', 'realized_pnl_rub'),
        ('conservative 100 bps, ₽', 'conservative_100bps_pnl_rub',
         'conservative_100bps_pnl_rub'),
    )
    def _money(value: Any) -> str:
        """Two decimals, or an em dash when the run has no such number."""
        if value is None:
            return '—'
        try:
            return f'{float(value):.2f}'
        except (TypeError, ValueError):
            return str(value)

    for label, key, base_key in rows:
        a = (summary.get('portfolio_a') or {}).get(key)
        b = (summary.get('portfolio_b') or {}).get(key)
        base_a = BASELINE_RUN_1['portfolio_a'].get(base_key, '—')
        base_b = BASELINE_RUN_1['portfolio_b'].get(base_key, '—')
        if key == 'entry_count' or key == 'closed_trade_count':
            add(f'| {label} | {a} | {b} | {base_a} | {base_b} |')
        else:
            add(f'| {label} | {_money(a)} | {_money(b)} | {_money(base_a)} | {_money(base_b)} |')
    # Per-trade PnL: one row, both arms, each value in its own column.
    per_trade: dict[str, Any] = {}
    for arm in ('portfolio_a', 'portfolio_b'):
        entries = (summary.get(arm) or {}).get('entry_count') or 0
        pnl = (summary.get(arm) or {}).get('realized_pnl_rub')
        per_trade[arm] = round(float(pnl) / entries, 2) if entries and pnl is not None else None
    add('| PnL на сделку, ₽ | '
        f'{_money(per_trade["portfolio_a"])} | {_money(per_trade["portfolio_b"])} | '
        f'{_money(BASELINE_RUN_1["portfolio_a"]["pnl_per_trade_rub"])} | '
        f'{_money(BASELINE_RUN_1["portfolio_b"]["pnl_per_trade_rub"])} |')
    add('')
    add('## Воронка')
    add('')
    add('```')
    add(str(funnel.get('chain') or 'n/a'))
    add('')
    add(f"entry_state             {funnel.get('entry_state')}")
    for reason, count in (funnel.get('portfolio_a_signal') or {}).items():
        add(f"  A signal: {reason:<52}{count}")
    for reason, count in (funnel.get('portfolio_b_signal') or {}).items():
        add(f"  B signal: {reason:<52}{count}")
    add(f"admitted_by_current_cap {funnel.get('admitted_by_current_cap')}")
    add(f"opened                  A={((funnel.get('opened') or {}).get('portfolio_a'))} "
        f"B={((funnel.get('opened') or {}).get('portfolio_b'))}")
    for arm in ('portfolio_a', 'portfolio_b'):
        for reason, count in ((funnel.get('entry_skips') or {}).get(arm) or {}).items():
            add(f"  {arm} skip: {reason:<52}{count}")
    add('```')
    add('')
    add('Дельта-полоса (сколько прошло бы при другом потолке):')
    add('')
    for key, count in (funnel.get('pass_if_cap') or {}).items():
        add(f'- `{key}`: {count}')
    add('')
    add(f"Бакеты delta: {funnel.get('delta_buckets')}")
    add('')
    add('## Гейт B: риск против слепой зоны')
    add('')
    add(f"- `gate_status`: {funnel.get('gate_status')}")
    add(f"- причины отказов: {funnel.get('gate_reasons')}")
    add(f"- идентичность дева: {funnel.get('probe_identity')}")
    a_only = funnel.get('a_only') or {}
    if a_only:
        add('- монеты, которые взял только A:')
        for klass, slot in a_only.items():
            add(f"  - `{klass}`: {slot.get('mints')} шт, pnl_leg сумма "
                f"{slot.get('pnl_leg_sum_rub')} ₽ (без входной комиссии), "
                f"причины: {slot.get('reasons')}")
    add('')
    add('## Как читать')
    add('')
    add('1. `unavailable` — это сбой телеметрии, а не сигнал риска; если его доля '
        'велика, сравнение A/B смещено.')
    add('2. `admitted_by_current_cap` против `pass_if_cap_*` отвечает, режет ли '
        'поток потолок delta, или событий просто было меньше.')
    add('3. PnL A против B имеет смысл только как проверка здравого смысла на '
        'n ≈ 10–30; доказанного эджа нет.')
    add('')
    return '\n'.join(lines)


def _strategy_metadata() -> dict[str, Any]:
    """Declared up front so a run can never be compared against an older one."""
    return {
        'build': _build_commit(),
        'build_baseline': (
            'run #1 = 9f2cd08 (30 min, birth_max 50.0): A -1035.56 RUB, '
            'B -672.77 RUB. This build is fix #7: birth_max 34.5 + diagnostics'
        ),
        'strategy_version': 'entry_v3+guard_v2+delta_cap_2.5+birth_max_34.5',
        'fix_7': (
            'v_sol_birth_max 50.0 -> 34.5. Measured on run #1: '
            'v_sol_birth = 29.96 + dev_buy_SOL (corr 0.99988, n=145), so the '
            'old ceiling admitted dev buys of 4-6 SOL; 36 of 50 such '
            'candidates fell <= -25% (median minimum -33.2%), and in the live '
            'run they were 4 mints for -538.57 RUB in A and -552.79 RUB in B. '
            'Expected 22-26 entries instead of 26 and A near -500 RUB: this '
            'cuts the catastrophic tail, it does NOT create profit.'
        ),
        'comparison_policy': 'не смешивать с сессиями до 2026-10-22',
        'timeouts_35_90': 'slot safety net, not part of 4-mode ladder',
        'known_hole': (
            'no_progress is one-shot at 5s; corridor -9.9%..+1.4% '
            'exits only by timeout'
        ),
        'known_gap': (
            'repeat_deploy indexed by program_creator only; event_actor path '
            'is log-only this run (creator_prior_tokens_10m_actor)'
        ),
        'edge_status': (
            'no proven edge; cap 2.5 chosen for drawdown/slot hygiene '
            '(archive: delta >= 2.5 -> median -19.8% drawdown, 11 of 13 '
            'observed stop-outs). It cuts 3 rockets, not 2: TzXn6LeT +106.8%, '
            'E8q82QfE +38.3%, and 6m3gx2eaH +26.8% — the last one from the '
            'delta == 0 bucket, so it is cut by delta_min 0.10, not by the cap'
        ),
        'judgement': (
            'archive: 2.5-4 median max +6.7% (0 rockets); 4-12 bimodal: '
            '9 dead / 2 rockets (+38.3%, +106.8%)'
        ),
        'delta_min_note': (
            'delta == 0 bucket: 26 of 27 observed dead, not 27 of 27; the '
            'single exception is 6m3gx2eaH +26.8%'
        ),
        'experiment_next': (
            'discriminator inside 4-12 bucket: initialBuy, 5s v_sol velocity, '
            'unique buyers. Run #1 added the missing half: the entry price is '
            'always worse than observations t0, and the 0.1-2.5 band has a '
            'median move of 0.0% with an asymmetric ladder, so zero movement '
            'becomes -6..-8% per trade. No threshold fixes that'
        ),
        'expected_flow': (
            'run #1 measured 236 entry_state per 30 min; cap 2.5 admitted 29 '
            '(12.3%), cap 4 would admit 43, cap 12 -> 103. With birth_max 34.5 '
            'expect 22-26 A entries and 3-6 B entries per 30 min. '
            'admitted_by_current_cap in the manifest answers this from data'
        ),
        'open_question_cap': (
            'delta cap 2.5 is an anti-selector: in the bought 0.1-2.5 band '
            'there was 1 rocket in 33 candidates (3%) and 0 rugs, while 4-12 '
            'held 9 rockets in 42 (21%) AND 25 rugs. Delta selects volatility, '
            'not quality. Changing the cap is fix #8, deliberately not mixed '
            'with #7'
        ),
        'entry': (
            '30.5 <= v_sol_birth <= 34.5 and '
            '0.10 <= (v_sol_now - v_sol_birth) <= 2.5'
        ),
        'entry_note': (
            'delta cap 2.5: 66 archived entry_state x observations give median '
            '-19.8% drawdown and 10/15 stop-outs for delta >= 2.5 SOL; '
            'the 12 SOL ceiling fitted the filter to one observation'
        ),
    }


def _safe_manifest(settings: Any, run_id: str, run_dir: Path) -> dict[str, Any]:
    strategy_meta = _strategy_metadata()
    common = {
        'initial_balance_rub': 1500,
        'position_size_rub': 500,
        'max_slots': 3,
        'entry_note': (
            'live-momentum band replaces the 2026-10 anti-filter '
            '(v_sol_now <= v_sol_birth + 0.8 rejected every real runner '
            'and bought dead delta==0 decoys). Birth ceiling is 34.5 since '
            'fix #7: v_sol_birth = 29.96 + dev_buy_SOL, so 50 admitted dev '
            'buys of 4-6 SOL and 36 of 50 such launches fell <= -25%'
        ),
        'stop_loss': 'before TP1/rocket bypass only, while best_pnl_pct < +10.0: pnl_pct <= -10.0; sell 100%',
        'tp1': 'when +30.0 <= pnl_pct < +50.0 before TP1/rocket bypass: sell 50%; retry same partial if exact-size quote is unavailable',
        'rocket_bypass': 'at pnl_pct >= +50.0 before TP1: retain 100%, set bypass, do not sell half',
        'trailing': (
            'once best_pnl_pct >= +10.0: sell remaining at '
            'pnl_pct <= 0.85 * best_pnl_pct (labelled SL/trail, not a take-profit)'
        ),
        'shelf_stagnation': 'sell remaining when pnl_pct >= +1.5 and no >=+0.4pp new high for 4.5s',
        'no_progress': 'once at held >= 5s: best_pnl_pct < +4.0 and pnl_pct < +1.5; evaluate once',
        'peak_timestamp_update': 'update only when pnl_pct >= best_pnl_pct + 0.4 percentage points',
        'timeout_before_trailing_sec': 35,
        'timeout_after_trailing_sec': 90,
    }
    return {
        'run_id': run_id,
        **strategy_meta,
        'status': 'starting',
        'early_stop_reason': None,
        'created_at_utc': _utc_now(),
        'planned_duration_sec': int(os.getenv('RUN_DURATION_SEC', RUN_DURATION_SEC_DEFAULT)),
        'paper_only': True,
        'signed_transactions': 0,
        'submitted_transactions': 0,
        'portfolio_scope': ['portfolio_a', 'portfolio_b'],
        'strategies': {
            'portfolio_a': {
                **common, **strategy_meta,
                'creator_probe': 'telemetry_only; no entry gate',
            },
            'portfolio_b': {
                **common, **strategy_meta,
                'creator_gate': (
                    'signature_count_0_25 >= 25 AND NOT burst (25+ sigs inside '
                    '1h) AND NOT repeat_deploy (>=2 tokens in 10m) AND '
                    'ata_exists AND token_balance_raw > 0'
                ),
            },
        },
        'rpc': {
            'provider': 'Helius',
            'http_host': urlsplit(settings.http_rpc_url).hostname,
            'wss_host': urlsplit(settings.wss_url).hostname,
            'methods': ['getMultipleAccounts', 'getSignaturesForAddress', 'getTokenAccountsByOwner'],
            'read_only': True,
            'credentials_stored': False,
            'http_shadows_enabled': False,
            'candidate_entry_batch_accounts': ['mint', 'curve', 'PUMP_GLOBAL_PDA', 'PUMP_FEE_CONFIG_PDA'],
            'candidate_entry_batch_commitment': 'processed',
            'null_retry': 'one retry after 120 ms',
            'latency_policy': 'measure actual latency/errors; no SLA or coverage promise',
        },
        'fees': {
            'source': 'on-chain Pump Global + FeeConfig; quote fills use SDK integer math',
            'entry_network_fee_sol_model': settings.entry_fee_sol,
            'exit_network_fee_sol_model_per_sell': settings.exit_fee_sol,
            'network_fees_are_simulation_assumptions': True,
            'on_chain_curve_fees': 'included in out_amount_raw; reported separately from network fees',
            'unsupported_or_unknown': 'fail closed',
        },
        'filters': {
            'sources': 'pumpfun', 'standard_supply': True, 'is_mayhem': False,
            'quote_mint': 'SOL/WSOL only', 'mint_authority_revoked': True,
            'freeze_authority_revoked': True, 'duplicate_symbol_gate': False,
            'creator_history_policy': 'advisory telemetry/warnings only',
            'portfolio_a_creator_gate': False,
            'portfolio_b_creator_gate': 'signature_count_0_25 == 25 AND derived ATA exists AND ATA token balance > 0',
            'creator_probe_failures': 'fail closed for B; do not gate A',
        },
        'observations_sec': list(_observation_offsets()),
        'observation_policy': 'scheduled real Pump curve RPC reads; no interpolation or DexScreener/GeckoTerminal fallback for Pump.fun',
        'accounting': {
            'snapshot_fill': 'out_amount_raw',
            'conservative_shadow': 'min_out_raw at QUOTE_SLIPPAGE_BPS=100',
            'quote_slippage_bps': 100,
            'gross_curve_pnl_rub': 'realized snapshot PnL before modeled network fees; on-chain curve fee remains embedded in quote output',
            'network_fees': 'tracked separately; deducted from realized returns and shown as open fee liability',
            'usd_rub_rate': settings.usd_rub_rate,
        },
        'wallet_state_files': [
            str(run_dir / 'wallet_portfolio_a.json'),
            str(run_dir / 'wallet_portfolio_b.json'),
        ],
        'data_files': {
            'sniper_db': str(run_dir / 'sniper.db'),
            'telemetry_db': str(run_dir / 'telemetry.sqlite'),
            'exports': str(run_dir / 'exports'),
        },
        'api_key_or_rpc_url_with_query': False,
    }


async def main() -> None:
    settings = load_settings()
    loop = asyncio.get_running_loop()
    external_stop = asyncio.Event()
    installed_signals: list[signal.Signals] = []
    try:
        loop.add_signal_handler(signal.SIGTERM, external_stop.set)
        installed_signals.append(signal.SIGTERM)
    except (NotImplementedError, RuntimeError, ValueError):
        pass
    run_id = os.getenv('RUN_ID', '').strip()
    run_dir_raw = os.getenv('RUN_DIR', '').strip()
    if not run_id or not run_dir_raw or not re.fullmatch(r'[A-Za-z0-9._-]+', run_id):
        raise SystemExit('RUN_ID and RUN_DIR are required and RUN_ID must be path-safe.')
    project_root = Path(__file__).resolve().parent
    runs_root = (project_root / 'runs').resolve()
    run_dir = Path(run_dir_raw).resolve()
    if run_dir.parent != runs_root or run_dir.name != f'rocket2-ab-paper-{run_id}':
        raise SystemExit('RUN_DIR must be the unique project runs/rocket2-ab-paper-<RUN_ID> archive.')
    if not run_dir.is_dir():
        raise SystemExit('RUN_DIR must already exist and be unique for this run.')
    if (run_dir / 'manifest.json').exists():
        raise SystemExit('RUN_DIR already has a manifest; refusing to overwrite an archive.')
    for existing in run_dir.iterdir():
        if existing.name == 'exports' and existing.is_dir() and not any(existing.iterdir()):
            continue
        # Files written by our own local runner before/while the bot starts.
        if existing.name in {
            'preflight_tests.log', 'run.log', 'supervisor_heartbeat.json',
            'supervisor_heartbeat.tmp',
        } and existing.is_file():
            continue
        raise SystemExit('RUN_DIR contains unexpected preexisting files; refusing to overwrite an archive.')
    if not settings.api_key:
        raise SystemExit('HELIUS_API_KEY is not present in the process environment; no RPC/run started.')
    rpc_host = (urlsplit(settings.http_rpc_url).hostname or '').lower()
    if 'helius' not in rpc_host:
        raise SystemExit('The configured HTTP RPC host is not Helius; no RPC/run started.')
    if settings.sources.strip().lower() != 'pumpfun':
        raise SystemExit('SOURCES must be exactly pumpfun for the isolated A/B run.')
    if settings.deposit_rub != 1500 or settings.max_positions != 3 or settings.max_trade_rub != 500:
        raise SystemExit('A/B wallet sizing must be 1,500 ₽ with 3 slots × 500 ₽.')
    if settings.honeypot_check:
        raise SystemExit('HTTP shadow/honeypot quote checks must be disabled for this run.')
    if os.getenv('PAPER_ONLY', '1').strip() != '1':
        raise SystemExit('PAPER_ONLY must remain enabled.')
    # Network fee model must be the real Solana cost. config.py defaults are
    # 0.001 / 0.0001 SOL for legacy callers, which would inflate fees 12.5x on a
    # direct `python main_ab.py`; refuse to run with anything but the paper model.
    if (
        not math.isclose(settings.entry_fee_sol, 0.00008, rel_tol=0, abs_tol=1e-12)
        or not math.isclose(settings.exit_fee_sol, 0.00005, rel_tol=0, abs_tol=1e-12)
    ):
        raise RuntimeError(
            'network fee model must be 0.00008 / 0.00005 SOL '
            f'(got {settings.entry_fee_sol} / {settings.exit_fee_sol}); '
            'use run_rocket2_ab_paper.sh or run_local_session.py'
        )

    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = _safe_manifest(settings, run_id, run_dir)
    _write_json(run_dir / 'manifest.json', manifest)
    reset_rpc_metrics()

    creator_history_raw = os.getenv('CREATOR_HISTORY_DIR', str(project_root / 'runs'))
    creator_history_dir = Path(creator_history_raw)
    if not creator_history_dir.is_absolute():
        creator_history_dir = project_root / creator_history_dir
    blocklist_raw = os.getenv('CREATOR_BLOCKLIST_PATH', str(creator_history_dir / 'known_scam_sellers.txt'))
    blocklist_path = Path(blocklist_raw)
    if not blocklist_path.is_absolute():
        blocklist_path = project_root / blocklist_path
    creator_watch = CreatorWatch(creator_history_dir, blocklist_path)
    try:
        history = await asyncio.to_thread(creator_watch.load_history)
        log(
            '👤 Creator history loaded: '
            f"wallets={history.get('creator_wallets', 0)}, "
            f"repeats={history.get('repeat_creator_wallets', 0)}, "
            f"files={history.get('files_scanned', 0)}; telemetry/warn-only.",
            Color.CYAN,
        )
    except Exception as exc:
        log(f'⚠️ Creator history unavailable ({type(exc).__name__}); probes remain non-blocking.', Color.YELLOW)

    telemetry_path = Path(os.getenv('TELEMETRY_DB_PATH', str(run_dir / 'telemetry.sqlite')))
    export_path = Path(os.getenv('TELEMETRY_EXPORT_DIR', str(run_dir / 'exports')))
    collector = CandidateCollector(
        str(telemetry_path), str(export_path),
        shadow_notional_rub=0.0,
        observation_offsets_sec=_observation_offsets(),
        creator_watch=creator_watch,
        enable_shadow_quotes=False,
    )
    await collector.start()
    set_collector(collector)
    collector.record_meta('run_id', run_id)
    collector.record_meta('portfolio_scope', ['portfolio_a', 'portfolio_b'])
    collector.record_meta('shadow_quotes_enabled', False)
    collector.record_meta('paper_only', True)
    collector.record_meta('rpc_host', rpc_host)

    pump_fast = False
    programs = select_programs(settings.sources)
    if settings.pump_source == 'pumpportal':
        pump_fast = await probe_pumpportal()
        if pump_fast:
            programs = {pid: name for pid, name in programs.items() if pid != PUMP_PROGRAM_ID}
        else:
            log('⚠️ PumpPortal unavailable; falling back to Helius Pump.fun program logs (read-only).', Color.YELLOW)

    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    trades_queue: asyncio.Queue = asyncio.Queue()
    coins_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    watch_stop = asyncio.Event()
    candidate_tasks: set[asyncio.Task] = set()
    storage_task: asyncio.Task | None = None
    producers: list[asyncio.Task] = []
    candidate_worker_task: asyncio.Task | None = None
    creator_probe: CreatorOnchainProbe | None = None
    simulator: ABPaperSimulator | None = None
    batcher: ReadOnlyRpcBatcher | None = None
    run_error: Optional[str] = None
    telemetry_stats: dict[str, Any] = {}
    external_stop_reason: Optional[str] = None
    actual_start_epoch = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        batcher = ReadOnlyRpcBatcher(
            session, settings.http_rpc_url,
            flush_window_sec=float(os.getenv('RPC_BATCH_WINDOW_SEC', '0.008')),
        )
        batcher.start()
        set_rpc_batcher(batcher)
        try:
            sol_usd = await asyncio.wait_for(get_sol_usd(session), timeout=12.5)
            if sol_usd and sol_usd > 0:
                log(f'📈 SOL/USD conversion ready (${sol_usd:.2f}); USD/RUB={settings.usd_rub_rate:.2f}.', Color.DIM)
            else:
                log('⚠️ SOL/USD unavailable; entries remain fail-closed.', Color.YELLOW)
        except asyncio.TimeoutError:
            log('⚠️ SOL/USD warm-up exceeded 12.5s; entries remain fail-closed until available.', Color.YELLOW)

        creator_probe = CreatorOnchainProbe(
            session, settings.http_rpc_url, timeout_sec=0.35,
            concurrency=2,
            signature_cache_sec=settings.creator_cache_ttl_sec,
            guard_min_signatures=settings.creator_guard_min_sig,
            burst_window_sec=settings.creator_burst_window_sec,
            creator_watch=creator_watch,
        )
        simulator = ABPaperSimulator(
            session, settings, run_id, run_dir, trades_queue,
            creator_watch, creator_probe,
        )
        storage_task = asyncio.create_task(storage_worker(trades_queue, settings.db_path), name='ab-trade-storage')

        try:
            await simulator.start()
            collector.record_meta('run_started_at_utc', _utc_now())
            collector.record_meta('rpc_preflight_host', rpc_host)
            manifest.update({
                'status': 'running',
                'started_at_utc': _utc_now(),
                'pumpportal_fast_source': pump_fast,
                'helius_program_fallback': not pump_fast,
                'sources': 'pumpfun',
            })
            _write_json(run_dir / 'manifest.json', manifest)

            requested_duration = max(0, int(os.getenv('RUN_DURATION_SEC', str(RUN_DURATION_SEC_DEFAULT))))
            deadline_raw = os.getenv('OVERNIGHT_RUN_DEADLINE_EPOCH', '').strip()
            if deadline_raw:
                try:
                    deadline_remaining = max(0, int(float(deadline_raw) - time.time()))
                except (TypeError, ValueError, OverflowError):
                    deadline_remaining = 0
                duration = min(requested_duration, deadline_remaining)
            else:
                duration = requested_duration
            if duration < 1:
                raise RuntimeError('session_deadline_elapsed_before_run')

            async def candidate_worker() -> None:
                while True:
                    coin = await coins_queue.get()
                    try:
                        try:
                            collector.observe_candidate(coin, session, settings)
                        except Exception as exc:
                            log(f'⚠️ Telemetry candidate enqueue failed ({type(exc).__name__}).', Color.YELLOW)
                        task = asyncio.create_task(simulator.handle_candidate(coin), name=f'ab-candidate-{coin.mint[:8]}')
                        candidate_tasks.add(task)
                        task.add_done_callback(candidate_tasks.discard)
                    finally:
                        coins_queue.task_done()

            candidate_worker_task = asyncio.create_task(candidate_worker(), name='ab-candidate-worker')
            # Helius program-log fallback uses a shared event queue. In the
            # normal PumpPortal path, no other source or strategy is started.
            if not pump_fast:
                events_queue: asyncio.Queue = asyncio.Queue()
                # Replace the placeholder task, if any, with the actual paired workers.
                producers = []
                producers.append(asyncio.create_task(
                    detector_worker(events_queue, coins_queue, session,
                                    settings.http_rpc_url, watch_stop),
                    name='ab-helius-detector',
                ))
                producers.append(asyncio.create_task(
                    ws_listener(events_queue, settings, programs, watch_stop),
                    name='ab-helius-ws-listener',
                ))
            if pump_fast:
                producers.append(asyncio.create_task(
                    pumpportal_listener(coins_queue, watch_stop), name='ab-pumpportal-listener'
                ))

            if deadline_raw:
                duration = min(duration, max(0, int(float(deadline_raw) - time.time())))
            if duration < 1:
                raise RuntimeError('session_deadline_elapsed_before_listener_start')
            actual_start_epoch = time.time()
            simulator.started_at_epoch = actual_start_epoch
            manifest['started_at_utc'] = _utc_now()
            _write_json(run_dir / 'manifest.json', manifest)
            log(
                f'▶️ RUN_ID={run_id} started; planned duration={duration}s; '
                'only portfolio_a + portfolio_b; no signed/submitted transactions.',
                Color.GREEN,
            )
            duration_task = asyncio.create_task(asyncio.sleep(duration), name='ab-run-duration')
            early_stop_task = asyncio.create_task(simulator.early_stop_event.wait(), name='ab-early-stop-watch')
            signal_task = asyncio.create_task(external_stop.wait(), name='ab-signal-watch')
            done, pending = await asyncio.wait(
                {duration_task, early_stop_task, signal_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if early_stop_task in done and simulator.early_stop_reason:
                log(f'⏹ Early stop: {simulator.early_stop_reason}; finalizing this RUN_ID.', Color.YELLOW)
            elif signal_task in done and external_stop.is_set():
                external_stop_reason = 'SIGTERM'
                log('⏹ Session supervisor requested stop; finalizing without synthetic exits.', Color.YELLOW)
        except asyncio.CancelledError:
            run_error = 'cancelled'
            raise
        except Exception as exc:
            run_error = type(exc).__name__
            log(f'❌ A/B run stopped by {run_error}; no transaction was sent.', Color.RED)
        finally:
            watch_stop.set()
            for task in producers:
                task.cancel()
            await asyncio.gather(*producers, return_exceptions=True)
            if candidate_worker_task is not None:
                try:
                    await asyncio.wait_for(coins_queue.join(), timeout=3.0)
                except asyncio.TimeoutError:
                    pass
                candidate_worker_task.cancel()
                await asyncio.gather(candidate_worker_task, return_exceptions=True)
            if candidate_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*list(candidate_tasks), return_exceptions=True), timeout=12.0)
                except asyncio.TimeoutError:
                    for task in list(candidate_tasks):
                        task.cancel()
                    await asyncio.gather(*list(candidate_tasks), return_exceptions=True)
            summary: dict[str, Any] = {}
            if simulator is not None:
                try:
                    summary = await simulator.close()
                except Exception as exc:
                    log(f'⚠️ A/B state save failed ({type(exc).__name__}); no synthetic exit counted.', Color.YELLOW)
            if creator_probe is not None:
                try:
                    await creator_probe.close()
                except Exception as exc:
                    log(f'⚠️ Creator probe shutdown ({type(exc).__name__}); probes are non-blocking.', Color.YELLOW)
            try:
                await asyncio.wait_for(trades_queue.join(), timeout=5.0)
            except asyncio.TimeoutError:
                log('⚠️ Trade DB queue did not fully drain before shutdown.', Color.YELLOW)
            if storage_task is not None:
                storage_task.cancel()
                await asyncio.gather(storage_task, return_exceptions=True)
            collector.record_meta('rpc_metrics_final', summary.get('rpc_metrics', {}))
            collector.record_meta('run_error_type', run_error or 'none')
            collector.record_meta('run_ended_at_utc', _utc_now())
            await collector.close()
            telemetry_stats = collector.stats()
            set_collector(None)
            set_rpc_batcher(None)
            if batcher is not None:
                await batcher.close()
            ended = time.time()
            manifest.update({
                'status': (
                    'stopped_with_error' if run_error else
                    'completed_early' if simulator is not None and simulator.early_stop_reason else
                    'stopped_by_signal' if external_stop_reason else 'completed'
                ),
                'ended_at_utc': _utc_now(),
                'actual_duration_sec': round(ended - actual_start_epoch, 3),
                'early_stop_reason': simulator.early_stop_reason if simulator is not None else None,
                'external_stop_reason': external_stop_reason,
                'run_error_type': run_error,
                'rpc_metrics': summary.get('rpc_metrics', {}),
                'portfolio_summary_file': str(run_dir / 'ab_summary.json'),
                'signed_transactions': 0,
                'submitted_transactions': 0,
                # Lost telemetry rows were previously visible only as one yellow
                # log line; now they reach the manifest and SUMMARY.md.
                'telemetry': telemetry_stats,
            })
            # Funnel + gate split, computed from the finished telemetry DB
            # (read-only, no RPC). A blind spot in the probe must stay visible
            # instead of being reported as a risk verdict.
            try:
                funnel = funnel_from_db(telemetry_path)
            except Exception as exc:
                funnel = {'available': False, 'reason': type(exc).__name__}
            manifest['funnel'] = funnel
            manifest['creator_gate'] = summary.get('creator_gate', {})
            _write_json(run_dir / 'manifest.json', manifest)
            try:
                (run_dir / 'SUMMARY.md').write_text(
                    _summary_markdown(manifest, funnel, summary), encoding='utf-8',
                )
                log(f'📄 SUMMARY.md записан: {run_dir / "SUMMARY.md"}', Color.CYAN)
            except Exception as exc:
                log(f'⚠️ SUMMARY.md не записан ({type(exc).__name__})', Color.YELLOW)
            log(f'⏹ A/B run finalized: {run_id}; open and pending positions remain separately recorded.', Color.CYAN)

    for installed_signal in installed_signals:
        loop.remove_signal_handler(installed_signal)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\nStopped. Paper-only; no transactions submitted.')
        sys.exit(0)
