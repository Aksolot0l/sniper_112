#!/usr/bin/env python3
"""Create a privacy-conscious, point-in-time A/B paper-run checkpoint."""
from __future__ import annotations

import json
import shutil
import sqlite3
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path('/home/user/solana_sniper')
RUNS = ROOT / 'runs'
CURRENT_ID = '20261021T174318Z-28258057'
PARTIAL_ID = '20261021T173118Z-085911ef'
OUT_ROOT = ROOT / 'checkpoints'


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def db_counts(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    counts: dict[str, int] = {}
    try:
        with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=8.0) as db:
            db.execute('PRAGMA busy_timeout=8000')
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table in ('candidates', 'detections', 'decisions', 'quotes', 'observations', 'creator_probes', 'paper_fills'):
                if table in tables:
                    counts[table] = int(db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
    except sqlite3.Error as exc:
        counts['_snapshot_error'] = type(exc).__name__
    return counts


def backup_db(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    source_uri = src.as_uri() + '?mode=ro'
    with sqlite3.connect(source_uri, uri=True, timeout=10.0) as source:
        source.execute('PRAGMA busy_timeout=10000')
        with sqlite3.connect(dst, timeout=10.0) as target:
            source.backup(target, pages=256, sleep=0.1)


def copy_run_snapshot(run_id: str, target_root: Path) -> dict:
    src = RUNS / f'rocket2-ab-paper-{run_id}'
    dest = target_root / run_id
    dest.mkdir(parents=True, exist_ok=True)
    for name in (
        'manifest.json', 'run.log', 'preflight_tests.log', 'trades.log',
        'ab_summary.json', 'SUMMARY.md', 'operator_stop_record.json',
        'summary.md', 'README.md',
    ):
        path = src / name
        if path.is_file():
            shutil.copy2(path, dest / name)
    # Run #7: four arms, so the wallet files are discovered, not enumerated.
    for path in sorted(src.glob('wallet_*.json')):
        shutil.copy2(path, dest / path.name)
    for db_name in ('sniper.db', 'telemetry.sqlite'):
        backup_db(src / db_name, dest / db_name)
    exports = src / 'exports'
    if exports.is_dir():
        out_exports = dest / 'exports'
        out_exports.mkdir(exist_ok=True)
        for path in exports.rglob('*'):
            if not path.is_file() or path.name.lower() in {'.ds_store'}:
                continue
            if path.suffix.lower() not in {'.csv', '.jsonl', '.md', '.json'}:
                continue
            relative = path.relative_to(exports)
            out = out_exports / relative
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, out)
    return {
        'run_id': run_id,
        'source_path': str(src),
        'manifest': read_json(src / 'manifest.json'),
        'wallets': {
            path.stem.removeprefix('wallet_'): read_json(path)
            for path in sorted(src.glob('wallet_*.json'))
        },
        'telemetry_counts': db_counts(src / 'telemetry.sqlite'),
        'sniper_db_exists': (src / 'sniper.db').exists(),
        'summary_exists': (src / 'ab_summary.json').exists(),
        'last_activity_utc': datetime.fromtimestamp(
            max((p.stat().st_mtime for p in src.iterdir() if p.is_file()), default=0), timezone.utc
        ).isoformat(timespec='seconds') if src.exists() else None,
    }


def portfolio_record(name: str, data: dict) -> dict:
    opened = data.get('open_positions', [])
    pending = data.get('pending_positions', [])
    if not isinstance(opened, list):
        opened = []
    if not isinstance(pending, list):
        pending = []
    return {
        'portfolio': name,
        'initial_balance_rub': data.get('initial_balance_rub', 1500),
        'cash_balance_rub': data.get('cash_balance_rub', 1500),
        'realized_pnl_rub': data.get('realized_pnl_rub', 0),
        'fees_paid_rub': data.get('fees_paid_rub', 0),
        'onchain_fees_paid_rub': data.get('onchain_fees_paid_rub', 0),
        'entry_count': data.get('entry_count', 0),
        'closed_trade_count': data.get('closed_trade_count', 0),
        'open_positions': opened,
        'pending_positions': pending,
        'poll_errors': data.get('poll_errors', 0),
    }


def rub(value: object) -> str:
    try:
        return f'{float(value):,.2f}'.replace(',', ' ') + ' ₽'
    except (TypeError, ValueError):
        return 'n/a'


def main() -> None:
    captured = datetime.now(timezone.utc)
    stamp = captured.strftime('%Y%m%dT%H%M%SZ')
    current_dir = RUNS / f'rocket2-ab-paper-{CURRENT_ID}'
    manifest = read_json(current_dir / 'manifest.json')
    stop_record = read_json(current_dir / 'operator_stop_record.json')
    effective_status = stop_record.get('status') or manifest.get('status', 'unknown')
    # Run #7: whatever arms the run actually had, in a stable order.
    wallet_files = sorted(current_dir.glob('wallet_*.json'))
    wallets = [
        portfolio_record(path.stem.removeprefix('wallet_'), read_json(path))
        for path in wallet_files
    ]
    if not wallets:
        raise SystemExit(f'no wallet_*.json in {current_dir}')
    ranked = sorted(wallets, key=lambda item: float(item.get('realized_pnl_rub') or 0), reverse=True)
    interim_leader = ranked[0]
    start_text = manifest.get('started_at_utc')
    elapsed_sec = None
    planned_end = None
    if start_text:
        start = datetime.fromisoformat(start_text.replace('Z', '+00:00'))
        elapsed_sec = int(stop_record.get('actual_elapsed_sec_at_stop', max(0, int((captured - start).total_seconds()))))
        planned_end = start + timedelta(seconds=int(manifest.get('planned_duration_sec', 3600)))
    local = captured.astimezone(ZoneInfo('Asia/Yekaterinburg'))
    base = OUT_ROOT / f'rocket2-ab-paper-{CURRENT_ID}-checkpoint-{stamp}'
    bundle_dir = base / 'bundle'
    bundle_dir.mkdir(parents=True, exist_ok=True)

    current_snapshot = copy_run_snapshot(CURRENT_ID, bundle_dir / 'runs')
    partial_snapshot = copy_run_snapshot(PARTIAL_ID, bundle_dir / 'runs')

    model_config = {
        'config_id': f'rocket2-ab-current-best-so-far-provisional-{stamp}',
        'config_kind': 'rule-based paper strategy configuration; not model weights',
        'label': 'current best-so-far candidate, provisional and user-requested',
        'captured_at_utc': captured.isoformat(timespec='seconds'),
        'captured_at_asia_yekaterinburg': local.isoformat(timespec='seconds'),
        'run_id': CURRENT_ID,
        'run_status': effective_status,
        'stopped_at_utc': stop_record.get('stopped_at_utc'),
        'planned_60_min_completed': stop_record.get('planned_60_min_completed', manifest.get('status') == 'completed'),
        'evaluation_basis': 'highest realized PnL at the stop snapshot only; open/unrealized PnL is not used to rank',
        'interim_leader': {
            'portfolio': interim_leader['portfolio'],
            'realized_pnl_rub': interim_leader['realized_pnl_rub'],
            'cash_balance_rub': interim_leader['cash_balance_rub'],
            'entries': interim_leader['entry_count'],
            'closed_trades': interim_leader['closed_trade_count'],
            'open_positions': len(interim_leader['open_positions']),
            'pending_positions': len(interim_leader['pending_positions']),
        },
        'comparison_snapshot': wallets,
        'arms': (manifest.get('ab_test') or {}).get('arms')
        or {wallet['portfolio']: {} for wallet in wallets},
        'strategies': manifest.get('strategies', {}),
        'shared_settings': {
            'paper_only': True,
            'sources': 'pumpfun',
            'balance_rub_per_portfolio': 1500,
            'position_size_rub': 500,
            'max_slots_per_portfolio': 3,
            'http_shadow_buy_sell': False,
            'quote_slippage_bps': 100,
            'position_poll_target_sec': 1.0,
            'dashboard_refresh_sec': 1.0,
            'official_fee_config_required': True,
            'fail_closed_on_unknown_fee_or_curve_state': True,
            'jupiter_only_when_curve_complete': True,
        },
        'validation': {
            'run_is_still_in_progress': effective_status == 'running',
            'stopped_early_by_user': bool(stop_record),
            'not_a_final_winner': True,
            'not_a_profitability_claim_or_future_return_guarantee': True,
            'current_snapshot_note': 'This is a stop-time comparison only; the run stopped before 60 minutes and both profiles remain unvalidated.',
        },
    }
    (bundle_dir / 'model_config_best_so_far_provisional.json').write_text(
        json.dumps(model_config, ensure_ascii=False, indent=2), encoding='utf-8'
    )

    def arms_table() -> str:
        arms = (manifest.get('ab_test') or {}).get('arms') or {}
        lines = ['| арм | полоса входа | что изменено против A |', '|---|---|---|']
        for name, meta in arms.items():
            lines.append(
                f"| {meta.get('arm_id') or name} (`{name}`) | "
                f"{meta.get('entry_band_label') or meta.get('band') or '—'} | "
                f"{', '.join(meta.get('single_boundary_vs_a') or []) or meta.get('changed_vs_a') or 'эталон'} |"
            )
        return '\n'.join(lines) if len(lines) > 2 else '- arms: see companion JSON'

    def portfolio_lines() -> str:
        out = ['| Портфель | Баланс | Реализованный PnL | Входы / закрытия | Открыто / pending | Сетевые расходы |',
               '|---|---:|---:|---:|---:|---:|']
        for p in wallets:
            out.append(
                f"| {p['portfolio']} | {rub(p['cash_balance_rub'])} | {rub(p['realized_pnl_rub'])} | "
                f"{p['entry_count']} / {p['closed_trade_count']} | {len(p['open_positions'])} / {len(p['pending_positions'])} | {rub(p['fees_paid_rub'])} |"
            )
        return '\n'.join(out)

    report = f'''# Rocket 2 A/B — checkpoint

Captured: **{captured.isoformat(timespec='seconds')} UTC** ({local.isoformat(timespec='seconds')} Yekaterinburg).

## Current run
- RUN_ID: `{CURRENT_ID}`
- State in the original manifest: **{manifest.get('status', 'unknown')}** (the runner was interrupted before it could finalize the manifest).
- Effective state: **{effective_status}**
- Started: `{start_text or 'unknown'}`
- Stopped on user request: `{stop_record.get('stopped_at_utc', 'not recorded')}`
- Planned duration: `{manifest.get('planned_duration_sec', 'unknown')} sec`; elapsed: `{elapsed_sec if elapsed_sec is not None else 'unknown'} sec` (about 38 min 36 sec, not the planned 60 min)
- Planned end: `{planned_end.isoformat(timespec='seconds') if planned_end else 'unknown'}` UTC
- Data files and state: `runs/rocket2-ab-paper-{CURRENT_ID}/`
- The normal final summary and final RPC metrics are **missing** because the run was stopped before its finalizer completed. `operator_stop_record.json` records the stop and last persisted wallet state.

## Portfolio snapshot

{portfolio_lines()}

**Stop-time leader by realized PnL only:** `{interim_leader['portfolio']}` at {rub(interim_leader['realized_pnl_rub'])}. This is only a point-in-time ranking; it does not include unrealized PnL and is not a final statistical winner. A checkpoint snapshot cannot decide an arm: the paired test needs the whole run, and the minimum detectable effect at ~165 pairs is about 15 ₽ per trade.

## Saved configuration

The companion JSON saves both A/B rule sets and marks the current higher-realized-PnL profile as a **provisional best-so-far candidate**, per the request. It is a rule-based config, not neural-network weights. Neither profile is claimed profitable or validated for future performance.

{arms_table()}
- Exit ladder (identical in every arm): `{(manifest.get('strategy_meta') or {}).get('exit_rule') or 'see manifest'}`.
- Position marks are polled once per slot-second; no-progress is evaluated once at 5 s. Dashboard refresh is separate from the trading loop.
- Helius read-only; `getMultipleAccounts` batches up to 100 accounts; one retry after 120 ms for null values. Aggregate end-of-run latency/error metrics were not finalized after the early stop; no SLA is claimed.
- HTTP shadow buy/sell disabled. Local Pump.fun fills require on-chain Global/FeeConfig and exact integer fee math; unsupported states fail closed. Jupiter fallback only when `complete=true`.
- Mayhem excluded; standard supply, SOL/WSOL quote, revoked mint/freeze authorities checked. Creator probes are telemetry/warn-only.
- Observation schedule: `{(manifest.get('telemetry') or {}).get('observation_offsets_sec') or '0/5/15/60'}` seconds. No signing or transaction submission.
- Virtual network fees are tracked separately from the 1,500 ₽ principal balances and deducted from realized proceeds; open entry-fee liabilities are recorded.

## Earlier partial run (preserved)
- RUN_ID: `{PARTIAL_ID}`
- It is included separately in the archive and was not overwritten or removed.
- Its manifest had no completed/final summary at checkpoint time; do not combine it with the current run as one continuous 60-minute result.

## Archive contents and secret handling

The ZIP includes point-in-time snapshots of the current run and the earlier partial run, including manifest, logs, A/B state, exports if present, and consistent SQLite backups of the trade and telemetry databases. It also includes selected implementation files, this report, and the companion model configuration. The Helius key, temporary download URL, environment, and any signing material are **not included**.

This is a stop-time snapshot for analysis, not a completed 60-minute run report. No forced or synthetic exits were added. Keep the old run folders; they are preserved in place as well as in this package.
'''
    report_path = base / f'checkpoint-{CURRENT_ID}-{stamp}.md'
    report_path.write_text(report, encoding='utf-8')
    config_path = base / 'model_config_best_so_far_provisional.json'
    shutil.copy2(bundle_dir / config_path.name, config_path)
    (bundle_dir / report_path.name).write_text(report, encoding='utf-8')

    implementation = bundle_dir / 'implementation'
    implementation.mkdir(exist_ok=True)
    for rel in (
        'main_ab.py', 'portfolio_ab.py', 'pump_curve_quote.py', 'price.py',
        'telemetry.py', 'live_ab_dashboard.py', 'run_rocket2_ab_paper.sh',
        'config.py', 'tests/test_portfolio_ab.py', 'tests/test_rpc_batcher.py',
        'tests/test_telemetry.py',
    ):
        src = ROOT / rel
        if src.is_file():
            out = implementation / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, out)

    zip_path = base / f'rocket2-ab-paper-{CURRENT_ID}-checkpoint-{stamp}.zip'
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in sorted(bundle_dir.rglob('*')):
            if path.is_file():
                zf.write(path, path.relative_to(bundle_dir.parent))

    result = {
        'captured_at_utc': captured.isoformat(timespec='seconds'),
        'current_run': CURRENT_ID,
        'current_status': effective_status,
        'interim_leader_by_realized_pnl': interim_leader['portfolio'],
        'realized_pnl_rub': interim_leader['realized_pnl_rub'],
        'report': str(report_path),
        'model_config': str(config_path),
        'archive': str(zip_path),
        'archive_size_bytes': zip_path.stat().st_size,
        'current_telemetry_counts': current_snapshot['telemetry_counts'],
        'partial_run_summary_exists': partial_snapshot['summary_exists'],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
