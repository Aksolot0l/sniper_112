#!/usr/bin/env python3
"""Read-only live dashboard for one isolated A/B paper run."""
from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parent
RUNS_ROOT = (PROJECT_ROOT / 'runs').resolve()
ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')
SECRET_RE = re.compile(r'(?i)(api[_-]?key|authorization|bearer\s+|secret\s*=)')

HTML = (PROJECT_ROOT / 'paper_ab_dashboard.html').read_text(encoding='utf-8')


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def safe_log_tail(path: Path, limit: int = 35) -> list[str]:
    try:
        lines = path.read_text(encoding='utf-8', errors='replace').splitlines()[-limit:]
    except OSError:
        return []
    out = []
    for line in lines:
        line = ANSI_RE.sub('', line)
        if SECRET_RE.search(line):
            continue
        out.append(line[:700])
    return out


def telemetry_counts(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    result: dict[str, int] = {}
    try:
        uri = path.as_uri() + '?mode=ro'
        with sqlite3.connect(uri, uri=True, timeout=0.3) as db:
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table in ('candidates', 'decisions', 'observations', 'creator_probes', 'paper_fills'):
                if table in names:
                    result[table] = int(db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
    except sqlite3.Error:
        return result
    return result


def telemetry_funnel(path: Path) -> dict:
    """Decision funnel per A/B arm; read-only, tolerant to a live WAL db."""
    empty = {'stages': {}, 'entry_reasons': {'portfolio_a': {}, 'portfolio_b': {}},
             'guard_reasons': {}, 'available': False}
    if not path.exists():
        return empty
    out = {'stages': {}, 'entry_reasons': {'portfolio_a': {}, 'portfolio_b': {}},
           'guard_reasons': {}, 'available': True}
    try:
        uri = path.as_uri() + '?mode=ro'
        with sqlite3.connect(uri, uri=True, timeout=0.5) as db:
            names = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'decisions' not in names:
                return empty
            for stage, decision, count in db.execute(
                'SELECT stage, decision, count(*) FROM decisions GROUP BY stage, decision'
            ):
                out['stages'].setdefault(str(stage), {})[str(decision)] = int(count)
            for stage, reasons in db.execute(
                "SELECT stage, reasons_json FROM decisions WHERE stage IN "
                "('portfolio_a_signal','portfolio_b_signal','portfolio_b_creator_gate')"
            ):
                bucket = None
                if stage == 'portfolio_b_creator_gate':
                    bucket = out['guard_reasons']
                elif stage == 'portfolio_a_signal':
                    bucket = out['entry_reasons']['portfolio_a']
                elif stage == 'portfolio_b_signal':
                    bucket = out['entry_reasons']['portfolio_b']
                try:
                    parsed = json.loads(reasons or '[]')
                except (TypeError, ValueError):
                    parsed = []
                if not parsed:
                    continue
                key = str(parsed[0])[:80]
                bucket[key] = bucket.get(key, 0) + 1
    except sqlite3.Error as exc:
        out['error'] = type(exc).__name__
    return out


def make_snapshot(run_dir: Path) -> dict:
    now = time.time()
    manifest = read_json(run_dir / 'manifest.json')
    portfolios = []
    manifest_profiles = manifest.get('strategies', {})
    if not isinstance(manifest_profiles, dict):
        manifest_profiles = {}
    for filename, name in (('wallet_portfolio_a.json', 'portfolio_a'), ('wallet_portfolio_b.json', 'portfolio_b')):
        wallet_path = run_dir / filename
        data = read_json(wallet_path)
        has_state = wallet_path.is_file() and bool(data)
        raw_open = data.get('open_positions', []) if has_state else []
        raw_pending = data.get('pending_positions', []) if has_state else []
        raw_closed = data.get('closed_trades', []) if has_state else []
        open_positions = raw_open if isinstance(raw_open, list) else []
        pending_positions = raw_pending if isinstance(raw_pending, list) else []
        closed_trades = raw_closed if isinstance(raw_closed, list) else []
        strategy = data.get('strategy') if has_state else None
        strategy_source = 'wallet_state'
        if not isinstance(strategy, dict) or not strategy:
            strategy = manifest_profiles.get(name, {})
            strategy_source = 'manifest' if isinstance(strategy, dict) and strategy else 'unavailable'
        if not isinstance(strategy, dict):
            strategy = {}
        max_slots = data.get('max_slots') if has_state else None
        if max_slots is None:
            max_slots = strategy.get('max_slots')
        position_size = data.get('position_size_rub') if has_state else None
        if position_size is None:
            position_size = strategy.get('position_size_rub')
        initial_balance = data.get('initial_balance_rub') if has_state else None
        if initial_balance is None:
            initial_balance = strategy.get('initial_balance_rub')
        closed_count = data.get('closed_trade_count') if has_state else None
        if closed_count is None and has_state:
            closed_count = len(closed_trades)
        portfolios.append({
            'name': name,
            'has_state': has_state,
            'state_run_id': data.get('run_id') if has_state else None,
            'updated_at_utc': data.get('updated_at_utc') if has_state else None,
            'initial_balance_rub': initial_balance,
            'cash_balance_rub': data.get('cash_balance_rub') if has_state else None,
            'net_cash_after_open_entry_fee_liability_rub': data.get('net_cash_after_open_entry_fee_liability_rub') if has_state else None,
            'realized_pnl_rub': data.get('realized_pnl_rub') if has_state else None,
            'snapshot_pnl_rub': data.get('snapshot_pnl_rub') if has_state else None,
            'gross_curve_pnl_rub': data.get('gross_curve_pnl_rub') if has_state else None,
            'realized_network_fees_rub': data.get('realized_network_fees_rub') if has_state else None,
            'fees_paid_rub': data.get('fees_paid_rub') if has_state else None,
            'onchain_fees_paid_rub': data.get('onchain_fees_paid_rub') if has_state else None,
            'open_entry_fee_liability_rub': data.get('open_entry_fee_liability_rub') if has_state else None,
            'conservative_100bps_pnl_rub': data.get('conservative_100bps_pnl_rub') if has_state else None,
            'conservative_100bps_partial_sum_rub': data.get('conservative_100bps_pnl_partial_sum_rub') if has_state else None,
            'conservative_100bps_complete': data.get('conservative_100bps_complete') if has_state else None,
            'conservative_100bps_incomplete_trades': data.get('conservative_100bps_incomplete_trades') if has_state else None,
            'entry_count': data.get('entry_count') if has_state else None,
            'closed_trade_count': closed_count,
            'poll_errors': data.get('poll_errors') if has_state else None,
            'max_slots': max_slots,
            'position_size_rub': position_size,
            'open_count': len(open_positions) if has_state else None,
            'pending_count': len(pending_positions) if has_state else None,
            'open_positions': open_positions[:10],
            'pending_positions': pending_positions[:10],
            'closed_trades': closed_trades[-50:],
            'closed_trades_total': len(closed_trades),
            'closed_trades_truncated': len(closed_trades) > 50,
            'strategy': strategy,
            'strategy_source': strategy_source,
        })
    heartbeat_path = run_dir / 'supervisor_heartbeat.json'
    heartbeat = read_json(heartbeat_path)
    try:
        heartbeat_epoch = float(heartbeat.get('timestamp_epoch'))
        if not math.isfinite(heartbeat_epoch):
            heartbeat_epoch = None
    except (TypeError, ValueError, OverflowError):
        heartbeat_epoch = None
    heartbeat_age = max(0.0, now - heartbeat_epoch) if heartbeat_epoch is not None else None
    expected_run_id = manifest.get('run_id', run_dir.name)
    heartbeat_alive = heartbeat.get('observed_child_alive') is True
    heartbeat_run_matches = heartbeat.get('run_id') == expected_run_id

    tracked = [
        run_dir / 'run.log', run_dir / 'manifest.json', heartbeat_path,
        run_dir / 'wallet_portfolio_a.json', run_dir / 'wallet_portfolio_b.json',
        run_dir / 'telemetry.sqlite', run_dir / 'telemetry.sqlite-wal',
        run_dir / 'sniper.db', run_dir / 'sniper.db-wal',
    ]
    mtimes = [p.stat().st_mtime for p in tracked if p.exists()]
    latest = max(mtimes) if mtimes else None
    age = max(0.0, now - latest) if latest is not None else None
    manifest_status = manifest.get('status', 'unknown')
    terminal_statuses = {'completed', 'completed_early', 'stopped_by_signal', 'stopped_with_error'}
    if manifest_status in terminal_statuses:
        freshness, label = 'complete', 'Запуск завершён'
        notice = f'Этот RUN_ID завершён (manifest={manifest_status}); исторические файлы не выдаются за активный запуск.'
    elif (
        heartbeat_path.is_file() and heartbeat_age is not None and heartbeat_age <= 20.0
        and heartbeat_alive and heartbeat_run_matches and manifest_status == 'running'
    ):
        freshness, label = 'fresh', 'Супервизор видит живой процесс'
        notice = 'Heartbeat обновлён супервизором после проверки живого дочернего процесса; PAPER_ONLY, никаких реальных ордеров.'
    elif heartbeat_path.is_file():
        freshness, label = 'stale', 'Живой процесс не подтверждён'
        notice = (
            f'Последний supervisor heartbeat {heartbeat_age:.0f} сек назад; '
            f'alive={heartbeat_alive}, run_id_match={heartbeat_run_matches}, manifest={manifest_status}. '
            'Manifest сам по себе не считается доказательством liveness.'
            if heartbeat_age is not None else
            f'Heartbeat повреждён или отсутствует; manifest={manifest_status}. Процесс не считается активным.'
        )
    elif age is None:
        freshness, label = 'unknown', 'Нет данных'
        notice = 'Файлы запуска пока не обновлялись.'
    elif age <= 90.0:
        freshness, label = 'unverified', 'Файлы свежие; процесс не подтверждён'
        notice = f'Последнее изменение файлов {age:.0f} сек назад, но нет supervisor heartbeat; процесс не показывается как live.'
    else:
        freshness, label = 'stale', 'Нет свежих данных'
        notice = f'Файлы не менялись {age/60:.1f} мин. Manifest: {manifest_status}; это не подтверждает, что процесс всё ещё работает.'
    latest_iso = datetime.fromtimestamp(latest, timezone.utc).isoformat(timespec='seconds') if latest is not None else None
    return {
        'run_id': expected_run_id,
        'manifest_status': manifest_status,
        'supervisor_heartbeat': {
            'timestamp_epoch': heartbeat_epoch,
            'age_sec': round(heartbeat_age, 3) if heartbeat_age is not None else None,
            'observed_child_alive': heartbeat_alive,
            'run_id_matches': heartbeat_run_matches,
            'child_pid': heartbeat.get('child_pid'),
        },
        'freshness': freshness,
        'freshness_label': label,
        'notice': notice,
        'last_activity_epoch': latest,
        'last_activity_utc': latest_iso,
        'last_activity_age_sec': age,
        'last_activity_label': f'{age:.0f} сек назад ({latest_iso})' if age is not None else '—',
        'portfolios': portfolios,
        'telemetry_counts': telemetry_counts(run_dir / 'telemetry.sqlite'),
        'funnel': telemetry_funnel(run_dir / 'telemetry.sqlite'),
        'events': safe_log_tail(run_dir / 'run.log', limit=150),
        'paper_only': True,
    }


class Handler(BaseHTTPRequestHandler):
    run_dir: Path

    def _send(self, content: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store, max-age=0')
        self.send_header('X-Content-Type-Options', 'nosniff')
        # The page also works when opened from disk (file://) on a local PC.
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:
        route = urlsplit(self.path).path
        if route == '/':
            self._send(HTML.encode('utf-8'), 'text/html; charset=utf-8')
        elif route == '/api/status':
            body = json.dumps(make_snapshot(self.run_dir), ensure_ascii=False, allow_nan=False).encode('utf-8')
            self._send(body, 'application/json; charset=utf-8')
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        self.send_error(405, 'Read-only monitor')

    def log_message(self, _format: str, *_args) -> None:
        return


def latest_run_dir() -> Path | None:
    if not RUNS_ROOT.is_dir():
        return None
    candidates = [
        path for path in RUNS_ROOT.iterdir()
        if path.is_dir() and (path / 'manifest.json').is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path / 'manifest.json').stat().st_mtime)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-dir', default=None,
                        help='run folder; omit to monitor the newest run')
    parser.add_argument('--latest', action='store_true',
                        help='monitor the newest run under runs/ (default when --run-dir is omitted)')
    parser.add_argument('--port', type=int, default=8767)
    # Loopback by default: the page is served to this machine only.
    # Use --host 0.0.0.0 explicitly if a remote browser must reach it.
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    if args.run_dir:
        run_dir = Path(args.run_dir).resolve()
    else:
        run_dir = latest_run_dir()
        if run_dir is None:
            raise SystemExit('no run folders found under runs/; start a run first')
        print(f'Monitoring newest run: {run_dir.name}', flush=True)
    try:
        run_dir.relative_to(RUNS_ROOT)
    except ValueError as exc:
        raise SystemExit('run directory must be inside the project runs folder') from exc
    if not run_dir.is_dir():
        raise SystemExit('run directory does not exist')
    Handler.run_dir = run_dir
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    print(f'Read-only A/B monitor listening on {args.host}:{args.port}; run_id={read_json(run_dir / "manifest.json").get("run_id", run_dir.name)}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
