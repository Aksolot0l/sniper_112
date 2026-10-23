#!/usr/bin/env python3
"""Cross-platform local runner for the PAPER A/B session.

Why this exists: the previous runs died because the bot was started with
``nohup ... &`` inside a cloud sandbox and the sandbox froze the detached
process. This runner keeps one bot process in the foreground of your own
machine, writes a heartbeat, serves the read-only dashboard from the same
process, and finalises cleanly on Ctrl+C.

Guarantees:
- exactly ONE bot process (main_ab.py) - the dashboard runs as a thread;
- paper-only: it never signs or sends anything, and refuses PAPER_ONLY != 1;
- works on macOS/Linux and Windows (no fcntl, no POSIX-only signals).

Examples
    python run_local_session.py --duration-sec 28800   # full 8h run (прогон №3)
    python run_local_session.py --smoke 150            # 2.5 min live check
    python run_local_session.py --duration 28800 --no-dashboard
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
RUNS = PROJECT / 'runs'
HEARTBEAT = 'supervisor_heartbeat.json'
LOCK_FILE = 'local-session.lock'


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def python_executable() -> str:
    return sys.executable


def child_environment(run_id: str, run_dir: Path, duration: int) -> dict[str, str]:
    env = os.environ.copy()
    # Never leak unrelated secrets into the child process.
    for name in list(env):
        upper = name.upper()
        if name == 'HELIUS_API_KEY':
            continue
        if any(part in upper for part in ('KEY', 'TOKEN', 'MNEMONIC', 'SEED', 'SECRET', 'PASSWORD')):
            env.pop(name, None)
    env.update({
        'RUN_ID': run_id,
        'RUN_DIR': str(run_dir),
        'RUN_DURATION_SEC': str(int(duration)),
        'PAPER_ONLY': '1',
        'MODE': 'portfolio',
        'SOURCES': 'pumpfun',
        'PORTFOLIO_SCOPE': 'ab_only',
        'PUMP_SOURCE': 'pumpportal',
        'DEPOSIT_RUB': '1500',
        'MAX_POSITIONS': '3',
        'MAX_TRADE_RUB': '500',
        'MAX_HOLD_SEC': '90',
        'USD_RUB_RATE': '90',
        'ENTRY_FEE_SOL': '0.00008',
        'EXIT_FEE_SOL': '0.00005',
        'PROTOCOL_FEE_PCT': '1.25',
        'HONEYPOT_CHECK': 'off',
        'ONLY_SOL_PAIRS': 'on',
        'FILTER_CONCURRENCY': '4',
        'INDEXER_RETRIES': '0',
        'INDEXER_RETRY_DELAY_SEC': '0.12',
        'RPC_BATCH_WINDOW_SEC': '0.008',
        'CREATOR_RISK_POLICY': 'warn',
        'CREATOR_HISTORY_DIR': str(RUNS),
        'CREATOR_BLOCKLIST_PATH': str(RUNS / 'known_scam_sellers.txt'),
        'ENTRY_BIRTH_MIN_SOL': env.get('ENTRY_BIRTH_MIN_SOL', '30.5'),
        'ENTRY_BIRTH_MAX_SOL': env.get('ENTRY_BIRTH_MAX_SOL', '34.5'),
        'ENTRY_DELTA_MIN_SOL': env.get('ENTRY_DELTA_MIN_SOL', '0.10'),
        'ENTRY_DELTA_MAX_SOL': env.get('ENTRY_DELTA_MAX_SOL', '2.5'),
        # Run #7: each experimental arm moves ONE entry boundary of A.
        'ARMS': env.get(
            'ARMS',
            'portfolio_a,portfolio_b1_narrow,portfolio_b2_birth,portfolio_b3_t0',
        ),
        'ENTRY_DELTA_MAX_SOL_B1': env.get('ENTRY_DELTA_MAX_SOL_B1', '1.00'),
        'ENTRY_BIRTH_MAX_SOL_B2': env.get('ENTRY_BIRTH_MAX_SOL_B2', '31.00'),
        'ENTRY_DELTA_MIN_SOL_B3': env.get('ENTRY_DELTA_MIN_SOL_B3', '0.00'),
        'ENTRY_ENRICH': env.get('ENTRY_ENRICH', '1'),
        'CREATOR_GUARD_MIN_SIG': env.get('CREATOR_GUARD_MIN_SIG', '25'),
        'CREATOR_BURST_WINDOW_SEC': env.get('CREATOR_BURST_WINDOW_SEC', '3600'),
        'CREATOR_CACHE_TTL_SEC': env.get('CREATOR_CACHE_TTL_SEC', '300'),
        'TELEMETRY_RUN_ID': run_id,
        'TELEMETRY_OBSERVATION_OFFSETS_SEC': env.get(
            'TELEMETRY_OBSERVATION_OFFSETS_SEC', '0,5,15,60',
        ),
        'WALLET_PATH': str(run_dir / 'wallet_portfolio_a.json'),
        'TRADE_LOG': str(run_dir / 'trades.log'),
        'DB_PATH': str(run_dir / 'sniper.db'),
        'TELEMETRY_DB_PATH': str(run_dir / 'telemetry.sqlite'),
        'TELEMETRY_EXPORT_DIR': str(run_dir / 'exports'),
    })
    # A keyed override would end up in logs if it ever reached the shell.
    env.pop('SOLANA_HTTP_RPC_URL', None)
    env.pop('SOLANA_WSS_RPC_URL', None)
    return env


def previous_session_alive() -> bool:
    """Portable 'is another session running' check via a fresh heartbeat."""
    lock = RUNS / LOCK_FILE
    if not lock.is_file():
        return False
    try:
        payload = json.loads(lock.read_text(encoding='utf-8'))
        stamp = float(payload.get('timestamp_epoch') or 0)
    except (OSError, ValueError, TypeError):
        return False
    return (time.time() - stamp) < 30.0


def write_lock(run_id: str) -> None:
    RUNS.mkdir(parents=True, exist_ok=True)
    (RUNS / LOCK_FILE).write_text(json.dumps({
        'run_id': run_id, 'pid': os.getpid(), 'timestamp_epoch': time.time(),
    }), encoding='utf-8')


def start_dashboard(run_dir: Path, port: int) -> threading.Thread | None:
    try:
        from http.server import ThreadingHTTPServer
        import live_ab_dashboard as dashboard
    except Exception as exc:  # pragma: no cover - defensive
        print(f'⚠️  дашборд не запущен: {exc}', flush=True)
        return None
    handler = type('LocalHandler', (dashboard.Handler,), {'run_dir': run_dir})
    try:
        server = ThreadingHTTPServer(('127.0.0.1', port), handler)
    except OSError as exc:
        print(f'⚠️  порт {port} занят ({exc}); дашборд выключен', flush=True)
        return None
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, name='dashboard', daemon=True)
    thread.start()
    print(f'📊 Дашборд: http://127.0.0.1:{port}/  (страница: paper_ab_dashboard.html)', flush=True)
    return thread


def heartbeat_loop(run_dir: Path, run_id: str, process: subprocess.Popen, stop: threading.Event) -> None:
    path = run_dir / HEARTBEAT
    while not stop.is_set():
        alive = process.poll() is None
        payload = {
            'run_id': run_id, 'observed_child_alive': alive,
            'child_pid': process.pid, 'timestamp_epoch': time.time(),
            'written_at_utc': utc_now(),
        }
        tmp = path.with_suffix('.tmp')
        try:
            tmp.write_text(json.dumps(payload), encoding='utf-8')
            os.replace(tmp, path)
            lock = RUNS / LOCK_FILE
            lock.write_text(json.dumps({
                'run_id': run_id, 'pid': os.getpid(), 'timestamp_epoch': time.time(),
            }), encoding='utf-8')
        except OSError:
            pass
        stop.wait(5.0)


def print_summary(run_dir: Path) -> None:
    summary_path = run_dir / 'ab_summary.json'
    print(flush=True)
    if not summary_path.is_file():
        print('⚠️  ab_summary.json не записан: процесс завершился раньше времени. '
              'Данные сделок остались в wallet_*.json и telemetry.sqlite.', flush=True)
        return
    try:
        summary = json.loads(summary_path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        print('⚠️  ab_summary.json повреждён', flush=True)
        return
    print('=== ИТОГИ СЕССИИ ===', flush=True)
    print(f"  длительность: {summary.get('duration_sec')} c | "
          f"paper_only={summary.get('paper_only')} | "
          f"подписано транзакций: {summary.get('signed_transactions')}", flush=True)
    arm_keys = [
        name for name in (summary.get('portfolio_scope') or [])
        if isinstance(summary.get(name), dict)
    ] or [
        name for name in summary
        if isinstance(summary.get(name), dict) and name.startswith('portfolio_')
    ]
    for name in arm_keys:
        book = summary.get(name)
        if not isinstance(book, dict):
            continue
        arm = name if name == 'portfolio_a' else str(book.get('arm_id') or name)
        if name == 'portfolio_a':
            arm = 'A (эталон, portfolio_a)'
        else:
            arm = (f"{arm} ({name}) | полоса: {book.get('entry_band_label') or '—'}")
        print(
            f"  {arm}: кэш={book.get('cash_balance_rub')} ₽ | "
            f"realized={book.get('realized_pnl_rub')} ₽ | "
            f"входов={book.get('entry_count')} | "
            f"закрыто={book.get('closed_trade_count')} | "
            f"открыто={book.get('open_position_count')}",
            flush=True,
        )
        trades = book.get('closed_trades') or []
        for trade in trades[-5:]:
            print(
                f"      · {str(trade.get('mint'))[:8]}… {trade.get('reason')} "
                f"({trade.get('pnl_pct'):+.1f}%, {trade.get('pnl_rub'):+.2f} ₽)",
                flush=True,
            )
    if summary.get('early_stop_reason'):
        print(f"  early_stop: {summary['early_stop_reason']}", flush=True)
    print(f"  файлы: {run_dir}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    """CLI, kept separate so `--duration-sec` is covered by tests."""
    parser = argparse.ArgumentParser(description='PAPER A/B local session runner')
    parser.add_argument(
        '--duration', '--duration-sec', dest='duration', type=int, default=28800,
        help='длительность сессии в секундах (алиас --duration-sec; по умолчанию '
             '28800 = 8 ч. После 10-минутной проверки ставится 10800 = 3 ч)',
    )
    parser.add_argument('--smoke', type=int, default=0,
                        help='короткий проверочный прогон: --smoke 150')
    parser.add_argument('--port', type=int, default=8767)
    parser.add_argument('--no-dashboard', action='store_true')
    parser.add_argument('--open-browser', action='store_true')
    parser.add_argument('--run-id', default='')
    return parser


def main() -> int:
    args = build_parser().parse_args()

    duration = args.smoke if args.smoke > 0 else args.duration
    if duration < 10:
        print('ERROR: длительность должна быть >= 10 секунд', flush=True)
        return 2
    if not os.environ.get('HELIUS_API_KEY', '').strip():
        print('ERROR: HELIUS_API_KEY не задан в окружении. Запусти через START_MAC_LINUX.sh / '
              'START_WINDOWS.bat (они попросят ключ) и повтори.', flush=True)
        return 10
    if os.getenv('PAPER_ONLY', '1').strip() != '1':
        print('ERROR: PAPER_ONLY must stay enabled.', flush=True)
        return 11

    RUNS.mkdir(parents=True, exist_ok=True)
    if previous_session_alive():
        print('ERROR: другая локальная сессия уже идёт (свежий lock). '
              'Второй процесс запрещён — лимит Helius RPS.', flush=True)
        return 2

    run_id = args.run_id or (
        datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-local'
    )
    run_dir = RUNS / f'rocket2-ab-paper-{run_id}'
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / 'exports').mkdir(exist_ok=True)
    write_lock(run_id)

    print('=' * 78, flush=True)
    print(f'PAPER A/B · run_id={run_id} · длительность {duration} c '
          f'({duration / 60:.1f} мин)', flush=True)
    print(f'проект: {PROJECT}', flush=True)
    print(f'run_dir: {run_dir}', flush=True)
    print('режим: PAPER_ONLY · read-only RPC · 3 слота × 500 ₽ на портфель · '
          'ни одной подписи транзакции', flush=True)
    print('=' * 78, flush=True)

    if not args.no_dashboard:
        start_dashboard(run_dir, args.port)
        if args.open_browser:
            threading.Timer(1.5, lambda: webbrowser.open(f'http://127.0.0.1:{args.port}/')).start()

    env = child_environment(run_id, run_dir, duration)
    creationflags = 0
    if os.name == 'nt':
        creationflags = getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0)
    process = subprocess.Popen(
        [python_executable(), '-u', 'main_ab.py'],
        cwd=str(PROJECT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        bufsize=1, text=True, errors='replace',
        creationflags=creationflags,
    )
    log_path = run_dir / 'run.log'
    stop_heartbeat = threading.Event()
    threading.Thread(
        target=heartbeat_loop, args=(run_dir, run_id, process, stop_heartbeat),
        name='heartbeat', daemon=True,
    ).start()

    interrupted = False

    def _stop(signum, _frame):  # noqa: ANN001
        nonlocal interrupted
        interrupted = True
        print(f'\n⏹  получен сигнал {signum}; просим бота завершиться аккуратно…', flush=True)
        try:
            if os.name == 'nt':
                process.send_signal(getattr(signal, 'CTRL_BREAK_EVENT', signal.SIGTERM))
            else:
                process.send_signal(signal.SIGTERM)
        except Exception:
            pass

    previous = {}
    for sig_name in ('SIGINT', 'SIGTERM'):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                previous[sig] = signal.getsignal(sig)
                signal.signal(sig, _stop)
            except (ValueError, OSError):
                pass

    started = time.time()
    try:
        assert process.stdout is not None
        with log_path.open('a', encoding='utf-8') as log_file:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_file.write(line)
        process.wait()
    except KeyboardInterrupt:
        _stop(signal.SIGINT, None)
        process.wait()
    finally:
        stop_heartbeat.set()
        for sig, handler in previous.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass

    try:
        (RUNS / LOCK_FILE).unlink()
    except OSError:
        pass

    elapsed = time.time() - started
    print(f'\nсессия завершена за {elapsed:.0f} c (код {process.returncode})', flush=True)
    if interrupted:
        print('остановлено вручную — синтетических выходов не добавлялось', flush=True)
    print_summary(run_dir)
    print(f'лог: {log_path}', flush=True)
    return int(process.returncode or 0)


if __name__ == '__main__':
    sys.exit(main())
