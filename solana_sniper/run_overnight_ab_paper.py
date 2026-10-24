#!/usr/bin/env python3
"""Timed PAPER_ONLY A/B session coordinator (at most three immutable episodes)."""
from __future__ import annotations

import fcntl
import gzip
import json
import os
import secrets
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlite_snapshots import SQLiteSnapshotWorker

PROJECT = Path(__file__).resolve().parent
RUNS = PROJECT / 'runs'
DEFAULT_SESSION_SECONDS = 9_000


def _session_seconds_from_env(value: str | None) -> int:
    if value is None or not value.strip():
        return DEFAULT_SESSION_SECONDS
    try:
        seconds = int(value.strip())
    except ValueError as exc:
        raise ValueError('OVERNIGHT_SESSION_SECONDS must be an integer') from exc
    if seconds < 1:
        raise ValueError('OVERNIGHT_SESSION_SECONDS must be positive')
    return seconds


SESSION_SECONDS = _session_seconds_from_env(os.environ.get('OVERNIGHT_SESSION_SECONDS'))
MAX_EPISODES = 3
RESTART_MIN_REMAINING_SEC = 300
FINALIZE_BUFFER_SEC = 45


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def atomic_create_json(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f'refusing to overwrite existing session archive: {path.name}')
    temp = path.with_name(path.name + '.tmp-' + secrets.token_hex(4))
    try:
        with temp.open('x', encoding='utf-8') as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def read_manifest(run_dir: Path) -> dict[str, Any]:
    path = run_dir / 'manifest.json'
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _write_supervisor_heartbeat(
    run_dir: Path,
    *,
    session_id: str,
    run_id: str,
    episode: int,
    child_pid: int,
    observed_child_alive: bool,
) -> bool:
    """Atomically publish the coordinator's recent observation of child liveness."""
    if not run_dir.is_dir():
        return False
    path = run_dir / 'supervisor_heartbeat.json'
    temp = path.with_name(path.name + '.tmp-' + secrets.token_hex(4))
    payload = {
        'session_id': session_id,
        'run_id': run_id,
        'episode': episode,
        'supervisor_pid': os.getpid(),
        'child_pid': int(child_pid),
        'observed_child_alive': bool(observed_child_alive),
        'timestamp_epoch': time.time(),
        'timestamp_utc': utc_now(),
    }
    try:
        with temp.open('x', encoding='utf-8') as stream:
            json.dump(payload, stream, ensure_ascii=False, separators=(',', ':'))
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        # Heartbeats are intentionally replaced atomically; immutable archives are not.
        os.replace(temp, path)
        return True
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _verify_60s_checkpoint(run_dir: Path, child: subprocess.Popen[bytes]) -> dict[str, Any]:
    """Record process, manifest, SQLite, WSS-log, and creator-probe evidence."""
    manifest = read_manifest(run_dir)
    process_alive = child.poll() is None
    log_path = run_dir / 'run.log'
    try:
        log_text = log_path.read_text(encoding='utf-8', errors='replace')[-250_000:]
    except OSError:
        log_text = ''
    low_log = log_text.lower()
    wss_connected = (
        'поток созданий pump.fun подключён' in low_log
        or 'connected to pumpportal' in low_log
        or 'pumpportal websocket connected' in low_log
    )
    telemetry_path = run_dir / 'telemetry.sqlite'
    telemetry_readable = False
    creator_probe_count: int | None = None
    telemetry_tables: list[str] = []
    telemetry_error: str | None = None
    if telemetry_path.is_file():
        try:
            with sqlite3.connect(telemetry_path.as_uri() + '?mode=ro', uri=True, timeout=1.0) as db:
                telemetry_tables = [
                    str(row[0]) for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                    )
                ]
                telemetry_readable = True
                if 'creator_probes' in telemetry_tables:
                    creator_probe_count = int(
                        db.execute('SELECT count(*) FROM creator_probes').fetchone()[0]
                    )
        except sqlite3.Error as exc:
            telemetry_error = type(exc).__name__
    manifest_running = manifest.get('status') == 'running'
    paper_invariants = (
        manifest.get('paper_only') is True
        and manifest.get('signed_transactions', 0) == 0
        and manifest.get('submitted_transactions', 0) == 0
    )
    critical_ok = bool(
        process_alive and manifest_running and telemetry_readable
        and paper_invariants and wss_connected
    )
    try:
        log_age_sec = max(0.0, time.time() - log_path.stat().st_mtime)
    except OSError:
        log_age_sec = None
    return {
        'checked_after_sec': 60,
        'process_alive': process_alive,
        'manifest_status': manifest.get('status', 'missing_or_unreadable'),
        'paper_only_invariants_ok': paper_invariants,
        'telemetry_db_readable': telemetry_readable,
        'telemetry_db_error_type': telemetry_error,
        'telemetry_tables': telemetry_tables,
        'creator_probe_count': creator_probe_count,
        'creator_probe_status': (
            'events_seen' if (creator_probe_count or 0) > 0
            else 'no_candidate_probe_event_seen_by_checkpoint'
        ),
        'wss_connected_log_marker': wss_connected,
        'run_log_age_sec': round(log_age_sec, 3) if log_age_sec is not None else None,
        'critical_ok': critical_ok,
    }


def _file_contains_secret(path: Path, secret: bytes) -> bool:
    if not secret:
        return False
    opener = gzip.open if path.name.endswith('.gz') else open
    overlap = b''
    with opener(path, 'rb') as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                return False
            data = overlap + chunk
            if secret in data:
                return True
            overlap = data[-max(0, len(secret) - 1):] if len(secret) > 1 else b''


def _create_session_archive(
    archive_path: Path,
    *,
    summary: dict[str, Any],
    log_path: Path,
    episodes: list[dict[str, Any]],
    snapshots_root: Path,
    secret_value: str,
) -> None:
    """Create a no-clobber archive containing only this session's outputs."""
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if archive_path.exists():
        raise FileExistsError('session archive already exists')
    temp_path = archive_path.with_name(archive_path.name + '.tmp-' + secrets.token_hex(5))
    secret = secret_value.encode('utf-8') if secret_value else b''
    try:
        with zipfile.ZipFile(temp_path, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=5) as archive:
            archive.writestr(
                'session_summary.json',
                json.dumps(summary, ensure_ascii=False, indent=2).encode('utf-8'),
            )
            if log_path.is_file():
                if _file_contains_secret(log_path, secret):
                    raise RuntimeError('secret_material_detected_in_session_log')
                archive.write(log_path, 'session.log')
            for episode in episodes:
                run_dir = Path(episode['run_dir'])
                if not run_dir.is_dir():
                    continue
                for file_path in sorted(p for p in run_dir.rglob('*') if p.is_file()):
                    if file_path.name.lower() in {
                        'helius_key.txt', '.env', 'id_rsa', 'id_ed25519', 'wallet.json',
                    }:
                        raise RuntimeError('private_or_credential_file_detected')
                    if _file_contains_secret(file_path, secret):
                        raise RuntimeError('secret_material_detected_in_episode_file')
                    archive.write(
                        file_path,
                        f"episodes/{run_dir.name}/{file_path.relative_to(run_dir).as_posix()}",
                    )
            if snapshots_root.is_dir():
                for file_path in sorted(p for p in snapshots_root.rglob('*') if p.is_file()):
                    if _file_contains_secret(file_path, secret):
                        raise RuntimeError('secret_material_detected_in_snapshot')
                    archive.write(
                        file_path,
                        f"sqlite-snapshots/{file_path.relative_to(snapshots_root).as_posix()}",
                    )
        if archive_path.exists():
            raise FileExistsError('session archive already exists')
        os.link(temp_path, archive_path)
        temp_path.unlink()
        try:
            fd = os.open(archive_path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def child_environment(run_id: str, duration: int, deadline_epoch: float) -> dict[str, str]:
    env = os.environ.copy()
    # The simulator needs only the read-only Helius credential. Do not pass
    # wallet, signing, bot, database, or other API secrets into the child.
    for name in list(env):
        upper = name.upper()
        if name == 'HELIUS_API_KEY':
            continue
        if any(part in upper for part in (
            'KEY', 'TOKEN', 'MNEMONIC', 'SEED', 'SECRET', 'PASSWORD',
        )):
            env.pop(name, None)
    env.update({
        'RUN_ID': run_id,
        'RUN_DURATION_SEC': str(max(0, int(duration))),
        'OVERNIGHT_RUN_DEADLINE_EPOCH': f'{deadline_epoch:.3f}',
        'PAPER_ONLY': '1',
        'MODE': 'portfolio',
        'PORTFOLIO_SCOPE': 'ab_only',
        'SOURCES': 'pumpfun',
        'PUMP_SOURCE': 'pumpportal',
    })
    return env


def terminate_process_group(process: subprocess.Popen[bytes], wait_sec: float = 12.0) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=max(0.1, wait_sec))
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=3.0)


def main() -> int:
    if not os.environ.get('HELIUS_API_KEY', '').strip():
        print('ERROR: HELIUS_API_KEY is not set in the local process environment; no session started.', flush=True)
        return 10
    if not (PROJECT / '.venv' / 'bin' / 'python3').is_file():
        print('ERROR: project .venv is missing; install requirements and rerun; no session started.', flush=True)
        return 4

    RUNS.mkdir(parents=True, exist_ok=True)
    lock_path = RUNS / 'overnight-session.lock'
    lock_stream = lock_path.open('a+', encoding='utf-8')
    try:
        fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print('ERROR: another overnight coordinator is active; no second session started.', flush=True)
        return 2

    session_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + secrets.token_hex(4)
    summary_path = RUNS / f'overnight-session-{session_id}.json'
    log_path = RUNS / f'overnight-session-{session_id}.log'
    snapshots_root = RUNS / f'overnight-session-{session_id}-snapshots'
    archive_path = RUNS / f'overnight-session-{session_id}-results.zip'
    if any(path.exists() for path in (summary_path, log_path, snapshots_root, archive_path)):
        print('ERROR: generated session archive path already exists; refusing reuse.', flush=True)
        return 9
    snapshots_root.mkdir(parents=True, exist_ok=False)

    started_epoch = time.time()
    started_mono = time.monotonic()
    deadline_mono = started_mono + SESSION_SECONDS
    deadline_epoch = started_epoch + SESSION_SECONDS
    stop_signal: str | None = None
    stop_event = threading.Event()
    child: subprocess.Popen[bytes] | None = None
    snapshot_worker: SQLiteSnapshotWorker | None = None
    episodes: list[dict[str, Any]] = []

    def on_signal(signum: int, _frame: Any) -> None:
        nonlocal stop_signal
        stop_signal = signal.Signals(signum).name
        stop_event.set()
        if child is not None:
            terminate_process_group(child)

    previous_handlers: dict[int, Any] = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[sig] = signal.signal(sig, on_signal)

    print(
        f'Session {session_id} prepared: paper-only, {SESSION_SECONDS:,} seconds, maximum {MAX_EPISODES} episodes.',
        flush=True,
    )
    status = 'completed'
    failure_reason: str | None = None
    try:
        with log_path.open('x', encoding='utf-8') as session_log:
            for episode_number in range(1, MAX_EPISODES + 1):
                remaining = max(0, int(deadline_mono - time.monotonic()))
                if stop_signal or remaining <= FINALIZE_BUFFER_SEC:
                    break
                if episodes:
                    previous = episodes[-1]
                    if previous.get('manifest_status') != 'completed_early':
                        break
                    if remaining < RESTART_MIN_REMAINING_SEC:
                        break
                run_id = f'{session_id}-ep{episode_number:02d}-{secrets.token_hex(3)}'
                run_dir = RUNS / f'rocket2-ab-paper-{run_id}'
                if run_dir.exists():
                    status = 'stopped_with_error'
                    failure_reason = 'run_id_archive_already_exists'
                    break

                episode_remaining = max(0, int(deadline_mono - time.monotonic()))
                run_duration = max(0, episode_remaining - FINALIZE_BUFFER_SEC)
                episode_deadline_epoch = time.time() + run_duration
                if run_duration < 1:
                    break
                env = child_environment(run_id, run_duration, episode_deadline_epoch)
                command = ['/bin/bash', str(PROJECT / 'run_rocket2_ab_paper.sh')]
                session_log.write(
                    f'\n[{utc_now()}] episode={episode_number} run_id={run_id} '
                    f'planned_sec={run_duration} paper_only=1\n'
                )
                session_log.flush()
                print(f'Starting episode {episode_number}/{MAX_EPISODES}: {run_id} ({run_duration}s budget).', flush=True)

                run_started_epoch = time.time()
                run_started_mono = time.monotonic()
                child = subprocess.Popen(
                    command, cwd=PROJECT, env=env, stdin=subprocess.DEVNULL,
                    stdout=session_log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                snapshot_dir = snapshots_root / f'ep{episode_number:02d}'
                snapshot_worker = SQLiteSnapshotWorker(
                    run_dir, snapshot_dir,
                    session_id=session_id, episode=episode_number,
                    # The user waived periodic snapshots; wait beyond the
                    # session window and take only the final consistent backup.
                    interval_sec=float(SESSION_SECONDS + FINALIZE_BUFFER_SEC + 60),
                )
                snapshot_worker.start()
                checkpoint_60s: dict[str, Any] | None = None
                checkpoint_failed = False
                heartbeat_due = run_started_mono
                wait_deadline = deadline_mono - FINALIZE_BUFFER_SEC / 2
                while child.poll() is None:
                    current_mono = time.monotonic()
                    elapsed = current_mono - run_started_mono
                    if current_mono >= heartbeat_due:
                        _write_supervisor_heartbeat(
                            run_dir, session_id=session_id, run_id=run_id,
                            episode=episode_number, child_pid=child.pid,
                            observed_child_alive=child.poll() is None,
                        )
                        heartbeat_due = current_mono + 10.0
                    if checkpoint_60s is None and elapsed >= 60.0:
                        checkpoint_60s = _verify_60s_checkpoint(run_dir, child)
                        checkpoint_60s['actual_elapsed_sec'] = round(elapsed, 3)
                        try:
                            atomic_create_json(run_dir / 'checkpoint_60s.json', checkpoint_60s)
                        except FileExistsError:
                            checkpoint_60s['checkpoint_file_error'] = 'already_exists_refused'
                        session_log.write(
                            f'[{utc_now()}] 60s checkpoint: '
                            f'{json.dumps(checkpoint_60s, ensure_ascii=False, separators=(",", ":"))}\n'
                        )
                        session_log.flush()
                        print(
                            f'60s checkpoint episode {episode_number}: '
                            f'critical_ok={checkpoint_60s["critical_ok"]}; '
                            f'wss={checkpoint_60s["wss_connected_log_marker"]}; '
                            f'creator_probes={checkpoint_60s["creator_probe_count"]}.',
                            flush=True,
                        )
                        if not checkpoint_60s['critical_ok']:
                            checkpoint_failed = True
                            status = 'stopped_with_error'
                            failure_reason = '60s_checkpoint_failed'
                            terminate_process_group(child)
                            break
                        continue

                    remaining_wait = wait_deadline - current_mono
                    if remaining_wait <= 0:
                        session_log.write(f'[{utc_now()}] supervisor deadline; requesting graceful stop\n')
                        session_log.flush()
                        terminate_process_group(child)
                        break
                    wake_deadlines = [heartbeat_due, wait_deadline]
                    if checkpoint_60s is None:
                        wake_deadlines.append(run_started_mono + 60.0)
                    next_wake = min(wake_deadlines)
                    timeout = min(remaining_wait, max(0.1, next_wake - current_mono))
                    try:
                        child.wait(timeout=timeout)
                    except subprocess.TimeoutExpired:
                        continue
                if child.poll() is None:
                    terminate_process_group(child)
                exit_code = child.wait()
                _write_supervisor_heartbeat(
                    run_dir, session_id=session_id, run_id=run_id,
                    episode=episode_number, child_pid=child.pid,
                    observed_child_alive=False,
                )
                run_ended_epoch = time.time()
                snapshot_record = snapshot_worker.stop_and_snapshot() if snapshot_worker else {}
                snapshot_count = len(snapshot_worker.records) if snapshot_worker else 0
                snapshot_worker = None
                manifest = read_manifest(run_dir)
                episode = {
                    'episode': episode_number,
                    'run_id': run_id,
                    'run_dir': str(run_dir),
                    'started_at_utc': datetime.fromtimestamp(run_started_epoch, timezone.utc).isoformat(timespec='seconds'),
                    'ended_at_utc': datetime.fromtimestamp(run_ended_epoch, timezone.utc).isoformat(timespec='seconds'),
                    'duration_sec': round(run_ended_epoch - run_started_epoch, 3),
                    'exit_code': exit_code,
                    'manifest_status': manifest.get('status'),
                    'early_stop_reason': manifest.get('early_stop_reason'),
                    'run_error_type': manifest.get('run_error_type'),
                    'paper_only': manifest.get('paper_only') is True,
                    'signed_transactions': manifest.get('signed_transactions', 0),
                    'submitted_transactions': manifest.get('submitted_transactions', 0),
                    'checkpoint_60s': checkpoint_60s or 'process_exited_before_60s',
                    'sqlite_snapshot_count': snapshot_count,
                    'sqlite_snapshot_final_record': snapshot_record,
                }
                episodes.append(episode)
                child = None
                print(
                    f'Episode {episode_number} ended: status={episode["manifest_status"] or "no_manifest"}; '
                    f'early_stop_reason={episode["early_stop_reason"] or "none"}; exit={exit_code}.',
                    flush=True,
                )
                if checkpoint_failed:
                    break
                if stop_signal:
                    status = 'stopped_by_signal'
                    failure_reason = stop_signal
                    break
                if exit_code != 0 or not manifest:
                    status = 'stopped_with_error'
                    failure_reason = 'episode_failed_preflight_or_runtime'
                    break
                if manifest.get('paper_only') is not True or manifest.get('signed_transactions', 0) != 0 or manifest.get('submitted_transactions', 0) != 0:
                    status = 'stopped_with_error'
                    failure_reason = 'paper_only_invariant_violation'
                    break
                if manifest.get('status') == 'completed_early':
                    if not manifest.get('early_stop_reason'):
                        status = 'stopped_with_error'
                        failure_reason = 'early_stop_missing_reason'
                        break
                    continue
                if manifest.get('status') == 'completed':
                    break
                if manifest.get('status') == 'stopped_by_signal' and stop_signal:
                    status = 'stopped_by_signal'
                    break
                status = 'stopped_with_error'
                failure_reason = 'unexpected_episode_status'
                break

            if not episodes:
                status = 'stopped_with_error' if not stop_signal else 'stopped_by_signal'
                failure_reason = failure_reason or ('supervisor_signal' if stop_signal else 'no_episode_started')

            # If a depleted episode ended with <300 seconds remaining, or the
            # three-episode cap was reached, keep the session window intact.
            if status == 'completed' and not stop_signal:
                remaining = deadline_mono - time.monotonic()
                if remaining > 0 and not stop_signal:
                    print(f'No further episode scheduled; holding session window for {int(remaining)}s.', flush=True)
                    stop_event.wait(remaining)
    except FileExistsError:
        print('ERROR: session log already exists; refusing to overwrite archive.', flush=True)
        return 9
    except Exception as exc:
        status = 'stopped_with_error'
        failure_reason = type(exc).__name__
        print(f'ERROR: session supervisor failed safely ({failure_reason}); no trading transaction was sent.', flush=True)
    finally:
        if child is not None:
            terminate_process_group(child)
            child = None
        if snapshot_worker is not None:
            snapshot_worker.stop_and_snapshot()
            snapshot_worker = None
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    ended_epoch = time.time()
    if stop_signal and status == 'completed':
        status = 'stopped_by_signal'
        failure_reason = stop_signal
    summary = {
        'session_id': session_id,
        'status': status,
        'started_at_utc': datetime.fromtimestamp(started_epoch, timezone.utc).isoformat(timespec='seconds'),
        'ended_at_utc': datetime.fromtimestamp(ended_epoch, timezone.utc).isoformat(timespec='seconds'),
        'requested_duration_sec': SESSION_SECONDS,
        'actual_duration_sec': round(ended_epoch - started_epoch, 3),
        'session_deadline_epoch': deadline_epoch,
        'paper_only': True,
        'signed_transactions': 0,
        'submitted_transactions': 0,
        'read_only_rpc': True,
        'creator_gate_contrast': 'A=telemetry_only; B=signature_count_0_25==25 AND derived_ata_exists AND token_balance_raw>0',
        'max_episodes': MAX_EPISODES,
        'restart_min_remaining_sec': RESTART_MIN_REMAINING_SEC,
        'finalize_buffer_sec': FINALIZE_BUFFER_SEC,
        'early_stop_policy': 'episode restart only after both portfolios have no open/pending positions and both cash balances are below 500 RUB',
        'sqlite_snapshots': {
            'method': 'one final sqlite3.Connection.backup to a temporary DB, gzip, then atomic no-clobber publish',
            'periodic_snapshots': False,
            'directory': str(snapshots_root),
            'location': 'local workspace until session end; Dropbox upload requires a later connected-app tool call',
        },
        'results_archive_path': str(archive_path),
        'results_archive_created': True,
        'stop_signal': stop_signal,
        'failure_reason': failure_reason,
        'early_stop_reason': episodes[-1].get('early_stop_reason') if episodes else None,
        'episodes': episodes,
        'session_log': str(log_path),
    }
    archive_error: str | None = None
    try:
        _create_session_archive(
            archive_path, summary=summary, log_path=log_path,
            episodes=episodes, snapshots_root=snapshots_root,
            secret_value=os.environ.get('HELIUS_API_KEY', ''),
        )
    except Exception as exc:
        summary['results_archive_created'] = False
        archive_error = type(exc).__name__
        summary['results_archive_error_type'] = archive_error
        print(f'WARNING: local session archive was not created safely ({archive_error}).', flush=True)
    try:
        atomic_create_json(summary_path, summary)
    except FileExistsError:
        print('ERROR: refusing to overwrite an existing overnight summary.', flush=True)
        return 9
    print(
        f'Session summary saved: {summary_path}; status={status}; episodes={len(episodes)}; '
        f'results_archive={"created" if summary["results_archive_created"] else "not_created"}.',
        flush=True,
    )
    return 0 if status == 'completed' and summary['results_archive_created'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
