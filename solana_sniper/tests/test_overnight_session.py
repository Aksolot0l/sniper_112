from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from main_ab import _observation_offsets, _safe_manifest
from run_overnight_ab_paper import (
    DEFAULT_SESSION_SECONDS,
    _session_seconds_from_env,
    atomic_create_json,
    child_environment,
    _verify_60s_checkpoint,
    _write_supervisor_heartbeat,
    _create_session_archive,
)


class OvernightSessionTests(unittest.TestCase):
    def test_requested_window_defaults_to_two_hours_thirty_minutes(self) -> None:
        self.assertEqual(DEFAULT_SESSION_SECONDS, 9_000)
        self.assertEqual(_session_seconds_from_env(None), 9_000)

    def test_configurable_session_window_accepts_sixty_minutes(self) -> None:
        self.assertEqual(_session_seconds_from_env('3600'), 3_600)
        self.assertEqual(_session_seconds_from_env(None), 9_000)
        with self.assertRaises(ValueError):
            _session_seconds_from_env('not-a-duration')
        with self.assertRaises(ValueError):
            _session_seconds_from_env('0')

    def test_child_environment_forwards_only_readonly_helius_credential_and_paper_flags(self) -> None:
        with patch.dict(os.environ, {
            'HELIUS_API_KEY': 'test-helius-key-never-log',
            'SOLANA_PRIVATE_KEY': 'test-private-key-never-forward',
            'BOT_TOKEN': 'test-bot-token-never-forward',
            'OTHER_API_KEY': 'test-api-key-never-forward',
            'UNRELATED_SETTING': 'kept',
        }, clear=True):
            env = child_environment('session-ep01-abc123', 7000, 1_900_000_000.0)
        self.assertEqual(env['HELIUS_API_KEY'], 'test-helius-key-never-log')
        self.assertNotIn('SOLANA_PRIVATE_KEY', env)
        self.assertNotIn('BOT_TOKEN', env)
        self.assertNotIn('OTHER_API_KEY', env)
        self.assertEqual(env['UNRELATED_SETTING'], 'kept')
        self.assertEqual(env['PAPER_ONLY'], '1')
        self.assertEqual(env['PORTFOLIO_SCOPE'], 'ab_only')
        self.assertEqual(env['RUN_ID'], 'session-ep01-abc123')
        self.assertNotIn('test-private-key-never-forward', json.dumps(env))
        self.assertNotIn('test-bot-token-never-forward', json.dumps(env))

    def test_manifest_uses_helius_host_without_serializing_url_credentials(self) -> None:
        settings = SimpleNamespace(
            http_rpc_url='https://mainnet.helius-rpc.com/?api-key=secret-never-export',
            wss_url='wss://mainnet.helius-rpc.com/?api-key=secret-never-export',
            entry_fee_sol=0.00008,
            exit_fee_sol=0.00005,
            usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {
            'RUN_DURATION_SEC': '7200',
            'TELEMETRY_OBSERVATION_OFFSETS_SEC': '0,5,10,15,60,120',
        }, clear=True):
            manifest = _safe_manifest(settings, 'test-run-id', Path('/tmp/test-run'))
            offsets = _observation_offsets()
        encoded = json.dumps(manifest)
        self.assertNotIn('secret-never-export', encoded)
        self.assertEqual(manifest['rpc']['http_host'], 'mainnet.helius-rpc.com')
        self.assertEqual(manifest['rpc']['wss_host'], 'mainnet.helius-rpc.com')
        self.assertEqual(manifest['planned_duration_sec'], 7200)
        self.assertEqual(manifest['fees']['entry_network_fee_sol_model'], 0.00008)
        self.assertEqual(manifest['fees']['exit_network_fee_sol_model_per_sell'], 0.00005)
        self.assertEqual(offsets, (0, 5, 10, 15, 60, 120))
        self.assertEqual(manifest['filters']['duplicate_symbol_gate'], False)
        common = manifest['strategies']['portfolio_a']
        self.assertIn('pnl_pct <= -10.0', common['stop_loss'])
        self.assertIn('+30.0 <= pnl_pct < +50.0', common['tp1'])
        self.assertIn('pnl_pct >= +50.0', common['rocket_bypass'])
        self.assertIn('0.85 * best_pnl_pct', common['trailing'])
        self.assertIn('4.5s', common['shelf_stagnation'])
        self.assertIn('held >= 5s', common['no_progress'])
        self.assertEqual(common['timeout_before_trailing_sec'], 35)
        self.assertEqual(common['timeout_after_trailing_sec'], 90)

    def test_supervisor_heartbeat_is_atomic_and_contains_no_credential_material(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp) / 'run'
            run_dir.mkdir()
            self.assertTrue(_write_supervisor_heartbeat(
                run_dir, session_id='session-id', run_id='run-id',
                episode=1, child_pid=123, observed_child_alive=True,
            ))
            first = json.loads((run_dir / 'supervisor_heartbeat.json').read_text())
            self.assertTrue(first['observed_child_alive'])
            self.assertEqual(first['child_pid'], 123)
            self.assertTrue(_write_supervisor_heartbeat(
                run_dir, session_id='session-id', run_id='run-id',
                episode=1, child_pid=123, observed_child_alive=False,
            ))
            second = json.loads((run_dir / 'supervisor_heartbeat.json').read_text())
            self.assertFalse(second['observed_child_alive'])
            self.assertFalse(list(run_dir.glob('*.tmp-*')))

    def test_60s_checkpoint_checks_process_manifest_sqlite_wss_and_creator_probes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp) / 'run'
            run_dir.mkdir()
            (run_dir / 'manifest.json').write_text(json.dumps({
                'status': 'running', 'paper_only': True,
                'signed_transactions': 0, 'submitted_transactions': 0,
            }))
            (run_dir / 'run.log').write_text(
                'Pumportal: поток созданий Pump.fun подключён\n', encoding='utf-8',
            )
            with sqlite3.connect(run_dir / 'telemetry.sqlite') as db:
                db.execute('CREATE TABLE creator_probes (id INTEGER PRIMARY KEY)')
                db.execute('INSERT INTO creator_probes DEFAULT VALUES')

            class LiveChild:
                def poll(self):
                    return None

            checkpoint = _verify_60s_checkpoint(run_dir, LiveChild())
            self.assertTrue(checkpoint['critical_ok'])
            self.assertTrue(checkpoint['process_alive'])
            self.assertTrue(checkpoint['telemetry_db_readable'])
            self.assertTrue(checkpoint['wss_connected_log_marker'])
            self.assertEqual(checkpoint['creator_probe_count'], 1)

    def test_60s_checkpoint_is_not_ok_without_wss_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp) / 'run'
            run_dir.mkdir()
            (run_dir / 'manifest.json').write_text(json.dumps({
                'status': 'running', 'paper_only': True,
                'signed_transactions': 0, 'submitted_transactions': 0,
            }))
            (run_dir / 'run.log').write_text('no websocket yet\n', encoding='utf-8')
            with sqlite3.connect(run_dir / 'telemetry.sqlite') as db:
                db.execute('CREATE TABLE creator_probes (id INTEGER PRIMARY KEY)')

            class LiveChild:
                def poll(self):
                    return None

            checkpoint = _verify_60s_checkpoint(run_dir, LiveChild())
            self.assertFalse(checkpoint['critical_ok'])
            self.assertFalse(checkpoint['wss_connected_log_marker'])
            self.assertEqual(checkpoint['creator_probe_status'], 'no_candidate_probe_event_seen_by_checkpoint')

    def test_session_archive_is_append_only_and_contains_only_new_session_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_dir = root / 'run-unique'
            snapshots = root / 'snapshots'
            run_dir.mkdir()
            snapshots.mkdir()
            (run_dir / 'run.log').write_text('paper-only test log\n', encoding='utf-8')
            (snapshots / 'snapshot-index.jsonl').write_text('{}\n', encoding='utf-8')
            archive_path = root / 'results.zip'
            summary = {'session_id': 'unique-session', 'paper_only': True}
            _create_session_archive(
                archive_path, summary=summary, log_path=run_dir / 'run.log',
                episodes=[{'run_dir': str(run_dir)}], snapshots_root=snapshots,
                secret_value='credential-that-is-not-present',
            )
            with zipfile.ZipFile(archive_path) as archive:
                names = set(archive.namelist())
                self.assertIn('session_summary.json', names)
                self.assertIn('session.log', names)
                self.assertIn('episodes/run-unique/run.log', names)
                self.assertIn('sqlite-snapshots/snapshot-index.jsonl', names)
                self.assertNotIn('wallet.json', '\n'.join(names))
            with self.assertRaises(FileExistsError):
                _create_session_archive(
                    archive_path, summary=summary, log_path=run_dir / 'run.log',
                    episodes=[{'run_dir': str(run_dir)}], snapshots_root=snapshots,
                    secret_value='',
                )

    def test_session_archive_fails_closed_if_helius_key_value_is_in_a_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_dir = root / 'run-unique'
            run_dir.mkdir()
            (run_dir / 'run.log').write_text('do not archive this test-secret-value', encoding='utf-8')
            archive_path = root / 'blocked.zip'
            with self.assertRaisesRegex(RuntimeError, 'secret_material_detected'):
                _create_session_archive(
                    archive_path, summary={'session_id': 'unique'},
                    log_path=run_dir / 'run.log',
                    episodes=[{'run_dir': str(run_dir)}],
                    snapshots_root=root / 'no-snapshots',
                    secret_value='test-secret-value',
                )
            self.assertFalse(archive_path.exists())
            self.assertFalse(list(root.glob('blocked.zip.tmp-*')))

    def test_session_summary_create_is_atomic_and_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'overnight-session-test.json'
            payload = {'session_id': 'test', 'paper_only': True, 'episodes': []}
            atomic_create_json(path, payload)
            self.assertEqual(json.loads(path.read_text()), payload)
            with self.assertRaises(FileExistsError):
                atomic_create_json(path, {'session_id': 'must-not-overwrite'})
            self.assertEqual(json.loads(path.read_text()), payload)


if __name__ == '__main__':
    unittest.main()
