from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from live_ab_dashboard import HTML, make_snapshot


class LiveABDashboardTests(unittest.TestCase):
    def test_page_has_only_logs_and_portfolio_tabs_with_live_config_fields(self) -> None:
        self.assertIn('/api/status', HTML)
        self.assertIn('data-view="logs"', HTML)
        self.assertIn('data-view="portfolios"', HTML)
        self.assertNotIn('data-view="overview"', HTML)
        self.assertNotIn('data-view="positions"', HTML)
        self.assertIn('max_slots', HTML)
        self.assertIn('profit_retain_ratio', HTML)
        self.assertIn('rocket_bypass_pct', HTML)
        self.assertIn('creator_gate', HTML)
        self.assertIn('closed_trades', HTML)
        self.assertIn('Слот ${index + 1}', HTML)  # three numbered slot tiles
        self.assertNotIn('20261021T174318Z-28258057', HTML)
        self.assertNotIn('TP1 +35%', HTML)
        self.assertNotIn('TP1 +40%', HTML)

    def test_snapshot_exposes_portfolio_config_accounting_and_slot_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / 'run'
            run_dir.mkdir()
            (run_dir / 'manifest.json').write_text(json.dumps({
                'run_id': 'unique-run', 'status': 'running', 'paper_only': True,
                'strategies': {'portfolio_a': {'initial_balance_rub': 1500,
                    'position_size_rub': 500, 'max_slots': 3}},
            }), encoding='utf-8')
            (run_dir / 'wallet_portfolio_a.json').write_text(json.dumps({
                'run_id': 'unique-run', 'portfolio': 'portfolio_a',
                'strategy': {'name': 'portfolio_a', 'v_sol_birth_min': 30.5,
                    'v_sol_birth_max': 38.0, 'max_slots': 3},
                'initial_balance_rub': 1500, 'position_size_rub': 500,
                'max_slots': 3, 'cash_balance_rub': 1475,
                'realized_pnl_rub': -25, 'snapshot_pnl_rub': -25,
                'open_positions': [{'mint': 'open-mint', 'status': 'open'}],
                'pending_positions': [], 'closed_trades': [{'mint': 'closed-mint'}],
                'closed_trade_count': 1, 'updated_at_utc': '2026-10-22T06:00:00Z',
            }), encoding='utf-8')
            snapshot = make_snapshot(run_dir)
        portfolio_a = snapshot['portfolios'][0]
        self.assertEqual(portfolio_a['max_slots'], 3)
        self.assertEqual(portfolio_a['position_size_rub'], 500)
        self.assertEqual(portfolio_a['open_count'], 1)
        self.assertEqual(portfolio_a['closed_trade_count'], 1)
        self.assertEqual(portfolio_a['strategy']['v_sol_birth_min'], 30.5)
        self.assertEqual(portfolio_a['closed_trades'][0]['mint'], 'closed-mint')
        self.assertIsNone(snapshot['portfolios'][1]['cash_balance_rub'])

    def test_missing_run_state_is_unknown_not_fabricated_as_1500_rub(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = make_snapshot(Path(tmp) / 'not-created-yet')
        self.assertEqual(snapshot['freshness'], 'unknown')
        self.assertIsNone(snapshot['portfolios'][0]['cash_balance_rub'])
        self.assertIsNone(snapshot['portfolios'][1]['cash_balance_rub'])

    def test_manifest_running_without_supervisor_heartbeat_is_not_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / 'run'
            run_dir.mkdir()
            (run_dir / 'manifest.json').write_text(json.dumps({
                'run_id': 'unique-run', 'status': 'running', 'paper_only': True,
            }), encoding='utf-8')
            (run_dir / 'run.log').write_text('old log line\n', encoding='utf-8')
            snapshot = make_snapshot(run_dir)
        self.assertEqual(snapshot['freshness'], 'unverified')
        self.assertIn('процесс не подтверждён', snapshot['freshness_label'].lower())

    def test_only_fresh_matching_alive_supervisor_heartbeat_is_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / 'run'
            run_dir.mkdir()
            (run_dir / 'manifest.json').write_text(json.dumps({
                'run_id': 'unique-run', 'status': 'running', 'paper_only': True,
            }), encoding='utf-8')
            heartbeat = run_dir / 'supervisor_heartbeat.json'
            heartbeat.write_text(json.dumps({
                'run_id': 'unique-run', 'observed_child_alive': True,
                'child_pid': 12345, 'timestamp_epoch': time.time(),
            }), encoding='utf-8')
            fresh = make_snapshot(run_dir)
            self.assertEqual(fresh['freshness'], 'fresh')
            self.assertTrue(fresh['supervisor_heartbeat']['observed_child_alive'])

            heartbeat.write_text(json.dumps({
                'run_id': 'unique-run', 'observed_child_alive': True,
                'child_pid': 12345, 'timestamp_epoch': time.time() - 30,
            }), encoding='utf-8')
            stale = make_snapshot(run_dir)
        self.assertEqual(stale['freshness'], 'stale')


if __name__ == '__main__':
    unittest.main()
