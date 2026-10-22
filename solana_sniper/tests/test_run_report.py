from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from main_ab import BASELINE_RUN_1, _safe_manifest, _summary_markdown
from telemetry import funnel_from_db


def _build_db(path: Path) -> None:
    """A tiny run: 6 candidates, 3 admits, 2 A-opened, 1 B-opened."""
    with sqlite3.connect(path) as db:
        db.execute(
            'CREATE TABLE candidates (candidate_id TEXT PRIMARY KEY, chain_id TEXT, '
            'mint TEXT, source TEXT, first_seen_at_utc TEXT)'
        )
        db.execute(
            'CREATE TABLE decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, '
            'candidate_id TEXT, mint TEXT, source TEXT, stage TEXT, decision TEXT, '
            'reasons_json TEXT, values_json TEXT, occurred_at_utc TEXT)'
        )
        db.execute(
            'CREATE TABLE paper_fills (id INTEGER PRIMARY KEY AUTOINCREMENT, '
            'mint TEXT, source TEXT, portfolio TEXT, pnl_leg_rub REAL)'
        )
        for i in range(6):
            db.execute('INSERT INTO candidates VALUES (?,?,?,?,?)',
                       (f'c{i}', 'solana-mainnet', f'mint{i}', 'Pump.fun', 't'))

        def decision(mint: str, stage: str, verdict: str, reason: str = '', values=None):
            db.execute(
                'INSERT INTO decisions (candidate_id,mint,source,stage,decision,'
                'reasons_json,values_json,occurred_at_utc) VALUES (?,?,?,?,?,?,?,?)',
                (mint, mint, 'Pump.fun', stage, verdict,
                 json.dumps([reason] if reason else []),
                 json.dumps(values or {}), 't'),
            )

        for i in range(5):
            decision(f'mint{i}', 'security_and_market_filters', 'pass')
        # funnels: 2 birth rejects, 2 too-late, 2 admits (A), 1 B admit
        for i in range(2):
            decision(f'mint{i}', 'entry_state', 'fresh_state_ok', '',
                     {'delta_bucket': '4-12', 'pass_if_cap_2.5': False,
                      'pass_if_cap_4': True, 'dev_buy_sol': None})
        for i in (2, 3):
            decision(f'mint{i}', 'entry_state', 'fresh_state_ok', '',
                     {'delta_bucket': '0.1-1', 'pass_if_cap_2.5': True,
                      'pass_if_cap_4': True, 'dev_buy_sol': 3.0})
        decision('mint0', 'portfolio_a_signal', 'skip', 'birth_out_of_band')
        decision('mint1', 'portfolio_a_signal', 'skip', 'birth_out_of_band')
        decision('mint2', 'portfolio_a_signal', 'pass', 'live_momentum_entry')
        decision('mint3', 'portfolio_a_signal', 'pass', 'live_momentum_entry')
        for mint in ('mint2', 'mint3'):
            decision(mint, 'portfolio_a_entry', 'opened', 'local_exact_quote_confirmed')
        decision('mint2', 'portfolio_b_creator_gate', 'pass', '', {
            'gate_status': 'pass', 'guard_reason': 'creator_guard_met',
            'creator_identity_type': 'pump_pportal_event_actor',
        })
        decision('mint3', 'portfolio_b_creator_gate', 'censored',
                 'creator_token_ata_missing', {
                     'gate_status': 'unavailable',
                     'guard_reason': 'creator_token_ata_missing',
                     'creator_identity_type': 'pump_curve_creator',
                 })
        decision('mint3', 'portfolio_b_entry', 'skipped', 'creator_guard_censored')
        decision('mint2', 'portfolio_b_entry', 'opened', 'local_exact_quote_confirmed')
        db.execute("INSERT INTO paper_fills (mint,source,portfolio,pnl_leg_rub) "
                   "VALUES ('mint3','Pump.fun','portfolio_a',-41.5)")
        db.commit()


class FunnelReportTests(unittest.TestCase):
    def test_funnel_counts_a_only_split_and_shadow_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        self.assertTrue(funnel['available'])
        self.assertEqual(funnel['candidates'], 6)
        self.assertEqual(funnel['security_pass'], 5)
        self.assertEqual(funnel['entry_state'], 4)
        self.assertEqual(funnel['portfolio_a_signal'],
                         {'birth_out_of_band': 2, 'live_momentum_entry': 2})
        self.assertEqual(funnel['admitted_by_current_cap'], 2)
        self.assertEqual(funnel['opened'], {'portfolio_a': 2, 'portfolio_b': 1})
        self.assertEqual(funnel['pass_if_cap'], {'pass_if_cap_2.5': 2, 'pass_if_cap_4': 4})
        self.assertEqual(funnel['delta_buckets'], {'0.1-1': 2, '4-12': 2})
        self.assertEqual(funnel['dev_buy_presence'], {'bought': 2, 'unknown': 2})
        self.assertEqual(funnel['gate_status'], {'pass': 1, 'unavailable': 1})
        self.assertEqual(funnel['gate_reasons'], {'creator_token_ata_missing': 1})
        # The A-only mint was dropped by a telemetry blind spot, not by a risk
        # signal, and its loss is attributed to that class.
        self.assertEqual(funnel['a_only']['unavailable']['mints'], 1)
        self.assertEqual(funnel['a_only']['unavailable']['pnl_leg_sum_rub'], -41.5)

    def test_missing_db_is_reported_not_raised(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            funnel = funnel_from_db(Path(tmp) / 'nope.sqlite')
        self.assertFalse(funnel['available'])
        self.assertEqual(funnel['reason'], 'telemetry_db_missing')

    def test_summary_markdown_contains_funnel_and_baseline_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        settings = SimpleNamespace(
            http_rpc_url='https://mainnet.helius-rpc.com/?api-key=secret',
            wss_url='wss://mainnet.helius-rpc.com/?api-key=secret',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        manifest.update({'actual_duration_sec': 1800, 'signed_transactions': 0})
        summary = {
            'portfolio_a': {'entry_count': 2, 'closed_trade_count': 2,
                            'realized_pnl_rub': -41.5, 'conservative_100bps_pnl_rub': -48.0},
            'portfolio_b': {'entry_count': 1, 'closed_trade_count': 1,
                            'realized_pnl_rub': 12.0, 'conservative_100bps_pnl_rub': 9.5},
        }
        text = _summary_markdown(manifest, funnel, summary)
        self.assertIn('# Прогон run-2', text)
        self.assertIn('30.5 <= v_sol_birth <= 34.5', text)
        self.assertIn('admitted_by_current_cap 2', text)
        self.assertIn(str(BASELINE_RUN_1['portfolio_a']['realized_pnl_rub']), text)
        self.assertIn(str(BASELINE_RUN_1['portfolio_b']['realized_pnl_rub']), text)
        self.assertIn('unavailable', text)
        self.assertNotIn('secret', text)
        # Baseline column must not shift by one row (a bug caught in review).
        closed_row = [ln for ln in text.splitlines() if ln.startswith('| закрыто |')][0]
        self.assertIn('| 26 |', closed_row)
        self.assertIn('| 16 |', closed_row)
        pnl_row = [ln for ln in text.splitlines() if ln.startswith('| realized PnL')][0]
        self.assertIn('-1035.56', pnl_row)

    def test_manifest_declares_fix_7_and_keeps_cap_and_ladder(self) -> None:
        settings = SimpleNamespace(
            http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        self.assertEqual(manifest['entry'], '30.5 <= v_sol_birth <= 34.5 and '
                                           '0.10 <= (v_sol_now - v_sol_birth) <= 2.5')
        self.assertIn('does NOT create profit', manifest['fix_7'])
        self.assertIn('fix #8', manifest['open_question_cap'])
        common = manifest['strategies']['portfolio_a']
        # Fix #7 must not move a single exit number.
        self.assertIn('pnl_pct <= -10.0', common['stop_loss'])
        self.assertEqual(common['timeout_before_trailing_sec'], 35)
        self.assertEqual(common['timeout_after_trailing_sec'], 90)


if __name__ == '__main__':
    unittest.main()
