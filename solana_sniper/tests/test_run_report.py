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


class LegacyGateReplayTests(unittest.TestCase):
    """DBs from earlier builds have no gate_status; derive it, don't guess."""

    def test_missing_gate_status_is_derived_from_guard_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            with sqlite3.connect(db_path) as db:
                db.execute(
                    'CREATE TABLE decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, '
                    'candidate_id TEXT, mint TEXT, source TEXT, stage TEXT, '
                    'decision TEXT, reasons_json TEXT, values_json TEXT, '
                    'occurred_at_utc TEXT)'
                )
                db.execute(
                    'CREATE TABLE paper_fills (id INTEGER PRIMARY KEY AUTOINCREMENT, '
                    'mint TEXT, source TEXT, portfolio TEXT, pnl_leg_rub REAL)'
                )

                def row(mint: str, values: dict) -> None:
                    db.execute(
                        'INSERT INTO decisions (candidate_id,mint,source,stage,decision,'
                        'reasons_json,values_json,occurred_at_utc) VALUES (?,?,?,?,?,?,?,?)',
                        ('c', mint, 'Pump.fun', 'portfolio_b_creator_gate', 'pass',
                         '[]', json.dumps(values), 't'),
                    )

                # old schema: guard_reason only, no gate_status
                for i in range(17):
                    row(f'ok{i}', {'guard_reason': 'creator_guard_met',
                                   'creator_guard_passed': True,
                                   'creator_identity_type': 'pumpportal_event_actor'})
                row('blind', {'guard_reason': 'creator_token_ata_missing',
                              'creator_guard_passed': False})
                row('risk', {'guard_reason': 'creator_burst_activity',
                             'creator_guard_passed': False})
                # confirmed pass without the boolean must still be a pass
                row('met_only', {'guard_reason': 'creator_guard_met'})
                db.commit()
            funnel = funnel_from_db(db_path)
        self.assertEqual(funnel['gate_status'], {'fail_risk': 1, 'pass': 18, 'unavailable': 1})
        self.assertNotIn('creator_guard_met', funnel['gate_reasons'])
        self.assertEqual(funnel['gate_reasons'],
                         {'creator_burst_activity': 1, 'creator_token_ata_missing': 1})

    def test_helper_agrees_with_portfolio_ab_classification(self) -> None:
        from portfolio_ab import gate_status as profile_class
        from telemetry import gate_status_from_values

        cases = [
            (True, 'creator_guard_met', {}),
            (False, 'creator_history_unavailable', {}),
            (False, 'creator_token_ata_missing', {}),
            (False, 'creator_burst_activity', {}),
            (False, 'creator_repeat_deploy_in_session', {}),
            (False, 'creator_token_balance_zero', {}),
            (False, 'creator_history_too_short',
             {'creator_identity_type': 'pump_curve_creator',
              'pumpportal_event_actor': 'Actor'}),
            (False, 'creator_history_too_short',
             {'creator_identity_type': 'pumpportal_event_actor',
              'pumpportal_event_actor': 'Actor'}),
        ]
        for passed, reason, probe in cases:
            expected = profile_class(passed, reason, probe)
            values = {'guard_reason': reason, 'creator_guard_passed': passed, **probe}
            self.assertEqual(gate_status_from_values(values), expected, (passed, reason))


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

    def test_summary_renders_shadow_table_and_preregistered_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        manifest = {'run_id': 'run-3', 'build': 'x', 'entry': 'e',
                    'actual_duration_sec': 28800, 'planned_duration_sec': 28800,
                    'paper_only': True, 'signed_transactions': 0}
        summary = {
            'portfolio_a': {'entry_count': 12, 'realized_pnl_rub': -240.0,
                            'stats': {'n': 12, 'mean': -20.0, 'median': -21.0,
                                      'sd': 40.0, 'ci95_mean': [-42.0, 2.0],
                                      'sum': -240.0, 'sum_without_top2': -300.0,
                                      'sum_without_worst2': -100.0}},
            'portfolio_b': {'entry_count': 15, 'realized_pnl_rub': -150.0,
                            'stats': {'n': 15, 'mean': -10.0, 'median': -11.0,
                                      'sd': 38.0, 'ci95_mean': [-30.0, 10.0],
                                      'sum': -150.0, 'sum_without_top2': -210.0,
                                      'sum_without_worst2': -20.0}},
            'shadow_exit25': {'is_shadow': True, 'entry_count': 12,
                              'stats': {'n': 12, 'mean': -18.0, 'median': -19.0,
                                        'sd': 30.0, 'ci95_mean': [-35.0, -2.0],
                                        'sum': -216.0, 'sum_without_top2': -270.0,
                                        'sum_without_worst2': -90.0}},
            'ab_test': {
                'single_difference': 'delta_max_sol',
                'decision_rule': 'B beats A iff ...',
                'portfolio_a': {'delta_max_sol': 2.5},
                'portfolio_b': {'delta_max_sol': 3.0},
                'difference': {'mean_a': -20.0, 'mean_b': -10.0,
                               'mean_diff': 10.0, 'ci95': [1.0, 19.0],
                               'n_a': 12, 'n_b': 15},
            },
        }
        text = _summary_markdown(manifest, funnel, summary)
        self.assertIn('shadow_exit25 (тень)', text)
        self.assertIn('CI95 среднего', text)
        self.assertIn('Предрегистрированный критерий', text)
        self.assertIn('B лучше A по критерию', text)
        # The shadow row must not leak into the A/B comparison columns.
        self.assertIn('| portfolio_a | 12 |', text)

    def test_summary_tolerates_scalar_top_level_summary_keys(self) -> None:
        # A real ab_summary.json mixes books with scalars (run_id, duration,
        # fee_model); iterating it blindly crashed the run's last artefact.
        summary = {
            'run_id': 'run-5', 'duration_sec': 28800.0, 'fee_model': 'x',
            'portfolio_a': {'stats': {'n': 1, 'mean': -1.0, 'median': -1.0,
                                      'sd': 0.0, 'ci95_mean': None, 'sum': -1.0}},
            'portfolio_b': {'stats': {'n': 0}},
        }
        text = _summary_markdown({'run_id': 'run-5'}, {}, summary)
        self.assertIn('| portfolio_a | 1 |', text)
        self.assertIn('| portfolio_b | 0 |', text)

    def test_summary_reports_failure_of_the_criterion_without_spin(self) -> None:
        manifest = {'run_id': 'run-4', 'paper_only': True, 'signed_transactions': 0}
        summary = {
            'portfolio_a': {'stats': {'n': 10, 'mean': -20.0, 'median': -20.0,
                                      'sd': 30.0, 'ci95_mean': [-40.0, 0.0],
                                      'sum': -200.0}},
            'portfolio_b': {'stats': {'n': 10, 'mean': -18.0, 'median': -18.0,
                                      'sd': 30.0, 'ci95_mean': [-38.0, 2.0],
                                      'sum': -180.0}},
            'ab_test': {'single_difference': 'delta_max_sol',
                        'portfolio_a': {'delta_max_sol': 2.5},
                        'portfolio_b': {'delta_max_sol': 3.0},
                        'difference': {'mean_diff': 2.0, 'ci95': [-9.0, 13.0]}},
        }
        text = _summary_markdown(manifest, {}, summary)
        self.assertIn('критерий не выполнен', text)
        self.assertIn('5-секундный', text)
        self.assertNotIn('стало прибыльно', text.lower())

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

    def test_per_trade_row_keeps_each_arm_in_its_own_column(self) -> None:
        """Regression: both arms used to print into the portfolio_a column."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        settings = SimpleNamespace(
            http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        # One trade per arm, so per-trade PnL equals the fixture PnL exactly.
        summary = {
            'portfolio_a': {'entry_count': 1, 'closed_trade_count': 1,
                            'realized_pnl_rub': -41.5, 'conservative_100bps_pnl_rub': -48.0},
            'portfolio_b': {'entry_count': 1, 'closed_trade_count': 1,
                            'realized_pnl_rub': 12.0, 'conservative_100bps_pnl_rub': 9.5},
        }
        text = _summary_markdown(manifest, funnel, summary)
        rows = [ln for ln in text.splitlines() if ln.startswith('| PnL на сделку')]
        self.assertEqual(len(rows), 1, rows)
        cells = [c.strip() for c in rows[0].strip('|').split('|')]
        self.assertEqual(len(cells), 5, rows[0])
        self.assertEqual(cells[0], 'PnL на сделку, ₽')
        self.assertEqual(cells[1], '-41.50')
        self.assertEqual(cells[2], '12.00')
        self.assertEqual(cells[3], '-39.83')   # baseline run #1, arm A
        self.assertEqual(cells[4], '-42.05')   # baseline run #1, arm B

    def test_money_columns_use_two_decimals_and_dash_for_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        settings = SimpleNamespace(
            http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        summary = {
            'portfolio_a': {'entry_count': 2, 'closed_trade_count': 2,
                            'realized_pnl_rub': -1035.555661,
                            'conservative_100bps_pnl_rub': None},
            'portfolio_b': {'entry_count': 0, 'closed_trade_count': 0,
                            'realized_pnl_rub': 0.0, 'conservative_100bps_pnl_rub': 0.0},
        }
        text = _summary_markdown(manifest, funnel, summary)
        self.assertIn('| realized PnL, ₽ | -1035.56 | 0.00 |', text)
        self.assertNotIn('-1035.555661', text)
        # No entries -> no per-trade number, and a dash instead of 0.00.
        per_trade = [ln for ln in text.splitlines() if ln.startswith('| PnL на сделку')][0]
        cells = [c.strip() for c in per_trade.strip('|').split('|')]
        self.assertEqual(cells[1], '-517.78')
        self.assertEqual(cells[2], '—')
        # Conservative None must not leak into the table as "None".
        self.assertNotIn('| None |', text)

    def test_funnel_lines_do_not_glue_reason_and_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        settings = SimpleNamespace(
            http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        funnel['portfolio_b_signal'] = {
            'creator_guard_creator_history_too_short': 5,
            'live_momentum_entry': 7,
        }
        text = _summary_markdown(manifest, funnel, {
            'portfolio_a': {'entry_count': 0}, 'portfolio_b': {'entry_count': 0},
        })
        self.assertNotIn('too_short5', text)
        self.assertIn('creator_guard_creator_history_too_short', text)
        for line in text.splitlines():
            if 'creator_guard_creator_history_too_short' in line:
                self.assertTrue(line.endswith('5'), line)
                # The count must be separated from the reason by whitespace.
                self.assertEqual(line[-2], ' ', f'glued reason/count: {line!r}')

    def test_summary_surfaces_lost_telemetry_rows(self) -> None:
        """A dropped row used to be one yellow log line and nothing else."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        settings = SimpleNamespace(
            http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        manifest['telemetry'] = {
            'dropped_events': 0, 'write_errors': 1, 'writer_error': 'ProgrammingError',
        }
        text = _summary_markdown(manifest, funnel, {
            'portfolio_a': {'entry_count': 1, 'realized_pnl_rub': -41.5},
            'portfolio_b': {'entry_count': 1, 'realized_pnl_rub': 12.0},
        })
        self.assertIn('write_errors=1', text)
        self.assertIn('ProgrammingError', text)
        self.assertIn('не записалась', text)

        # Clean run: counters are still reported, without the warning.
        manifest['telemetry'] = {'dropped_events': 0, 'write_errors': 0, 'writer_error': None}
        text = _summary_markdown(manifest, funnel, {
            'portfolio_a': {'entry_count': 1, 'realized_pnl_rub': -41.5},
            'portfolio_b': {'entry_count': 1, 'realized_pnl_rub': 12.0},
        })
        self.assertIn('write_errors=0', text)
        self.assertNotIn('не записалась', text)

    def test_manifest_declares_fix_7_and_keeps_cap_and_ladder(self) -> None:
        settings = SimpleNamespace(
            http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '1800'}, clear=True):
            manifest = _safe_manifest(settings, 'run-2', Path('/tmp/run-2'))
        # Run #3: the band is per arm, one difference only.
        self.assertEqual(manifest['entry'], '30.5 <= v_sol_birth <= 34.5 and '
                                           '0.10 <= (v_sol_now - v_sol_birth) <= A 2.5 / B 3.0')
        self.assertEqual(manifest['profiles']['portfolio_a'],
                         {'delta_cap': 2.5, 'creator_gate': False})
        self.assertEqual(manifest['profiles']['portfolio_b'],
                         {'delta_cap': 3.0, 'creator_gate': False})
        self.assertEqual(manifest['run3_single_difference'], 'delta_max_sol')
        self.assertFalse(manifest['strategies']['portfolio_b']['creator_gate'])
        self.assertTrue(manifest['strategies']['shadow_gate']['creator_gate'])
        self.assertIn('does NOT create profit', manifest['fix_7'])
        self.assertIn('fix #8', manifest['open_question_cap'])
        common = manifest['strategies']['portfolio_a']
        # Fix #7 must not move a single exit number.
        self.assertIn('pnl_pct <= -10.0', common['stop_loss'])
        self.assertEqual(common['timeout_before_trailing_sec'], 35)
        self.assertEqual(common['timeout_after_trailing_sec'], 90)


if __name__ == '__main__':
    unittest.main()
