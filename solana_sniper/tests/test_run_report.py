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

    def test_summary_renders_bank_top_ups_and_bust(self) -> None:
        summary = {
            'portfolio_a': {'stats': {'n': 5, 'mean': -20.0, 'median': -20.0,
                                      'sd': 1.0, 'ci95_mean': None, 'sum': -100.0},
                            'top_ups': 2, 'capital_injected_rub': 1_400.0,
                            'pnl_with_topups': -1_260.0, 'equity_no_topups': 240.0,
                            'bust_at_trade': 7,
                            'bust_at_utc': '2026-10-22T10:00:00Z'},
            'portfolio_b': {'stats': {'n': 4, 'mean': -10.0, 'median': -10.0,
                                      'sd': 1.0, 'ci95_mean': None, 'sum': -40.0},
                            'top_ups': 0, 'capital_injected_rub': 0.0,
                            'pnl_with_topups': -40.0, 'equity_no_topups': 1_460.0,
                            'bust_at_trade': None, 'bust_at_utc': None},
            'ab_test': {'single_difference': 'delta_max_sol',
                        'portfolio_a': {'delta_max_sol': 2.5},
                        'portfolio_b': {'delta_max_sol': 3.0},
                        'difference': {'mean_diff': 10.0, 'ci95': [1.0, 19.0]}},
        }
        text = _summary_markdown({'run_id': 'run-6'}, {}, summary)
        self.assertIn('## Банк 1 500 ₽', text)
        self.assertIn('| portfolio_a | 2 | 1400.00 | -1260.00 | 240.00 | №7 | '
                      '2026-10-22T10:00:00Z |', text)
        self.assertIn('не умирал', text)
        self.assertIn('Пополнение — только бухгалтерия', text)

    def test_summary_shows_early_stop_reason(self) -> None:
        text = _summary_markdown(
            {'run_id': 'run-7',
             'early_stop_reason': 'rpc_error_rate (failed=40/2000 = 2.00%)'},
            {}, {},
        )
        self.assertIn('досрочная остановка', text)
        self.assertIn('rpc_error_rate', text)

    def test_duration_is_a_parameter_not_a_constant(self) -> None:
        from run_local_session import build_parser

        parser = build_parser()
        self.assertEqual(parser.parse_args(['--duration-sec', '10800']).duration, 10_800)
        self.assertEqual(parser.parse_args(['--duration', '10800']).duration, 10_800)
        # The 8 h default is a default, not a hard-coded run length.
        self.assertEqual(parser.parse_args([]).duration, 28_800)

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
            'ab_test': {'single_difference': 'delta_min',
                        'entry_latency_ms': 150.0,
                        'portfolio_a': {'delta_min_sol': 0.10},
                        'portfolio_b': {'delta_min_sol': 0.0},
                        'difference': {'mean_diff': 2.0, 'ci95': [-9.0, 13.0]}},
        }
        text = _summary_markdown(manifest, {}, summary)
        self.assertIn('критерий не выполнен', text)
        self.assertNotIn('стало прибыльно', text.lower())
        # The run must never be sold as an improvement.
        self.assertIn('B остаётся убыточным', text)
        self.assertIn('entry_latency_ms = 150.0', text)
        self.assertIn('| portfolio_a | 10 | -20.00 |', text)
        # Exit reasons and the P0 acceptance table are part of the report.
        self.assertIn('exit_reason × портфель', text)
        self.assertIn('P0: принят ли прогон', text)

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
        # Run #4: the single live difference is the EXIT rule.
        self.assertEqual(
            manifest['entry'],
            '30.5 <= v_sol_birth <= 34.5 and '
            '0.10 <= (v_sol_now - v_sol_birth) <= 2.5  (identical for A and B)',
        )
        self.assertEqual(manifest['profiles']['portfolio_a']['exit_rule'],
                         '4-mode ladder (Round 6, unchanged)')
        self.assertIn('time_exit_15s', manifest['profiles']['portfolio_b']['exit_rule'])
        self.assertEqual(manifest['profiles']['portfolio_a']['delta_min'], 0.10)
        self.assertEqual(manifest['profiles']['portfolio_b']['delta_min'], 0.10)
        self.assertEqual(manifest['run4_single_difference'], 'exit_rule')
        self.assertEqual(manifest['entry_latency_ms'], 150.0)
        self.assertEqual(manifest['ab_test']['single_difference'], 'exit_rule')
        self.assertEqual(manifest['profiles']['shadow_no_trail']['max_slots'], 1)
        self.assertEqual(manifest['profiles']['shadow_no_trail']['mirror_of'],
                         'portfolio_a')
        self.assertNotIn('shadow_exit25', manifest['profiles'])
        self.assertIn('+8 RUB', manifest['run4_decision_rule'])
        self.assertIn('2%', manifest['stop_list'])
        # Run #3's declaration stays in the manifest as a closed result.
        self.assertTrue(manifest['run3_single_difference'].startswith('delta_max_sol'))
        self.assertIn('does NOT create profit', manifest['fix_7'])
        self.assertIn('fix #8', manifest['open_question_cap'])
        common = manifest['strategies']['portfolio_a']
        # Fix #7 must not move a single exit number.
        self.assertIn('pnl_pct <= -10.0', common['stop_loss'])
        self.assertEqual(common['timeout_before_trailing_sec'], 35)
        self.assertEqual(common['timeout_after_trailing_sec'], 90)



class ReportRegressionTests(unittest.TestCase):
    """Defects found while reviewing fix #7 — locked so they cannot return."""

    def _summary(self) -> dict:
        return {
            'portfolio_a': {'entry_count': 26, 'realized_pnl_rub': -1035.555661,
                            'stats': {'n': 26, 'mean': -39.83, 'median': -40.0,
                                      'sd': 10.0, 'ci95_mean': [-45.0, -35.0],
                                      'sum': -1035.56}},
            'portfolio_b': {'entry_count': 16, 'realized_pnl_rub': -672.77,
                            'stats': {'n': 16, 'mean': -42.05, 'median': -42.0,
                                      'sd': 10.0, 'ci95_mean': [-47.0, -37.0],
                                      'sum': -672.77}},
            'ab_test': {
                'single_difference': 'delta_min', 'entry_latency_ms': 150.0,
                'portfolio_a': {'delta_min_sol': 0.1},
                'portfolio_b': {'delta_min_sol': 0.0},
                'difference': {'mean_diff': -2.0, 'ci95': [-20.0, 16.0]},
            },
        }

    @staticmethod
    def _manifest() -> dict:
        return {'run_id': 'run-x', 'build': 'x', 'entry': 'e',
                'actual_duration_sec': 1800, 'planned_duration_sec': 1800,
                'paper_only': True, 'signed_transactions': 0}

    def test_per_trade_pnl_keeps_each_arm_in_its_own_column(self) -> None:
        # D1: the row used to print both arms into the portfolio_a column, so
        # a reader compared A against A and "run #1 B" stayed empty.
        text = _summary_markdown(self._manifest(), {}, self._summary())
        row = next(line for line in text.splitlines()
                   if line.startswith('| PnL на сделку'))
        cells = [cell.strip() for cell in row.strip('|').split('|')]
        self.assertEqual(cells[0], 'PnL на сделку, ₽')
        # A, B, run #1 A, run #1 B — each number under its own arm.
        self.assertEqual(cells[1], '-39.83')
        self.assertEqual(cells[2], '-42.05')
        self.assertEqual(cells[3], '-39.83')
        self.assertEqual(cells[4], '-42.05')

    def test_per_trade_pnl_is_dash_when_an_arm_has_no_entries(self) -> None:
        summary = self._summary()
        summary['portfolio_b'] = {'entry_count': 0, 'realized_pnl_rub': 0.0,
                                  'stats': {'n': 0}}
        text = _summary_markdown(self._manifest(), {}, summary)
        row = next(line for line in text.splitlines()
                   if line.startswith('| PnL на сделку'))
        cells = [cell.strip() for cell in row.strip('|').split('|')]
        self.assertEqual(cells[1], '-39.83')
        self.assertEqual(cells[2], '—')     # never a fake 0 or a divide by zero

    def test_pnl_is_rounded_not_printed_as_a_raw_float(self) -> None:
        # D5: -1035.555661 must not leak into the table.
        text = _summary_markdown(self._manifest(), {}, self._summary())
        self.assertNotIn('-1035.555661', text)
        self.assertIn('| realized PnL, ₽ | -1035.56 | -672.77 |', text)

    def test_long_skip_reasons_stay_separated_from_their_count(self) -> None:
        # D2: width 30 glued "creator_guard_creator_history_too_short5".
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
                    'INSERT INTO decisions (candidate_id,mint,source,stage,'
                    'decision,reasons_json,values_json,occurred_at_utc) '
                    'VALUES (?,?,?,?,?,?,?,?)',
                    ('m1', 'm1', 'Pump.fun', 'portfolio_b_entry', 'skipped',
                     json.dumps(['creator_guard_creator_history_too_short']),
                     '{}', 't'),
                )
                db.commit()
            funnel = funnel_from_db(db_path)
        text = _summary_markdown(self._manifest(), funnel, self._summary())
        line = next(line for line in text.splitlines()
                    if 'creator_guard_creator_history_too_short' in line)
        self.assertRegex(line, r'creator_history_too_short\s{2,}1$')

    def test_passing_gate_rows_never_reach_gate_reasons(self) -> None:
        # D4: a successful probe must not be filed under "reasons for refusal",
        # even when an old build stored a stale gate_status.
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            with sqlite3.connect(db_path) as db:
                db.execute(
                    'CREATE TABLE decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, '
                    'candidate_id TEXT, mint TEXT, source TEXT, stage TEXT, '
                    'decision TEXT, reasons_json TEXT, values_json TEXT, '
                    'occurred_at_utc TEXT)'
                )
                for mint, values in (
                    ('a', {'gate_status': 'unavailable',
                           'guard_reason': 'creator_guard_met',
                           'creator_guard_passed': True,
                           'creator_identity_type': 'pump_curve_creator'}),
                    ('b', {'guard_reason': 'creator_burst_activity',
                           'creator_guard_passed': False, 'status': 'ok',
                           'creator_identity_type': 'pump_curve_creator'}),
                ):
                    db.execute(
                        'INSERT INTO decisions (candidate_id,mint,source,stage,'
                        'decision,reasons_json,values_json,occurred_at_utc) '
                        'VALUES (?,?,?,?,?,?,?,?)',
                        (mint, mint, 'Pump.fun', 'portfolio_b_creator_gate',
                         'pass' if values.get('creator_guard_passed') else 'fail',
                         '[]', json.dumps(values), 't'),
                    )
                db.commit()
            funnel = funnel_from_db(db_path)
        self.assertEqual(funnel['gate_status'].get('pass'), 1)
        self.assertNotIn('creator_guard_met', funnel['gate_reasons'])
        self.assertEqual(funnel['gate_reasons'], {'creator_burst_activity': 1})


if __name__ == '__main__':
    unittest.main()
