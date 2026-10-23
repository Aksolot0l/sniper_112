from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from main_ab import _safe_manifest, _summary_markdown
from portfolio_ab import ARM_IDS
from telemetry import funnel_from_db


def _build_db(path: Path) -> None:
    """A tiny run: 6 candidates, 5 passes, 3 arms, 2 A-opened, 1 B1-opened."""
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
        for i in range(2):
            decision(f'mint{i}', 'entry_state', 'fresh_state_ok', '',
                     {'delta_bucket': '4-12', 'pass_if_cap_2.5': False,
                      'pass_if_cap_4': True, 'dev_buy_sol': None})
        for i in (2, 3):
            decision(f'mint{i}', 'entry_state', 'fresh_state_ok', '',
                     {'delta_bucket': '0.1-1', 'pass_if_cap_2.5': True,
                      'pass_if_cap_4': True, 'dev_buy_sol': 3.0})
        # Run #7: the funnel discovers the arms from the stages themselves.
        decision('mint0', 'portfolio_a_signal', 'skip', 'birth_out_of_band')
        decision('mint1', 'portfolio_a_signal', 'skip', 'birth_out_of_band')
        decision('mint2', 'portfolio_a_signal', 'pass', 'live_momentum_entry')
        decision('mint3', 'portfolio_a_signal', 'pass', 'live_momentum_entry')
        decision('mint2', 'portfolio_b1_narrow_signal', 'pass', 'live_momentum_entry')
        decision('mint3', 'portfolio_b1_narrow_signal', 'skip', 'momentum_too_late')
        decision('mint2', 'portfolio_b2_birth_signal', 'skip', 'birth_out_of_band')
        decision('mint3', 'portfolio_b2_birth_signal', 'skip', 'birth_out_of_band')
        for mint in ('mint2', 'mint3'):
            decision(mint, 'portfolio_a_entry', 'opened', 'local_exact_quote_confirmed')
        decision('mint2', 'portfolio_b1_narrow_entry', 'opened',
                 'local_exact_quote_confirmed')
        decision('mint3', 'portfolio_b1_narrow_entry', 'skipped', 'slots_full')
        db.execute("INSERT INTO paper_fills (mint,source,portfolio,pnl_leg_rub) "
                   "VALUES ('mint3','Pump.fun','portfolio_a',-41.5)")
        db.commit()


def _settings():
    return SimpleNamespace(
        http_rpc_url='https://rpc.example.invalid', wss_url='wss://rpc.example.invalid',
        entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
    )


BANDS = {
    'portfolio_a': '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 2.50',
    'portfolio_b1_narrow': '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 1.00',
    'portfolio_b2_birth': '30.50 <= v_sol_birth <= 31.00; 0.10 <= delta <= 2.50',
    'portfolio_b3_t0': '30.50 <= v_sol_birth <= 34.50; 0.00 <= delta <= 2.50',
}


def _run7_summary() -> dict:
    """Four arms, one paired comparison each, no shadows anywhere."""
    def book(arm: str, band: str, *, n: int, total: float, **extra) -> dict:
        stats = {
            'n': n, 'mean': round(total / n, 4) if n else None,
            'median': -15.2, 'sd': 98.0 if n else None,
            'ci95_mean': [-9.83, 13.78] if n else None,
            'sum': total, 'sum_without_top2': total - 744.64,
        }
        return {
            'arm_id': arm, 'entry_band_label': band, 'stats': stats,
            'realized_pnl_rub': total, 'top_ups': 0, 'capital_injected_rub': 0.0,
            'pnl_with_topups': total, 'equity_no_topups': 1500.0 + total,
            'bust_at_trade': None, 'exit_rule': '4-mode ladder (Round 6, unchanged)',
            'exit_breakdown': {'timeout': {'n': n, 'sum_rub': total,
                                           'mean_rub': stats['mean'],
                                           'median_hold_sec': 35.0}},
            'blocks': {'h1': {'n': n, 'sum': total, 'mean': stats['mean']}},
            'rocket_accounting': {'n': n, 'rockets': 13, 'rocket_share': 0.0778,
                                  'rockets_mean_rub': 155.6, 'others_mean_rub': -11.5,
                                  'break_even_rocket_share': 0.0689},
            **extra,
        }

    return {
        'run_id': 'run-7',
        'portfolio_a': book('A', BANDS['portfolio_a'],
                            n=167, total=248.61, paired_vs_a={'reference': True},
                            stability={'reference': True}),
        'portfolio_b1_narrow': book(
            'B1', BANDS['portfolio_b1_narrow'],
            n=146, total=100.0,
            paired_vs_a={'n_pairs': 140, 'mean_diff': 4.24, 'sd_diff': 98.0,
                         'ci95_diff': [-9.2, 17.7], 'ci_level': 0.9875,
                         'wins': 60, 'losses': 70, 'ties': 10},
            stability={'first_half': {'mean_diff': -1.0},
                       'second_half': {'mean_diff': 9.0}, 'sign_stable': False},
        ),
        'portfolio_b2_birth': book(
            'B2', BANDS['portfolio_b2_birth'],
            n=94, total=-506.7,
            paired_vs_a={'n_pairs': 90, 'mean_diff': -8.1, 'sd_diff': 99.0,
                         'ci95_diff': [-28.0, 12.0], 'ci_level': 0.9875,
                         'wins': 30, 'losses': 55, 'ties': 5},
            stability={'first_half': {'mean_diff': -6.0},
                       'second_half': {'mean_diff': -10.0}, 'sign_stable': True},
        ),
        'portfolio_b3_t0': book(
            'B3', BANDS['portfolio_b3_t0'],
            n=210, total=-210.0,
            paired_vs_a={'n_pairs': 160, 'mean_diff': -3.0, 'sd_diff': 98.0,
                         'ci95_diff': [-18.0, 12.0], 'ci_level': 0.9875,
                         'wins': 70, 'losses': 80, 'ties': 10},
            stability={'first_half': {'mean_diff': -1.0},
                       'second_half': {'mean_diff': -5.0}, 'sign_stable': True},
        ),
        'ab_test': {
            'single_difference': 'entry_band',
            'reference_arm': 'portfolio_a',
            'arms': {
                'portfolio_a': {'arm_id': 'A'},
                'portfolio_b1_narrow': {'arm_id': 'B1',
                                        'single_boundary_vs_a': ['delta_max_sol 2.50 -> 1.00']},
                'portfolio_b2_birth': {'arm_id': 'B2',
                                       'single_boundary_vs_a': ['v_sol_birth_max 34.50 -> 31.00']},
                'portfolio_b3_t0': {'arm_id': 'B3',
                                    'single_boundary_vs_a': ['delta_min_sol 0.10 -> 0.00']},
            },
            'expectation': {'portfolio_b1_narrow': '+1 … +6 ₽',
                            'portfolio_b2_birth': '−8 … +8 ₽',
                            'portfolio_b3_t0': '+3 … +9 ₽'},
            'power_note': 'на 8 часах шумовой пол ±10 ₽; разница < 15 ₽ на 3 часах нерешаема',
            'decision_rule': 'paired mean(arm − A) >= +8 RUB and the CI excludes 0',
        },
        'profiles': {
            'portfolio_a': {'arm_id': 'A', 'entry_band_label': BANDS['portfolio_a']},
            'portfolio_b1_narrow': {'arm_id': 'B1', 'entry_band_label': BANDS['portfolio_b1_narrow']},
            'portfolio_b2_birth': {'arm_id': 'B2', 'entry_band_label': BANDS['portfolio_b2_birth']},
            'portfolio_b3_t0': {'arm_id': 'B3', 'entry_band_label': BANDS['portfolio_b3_t0']},
        },
    }


class FunnelReportTests(unittest.TestCase):
    def _manifest(self) -> dict:
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '28800',
                                     'ENTRY_ENRICH': '1'}, clear=True):
            return _safe_manifest(_settings(), 'run-7', Path('/tmp/run-7'))

    def test_manifest_says_the_run_has_no_reference_arm(self) -> None:
        # Run #8: S1 + S0 only. reference_arm must be null and the manifest must
        # spell out that there is no comparison with A, rather than leaving a
        # dangling "portfolio_a" that was never run.
        with patch.dict(os.environ, {
            'RUN_DURATION_SEC': '1800', 'ARMS': 'portfolio_s1,portfolio_s0',
            'ALLOW_SINGLE_ARM': '1',
        }, clear=True):
            manifest = _safe_manifest(_settings(), 'run-8', Path('/tmp/run-8'))
        self.assertEqual(manifest['portfolio_scope'], ['portfolio_s1', 'portfolio_s0'])
        self.assertIsNone(manifest['ab_test']['reference_arm'])
        self.assertEqual(manifest['ab_test']['mode'], 'single_arm_smoke')
        self.assertEqual(manifest['ab_test']['reference_note'],
                         'сравнения с A в этом прогоне нет')
        self.assertEqual(manifest['ab_test']['arm_reference']['portfolio_s0'],
                         'portfolio_s1')
        # Both books of a run always share the exit rule.
        self.assertEqual({
            strategy['exit_rule'] for strategy in manifest['strategies'].values()
        }.__len__(), 1)

    def test_funnel_discovers_the_arms_from_the_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        self.assertTrue(funnel['available'])
        self.assertEqual(funnel['candidates'], 6)
        self.assertEqual(funnel['security_pass'], 5)
        self.assertEqual(funnel['entry_state'], 4)
        # No hardcoded A/B list: whatever `*_signal` stages exist are the arms.
        self.assertEqual(funnel['arms'],
                         ['portfolio_a', 'portfolio_b1_narrow', 'portfolio_b2_birth'])
        self.assertEqual(funnel['portfolio_a_signal'],
                         {'birth_out_of_band': 2, 'live_momentum_entry': 2})
        self.assertEqual(funnel['portfolio_b1_narrow_signal'],
                         {'live_momentum_entry': 1, 'momentum_too_late': 1})
        self.assertEqual(funnel['opened'],
                         {'portfolio_a': 2, 'portfolio_b1_narrow': 1,
                          'portfolio_b2_birth': 0})
        self.assertEqual(funnel['entry_skips']['portfolio_b1_narrow'], {'slots_full': 1})
        self.assertEqual(funnel['admitted_by_current_cap'], 2)
        self.assertEqual(funnel['delta_buckets'], {'0.1-1': 2, '4-12': 2})
        # The creator gate is gone: no gate funnel, no gate keys at all.
        self.assertNotIn('gate_status', funnel)
        self.assertNotIn('gate_reasons', funnel)
        self.assertNotIn('a_only', funnel)

    def test_missing_db_is_reported_not_raised(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            funnel = funnel_from_db(Path(tmp) / 'nope.sqlite')
        self.assertFalse(funnel['available'])
        self.assertEqual(funnel['reason'], 'telemetry_db_missing')

    def test_summary_renders_the_four_arms_and_the_paired_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / 'telemetry.sqlite'
            _build_db(db_path)
            funnel = funnel_from_db(db_path)
        manifest = self._manifest()
        manifest.update({'actual_duration_sec': 28_800, 'telemetry': {},
                         'rpc_metrics': {'requests_per_sec': 1.62,
                                         'unrecovered_rate': 0.0,
                                         'unrecovered_failures': 0, 'requests': 3_000},
                         'enrich': {'enabled': True, 'status': 'written', 'rows': 400,
                                    'ok': 380, 'unavailable': 20}})
        funnel['security_fail_skipped_deep'] = 7
        text = _summary_markdown(manifest, funnel, _run7_summary())
        # Every arm gets its own row, with its own band.
        for arm, band in (('A', '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 2.50'),
                          ('B1', '0.10 <= delta <= 1.00'),
                          ('B2', '30.50 <= v_sol_birth <= 31.00'),
                          ('B3', '0.00 <= delta <= 2.50')):
            self.assertIn(band, text, arm)
        self.assertIn('## Армы: каждая отличается от A одной границей', text)
        self.assertIn('delta_max_sol 2.50 -> 1.00', text)
        self.assertIn('v_sol_birth_max 34.50 -> 31.00', text)
        self.assertIn('delta_min_sol 0.10 -> 0.00', text)
        # Paired columns: A is the reference, the others carry a number + CI.
        self.assertIn('парная разница с A', text)
        self.assertIn('эталон', text)
        self.assertIn('[-9.20, 17.70]', text)
        self.assertIn('60/70/10', text)
        # Pre-registered criteria, P0 table and the expectations line.
        self.assertIn('## Критерии решения (записаны ДО запуска)', text)
        self.assertIn('Bonferroni', text)
        self.assertIn('### P0: принят ли прогон', text)
        self.assertIn('security_fail_skipped_deep > 0', text)
        self.assertIn('enrich.csv пишется', text)
        self.assertIn('+3 … +9 ₽', text)
        self.assertIn('−8 … +8 ₽', text)
        # Hourly noise, half-to-half sign, rockets.
        self.assertIn('### Почасовые блоки', text)
        self.assertIn('### Стабильность знака', text)
        self.assertIn('### Учёт ракет', text)
        self.assertIn('6.89 %', text)
        # Never claim profitability, and never hide the median.
        for line in text.splitlines():
            if 'стало прибыльно' in line:
                self.assertIn('не означает', line)
        self.assertIn('медиана сделки −15.20', text)
        self.assertNotIn('shadow', text)

    def test_summary_shows_the_stop_loss_fly_through(self) -> None:
        # Run #6: the SL sits at -10 % but the trade left at a median -12.17 %.
        summary = _run7_summary()
        summary['tick_quality'] = {
            'stop_loss_fills': {'n': 34, 'median': -12.17, 'mean': -12.9,
                                'worst': -23.17},
            'stop_loss_worse_than_minus15': 8,
            'stop_loss_trigger_pct': -10.0,
            'mark_gap_ms': {'n': 900, 'median': 500.0, 'mean': 520.0, 'worst': 3_400.0},
            'rows': 4_000,
        }
        text = _summary_markdown(self._manifest(), {}, summary)
        self.assertIn('### Пролёт стоп-лосса', text)
        line = [ln for ln in text.splitlines() if 'фактический филл' in ln][0]
        self.assertIn('-12.17', line)
        self.assertIn('-23.17', line)
        self.assertIn('хуже −15 %: 8 из 34', text)
        self.assertIn('шаг между метками', text)

    def test_summary_reports_p0_failures_without_spin(self) -> None:
        manifest = self._manifest()
        manifest.update({
            'actual_duration_sec': 900,
            'early_stop_reason': 'rpc_unrecovered_rate (60/4000 = 1.50%)',
            'telemetry': {'dropped_events': 2, 'write_errors': 1,
                          'writer_error': 'ProgrammingError'},
            'rpc_metrics': {'requests_per_sec': 2.57, 'unrecovered_rate': 0.015,
                            'unrecovered_failures': 60, 'requests': 4_000},
            'enrich': {'enabled': True, 'status': 'empty'},
        })
        text = _summary_markdown(manifest, {}, _run7_summary())
        self.assertIn('досрочная остановка', text)
        self.assertIn('rpc_unrecovered_rate', text)
        for cell in ('dropped_events = 0', 'write_errors = 0',
                     'requests_per_sec ≤ 1.8', 'enrich.csv пишется'):
            line = [ln for ln in text.splitlines() if ln.startswith(f'| {cell}')][0]
            self.assertIn('❌', line, cell)
        # A failed P0 line must not be dressed up as a success.
        self.assertIn('не записалась', text)
        self.assertIn('1.50 %', text)

    def test_summary_renders_bank_top_ups_and_bust(self) -> None:
        summary = _run7_summary()
        summary['portfolio_b2_birth'].update({
            'top_ups': 2, 'capital_injected_rub': 1_400.0,
            'pnl_with_topups': -1_260.0, 'equity_no_topups': 240.0,
            'bust_at_trade': 7, 'bust_at_utc': '2026-10-22T10:00:00Z',
        })
        text = _summary_markdown({'run_id': 'run-7'}, {}, summary)
        self.assertIn('### Банк 1 500 ₽', text)
        row = [ln for ln in text.splitlines() if '№7' in ln][0]
        self.assertIn('| 2 | 1400.00 | -1260.00 | 240.00 | №7 |', row)
        self.assertIn('не умирал', text)
        self.assertIn('Пополнение — только бухгалтерия', text)

    def test_summary_tolerates_scalar_top_level_summary_keys(self) -> None:
        # A real ab_summary.json mixes books with scalars (run_id, duration,
        # fee_model); iterating it blindly crashed the run's last artefact.
        summary = {
            'run_id': 'run-7', 'duration_sec': 28_800.0, 'fee_model': 'x',
            'profiles': {'portfolio_a': {'arm_id': 'A'},
                         'portfolio_b1_narrow': {'arm_id': 'B1'}},
            'portfolio_a': {'stats': {'n': 1, 'mean': -1.0, 'median': -1.0,
                                      'sd': 0.0, 'ci95_mean': None, 'sum': -1.0}},
            'portfolio_b1_narrow': {'stats': {'n': 0}},
            'early_stop_reason': None,
        }
        text = _summary_markdown({'run_id': 'run-7'}, {}, summary)
        self.assertIn('| A (`portfolio_a`) | 1 |', text)
        self.assertIn('| B1 (`portfolio_b1_narrow`) | 0 |', text)

    def test_duration_is_a_parameter_not_a_constant(self) -> None:
        from run_local_session import build_parser

        parser = build_parser()
        self.assertEqual(parser.parse_args(['--duration-sec', '10800']).duration, 10_800)
        self.assertEqual(parser.parse_args(['--duration', '10800']).duration, 10_800)
        # The default is a default, not a hard-coded run length (run #8: 30 min).
        self.assertEqual(parser.parse_args([]).duration, 1_800)

    def test_summary_surfaces_lost_telemetry_rows(self) -> None:
        manifest = self._manifest()
        manifest['telemetry'] = {
            'dropped_events': 0, 'write_errors': 1, 'writer_error': 'ProgrammingError',
        }
        text = _summary_markdown(manifest, {}, _run7_summary())
        self.assertIn('write_errors=1', text)
        self.assertIn('ProgrammingError', text)
        self.assertIn('не записалась', text)

        manifest['telemetry'] = {'dropped_events': 0, 'write_errors': 0,
                                 'writer_error': None}
        text = _summary_markdown(manifest, {}, _run7_summary())
        self.assertIn('write_errors=0', text)
        self.assertNotIn('не записалась', text)

    def test_money_cells_are_two_decimals_and_dashes(self) -> None:
        summary = _run7_summary()
        summary['portfolio_b3_t0']['stats'] = {'n': 0, 'sum': None}
        text = _summary_markdown({'run_id': 'run-7'}, {}, summary)
        row = [
            ln for ln in text.splitlines()
            if ln.startswith('| B3 (`portfolio_b3_t0`)') and ln.count('|') == 12
        ][0]
        cells = [cell.strip() for cell in row.strip('|').split('|')]
        self.assertEqual(cells[1], '0')
        # No trades -> no number, and never a fabricated 0.00.
        self.assertEqual(cells[2], '—')
        a_row = [
            ln for ln in text.splitlines()
            if ln.startswith('| A (`portfolio_a`)') and ln.count('|') == 12
        ][0]
        self.assertIn('248.61', a_row)
        self.assertNotIn('248.610', a_row)

    def test_manifest_declares_run7_and_keeps_the_ladder(self) -> None:
        manifest = self._manifest()
        self.assertEqual(manifest['ab_test']['single_difference'], 'entry_band')
        self.assertEqual(manifest['ab_test']['reference_arm'], 'portfolio_a')
        self.assertEqual(manifest['portfolio_scope'], list(ARM_IDS))
        self.assertEqual(manifest['observations_sec'], [0, 5, 15, 60])
        self.assertEqual(manifest['entry_latency_ms'], 150.0)
        self.assertIn('+8', manifest['ab_test']['decision_rule'])
        self.assertIn('Bonferroni', manifest['ab_test']['decision_rule'])
        self.assertTrue(manifest['enrich']['enabled'])
        self.assertEqual(manifest['enrich']['path'], '/tmp/run-7/exports/enrich.csv')
        # Four strategies, one wallet file each, no shadows and no live gate.
        self.assertEqual(list(manifest['strategies']), manifest['portfolio_scope'])
        for name, strategy in manifest['strategies'].items():
            self.assertFalse(strategy['creator_gate'], name)
            self.assertEqual(strategy['max_slots'], 3)
            self.assertEqual(strategy['position_size_rub'], 500)
            self.assertEqual(strategy['initial_balance_rub'], 1_500)
            self.assertIn('entry_band_label', strategy)
        self.assertFalse(manifest['filters']['creator_gate_in_live_arms'])
        self.assertIn('not used anywhere', manifest['filters']['creator_gate_note'])
        self.assertEqual(len(manifest['wallet_state_files']), len(ARM_IDS))
        # Fix #7's ladder numbers are untouched in every arm.
        for name, strategy in manifest['strategies'].items():
            self.assertEqual(strategy['timeout_before_trailing_sec'], 35, name)
            self.assertEqual(strategy['timeout_after_trailing_sec'], 90)
            self.assertIn('pnl_pct <= -10.0', strategy['stop_loss'])
        self.assertNotIn('time_exit_15s', str(manifest))


class ReportRegressionTests(unittest.TestCase):
    """Defects found while reviewing earlier fixes — locked so they cannot return."""

    def _manifest(self) -> dict:
        with patch.dict(os.environ, {'RUN_DURATION_SEC': '28800',
                                     'ENTRY_ENRICH': '1'}, clear=True):
            return _safe_manifest(_settings(), 'run-7', Path('/tmp/run-7'))

    def test_funnel_lines_do_not_glue_reason_and_count(self) -> None:
        funnel = {'portfolio_a_signal': {'a_reason_that_is_much_longer_than_thirty': 7}}
        text = _summary_markdown(self._manifest(), funnel, _run7_summary())
        for line in text.splitlines():
            if 'a_reason_that_is_much_longer_than_thirty' in line:
                self.assertTrue(line.endswith('7'), line)
                # The count must be separated from the reason by whitespace.
                self.assertEqual(line[-2], ' ', f'glued reason/count: {line!r}')

    def test_summary_never_claims_profitability(self) -> None:
        text = _summary_markdown(self._manifest(), {}, _run7_summary())
        # The only place the phrase may appear is the explicit denial.
        for line in text.splitlines():
            if 'стало прибыльно' in line:
                self.assertIn('не означает', line)
        self.assertIn('Ни одно число здесь не означает', text)

    def test_pnl_is_rounded_not_printed_as_a_raw_float(self) -> None:
        summary = _run7_summary()
        summary['portfolio_a']['stats']['sum'] = -1035.555661
        text = _summary_markdown(self._manifest(), {}, summary)
        self.assertNotIn('-1035.555661', text)
        self.assertIn('-1035.56', text)


if __name__ == '__main__':
    unittest.main()
