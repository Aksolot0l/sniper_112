"""Run #9: the S0/S2 selection run and the one pre-agreed money stop."""
from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from main_ab import (
    _is_run8,
    _is_run9,
    _run9_summary_markdown,
    _safe_manifest,
    _summary_markdown,
)
from portfolio_ab import (
    minute_pairs,
    profiles_from_settings,
    selection_decomposition,
)
from tests.test_run_report import _settings
from tests.test_portfolio_ab import make_simulator

RUN9_ENV = {
    'ARMS': 'portfolio_s0,portfolio_s2', 'ALLOW_SINGLE_ARM': '1',
    'RUN_DURATION_SEC': '1800',
    'ENTRY_BIRTH_MIN_S0': '30.50', 'MAX_NAME_COPIES_S0': 'off',
    'ENTRY_BIRTH_MIN_S2': '31.96', 'MAX_NAME_COPIES_S2': '0',
    'ENTRY_DELTA_MIN_SOL': '0.00', 'ENTRY_DELTA_MAX_SOL': '1.00',
    'ENTRY_BIRTH_MAX_SOL': '34.50', 'EXIT_MODE': 'fee_cover',
    'EXIT_FEE_COVER_PCT': '3.5', 'EXIT_TIME_SEC': '5.0',
    'EXIT_HARD_STOP_PCT': '10.0', 'MARK_POLL_SEC': '0.25',
    'ENTRY_LATENCY_MS': '0', 'POSITION_SIZE_RUB': '500',
    'MAX_POSITIONS': '3', 'INITIAL_BALANCE_RUB': '1500',
}


def _run9_profiles():
    with patch.dict(os.environ, RUN9_ENV, clear=True):
        return profiles_from_settings(object())


def _trade(mint: str, pnl: float, *, opened: float, dev: float = 2.5,
           copies: int = 0, reason: str = 'Fee cover (+3.5%)') -> dict:
    return {
        'mint': mint, 'pnl_rub': pnl,
        'pnl_pct': pnl / 5.0,
        'opened_at_epoch': opened,
        'dev_buy_sol': dev,
        'name_copies_120s': copies,
        'reason': reason,
    }


class RunNineManifestTests(unittest.TestCase):
    def test_manifest_names_both_books_and_their_selection(self) -> None:
        with patch.dict(os.environ, RUN9_ENV, clear=True):
            manifest = _safe_manifest(_settings(), 'run-9', Path('/tmp/run-9'))
        self.assertEqual(manifest['run_number'], 'run9')
        self.assertEqual(manifest['portfolio_scope'], ['portfolio_s0', 'portfolio_s2'])
        self.assertEqual(manifest['exit_mode'], 'fee_cover')
        params = manifest['entry_parameters']
        self.assertEqual(params['portfolio_s0']['v_sol_birth_min'], 30.50)
        self.assertIsNone(params['portfolio_s0']['max_name_copies_120s'])
        self.assertEqual(params['portfolio_s2']['v_sol_birth_min'], 31.96)
        self.assertEqual(params['portfolio_s2']['max_name_copies_120s'], 0)
        # Both books carry the same exit rule, verbatim.
        self.assertEqual({
            strategy['exit_rule'] for strategy in manifest['strategies'].values()
        }.__len__(), 1)
        self.assertIn('31.96', manifest['strategies']['portfolio_s2']['entry_band_label'])
        self.assertIn('30.50', manifest['strategies']['portfolio_s0']['entry_band_label'])
        # The pre-agreed effect rule and the one money stop are declared.
        self.assertIn('CI95', manifest['run9_effect_rule'])
        self.assertIn('negative_edge_S2', manifest['run9_early_stop'])

    def test_only_a_run9_manifest_gets_the_run9_page(self) -> None:
        run9 = {'portfolio_scope': ['portfolio_s0', 'portfolio_s2'],
                'entry_parameters': {'portfolio_s2': {}}}
        run8 = {'portfolio_scope': ['portfolio_s1', 'portfolio_s0'],
                'entry_parameters': {'portfolio_s0': {}}, 'exit_mode': 'fee_cover'}
        run7 = {'portfolio_scope': ['portfolio_a']}
        self.assertTrue(_is_run9(run9))
        self.assertFalse(_is_run8(run9))
        self.assertFalse(_is_run9(run8))
        self.assertTrue(_is_run8(run8))
        self.assertFalse(_is_run9(run7))


class RunNineReportTests(unittest.TestCase):
    def _report(self, *, duration: int = 1800) -> str:
        with patch.dict(os.environ, {**RUN9_ENV,
                                     'RUN_DURATION_SEC': str(duration)}, clear=True):
            manifest = _safe_manifest(_settings(), 'run-9', Path('/tmp/run-9'))
        manifest['actual_duration_sec'] = duration
        manifest['telemetry'] = {'dropped_events': 0, 'write_errors': 0}
        summary = {
            'portfolio_s0': {
                'stats': {'n': 2, 'sum': 10.0, 'mean': 5.0, 'median': 5.0,
                          'sd': 1.0, 'ci95_mean': [1.0, 9.0],
                          'sum_without_top2': 0.0},
                'exit_shares': {'fee_cover_n': 1, 'time_exit_n': 1,
                                'hard_stop_n': 0, 'shares': {}, 'ladder_kinds_present': []},
                'latency_chain': {'n': 2, 'share_under_200ms': 1.0,
                                  'stages': {'t_total_ms': {'median': 230.0}}},
                'round_trip_cost': {'entry_fill_premium_pct_median': 1.4,
                                    'round_trip_exact_pct_median': 3.3, 'n_flat_exact': 1},
                'post_exit': {'price_at_5s_trades': 2, 'price_at_5s_coverage': 1.0,
                              'pnl_pct_at_5s_median': 1.8, 'pnl_pct_at_5s_mean': 1.7,
                              'exits_followed': 2, 'marks_per_exit_min': 30,
                              'marks_per_exit_median': 40,
                              'required_point_coverage': {'+1s': 1.0},
                              'mean_pnl_vs_exit_pct_at': {'+1s': 0.4}},
                'reject_counts': {'name_copies': 0, 'birth_band': 5, 'delta_min': 1,
                                  'delta_max': 2, 'slots_full': 3, 'no_price': 0},
                'selection_decomposition': selection_decomposition([
                    _trade('m1', 10.0, opened=1_000.0),
                    _trade('m2', 0.0, opened=1_060.0, copies=1, reason='Time exit (5.0s)'),
                ]),
                'minute_pairs_vs_reference': None,
            },
            'portfolio_s2': {
                'stats': {'n': 1, 'sum': -5.0, 'mean': -5.0, 'median': -5.0,
                          'sd': None, 'ci95_mean': None, 'sum_without_top2': -5.0},
                'exit_shares': {'fee_cover_n': 0, 'time_exit_n': 1, 'hard_stop_n': 0,
                                'shares': {}, 'ladder_kinds_present': []},
                'latency_chain': {'n': 1, 'share_under_200ms': 1.0,
                                  'stages': {'t_total_ms': {'median': 240.0}}},
                'round_trip_cost': {'entry_fill_premium_pct_median': 0.0,
                                    'round_trip_exact_pct_median': 3.1, 'n_flat_exact': 1},
                'post_exit': {'price_at_5s_trades': 1, 'price_at_5s_coverage': 1.0,
                              'pnl_pct_at_5s_median': -2.0, 'pnl_pct_at_5s_mean': -2.0,
                              'exits_followed': 1, 'marks_per_exit_min': 30,
                              'marks_per_exit_median': 35,
                              'required_point_coverage': {'+1s': 1.0},
                              'mean_pnl_vs_exit_pct_at': {'+1s': -0.2}},
                'reject_counts': {'name_copies': 4, 'birth_band': 9, 'delta_min': 1,
                                  'delta_max': 2, 'slots_full': 1, 'no_price': 0},
                'selection_decomposition': selection_decomposition([
                    _trade('m1', -5.0, opened=1_000.0, reason='Time exit (5.0s)'),
                ]),
                'minute_pairs_vs_reference': minute_pairs(
                    [_trade('m1', 10.0, opened=1_000.0),
                     _trade('m9', 3.0, opened=1_200.0)],
                    [_trade('m1', -5.0, opened=1_000.0),
                     _trade('m2', 1.0, opened=1_260.0)],
                ),
            },
            'profiles': {'portfolio_s0': {'arm_id': 'S0'},
                         'portfolio_s2': {'arm_id': 'S2'}},
            'mark_poll': {'mark_poll_sec': 0.25, 'mark_poll_adapted': False,
                          'rps_limit_10min': 2.5},
            'drain': {'left_open': 0, 'waited_sec': 0.0},
        }
        return _run9_summary_markdown(manifest, {'delta_buckets': {'0.1-1': 2}}, summary)

    def test_report_answers_the_run9_questions(self) -> None:
        text = self._report()
        for block in ('## Схема опыта', '## Результат по книгам',
                      '## Латентность входа и стоимость круга',
                      '## «Призрак»: что было после выхода',
                      '## Воронка отказов', '## S0 против S2: парно по минутам',
                      '## Разложение вкладов', '## Приёмка прогона #9',
                      '## Чего этот прогон не говорит'):
            self.assertIn(block, text)
        # The bands and the copies rule are printed, not implied.
        self.assertIn('31.96–34.50', text)
        self.assertIn('30.50–34.50', text)
        self.assertIn('name_copies', text)
        # The §5(4) block carries a real number, not a placeholder.
        self.assertIn('| S2 | 2 / 2 | 1 |', text)
        self.assertIn('доля минут', text)
        # The B gate is spelled out on a 30-minute smoke.
        self.assertIn('ворота прогона b', text.lower())
        self.assertIn('прогон b запускается', text.lower())
        # And it never claims money.
        self.assertIn('не про деньги', text)
        self.assertNotIn('стало прибыльно', text.replace('«стало прибыльно»', ''))

    def test_long_run_switches_the_acceptance_table_to_b(self) -> None:
        text = self._report(duration=43_200)
        self.assertIn('длительность ≥ 43 000 c', text)
        self.assertIn('неисправленные ошибки RPC ≤ 1 %', text)
        self.assertNotIn('технический смоук на 30 минут', text)

    def test_summary_dispatch_prefers_the_run9_page(self) -> None:
        with patch.dict(os.environ, RUN9_ENV, clear=True):
            manifest = _safe_manifest(_settings(), 'run-9', Path('/tmp/run-9'))
        text = _summary_markdown(manifest, {}, {'portfolio_s0': {'stats': None}})
        self.assertIn('Приёмка прогона #9', text)


class MinutePairTests(unittest.TestCase):
    def test_minutes_are_paired_and_empty_minutes_are_not_zero(self) -> None:
        reference = [_trade('a', 10.0, opened=1_000.0), _trade('b', 5.0, opened=1_030.0),
                     _trade('c', -4.0, opened=1_100.0)]
        arm = [_trade('a', 20.0, opened=1_000.0), _trade('d', -1.0, opened=1_130.0)]
        result = minute_pairs(reference, arm)
        self.assertEqual(result['minutes_with_trades'], {'reference': 3, 'arm': 2})
        self.assertEqual(result['minutes_paired'], 2)
        # Minutes 16 and 18 are shared; minute 1 is not (1030 // 60 == 17).
        self.assertEqual((result['n_better'], result['n_worse']), (2, 0))
        self.assertAlmostEqual(result['share_arm_better'], 1.0)
        # minute 16: ref 10 vs arm 20 -> +10; minute 18: ref -4 vs arm -1 -> +3.
        self.assertAlmostEqual(result['mean_diff_rub'], 6.5)

    def test_no_shared_minutes_is_reported_as_absent(self) -> None:
        result = minute_pairs([_trade('a', 1.0, opened=1_000.0)],
                              [_trade('b', 1.0, opened=5_000.0)])
        self.assertEqual(result['minutes_paired'], 0)
        self.assertIsNone(result['mean_diff_rub'])


class SelectionDecompositionTests(unittest.TestCase):
    def test_the_three_declared_cells(self) -> None:
        result = selection_decomposition([
            _trade('a', 10.0, opened=1.0, dev=3.0, copies=0),
            _trade('b', -2.0, opened=2.0, dev=2.2, copies=2),
            _trade('c', -8.0, opened=3.0, dev=0.4, copies=0),
            _trade('d', 4.0, opened=4.0, dev=4.4, copies=0),
        ])
        cells = result['cells']
        self.assertEqual(cells['dev_2_4.5_copies_0']['n'], 2)
        self.assertAlmostEqual(cells['dev_2_4.5_copies_0']['mean'], 7.0)
        self.assertEqual(cells['dev_2_4.5_copies_pos']['n'], 1)
        self.assertEqual(cells['dev_lt_2']['n'], 1)
        self.assertEqual(cells['dev_2_4.5_copies_pos']['fee_cover_n'], 1)


class NegativeEdgeStopTests(unittest.TestCase):
    """§5: the one pre-agreed money stop, and it only runs when declared."""

    def _simulator(self, pnls: list[float]):
        profiles = _run9_profiles()
        simulator = make_simulator(Path(tempfile.mkdtemp()), profiles=profiles)
        simulator.started_at_epoch = time.time() - 30_000.0
        for name, book in simulator.books.items():
            if name != 'portfolio_s2':
                continue
            for index, pnl in enumerate(pnls):
                book.closed_trades.append(_trade(f'm{index}', pnl, opened=1.0))
        return simulator

    def test_a_negative_ci_stops_the_run_when_the_rule_is_declared(self) -> None:
        simulator = self._simulator([-40.0, -35.0, -50.0, -30.0, -45.0])
        with patch.dict(os.environ, {
            'EARLY_STOP_NEGATIVE_EDGE_AT_SEC': '28800',
            'EARLY_STOP_NEGATIVE_EDGE_BOOK': 'portfolio_s2',
        }, clear=False):
            reason = simulator.negative_edge_reason()
        self.assertIsNotNone(reason)
        self.assertIn('negative_edge_S2', reason)
        self.assertEqual(simulator.stop_policy_state['negative_edge']['n'], 5)

    def test_a_positive_or_undecided_ci_does_not_stop_anything(self) -> None:
        simulator = self._simulator([-40.0, 35.0, -50.0, 30.0, -45.0])
        with patch.dict(os.environ, {
            'EARLY_STOP_NEGATIVE_EDGE_AT_SEC': '28800',
        }, clear=False):
            self.assertIsNone(simulator.negative_edge_reason())
        # Two trades are not a CI; the rule refuses to guess.
        simulator = self._simulator([-40.0])
        with patch.dict(os.environ, {
            'EARLY_STOP_NEGATIVE_EDGE_AT_SEC': '28800',
        }, clear=False):
            self.assertIsNone(simulator.negative_edge_reason())

    def test_without_the_declared_rule_pnl_never_stops_a_run(self) -> None:
        simulator = self._simulator([-40.0, -35.0, -50.0])
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(simulator.negative_edge_reason())
        simulator._maybe_early_stop()
        self.assertIsNone(simulator.early_stop_reason)

    def test_the_rule_waits_for_its_hour(self) -> None:
        simulator = self._simulator([-40.0, -35.0, -50.0])
        simulator.started_at_epoch = time.time() - 60.0
        with patch.dict(os.environ, {
            'EARLY_STOP_NEGATIVE_EDGE_AT_SEC': '28800',
        }, clear=False):
            self.assertIsNone(simulator.negative_edge_reason())
