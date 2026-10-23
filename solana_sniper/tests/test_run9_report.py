"""Run #9: the S0/S2 selection run and the one pre-agreed money stop."""
from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import json
import sqlite3

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
from telemetry import _SCHEMA, funnel_from_db
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
                              'price_at_5s_median_distance_sec': 0.12,
                              'exits_followed': 2, 'exits_with_marks': 2,
                              'marks_per_exit_min': 8, 'marks_per_exit_median': 8,
                              'required_point_coverage': {
                                  '+0.5s': 1.0, '+1s': 1.0, '+2s': 1.0,
                                  '+5s': 1.0, '+10s': 1.0},
                              'points_planned': 16, 'points_written': 16,
                              'points_write_ratio': 1.0, 'missed_points_total': 0,
                              'points_recovered_by_retry': 1, 'attempts_total': 1,
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
                              'price_at_5s_median_distance_sec': 0.6,
                              'exits_followed': 1, 'exits_with_marks': 1,
                              'marks_per_exit_min': 8, 'marks_per_exit_median': 8,
                              'required_point_coverage': {
                                  '+0.5s': 1.0, '+1s': 1.0, '+2s': 1.0,
                                  '+5s': 1.0, '+10s': 1.0},
                              'points_planned': 8, 'points_written': 8,
                              'points_write_ratio': 1.0, 'missed_points_total': 0,
                              'points_recovered_by_retry': 0, 'attempts_total': 0,
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
        # The stage-10b gate is spelled out on a 30-minute smoke.
        self.assertIn('ворота этапа 10b', text.lower())
        self.assertIn('этап 10b запускается', text.lower())
        # And it never claims money.
        self.assertIn('не про деньги', text)
        self.assertNotIn('стало прибыльно', text.replace('«стало прибыльно»', ''))

    def test_long_run_switches_the_acceptance_table_to_b(self) -> None:
        text = self._report(duration=43_200)
        self.assertIn('длительность ≥ 43 000 c', text)
        self.assertIn('неисправленные ошибки RPC ≤ 1 %', text)
        self.assertNotIn('технический смоук на 30 минут', text)

    def test_the_one_hour_stage_gets_its_own_acceptance(self) -> None:
        # Run #10a is a technical hour: 3 500 s, 2.2 rps window, 429 budget,
        # 8 ghost marks, S0 >= 80 and S2 >= 8 trades — and money is not read.
        text = self._report(duration=3_600)
        self.assertIn('этап: **10a (1 ч)**', text)
        self.assertIn('длительность ≥ 3 500 c', text)
        self.assertIn('худшее 10-мин окно ≤ 2.2 rps', text)
        self.assertIn('429 ≤ 0.5 % запросов', text)
        self.assertIn('post_exit.csv: покрытие точек ≥ 0.97', text)
        self.assertIn('меток на выход мин ≥ 7, медиана = 8', text)
        self.assertIn('S0 ≥ 80, S2 ≥ 8', text)
        self.assertIn('точки призрака записано/план', text)
        self.assertIn('неисправленные ошибки RPC ≤ 1 %', text)
        self.assertNotIn('технический смоук на 30 минут', text)

    def test_the_twelve_hour_stage_demands_the_money_sample(self) -> None:
        text = self._report(duration=43_200)
        self.assertIn('этап: **10b (12 ч)**', text)
        self.assertIn('S0 ≥ 400, S2 ≥ 80', text)
        self.assertIn('≥ +10 ₽', text)
        # The 12-hour page must not describe itself as a technical smoke.
        self.assertNotIn('технический смоук', text)
        self.assertIn('CI95 средней S2 и разница', text)

    def test_a_smoke_keeps_its_lighter_thresholds(self) -> None:
        text = self._report(duration=1_800)
        self.assertIn('этап: **смоук**', text)
        self.assertIn('длительность ≥ 1 790 c', text)
        self.assertIn('худшее 10-мин окно ≤ 2.5 rps', text)

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


class RoundTripCostExactTests(unittest.TestCase):
    """§3/§4: the cost of the round trip on trades whose price stood still."""

    def _book(self, root: str):
        with patch.dict(os.environ, RUN9_ENV, clear=True):
            simulator = make_simulator(root, profiles=_run9_profiles())
        return simulator, simulator.books['portfolio_s0']

    def test_only_flat_trades_produce_a_cost_and_a_median(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            simulator, book = self._book(root)
            book.closed_trades = [
                {'mfe_pct': 0.4, 'mae_pct': -0.3, 'pnl_pct': -3.2,
                 'round_trip_cost_pct': 3.2},
                {'mfe_pct': 0.2, 'mae_pct': -0.1, 'pnl_pct': -3.6,
                 'round_trip_cost_pct': 3.6},
                # The price moved: this trade cannot measure the cost.
                {'mfe_pct': 5.0, 'mae_pct': -1.0, 'pnl_pct': 1.4,
                 'round_trip_cost_pct': None},
            ]
            block = simulator._round_trip_cost(book)
        self.assertEqual(block['n_flat_exact'], 2)
        self.assertAlmostEqual(block['round_trip_exact_pct_median'], 3.4, places=6)
        self.assertEqual(block['exact_definition'],
                         'exit mid / entry mid - 1 within 0.01 %')

    def test_no_flat_trade_records_no_median_rather_than_zero(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            simulator, book = self._book(root)
            book.closed_trades = [
                {'mfe_pct': 5.0, 'mae_pct': -1.0, 'pnl_pct': 1.4,
                 'round_trip_cost_pct': None},
            ]
            block = simulator._round_trip_cost(book)
        self.assertEqual(block['n_flat_exact'], 0)
        self.assertIsNone(block['round_trip_exact_pct_median'])


class GhostDrainTests(unittest.IsolatedAsyncioTestCase):
    """An exit in the last seconds of the run must still reach +10 s."""

    async def test_an_expired_ghost_costs_no_wait(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, RUN9_ENV, clear=True):
                simulator = make_simulator(root, profiles=_run9_profiles())
            simulator.mark_poll_sec = 0.01
            simulator._ghosts = [{'until_epoch': time.time() - 1.0}]
            left = await simulator.drain_post_exit_ghosts()
            self.assertEqual(left, 0)
            self.assertLess(simulator.ghost_drain_waited_sec, 1.0)

    async def test_a_running_ghost_is_waited_for_and_then_reported(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, RUN9_ENV, clear=True):
                simulator = make_simulator(root, profiles=_run9_profiles())
            simulator.mark_poll_sec = 0.01
            simulator._ghosts = [{'until_epoch': time.time() + 0.05}]
            left = await simulator.drain_post_exit_ghosts(max_wait_sec=0.3)
            self.assertEqual(left, 0)
            self.assertGreaterEqual(simulator.ghost_drain_waited_sec, 0.0)


class FunnelChainArmTests(unittest.TestCase):
    """The funnel chain used to name book A, which run #9 does not have."""

    def _db(self, root: Path, arms: list[str]) -> Path:
        path = root / 'telemetry.sqlite'
        with sqlite3.connect(path) as db:
            for statement in _SCHEMA:
                db.execute(statement)
            epoch = time.time()
            for index, arm in enumerate(arms):
                for stage, decision, reasons in (
                    (f'{arm}_signal', 'pass', ['live_momentum_entry']),
                    (f'{arm}_entry', 'opened', ['live_momentum_entry']),
                ):
                    db.execute(
                        'INSERT INTO decisions (event_id, mint, source, stage, '
                        'decision, reasons_json, values_json, occurred_at_utc) '
                        'VALUES (?,?,?,?,?,?,?,?)',
                        (f'{stage}-{index}', f'MINT{index}', 'Pump.fun', stage,
                         decision, json.dumps(reasons), '{}',
                         '2026-10-23T00:00:00Z'),
                    )
        return path

    def test_chain_follows_the_books_the_run_actually_had(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = self._db(Path(root), ['portfolio_s0', 'portfolio_s2'])
            report = funnel_from_db(path)
        self.assertTrue(report['available'])
        self.assertEqual(report['arms'], ['portfolio_s0', 'portfolio_s2'])
        self.assertEqual(report['chain_arm'], 'portfolio_s0')
        self.assertIn('opened=1', report['chain'])
        # The per-book counts are keyed by the real books, not by A.
        self.assertEqual(report['opened'], {'portfolio_s0': 1, 'portfolio_s2': 1})


class SummaryPostExitTests(unittest.TestCase):
    """`summary()[book]['post_exit']` must exist for every book."""

    def test_every_book_carries_its_own_post_exit_block(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, RUN9_ENV, clear=True):
                simulator = make_simulator(root, profiles=_run9_profiles())
            summary = simulator.summary()
            self.assertEqual(summary['portfolio_scope'], ['portfolio_s0', 'portfolio_s2'])
            for name in summary['portfolio_scope']:
                block = summary[name]
                self.assertIn('post_exit', block)
                self.assertIsInstance(block['post_exit'], dict)
                self.assertIn('price_at_5s_trades', block['post_exit'])
