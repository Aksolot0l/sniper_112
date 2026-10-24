"""Run #10b §3: the ghost is judged by uncovered points, not by luck.

In 10a nothing was lost — 1032/1032 points for S0 and 152/152 for S2, missed 0,
attempts 0 — and the cover threshold still failed, because the "+10 s" point of
one S2 exit arrived at 11.35 s and three S0 points arrived 1.08-2.08 s late. A
share over 19 exits cannot express that: one late point is 18/19 = 0.947.

The row now asks for every planned point to be written, allows
max(2, 1 % of the plan) uncovered points, and reports the worst lateness. The
median stays at 8 and the minimum at 7 (5 for a smoke).

The nearest-observation tests below are the run #10 §3 guard: the mark nearest
to entry+5 s wins, whoever produced it.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from main_ab import _run9_summary_markdown, _safe_manifest
from portfolio_ab import POST_EXIT_SCHEDULE_SEC
from tests.test_run_report import _settings
from tests.test_portfolio_ab import make_simulator
from tests.test_run9_report import RUN9_ENV, _run9_profiles

FULL_COVERAGE = {f'+{point:g}s': 1.0 for point in (0.5, 1.0, 2.0, 5.0, 10.0)}


def _book(n: int, *, uncovered: int = 0, lateness: float = 0.0,
          marks_min: int = 8, marks_median: float = 8,
          planned: int | None = None, written: int | None = None,
          missed: int = 0, recovered: int = 0) -> dict:
    planned = (n * len(POST_EXIT_SCHEDULE_SEC)) if planned is None else planned
    written = (planned - missed) if written is None else written
    required_total = n * 5
    return {
        'stats': {'n': n, 'sum': 10.0, 'mean': 5.0, 'median': 5.0, 'sd': 1.0,
                  'ci95_mean': [1.0, 9.0], 'sum_without_top2': 0.0},
        'exit_shares': {'fee_cover_n': 1, 'time_exit_n': 1, 'hard_stop_n': 0,
                        'shares': {}, 'ladder_kinds_present': []},
        'latency_chain': {'n': n, 'share_under_200ms': 1.0,
                          'stages': {'t_total_ms': {'median': 230.0}}},
        'round_trip_cost': {'entry_fill_premium_pct_median': 1.4,
                            'round_trip_exact_pct_median': 3.3, 'n_flat_exact': 6},
        'post_exit': {'price_at_5s_trades': n, 'price_at_5s_coverage': 1.0,
                      'price_at_5s_median_distance_sec': 0.13,
                      'pnl_pct_at_5s_median': 1.8, 'pnl_pct_at_5s_mean': 1.7,
                      'exits_followed': n, 'exits_with_marks': n,
                      'marks_per_exit_min': marks_min,
                      'marks_per_exit_median': marks_median,
                      'required_points_total': required_total,
                      'uncovered_points': uncovered,
                      'uncovered_share': (
                          round(uncovered / planned, 4) if planned else 0.0
                      ),
                      'max_lateness_sec': lateness,
                      'lateness_p95_sec': lateness,
                      'points_planned': planned, 'points_written': written,
                      'points_write_ratio': round(written / planned, 4) if planned else None,
                      'missed_points_total': missed,
                      'points_recovered_by_retry': recovered,
                      'attempts_total': recovered,
                      'mean_pnl_vs_exit_pct_at': {'+1s': 0.4}},
        'reject_counts': {'name_copies': 0, 'birth_band': 5, 'delta_min': 1,
                          'delta_max': 2, 'slots_full': 3, 'no_price': 0},
        'selection_decomposition': {'cells': {}},
        'minute_pairs_vs_reference': None,
    }


def _report(duration: int, **books) -> str:
    env = {**RUN9_ENV, 'RUN_DURATION_SEC': str(duration)}
    with patch.dict(os.environ, env, clear=True):
        manifest = _safe_manifest(_settings(), 'run-10', Path('/tmp/run-10'))
    manifest['actual_duration_sec'] = duration
    manifest['telemetry'] = {'dropped_events': 0, 'write_errors': 0}
    summary = {
        **books,
        'profiles': {'portfolio_s0': {'arm_id': 'S0'},
                     'portfolio_s2': {'arm_id': 'S2'}},
        'mark_poll': {'mark_poll_sec': 0.25, 'mark_poll_adapted': False,
                      'rps_limit_10min': 2.5},
        'drain': {'left_open': 0, 'waited_sec': 0.0},
    }
    return _run9_summary_markdown(manifest, {'delta_buckets': {}}, summary)


def _row(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if line.startswith(prefix):
            return line
    raise AssertionError(f'row {prefix!r} not found')


class GhostThresholdTests(unittest.TestCase):
    """A late point must be visible and bounded, not fatal."""

    def test_late_points_inside_the_budget_pass(self) -> None:
        # Exactly the 10a shape: everything written, two points late.
        text = _report(
            3_600,
            portfolio_s0=_book(129, uncovered=3, lateness=2.08),
            portfolio_s2=_book(19, uncovered=1, lateness=1.35),
        )
        row = _row(text, '| призрак: точки записаны/план')
        self.assertIn('S0: 1032/1032', row)
        self.assertIn('недобрано 3', row)
        self.assertIn('✅', row)

    def test_more_uncovered_points_than_the_budget_fail(self) -> None:
        # 1 560 exits plan 12 480 points, so 1 % is 124.8: 124 late points are
        # inside the budget, 125 are not.
        text = _report(
            43_200,
            portfolio_s0=_book(1_560, uncovered=125),
            portfolio_s2=_book(216),
        )
        self.assertIn('❌', _row(text, '| призрак: точки записаны/план'))
        text = _report(
            43_200,
            portfolio_s0=_book(1_560, uncovered=124),
            portfolio_s2=_book(216),
        )
        self.assertIn('✅', _row(text, '| призрак: точки записаны/план'))

    def test_a_small_book_gets_the_floor_of_two_points(self) -> None:
        # 19 exits plan 152 points, so 1 % is 1.52 and the floor of two applies:
        # one late point (what 10a actually had) passes, three do not.
        text = _report(3_600,
                       portfolio_s0=_book(130, uncovered=10),
                       portfolio_s2=_book(19, uncovered=2))
        self.assertIn('✅', _row(text, '| призрак: точки записаны/план'))
        text = _report(3_600,
                       portfolio_s0=_book(130, uncovered=10),
                       portfolio_s2=_book(19, uncovered=3))
        self.assertIn('❌', _row(text, '| призрак: точки записаны/план'))

    def test_a_plan_that_was_not_written_in_full_fails(self) -> None:
        text = _report(3_600,
                       portfolio_s0=_book(130, missed=4),
                       portfolio_s2=_book(18))
        row = _row(text, '| призрак: точки записаны/план')
        self.assertIn('1036/1040', row)
        self.assertIn('❌', row)

    def test_the_hour_stage_keeps_median_eight_and_minimum_seven(self) -> None:
        text = _report(3_600,
                       portfolio_s0=_book(130, marks_min=7),
                       portfolio_s2=_book(18, marks_min=7, marks_median=7.5))
        self.assertIn('❌', _row(text, '| призрак: точки записаны/план'))
        text = _report(3_600,
                       portfolio_s0=_book(130, marks_min=7),
                       portfolio_s2=_book(18, marks_min=7))
        self.assertIn('✅', _row(text, '| призрак: точки записаны/план'))

    def test_a_smoke_keeps_the_light_threshold(self) -> None:
        row = _row(_report(1_800,
                           portfolio_s0=_book(10, marks_min=6),
                           portfolio_s2=_book(3, marks_min=6)),
                   '| призрак: точки записаны/план')
        self.assertIn('мин ≥ 5', row)
        self.assertIn('✅', row)


class GhostPointAccountingTests(unittest.TestCase):
    """A lost or late point has to be readable from the report, not guessed."""

    def test_the_line_shows_points_uncovered_and_worst_lateness(self) -> None:
        text = _report(
            43_200,
            portfolio_s0=_book(1_560, planned=12_480, written=12_470, missed=10,
                               recovered=4, uncovered=10, marks_min=7),
            portfolio_s2=_book(216, missed=1),
        )
        row = _row(text, '| призрак: точки записаны/план')
        self.assertIn('S0: 12470/12480', row)
        self.assertIn('недобрано 10', row)
        self.assertIn('0.08', row)
        self.assertIn('❌', row)  # 12 470 of 12 480 written

    def test_a_book_without_ghosts_fails_the_row_instead_of_passing_silently(self) -> None:
        text = _report(3_600,
                       portfolio_s0=_book(130, planned=0, written=0),
                       portfolio_s2=_book(18))
        row = _row(text, '| призрак: точки записаны/план')
        self.assertIn('❌', row)


class GhostLatenessAccountingTests(unittest.TestCase):
    """The counters the row reads must come from the rows, not from a guess."""

    def _simulator(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, RUN9_ENV, clear=True):
                return make_simulator(root, profiles=_run9_profiles())

    @staticmethod
    def _mark(sec_since_exit: float, exit_epoch: float = 1_000.0) -> dict:
        return {
            'portfolio': 'portfolio_s0', 'mint': 'mint-late',
            'exit_at_epoch': exit_epoch, 'sec_since_exit': sec_since_exit,
            'price_usd': 1e-5, 'pnl_vs_entry_pct': 1.0, 'pnl_vs_exit_pct': 0.5,
        }

    def test_a_late_last_point_is_counted_as_uncovered_not_as_lost(self) -> None:
        simulator = self._simulator()
        # Eight points written, but the "+10 s" one arrived at 11.35 s — the
        # exact shape of 10a's S2 exit.
        simulator.post_exit_rows = [
            self._mark(sec) for sec in (0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 7.0, 11.35)
        ]
        simulator.post_exit_planned = {'portfolio_s0': 8}
        simulator.post_exit_written = {'portfolio_s0': 8}
        simulator.post_exit_started = {'portfolio_s0': 1}
        stats = simulator._post_exit_stats()['per_book']['portfolio_s0']
        self.assertEqual(stats['points_written'], stats['points_planned'])
        self.assertEqual(stats['uncovered_points'], 1)
        self.assertEqual(stats['required_points_total'], 5)
        self.assertAlmostEqual(stats['max_lateness_sec'], 1.35, places=6)
        self.assertAlmostEqual(stats['uncovered_share'], 1 / 8, places=4)
        self.assertEqual(stats['point_lateness_sec']['+10s']['max'], 1.35)
        self.assertEqual(stats['point_lateness_sec']['+5s']['max'], 0.0)

    def test_points_inside_the_tolerance_are_covered(self) -> None:
        simulator = self._simulator()
        simulator.post_exit_rows = [
            self._mark(sec) for sec in (0.55, 1.1, 2.2, 5.4, 10.7)
        ]
        simulator.post_exit_planned = {'portfolio_s0': 8}
        simulator.post_exit_written = {'portfolio_s0': 8}
        simulator.post_exit_started = {'portfolio_s0': 1}
        stats = simulator._post_exit_stats()['per_book']['portfolio_s0']
        self.assertEqual(stats['uncovered_points'], 0)
        self.assertEqual(stats['max_lateness_sec'], 0.0)
        self.assertAlmostEqual(stats['lateness_p95_sec'], 0.7, places=6)

    def test_a_missing_point_counts_as_uncovered(self) -> None:
        simulator = self._simulator()
        simulator.post_exit_rows = [self._mark(sec) for sec in (0.5, 1.0, 2.0, 5.0)]
        simulator.post_exit_planned = {'portfolio_s0': 8}
        simulator.post_exit_written = {'portfolio_s0': 4}
        simulator.post_exit_started = {'portfolio_s0': 1}
        stats = simulator._post_exit_stats()['per_book']['portfolio_s0']
        self.assertEqual(stats['uncovered_points'], 1)
        # The nearest mark to +10 s is the +5 s one, so the point reads as 5 s
        # late rather than as a missing row — the reader sees how blind it is.
        self.assertEqual(stats['point_lateness_sec']['+10s']['max'], 5.0)
        self.assertEqual(stats['max_lateness_sec'], 5.0)


class NearestObservationTests(unittest.TestCase):
    """§3: the mark nearest to entry+5 s wins, whoever produced it."""

    def _simulator(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, RUN9_ENV, clear=True):
                return make_simulator(root, profiles=_run9_profiles())

    def _ghost(self, simulator, marks: list[tuple[float, float]]) -> None:
        simulator._ghosts = [{
            'portfolio': 'portfolio_s0', 'mint': 'mint-5s', 'curve': 'curve-5s',
            'exit_at_epoch': 1_000.0, 'entry_at_epoch': 1_000.0,
            'entry_mid_price_usd': 1e-5, 'exit_mid_price_usd': 1e-5,
            'until_epoch': 2_000.0, 'schedule_index': 0, 'attempts': 0,
            'missed': 0, 'trade': None, 'marks': 0,
            '_rows': [
                {'price_usd': price, '_at_epoch': at} for at, price in marks
            ],
        }]

    def _tick(self, simulator, at: float, price: float) -> None:
        simulator.ticks = [{
            'portfolio': 'portfolio_s0', 'mint': 'mint-5s', 'event': 'mark',
            'price_usd': price, '_at_epoch': at, 't_sec': at - 1_000.0,
        }]

    def test_a_closer_tick_wins_over_a_later_ghost_mark(self) -> None:
        simulator = self._simulator()
        self._tick(simulator, 1_005.03, 2e-5)
        self._ghost(simulator, [(1_005.60, 9e-5)])
        price, source, distance = simulator._price_at_offset(
            'portfolio_s0', 'mint-5s', 1_000.0, 5.0)
        self.assertEqual(source, 'tick')
        self.assertAlmostEqual(distance, 0.03, places=6)
        self.assertAlmostEqual(price, 2e-5)

    def test_a_closer_ghost_mark_wins_over_a_later_tick(self) -> None:
        simulator = self._simulator()
        self._tick(simulator, 1_005.60, 9e-5)
        self._ghost(simulator, [(1_005.05, 2e-5)])
        price, source, distance = simulator._price_at_offset(
            'portfolio_s0', 'mint-5s', 1_000.0, 5.0)
        self.assertEqual(source, 'post_exit')
        self.assertAlmostEqual(distance, 0.05, places=6)
        self.assertAlmostEqual(price, 2e-5)

    def test_nothing_inside_the_tolerance_returns_nothing(self) -> None:
        simulator = self._simulator()
        self._tick(simulator, 1_005.90, 2e-5)
        self._ghost(simulator, [(1_004.00, 9e-5)])
        self.assertIsNone(simulator._price_at_offset(
            'portfolio_s0', 'mint-5s', 1_000.0, 5.0))

    def test_the_ghost_never_overwrites_a_closer_stored_tick(self) -> None:
        simulator = self._simulator()
        self._ghost(simulator, [(1_005.60, 9e-5)])
        trade = {'price_at_5s': 2e-5, 'price_at_5s_source': 'tick',
                 'price_at_5s_distance_sec': 0.03}
        simulator._ghosts[0]['trade'] = trade
        simulator._record_post_exit_mark(simulator._ghosts[0], 1_005.60, 9e-5)
        self.assertEqual(trade['price_at_5s_source'], 'tick')
        self.assertAlmostEqual(trade['price_at_5s'], 2e-5)
        # The mark itself is still written to post_exit.csv.
        self.assertEqual(len(simulator.post_exit_rows), 1)

    def test_a_closer_ghost_mark_does_overwrite_it(self) -> None:
        simulator = self._simulator()
        self._ghost(simulator, [(1_005.05, 9e-5)])
        trade = {'price_at_5s': 2e-5, 'price_at_5s_source': 'tick',
                 'price_at_5s_distance_sec': 0.40}
        simulator._ghosts[0]['trade'] = trade
        simulator._record_post_exit_mark(simulator._ghosts[0], 1_005.05, 9e-5)
        self.assertEqual(trade['price_at_5s_source'], 'post_exit')
        self.assertAlmostEqual(trade['price_at_5s_distance_sec'], 0.05, places=6)


class GraceTests(unittest.TestCase):
    def test_the_ghost_outlives_its_last_point_by_two_seconds(self) -> None:
        from portfolio_ab import POST_EXIT_SCHEDULE_GRACE_SEC, POST_EXIT_WINDOW_SEC
        self.assertGreaterEqual(POST_EXIT_SCHEDULE_GRACE_SEC, 2.0)
        # The 10 s point is the last one; two seconds of slack cover a late
        # cycle caused by latency or a 429 backoff.
        self.assertGreaterEqual(POST_EXIT_SCHEDULE_GRACE_SEC, 2.0 * 0.25)

    def test_the_window_reaches_the_last_scheduled_point(self) -> None:
        from portfolio_ab import (POST_EXIT_SCHEDULE_SEC, POST_EXIT_WINDOW_SEC)
        self.assertGreaterEqual(
            POST_EXIT_WINDOW_SEC + 2.0, max(POST_EXIT_SCHEDULE_SEC))


if __name__ == '__main__':
    unittest.main()
