"""Run #10 addendum §1-§3: reachable ghost thresholds, visible point losses,
and "the nearest observation wins" for price_at_5s.

§1 was arithmetic: 12 hours plans ~14 200 ghost points, so requiring 8 marks on
every single exit would fail the stage whenever 0.007 % of points were lost.
The threshold is now the share of required points covered, with the median at
8 and the minimum at 7.

§2 makes a lost point visible: planned / written / missed / recovered per book.

§3 was a silent bias: `_price_at_offset` returned no distance, so the trade had
no `price_at_5s_distance_sec` and any ghost mark inside the tolerance overwrote
a tick that could be three times closer to entry+5 s.
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


def _book(n: int, *, coverage: float = 1.0, marks_min: int = 8,
          marks_median: float = 8, planned: int | None = None,
          written: int | None = None, missed: int = 0,
          recovered: int = 0) -> dict:
    planned = (n * len(POST_EXIT_SCHEDULE_SEC)) if planned is None else planned
    written = (planned - missed) if written is None else written
    return {
        'stats': {'n': n, 'sum': 10.0, 'mean': 5.0, 'median': 5.0, 'sd': 1.0,
                  'ci95_mean': [1.0, 9.0], 'sum_without_top2': 0.0},
        'exit_shares': {'fee_cover_n': 1, 'time_exit_n': 1, 'hard_stop_n': 0,
                        'shares': {}, 'ladder_kinds_present': []},
        'latency_chain': {'n': n, 'share_under_200ms': 1.0,
                          'stages': {'t_total_ms': {'median': 230.0}}},
        'round_trip_cost': {'entry_fill_premium_pct_median': 1.4,
                            'round_trip_exact_pct_median': 3.3, 'n_flat_exact': 6},
        'post_exit': {**{f'+{p:g}s': coverage for p in (0.5, 1.0, 2.0, 5.0, 10.0)},
                      'price_at_5s_trades': n, 'price_at_5s_coverage': 1.0,
                      'price_at_5s_median_distance_sec': 0.13,
                      'pnl_pct_at_5s_median': 1.8, 'pnl_pct_at_5s_mean': 1.7,
                      'exits_followed': n, 'exits_with_marks': n,
                      'marks_per_exit_min': marks_min,
                      'marks_per_exit_median': marks_median,
                      'required_point_coverage': {
                          key: coverage for key in FULL_COVERAGE},
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
    """One lost point in a 12-hour run must not paint the stage red."""

    def test_a_12h_run_with_one_lost_point_still_passes(self) -> None:
        # 14 200 planned points, one missed: coverage 0.995, min marks 7.
        text = _report(
            43_200,
            portfolio_s0=_book(1_560, missed=1, marks_min=7),
            portfolio_s2=_book(216, marks_min=8),
        )
        row = _row(text, '| post_exit.csv: покрытие точек ≥ 0.95')
        self.assertIn('мин 7', row)
        self.assertIn('✅', row)

    def test_the_hour_stage_is_stricter_than_12_hours(self) -> None:
        hour = _row(_report(3_600,
                            portfolio_s0=_book(130), portfolio_s2=_book(18)),
                    '| post_exit.csv: покрытие точек')
        long = _row(_report(43_200,
                            portfolio_s0=_book(1_560), portfolio_s2=_book(216)),
                    '| post_exit.csv: покрытие точек')
        self.assertIn('≥ 0.97', hour)
        self.assertIn('≥ 0.95', long)

    def test_coverage_below_the_stage_threshold_fails(self) -> None:
        # 0.96 passes the 12-hour bar and fails the one-hour bar.
        long = _row(_report(43_200,
                            portfolio_s0=_book(1_560, coverage=0.96),
                            portfolio_s2=_book(216, coverage=0.96)),
                    '| post_exit.csv: покрытие точек')
        hour = _row(_report(3_600,
                            portfolio_s0=_book(130, coverage=0.96),
                            portfolio_s2=_book(18, coverage=0.96)),
                    '| post_exit.csv: покрытие точек')
        self.assertIn('✅', long)
        self.assertIn('❌', hour)

    def test_a_smoke_keeps_the_light_threshold(self) -> None:
        row = _row(_report(1_800,
                           portfolio_s0=_book(10, coverage=0.92, marks_min=6),
                           portfolio_s2=_book(3, coverage=0.92, marks_min=6)),
                   '| post_exit.csv: покрытие точек')
        self.assertIn('≥ 0.90', row)
        self.assertIn('мин ≥ 5', row)
        self.assertIn('✅', row)


class GhostPointAccountingTests(unittest.TestCase):
    """A lost point has to be readable from the report, not guessed."""

    def test_the_line_shows_written_planned_missed_and_recovered(self) -> None:
        text = _report(
            43_200,
            portfolio_s0=_book(1_560, planned=12_480, written=12_470, missed=10,
                               recovered=4, marks_min=7),
            portfolio_s2=_book(216, planned=1_728, written=1_727, missed=1),
        )
        row = _row(text, '| точки призрака записано/план')
        self.assertIn('S0: 12470/12480', row)
        self.assertIn('99.92', row)
        self.assertIn('потеряно 10', row)
        self.assertIn('спасено повторами 4', row)

    def test_a_book_without_ghosts_fails_the_row_instead_of_passing_silently(self) -> None:
        text = _report(3_600,
                       portfolio_s0=_book(130, planned=0, written=0),
                       portfolio_s2=_book(18))
        row = _row(text, '| точки призрака записано/план')
        self.assertIn('❌', row)


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
