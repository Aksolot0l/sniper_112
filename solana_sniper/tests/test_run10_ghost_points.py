"""Run #10 addendum §3: the nearest observation to entry+5 s wins.

`_price_at_offset` returned no distance, so the closed trade had no
`price_at_5s_distance_sec` and the ghost's comparison fell into its
`current is None` branch: any mark inside the 0.75 s tolerance overwrote a tick
that could be three times closer. In the 30-minute smoke 32 of 32 `post_exit`
picks had a closer tick in the stream (0.64 s vs 0.13 s), which is why run #10
measures ~5.6 s after entry where run #9 measured ~5.1 s.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

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



if __name__ == '__main__':
    unittest.main()
