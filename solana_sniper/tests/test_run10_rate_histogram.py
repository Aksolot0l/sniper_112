"""Run #10c §1 — the peak is measured from a histogram, not asserted.

`requests_per_sec_max_10min` used to be a single number computed in memory: it
could be neither recounted nor decomposed, so the request audit had to scale
poll cycles linearly — 310 in one window against 431 actual. Now every request
lands in a five-second bucket, the peak is computed from those buckets, and the
array itself reaches the manifest.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import price
from main_ab import _peak_ok, _peak_window_text
from tools.rpc_decomposition import decompose


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


class HistogramTests(unittest.TestCase):
    def setUp(self) -> None:
        price.reset_rpc_metrics()

    def tearDown(self) -> None:
        price.reset_rpc_metrics()

    def _record(self, times: list[float]) -> dict:
        start = price._rpc_metrics.started_wall
        with patch.object(price.time, 'time', side_effect=[start + t for t in times]):
            for _ in times:
                price._rpc_metrics.record(1.0, 4)
        with patch.object(price.time, 'time', return_value=start + times[-1] + 1):
            return price.rpc_metrics_snapshot()

    def test_every_request_lands_in_a_five_second_bucket(self) -> None:
        # 1 request/s for 100 s: the first bucket [0,5) holds 4 requests, the
        # last one at t=100 opens a 21st bucket. The sum is the request count.
        metrics = self._record([float(t) for t in range(1, 101)])
        buckets = metrics['requests_per_5sec']
        self.assertEqual(metrics['requests_per_5sec_bucket_sec'], 5.0)
        self.assertEqual(len(buckets), 21)
        self.assertEqual(sum(buckets), metrics['requests'])
        self.assertEqual(buckets[0], 4)

    def test_the_peak_is_the_busiest_window_of_the_array(self) -> None:
        # 600 requests over 600 s, then 240 requests inside 120 s.
        times = [float(t) for t in range(1, 601)]
        times += [600.0 + 0.5 * index for index in range(240)]
        metrics = self._record(times)
        buckets = metrics['requests_per_5sec']
        peak = metrics['requests_per_sec_max_10min']
        window = metrics['requests_per_sec_max_10min_window_sec']
        self.assertIsNotNone(peak)
        # The busiest 120 buckets of the array, recomputed by hand.
        span = 120
        best = 0
        best_end = 0
        for end in range(len(buckets)):
            start = max(0, end - span + 1)
            count = sum(buckets[start:end + 1])
            span_sec = min(600.0, (end + 1) * 5.0)
            rate = count / span_sec
            if rate > best:
                best = rate
                best_end = end
        self.assertAlmostEqual(peak, round(best, 3), places=3)
        self.assertEqual(window[1], round((best_end + 1) * 5.0, 1))
        self.assertEqual(metrics['requests_per_sec_max_10min_count'],
                         sum(buckets[max(0, best_end - span + 1):best_end + 1]))

    def test_the_ratio_is_the_peak_over_the_mean(self) -> None:
        metrics = self._record([float(t) for t in range(1, 601)])
        self.assertIsNotNone(metrics['requests_per_sec_peak_to_mean_ratio'])
        self.assertAlmostEqual(
            metrics['requests_per_sec_peak_to_mean_ratio'],
            round(metrics['requests_per_sec_max_10min'] / metrics['requests_per_sec'], 3),
            places=3,
        )

    def test_a_run_shorter_than_the_window_reports_no_peak(self) -> None:
        metrics = self._record([float(t) for t in range(1, 101)])
        self.assertIsNone(metrics['requests_per_sec_max_10min'])
        self.assertIsNone(metrics['requests_per_sec_max_10min_window_sec'])
        self.assertIsNone(metrics['requests_per_sec_peak_to_mean_ratio'])


class PeakAcceptanceTests(unittest.TestCase):
    """§3: 2.2 absolute, or a peak explained by flow — but never by errors."""

    def _rpc(self, peak: float, mean: float, **extra) -> dict:
        base = {
            'requests': 5_000, 'requests_per_sec': mean,
            'requests_per_sec_max_10min': peak,
            'requests_per_sec_peak_to_mean_ratio': round(peak / mean, 3),
            'http_429_count': 10, 'unrecovered_rate': 0.0,
            'requests_per_sec_max_10min_window_sec': [1_650.0, 2_250.0],
        }
        base.update(extra)
        return base

    def test_the_expected_10a_prime_shape_passes_on_the_ratio(self) -> None:
        self.assertTrue(_peak_ok(self._rpc(2.03, 1.37), 2.2))

    def test_a_peak_under_the_absolute_ceiling_passes_even_with_a_bad_ratio(self) -> None:
        self.assertTrue(_peak_ok(self._rpc(2.1, 0.9), 2.2))

    def test_an_unexplained_peak_fails(self) -> None:
        self.assertFalse(_peak_ok(self._rpc(2.9, 1.37), 2.2))

    def test_a_ratio_above_1_75_fails_even_at_a_good_mean(self) -> None:
        self.assertFalse(_peak_ok(self._rpc(2.4, 1.30), 2.2))

    def test_429_above_the_budget_turns_a_ratio_pass_into_a_fail(self) -> None:
        rpc = self._rpc(2.4, 1.40, http_429_count=40)  # 0.8 % of 5 000
        self.assertFalse(_peak_ok(rpc, 2.2))

    def test_unrecovered_errors_turn_a_ratio_pass_into_a_fail(self) -> None:
        self.assertFalse(_peak_ok(self._rpc(2.4, 1.40, unrecovered_rate=0.02), 2.2))

    def test_a_run_without_a_window_passes(self) -> None:
        self.assertTrue(_peak_ok({'requests_per_sec_max_10min': None}, 2.2))


class PeakLineTests(unittest.TestCase):
    def test_the_line_carries_peak_window_mean_and_ratio(self) -> None:
        text = _peak_window_text({
            'requests_per_sec_max_10min': 2.03,
            'requests_per_sec_max_10min_window_sec': [1_650.0, 2_250.0],
            'requests_per_sec': 1.37,
            'requests_per_sec_peak_to_mean_ratio': 1.48,
        })
        self.assertEqual(
            text,
            'пик 10-мин окна: 2.03 rps (окно 27.5–37.5 мин), '
            'среднее 1.37, отношение 1.48',
        )

    def test_a_young_run_says_the_window_is_not_there(self) -> None:
        self.assertIn(
            'окно ещё не набрано',
            _peak_window_text({'requests_per_sec_max_10min': None}),
        )


class DecompositionFromHistogramTests(unittest.TestCase):
    """The audit reads the window from the array, not from a linear share."""

    def _run_dir(self, root: Path) -> Path:
        started = 1_700_000_000.0
        run_dir = root / 'run'
        run_dir.mkdir()
        buckets = [0] * 720
        for index in range(720):          # 26 requests per 5 s = 5.2 rps
            buckets[index] = 26
        manifest = {
            'started_at_utc': _iso(started),
            'actual_duration_sec': 3_600.0,
            'rpc_metrics': {
                'requests': sum(buckets),
                'requests_per_5sec': buckets,
                'requests_per_5sec_bucket_sec': 5.0,
                'requests_per_sec_max_10min': 5.2,
                'requests_per_sec_max_10min_window_sec': [0.0, 600.0],
                'requests_per_sec_peak_to_mean_ratio': 1.0,
                'requests_per_sec': 5.2,
                'retry_count': 0,
            },
        }
        (run_dir / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
        db = sqlite3.connect(run_dir / 'telemetry.sqlite')
        db.execute('CREATE TABLE decisions (mint TEXT, stage TEXT, occurred_at_utc TEXT)')
        db.execute('CREATE TABLE observations (mint TEXT, target_offset_sec INT, '
                   'observed_at_utc TEXT, status TEXT)')
        # 120 candidates inside 20–30 min, each with a "+0 s" (same moment) and
        # a "+5 s" observation.
        for index in range(120):
            at = started + 1_200 + index
            db.execute('INSERT INTO decisions VALUES (?,?,?)',
                       (f'mint{index}', 'security_and_market_filters', _iso(at)))
            db.execute('INSERT INTO observations VALUES (?,?,?,?)',
                       (f'mint{index}', 0, _iso(at + 0.1), 'ok'))
            db.execute('INSERT INTO observations VALUES (?,?,?,?)',
                       (f'mint{index}', 5, _iso(at + 5.0), 'ok'))
        db.commit()
        db.close()
        return run_dir

    def test_the_window_total_is_the_histogram_sum(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = self._run_dir(Path(temp))
            result = decompose(run_dir, 20.0, 30.0)
        self.assertEqual(result['buckets_available'], 720)
        self.assertEqual(result['window_total_from_histogram'], 3_120)  # 120 × 26
        self.assertIn('histogram total', result['poll_note'])
        # 120 filter reads + 120 "+5 s" reads are candidate-driven; the poll
        # part is the remainder inside the same measured window.
        self.assertEqual(result['filters'], 120)
        self.assertEqual(result['observation_5'], 120)
        self.assertEqual(result['poll_estimate'], 3_120 - 240)
        # The model total is the window it was measured over: nothing scaled,
        # and the "+0 s" observation (120 reads) is what the fix removes.
        self.assertEqual(result['after'], 3_120)
        self.assertEqual(result['before'], 3_240)
        self.assertEqual(result['peak_10min_rps'], 5.2)

    def test_a_legacy_run_without_the_array_says_it_scaled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = self._run_dir(Path(temp))
            manifest = json.loads((run_dir / 'manifest.json').read_text())
            manifest['rpc_metrics'].pop('requests_per_5sec')
            (run_dir / 'manifest.json').write_text(json.dumps(manifest))
            result = decompose(run_dir, 20.0, 30.0)
        self.assertEqual(result['buckets_available'], 0)
        self.assertIn('legacy run without requests_per_5sec', result['poll_note'])


if __name__ == '__main__':
    unittest.main()
