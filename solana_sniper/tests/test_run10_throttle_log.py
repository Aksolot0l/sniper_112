"""Run #10b §2 — throttling and pace must reach the console.

In 10a `http_429_count` was 62 and `grep 429 run.log` found three matches, all
of them inside token totals. An operator watching the console could read "429
are still those three" while the provider was throttling once a minute.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import price


class ThrottleLogTests(unittest.TestCase):
    def setUp(self) -> None:
        price.reset_rpc_metrics()

    def tearDown(self) -> None:
        price.reset_rpc_metrics()

    def test_a_burst_is_logged_with_the_running_count_and_retry_after(self) -> None:
        metrics = price._rpc_metrics
        start = metrics.started_wall
        messages: list[str] = []
        with patch('price.log', side_effect=lambda msg, color='': messages.append(str(msg))):
            with patch.object(price.time, 'time', side_effect=[start + 1, start + 1, start + 1]):
                metrics.record(1.0, 4, error_type='http_429', retry_after=0.4)
                metrics.record(1.0, 4, error_type='http_429')
                metrics.record(1.0, 4, error_type='http_429')
        throttle = [m for m in messages if '429' in m]
        self.assertEqual(len(throttle), 1, messages)
        # The line is written once per interval and carries the running count
        # at the moment it is written; the next line carries the total.
        self.assertIn('1 за прогон', throttle[0])
        self.assertIn('0.4 c', throttle[0])
        self.assertEqual(metrics.http_429_count, 3)

    def test_the_next_line_waits_at_least_ten_seconds(self) -> None:
        metrics = price._rpc_metrics
        start = metrics.started_wall
        messages: list[str] = []
        with patch('price.log', side_effect=lambda msg, color='': messages.append(str(msg))):
            with patch.object(price.time, 'time', side_effect=[
                start + 1, start + 5, start + 11,
            ]):
                metrics.record(1.0, 4, error_type='http_429', retry_after=0.2)
                metrics.record(1.0, 4, error_type='http_429', retry_after=0.2)
                metrics.record(1.0, 4, error_type='http_429', retry_after=0.9)
        throttle = [m for m in messages if 'Темп RPC: ' not in m]
        self.assertEqual(len(throttle), 2, messages)
        self.assertIn('3 за прогон', throttle[-1])
        self.assertIn('0.9 c', throttle[-1])

    def test_pace_line_carries_rps_window_429_and_retry_every_ten_minutes(self) -> None:
        metrics = price._rpc_metrics
        start = metrics.started_wall
        messages: list[str] = []
        # 601 requests one second apart: the 10-minute line is due at the end.
        times = [start + index for index in range(1, 602)]
        with patch('price.log', side_effect=lambda msg, color='': messages.append(str(msg))):
            with patch.object(price.time, 'time', side_effect=times):
                for _ in times:
                    metrics.record(1.0, 4)
        pace = [m for m in messages if 'Темп RPC: ' in m]
        self.assertEqual(len(pace), 1, messages)
        self.assertIn('rps средний', pace[0])
        self.assertIn('худшее 10-мин окно', pace[0])
        self.assertIn('429 0', pace[0])
        self.assertIn('повторов 0', pace[0])

    def test_no_429_line_when_nothing_was_throttled(self) -> None:
        metrics = price._rpc_metrics
        start = metrics.started_wall
        messages: list[str] = []
        with patch('price.log', side_effect=lambda msg, color='': messages.append(str(msg))):
            with patch.object(price.time, 'time', side_effect=[start + 1, start + 2]):
                metrics.record(1.0, 4)
                metrics.record(1.0, 4)
        self.assertEqual([m for m in messages if '429' in m], [])


if __name__ == '__main__':
    unittest.main()
