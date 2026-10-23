"""Run #10 §2a: the post-exit ghost is read on a schedule, not every cycle.

Run #9 pulled the ghost along in *every* mark batch: 1 839 of 2 735 batches in
the 30-minute smoke existed only for ghosts, which is what pushed the run to
2.23 rps mean and 2.83 rps in the worst window. The ghost needs eight points,
not forty.
"""
from __future__ import annotations

import asyncio
import base64
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import portfolio_ab as module
from portfolio_ab import (
    POST_EXIT_REQUIRED_SEC,
    POST_EXIT_SCHEDULE_GRACE_SEC,
    POST_EXIT_SCHEDULE_SEC,
    POST_EXIT_WINDOW_SEC,
)
from tests.test_portfolio_ab import make_curve_raw, make_simulator


def _account() -> dict:
    return {
        'owner': module.PUMP_PROGRAM_ID,
        'data': [base64.b64encode(make_curve_raw()).decode(), 'base64'],
    }


def _ghost(*, exit_epoch: float, until_epoch: float, index: int = 0) -> dict:
    return {
        'portfolio': 'portfolio_a',
        'mint': 'mint-ghost',
        'curve': 'curve-ghost',
        'exit_at_epoch': float(exit_epoch),
        'entry_at_epoch': float(exit_epoch) - 5.0,
        'entry_mid_price_usd': 1e-5,
        'exit_mid_price_usd': 1e-5,
        'until_epoch': float(until_epoch),
        'schedule_index': index,
        'trade': None,
        'marks': 0,
    }


class GhostScheduleShapeTests(unittest.TestCase):
    def test_the_schedule_covers_every_required_point_exactly(self) -> None:
        for point in POST_EXIT_REQUIRED_SEC:
            self.assertIn(point, POST_EXIT_SCHEDULE_SEC, point)

    def test_the_schedule_is_short_and_inside_the_window(self) -> None:
        # Eight points, strictly increasing, none of them past the window.
        self.assertEqual(len(POST_EXIT_SCHEDULE_SEC), 8)
        self.assertEqual(list(POST_EXIT_SCHEDULE_SEC),
                         sorted(set(POST_EXIT_SCHEDULE_SEC)))
        self.assertLessEqual(max(POST_EXIT_SCHEDULE_SEC), POST_EXIT_WINDOW_SEC)


class DueGhostTests(unittest.TestCase):
    """`_due_post_exit_ghosts` is the whole rule: one point at a time."""

    def _simulator(self):
        with tempfile.TemporaryDirectory() as root:
            return make_simulator(root)

    def test_nothing_is_due_before_the_first_point(self) -> None:
        simulator = self._simulator()
        ghost = _ghost(exit_epoch=1_000.0, until_epoch=1_010.5)
        simulator._ghosts = [ghost]
        self.assertEqual(simulator._due_post_exit_ghosts(1_000.4), [])
        self.assertEqual([g['mint'] for g in simulator._due_post_exit_ghosts(1_000.5)],
                         ['mint-ghost'])

    def test_a_point_is_due_once_and_then_the_schedule_advances(self) -> None:
        simulator = self._simulator()
        ghost = _ghost(exit_epoch=1_000.0, until_epoch=1_010.5)
        simulator._ghosts = [ghost]
        ghost['schedule_index'] = 1  # 0.5 s already taken
        self.assertEqual(simulator._due_post_exit_ghosts(1_000.9), [])
        self.assertEqual(len(simulator._due_post_exit_ghosts(1_001.0)), 1)

    def test_an_exhausted_schedule_is_never_due_again(self) -> None:
        simulator = self._simulator()
        ghost = _ghost(exit_epoch=1_000.0, until_epoch=1_010.5,
                       index=len(POST_EXIT_SCHEDULE_SEC))
        simulator._ghosts = [ghost]
        self.assertEqual(simulator._due_post_exit_ghosts(1_011.0), [])

    def test_the_ghost_outlives_its_last_point(self) -> None:
        # Otherwise the 10 s mark would be pruned before it could be read.
        self.assertGreater(POST_EXIT_SCHEDULE_GRACE_SEC, 0.0)


class PollLoopScheduleTests(unittest.IsolatedAsyncioTestCase):
    """The loop reads a ghost only when a point is due."""

    async def test_eight_scheduled_marks_and_no_request_between_them(self) -> None:
        schedule = (0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40)
        with tempfile.TemporaryDirectory() as root, \
                patch.object(module, 'POST_EXIT_SCHEDULE_SEC', schedule), \
                patch('portfolio_ab.get_sol_usd', new=AsyncMock(return_value=200.0)):
            simulator = make_simulator(root)
            simulator.mark_poll_sec = 0.01
            exit_epoch = time.time()
            ghost = _ghost(
                exit_epoch=exit_epoch,
                until_epoch=exit_epoch + 10.0 + POST_EXIT_SCHEDULE_GRACE_SEC,
            )
            simulator._ghosts = [ghost]
            calls: list[list[str]] = []

            async def fake_rpc(session, url, addresses, **kwargs):
                calls.append(list(addresses))
                return {a: _account() for a in addresses if a == 'curve-ghost'}, 12

            with patch('portfolio_ab.rpc_get_multiple_accounts', new=fake_rpc):
                task = asyncio.create_task(simulator._position_poll_loop())
                await asyncio.sleep(0.6)
                simulator._stopping.set()
                await asyncio.wait_for(task, timeout=1.0)

            self.assertEqual(len(simulator.post_exit_rows), 8)
            # Eight points, so at most a handful of batches — not one per cycle.
            self.assertLessEqual(len(calls), 10, calls)
            self.assertEqual(ghost['schedule_index'], len(schedule))

    async def test_a_coin_nobody_holds_costs_nothing_between_points(self) -> None:
        with tempfile.TemporaryDirectory() as root, \
                patch.object(module, 'POST_EXIT_SCHEDULE_SEC', (5.0,)), \
                patch('portfolio_ab.get_sol_usd', new=AsyncMock(return_value=200.0)):
            simulator = make_simulator(root)
            simulator.mark_poll_sec = 0.01
            now = time.time()
            simulator._ghosts = [_ghost(exit_epoch=now, until_epoch=now + 15.0)]
            with patch('portfolio_ab.rpc_get_multiple_accounts',
                       new=AsyncMock()) as rpc:
                task = asyncio.create_task(simulator._position_poll_loop())
                await asyncio.sleep(0.15)
                simulator._stopping.set()
                await asyncio.wait_for(task, timeout=1.0)
                # 0.15 s of polling, the first point is 5 s away: no request.
                rpc.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
