from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import price


class _FakeResponse:
    """Minimal aiohttp-like response: only what the RPC layer reads."""

    def __init__(self, status: int, payload: dict, retry_after: str | None = None):
        self.status = status
        self._payload = payload
        self.headers = {'Retry-After': retry_after} if retry_after else {}

    async def json(self):
        return self._payload

    async def text(self):
        return str(self._payload)

    def __await__(self):
        return iter(())

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakePost:
    """A session.post() that replays a list of responses, then keeps the last."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, *args, **kwargs):
        index = min(self.calls, len(self.responses) - 1)
        self.calls += 1
        return self.responses[index]


def _ok_response():
    return _FakeResponse(200, {
        'jsonrpc': '2.0', 'id': 1,
        'result': {'context': {'slot': 42}, 'value': [{'data': ['x', 'base64']}]},
    })


def _rate_limited_response(retry_after=None):
    return _FakeResponse(429, {
        'jsonrpc': '2.0', 'id': 1,
        'error': {'code': -32429, 'message': 'rate limited'},
    }, retry_after=retry_after)


class RpcRetryTests(unittest.IsolatedAsyncioTestCase):
    """P0-3: a 429 is back-pressure; only unrecovered reads are failures."""

    def setUp(self) -> None:
        price.reset_rpc_metrics()
        # No real sleeping: the backoff values are asserted separately.
        self._sleep = patch.object(price.asyncio, 'sleep', new=AsyncMock())
        self._sleep.start()
        self.addCleanup(self._sleep.stop)

    async def _read(self, session):
        return await price._rpc_multiple_accounts_with_retry(
            session, 'https://rpc.invalid', ['addr-1'], 'base64', 'processed',
        )

    async def test_a_retried_429_is_not_a_failed_request(self) -> None:
        session = type('S', (), {'post': _FakePost([_rate_limited_response(), _ok_response()])})()
        values, slot, error = await self._read(session)
        self.assertIsNone(error)
        self.assertEqual(slot, 42)
        self.assertEqual(len(values), 1)
        metrics = price.rpc_metrics_snapshot()
        self.assertEqual(metrics['http_429_count'], 1)
        self.assertEqual(metrics['retry_count'], 1)
        self.assertEqual(metrics['recovered_failures'], 1)
        # The number the stop-list judges stays at zero.
        self.assertEqual(metrics['unrecovered_failures'], 0)
        # And nothing was booked as a plain failed request either.
        self.assertEqual(metrics['failed_requests'], 0)

    async def test_three_retries_then_success(self) -> None:
        session = type('S', (), {'post': _FakePost([
            _rate_limited_response(), _rate_limited_response(),
            _rate_limited_response(), _ok_response(),
        ])})()
        _values, _slot, error = await self._read(session)
        self.assertIsNone(error)
        metrics = price.rpc_metrics_snapshot()
        self.assertEqual(metrics['retry_count'], 3)       # first try + 3 retries
        self.assertEqual(metrics['recovered_failures'], 1)
        self.assertEqual(metrics['unrecovered_failures'], 0)

    async def test_a_permanent_429_counts_as_one_unrecovered_failure(self) -> None:
        session = type('S', (), {'post': _FakePost([_rate_limited_response()])})()
        _values, slot, error = await self._read(session)
        self.assertEqual(error, 'http_429')
        self.assertIsNone(slot)
        metrics = price.rpc_metrics_snapshot()
        self.assertEqual(metrics['retry_count'], 3)
        self.assertEqual(metrics['http_429_count'], 4)
        self.assertEqual(metrics['unrecovered_failures'], 1)
        # Exactly one request failed, not one per attempt.
        self.assertEqual(metrics['failed_requests'], 1)

    async def test_retry_after_is_honoured_and_clamped(self) -> None:
        self.assertEqual(price._retry_delay_sec(0, 1.5), 1.5)
        self.assertEqual(price._retry_delay_sec(2, 30.0), 5.0)   # clamped to 5 s
        # Without a header: 0.2 / 0.5 / 1.2 s plus jitter <= 0.15 s.
        for attempt, base in enumerate(price.RPC_RETRY_BACKOFF_SEC):
            delay = price._retry_delay_sec(attempt, None)
            self.assertGreaterEqual(delay, base)
            self.assertLessEqual(delay, base + price.RPC_RETRY_JITTER_SEC)
        # The last index repeats for any attempt beyond the table.
        self.assertGreaterEqual(price._retry_delay_sec(9, None), 1.2)

    async def test_retry_after_header_is_parsed(self) -> None:
        self.assertEqual(price._retry_after_sec(_rate_limited_response('2')), 2.0)
        self.assertIsNone(price._retry_after_sec(_rate_limited_response()))
        self.assertIsNone(price._retry_after_sec(_FakeResponse(429, {}, 'soon')))
        self.assertIsNone(price._retry_after_sec(object()))

    async def test_non_retryable_error_is_not_retried(self) -> None:
        session = type('S', (), {'post': _FakePost([
            _FakeResponse(400, {'jsonrpc': '2.0', 'id': 1,
                                'error': {'code': -32602, 'message': 'bad params'}}),
        ])})()
        _values, _slot, error = await self._read(session)
        self.assertEqual(error, 'http_400')
        metrics = price.rpc_metrics_snapshot()
        self.assertEqual(metrics['retry_count'], 0)
        self.assertEqual(metrics['unrecovered_failures'], 1)

    async def test_requests_per_sec_stays_a_rate_not_a_count(self) -> None:
        with patch.object(price.time, 'time', return_value=price._rpc_metrics.started_wall + 10.0):
            price._rpc_metrics.record(12.0, 5)
            metrics = price.rpc_metrics_snapshot()
        self.assertAlmostEqual(metrics['requests_per_sec'], 0.1, places=3)


class SemaphoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrency_cap_is_bounded(self) -> None:
        sem = price._rpc_sem()
        self.assertEqual(sem._value, price.RPC_MAX_CONCURRENT)
        self.assertLessEqual(price.RPC_MAX_CONCURRENT, 6)
