from __future__ import annotations

import asyncio
import base64
import unittest

from price import (
    ReadOnlyRpcBatcher,
    reset_rpc_metrics,
    rpc_get_multiple_accounts,
    rpc_metrics_snapshot,
    set_rpc_batcher,
)


class FakeResponse:
    status = 200

    def __init__(self, payload: dict) -> None:
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def json(self):
        return self.payload


class FakeRpcSession:
    def __init__(self, null_once: set[str] | None = None) -> None:
        self.calls: list[dict] = []
        self.null_once = set(null_once or ())
        self.null_seen: set[str] = set()

    def post(self, url: str, **kwargs):
        payload = kwargs['json']
        addresses = payload['params'][0]
        self.calls.append(payload)
        values = []
        for address in addresses:
            if address in self.null_once and address not in self.null_seen:
                self.null_seen.add(address)
                values.append(None)
            else:
                values.append({
                    'owner': 'owner-test',
                    'data': [base64.b64encode(address.encode()).decode(), 'base64'],
                })
        return FakeResponse({'result': {'context': {'slot': 77}, 'value': values}})


class ReadOnlyRpcBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        set_rpc_batcher(None)

    async def test_more_than_100_accounts_are_split_and_null_is_retried_once(self) -> None:
        reset_rpc_metrics()
        session = FakeRpcSession(null_once={'mint-0'})
        addresses = [f'mint-{i}' for i in range(101)]
        results, slot = await rpc_get_multiple_accounts(
            session, 'https://rpc.invalid', addresses,
            encoding='base64', retry_null=True,
        )
        self.assertEqual(len(results), 101)
        self.assertIsNotNone(results['mint-0'])
        self.assertEqual(slot, 77)
        self.assertEqual(session.calls[0]['method'], 'getMultipleAccounts')
        self.assertEqual(len(session.calls[0]['params'][0]), 100)
        self.assertEqual(len(session.calls), 3)  # 100 + 1, then just the null key
        metrics = rpc_metrics_snapshot()
        self.assertEqual(metrics['null_retries'], 1)
        self.assertEqual(metrics['failed_requests'], 0)

    async def test_concurrent_requests_are_coalesced_into_one_read_only_batch(self) -> None:
        reset_rpc_metrics()
        session = FakeRpcSession()
        batcher = ReadOnlyRpcBatcher(session, 'https://rpc.invalid', flush_window_sec=0.01)
        batcher.start()
        set_rpc_batcher(batcher)
        left, right = await asyncio.gather(
            rpc_get_multiple_accounts(session, 'https://rpc.invalid', ['curve-a']),
            rpc_get_multiple_accounts(session, 'https://rpc.invalid', ['curve-b']),
        )
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.calls[0]['params'][0], ['curve-a', 'curve-b'])
        self.assertIsNotNone(left[0]['curve-a'])
        self.assertIsNotNone(right[0]['curve-b'])
        metrics = rpc_metrics_snapshot()
        self.assertEqual(metrics['logical_requests'], 2)
        self.assertEqual(metrics['requests'], 1)
        self.assertEqual(metrics['batch_groups'], 1)
        await batcher.close()


if __name__ == '__main__':
    unittest.main()
