"""Run #10b §1 — the "+0 s" observation prices the snapshot, not a second read.

The security filter already read `[mint, curve, Global, FeeConfig]` for this
coin in this slot. Re-reading it for the t0 sample was one of the two identical
batches per candidate that made run #10a spend 6 565 requests (1.82 rps). The
sample stays a real curve read — it is the same read, taken earlier — and a
coin without a snapshot still takes the ordinary one.
"""

from __future__ import annotations

import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from models import NewCoin
from pump_curve_quote import BONDING_CURVE_DISCRIMINATOR
from price import pump_curve_price_from_raw
from telemetry import CandidateCollector


def _curve_raw() -> bytes:
    raw = bytearray(166)
    raw[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into('<QQ', raw, 8, 1_073_000_191_279_242, 30_000_000_000)
    struct.pack_into('<Q', raw, 32, 30_000_000_000)
    struct.pack_into('<Q', raw, 40, 1_073_000_191_279_242)
    return bytes(raw)


SETTINGS = SimpleNamespace(
    max_trade_rub=250, max_positions=1, min_liquidity_usd=3000,
    max_entry_price_impact_pct=2, take_profit_pct=35,
    stop_loss_pct=10, stagnation_sec=60, stagnation_range_pct=2,
    max_hold_sec=120, usd_rub_rate=90,
    http_rpc_url='https://rpc.example.invalid',
)


class T0SnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def _observe(self, coin: NewCoin) -> tuple[list, sqlite3.Row]:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'exports').mkdir(parents=True, exist_ok=True)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(0,), enable_shadow_quotes=False,
            )
            await collector.start()
            with patch('telemetry.get_sol_usd', new=AsyncMock(return_value=100.0)), \
                 patch('telemetry.pump_curve_price_usd',
                       new=AsyncMock(return_value=123.0)) as reader:
                collector.observe_candidate(coin, object(), SETTINGS)
                await asyncio_gather(collector)
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                db.row_factory = sqlite3.Row
                row = db.execute(
                    'SELECT status, price_usd, price_source, latency_ms '
                    'FROM observations WHERE target_offset_sec=0'
                ).fetchone()
            return reader, row

    async def test_a_snapshot_is_priced_without_a_read(self) -> None:
        coin = NewCoin(
            mint='MintWithSnapshot', source='Pump.fun', signature='sig',
            curve='CurveAddress',
            entry_snapshot={'curve_raw': _curve_raw(), 'observed_at_epoch': 1_000.0},
        )
        reader, row = await self._observe(coin)
        self.assertEqual(reader.await_count, 0, 'the t0 sample must not re-read')
        self.assertEqual(row['status'], 'ok')
        self.assertEqual(row['price_source'], 'on_chain_pump_curve')
        self.assertGreater(row['price_usd'], 0)
        self.assertEqual(row['latency_ms'], 0.0)

    async def test_without_a_snapshot_the_ordinary_read_is_taken(self) -> None:
        coin = NewCoin(
            mint='MintWithoutSnapshot', source='Pump.fun', signature='sig',
            curve='CurveAddress',
        )
        reader, row = await self._observe(coin)
        self.assertEqual(reader.await_count, 1)
        self.assertEqual(row['status'], 'ok')

    def test_the_price_arithmetic_is_the_curve_read(self) -> None:
        price = pump_curve_price_from_raw(_curve_raw(), 100.0)
        self.assertAlmostEqual(price, (30.0 / 1_073_000_191.279242) * 100.0, places=12)
        self.assertIsNone(pump_curve_price_from_raw(None, 100.0))
        self.assertIsNone(pump_curve_price_from_raw(_curve_raw(), None))


async def asyncio_gather(collector: CandidateCollector) -> None:
    import asyncio
    await asyncio.gather(*list(collector._tasks))


if __name__ == '__main__':
    unittest.main()
