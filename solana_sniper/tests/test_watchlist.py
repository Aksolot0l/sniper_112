from __future__ import annotations

import struct
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from models import NewCoin, SwapQuote
from price import WSOL
from pumpportal_validation import WSOL_MINT, b58decode
from watchlist import (
    WatchCandidate,
    WatchlistConfig,
    WatchlistSimulator,
    curve_price_sol,
    parse_curve_state,
    watchlist_entry_signal,
    watchlist_exit_quote_confirms,
)


class WatchlistRuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = WatchlistConfig()

    def test_entry_requires_two_prior_rising_intervals_and_current_delta(self) -> None:
        self.assertTrue(watchlist_entry_signal(
            age_sec=4.0, r_sol_t0=2.0, r_sol_prev=4.2, r_sol_now=4.5,
            r_sol_max=4.5, rising_intervals=2, config=self.config,
        ))
        self.assertFalse(watchlist_entry_signal(
            age_sec=4.0, r_sol_t0=2.0, r_sol_prev=4.2, r_sol_now=4.5,
            r_sol_max=4.5, rising_intervals=1, config=self.config,
        ))
        self.assertFalse(watchlist_entry_signal(
            age_sec=4.0, r_sol_t0=2.0, r_sol_prev=4.3, r_sol_now=4.5,
            r_sol_max=4.5, rising_intervals=2, config=self.config,
        ))

    def test_entry_age_growth_reserve_and_retrace_boundaries(self) -> None:
        args = dict(
            r_sol_t0=2.0, r_sol_prev=4.2, r_sol_now=4.5,
            r_sol_max=4.5, rising_intervals=2, config=self.config,
        )
        self.assertFalse(watchlist_entry_signal(age_sec=3.49, **args))
        self.assertTrue(watchlist_entry_signal(age_sec=3.5, **args))
        self.assertTrue(watchlist_entry_signal(age_sec=40.0, **args))
        self.assertFalse(watchlist_entry_signal(age_sec=40.01, **args))
        self.assertFalse(watchlist_entry_signal(
            age_sec=4.0, r_sol_t0=2.0, r_sol_prev=3.9, r_sol_now=4.2,
            r_sol_max=5.71, rising_intervals=2, config=self.config,
        ))
        self.assertFalse(watchlist_entry_signal(
            age_sec=4.0, r_sol_t0=2.0, r_sol_prev=17.8, r_sol_now=18.1,
            r_sol_max=18.1, rising_intervals=2, config=self.config,
        ))

    def test_exact_sell_quote_thresholds(self) -> None:
        c = self.config
        entry = 1.0
        ath = 2.0
        self.assertTrue(watchlist_exit_quote_confirms('SL (−12%)', 0.88, entry, ath, c))
        self.assertFalse(watchlist_exit_quote_confirms('SL (−12%)', 0.881, entry, ath, c))
        self.assertTrue(watchlist_exit_quote_confirms('TP1 (+35%)', 1.35, entry, ath, c))
        self.assertFalse(watchlist_exit_quote_confirms('TP1 (+35%)', 1.349, entry, ath, c))
        self.assertTrue(watchlist_exit_quote_confirms('Break-Even (≤+5%)', 1.05, entry, ath, c))
        self.assertTrue(watchlist_exit_quote_confirms('Trailing Stop (≤82% ATH)', 1.64, entry, ath, c))
        self.assertFalse(watchlist_exit_quote_confirms('Trailing Stop (≤82% ATH)', 1.641, entry, ath, c))
        self.assertTrue(watchlist_exit_quote_confirms('Timeout before TP1', 0.2, entry, ath, c))
        self.assertFalse(watchlist_exit_quote_confirms('SL', 0.0, entry, ath, c))

    def test_curve_state_uses_real_sol_reserve_offset_and_mayhem_byte(self) -> None:
        raw = bytearray(166)
        struct.pack_into('<QQ', raw, 8, 1_000_000_000_000_000, 30_000_000_000)
        struct.pack_into('<Q', raw, 32, 8_500_000_000)
        struct.pack_into('<Q', raw, 40, 1_000_000_000_000_000)
        quote_bytes = b58decode(WSOL_MINT)
        assert quote_bytes is not None
        raw[83:115] = quote_bytes
        state = parse_curve_state(bytes(raw))
        assert state is not None
        self.assertEqual(state['r_sol'], 8.5)
        self.assertFalse(state['mayhem'])
        self.assertAlmostEqual(curve_price_sol(bytes(raw)) or 0.0, 3e-8)
        raw[81] = 1
        mayhem_state = parse_curve_state(bytes(raw))
        assert mayhem_state is not None
        self.assertTrue(mayhem_state['mayhem'])
        self.assertIsNone(curve_price_sol(bytes(raw)))


class WatchlistSizingTests(unittest.IsolatedAsyncioTestCase):
    def settings(self, deposit=1500):
        return SimpleNamespace(
            deposit_rub=deposit, max_trade_rub=1500.0, usd_rub_rate=90.0,
            entry_fee_sol=0.0005, exit_fee_sol=0.0001,
            max_entry_price_impact_pct=2.0,
        )

    async def test_watchlist_uses_exact_max_trade_rub_with_separate_virtual_balance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coin = NewCoin(
                mint='MintSizingTest', source='Pump.fun', signature='public-sig',
                curve='CurveSizingTest', decimals=6,
            )
            sim = WatchlistSimulator(
                None, self.settings(), Path(tmp) / 'watchlist_wallet.json',
                WatchlistConfig(initial_balance_rub=5000.0),
            )
            sim._cached_sol_usd = AsyncMock(return_value=100.0)
            expected_input_lamports = int(1500.0 / (100.0 * 90.0) * 1_000_000_000)
            buy = SwapQuote(
                WSOL, coin.mint, expected_input_lamports,
                1_000_000_000, 1_000_000_000, 0.5,
            )
            sell = SwapQuote(
                coin.mint, WSOL, 1_000_000_000,
                150_000_000, 150_000_000, 0.5,
            )
            sim._quote_with_retries = AsyncMock(side_effect=[(buy, 5.0), (sell, 6.0)])
            events = []
            sim._event = Mock(side_effect=lambda *args: events.append(args))
            candidate = WatchCandidate(
                coin=coin, t0_epoch=1_700_000_000.0, r_sol_t0=2.0,
                r_sol_prev=4.2, r_sol_max=4.5, rising_intervals=2,
            )

            await sim._open_from_signal(candidate)

            self.assertEqual(sim.initial_balance_rub, 5000.0)
            self.assertEqual(sim.entry_count, 1)
            self.assertEqual(sim._quote_with_retries.await_args_list[0].args[2], expected_input_lamports)
            self.assertAlmostEqual(sim.positions[coin.mint].entry_rub, 1500.0, delta=0.001)
            self.assertGreater(sim.positions[coin.mint].invested_rub, 1500.0)  # fees are separate
            self.assertEqual(sim.pending_entry_reservations_rub, 0.0)
            opened = next(event for event in events if event[1] == 'entry_opened')
            self.assertEqual(opened[3]['requested_trade_rub'], 1500.0)
            self.assertIn('exact MAX_TRADE_RUB', opened[3]['position_size_policy'])

    async def test_insufficient_watchlist_cash_does_not_downsize_the_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coin = NewCoin(
                mint='MintTooSmallTest', source='Pump.fun', signature='public-sig',
                curve='CurveTooSmallTest', decimals=6,
            )
            sim = WatchlistSimulator(
                None, self.settings(deposit=1000), Path(tmp) / 'watchlist_wallet.json',
                WatchlistConfig(initial_balance_rub=1000.0),
            )
            sim._cached_sol_usd = AsyncMock(return_value=100.0)
            sim._quote_with_retries = AsyncMock()
            events = []
            sim._event = Mock(side_effect=lambda *args: events.append(args))
            candidate = WatchCandidate(
                coin=coin, t0_epoch=1_700_000_000.0, r_sol_t0=2.0,
                r_sol_prev=4.2, r_sol_max=4.5, rising_intervals=2,
            )

            await sim._open_from_signal(candidate)

            sim._quote_with_retries.assert_not_awaited()
            self.assertEqual(sim.entry_count, 0)
            self.assertEqual(sim.balance_rub, 1000.0)
            self.assertTrue(any(event[2] == 'watchlist_cash_below_exact_max_trade' for event in events))


if __name__ == '__main__':
    unittest.main()
