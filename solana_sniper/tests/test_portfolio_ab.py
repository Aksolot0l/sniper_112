from __future__ import annotations

import asyncio
import json
import struct
import tempfile
import time
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from portfolio_ab import (
    ABPaperSimulator,
    EntrySnapshot,
    ExecutableQuote,
    PROFILE_A,
    PROFILE_B,
    PortfolioBook,
    SimPosition,
    delta_bucket,
    entry_shadow_flags,
    profiles_from_settings,
)
from pump_curve_quote import (
    BONDING_CURVE_DISCRIMINATOR,
    FEE_CONFIG_DISCRIMINATOR,
    GLOBAL_DISCRIMINATOR,
    CurveState,
    make_fee_context,
    quote_sell_exact_tokens,
)
from pumpportal_validation import PUMP_PROGRAM_ID, WSOL_MINT, b58decode
from models import NewCoin, SwapQuote


def make_fee_context_for_test():
    global_raw = bytearray(1_088)
    global_raw[:8] = GLOBAL_DISCRIMINATOR
    struct.pack_into('<Q', global_raw, 105, 95)
    struct.pack_into('<Q', global_raw, 154, 30)
    global_raw[1045] = 0
    struct.pack_into('<Q', global_raw, 1046, 1_000)

    fee_raw = bytearray(FEE_CONFIG_DISCRIMINATOR)
    fee_raw.extend(b'\x01' + bytes(32))
    fee_raw.extend(struct.pack('<QQQ', 0, 95, 30))
    fee_raw.extend(struct.pack('<I', 1))
    fee_raw.extend((0).to_bytes(16, 'little'))
    fee_raw.extend(struct.pack('<QQQ', 0, 95, 30))
    fee_raw.extend(struct.pack('<I', 0))
    fee_raw.extend(struct.pack('<QQQ', 0, 0, 0))
    return make_fee_context(bytes(global_raw), bytes(fee_raw), context_slot=12)


def make_curve_raw(*, complete=False, vtok=1_073_000_000_000_000, vquote=30_000_000_000):
    raw = bytearray(166)
    raw[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into('<QQQQQ', raw, 8, vtok, vquote, 793_100_000_000_000, 0, 1_000_000_000_000_000)
    raw[48] = int(complete)
    raw[49:81] = bytes([5]) * 32
    raw[83:115] = bytes(32)
    return bytes(raw)


def make_curve(*, complete=False, vtok=1_073_000_000_000_000, vquote=30_000_000_000):
    return CurveState.decode(make_curve_raw(complete=complete, vtok=vtok, vquote=vquote))


def make_position(*, opened_ago=0.0, best_pnl=-100.0, status='open'):
    now = time.time()
    return SimPosition(
        portfolio='portfolio_a', mint='mint-test', source='Pump.fun', curve='curve-test',
        opened_at_epoch=now - opened_ago, opened_at_utc='2026-10-21T00:00:00+00:00',
        birth_v_sol=32.0, entry_v_sol=32.1, entry_sol_usd=200.0,
        entry_cost_rub=500.0, entry_fee_rub=5.0, remaining_entry_fee_rub=5.0,
        entry_price_usd=0.01, initial_tokens_raw=1_000_000,
        remaining_tokens_raw=1_000_000, entry_amount_lamports=27_000_000,
        entry_min_out_raw=990_000, entry_protocol_fee_raw=100,
        entry_creator_fee_raw=20, entry_fee_bps_protocol=95,
        entry_fee_bps_creator=30, entry_market_cap_lamports=30_000_000_000,
        entry_fee_tier_index=0, remaining_cost_rub=500.0,
        entry_out_amount_raw=1_000_000,
        initial_conservative_tokens_raw=990_000,
        remaining_conservative_tokens_raw=990_000,
        conservative_remaining_cost_rub=500.0,
        conservative_remaining_entry_fee_rub=5.0,
        best_pnl_pct=best_pnl, status=status,
    )


def make_settings():
    return SimpleNamespace(
        filter_concurrency=4,
        usd_rub_rate=90.0,
        entry_fee_sol=0.00008,
        exit_fee_sol=0.00005,
        http_rpc_url='https://rpc.invalid',
        entry_birth_min_sol=30.5,
        entry_birth_max_sol=50.0,
        entry_delta_min_sol=0.10,
        entry_delta_max_sol=2.5,
        creator_guard_min_sig=25,
        creator_burst_window_sec=3600.0,
        creator_cache_ttl_sec=300.0,
    )


def make_simulator(root: str | Path, *, trades_queue=None):
    return ABPaperSimulator(
        object(), make_settings(), 'run-test', root,
        trades_queue or asyncio.Queue(), creator_watch=object(), creator_probe=object(),
    )


class ABProfileTests(unittest.TestCase):
    def test_shared_entry_rule_requires_live_momentum(self) -> None:
        for profile in (PROFILE_A, PROFILE_B):
            # Dead launch: no buys after the creator -> never entered.
            ok, reason = profile.entry_check(31.0, 31.0)
            self.assertFalse(ok)
            self.assertEqual(reason, 'no_live_momentum')
            # Dust delta below the floor is still a dead launch.
            self.assertFalse(profile.entry_check(31.0, 31.05)[0])
            # Live early momentum passes.
            ok, reason = profile.entry_check(30.5, 31.3)
            self.assertTrue(ok)
            self.assertEqual(reason, 'live_momentum_entry')
            # Small live deltas still pass (the entry rule that replaced the cap).
            self.assertTrue(profile.entry_check(31.0, 31.5)[0])
            self.assertTrue(profile.entry_check(30.5, 31.4)[0])
            self.assertTrue(profile.entry_check(39.88, 40.41)[0])   # PERFORMA (+0.53)
            # delta cap is 2.5: the "big rocket" deltas are now late chases.
            self.assertEqual(profile.delta_max_sol, 2.5)
            for birth, now in ((33.95, 45.51), (33.16, 38.59),    # Turtle, Shigure
                               (36.0, 40.43), (35.0, 39.0)):       # CAVELON, SKILLSPECTOR
                ok, reason = profile.entry_check(birth, now)
                self.assertFalse(ok)
                self.assertEqual(reason, 'momentum_too_late')
            # Boundary of the cap itself.
            self.assertTrue(profile.entry_check(32.0, 34.5)[0])
            self.assertFalse(profile.entry_check(32.0, 34.5001)[0])
            # Birth band still guards supply-shape outliers.
            self.assertFalse(profile.entry_check(30.49, 31.3)[0])
            self.assertFalse(profile.entry_check(50.01, 50.5)[0])

    def test_delta_buckets_and_shadow_caps_are_pure_arithmetic(self) -> None:
        self.assertEqual(delta_bucket(-0.4), '0')
        self.assertEqual(delta_bucket(0.0), '0')
        self.assertEqual(delta_bucket(0.35), '0.1-1')
        self.assertEqual(delta_bucket(1.0), '1-2.5')
        self.assertEqual(delta_bucket(2.49), '1-2.5')
        self.assertEqual(delta_bucket(2.5), '2.5-4')
        self.assertEqual(delta_bucket(4.0), '4-12')
        self.assertEqual(delta_bucket(12.0), '>12')

        flags = entry_shadow_flags(PROFILE_A, 33.0, 33.0 + 3.0)
        self.assertEqual(
            sorted(flags), ['pass_if_cap_1', 'pass_if_cap_12', 'pass_if_cap_2.5', 'pass_if_cap_4'],
        )
        self.assertFalse(flags['pass_if_cap_1'])
        self.assertFalse(flags['pass_if_cap_2.5'])
        self.assertTrue(flags['pass_if_cap_4'])
        self.assertTrue(flags['pass_if_cap_12'])

        dead = entry_shadow_flags(PROFILE_A, 31.0, 31.0)
        self.assertFalse(any(dead.values()), 'a dead delta==0 launch never passes any cap')
        out_of_band = entry_shadow_flags(PROFILE_A, 20.0, 20.5)
        self.assertFalse(any(out_of_band.values()), 'birth band applies to every cap')

    def test_entry_thresholds_come_from_settings(self) -> None:
        base = make_settings()
        settings = SimpleNamespace(**{
            **vars(base),
            'entry_birth_max_sol': 44.0, 'entry_delta_min_sol': 0.5,
            'entry_delta_max_sol': 6.0, 'creator_guard_min_sig': 30,
        })
        profiles = profiles_from_settings(settings)
        self.assertEqual(profiles['portfolio_a'].v_sol_birth_max, 44.0)
        self.assertEqual(profiles['portfolio_a'].delta_min_sol, 0.5)
        self.assertEqual(profiles['portfolio_a'].delta_max_sol, 6.0)
        self.assertEqual(profiles['portfolio_b'].guard_min_signatures, 30)
        self.assertFalse(profiles['portfolio_a'].require_creator_gate)
        self.assertTrue(profiles['portfolio_b'].require_creator_gate)

    def test_profiles_differ_only_in_name_and_b_creator_gate(self) -> None:
        a, b = asdict(PROFILE_A), asdict(PROFILE_B)
        self.assertFalse(a.pop('require_creator_gate'))
        self.assertTrue(b.pop('require_creator_gate'))
        a.pop('name')
        b.pop('name')
        self.assertEqual(a, b)
        self.assertEqual((PROFILE_A.initial_balance_rub, PROFILE_B.initial_balance_rub), (1500, 1500))
        self.assertEqual((PROFILE_A.position_size_rub, PROFILE_B.position_size_rub), (500, 500))
        self.assertEqual((PROFILE_A.max_slots, PROFILE_B.max_slots), (3, 3))

    def test_books_use_isolated_wallet_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = PortfolioBook(PROFILE_A, root / 'wallet_a.json', cash_balance_rub=1500)
            b = PortfolioBook(PROFILE_B, root / 'wallet_b.json', cash_balance_rub=1500)
            a.save('run-ab')
            b.save('run-ab')
            self.assertNotEqual(a.state_path, b.state_path)
            self.assertEqual(json.loads(a.state_path.read_text())['cash_balance_rub'], 1500)
            self.assertEqual(json.loads(b.state_path.read_text())['portfolio'], 'portfolio_b')


class ABEntryGateAndAccountingTests(unittest.IsolatedAsyncioTestCase):
    async def _consider_both(self, root, creator_probe_result):
        simulator = make_simulator(root)
        coin = NewCoin(
            mint='mint-entry-test', source='Pump.fun', signature='sig-test',
            curve='curve-test', packet={'v_sol': 32.0},
        )
        coin.creator_probe_result = creator_probe_result
        snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
        decision_epoch = time.time()
        with patch('portfolio_ab.get_collector', return_value=None):
            await asyncio.gather(*(
                simulator._consider_entry(
                    simulator.books[name], coin, snapshot,
                    32.0, 32.4, 200.0, {}, decision_epoch,
                )
                for name in ('portfolio_a', 'portfolio_b')
            ))
        return simulator, coin

    async def test_a_does_not_gate_b_requires_all_three_creator_conditions(self) -> None:
        cases = (
            ({'signature_count': 24, 'ata_exists': True, 'token_balance_raw': '1',
              'creator_guard_passed': False, 'creator_guard_reason': 'creator_history_too_short',
              'censored': False}, False),
            ({'signature_count': 25, 'ata_exists': False, 'token_balance_raw': '1',
              'creator_guard_passed': False, 'creator_guard_reason': 'creator_token_ata_missing',
              'censored': False}, False),
            ({'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '0',
              'creator_guard_passed': False, 'creator_guard_reason': 'creator_token_balance_zero',
              'censored': False}, False),
            # Serial rug farm: 25 signatures inside the burst window.
            ({'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
              'creator_guard_passed': False, 'creator_guard_reason': 'creator_burst_activity',
              'creator_burst_activity': True, 'creator_wallet_age_sec': 900.0,
              'censored': False}, False),
            # RPC failure must not silently look like a clean creator.
            ({'signature_count': None, 'ata_exists': None, 'token_balance_raw': None,
              'creator_guard_passed': False, 'creator_guard_reason': 'creator_history_unavailable',
              'censored': True}, False),
            ({'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
              'creator_guard_passed': True, 'creator_guard_reason': 'creator_guard_met',
              'censored': False}, True),
        )
        for index, (probe, b_should_open) in enumerate(cases):
            with self.subTest(probe=probe), tempfile.TemporaryDirectory() as tmp:
                simulator, _coin = await self._consider_both(Path(tmp) / f'case-{index}', probe)
                self.assertIn('mint-entry-test', simulator.books['portfolio_a'].positions)
                self.assertEqual(
                    'mint-entry-test' in simulator.books['portfolio_b'].positions,
                    b_should_open,
                )

    async def test_one_snapshot_timestamp_and_out_amount_entry_fill_with_min_out_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            coin = NewCoin(
                mint='mint-fill-test', source='Pump.fun', signature='sig-test',
                curve='curve-test', packet={'v_sol': 32.0},
            )
            coin.creator_probe_result = {
                'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '5',
                'creator_guard_passed': True,
                'creator_guard_reason': 'creator_guard_met',
                'censored': False,
            }
            snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
            decision_epoch = time.time()
            collector = SimpleNamespace(
                record_quote=Mock(return_value='quote-entry'),
                record_decision=Mock(), record_fill=Mock(),
            )
            with patch('portfolio_ab.get_collector', return_value=collector):
                await asyncio.gather(*(
                    simulator._consider_entry(
                        simulator.books[name], coin, snapshot,
                        32.0, 32.4, 200.0, {}, decision_epoch,
                    )
                    for name in ('portfolio_a', 'portfolio_b')
                ))
                pos_a = simulator.books['portfolio_a'].positions[coin.mint]
                pos_b = simulator.books['portfolio_b'].positions[coin.mint]
                self.assertEqual(pos_a.opened_at_epoch, decision_epoch)
                self.assertEqual(pos_b.opened_at_epoch, decision_epoch)
                self.assertEqual(pos_a.entry_out_amount_raw, pos_a.initial_tokens_raw)
                self.assertEqual(pos_a.initial_tokens_raw, pos_b.initial_tokens_raw)
                self.assertEqual(pos_a.initial_conservative_tokens_raw, pos_a.entry_min_out_raw)
                self.assertLess(pos_a.initial_conservative_tokens_raw, pos_a.initial_tokens_raw)
                self.assertAlmostEqual(simulator.books['portfolio_a'].cash_balance_rub, 1000.0, places=3)
                self.assertAlmostEqual(simulator.books['portfolio_b'].cash_balance_rub, 1000.0, places=3)

                local = quote_sell_exact_tokens(
                    snapshot.curve, snapshot.fee_context, pos_a.remaining_tokens_raw,
                    slippage_bps=100, context_slot=12,
                )
                swap = SwapQuote(
                    input_mint=coin.mint, output_mint=WSOL_MINT,
                    in_amount_raw=pos_a.remaining_tokens_raw,
                    out_amount_raw=local.out_amount_raw,
                    min_out_raw=local.min_out_raw,
                    price_impact_pct=local.price_impact_pct,
                    route_labels=local.route_labels(), context_slot=12,
                )
                executable = ExecutableQuote(
                    swap, local.protocol_fee_raw, local.creator_fee_raw,
                    local.protocol_fee_bps, local.creator_fee_bps,
                    local.market_cap_lamports, local.tier_index,
                    'Pump.fun bonding curve local exact quote',
                )
                await simulator._apply_sell_fill(
                    simulator.books['portfolio_a'], pos_a, pos_a.remaining_tokens_raw,
                    'Timeout (35s)', executable, snapshot.curve,
                    snapshot.fee_context, 12, 200.0, -5.0, 490.0,
                )
                self.assertNotIn(coin.mint, simulator.books['portfolio_a'].positions)
                collector.record_fill.assert_called_once()
                fill_args = collector.record_fill.call_args.args
                fill_kwargs = collector.record_fill.call_args.kwargs
                self.assertEqual(fill_kwargs['out_amount_raw'], local.out_amount_raw)
                self.assertEqual(fill_args[9], local.min_out_raw)
                self.assertEqual(fill_kwargs['snapshot_pnl_rub'], pos_a.realized_pnl_rub)
                self.assertIsNotNone(fill_kwargs['conservative_100bps_pnl_rub'])
                self.assertEqual(
                    fill_kwargs['conservative_min_out_raw'],
                    fill_kwargs['conservative_out_amount_raw'] * (10_000 - 100) // 10_000,
                )
                self.assertNotEqual(
                    fill_kwargs['conservative_100bps_pnl_rub'],
                    fill_kwargs['snapshot_pnl_rub'],
                )


class ABExitRuleTests(unittest.IsolatedAsyncioTestCase):
    def test_no_progress_is_checked_once_at_five_seconds(self) -> None:
        now = time.time()
        no_progress = make_position(opened_ago=6.0, best_pnl=3.99)
        no_progress.best_pnl_updated_at = now
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, no_progress, 1.49, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('No progress', trigger[0])
        self.assertEqual(trigger[1], no_progress.remaining_tokens_raw)
        self.assertTrue(no_progress.no_progress_checked)

        progressed = make_position(opened_ago=6.0, best_pnl=3.99)
        progressed.best_pnl_updated_at = now
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, progressed, 1.5, now)
        self.assertIsNone(trigger)
        self.assertTrue(progressed.no_progress_checked)

        peak_at_boundary = make_position(opened_ago=6.0, best_pnl=4.0)
        peak_at_boundary.best_pnl_updated_at = now
        self.assertIsNone(ABPaperSimulator._exit_trigger(None, PROFILE_A, peak_at_boundary, 1.0, now))

    def test_stop_loss_and_timeouts(self) -> None:
        stop = make_position(opened_ago=2.0, best_pnl=5.0)
        now = time.time()
        stop.best_pnl_updated_at = now
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, stop, -10.0, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertTrue(trigger[0].startswith('SL'))
        self.assertEqual(trigger[1], stop.initial_tokens_raw)

        before_trailing = make_position(opened_ago=35.0, best_pnl=9.99)
        after_trailing = make_position(opened_ago=90.0, best_pnl=10.0)
        now = time.time()
        before_trailing.opened_at_epoch = now - 35.0
        before_trailing.best_pnl_updated_at = now
        before_trailing.no_progress_checked = True
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, before_trailing, 2.0, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('Timeout', trigger[0])

        after_trailing.opened_at_epoch = now - 90.0
        after_trailing.best_pnl_updated_at = now
        after_trailing.no_progress_checked = True
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, after_trailing, 10.0, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('Timeout', trigger[0])

    def test_stagnation_at_plus_22_closes_full_position_after_4_5_seconds(self) -> None:
        now = 100.0
        pos = make_position(opened_ago=10.0, best_pnl=22.0)
        pos.best_pnl_updated_at = now - 4.5
        pos.no_progress_checked = True
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, pos, 22.0, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('Shelf stagnation', trigger[0])
        self.assertEqual(trigger[1], pos.initial_tokens_raw)

    def test_shelf_stagnation_after_tp1_closes_only_the_remaining_half(self) -> None:
        now = 100.0
        pos = make_position(opened_ago=10.0, best_pnl=28.0)
        pos.tp1_done = True
        pos.remaining_tokens_raw = pos.initial_tokens_raw // 2
        pos.best_pnl_updated_at = now - 4.5
        pos.no_progress_checked = True
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, pos, 25.0, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('Shelf stagnation', trigger[0])
        self.assertEqual(trigger[1], pos.initial_tokens_raw // 2)

    def test_tp1_zone_sells_half_but_exact_50_percent_bypasses_tp1(self) -> None:
        now = 100.0
        normal = make_position(opened_ago=1.0, best_pnl=32.0)
        normal.best_pnl_updated_at = now
        trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, normal, 32.0, now)
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('TP1', trigger[0])
        self.assertEqual(trigger[1], normal.initial_tokens_raw // 2)
        self.assertTrue(normal.tp1_partial_pending)
        self.assertFalse(normal.tp1_done)

        rocket = make_position(opened_ago=1.0, best_pnl=50.0)
        rocket.best_pnl_updated_at = now
        self.assertIsNone(ABPaperSimulator._exit_trigger(None, PROFILE_A, rocket, 50.0, now))
        self.assertTrue(rocket.tp1_rocket_bypass)
        self.assertFalse(rocket.tp1_done)
        self.assertFalse(rocket.tp1_partial_pending)
        self.assertEqual(rocket.remaining_tokens_raw, rocket.initial_tokens_raw)

    async def test_tp1_quote_unavailable_retries_same_partial_amount(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            book = simulator.books['portfolio_a']
            pos = make_position(opened_ago=1.0, best_pnl=32.0)
            now = time.time()
            pos.best_pnl_updated_at = now
            trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, pos, 32.0, now)
            assert trigger is not None
            reason, amount = trigger
            book.positions[pos.mint] = pos

            curve = make_curve()
            fresh = make_fee_context_for_test()
            full_local = quote_sell_exact_tokens(
                curve, fresh, pos.remaining_tokens_raw,
                slippage_bps=100, context_slot=12,
            )
            full_swap = SwapQuote(
                input_mint=pos.mint, output_mint=WSOL_MINT,
                in_amount_raw=pos.remaining_tokens_raw,
                out_amount_raw=full_local.out_amount_raw,
                min_out_raw=full_local.min_out_raw,
                price_impact_pct=full_local.price_impact_pct,
                route_labels=full_local.route_labels(), context_slot=12,
            )
            executable = ExecutableQuote(
                full_swap, full_local.protocol_fee_raw, full_local.creator_fee_raw,
                full_local.protocol_fee_bps, full_local.creator_fee_bps,
                full_local.market_cap_lamports, full_local.tier_index,
                'Pump.fun bonding curve local exact quote',
            )
            stale = SimpleNamespace(observed_at_epoch=time.time() - 10.0)
            with patch('portfolio_ab.get_collector', return_value=None):
                await simulator._apply_sell_fill(
                    book, pos, amount, reason, executable,
                    curve, stale, 12, 200.0, 32.0, 640.0,
                )
            self.assertFalse(pos.tp1_done)
            self.assertTrue(pos.tp1_partial_pending)
            self.assertEqual(pos.status, 'exit_pending')
            retry = ABPaperSimulator._exit_trigger(
                None, PROFILE_A, pos, -5.0, time.time(),
            )
            self.assertIsNotNone(retry)
            assert retry is not None
            self.assertIn('retry', retry[0])
            self.assertEqual(retry[1], amount)

    async def test_tp1_fill_marks_done_and_trailing_closes_only_remainder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            book = simulator.books['portfolio_a']
            book.cash_balance_rub = 1_000.0
            pos = make_position(opened_ago=1.0, best_pnl=32.0)
            base = time.time()
            pos.best_pnl_updated_at = base
            book.positions[pos.mint] = pos
            trigger = ABPaperSimulator._exit_trigger(None, PROFILE_A, pos, 32.0, base)
            assert trigger is not None
            reason, amount = trigger

            curve = make_curve()
            fee_context = make_fee_context_for_test()
            full_local = quote_sell_exact_tokens(
                curve, fee_context, pos.remaining_tokens_raw,
                slippage_bps=100, context_slot=12,
            )
            full_swap = SwapQuote(
                input_mint=pos.mint, output_mint=WSOL_MINT,
                in_amount_raw=pos.remaining_tokens_raw,
                out_amount_raw=full_local.out_amount_raw,
                min_out_raw=full_local.min_out_raw,
                price_impact_pct=full_local.price_impact_pct,
                route_labels=full_local.route_labels(), context_slot=12,
            )
            executable = ExecutableQuote(
                full_swap, full_local.protocol_fee_raw, full_local.creator_fee_raw,
                full_local.protocol_fee_bps, full_local.creator_fee_bps,
                full_local.market_cap_lamports, full_local.tier_index,
                'Pump.fun bonding curve local exact quote',
            )
            with patch('portfolio_ab.get_collector', return_value=None):
                await simulator._apply_sell_fill(
                    book, pos, amount, reason, executable,
                    curve, fee_context, 12, 200.0, 32.0, 640.0,
                )
            self.assertTrue(pos.tp1_done)
            self.assertFalse(pos.tp1_partial_pending)
            self.assertEqual(pos.tp1_pending_sell_raw, 0)
            self.assertEqual(pos.remaining_tokens_raw, pos.initial_tokens_raw // 2)
            self.assertAlmostEqual(pos.remaining_fraction, 0.5)

            self.assertIsNone(ABPaperSimulator._exit_trigger(
                None, PROFILE_A, pos, 40.0, base + 2.0,
            ))
            pos.best_pnl_pct = 50.0
            pos.best_pnl_updated_at = base + 3.0
            trigger = ABPaperSimulator._exit_trigger(
                None, PROFILE_A, pos, 42.5, base + 6.0,
            )
            self.assertIsNotNone(trigger)
            assert trigger is not None
            self.assertIn('SL/trail', trigger[0])
            self.assertEqual(trigger[1], pos.initial_tokens_raw // 2)

    def test_rocket_bypass_holds_through_100_then_exits_at_85_percent_of_peak(self) -> None:
        pos = make_position(opened_ago=1.0, best_pnl=-100.0)
        opened = pos.opened_at_epoch

        ABPaperSimulator._update_best_pnl(pos, PROFILE_A, 60.0, opened + 1.0)
        self.assertIsNone(ABPaperSimulator._exit_trigger(
            None, PROFILE_A, pos, 60.0, opened + 1.0,
        ))
        self.assertTrue(pos.tp1_rocket_bypass)
        self.assertEqual(pos.remaining_tokens_raw, pos.initial_tokens_raw)

        ABPaperSimulator._update_best_pnl(pos, PROFILE_A, 100.0, opened + 3.5)
        self.assertIsNone(ABPaperSimulator._exit_trigger(
            None, PROFILE_A, pos, 100.0, opened + 3.5,
        ))
        self.assertEqual(pos.best_pnl_updated_at, opened + 3.5)

        trigger = ABPaperSimulator._exit_trigger(
            None, PROFILE_A, pos, 85.0, opened + 4.5,
        )
        self.assertIsNotNone(trigger)
        assert trigger is not None
        self.assertIn('SL/trail', trigger[0])
        self.assertEqual(trigger[1], pos.initial_tokens_raw)

    def test_new_high_timestamp_uses_plus_0_4_pp_and_rising_marks_refresh_every_2_5s(self) -> None:
        pos = make_position(best_pnl=10.0)
        pos.best_pnl_updated_at = 1000.0
        self.assertFalse(ABPaperSimulator._update_best_pnl(pos, PROFILE_A, 10.39, 1002.5))
        self.assertEqual(pos.best_pnl_updated_at, 1000.0)
        self.assertTrue(ABPaperSimulator._update_best_pnl(pos, PROFILE_A, 10.4, 1005.0))
        self.assertEqual(pos.best_pnl_updated_at, 1005.0)

        rising = make_position(opened_ago=0.0, best_pnl=-100.0)
        rising.opened_at_epoch = 1000.0
        for index in range(4):
            now = 1000.5 + index * 2.5
            pnl = 1.0 + index * 0.5
            self.assertTrue(ABPaperSimulator._update_best_pnl(rising, PROFILE_A, pnl, now))
            self.assertIsNone(ABPaperSimulator._exit_trigger(
                None, PROFILE_A, rising, pnl, now,
            ))
            self.assertEqual(rising.best_pnl_updated_at, now)


class ABEarlyStopAndPollingTests(unittest.IsolatedAsyncioTestCase):
    async def test_early_stop_requires_both_books_empty_and_below_500(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            a = simulator.books['portfolio_a']
            b = simulator.books['portfolio_b']
            a.cash_balance_rub = 499.99
            b.cash_balance_rub = 500.0
            simulator._maybe_early_stop()
            self.assertFalse(simulator.early_stop_event.is_set())

            b.cash_balance_rub = 499.99
            b.positions['pending'] = make_position(status='exit_pending')
            simulator._maybe_early_stop()
            self.assertFalse(simulator.early_stop_event.is_set())

            b.positions.clear()
            b.positions['stale-closed'] = make_position(status='closed')
            simulator._maybe_early_stop()
            self.assertTrue(simulator.early_stop_event.is_set())
            self.assertEqual(simulator.early_stop_reason, 'both_portfolios_below_slot_size')
            self.assertEqual(simulator.summary()['early_stop_reason'], 'both_portfolios_below_slot_size')

    async def test_empty_slots_issue_no_position_poll_rpc(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            with patch('portfolio_ab.rpc_get_multiple_accounts', new=AsyncMock()) as rpc:
                task = asyncio.create_task(simulator._position_poll_loop())
                await asyncio.sleep(0.03)
                simulator._stopping.set()
                await asyncio.wait_for(task, timeout=0.5)
                rpc.assert_not_awaited()


class ABQuoteRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_active_curve_uses_local_quote_and_complete_curve_uses_jupiter_only(self) -> None:
        simulator = object.__new__(ABPaperSimulator)
        simulator.session = object()
        simulator.settings = SimpleNamespace(usd_rub_rate=90, exit_fee_sol=0.00005)
        pos = make_position()
        fee_context = make_fee_context_for_test()
        with patch('portfolio_ab.get_jupiter_quote', new_callable=AsyncMock) as jupiter:
            active = await simulator._build_sell_quote(
                pos, make_curve(), fee_context, 200.0, 12,
            )
            self.assertIsNotNone(active)
            assert active is not None
            self.assertIn('local exact', active.route)
            jupiter.assert_not_awaited()

            jupiter.return_value = SwapQuote(
                input_mint=pos.mint, output_mint=WSOL_MINT,
                in_amount_raw=pos.remaining_tokens_raw, out_amount_raw=10_000_000,
                min_out_raw=9_900_000, price_impact_pct=1.0,
                route_labels=('PumpSwap',), context_slot=99,
            )
            migrated = await simulator._build_sell_quote(
                pos, make_curve(complete=True), None, 200.0, 99,
            )
            self.assertIsNotNone(migrated)
            assert migrated is not None
            self.assertIn('complete=true', migrated.route)
            jupiter.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
