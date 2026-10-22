from __future__ import annotations

import asyncio
import json
import struct
import tempfile
import time
import unittest
from dataclasses import asdict, replace
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
    SHADOW_GATE,
    SHADOW_NO_TRAIL,
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
        entry_birth_max_sol=34.5,
        entry_delta_min_sol=0.10,
        entry_delta_max_sol=2.5,
        creator_guard_min_sig=25,
        creator_burst_window_sec=3600.0,
        creator_cache_ttl_sec=300.0,
    )


def make_simulator(root: str | Path, *, trades_queue=None, profiles=None):
    return ABPaperSimulator(
        object(), make_settings(), 'run-test', root,
        trades_queue or asyncio.Queue(), creator_watch=object(), creator_probe=object(),
        profiles=profiles,
    )


class ABProfileTests(unittest.TestCase):
    def test_run4_arms_share_the_entry_and_the_t0_option_is_off(self) -> None:
        # The t0 entry (delta_min = 0) is a DEFERRED experiment: both live arms
        # must enter exactly alike, otherwise the exit rule is not isolated.
        self.assertEqual(PROFILE_A.delta_min_sol, 0.10)
        self.assertEqual(PROFILE_B.delta_min_sol, 0.10)
        self.assertEqual(PROFILE_A.delta_max_sol, PROFILE_B.delta_max_sol)
        for profile in (PROFILE_A, PROFILE_B, SHADOW_GATE, SHADOW_NO_TRAIL):
            self.assertFalse(profile.entry_at_detection)
            self.assertFalse(profile.delta_max_from_birth_ceiling)
        # delta == 0 is rejected by both arms as a dead launch.
        self.assertEqual(PROFILE_A.entry_check(32.0, 32.0), (False, 'no_live_momentum'))
        self.assertEqual(PROFILE_B.entry_check(32.0, 32.0), (False, 'no_live_momentum'))
        self.assertEqual(PROFILE_A.entry_check(32.0, 32.10), (True, 'live_momentum_entry'))
        self.assertEqual(PROFILE_B.entry_check(32.0, 32.10), (True, 'live_momentum_entry'))
        # The birth band is shared and unchanged.
        for profile in (PROFILE_A, PROFILE_B):
            self.assertEqual(profile.entry_check(29.96, 30.2)[1], 'birth_out_of_band')
            self.assertEqual(profile.entry_check(39.88, 40.41)[1], 'birth_out_of_band')

    def test_shared_entry_rule_requires_live_momentum(self) -> None:
        for profile in (PROFILE_A, PROFILE_B):
            # Dead launch: no buys after the creator -> rejected by both arms.
            self.assertEqual(profile.entry_check(31.0, 31.0), (False, 'no_live_momentum'))
            # Dust delta below the floor is a dead launch in both arms.
            self.assertFalse(profile.entry_check(31.0, 31.05)[0])
            # Live early momentum passes in both arms.
            self.assertEqual(profile.entry_check(30.5, 31.3), (True, 'live_momentum_entry'))
            # Small live deltas still pass (the entry rule that replaced the cap).
            self.assertTrue(profile.entry_check(31.0, 31.5)[0])
            self.assertTrue(profile.entry_check(30.5, 31.4)[0])
            # PERFORMA was born at 39.88 — inside the old 50 SOL band, outside
            # the 34.5 one: the new ceiling is a dev-buy filter, not a delta one.
            self.assertEqual(profile.entry_check(39.88, 40.41)[1], 'birth_out_of_band')
            self.assertEqual(profile.delta_min_sol, 0.10)
            # Turtle / Shigure style late chases stay out of both arms.
            for birth, now in ((33.95, 45.51), (33.16, 38.59)):
                ok, reason = profile.entry_check(birth, now)
                self.assertFalse(ok)
                self.assertEqual(reason, 'momentum_too_late')
            # Birth band still guards supply-shape outliers.
            self.assertFalse(profile.entry_check(30.49, 31.3)[0])
            self.assertFalse(profile.entry_check(35.01, 35.5)[0])

    def test_birth_max_boundary_is_34_5_for_both_arms(self) -> None:
        for profile in (PROFILE_A, PROFILE_B):
            self.assertEqual(profile.v_sol_birth_max, 34.5)
            # Inclusive upper bound: 34.5 is admitted, 34.51 is not. Each arm
            # is probed with a delta it is willing to take: A needs >= 0.10
            # (fixed 2.5 cap), B enters at exactly 0 there (cap = 34.5 - 34.5).
            self.assertTrue(profile.entry_check(34.5, 34.7)[0])
            self.assertEqual(profile.entry_check(34.51, 34.8), (False, 'birth_out_of_band'))
            # The measured mechanism: v_sol_birth = 29.96 + dev_buy_SOL.
            self.assertEqual(profile.entry_check(29.96, 30.2), (False, 'birth_out_of_band'))
            self.assertEqual(profile.entry_check(29.96 + 4.55, 34.8), (False, 'birth_out_of_band'))
        # Both live arms share one fixed cap, so the same probe passes.
        self.assertTrue(PROFILE_A.entry_check(29.96 + 4.5, 34.7)[0])
        self.assertTrue(PROFILE_B.entry_check(29.96 + 4.5, 34.7)[0])
        self.assertEqual(PROFILE_A.entry_check(34.49, 37.2)[1], 'momentum_too_late')
        self.assertEqual(PROFILE_B.entry_check(34.49, 37.2)[1], 'momentum_too_late')
        # A keeps the 0.10 momentum floor and the 2.5 cap.
        self.assertEqual(PROFILE_A.entry_check(33.0, 33.09)[1], 'no_live_momentum')
        self.assertEqual(PROFILE_A.entry_check(33.0, 33.10), (True, 'live_momentum_entry'))
        self.assertEqual(PROFILE_A.entry_check(33.0, 35.5), (True, 'live_momentum_entry'))
        self.assertEqual(PROFILE_A.entry_check(33.0, 35.51)[1], 'momentum_too_late')

    def test_gate_status_separates_blind_spot_from_risk(self) -> None:
        from portfolio_ab import gate_status

        self.assertEqual(gate_status(True, 'creator_guard_met', {}), 'pass')
        # Fail-closed telemetry, not a verdict about the creator.
        for reason in ('creator_history_unavailable', 'creator_token_ata_missing',
                       'creator_guard_unavailable', 'creator_guard_censored'):
            self.assertEqual(gate_status(False, reason, {}), 'unavailable', reason)
        # Run #3 P0-2: a successfully read page with < 25 signatures is a
        # verdict (fail_history), not a blind spot.
        self.assertEqual(
            gate_status(False, 'creator_history_too_short',
                        {'status': 'ok', 'signature_count': 2}),
            'fail_history',
        )
        # A failed/censored read of the same reason stays unavailable.
        self.assertEqual(
            gate_status(False, 'creator_history_too_short',
                        {'status': 'error', 'signature_count': None}),
            'unavailable',
        )
        self.assertEqual(
            gate_status(False, 'creator_history_too_short',
                        {'status': 'ok', 'signature_count': 2, 'censored': True}),
            'unavailable',
        )
        # 25-signature floor hit on the curve fee vault while a real actor
        # exists = identity uncertainty (run #1: 4 of 12 A-only mints).
        self.assertEqual(
            gate_status(False, 'creator_history_too_short', {
                'creator_identity_type': 'pump_curve_creator',
                'pumpportal_event_actor': 'ActorWallet111',
            }),
            'unavailable',
        )
        # Same reason on a resolved deployer is a real risk signal.
        self.assertEqual(
            gate_status(False, 'creator_history_too_short', {
                'creator_identity_type': 'pumpportal_event_actor',
                'pumpportal_event_actor': 'ActorWallet111',
            }),
            'fail_risk',
        )
        for reason in ('creator_burst_activity', 'creator_repeat_deploy_in_session',
                       'creator_token_balance_zero'):
            self.assertEqual(gate_status(False, reason, {}), 'fail_risk', reason)

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
            sorted(flags),
            ['pass_if_cap_1', 'pass_if_cap_12', 'pass_if_cap_2.5', 'pass_if_cap_3', 'pass_if_cap_4'],
        )
        self.assertFalse(flags['pass_if_cap_1'])
        self.assertFalse(flags['pass_if_cap_2.5'])
        self.assertTrue(flags['pass_if_cap_3'])    # delta = 3.0, cap is inclusive
        self.assertTrue(flags['pass_if_cap_4'])
        self.assertTrue(flags['pass_if_cap_12'])

        dead = entry_shadow_flags(PROFILE_A, 31.0, 31.0)
        self.assertFalse(any(dead.values()), 'a dead delta==0 launch never passes any cap')
        out_of_band = entry_shadow_flags(PROFILE_A, 20.0, 20.5)
        self.assertFalse(any(out_of_band.values()), 'birth band applies to every cap')

    def test_entry_state_carries_dev_buy_fields_from_create_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            collector = SimpleNamespace(record_decision=Mock())
            coin = NewCoin(
                mint='mint-dev-buy', source='Pump.fun', signature='sig',
                curve='curve', packet={
                    'v_sol': 32.0,
                    'dev_buy_sol': 3.0,
                    'initial_buy_raw': 97545455,
                },
            )
            snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
            with patch('portfolio_ab.get_collector', return_value=collector):
                simulator.record_entry_state(coin, snapshot, 31.0, 31.5, time.time())
            self.assertEqual(collector.record_decision.call_count, 1)
            # record_decision(mint, source, stage, decision, reasons, values)
            args = collector.record_decision.call_args.args
            self.assertEqual(args[2], 'entry_state')
            self.assertEqual(args[5]['dev_buy_sol'], 3.0)
            self.assertEqual(args[5]['initial_buy_raw'], 97545455)
            self.assertTrue(args[5]['dev_bought_at_birth'])
            self.assertEqual(args[5]['delta_bucket'], '0.1-1')

            # A coin without the fields still writes both keys as None.
            collector.record_decision.reset_mock()
            bare = NewCoin(mint='mint-bare', source='Pump.fun', signature='sig2', packet={'v_sol': 32.0})
            with patch('portfolio_ab.get_collector', return_value=collector):
                simulator.record_entry_state(bare, snapshot, 31.0, 31.5, time.time())
            values = collector.record_decision.call_args.args[5]
            self.assertIn('dev_buy_sol', values)
            self.assertIn('initial_buy_raw', values)
            self.assertIsNone(values['dev_buy_sol'])
            self.assertIsNone(values['initial_buy_raw'])
            self.assertIsNone(values['dev_bought_at_birth'])

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
        # Run #4: B's ENTRY is A's entry — the shared env values must reach B,
        # otherwise the exit rule would not be the only difference.
        self.assertEqual(profiles['portfolio_b'].delta_min_sol, 0.5)
        self.assertEqual(profiles['portfolio_b'].delta_max_sol, 6.0)
        self.assertEqual(profiles['portfolio_b'].v_sol_birth_max, 44.0)
        self.assertFalse(profiles['portfolio_a'].require_creator_gate)
        self.assertFalse(profiles['portfolio_b'].require_creator_gate)
        # The gate now lives on the shadow only.
        self.assertTrue(profiles['shadow_gate'].require_creator_gate)
        self.assertEqual(profiles['shadow_gate'].guard_min_signatures, 30)
        self.assertEqual(profiles['shadow_gate'].capital_rub, 0.0)
        self.assertEqual(profiles['shadow_gate'].mirror_of, 'portfolio_a')
        # shadow_no_trail: A's entry, trail removed, 15 s clock.
        self.assertEqual(profiles['shadow_no_trail'].time_exit_sec, 15.0)
        self.assertEqual(profiles['shadow_no_trail'].mirror_of, 'portfolio_a')
        self.assertFalse(profiles['shadow_no_trail'].trailing_enabled)
        self.assertEqual(profiles['shadow_no_trail'].tp1_pct, 30.0)
        self.assertEqual(profiles['shadow_no_trail'].max_slots, 1)
        # shadow_exit25 was removed: it lost (-13.88 RUB) and cost RPC.
        self.assertNotIn('shadow_exit25', profiles)
        self.assertNotIn('shadow_t0_5s', profiles)
        for shadow in ('shadow_gate', 'shadow_no_trail'):
            self.assertTrue(profiles[shadow].is_shadow)
            self.assertEqual(profiles[shadow].capital_rub, 0.0)
            self.assertEqual(profiles[shadow].initial_balance_rub, 1500.0)
        # The latency model is configurable and lands in every profile.
        latency_settings = SimpleNamespace(**{**vars(base), 'entry_latency_ms': 250.0})
        self.assertEqual(
            profiles_from_settings(latency_settings)['portfolio_b'].entry_latency_ms, 250.0,
        )
        self.assertEqual(PROFILE_B.entry_latency_ms, 150.0)

    def test_arms_differ_only_in_the_exit_rule(self) -> None:
        a, b = asdict(PROFILE_A), asdict(PROFILE_B)
        a.pop('name')
        b.pop('name')
        # The declared single difference: the exit block.
        exit_fields = (
            'time_exit_sec', 'stop_loss_pct', 'stop_loss_always', 'tp1_pct',
            'tp1_sell_pct', 'rocket_bypass_pct', 'trailing_enabled',
            'shelf_enabled', 'no_progress_enabled',
        )
        for field in exit_fields:
            a.pop(field)
            b.pop(field)
        self.assertEqual(a, b)
        # Entry, stake, slots and the instrument are identical.
        self.assertEqual(PROFILE_A.delta_min_sol, PROFILE_B.delta_min_sol)
        self.assertEqual(PROFILE_A.delta_max_sol, PROFILE_B.delta_max_sol)
        self.assertEqual(PROFILE_A.entry_latency_ms, PROFILE_B.entry_latency_ms)
        self.assertEqual((PROFILE_A.initial_balance_rub, PROFILE_B.initial_balance_rub), (1500, 1500))
        self.assertEqual((PROFILE_A.position_size_rub, PROFILE_B.position_size_rub), (500, 500))
        self.assertEqual((PROFILE_A.max_slots, PROFILE_B.max_slots), (3, 3))
        self.assertFalse(PROFILE_A.require_creator_gate)
        self.assertFalse(PROFILE_B.require_creator_gate)
        # A is the Round 6 reference: every exit number untouched.
        self.assertIsNone(PROFILE_A.time_exit_sec)
        self.assertEqual(PROFILE_A.stop_loss_pct, 10.0)
        self.assertEqual(PROFILE_A.tp1_pct, 30.0)
        self.assertEqual(PROFILE_A.rocket_bypass_pct, 50.0)
        self.assertTrue(PROFILE_A.trailing_enabled)
        self.assertTrue(PROFILE_A.shelf_enabled)
        self.assertTrue(PROFILE_A.no_progress_enabled)
        self.assertFalse(PROFILE_A.stop_loss_always)
        # B: clock instead of trail.
        self.assertEqual(PROFILE_B.time_exit_sec, 15.0)
        self.assertEqual(PROFILE_B.stop_loss_pct, 20.0)
        self.assertTrue(PROFILE_B.stop_loss_always)
        self.assertEqual(PROFILE_B.tp1_pct, 100.0)
        self.assertEqual(PROFILE_B.tp1_sell_pct, 50.0)
        self.assertFalse(PROFILE_B.trailing_enabled)
        self.assertFalse(PROFILE_B.shelf_enabled)
        self.assertFalse(PROFILE_B.no_progress_enabled)

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


class RunThreeShadowTests(unittest.IsolatedAsyncioTestCase):
    """Run #3 invariants: one live difference, shadows are isolated."""

    async def _open(self, simulator, mint, delta, decision_epoch):
        coin = NewCoin(
            mint=mint, source='Pump.fun', signature=f'sig-{mint}',
            curve='curve-test', packet={'v_sol': 32.0},
        )
        coin.creator_probe_result = {
            'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
            'creator_guard_passed': True, 'creator_guard_reason': 'creator_guard_met',
            'censored': False, 'status': 'ok',
        }
        snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
        with patch('portfolio_ab.get_collector', return_value=None):
            for name in ('portfolio_a', 'portfolio_b'):
                await simulator._consider_entry(
                    simulator.books[name], coin, snapshot,
                    32.0, 32.0 + delta, 200.0, {}, decision_epoch,
                )
            # Mirrors are evaluated after the live arms, exactly as in
            # _on_candidate.
            for name, book in simulator.books.items():
                if book.profile.mirror_of:
                    await simulator._consider_entry(
                        book, coin, snapshot,
                        32.0, 32.0 + delta, 200.0, {}, decision_epoch,
                    )
        return coin

    async def test_run4_the_arms_enter_alike_and_only_the_exit_splits_them(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            # Below A's 0.10 momentum floor: neither arm enters (the t0 entry is
            # a deferred experiment, not part of this run).
            await self._open(simulator, 'mint-no-momentum', 0.05, time.time())
            self.assertNotIn('mint-no-momentum', simulator.books['portfolio_a'].positions)
            self.assertNotIn('mint-no-momentum', simulator.books['portfolio_b'].positions)
            # Ordinary momentum: both live arms take the same coin...
            await self._open(simulator, 'mint-momentum', 1.2, time.time())
            self.assertIn('mint-momentum', simulator.books['portfolio_a'].positions)
            self.assertIn('mint-momentum', simulator.books['portfolio_b'].positions)
            a = simulator.books['portfolio_a'].positions['mint-momentum']
            b = simulator.books['portfolio_b'].positions['mint-momentum']
            # ...at the same price and size; only the exit rule differs.
            self.assertEqual(a.entry_price_usd, b.entry_price_usd)
            self.assertEqual(a.entry_cost_rub, b.entry_cost_rub)
            self.assertEqual(a.remaining_tokens_raw, b.remaining_tokens_raw)
            # Late chase: neither arm.
            await self._open(simulator, 'mint-too-late', 3.0, time.time())
            self.assertNotIn('mint-too-late', simulator.books['portfolio_a'].positions)
            self.assertNotIn('mint-too-late', simulator.books['portfolio_b'].positions)

    async def test_shadow_no_trail_holds_to_the_clock_and_has_no_trail(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        pos = SimpleNamespace(
            remaining_tokens_raw=1_000, opened_at_epoch=100.0, tp1_done=False,
            tp1_rocket_bypass=False, tp1_partial_pending=False,
            tp1_pending_sell_raw=0, best_pnl_pct=40.0,
            best_pnl_updated_at=101.0, no_progress_checked=False,
        )
        # The trail would fire here (peak 40%, floor 34%) — the shadow has none.
        self.assertIsNone(simulator._exit_trigger(SHADOW_NO_TRAIL, pos, 20.0, 104.0))
        # TP1 +30% still sells half, exactly as in A.
        trigger = simulator._exit_trigger(SHADOW_NO_TRAIL, pos, 31.0, 102.0)
        self.assertTrue(trigger[0].startswith('TP1 +30%'))
        self.assertEqual(trigger[1], 500)
        # At 15 s the rest goes out at market.
        trigger = simulator._exit_trigger(SHADOW_NO_TRAIL, pos, -5.0, 115.0)
        self.assertTrue(trigger[0].startswith('time_exit_15s'))
        self.assertEqual(trigger[1], 1_000)
        # A keeps its trail: the same mark closes A.
        live = SimpleNamespace(
            remaining_tokens_raw=1_000, opened_at_epoch=100.0, tp1_done=False,
            tp1_rocket_bypass=False, tp1_partial_pending=False,
            tp1_pending_sell_raw=0, best_pnl_pct=40.0,
            best_pnl_updated_at=101.0, no_progress_checked=False,
        )
        self.assertTrue(
            simulator._exit_trigger(PROFILE_A, live, 20.0, 104.0)[0].startswith('SL/trail')
        )

    async def test_disabling_shadows_does_not_change_the_live_arms(self) -> None:
        epoch = 1_700_000_000.0
        with tempfile.TemporaryDirectory() as tmp:
            full = make_simulator(
                Path(tmp) / 'with-shadows',
                profiles={
                    'portfolio_a': PROFILE_A, 'portfolio_b': PROFILE_B,
                    'shadow_gate': SHADOW_GATE, 'shadow_no_trail': SHADOW_NO_TRAIL,
                },
            )
            live_only = make_simulator(
                Path(tmp) / 'without-shadows',
                profiles={'portfolio_a': PROFILE_A, 'portfolio_b': PROFILE_B},
            )
            for mint in ('mint-iso-1', 'mint-iso-2'):
                await self._open(full, mint, 0.4, epoch + 1.0)
                await self._open(live_only, mint, 0.4, epoch + 1.0)
            for name in ('portfolio_a', 'portfolio_b'):
                a, b = full.books[name], live_only.books[name]
                self.assertEqual(round(a.cash_balance_rub, 6), round(b.cash_balance_rub, 6))
                self.assertEqual(sorted(a.positions), sorted(b.positions))
                self.assertEqual(a.entry_count, b.entry_count)
                self.assertEqual(a.closed_trades, b.closed_trades)
            # The shadows really did mirror A, with zero real capital. They
            # are capped at ONE position each (a shadow must not buy RPC budget
            # from the live arms), so with A holding two slots the mirror can
            # only carry the first.
            for name in ('shadow_gate', 'shadow_no_trail'):
                shadow = full.books[name]
                self.assertEqual(shadow.profile.max_slots, 1)
                self.assertEqual(sorted(shadow.positions), ['mint-iso-1'])
                self.assertTrue(set(shadow.positions) <= set(full.books['portfolio_a'].positions))
                self.assertEqual(shadow.profile.capital_rub, 0.0)


class RunThreeBankAndStopListTests(unittest.IsolatedAsyncioTestCase):
    """Addendum: bank top-ups, bust metric and the four-reason stop-list."""

    def _live_profiles(self, **overrides):
        a = replace(PROFILE_A, **overrides)
        return {'portfolio_a': a, 'portfolio_b': replace(PROFILE_B, **overrides)}

    async def _consider(self, simulator, coin, delta=0.4, epoch=None):
        snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
        epoch = time.time() if epoch is None else epoch
        with patch('portfolio_ab.get_collector', return_value=None):
            for name in ('portfolio_a', 'portfolio_b'):
                await simulator._consider_entry(
                    simulator.books[name], coin, snapshot,
                    32.0, 32.0 + delta, 200.0, {}, epoch,
                )

    def _coin(self, mint):
        coin = NewCoin(
            mint=mint, source='Pump.fun', signature=f'sig-{mint}',
            curve='curve-test', packet={'v_sol': 32.0},
        )
        coin.creator_probe_result = {
            'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
            'creator_guard_passed': True, 'creator_guard_reason': 'creator_guard_met',
            'censored': False, 'status': 'ok',
        }
        return coin

    async def test_minus_1200_realized_gives_exactly_one_top_up_and_the_trade_opens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(Path(tmp) / 'bank')
            book = simulator.books['portfolio_a']
            # cumPnL = -1200 -> cash 300, below one 500 RUB slot.
            book.realized_pnl_rub = -1_200.0
            book.cash_balance_rub = 300.0
            await self._consider(simulator, self._coin('mint-bank-1'))
            self.assertIn('mint-bank-1', book.positions)
            self.assertEqual(book.top_ups, 1)
            self.assertEqual(len(book.top_up_events), 1)
            event = book.top_up_events[0]
            self.assertLess(event['cash_before'], 500)
            self.assertEqual(event['cash_after'], 1_500.0)
            self.assertAlmostEqual(event['amount_rub'], 1_200.0, places=6)
            self.assertAlmostEqual(book.capital_injected_rub, 1_200.0, places=6)
            # The trade itself is a normal 500 RUB slot: 1500 - 500 = 1000.
            self.assertAlmostEqual(book.cash_balance_rub, 1_000.0, places=3)
            # No second injection until the cash drops below the slot again.
            self.assertIsNone(book.maybe_top_up())
            # The real 1500 RUB bank would have died here (equity 300 < 500).
            self.assertEqual(book.bust_at_trade, 0)
            self.assertIsNotNone(book.bust_at_utc)
            self.assertAlmostEqual(book.equity_no_topups(), 300.0, places=6)

    async def test_top_up_changes_only_availability_not_a_single_fill(self) -> None:
        epoch = 1_700_000_000.0
        coins = [self._coin('mint-iso-a'), self._coin('mint-iso-b')]
        with tempfile.TemporaryDirectory() as tmp:
            topped = make_simulator(
                Path(tmp) / 'topped', profiles=self._live_profiles(auto_top_up=True),
            )
            plain = make_simulator(
                Path(tmp) / 'plain', profiles=self._live_profiles(auto_top_up=False),
            )
            # Only A is drained; B keeps its normal 1500 RUB as the twin whose
            # fills the topped-up A must reproduce exactly.
            for simulator in (topped, plain):
                simulator.books['portfolio_a'].cash_balance_rub = 300.0
            for coin in coins:
                await self._consider(topped, coin, epoch=epoch)
                await self._consider(plain, coin, epoch=epoch)
            # Without the top-up both arms are stuck at 300 RUB.
            self.assertEqual(plain.books['portfolio_a'].positions, {})
            self.assertEqual(plain.books['portfolio_a'].top_ups, 0)
            self.assertEqual(plain.books['portfolio_a'].cash_below_slot_count, 2)
            # With the top-up the same coins open, at the unchanged slot size.
            self.assertEqual(sorted(topped.books['portfolio_a'].positions),
                             ['mint-iso-a', 'mint-iso-b'])
            for mint in ('mint-iso-a', 'mint-iso-b'):
                pos = topped.books['portfolio_a'].positions[mint]
                self.assertAlmostEqual(pos.entry_cost_rub, 500.0, places=3)
                # Identical inputs give an identical fill at the same slot size:
                # B never needed a top-up, so compare A against B.
                twin = topped.books['portfolio_b'].positions[mint]
                self.assertGreater(pos.initial_tokens_raw, 0)
                self.assertEqual(pos.entry_amount_lamports, twin.entry_amount_lamports)
                self.assertEqual(pos.initial_tokens_raw, twin.initial_tokens_raw)
                self.assertEqual(pos.entry_price_usd, twin.entry_price_usd)
            # Only the A book got injections; B was never short.
            self.assertEqual(topped.books['portfolio_b'].top_ups, 0)
            self.assertEqual(topped.books['portfolio_a'].top_ups, 1)
            # ...and the actual paper_fills are byte-identical to the twin's.
            fills = await self._sell_both_topped_and_twin(topped, 'mint-iso-a')
            a_args, a_kwargs = fills['portfolio_a']
            b_args, b_kwargs = fills['portfolio_b']
            self.assertEqual(a_args, b_args)
            self.assertEqual(
                {k: v for k, v in a_kwargs.items() if k != 'portfolio'},
                {k: v for k, v in b_kwargs.items() if k != 'portfolio'},
            )
            self.assertEqual(a_kwargs.get('portfolio'), 'portfolio_a')
            self.assertEqual(b_kwargs.get('portfolio'), 'portfolio_b')

    async def _sell_both_topped_and_twin(self, simulator, mint):
        """Sell the same position out of A (topped) and B (never short).

        Returns the `record_fill` argument tuple per book minus the portfolio,
        which is what "paper_fills are unchanged" actually means.
        """
        collector = SimpleNamespace(record_fill=Mock(), record_decision=Mock(),
                                    record_quote=Mock(return_value='q'))
        captured = {}
        curve = make_curve()
        fees = make_fee_context_for_test()
        with patch('portfolio_ab.get_collector', return_value=collector):
            for name in ('portfolio_a', 'portfolio_b'):
                book = simulator.books[name]
                pos = book.positions[mint]
                local = quote_sell_exact_tokens(
                    curve, fees, pos.remaining_tokens_raw,
                    slippage_bps=100, context_slot=12,
                )
                swap = SwapQuote(
                    input_mint=mint, output_mint=WSOL_MINT,
                    in_amount_raw=pos.remaining_tokens_raw,
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
                    book, pos, pos.remaining_tokens_raw, 'Timeout (35s)',
                    executable, curve, fees, 12, 200.0, -5.0, 490.0,
                )
                call = collector.record_fill.call_args
                captured[name] = (call.args, call.kwargs)
                collector.record_fill.reset_mock()
        return captured

    async def test_summary_reports_bank_fields_per_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(Path(tmp) / 'summary')
            book = simulator.books['portfolio_b']
            book.realized_pnl_rub = -1_150.0
            book.cash_balance_rub = 350.0
            await self._consider(simulator, self._coin('mint-bank-2'))
            summary = simulator.summary()
        for name in ('portfolio_a', 'portfolio_b', 'shadow_gate', 'shadow_no_trail'):
            entry = summary[name]
            for key in ('top_ups', 'capital_injected_rub', 'pnl_with_topups',
                        'equity_no_topups', 'bust_at_trade', 'bust_at_utc'):
                self.assertIn(key, entry, (name, key))
        b = summary['portfolio_b']
        self.assertEqual(b['top_ups'], 1)
        self.assertAlmostEqual(b['capital_injected_rub'], 1_150.0, places=6)
        self.assertAlmostEqual(b['pnl_with_topups'], -1_150.0, places=6)
        self.assertAlmostEqual(b['equity_no_topups'], 350.0, places=6)
        self.assertEqual(b['bust_at_trade'], 0)
        self.assertIsNotNone(b['bust_at_utc'])
        self.assertIn('stop_policy', summary)

    def test_stop_list_telemetry_loss_stops_and_is_recorded(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        reason = simulator._evaluate_stop_reason(
            now=1_000.0, telemetry={'dropped_events': 3, 'write_errors': 0},
        )
        self.assertTrue(reason.startswith('telemetry_loss'), reason)
        self.assertIn('dropped_events=3', reason)
        # The event is what the main loop waits on; artifacts are written by
        # close(), which is covered below.
        simulator.early_stop_reason = None
        with patch.object(simulator, '_evaluate_stop_reason', return_value=reason):
            simulator._maybe_early_stop()
        self.assertTrue(simulator.early_stop_event.is_set())
        self.assertEqual(simulator.early_stop_reason, reason)
        self.assertEqual(simulator.summary()['early_stop_reason'], reason)
        self.assertEqual(simulator.summary()['stop_policy']['reason'], reason)

    def test_stop_list_rpc_stall_and_resource_reasons(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        base = {'requests': 0, 'failed_requests': 0, 'latency_ms_p95': 100.0,
                'unrecovered_failures': 0}
        self.assertIsNone(simulator._evaluate_stop_reason(
            now=100.0, telemetry={}, rpc=dict(base), disk_free_bytes=10 ** 12,
            quota_pct=50.0,
        ))
        # Run #4: a 429 burst that the retry layer recovers is back-pressure,
        # not a telemetry failure — raw failures must not stop the run.
        self.assertIsNone(simulator._evaluate_stop_reason(
            now=700.0, telemetry={},
            rpc={'requests': 2_000, 'failed_requests': 400,
                 'latency_ms_p95': 100.0, 'unrecovered_failures': 0},
            disk_free_bytes=10 ** 12,
        ))
        # 3 % UNRECOVERED reads is above the 2 % threshold.
        reason = simulator._evaluate_stop_reason(
            now=1_300.0, telemetry={},
            rpc={'requests': 4_000, 'failed_requests': 900,
                 'latency_ms_p95': 100.0, 'unrecovered_failures': 60},
            disk_free_bytes=10 ** 12,
        )
        self.assertTrue(reason.startswith('rpc_unrecovered_rate'), reason)
        # 1.5 % unrecovered stays below it.
        self.assertIsNone(simulator._evaluate_stop_reason(
            now=1_900.0, telemetry={},
            rpc={'requests': 6_000, 'failed_requests': 1_500,
                 'latency_ms_p95': 100.0, 'unrecovered_failures': 90},
            disk_free_bytes=10 ** 12,
        ))
        # Latency must hold for five minutes, not one sample.
        slow = make_simulator(tempfile.mkdtemp())
        self.assertIsNone(slow._evaluate_stop_reason(
            now=1_000.0, telemetry={},
            rpc={'requests': 1, 'failed_requests': 0, 'latency_ms_p95': 2_500.0},
            disk_free_bytes=10 ** 12,
        ))
        reason = slow._evaluate_stop_reason(
            now=1_400.0, telemetry={},
            rpc={'requests': 2, 'failed_requests': 0, 'latency_ms_p95': 2_500.0},
            disk_free_bytes=10 ** 12,
        )
        self.assertTrue(reason.startswith('rpc_latency_p95'), reason)
        # Stall: 30 min without an entry while entry_state keeps growing.
        stalled = make_simulator(tempfile.mkdtemp())
        stalled._entry_state_total = 40
        reason = stalled._evaluate_stop_reason(
            now=stalled.started_at_epoch + 1_801.0, telemetry={},
            rpc={'requests': 0, 'failed_requests': 0, 'latency_ms_p95': None},
            disk_free_bytes=10 ** 12,
        )
        self.assertTrue(reason.startswith('portfolio_stalled'), reason)
        # A quiet market (no new candidates) is not a reason to stop.
        quiet = make_simulator(tempfile.mkdtemp())
        self.assertIsNone(quiet._evaluate_stop_reason(
            now=quiet.started_at_epoch + 1_801.0, telemetry={},
            rpc={'requests': 0, 'failed_requests': 0, 'latency_ms_p95': None},
            disk_free_bytes=10 ** 12,
        ))
        # Resources.
        self.assertTrue(make_simulator(tempfile.mkdtemp())._evaluate_stop_reason(
            now=1.0, telemetry={}, rpc={'requests': 0, 'failed_requests': 0,
                                        'latency_ms_p95': None},
            disk_free_bytes=1024 ** 3,
        ).startswith('disk_low'))
        self.assertTrue(make_simulator(tempfile.mkdtemp())._evaluate_stop_reason(
            now=1.0, telemetry={}, rpc={'requests': 0, 'failed_requests': 0,
                                        'latency_ms_p95': None},
            disk_free_bytes=10 ** 12, quota_pct=4.0,
        ).startswith('helius_quota_low'))

    async def test_close_after_stop_keeps_artifacts_and_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / 'stop-run'
            simulator = make_simulator(run_dir)
            with patch.object(simulator, 'write_ticks'), \
                    patch.object(simulator, 'write_shadow_fills'):
                simulator.early_stop_reason = 'telemetry_loss (dropped_events=1, write_errors=0)'
                simulator.stop_policy_state['reason'] = simulator.early_stop_reason
                summary = await simulator.close()
            self.assertEqual(summary['early_stop_reason'],
                             'telemetry_loss (dropped_events=1, write_errors=0)')
            written = json.loads((run_dir / 'ab_summary.json').read_text())
            self.assertEqual(written['early_stop_reason'],
                             'telemetry_loss (dropped_events=1, write_errors=0)')
            self.assertEqual(written['stop_policy']['reason'],
                             'telemetry_loss (dropped_events=1, write_errors=0)')
            # The run is not zeroed out: wallets and per-profile books remain.
            self.assertIn('portfolio_a', written)

    async def test_pnl_alone_never_stops_the_run(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        for book in simulator.books.values():
            if not book.profile.is_shadow:
                book.realized_pnl_rub = -5_000.0
                book.cash_balance_rub = 0.0
        simulator._entry_state_total = 0
        simulator._rpc_samples = []
        self.assertIsNone(simulator._evaluate_stop_reason(
            now=1_000.0, telemetry={},
            rpc={'requests': 1, 'failed_requests': 0, 'latency_ms_p95': 90.0},
            disk_free_bytes=10 ** 12, quota_pct=90.0,
        ))
        simulator._maybe_early_stop()
        self.assertIsNone(simulator.early_stop_reason)


class ABEntryGateAndAccountingTests(unittest.IsolatedAsyncioTestCase):
    async def _consider_both(self, root, creator_probe_result):
        # Run #3: the gate is a shadow now, so "both" = portfolio_a + shadow_gate.
        simulator = make_simulator(
            root, profiles={'portfolio_a': PROFILE_A, 'shadow_gate': SHADOW_GATE},
        )
        coin = NewCoin(
            mint='mint-entry-test', source='Pump.fun', signature='sig-test',
            curve='curve-test', packet={'v_sol': 32.0},
        )
        coin.creator_probe_result = creator_probe_result
        snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
        decision_epoch = time.time()
        with patch('portfolio_ab.get_collector', return_value=None):
            # A first: the shadow mirrors an existing A position, so the two
            # calls must be sequential (the mirror checks A's book).
            for name in ('portfolio_a', 'shadow_gate'):
                await simulator._consider_entry(
                    simulator.books[name], coin, snapshot,
                    32.0, 32.4, 200.0, {}, decision_epoch,
                )
        return simulator, coin

    async def test_gate_lives_in_shadow_so_b_is_never_filtered_by_creator(self) -> None:
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
                    'mint-entry-test' in simulator.books['shadow_gate'].positions,
                    b_should_open,
                )
                # The live B arm never looks at the creator: one difference only.
                self.assertFalse(PROFILE_B.require_creator_gate)

    async def test_b_enters_the_same_coin_a_does_even_for_bad_creator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # A rug-farm creator that the shadow gate rejects: B still trades.
            simulator, coin = await self._consider_both(
                Path(tmp),
                {'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
                 'creator_guard_passed': False, 'creator_guard_reason': 'creator_burst_activity',
                 'creator_burst_activity': True, 'censored': False},
            )
            self.assertIn('mint-entry-test', simulator.books['portfolio_a'].positions)
            self.assertNotIn('mint-entry-test', simulator.books['shadow_gate'].positions)

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
            # The legacy depleted net now applies only to books that opted out
            # of the run #3 top-up; with it on, a book refills and never stops.
            simulator = make_simulator(tmp, profiles={
                'portfolio_a': replace(PROFILE_A, auto_top_up=False),
                'portfolio_b': replace(PROFILE_B, auto_top_up=False),
            })
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


class RunFourEntryQualityTests(unittest.IsolatedAsyncioTestCase):
    """Run #4 acceptance items 1-4: the premium, the latency, the 5-second book."""

    async def _open(self, simulator, mint, birth, now_v_sol, decision_epoch):
        coin = NewCoin(
            mint=mint, source='Pump.fun', signature=f'sig-{mint}',
            curve='curve-test', packet={'v_sol': birth},
        )
        coin.creator_probe_result = {
            'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
            'creator_guard_passed': True, 'creator_guard_reason': 'creator_guard_met',
            'censored': False, 'status': 'ok',
        }
        snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
        with patch('portfolio_ab.get_collector', return_value=None):
            for book in simulator.books.values():
                await simulator._consider_entry(
                    book, coin, snapshot, birth, now_v_sol,
                    200.0, {}, decision_epoch,
                )
        return coin

    async def test_entry_premium_is_measured_against_t0_not_the_packet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            # Along the curve price = v_sol^2 / k, so paying 2 % more than the
            # coin's t0 price means v_sol grew by sqrt(1.02) - 1 = 0.995 %.
            await self._open(simulator, 'mint-premium', 32.0, 32.32, time.time())
            book = simulator.books['portfolio_a']
            pos = book.positions['mint-premium']
            expected = ((32.32 / 32.0) ** 2 - 1.0) * 100.0
            self.assertAlmostEqual(pos.entry_premium_pct, expected, places=3)
            # The wait premium is NOT the fee: the same-snapshot number stays
            # small and is reported separately.
            self.assertIsNotNone(pos.entry_fill_premium_pct)
            self.assertLess(abs(pos.entry_fill_premium_pct), 6.0)
            # Both arms price the same curve read, so the premium is equal.
            b_pos = simulator.books['portfolio_b'].positions['mint-premium']
            self.assertAlmostEqual(
                b_pos.entry_premium_pct, pos.entry_premium_pct, places=6,
            )
            # The t0 option belongs to the deferred delta_min=0 experiment.
            self.assertFalse(simulator.books['portfolio_b'].profile.entry_at_detection)

    async def test_entry_lag_and_latency_are_recorded_per_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            decision = time.time()
            snapshot_observed = decision - 0.4      # read 400 ms before deciding
            coin = NewCoin(
                mint='mint-lag', source='Pump.fun', signature='sig-lag',
                curve='curve-test', packet={'v_sol': 32.0},
            )
            coin.creator_probe_result = {
                'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1',
                'creator_guard_passed': True, 'creator_guard_reason': 'creator_guard_met',
                'censored': False, 'status': 'ok',
            }
            coin.found_at = decision - 0.95
            snapshot = EntrySnapshot(
                make_curve(), make_fee_context_for_test(), 12, snapshot_observed,
            )
            with patch('portfolio_ab.get_collector', return_value=None):
                for book in simulator.books.values():
                    await simulator._consider_entry(
                        book, coin, snapshot, 32.0, 32.32, 200.0, {}, decision,
                    )
            pos = simulator.books['portfolio_a'].positions['mint-lag']
            # Measured from the curve read, not from the decision timestamp.
            self.assertAlmostEqual(pos.entry_lag_ms, 550.0, delta=5.0)
            self.assertEqual(pos.entry_latency_ms, 150.0)
            self.assertTrue(pos.entry_latency_floor_met)
            self.assertLess(pos.entry_lag_ms, 1000.0)
            quality = simulator._entry_quality(simulator.books['portfolio_a'])
            self.assertEqual(quality['entry_lag_ms_over_1000'], 0)
            self.assertEqual(quality['entry_latency_ms_model'], 150.0)
            self.assertEqual(simulator.entry_latency_floor_violations, 0)

    async def test_entry_latency_floor_delays_the_decision_without_an_rpc(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        coin = SimpleNamespace(found_at=time.time())
        started = time.monotonic()
        waited = await simulator._await_entry_latency_floor(coin)
        elapsed = time.monotonic() - started
        self.assertGreater(waited, 0.10)
        self.assertGreater(elapsed, 0.10)
        self.assertLess(elapsed, 0.60)
        # An old candidate is never delayed.
        coin.found_at = time.time() - 5.0
        self.assertEqual(await simulator._await_entry_latency_floor(coin), 0.0)

    def test_live_arm_b_exits_on_the_clock_and_honours_sl_and_tp(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        now = 1_000.0

        def position(age: float, **fields) -> SimpleNamespace:
            base = dict(
                remaining_tokens_raw=1_000, opened_at_epoch=now - age,
                tp1_done=False, tp1_rocket_bypass=False,
                tp1_partial_pending=False, tp1_pending_sell_raw=0,
                best_pnl_pct=0.0, best_pnl_updated_at=now - age,
                no_progress_checked=False,
            )
            base.update(fields)
            return SimpleNamespace(**base)

        # Nothing closes B before the clock, not even a -19% drawdown.
        self.assertIsNone(simulator._exit_trigger(PROFILE_B, position(14.9), -19.0, now))
        # At 15.0 s exactly: 100% at market, whatever the PnL.
        for pnl in (-19.0, 0.0, 40.0, 200.0):
            trigger = simulator._exit_trigger(
                PROFILE_B, position(15.0, tp1_done=pnl > 100), pnl, now,
            )
            self.assertIsNotNone(trigger, pnl)
            self.assertTrue(trigger[0].startswith('time_exit_15s'), trigger)
            self.assertEqual(trigger[1], 1_000)
        # SL -20% (not -10%) closes everything, before or after TP1.
        self.assertIsNone(simulator._exit_trigger(PROFILE_B, position(10.0), -19.9, now))
        trigger = simulator._exit_trigger(PROFILE_B, position(10.0), -20.0, now)
        self.assertTrue(trigger[0].startswith('SL'))
        self.assertEqual(trigger[1], 1_000)
        self.assertTrue(
            simulator._exit_trigger(
                PROFILE_B, position(10.0, tp1_done=True), -21.0, now,
            )[0].startswith('SL')
        )
        # TP +100% sells exactly half and leaves the rest to the clock.
        trigger = simulator._exit_trigger(PROFILE_B, position(10.0), 100.0, now)
        self.assertTrue(trigger[0].startswith('TP1 +100% partial 50%'), trigger)
        self.assertEqual(trigger[1], 500)
        # The trail, the shelf and no-progress are off for B: a position that
        # peaked at +40% and fell back to +20% is NOT closed at 10 s.
        self.assertIsNone(
            simulator._exit_trigger(
                PROFILE_B,
                position(10.0, best_pnl_pct=40.0, best_pnl_updated_at=now - 9.0),
                20.0, now,
            )
        )
        # A, on the same mark, is closed by its trail.
        self.assertTrue(
            simulator._exit_trigger(
                PROFILE_A, position(10.0, best_pnl_pct=40.0), 20.0, now,
            )[0].startswith('SL/trail')
        )

    def test_shadow_no_trail_is_registered_and_capital_free(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        self.assertIn('shadow_no_trail', simulator.books)
        book = simulator.books['shadow_no_trail']
        self.assertTrue(book.profile.is_shadow)
        self.assertEqual(book.profile.capital_rub, 0.0)
        self.assertEqual(book.profile.mirror_of, 'portfolio_a')
        self.assertEqual(book.profile.max_slots, 1)
        self.assertEqual(book.profile.time_exit_sec, 15.0)
        self.assertFalse(book.profile.trailing_enabled)
        summary = simulator.summary()
        self.assertIn('shadow_no_trail', summary)
        self.assertEqual(summary['ab_test']['single_difference'], 'exit_rule')
        self.assertIn('shadow_no_trail', summary['ab_test'])
        self.assertNotIn('shadow_exit25', summary)


class RunFourReportAndSnapshotTests(unittest.IsolatedAsyncioTestCase):
    """P0-4 exit reasons and P0-5 artefacts written while the run is alive."""

    def test_exit_breakdown_groups_trades_by_what_closed_them(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        book = simulator.books['portfolio_b']
        book.closed_trades.extend([
            {'reason': 'time_exit_15s (pnl=-3.2%)', 'pnl_rub': -16.0, 'held_sec': 15.0},
            {'reason': 'time_exit_15s (pnl=+12.0%)', 'pnl_rub': 60.0, 'held_sec': 15.0},
            {'reason': 'SL -20% (pnl=-21.0%)', 'pnl_rub': -105.0, 'held_sec': 4.0},
            {'reason': 'TP1 +100% partial 50% (pnl=+104.0%)', 'pnl_rub': 260.0,
             'held_sec': 9.0},
        ])
        breakdown = simulator._exit_breakdown(book)
        self.assertEqual(breakdown['time_exit']['n'], 2)
        self.assertEqual(breakdown['time_exit']['sum_rub'], 44.0)
        self.assertEqual(breakdown['time_exit']['mean_rub'], 22.0)
        self.assertEqual(breakdown['time_exit']['median_hold_sec'], 15.0)
        self.assertEqual(breakdown['stop_loss']['n'], 1)
        self.assertEqual(breakdown['tp']['sum_rub'], 260.0)
        # Reason categories are stable, so the report table never splits a
        # single rule into several rows.
        self.assertEqual(simulator.exit_reason_kind('Shelf stagnation 4.5s'), 'shelf')
        self.assertEqual(simulator.exit_reason_kind('No progress 5s'), 'no_progress')
        self.assertEqual(simulator.exit_reason_kind('SL/trail 0.85x'), 'trail')
        self.assertEqual(simulator.exit_reason_kind(''), 'unknown')

    def test_periodic_snapshot_writes_artefacts_before_the_run_ends(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            simulator.started_at_epoch = time.time() - 125.0
            result = simulator.snapshot_artifacts(reason='periodic')
            self.assertEqual(result['reason'], 'periodic')
            self.assertIn('ab_summary.json', result['written'])
            self.assertIn('ticks.csv', result['written'])
            self.assertTrue((Path(tmp) / 'ab_summary.json').exists())
            status = (Path(tmp) / 'STATUS.txt').read_text(encoding='utf-8')
            self.assertIn('snapshot=periodic', status)
            self.assertIn('paper_only=true', status)
            self.assertIn('duration_sec=', status)
            summary = json.loads((Path(tmp) / 'ab_summary.json').read_text(encoding='utf-8'))
            self.assertEqual(summary['ab_test']['single_difference'], 'exit_rule')
            # The snapshot must never be mistaken for a final summary.
            self.assertIsNone(simulator.ended_at_epoch)

    async def test_snapshot_task_writes_on_its_own_and_stops_cleanly(self) -> None:
        import main_ab

        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            calls: list[str] = []
            original = simulator.snapshot_artifacts

            def spy(*, reason='periodic'):
                calls.append(reason)
                return original(reason=reason)

            simulator.snapshot_artifacts = spy
            with patch.object(main_ab, 'SNAPSHOT_INTERVAL_SEC', 0.05):
                task = asyncio.create_task(
                    main_ab._periodic_snapshots(simulator, Path(tmp)),
                )
                await asyncio.sleep(0.18)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            self.assertGreaterEqual(len(calls), 2)
            self.assertTrue(all(reason == 'periodic' for reason in calls))
            self.assertTrue((Path(tmp) / 'ab_summary.json').exists())
