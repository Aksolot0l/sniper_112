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
    SHADOW_GATE,
    SHADOW_EXIT25,
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
            # PERFORMA was born at 39.88 — inside the old 50 SOL band, outside
            # the 34.5 one: the new ceiling is a dev-buy filter, not a delta one.
            self.assertEqual(profile.entry_check(39.88, 40.41)[1], 'birth_out_of_band')
            # Run #3: the arms differ ONLY in the delta cap (A 2.5, B 3.0).
            self.assertEqual(profile.delta_max_sol, 2.5 if profile is PROFILE_A else 3.0)
            for birth, now in ((33.95, 45.51), (33.16, 38.59)):   # Turtle, Shigure
                ok, reason = profile.entry_check(birth, now)
                self.assertFalse(ok)
                self.assertEqual(reason, 'momentum_too_late')
            # Boundary of each arm's own cap.
            cap = profile.delta_max_sol
            self.assertTrue(profile.entry_check(32.0, 32.0 + cap)[0])
            self.assertFalse(profile.entry_check(32.0, 32.0 + cap + 0.0001)[0])
            # Birth band still guards supply-shape outliers.
            self.assertFalse(profile.entry_check(30.49, 31.3)[0])
            self.assertFalse(profile.entry_check(35.01, 35.5)[0])

    def test_birth_max_boundary_is_34_5_and_delta_band_is_untouched(self) -> None:
        for profile in (PROFILE_A, PROFILE_B):
            self.assertEqual(profile.v_sol_birth_max, 34.5)
            self.assertEqual(profile.delta_min_sol, 0.10)
            self.assertEqual(profile.delta_max_sol, 2.5 if profile is PROFILE_A else 3.0)
            # Inclusive upper bound: 34.5 is admitted, 34.51 is not.
            self.assertEqual(profile.entry_check(34.49, 34.8), (True, 'live_momentum_entry'))
            self.assertEqual(profile.entry_check(34.5, 34.8), (True, 'live_momentum_entry'))
            self.assertEqual(profile.entry_check(34.51, 34.8), (False, 'birth_out_of_band'))
            # The measured mechanism: v_sol_birth = 29.96 + dev_buy_SOL.
            self.assertEqual(profile.entry_check(29.96, 30.2), (False, 'birth_out_of_band'))
            self.assertEqual(profile.entry_check(29.96 + 4.5, 34.7), (True, 'live_momentum_entry'))
            self.assertEqual(profile.entry_check(29.96 + 4.55, 34.8), (False, 'birth_out_of_band'))
            # Both bounds of the delta window are unchanged by fix #7.
            self.assertEqual(profile.entry_check(33.0, 33.09)[1], 'no_live_momentum')
            self.assertEqual(profile.entry_check(33.0, 33.10), (True, 'live_momentum_entry'))
            cap = profile.delta_max_sol
            self.assertEqual(profile.entry_check(33.0, 33.0 + cap), (True, 'live_momentum_entry'))
            self.assertEqual(
                profile.entry_check(33.0, 33.0 + cap + 0.01)[1], 'momentum_too_late',
            )

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
        # The cap override applies to A only; B keeps the fixed +0.5 gap that
        # makes it the single-difference arm, unless its own knob is set.
        self.assertEqual(profiles['portfolio_b'].delta_max_sol, 3.0)
        self.assertFalse(profiles['portfolio_a'].require_creator_gate)
        self.assertFalse(profiles['portfolio_b'].require_creator_gate)
        # The gate now lives on the shadow only.
        self.assertTrue(profiles['shadow_gate'].require_creator_gate)
        self.assertEqual(profiles['shadow_gate'].guard_min_signatures, 30)
        self.assertEqual(profiles['shadow_gate'].capital_rub, 0.0)
        self.assertEqual(profiles['shadow_gate'].mirror_of, 'portfolio_a')
        self.assertEqual(profiles['shadow_exit25'].single_exit_pct, 25.0)
        for shadow in (profiles['shadow_gate'], profiles['shadow_exit25']):
            self.assertTrue(shadow.is_shadow)
            self.assertEqual(shadow.initial_balance_rub, 1500.0)
        # B's own cap knob exists but shares everything else with A.
        b_settings = SimpleNamespace(**{**vars(base), 'entry_delta_max_sol_b': 4.0})
        self.assertEqual(
            profiles_from_settings(b_settings)['portfolio_b'].delta_max_sol, 4.0,
        )

    def test_arms_differ_only_in_delta_cap(self) -> None:
        a, b = asdict(PROFILE_A), asdict(PROFILE_B)
        self.assertEqual(a.pop('delta_max_sol'), 2.5)
        self.assertEqual(b.pop('delta_max_sol'), 3.0)
        a.pop('name')
        b.pop('name')
        # One difference, nothing else: no gate flag, no guard threshold.
        self.assertEqual(a, b)
        self.assertFalse(PROFILE_A.require_creator_gate)
        self.assertFalse(PROFILE_B.require_creator_gate)
        self.assertEqual((PROFILE_A.initial_balance_rub, PROFILE_B.initial_balance_rub), (1500, 1500))
        self.assertEqual((PROFILE_A.position_size_rub, PROFILE_B.position_size_rub), (500, 500))
        self.assertEqual((PROFILE_A.max_slots, PROFILE_B.max_slots), (3, 3))
        # Exits stay identical between the live arms.
        self.assertIsNone(PROFILE_A.single_exit_pct)
        self.assertIsNone(PROFILE_B.single_exit_pct)

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

    async def test_per_profile_delta_cap_splits_the_2_5_to_3_band(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(tmp)
            await self._open(simulator, 'mint-cap-split', 2.9, time.time())
            self.assertNotIn('mint-cap-split', simulator.books['portfolio_a'].positions)
            self.assertIn('mint-cap-split', simulator.books['portfolio_b'].positions)
            # Inside A's own cap both arms trade the same coin.
            await self._open(simulator, 'mint-cap-inside', 2.4, time.time())
            self.assertIn('mint-cap-inside', simulator.books['portfolio_a'].positions)
            self.assertIn('mint-cap-inside', simulator.books['portfolio_b'].positions)

    async def test_shadow_exit25_sells_everything_on_the_first_tick_at_25(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        pos = SimpleNamespace(remaining_tokens_raw=1_000, opened_at_epoch=100.0)
        now = 103.0
        # Exactly at the threshold: 100% out on this very tick.
        trigger = simulator._exit_trigger(SHADOW_EXIT25, pos, 25.0, now)
        self.assertIsNotNone(trigger)
        self.assertTrue(trigger[0].startswith('Exit25'))
        self.assertEqual(trigger[1], 1_000)
        # Just below: hold.
        self.assertIsNone(simulator._exit_trigger(SHADOW_EXIT25, pos, 24.9, now))
        # Overshooting the threshold still exits 100%, never a half TP1.
        trigger = simulator._exit_trigger(SHADOW_EXIT25, pos, 80.0, now)
        self.assertEqual(trigger[1], 1_000)
        self.assertNotIn('TP1', trigger[0])
        # SL and the 35 s timeout stay exactly as in the live ladder.
        self.assertTrue(simulator._exit_trigger(SHADOW_EXIT25, pos, -10.0, now)[0].startswith('SL'))
        self.assertIsNone(simulator._exit_trigger(SHADOW_EXIT25, pos, 3.0, 134.0, ))
        self.assertTrue(
            simulator._exit_trigger(SHADOW_EXIT25, pos, 3.0, 135.1)[0].startswith('Timeout')
        )
        # The live arms keep the unchanged 4-mode ladder.
        live = SimpleNamespace(
            remaining_tokens_raw=1_000, opened_at_epoch=100.0, tp1_done=False,
            tp1_rocket_bypass=False, tp1_partial_pending=False, tp1_pending_sell_raw=0,
            best_pnl_pct=0.0, best_pnl_updated_at=100.0, no_progress_checked=False,
        )
        self.assertTrue(
            simulator._exit_trigger(PROFILE_A, live, 30.0, 101.0)[0].startswith('TP1')
        )

    async def test_disabling_shadows_does_not_change_the_live_arms(self) -> None:
        epoch = 1_700_000_000.0
        with tempfile.TemporaryDirectory() as tmp:
            full = make_simulator(
                Path(tmp) / 'with-shadows',
                profiles={
                    'portfolio_a': PROFILE_A, 'portfolio_b': PROFILE_B,
                    'shadow_gate': SHADOW_GATE, 'shadow_exit25': SHADOW_EXIT25,
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
            # The shadows really did mirror A, with zero real capital.
            for name in ('shadow_gate', 'shadow_exit25'):
                self.assertEqual(sorted(full.books[name].positions),
                                 sorted(full.books['portfolio_a'].positions))
                self.assertEqual(full.books[name].profile.capital_rub, 0.0)


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
