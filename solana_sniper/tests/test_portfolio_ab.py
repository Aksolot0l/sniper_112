from __future__ import annotations

import asyncio
import json
import os
import struct
import tempfile
import time
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from portfolio_ab import (
    ARM_DEFAULTS,
    ARM_ENV_KEYS,
    ARM_IDS,
    ARM_LABELS,
    ARM_REFERENCE,
    ABPaperSimulator,
    EntrySnapshot,
    ExecutableQuote,
    PROFILE_A,
    PortfolioBook,
    SimPosition,
    arm_profiles,
    delta_bucket,
    entry_band_label,
    entry_shadow_flags,
    hourly_blocks,
    paired_stats,
    profiles_from_settings,
    rocket_accounting,
    stability_split,
)

# Run #7: the arms are fixed by name; B1 is the "another arm" used wherever a
# test needs a second live book.
ARMS = arm_profiles()
PROFILE_B1 = ARMS['portfolio_b1_narrow']
# The four arms of run #7. Run #8 added S1/S0, which are asserted separately.
RUN7_ARMS = ('portfolio_b1_narrow', 'portfolio_b2_birth', 'portfolio_b3_t0')
PROFILE_B3 = ARMS['portfolio_b3_t0']
B1 = 'portfolio_b1_narrow'
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
    """Run #7: four arms, one entry boundary each, one exit ladder for all."""

    def test_four_arms_each_move_exactly_one_boundary_of_a(self) -> None:
        profiles = arm_profiles()
        self.assertEqual(list(profiles), list(ARM_IDS))
        reference = asdict(PROFILE_A)
        for name in RUN7_ARMS:
            arm = profiles[name]
            diff = {
                key for key, value in asdict(arm).items()
                if key not in ('name', 'arm_id', 'entry_band_label')
                and reference[key] != value
            }
            # Exactly one boundary moves, and it is a boundary — not the exit.
            self.assertEqual(len(diff), 1, (name, diff))
            self.assertIn(
                diff.pop(), ('delta_max_sol', 'v_sol_birth_max', 'delta_min_sol'), name,
            )
        # The declared boundaries.
        self.assertEqual(profiles['portfolio_b1_narrow'].delta_max_sol, 1.00)
        self.assertEqual(profiles['portfolio_b2_birth'].v_sol_birth_max, 31.00)
        self.assertEqual(profiles['portfolio_b3_t0'].delta_min_sol, 0.00)
        # B3 keeps A's whole band: only the momentum floor moves.
        self.assertEqual(profiles['portfolio_b3_t0'].delta_max_sol, 2.50)
        self.assertEqual(profiles['portfolio_b3_t0'].v_sol_birth_max, 34.50)
        for name in ('portfolio_a', 'portfolio_b1_narrow', 'portfolio_b2_birth'):
            self.assertEqual(profiles[name].delta_min_sol, 0.10, name)
        for name in ARM_IDS:
            arm = profiles[name]
            self.assertEqual(arm.v_sol_birth_min, 30.5, name)
            self.assertEqual(arm.stop_loss_pct, 10.0, name)
            self.assertEqual(arm.tp1_pct, 30.0, name)
            self.assertEqual(arm.tp1_sell_pct, 50.0, name)
            self.assertEqual(arm.max_slots, 3, name)
            self.assertEqual(arm.position_size_rub, 500.0, name)
            self.assertEqual(arm.initial_balance_rub, 1_500.0, name)
        self.assertIn('0.10 <= delta <= 1.00', profiles['portfolio_b1_narrow'].entry_band_label)
        self.assertIn('30.50 <= v_sol_birth <= 31.00',
                      profiles['portfolio_b2_birth'].entry_band_label)
        self.assertIn('0.00 <= delta <= 2.50', profiles['portfolio_b3_t0'].entry_band_label)
        self.assertEqual(ARM_LABELS['portfolio_b3_t0'], 'B3')

    def test_run8_pair_is_b1_and_b1_without_the_momentum_gate(self) -> None:
        profiles = arm_profiles()
        s1 = profiles['portfolio_s1']
        s0 = profiles['portfolio_s0']
        # S1 IS B1 — not "similar to", the same band.
        self.assertEqual(entry_band_label(s1), entry_band_label(profiles['portfolio_b1_narrow']))
        # S0 is S1 with exactly one boundary moved, and it is a boundary.
        diff = {
            key for key, value in asdict(s0).items()
            if key not in ('name', 'arm_id', 'entry_band_label')
            and asdict(s1)[key] != value
        }
        self.assertEqual(diff, {'delta_min_sol'})
        self.assertEqual(s0.delta_min_sol, 0.00)
        self.assertEqual(s0.delta_max_sol, 1.00)
        self.assertEqual(s0.v_sol_birth_max, 34.50)
        self.assertIn('0.10 <= delta <= 1.00', s1.entry_band_label)
        self.assertIn('0.00 <= delta <= 1.00', s0.entry_band_label)
        self.assertEqual((ARM_LABELS['portfolio_s1'], ARM_LABELS['portfolio_s0']), ('S1', 'S0'))
        # S0 is compared with S1, not with A: that is the whole control.
        self.assertEqual(ARM_REFERENCE['portfolio_s0'], 'portfolio_s1')
        self.assertEqual(ARM_REFERENCE['portfolio_s1'], 'portfolio_a')
        with patch.dict(os.environ, {
            'ENTRY_DELTA_MAX_SOL_S1': '0.75', 'ENTRY_DELTA_MIN_SOL_S0': '0.05',
        }, clear=False):
            env_profiles = profiles_from_settings(SimpleNamespace())
        self.assertEqual(env_profiles['portfolio_s1'].delta_max_sol, 0.75)
        self.assertEqual(env_profiles['portfolio_s0'].delta_max_sol, 1.00)
        self.assertEqual(env_profiles['portfolio_s0'].delta_min_sol, 0.05)

    def test_single_arm_run_needs_the_explicit_flag(self) -> None:
        # Run #8 runs S1+S0 only. Without A the run must refuse to start,
        # unless ALLOW_SINGLE_ARM=1 says the comparison is deliberately absent.
        with patch.dict(os.environ, {'ARMS': 'portfolio_s1,portfolio_s0'}, clear=False):
            os.environ.pop('ALLOW_SINGLE_ARM', None)
            with self.assertRaises(ValueError):
                profiles_from_settings(SimpleNamespace())
            with patch.dict(os.environ, {'ALLOW_SINGLE_ARM': '1'}, clear=False):
                profiles = profiles_from_settings(SimpleNamespace())
        self.assertEqual(list(profiles), ['portfolio_s1', 'portfolio_s0'])
        self.assertNotIn('portfolio_a', profiles)

    def test_b3_is_the_t0_arm_not_a_wide_one(self) -> None:
        # The wide arm (delta > 2.5) is closed and must not come back: its
        # EV@15 was -9.25 / -5.84 / -5.54 % in three runs and it held no extra
        # rockets inside A's birth band.
        self.assertNotIn('portfolio_b3_wide', ARM_IDS)
        self.assertNotIn('portfolio_b3_wide', ARM_DEFAULTS)
        self.assertNotIn('portfolio_b3_wide', ARM_LABELS)
        self.assertNotIn('ENTRY_DELTA_MAX_SOL_B3', ARM_ENV_KEYS['portfolio_b3_t0'])
        self.assertEqual(ARM_ENV_KEYS['portfolio_b3_t0'],
                         ('ENTRY_DELTA_MIN_SOL_B3', 'delta_min_sol'))
        self.assertEqual(ARM_DEFAULTS['portfolio_b3_t0'], {'delta_min_sol': 0.00})
        self.assertEqual(ARM_LABELS['portfolio_b3_t0'], 'B3')
        # The t0 arm still carries A's delta ceiling: it only drops the floor.
        self.assertEqual(ARM_DEFAULTS['portfolio_a' if False else 'portfolio_b1_narrow'],
                         {'delta_max_sol': 1.00})
        self.assertEqual(arm_profiles()['portfolio_b3_t0'].delta_max_sol,
                         PROFILE_A.delta_max_sol)
        with patch.dict(os.environ, {'ENTRY_DELTA_MIN_SOL_B3': '0.05'}, clear=False):
            self.assertEqual(
                profiles_from_settings(SimpleNamespace())['portfolio_b3_t0'].delta_min_sol,
                0.05,
            )

    def test_entry_rule_requires_live_momentum(self) -> None:
        # Dead launch: A, B1 and B2 wait for an impulse and reject it.
        for name in ('portfolio_a', 'portfolio_b1_narrow', 'portfolio_b2_birth'):
            profile = arm_profiles()[name]
            self.assertEqual(
                profile.entry_check(31.0, 31.0), (False, 'no_live_momentum'), name,
            )
            self.assertFalse(profile.entry_check(31.0, 31.05)[0], name)
        # B3 is the t0 arm: it enters on the first read, delta == 0 included.
        self.assertEqual(
            arm_profiles()['portfolio_b3_t0'].entry_check(31.0, 31.0),
            (True, 'live_momentum_entry'),
        )
        self.assertEqual(PROFILE_A.entry_check(30.5, 31.3), (True, 'live_momentum_entry'))
        # PERFORMA was born at 39.88 — inside the old 50 SOL band, outside the
        # 34.5 one: the ceiling is a dev-buy filter, not a delta one.
        self.assertEqual(PROFILE_A.entry_check(39.88, 40.41)[1], 'birth_out_of_band')
        # Turtle / Shigure style late chases stay out of A.
        for birth, now in ((33.95, 45.51), (33.16, 38.59)):
            self.assertEqual(PROFILE_A.entry_check(birth, now)[1], 'momentum_too_late')
        self.assertFalse(PROFILE_A.entry_check(30.49, 31.3)[0])

    def test_no_arm_buys_above_the_delta_ceiling(self) -> None:
        profiles = arm_profiles()
        # A wide arm (delta > 2.5) was rejected on data: EV@15 -9.25/-5.84/-5.54 %
        # in three runs and no extra rockets inside A's birth band.
        for name, profile in profiles.items():
            self.assertFalse(profile.entry_check(32.0, 35.0)[0], name)
            self.assertFalse(profile.entry_check(30.8, 40.0)[0], name)
        self.assertEqual(profiles['portfolio_a'].entry_check(32.0, 35.0)[1],
                         'momentum_too_late')
        # B2 rejects the same coin at the door: its birth is above 31.0.
        self.assertEqual(profiles['portfolio_b2_birth'].entry_check(32.0, 35.0)[1],
                         'birth_out_of_band')
        self.assertEqual(profiles['portfolio_b2_birth'].entry_check(30.8, 33.4)[1],
                         'momentum_too_late')
        self.assertEqual(profiles['portfolio_b1_narrow'].entry_check(32.0, 33.5)[1],
                         'momentum_too_late')
        # B3 differs only by admitting the zero-impulse read.
        self.assertEqual(profiles['portfolio_b3_t0'].entry_check(32.0, 32.0),
                         (True, 'live_momentum_entry'))
        self.assertFalse(profiles['portfolio_b3_t0'].entry_check(32.0, 35.0)[0])

    def test_birth_cut_arm_rejects_what_a_admits(self) -> None:
        profiles = arm_profiles()
        self.assertTrue(profiles['portfolio_a'].entry_check(33.0, 33.5)[0])
        self.assertEqual(
            profiles['portfolio_b2_birth'].entry_check(33.0, 33.5),
            (False, 'birth_out_of_band'),
        )
        self.assertTrue(profiles['portfolio_b2_birth'].entry_check(30.8, 31.3)[0])
        # The reference ceiling itself is unchanged and inclusive.
        self.assertTrue(PROFILE_A.entry_check(34.5, 34.7)[0])
        self.assertEqual(PROFILE_A.entry_check(34.51, 34.8), (False, 'birth_out_of_band'))
        self.assertEqual(PROFILE_A.entry_check(33.0, 33.09)[1], 'no_live_momentum')
        self.assertEqual(PROFILE_A.entry_check(33.0, 35.51)[1], 'momentum_too_late')
        self.assertTrue(PROFILE_A.entry_check(29.96 + 4.5, 34.7)[0])

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
            'entry_delta_max_sol': 6.0,
            'entry_delta_max_sol_b1': 1.5, 'entry_birth_max_sol_b2': 33.0,
            'entry_delta_min_sol_b3': 0.25,
        })
        profiles = profiles_from_settings(settings)
        self.assertEqual(profiles['portfolio_a'].v_sol_birth_max, 44.0)
        self.assertEqual(profiles['portfolio_a'].delta_min_sol, 0.5)
        self.assertEqual(profiles['portfolio_a'].delta_max_sol, 6.0)
        # Each arm reads its OWN override; A is the only one on the shared keys.
        self.assertEqual(profiles[B1].delta_max_sol, 1.5)
        self.assertEqual(profiles[B1].v_sol_birth_max, 44.0)
        self.assertEqual(profiles['portfolio_b2_birth'].v_sol_birth_max, 33.0)
        self.assertEqual(profiles['portfolio_b2_birth'].delta_max_sol, 6.0)
        self.assertEqual(profiles['portfolio_b3_t0'].delta_min_sol, 0.25)
        self.assertEqual(profiles['portfolio_b3_t0'].v_sol_birth_max, 44.0)
        # One band edge per arm, everything else identical to A.
        for name in ('portfolio_b2_birth', 'portfolio_b3_t0'):
            self.assertEqual(profiles[name].v_sol_birth_min, 30.5, name)
        self.assertEqual(profiles['portfolio_b2_birth'].delta_min_sol, 0.5)
        # No creator gate anywhere and no shadow books at all.
        for name in profiles:
            self.assertNotIn('shadow', name)
        self.assertEqual(len(profiles), len(ARM_IDS))
        # The latency model is configurable and lands in every profile.
        latency_settings = SimpleNamespace(**{**vars(base), 'entry_latency_ms': 250.0})
        for name in profiles:
            self.assertEqual(
                profiles_from_settings(latency_settings)[name].entry_latency_ms, 250.0,
            )
        self.assertEqual(PROFILE_B1.entry_latency_ms, 150.0)

    def test_all_arms_share_one_exit_ladder(self) -> None:
        profiles = arm_profiles()
        exit_fields = (
            'tp1_pct', 'tp1_sell_pct', 'no_progress_pct', 'stop_loss_pct',
            'no_progress_sec', 'no_progress_best_max_pct', 'trailing_activation_pct',
            'profit_retain_ratio', 'rocket_bypass_pct', 'shelf_stagnation_sec',
            'shelf_min_pnl_pct', 'new_high_epsilon_pct',
            'timeout_before_trailing_sec', 'timeout_after_trailing_sec',
            'entry_latency_ms', 'initial_balance_rub', 'position_size_rub',
            'max_slots', 'auto_top_up', 'capital_rub',
            # Run #8: the exit mode and its thresholds are run-level, so no arm
            # may hold a different value than the other.
            'exit_mode', 'fee_cover_pct', 'time_exit_sec', 'hard_stop_pct',
        )
        reference = asdict(PROFILE_A)
        for name, profile in profiles.items():
            data = asdict(profile)
            for field in exit_fields:
                self.assertEqual(data[field], reference[field], (name, field))
        # The Round 6 ladder, spelled out once.
        self.assertEqual(PROFILE_A.stop_loss_pct, 10.0)
        self.assertEqual(PROFILE_A.tp1_pct, 30.0)
        self.assertEqual(PROFILE_A.tp1_sell_pct, 50.0)
        self.assertEqual(PROFILE_A.rocket_bypass_pct, 50.0)
        self.assertEqual(PROFILE_A.trailing_activation_pct, 10.0)
        self.assertEqual(PROFILE_A.profit_retain_ratio, 0.85)
        self.assertEqual(PROFILE_A.shelf_stagnation_sec, 4.5)
        self.assertEqual(PROFILE_A.no_progress_sec, 5.0)
        self.assertEqual(PROFILE_A.timeout_before_trailing_sec, 35.0)
        self.assertEqual(PROFILE_A.timeout_after_trailing_sec, 90.0)
        for field in ('stop_loss_always', 'trailing_enabled',
                      'shelf_enabled', 'no_progress_enabled', 'require_creator_gate',
                      'is_shadow', 'mirror_of'):
            self.assertFalse(hasattr(PROFILE_A, field), field)
        # Entry happens on live momentum, not at detection (delta_min = 0 is
        # the closed experiment).
        # Ladder by default: a run has to ask for fee_cover explicitly.
        self.assertEqual(PROFILE_A.exit_mode, 'ladder')
        self.assertEqual(PROFILE_A.fee_cover_pct, 3.5)
        self.assertEqual(PROFILE_A.time_exit_sec, 5.0)
        self.assertEqual(PROFILE_A.hard_stop_pct, 10.0)
        self.assertFalse(PROFILE_A.entry_at_detection)
        self.assertFalse(PROFILE_A.delta_max_from_birth_ceiling)

    def test_books_use_isolated_wallet_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = PortfolioBook(PROFILE_A, root / 'wallet_a.json', cash_balance_rub=1500)
            b = PortfolioBook(PROFILE_B1, root / 'wallet_b.json', cash_balance_rub=1500)
            a.save('run-ab')
            b.save('run-ab')
            self.assertNotEqual(a.state_path, b.state_path)
            self.assertEqual(json.loads(a.state_path.read_text())['cash_balance_rub'], 1500)
            self.assertEqual(json.loads(b.state_path.read_text())['portfolio'], B1)


class RunThreeBankAndStopListTests(unittest.IsolatedAsyncioTestCase):
    """Addendum: bank top-ups, bust metric and the four-reason stop-list."""

    def _live_profiles(self, **overrides):
        a = replace(PROFILE_A, **overrides)
        return {'portfolio_a': a, B1: replace(PROFILE_B1, **overrides)}

    async def _consider(self, simulator, coin, delta=0.4, epoch=None):
        snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
        epoch = time.time() if epoch is None else epoch
        with patch('portfolio_ab.get_collector', return_value=None):
            for name in ('portfolio_a', B1):
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
                twin = topped.books[B1].positions[mint]
                self.assertGreater(pos.initial_tokens_raw, 0)
                self.assertEqual(pos.entry_amount_lamports, twin.entry_amount_lamports)
                self.assertEqual(pos.initial_tokens_raw, twin.initial_tokens_raw)
                self.assertEqual(pos.entry_price_usd, twin.entry_price_usd)
            # Only the A book got injections; B was never short.
            self.assertEqual(topped.books[B1].top_ups, 0)
            self.assertEqual(topped.books['portfolio_a'].top_ups, 1)
            # ...and the actual paper_fills are byte-identical to the twin's.
            fills = await self._sell_both_topped_and_twin(topped, 'mint-iso-a')
            a_args, a_kwargs = fills['portfolio_a']
            b_args, b_kwargs = fills[B1]
            self.assertEqual(a_args, b_args)
            self.assertEqual(
                {k: v for k, v in a_kwargs.items() if k != 'portfolio'},
                {k: v for k, v in b_kwargs.items() if k != 'portfolio'},
            )
            self.assertEqual(a_kwargs.get('portfolio'), 'portfolio_a')
            self.assertEqual(b_kwargs.get('portfolio'), B1)

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
            for name in ('portfolio_a', B1):
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
            book = simulator.books[B1]
            book.realized_pnl_rub = -1_150.0
            book.cash_balance_rub = 350.0
            await self._consider(simulator, self._coin('mint-bank-2'))
            summary = simulator.summary()
        for name in ARM_IDS:
            entry = summary[name]
            for key in ('top_ups', 'capital_injected_rub', 'pnl_with_topups',
                        'equity_no_topups', 'bust_at_trade', 'bust_at_utc'):
                self.assertIn(key, entry, (name, key))
        b = summary[B1]
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
                    patch.object(simulator, 'write_enrich'):
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


class ABEntryAccountingTests(unittest.IsolatedAsyncioTestCase):
    """Every arm sees one snapshot: no creator gate, no per-arm filtering."""

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
                    for name in simulator.books
                ))
                pos_a = simulator.books['portfolio_a'].positions[coin.mint]
                pos_b = simulator.books[B1].positions[coin.mint]
                self.assertEqual(pos_a.opened_at_epoch, decision_epoch)
                self.assertEqual(pos_b.opened_at_epoch, decision_epoch)
                self.assertEqual(pos_a.entry_out_amount_raw, pos_a.initial_tokens_raw)
                self.assertEqual(pos_a.initial_tokens_raw, pos_b.initial_tokens_raw)
                self.assertEqual(pos_a.initial_conservative_tokens_raw, pos_a.entry_min_out_raw)
                self.assertLess(pos_a.initial_conservative_tokens_raw, pos_a.initial_tokens_raw)
                self.assertAlmostEqual(simulator.books['portfolio_a'].cash_balance_rub, 1000.0, places=3)
                self.assertAlmostEqual(simulator.books[B1].cash_balance_rub, 1000.0, places=3)

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
            # Two arms are enough to prove the rule: it needs ALL books empty
            # and below one slot.
            simulator = make_simulator(tmp, profiles={
                'portfolio_a': replace(PROFILE_A, auto_top_up=False),
                B1: replace(PROFILE_B1, auto_top_up=False),
            })
            a = simulator.books['portfolio_a']
            b = simulator.books[B1]
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
            b_pos = simulator.books[B1].positions['mint-premium']
            self.assertAlmostEqual(
                b_pos.entry_premium_pct, pos.entry_premium_pct, places=6,
            )
            # The t0 option belongs to the deferred delta_min=0 experiment.
            self.assertFalse(simulator.books[B1].profile.entry_at_detection)

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
            # Run #7: the old `entry_latency_floor_violations` counter is gone —
            # it read like an error while only saying the read was older than
            # the modelled 150 ms (143/167 in run #6, median lag 101 ms). The
            # honest number is the read age itself.
            self.assertFalse(hasattr(simulator, 'entry_latency_floor_violations'))
            self.assertNotIn('entry_latency_floor_violations', quality)
            self.assertEqual(quality['entry_lag_ms_median'], 550.0)

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

    def test_every_arm_runs_the_same_exit_ladder(self) -> None:
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

        results = {}
        for name, profile in arm_profiles().items():
            # -10 % closes everything, before or after TP1.
            self.assertIsNone(simulator._exit_trigger(profile, position(2.0), -9.99, now), name)
            trigger = simulator._exit_trigger(profile, position(2.0), -10.0, now)
            self.assertTrue(trigger[0].startswith('SL'), (name, trigger))
            self.assertEqual(trigger[1], 1_000)
            # TP1 +30 % sells exactly half.
            trigger = simulator._exit_trigger(profile, position(2.0), 30.0, now)
            self.assertTrue(trigger[0].startswith('TP1 +30% partial 50%'), (name, trigger))
            self.assertEqual(trigger[1], 500)
            # +50 % bypasses TP1 and holds the whole position.
            rocket = position(2.0)
            self.assertIsNone(simulator._exit_trigger(profile, rocket, 50.0, now), name)
            self.assertTrue(rocket.tp1_rocket_bypass)
            # Timeouts: 35 s before the trail activates, 90 s after. The peak
            # timestamp is fresh, so no shelf rule can fire first.
            before = position(35.0, best_pnl_pct=9.99, no_progress_checked=True)
            before.best_pnl_updated_at = now
            self.assertIn('Timeout', simulator._exit_trigger(profile, before, 2.0, now)[0])
            after = position(90.0, best_pnl_pct=10.0, no_progress_checked=True)
            after.best_pnl_updated_at = now
            self.assertIn('Timeout', simulator._exit_trigger(profile, after, 10.0, now)[0])
            results[name] = simulator._exit_rule_label(profile)
        # One ladder for four arms: the label is byte-identical.
        self.assertEqual(len(set(results.values())), 1, results)


class RunFourReportAndSnapshotTests(unittest.IsolatedAsyncioTestCase):
    """P0-4 exit reasons and P0-5 artefacts written while the run is alive."""

    def test_exit_breakdown_groups_trades_by_what_closed_them(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        book = simulator.books[B1]
        book.closed_trades.extend([
            {'reason': 'Timeout (35s) (pnl=-3.2%)', 'pnl_rub': -16.0, 'held_sec': 35.0},
            {'reason': 'Timeout (35s) (pnl=+12.0%)', 'pnl_rub': 60.0, 'held_sec': 35.0},
            {'reason': 'SL -10% (pnl=-12.2%)', 'pnl_rub': -61.0, 'held_sec': 4.0},
            {'reason': 'TP1 +30% partial 50% (pnl=+31.0%)', 'pnl_rub': 78.0,
             'held_sec': 9.0},
        ])
        breakdown = simulator._exit_breakdown(book)
        self.assertEqual(breakdown['timeout']['n'], 2)
        self.assertEqual(breakdown['timeout']['sum_rub'], 44.0)
        self.assertEqual(breakdown['timeout']['mean_rub'], 22.0)
        self.assertEqual(breakdown['timeout']['median_hold_sec'], 35.0)
        self.assertEqual(breakdown['stop_loss']['n'], 1)
        self.assertEqual(breakdown['tp']['sum_rub'], 78.0)
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
            self.assertEqual(summary['ab_test']['single_difference'], 'entry_band')
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


class RunSevenReportTests(unittest.IsolatedAsyncioTestCase):
    """Run #7 §6: the four-arm report blocks must move together."""

    def test_paired_stats_pairs_only_the_common_mints(self) -> None:
        a_trades = [
            {'mint': 'm1', 'pnl_rub': 100.0},
            {'mint': 'm2', 'pnl_rub': -50.0},
            {'mint': 'a-only', 'pnl_rub': 900.0},
        ]
        arm_trades = [
            {'mint': 'm1', 'pnl_rub': 110.0},   # +10
            {'mint': 'm2', 'pnl_rub': -60.0},   # -10
            {'mint': 'arm-only', 'pnl_rub': 5.0},  # not paired
        ]
        stats = paired_stats(a_trades, arm_trades, iterations=2_000)
        # The A-only and arm-only coins are NOT part of the comparison: run #6
        # compared two independent means and lost most of its power that way.
        self.assertEqual(stats['n_pairs'], 2)
        self.assertAlmostEqual(stats['mean_diff'], 0.0, places=6)
        self.assertEqual((stats['wins'], stats['losses'], stats['ties']), (1, 1, 0))
        self.assertEqual(stats['ci_level'], 0.9875)
        self.assertEqual(len(stats['ci95_diff']), 2)
        self.assertLessEqual(stats['ci95_diff'][0], stats['ci95_diff'][1])
        # An arm with no overlap is reported, not silently zeroed.
        empty = paired_stats(a_trades, [{'mint': 'x', 'pnl_rub': 1.0}], iterations=10)
        self.assertEqual(empty['n_pairs'], 0)
        self.assertIsNone(empty['mean_diff'])
        self.assertIsNone(empty['ci95_diff'])

    def test_hourly_blocks_and_stability_need_the_same_sign(self) -> None:
        start = 1_000.0
        a_trades = [
            {'mint': 'm1', 'pnl_rub': 0.0, 'closed_at_epoch': start + 60.0},
            {'mint': 'm2', 'pnl_rub': 0.0, 'closed_at_epoch': start + 4_000.0},
        ]
        arm_trades = [
            {'mint': 'm1', 'pnl_rub': 20.0, 'closed_at_epoch': start + 60.0},
            {'mint': 'm2', 'pnl_rub': 30.0, 'closed_at_epoch': start + 4_000.0},
        ]
        blocks = hourly_blocks(arm_trades, start)
        self.assertEqual(sorted(blocks), ['h1', 'h2'])
        self.assertEqual(blocks['h1'], {'n': 1, 'sum': 20.0, 'mean': 20.0})
        self.assertEqual(blocks['h2']['sum'], 30.0)
        stable = stability_split(a_trades, arm_trades, start, 7_200.0)
        self.assertTrue(stable['sign_stable'])
        self.assertAlmostEqual(stable['first_half']['mean_diff'], 20.0, places=6)
        self.assertAlmostEqual(stable['second_half']['mean_diff'], 30.0, places=6)
        # A sign flip in the second half is a result, not noise to hide.
        flipped = stability_split(
            a_trades,
            [{'mint': 'm1', 'pnl_rub': 20.0, 'closed_at_epoch': start + 60.0},
             {'mint': 'm2', 'pnl_rub': -30.0, 'closed_at_epoch': start + 4_000.0}],
            start, 7_200.0,
        )
        self.assertFalse(flipped['sign_stable'])
        self.assertIsNone(stability_split(a_trades, arm_trades, None, 7_200.0)['sign_stable'])

    def test_rocket_accounting_uses_the_observation_window_not_the_trade(self) -> None:
        trades = [
            # In-trade peak was tiny, but the coin ran +80 % inside 60 s.
            {'mint': 'late-runner', 'pnl_rub': 12.0, 'peak_pct': 4.0},
            # In-trade peak looked like a rocket, but inside 60 s it only made 3 %.
            {'mint': 'in-trade-spike', 'pnl_rub': -30.0, 'peak_pct': 70.0},
            # No observation at all -> falls back to the trade, and says so.
            {'mint': 'unobserved', 'pnl_rub': -5.0, 'peak_pct': 60.0},
        ]
        peaks = {'late-runner': 80.0, 'in-trade-spike': 3.0}
        stats = rocket_accounting(trades, peaks=peaks)
        self.assertEqual(stats['peak_source'], 'observations')
        self.assertEqual(stats['window_sec'], 60.0)
        self.assertEqual(stats['fallback_trades'], 1)
        self.assertEqual(stats['rockets'], 2)  # late-runner + unobserved
        self.assertAlmostEqual(stats['rockets_sum_rub'], 7.0, places=6)
        self.assertAlmostEqual(stats['others_sum_rub'], -30.0, places=6)
        self.assertAlmostEqual(stats['rocket_share'], 2 / 3, places=4)
        # Without peaks the same trades are counted on their in-trade peak —
        # the number that made run #6 look five times better than it was.
        fallback = rocket_accounting(trades)
        self.assertEqual(fallback['peak_source'], 'in_trade')
        self.assertEqual(fallback['rockets'], 2)
        self.assertEqual(fallback['fallback_trades'], 3)

    def test_rocket_accounting_reports_the_break_even_share(self) -> None:
        # Run #6 shape: a few rockets pay for many small losses.
        trades = (
            [{'pnl_rub': 500.0, 'peak_pct': 120.0}] * 2
            + [{'pnl_rub': -50.0, 'peak_pct': 5.0}] * 20
        )
        data = rocket_accounting(trades)
        self.assertEqual(data['n'], 22)
        self.assertEqual(data['rockets'], 2)
        self.assertAlmostEqual(data['rocket_share'], 2 / 22, places=4)
        self.assertAlmostEqual(data['rockets_mean_rub'], 500.0, places=6)
        self.assertAlmostEqual(data['others_mean_rub'], -50.0, places=6)
        # p* = 50 / (500 + 50) = 9.09 %: fewer rockets than that and the book
        # loses money, exactly the run #6 question (6.9 % vs 7.8 %).
        self.assertAlmostEqual(data['break_even_rocket_share'], 0.0909, places=4)
        self.assertEqual(data['rocket_pct_threshold'], 50.0)
        # A book that only loses has no break-even point at all.
        self.assertIsNone(rocket_accounting([{'pnl_rub': -1.0, 'peak_pct': 1.0}])[
            'break_even_rocket_share'
        ])

    def test_ticks_carry_usd_marks_and_the_overshoot_columns(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        book = simulator.books['portfolio_a']
        pos = make_position(opened_ago=1.0, best_pnl=12.0)
        pos.exit_trigger_pnl_pct = -12.17
        # Two marks, then the exit: the columns describe mark-to-mark moves.
        simulator._record_tick(book, pos, -8.0, pos.opened_at_epoch + 1.0, 0.000_000_42)
        simulator._record_tick(book, pos, -11.5, pos.opened_at_epoch + 1.5, 0.000_000_40)
        simulator._record_tick(
            book, pos, -12.2, pos.opened_at_epoch + 2.0, 0.000_000_39, event='exit',
        )
        first, *_rest = simulator.ticks
        # price_usd is written AS-IS: run #6 divided a rouble price by
        # sol_usd * fx, so every entry/mark row was off by 1 / sol_usd.
        self.assertEqual(first['price_usd'], 0.000_000_42)
        self.assertIsNone(first['mark_prev_pct'])
        self.assertIsNone(first['step_gap_pp'])
        self.assertIsNone(first['mark_gap_ms'])
        last = simulator.ticks[-1]
        self.assertEqual(last['event'], 'exit')
        self.assertEqual(last['fill_pct'], -12.2)
        self.assertEqual(last['trigger_pct'], -12.17)
        self.assertAlmostEqual(last['mark_prev_pct'], -11.5, places=6)
        # The rule was already 0.67 pp past its trigger on the previous mark.
        self.assertAlmostEqual(last['step_gap_pp'], -0.67, places=4)
        self.assertAlmostEqual(last['mark_gap_ms'], 500.0, places=2)
        path = Path(simulator.run_dir) / 'exports' / 'ticks.csv'
        simulator.write_ticks(path)
        header = path.read_text(encoding='utf-8-sig').splitlines()[0]
        for column in ('price_usd', 'mark_prev_pct', 'step_gap_pp', 'mark_gap_ms'):
            self.assertIn(column, header)

    def test_mark_gap_worst_is_the_largest_gap_and_fly_through_splits_by_exit(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        book = simulator.books['portfolio_a']
        pos = make_position(opened_ago=1.0)
        # Marks 700 ms apart, then an 8 300 ms hole before the exit: the WORST
        # gap is the 8 300 one. Reporting min() there printed the best gap.
        simulator._record_tick(book, pos, -1.0, pos.opened_at_epoch + 1.0, 0.01)
        simulator._record_tick(book, pos, -2.0, pos.opened_at_epoch + 1.7, 0.01)
        simulator._record_tick(book, pos, -3.0, pos.opened_at_epoch + 2.4, 0.01)
        pos.exit_trigger_pnl_pct = -10.0
        simulator._record_tick(
            book, pos, -12.5, pos.opened_at_epoch + 10.7, 0.01,
            event='exit', exit_kind='stop_loss',
        )
        quality = simulator.tick_quality()
        self.assertAlmostEqual(quality['mark_gap_ms']['median'], 700.0, places=2)
        self.assertAlmostEqual(quality['mark_gap_ms']['worst'], 8300.0, places=2)
        # Fly-through is split by exit type: fill - trigger on the exit mark.
        self.assertAlmostEqual(
            quality['fly_through_by_exit']['stop_loss']['median'], -2.5, places=4,
        )
        self.assertEqual(quality['fly_through_by_exit']['stop_loss']['n'], 1)

    def test_tick_quality_measures_the_stop_loss_fly_through(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        book = simulator.books['portfolio_a']
        pos = make_position(opened_ago=1.0)
        # Three SL exits: -12.2, -16.5 and the worst -23.17, plus one timeout
        # exit and one trail exit that must NOT count as stop-loss fly-through.
        for index, (trigger, fill) in enumerate(
            ((-10.0, -12.2), (-10.0, -16.5), (-10.0, -23.17), (None, -5.0))
        ):
            pos.exit_trigger_pnl_pct = trigger
            simulator._record_tick(
                book, pos, fill, pos.opened_at_epoch + 1.0 + index, 0.01,
                event='exit',
            )
        quality = simulator.tick_quality()
        self.assertEqual(quality['stop_loss_fills']['n'], 3)
        self.assertAlmostEqual(quality['stop_loss_fills']['median'], -16.5, places=6)
        self.assertAlmostEqual(quality['stop_loss_fills']['worst'], -23.17, places=6)
        self.assertEqual(quality['stop_loss_worse_than_minus15'], 2)
        self.assertEqual(quality['stop_loss_trigger_pct'], -10.0)
        # The summary carries it, so the report never has to read ticks.csv.
        self.assertIn('tick_quality', simulator.summary())

    def test_summary_reports_four_arms_with_the_run7_blocks(self) -> None:
        simulator = make_simulator(tempfile.mkdtemp())
        summary = simulator.summary()
        self.assertEqual(summary['portfolio_scope'], list(ARM_IDS))
        self.assertEqual(summary['arms'], [ARM_LABELS[name] for name in ARM_IDS])
        self.assertEqual(summary['ab_test']['single_difference'], 'entry_band')
        self.assertEqual(summary['ab_test']['reference_arm'], 'portfolio_a')
        for name in ARM_IDS:
            entry = summary[name]
            self.assertIn('paired_vs_a', entry, name)
            self.assertIn('blocks', entry)
            self.assertIn('stability', entry)
            self.assertIn('rocket_accounting', entry)
            self.assertIn('entry_band_label', entry)
            # A is the reference; every other arm carries a paired result
            # against its own reference (S0 pairs with S1, not with A).
            if name == 'portfolio_a':
                self.assertTrue(entry['paired_vs_a']['reference'])
            else:
                self.assertEqual(entry['paired_vs_a']['n_pairs'], 0)
            self.assertEqual(entry['paired_against'],
                             None if name == 'portfolio_a' else ARM_REFERENCE[name])
            self.assertEqual(entry['exit_rule'], summary['profiles'][name]['exit_rule'])

    async def test_enrich_on_and_off_produce_identical_paper_fills(self) -> None:
        """§5 accept: the flag may not change a single number of a trade."""
        from enrich import EnrichCollector

        class _Response:
            status = 200

            async def json(self):
                return [{'id': 'mint-enrich', 'holderCount': 999,
                         'topHoldersPercentage': 41.0, 'organicScore': 12.0}]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

        class _Session:
            def get(self, *args, **kwargs):
                return _Response()

        async def one_run(enrich_enabled: bool) -> dict:
            with tempfile.TemporaryDirectory() as tmp:
                simulator = make_simulator(Path(tmp))
                if enrich_enabled:
                    simulator.enrich = EnrichCollector(_Session(), enabled=True)
                    # Same moment as the live path: enqueued right after the
                    # security filters, long before the exit is priced.
                    simulator.enrich.enqueue('mint-enrich')
                book = simulator.books['portfolio_a']
                coin = NewCoin(
                    mint='mint-enrich', source='Pump.fun', signature='sig-enrich',
                    curve='curve-test', packet={'v_sol': 30.8},
                )
                snapshot = EntrySnapshot(
                    make_curve(), make_fee_context_for_test(), 12, time.time(),
                )
                epoch = time.time()
                with patch('portfolio_ab.get_collector', return_value=None):
                    await simulator._consider_entry(
                        book, coin, snapshot, 30.8, 31.2, 200.0, {}, epoch,
                    )
                    pos = book.positions['mint-enrich']
                    curve = make_curve()
                    fee_context = make_fee_context_for_test()
                    local = quote_sell_exact_tokens(
                        curve, fee_context, pos.remaining_tokens_raw,
                        slippage_bps=100, context_slot=12,
                    )
                    swap = SwapQuote(
                        input_mint=pos.mint, output_mint=WSOL_MINT,
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
                        book, pos, pos.remaining_tokens_raw, 'Timeout 35s',
                        executable, curve, fee_context, 12, 200.0, -4.0, 480.0,
                    )
                    if enrich_enabled:
                        await simulator.enrich.flush()
                trade = book.closed_trades[-1]
                return {
                    'pnl_rub': round(float(trade['pnl_rub']), 9),
                    'pnl_pct': round(float(trade['pnl_pct']), 9),
                    'reason': trade['reason'],
                    'entry_price_usd': trade['entry_price_usd'],
                    'average_exit_price_usd': trade['average_exit_price_usd'],
                    'conservative_100bps_pnl_rub': trade['conservative_100bps_pnl_rub'],
                    'enrich_rows': len(simulator.enrich.rows),
                }

        off = await one_run(False)
        on = await one_run(True)
        self.assertEqual(off['enrich_rows'], 0)
        self.assertEqual(on['enrich_rows'], 1)
        # Everything except the enrich counter is bit-for-bit the same.
        self.assertEqual(
            {k: v for k, v in on.items() if k != 'enrich_rows'},
            {k: v for k, v in off.items() if k != 'enrich_rows'},
        )

    async def test_enrich_is_collected_once_and_never_blocks_a_decision(self) -> None:
        from enrich import EnrichCollector

        class _Response:
            status = 200

            async def json(self):
                return [
                    {'id': 'mint-1', 'holderCount': 1_234, 'organicScore': 88.5,
                     'stats1h': {'numBuys': 10, 'numSells': 4, 'numTraders': 9}},
                ]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

        class _Session:
            def __init__(self):
                self.calls: list[dict] = []

            def get(self, url, *, params=None, timeout=None):
                self.calls.append({'url': url, 'params': params, 'timeout': timeout})
                return _Response()

        session = _Session()
        collector = EnrichCollector(session, enabled=True, batch_size=7)
        self.assertTrue(collector.enqueue('mint-1'))
        # A mint is asked about once per run, never twice.
        self.assertFalse(collector.enqueue('mint-1'))
        await collector.flush()
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.calls[0]['params']['query'], 'mint-1')
        stats = collector.stats()
        self.assertEqual((stats['ok'], stats['unavailable'], stats['rows']), (1, 0, 1))
        self.assertEqual(collector.rows[0]['status'], 'ok')
        self.assertEqual(collector.rows[0]['holder_count'], 1_234)
        self.assertEqual(collector.rows[0]['num_buys_1h'], 10)

        # An unreachable upstream is a status, not an exception in the run.
        class _Broken:
            def get(self, *args, **kwargs):
                raise RuntimeError('network down')

        broken = EnrichCollector(_Broken(), enabled=True, batch_size=7)
        broken.enqueue('mint-2')
        await broken.flush()
        self.assertEqual(broken.rows[0]['status'], 'enrich_unavailable')
        self.assertEqual(broken.rows[0]['error'], 'exception_RuntimeError')
        # Disabled means zero network calls, ever.
        off = EnrichCollector(_Session(), enabled=False)
        self.assertFalse(off.enqueue('mint-3'))
        await off.flush()
        self.assertEqual(off.stats()['rows'], 0)


class RunEightFeeCoverExitTests(unittest.TestCase):
    """Run #8: hard stop, cover the round trip, or leave at 5 s — nothing else."""

    def _profile(self, **overrides):
        values = {'exit_mode': 'fee_cover'}
        values.update(overrides)
        return replace(ARMS['portfolio_s1'], **values)

    def _simulator(self):
        return make_simulator(tempfile.mkdtemp())

    def _trigger(self, pnl_pct, *, held=0.0, **overrides):
        simulator = self._simulator()
        profile = self._profile(**overrides)
        pos = make_position(opened_ago=held)
        return simulator._exit_trigger(profile, pos, pnl_pct, time.time())

    def test_three_rules_in_the_declared_order(self) -> None:
        # Hard stop outranks fee cover when a single mark jumps over both.
        self.assertEqual(self._trigger(-10.0)[0], 'Hard stop (−10.0%)')
        self.assertEqual(self._trigger(-35.0, held=9.0)[0], 'Hard stop (−10.0%)')
        self.assertEqual(self._trigger(+8.0)[0], 'Fee cover (+3.5%)')
        # Neither price rule fired -> the clock decides.
        self.assertEqual(self._trigger(+3.49, held=5.0)[0], 'Time exit (5.0s)')
        self.assertEqual(self._trigger(-9.99, held=5.0)[0], 'Time exit (5.0s)')
        # And before any of them: no exit at all.
        self.assertIsNone(self._trigger(+3.49, held=4.9))
        self.assertIsNone(self._trigger(-9.99, held=0.0))

    def test_every_exit_sells_the_whole_position(self) -> None:
        for pnl, held in ((-10.0, 0.0), (+3.5, 0.0), (+0.1, 5.0)):
            pos = make_position(opened_ago=held)
            profile = self._profile(tp1_sell_pct=50.0)
            reason, amount = self._simulator()._exit_trigger(
                profile, pos, pnl, time.time(),
            )
            self.assertEqual(amount, pos.remaining_tokens_raw, reason)
            self.assertEqual(amount, pos.initial_tokens_raw, reason)

    def test_no_ladder_rule_can_fire(self) -> None:
        # Every rung of Round 6 that would have sold something is unreachable:
        # +30 % is a fee cover (100 %, not TP1's half), +50 % likewise, a flat
        # +2 % shelf with 10 s held is a time exit, and -10 % + 20 s is a stop.
        self.assertEqual(self._trigger(+30.0), ('Fee cover (+3.5%)', 1_000_000))
        self.assertEqual(self._trigger(+60.0), ('Fee cover (+3.5%)', 1_000_000))
        self.assertEqual(self._trigger(+2.0, held=10.0)[0], 'Time exit (5.0s)')
        self.assertEqual(self._trigger(-10.0, held=20.0)[0], 'Hard stop (−10.0%)')
        # A peak above +10 % does not arm a trail: the ladder is not consulted.
        simulator = self._simulator()
        pos = make_position(opened_ago=1.0, best_pnl=+20.0)
        self.assertIsNone(simulator._exit_trigger(self._profile(), pos, +1.0, time.time()))

    def test_thresholds_come_from_the_profile(self) -> None:
        self.assertEqual(
            self._trigger(+2.6, fee_cover_pct=2.5)[0], 'Fee cover (+2.5%)',
        )
        self.assertEqual(self._trigger(-7.5, hard_stop_pct=7.5)[0], 'Hard stop (−7.5%)')
        self.assertEqual(self._trigger(+1.0, held=2.0, time_exit_sec=2.0)[0],
                         'Time exit (2.0s)')
        # A tighter stop does not close a +1 % trade on its own.
        self.assertIsNone(self._trigger(+1.0, held=2.0, hard_stop_pct=7.5))

    def test_ladder_mode_is_untouched(self) -> None:
        # The same mark that fee_cover closes at +3.5 % leaves the ladder open.
        simulator = self._simulator()
        pos = make_position(opened_ago=1.0)
        self.assertIsNone(simulator._exit_trigger(ARMS['portfolio_s1'], pos, +3.5,
                                                  time.time()))

    def test_reasons_are_classified_not_unknown(self) -> None:
        kind = ABPaperSimulator.exit_reason_kind
        self.assertEqual(kind('Hard stop (−10.0%)'), 'hard_stop')
        self.assertEqual(kind('Fee cover (+3.5%)'), 'fee_cover')
        self.assertEqual(kind('Time exit (5.0s)'), 'time_exit')
        # The ladder's own labels still resolve as before.
        self.assertEqual(kind('SL (−10.0%)'), 'stop_loss')
        self.assertEqual(kind('SL/trail (peak=+12.0%, floor=+10.2%)'), 'trail')
        self.assertEqual(kind('TP1 +30% partial 50%'), 'tp')
        self.assertEqual(kind('Timeout (35.0s)'), 'timeout')

    def test_exit_breakdown_only_the_three_run8_kinds(self) -> None:
        simulator = self._simulator()
        book = simulator.books['portfolio_s1']
        book.closed_trades.extend([
            {'reason': 'Fee cover (+3.5%)', 'pnl_rub': 10.0, 'held_sec': 4.0},
            {'reason': 'Fee cover (+3.5%)', 'pnl_rub': 8.0, 'held_sec': 2.0},
            {'reason': 'Time exit (5.0s)', 'pnl_rub': -6.0, 'held_sec': 5.01},
            {'reason': 'Hard stop (−10.0%)', 'pnl_rub': -52.0, 'held_sec': 1.5},
        ])
        breakdown = simulator._exit_breakdown(book)
        self.assertEqual(sorted(breakdown), ['fee_cover', 'hard_stop', 'time_exit'])
        self.assertEqual(breakdown['fee_cover']['n'], 2)
        self.assertAlmostEqual(breakdown['fee_cover']['mean_rub'], 9.0, places=6)
        self.assertEqual(breakdown['time_exit']['n'], 1)
        self.assertEqual(breakdown['hard_stop']['n'], 1)

    def test_profile_validation_and_label(self) -> None:
        with patch.dict(os.environ, {'EXIT_MODE': 'fee_cover'}, clear=False):
            profile = profiles_from_settings(SimpleNamespace())['portfolio_s1']
            self.assertEqual(profile.exit_mode, 'fee_cover')
            label = ABPaperSimulator._exit_rule_label(profile)
            self.assertIn('fee cover', label)
            self.assertIn('no TP1', label)
            with patch.dict(os.environ, {'EXIT_FEE_COVER_PCT': '0'}, clear=False):
                with self.assertRaises(ValueError):
                    profiles_from_settings(SimpleNamespace())
        with patch.dict(os.environ, {'EXIT_MODE': 'fee-cover'}, clear=False):
            with self.assertRaises(ValueError):
                profiles_from_settings(SimpleNamespace())
        # The ladder label is what run #7 shipped.
        self.assertIn('4-mode ladder', ABPaperSimulator._exit_rule_label(PROFILE_A))

    def test_both_books_of_a_run_share_the_exit(self) -> None:
        with patch.dict(os.environ, {
            'ARMS': 'portfolio_s1,portfolio_s0', 'ALLOW_SINGLE_ARM': '1',
            'EXIT_MODE': 'fee_cover', 'EXIT_FEE_COVER_PCT': '3.5',
            'EXIT_TIME_SEC': '5.0', 'EXIT_HARD_STOP_PCT': '10.0',
        }, clear=False):
            profiles = profiles_from_settings(SimpleNamespace())
        exits = {
            (p.exit_mode, p.fee_cover_pct, p.time_exit_sec, p.hard_stop_pct)
            for p in profiles.values()
        }
        self.assertEqual(exits, {('fee_cover', 3.5, 5.0, 10.0)})


class RunSevenArmParityTests(unittest.IsolatedAsyncioTestCase):
    """The isolation rule, executed: one candidate, four books."""

    async def test_all_arms_enter_the_same_coin_at_the_same_price(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(Path(tmp))
            coin = NewCoin(
                mint='mint-parity', source='Pump.fun', signature='sig-parity',
                curve='curve-test', packet={'v_sol': 30.8},
            )
            snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
            epoch = time.time()
            with patch('portfolio_ab.get_collector', return_value=None):
                await asyncio.gather(*(
                    simulator._consider_entry(
                        simulator.books[name], coin, snapshot,
                        30.8, 31.2, 200.0, {}, epoch,
                    )
                    for name in simulator.books
                ))
            positions = {
                name: book.positions['mint-parity'] for name, book in simulator.books.items()
            }
            # 30.8 birth and a 0.4 delta sit inside every declared band.
            self.assertEqual(sorted(positions), sorted(ARM_IDS))
            for name, pos in positions.items():
                self.assertEqual(pos.initial_tokens_raw,
                                 positions['portfolio_a'].initial_tokens_raw, name)
                self.assertAlmostEqual(pos.entry_cost_rub, 500.0, places=3)
                self.assertEqual(pos.opened_at_epoch, epoch)
            # And every book's cash is down by exactly one slot.
            for name, book in simulator.books.items():
                self.assertAlmostEqual(book.cash_balance_rub, 1_000.0, places=3, msg=name)

    async def test_t0_arm_enters_the_zero_impulse_coin_the_others_skip(self) -> None:
        # delta == 0: no impulse since detection. A/B1/B2 wait for momentum,
        # B3 buys the first curve read (that is the whole point of the arm).
        with tempfile.TemporaryDirectory() as tmp:
            simulator = make_simulator(Path(tmp))
            coin = NewCoin(
                mint='mint-t0', source='Pump.fun', signature='sig-t0',
                curve='curve-test', packet={'v_sol': 30.8},
            )
            snapshot = EntrySnapshot(make_curve(), make_fee_context_for_test(), 12, time.time())
            epoch = time.time()
            with patch('portfolio_ab.get_collector', return_value=None):
                for name, book in simulator.books.items():
                    await simulator._consider_entry(
                        book, coin, snapshot, 30.8, 30.8, 200.0, {}, epoch,
                    )
            self.assertIn('mint-t0', simulator.books['portfolio_b3_t0'].positions)
            for name in ('portfolio_a', 'portfolio_b1_narrow', 'portfolio_b2_birth'):
                self.assertNotIn('mint-t0', simulator.books[name].positions, name)
                self.assertAlmostEqual(
                    simulator.books[name].cash_balance_rub, 1_500.0, places=3, msg=name,
                )
