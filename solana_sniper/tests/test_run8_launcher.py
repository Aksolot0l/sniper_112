"""Run #8 ships three entry points; all three must declare the same run.

The reviewer launches the run from a shell script, but the environment that
actually reaches `main_ab.py` is built in `run_local_session.child_environment`.
If those two disagree, the run is not the run that was requested — so the
defaults are asserted here, in one place, from both sides.
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from portfolio_ab import profiles_from_settings
from run_local_session import build_parser, child_environment

PROJECT = Path(__file__).resolve().parent.parent

RUN8_ENV = {
    'ARMS': 'portfolio_s1,portfolio_s0',
    'ALLOW_SINGLE_ARM': '1',
    'EXIT_MODE': 'fee_cover',
    'EXIT_FEE_COVER_PCT': '3.5',
    'EXIT_TIME_SEC': '5.0',
    'EXIT_HARD_STOP_PCT': '10.0',
    'ENTRY_DELTA_MAX_SOL_S1': '1.00',
    'ENTRY_DELTA_MIN_SOL_S0': '0.00',
    'ENTRY_LATENCY_MS': '0',
    'MARK_POLL_SEC': '0.15',
    'TELEMETRY_OBSERVATION_OFFSETS_SEC': '0,5',
}


class ChildEnvironmentTests(unittest.TestCase):
    def _env(self, **overrides):
        with patch.dict(os.environ, overrides, clear=True):
            return child_environment('run8-session', Path('/tmp/run8'), 1800)

    def test_defaults_are_run8(self) -> None:
        env = self._env()
        for key, value in RUN8_ENV.items():
            self.assertEqual(env[key], value, key)
        # Frozen bank and stake: one cash box per book, never halved.
        self.assertEqual(env['DEPOSIT_RUB'], '1500')
        self.assertEqual(env['MAX_TRADE_RUB'], '500')
        self.assertEqual(env['MAX_POSITIONS'], '3')
        self.assertEqual(env['PAPER_ONLY'], '1')
        self.assertEqual(env['RUN_DURATION_SEC'], '1800')

    def test_env_can_override_every_run8_default(self) -> None:
        env = self._env(
            ARMS='portfolio_a', EXIT_MODE='ladder', MARK_POLL_SEC='0.25',
            TELEMETRY_OBSERVATION_OFFSETS_SEC='0,5,15,60',
        )
        self.assertEqual(env['ARMS'], 'portfolio_a')
        self.assertEqual(env['EXIT_MODE'], 'ladder')
        self.assertEqual(env['MARK_POLL_SEC'], '0.25')
        self.assertEqual(env['TELEMETRY_OBSERVATION_OFFSETS_SEC'], '0,5,15,60')

    def test_the_child_env_builds_the_declared_books(self) -> None:
        env = self._env()
        with patch.dict(os.environ, env, clear=False):
            profiles = profiles_from_settings(object())
        self.assertEqual(list(profiles), ['portfolio_s1', 'portfolio_s0'])
        s1, s0 = profiles['portfolio_s1'], profiles['portfolio_s0']
        # Run #9 added the name-copy clause to the band label: an entry
        # parameter belongs next to the band it is part of.
        self.assertEqual(s1.entry_band_label,
                         '30.50 <= v_sol_birth <= 34.50; 0.10 <= delta <= 1.00; '
                         'name copies: off')
        self.assertEqual(s0.entry_band_label,
                         '30.50 <= v_sol_birth <= 34.50; 0.00 <= delta <= 1.00; '
                         'name copies: off')
        self.assertEqual((s1.exit_mode, s1.fee_cover_pct, s1.time_exit_sec,
                          s1.hard_stop_pct), ('fee_cover', 3.5, 5.0, 10.0))
        self.assertEqual((s0.exit_mode, s0.fee_cover_pct, s0.time_exit_sec,
                          s0.hard_stop_pct), ('fee_cover', 3.5, 5.0, 10.0))
        self.assertEqual(s1.entry_latency_ms, 0.0)
        for profile in (s1, s0):
            self.assertEqual(profile.max_slots, 3)
            self.assertEqual(profile.position_size_rub, 500.0)
            self.assertEqual(profile.initial_balance_rub, 1500.0)

    def test_parser_default_is_thirty_minutes(self) -> None:
        self.assertEqual(build_parser().parse_args([]).duration, 1800)


class ShippedScriptDefaultsTests(unittest.TestCase):
    """The shell entry points carry the same numbers as the child env."""

    def _text(self, name: str) -> str:
        return (PROJECT / name).read_text(encoding='utf-8')

    def test_linux_launcher(self) -> None:
        script = self._text('START_MAC_LINUX.sh')
        self.assertIn('export ARMS="${ARMS:-portfolio_s1,portfolio_s0}"', script)
        self.assertIn('export ALLOW_SINGLE_ARM="${ALLOW_SINGLE_ARM:-1}"', script)
        self.assertIn('export EXIT_MODE="${EXIT_MODE:-fee_cover}"', script)
        self.assertIn('export EXIT_FEE_COVER_PCT="${EXIT_FEE_COVER_PCT:-3.5}"', script)
        self.assertIn('export EXIT_TIME_SEC="${EXIT_TIME_SEC:-5.0}"', script)
        self.assertIn('export EXIT_HARD_STOP_PCT="${EXIT_HARD_STOP_PCT:-10.0}"', script)
        self.assertIn('export ENTRY_DELTA_MAX_SOL_S1="${ENTRY_DELTA_MAX_SOL_S1:-1.00}"', script)
        self.assertIn('export ENTRY_DELTA_MIN_SOL_S0="${ENTRY_DELTA_MIN_SOL_S0:-0.00}"', script)
        self.assertIn('export ENTRY_LATENCY_MS="${ENTRY_LATENCY_MS:-0}"', script)
        self.assertIn('export MARK_POLL_SEC="${MARK_POLL_SEC:-0.15}"', script)
        self.assertIn('TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}', script)
        self.assertIn('--duration-sec "${DURATION_SEC:-1800}"', script)
        self.assertIn('MAX_TRADE_RUB:-500', script)
        self.assertIn('MAX_POSITIONS:-3', script)
        self.assertNotIn('28800', script)

    def test_windows_launcher(self) -> None:
        script = self._text('START_WINDOWS.bat')
        self.assertIn('set "ARMS=portfolio_s1,portfolio_s0"', script)
        self.assertIn('set "ALLOW_SINGLE_ARM=1"', script)
        self.assertIn('set "EXIT_MODE=fee_cover"', script)
        self.assertIn('set "EXIT_FEE_COVER_PCT=3.5"', script)
        self.assertIn('set "EXIT_TIME_SEC=5.0"', script)
        self.assertIn('set "EXIT_HARD_STOP_PCT=10.0"', script)
        self.assertIn('set "ENTRY_DELTA_MAX_SOL_S1=1.00"', script)
        self.assertIn('set "ENTRY_DELTA_MIN_SOL_S0=0.00"', script)
        self.assertIn('set "ENTRY_LATENCY_MS=0"', script)
        self.assertIn('set "MARK_POLL_SEC=0.15"', script)
        self.assertIn('set "TELEMETRY_OBSERVATION_OFFSETS_SEC=0,5"', script)
        self.assertIn('set "DURATION_SEC=1800"', script)
        self.assertIn('--duration-sec %DURATION_SEC%', script)

    def test_preflight_launcher(self) -> None:
        script = self._text('run_rocket2_ab_paper.sh')
        self.assertIn('RUN_DURATION_SEC:-1800', script)
        self.assertIn('ARMS:-portfolio_s1,portfolio_s0', script)
        self.assertIn('EXIT_MODE:-fee_cover', script)
        self.assertIn('MARK_POLL_SEC:-0.15', script)
        self.assertIn('TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}', script)
        self.assertIn('wallet_portfolio_s1.json', script)


if __name__ == '__main__':
    unittest.main()
