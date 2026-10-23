"""Run #9 ships three entry points; all three must declare the same run.

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

RUN9_ENV = {
    'ARMS': 'portfolio_s0,portfolio_s2',
    'ALLOW_SINGLE_ARM': '1',
    'EXIT_MODE': 'fee_cover',
    'EXIT_FEE_COVER_PCT': '3.5',
    'EXIT_TIME_SEC': '5.0',
    'EXIT_HARD_STOP_PCT': '10.0',
    'ENTRY_DELTA_MIN_SOL': '0.00',
    'ENTRY_DELTA_MAX_SOL': '1.00',
    'ENTRY_BIRTH_MIN_S0': '30.50',
    'MAX_NAME_COPIES_S0': 'off',
    'ENTRY_BIRTH_MIN_S2': '31.96',
    'MAX_NAME_COPIES_S2': '0',
    'ENTRY_LATENCY_MS': '0',
    'MARK_POLL_SEC': '0.25',
    'MARK_POLL_RPS_LIMIT': '2.5',
    'EARLY_STOP_NEGATIVE_EDGE_AT_SEC': '28800',
    'EARLY_STOP_NEGATIVE_EDGE_BOOK': 'portfolio_s2',
    'TELEMETRY_OBSERVATION_OFFSETS_SEC': '0,5',
}


class ChildEnvironmentTests(unittest.TestCase):
    def _env(self, **overrides):
        with patch.dict(os.environ, overrides, clear=True):
            return child_environment('run9-session', Path('/tmp/run9'), 1800)

    def test_defaults_are_run9(self) -> None:
        env = self._env()
        for key, value in RUN9_ENV.items():
            self.assertEqual(env[key], value, key)
        # The wallet anchor follows the first declared book, not a hardcoded S1.
        self.assertEqual(Path(env['WALLET_PATH']).name, 'wallet_portfolio_s0.json')
        # Frozen bank and stake: one cash box per book, never halved.
        self.assertEqual(env['DEPOSIT_RUB'], '1500')
        self.assertEqual(env['MAX_TRADE_RUB'], '500')
        self.assertEqual(env['MAX_POSITIONS'], '3')
        self.assertEqual(env['PAPER_ONLY'], '1')
        self.assertEqual(env['RUN_DURATION_SEC'], '1800')

    def test_env_can_override_every_run9_default(self) -> None:
        env = self._env(
            ARMS='portfolio_a', EXIT_MODE='ladder', MARK_POLL_SEC='0.15',
            TELEMETRY_OBSERVATION_OFFSETS_SEC='0,5,15,60',
        )
        self.assertEqual(env['ARMS'], 'portfolio_a')
        self.assertEqual(env['EXIT_MODE'], 'ladder')
        self.assertEqual(env['MARK_POLL_SEC'], '0.15')
        self.assertEqual(env['TELEMETRY_OBSERVATION_OFFSETS_SEC'], '0,5,15,60')
        # And the wallet anchor follows the override.
        self.assertEqual(Path(env['WALLET_PATH']).name, 'wallet_portfolio_a.json')

    def test_the_child_env_builds_the_declared_books(self) -> None:
        env = self._env()
        with patch.dict(os.environ, env, clear=False):
            profiles = profiles_from_settings(object())
        self.assertEqual(list(profiles), ['portfolio_s0', 'portfolio_s2'])
        s0, s2 = profiles['portfolio_s0'], profiles['portfolio_s2']
        # The band label carries the copies rule: an entry parameter belongs
        # next to the band it is part of.
        self.assertEqual(s0.entry_band_label,
                         '30.50 <= v_sol_birth <= 34.50; 0.00 <= delta <= 1.00; '
                         'name copies: off')
        self.assertEqual(s2.entry_band_label,
                         '31.96 <= v_sol_birth <= 34.50; 0.00 <= delta <= 1.00; '
                         'name copies 120s <= 0')
        self.assertIsNone(s0.max_name_copies_120s)
        self.assertEqual(s2.max_name_copies_120s, 0)
        # One unchanged exit for both books.
        for profile in (s0, s2):
            self.assertEqual(
                (profile.exit_mode, profile.fee_cover_pct, profile.time_exit_sec,
                 profile.hard_stop_pct), ('fee_cover', 3.5, 5.0, 10.0))
            self.assertEqual(profile.entry_latency_ms, 0.0)
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
        self.assertIn('export ARMS="${ARMS:-portfolio_s0,portfolio_s2}"', script)
        self.assertIn('export ENTRY_BIRTH_MIN_S0="${ENTRY_BIRTH_MIN_S0:-30.50}"', script)
        self.assertIn('export MAX_NAME_COPIES_S0="${MAX_NAME_COPIES_S0:-off}"', script)
        self.assertIn('export ENTRY_BIRTH_MIN_S2="${ENTRY_BIRTH_MIN_S2:-31.96}"', script)
        self.assertIn('export MAX_NAME_COPIES_S2="${MAX_NAME_COPIES_S2:-0}"', script)
        self.assertIn('EARLY_STOP_NEGATIVE_EDGE_AT_SEC:-28800', script)
        self.assertIn('MARK_POLL_RPS_LIMIT:-2.5', script)
        self.assertIn('export ALLOW_SINGLE_ARM="${ALLOW_SINGLE_ARM:-1}"', script)
        self.assertIn('export EXIT_MODE="${EXIT_MODE:-fee_cover}"', script)
        self.assertIn('export EXIT_FEE_COVER_PCT="${EXIT_FEE_COVER_PCT:-3.5}"', script)
        self.assertIn('export EXIT_TIME_SEC="${EXIT_TIME_SEC:-5.0}"', script)
        self.assertIn('export EXIT_HARD_STOP_PCT="${EXIT_HARD_STOP_PCT:-10.0}"', script)
        self.assertIn('export ENTRY_DELTA_MAX_SOL_S1="${ENTRY_DELTA_MAX_SOL_S1:-1.00}"', script)
        self.assertIn('export ENTRY_DELTA_MIN_SOL_S0="${ENTRY_DELTA_MIN_SOL_S0:-0.00}"', script)
        self.assertIn('export ENTRY_LATENCY_MS="${ENTRY_LATENCY_MS:-0}"', script)
        self.assertIn('export MARK_POLL_SEC="${MARK_POLL_SEC:-0.25}"', script)
        self.assertIn('TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}', script)
        self.assertIn('--duration-sec "${DURATION_SEC:-1800}"', script)
        self.assertIn('MAX_TRADE_RUB:-500', script)
        self.assertIn('MAX_POSITIONS:-3', script)
        # The A smoke is the default; B is the same script with 43200.
        self.assertIn('43200', script)

    def test_windows_launcher(self) -> None:
        script = self._text('START_WINDOWS.bat')
        self.assertIn('set "ARMS=portfolio_s0,portfolio_s2"', script)
        self.assertIn('set "ENTRY_BIRTH_MIN_S0=30.50"', script)
        self.assertIn('set "MAX_NAME_COPIES_S0=off"', script)
        self.assertIn('set "ENTRY_BIRTH_MIN_S2=31.96"', script)
        self.assertIn('set "MAX_NAME_COPIES_S2=0"', script)
        self.assertIn('set "EARLY_STOP_NEGATIVE_EDGE_AT_SEC=28800"', script)
        self.assertIn('set "ALLOW_SINGLE_ARM=1"', script)
        self.assertIn('set "EXIT_MODE=fee_cover"', script)
        self.assertIn('set "EXIT_FEE_COVER_PCT=3.5"', script)
        self.assertIn('set "EXIT_TIME_SEC=5.0"', script)
        self.assertIn('set "EXIT_HARD_STOP_PCT=10.0"', script)
        self.assertIn('set "ENTRY_DELTA_MAX_SOL_S1=1.00"', script)
        self.assertIn('set "ENTRY_DELTA_MIN_SOL_S0=0.00"', script)
        self.assertIn('set "ENTRY_LATENCY_MS=0"', script)
        self.assertIn('set "MARK_POLL_SEC=0.25"', script)
        self.assertIn('set "MARK_POLL_RPS_LIMIT=2.5"', script)
        self.assertIn('set "TELEMETRY_OBSERVATION_OFFSETS_SEC=0,5"', script)
        self.assertIn('set "DURATION_SEC=1800"', script)
        self.assertIn('--duration-sec %DURATION_SEC%', script)

    def test_preflight_launcher(self) -> None:
        script = self._text('run_rocket2_ab_paper.sh')
        self.assertIn('RUN_DURATION_SEC:-1800', script)
        self.assertIn('ARMS:-portfolio_s0,portfolio_s2', script)
        self.assertIn('EXIT_MODE:-fee_cover', script)
        self.assertIn('MARK_POLL_SEC:-0.25', script)
        self.assertIn('TELEMETRY_OBSERVATION_OFFSETS_SEC:-0,5}', script)
        self.assertIn('wallet_portfolio_s0.json', script)
        self.assertIn('run #9 preflight OK', script)


if __name__ == '__main__':
    unittest.main()
