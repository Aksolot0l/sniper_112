"""Run #10 §2b: the run number is declared, and both stages share it.

Run #10a (1 hour) and run #10b (12 hours) run the same code, the same books and
the same exit, so deriving the number from the arm composition would keep
calling both of them `run9`, and a declared `run10` would lose the report page.
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from main_ab import RUN9_PAGE_NUMBERS, _is_run9, _run_number
from run_local_session import child_environment

PROJECT = Path(__file__).resolve().parent.parent
RUN10_ENV = {
    'ARMS': 'portfolio_s0,portfolio_s2', 'ALLOW_SINGLE_ARM': '1',
    'ENTRY_BIRTH_MIN_S0': '30.50', 'MAX_NAME_COPIES_S0': 'off',
    'ENTRY_BIRTH_MIN_S2': '31.96', 'MAX_NAME_COPIES_S2': '0',
    'ENTRY_DELTA_MIN_SOL': '0.00', 'ENTRY_DELTA_MAX_SOL': '1.00',
    'EXIT_MODE': 'fee_cover',
}


class RunNumberTests(unittest.TestCase):
    def test_the_declared_number_wins_over_the_composition(self) -> None:
        with patch.dict(os.environ, {**RUN10_ENV, 'RUN_NUMBER': 'run10'}, clear=True):
            self.assertEqual(_run_number(), 'run10')

    def test_without_the_declaration_the_composition_still_names_run9(self) -> None:
        # The S0/S2 books are run #9's experiment; the declared number is what
        # makes run #10 a different label, not a different configuration.
        with patch.dict(os.environ, RUN10_ENV, clear=True):
            self.assertEqual(_run_number(), 'run9')


class RunTenReportRoutingTests(unittest.TestCase):
    def test_both_declared_numbers_open_the_selection_page(self) -> None:
        self.assertEqual(RUN9_PAGE_NUMBERS, ('run9', 'run10'))
        for number in ('run9', 'run10'):
            self.assertTrue(_is_run9({'run_number': number}), number)

    def test_another_run_still_gets_its_own_page(self) -> None:
        self.assertFalse(_is_run9({'run_number': 'run8'}))
        self.assertFalse(_is_run9({'run_number': 'run7'}))

    def test_a_manifest_without_a_number_falls_back_to_the_books(self) -> None:
        self.assertTrue(_is_run9({
            'portfolio_scope': ['portfolio_s0', 'portfolio_s2'],
            'entry_parameters': {'portfolio_s2': {'v_sol_birth_min': 31.96}},
        }))


class ChildEnvironmentNumberTests(unittest.TestCase):
    def test_the_child_is_told_which_run_it_is(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            env = child_environment('run10a-1h-test', PROJECT / 'runs' / 'x', 3600)
            self.assertEqual(env['RUN_NUMBER'], 'run10')
            self.assertEqual(env['RUN_DURATION_SEC'], '3600')
        with patch.dict(os.environ, {'RUN_NUMBER': 'run99'}, clear=True):
            env = child_environment('run99', PROJECT / 'runs' / 'x', 60)
            self.assertEqual(env['RUN_NUMBER'], 'run99')


class LauncherNumberTests(unittest.TestCase):
    def test_the_shell_launchers_declare_run10(self) -> None:
        sh = (PROJECT / 'START_MAC_LINUX.sh').read_text(encoding='utf-8')
        bat = (PROJECT / 'START_WINDOWS.bat').read_text(encoding='utf-8')
        self.assertIn('RUN_NUMBER="${RUN_NUMBER:-run10}"', sh)
        self.assertIn('set "RUN_NUMBER=run10"', bat)


if __name__ == '__main__':
    unittest.main()
