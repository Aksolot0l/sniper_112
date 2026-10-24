"""Run #10b §4 — the funnel must read as candidates, in the order of the checks.

Run #10a reported 525 copy rejects next to 544 `portfolio_s2_entry` rows, and
the natural reading — "96.5 % of the entries were stopped by copies" — was
wrong. The two sets share no mint: 539 coins reached `portfolio_s2_signal`, 525
were stopped before it, and 539 + 525 = 1 064, exactly the candidate count.
"""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from main_ab import _funnel_counts, _run9_summary_markdown, _safe_manifest
from tests.test_run_report import _settings
from tests.test_run9_report import RUN9_ENV


def _funnel() -> dict:
    """The exact 10a numbers."""
    return {
        'security_pass': 1064,
        'entry_state': 1064,
        'portfolio_s0_signal': {
            'birth_out_of_band': 768, 'live_momentum_entry': 129,
            'momentum_too_late': 167,
        },
        'portfolio_s2_signal': {
            'birth_out_of_band': 474, 'live_momentum_entry': 19,
            'momentum_too_late': 46,
        },
        'entry_skips': {'portfolio_s0': {}, 'portfolio_s2': {'name_copies_120s': 525}},
        'opened': {'portfolio_s0': 129, 'portfolio_s2': 19},
    }


def _report(funnel: dict) -> str:
    with patch.dict(os.environ, {**RUN9_ENV, 'RUN_DURATION_SEC': '3600'}, clear=True):
        manifest = _safe_manifest(_settings(), 'run-10', Path('/tmp/run-10'))
    manifest['actual_duration_sec'] = 3_600
    manifest['telemetry'] = {'dropped_events': 0, 'write_errors': 0}
    summary = {
        'portfolio_s0': {'stats': {'n': 129}},
        'portfolio_s2': {'stats': {'n': 19}},
        'profiles': {'portfolio_s0': {'arm_id': 'S0'},
                     'portfolio_s2': {'arm_id': 'S2'}},
        'mark_poll': {'mark_poll_sec': 0.25, 'mark_poll_adapted': False,
                      'rps_limit_10min': 2.5},
        'drain': {'left_open': 0, 'waited_sec': 0.0},
    }
    return _run9_summary_markdown(manifest, funnel, summary)


def _row(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if line.startswith(prefix):
            return line
    raise AssertionError(f'row {prefix!r} not found')


class FunnelTableTests(unittest.TestCase):
    def test_s2_is_read_as_shares_of_all_candidates(self) -> None:
        row = _row(_report(_funnel()), '| S2 | 1064 |')
        self.assertIn('525 (49.34 %)', row)
        self.assertIn('474 (44.55 %)', row)
        self.assertIn('46 (4.32 %)', row)
        self.assertIn('19 (1.79 %)', row)
        self.assertIn('56.00', row)  # candidates per opened trade

    def test_s0_has_no_copy_rejects(self) -> None:
        row = _row(_report(_funnel()), '| S0 | 1064 |')
        self.assertIn('0 (0.00 %)', row)
        self.assertIn('768 (72.18 %)', row)
        self.assertIn('167 (15.70 %)', row)
        self.assertIn('129 (12.12 %)', row)

    def test_the_row_sums_to_the_candidates(self) -> None:
        counts = _funnel_counts(_funnel(), 'portfolio_s2')
        self.assertEqual(counts['candidates'], 1064)
        self.assertEqual(
            counts['copies'] + counts['band'] + counts['moment']
            + counts['other'] + counts['opened'],
            1064,
        )
        self.assertEqual(counts['other'], 0)

    def test_the_order_of_checks_is_stated(self) -> None:
        text = _report(_funnel())
        self.assertIn('копии → полоса → момент', text)
        self.assertIn('Множества не пересекаются', text)

    def test_a_runtime_reject_without_a_stage_lands_in_other(self) -> None:
        funnel = _funnel()
        funnel['opened'] = {'portfolio_s0': 129, 'portfolio_s2': 10}
        funnel['entry_skips'] = {'portfolio_s0': {}, 'portfolio_s2': {
            'name_copies_120s': 525, 'slots_full': 9,
        }}
        counts = _funnel_counts(funnel, 'portfolio_s2')
        self.assertEqual(counts['other'], 9)
        self.assertEqual(
            counts['copies'] + counts['band'] + counts['moment']
            + counts['other'] + counts['opened'],
            1064,
        )

    def test_an_empty_funnel_does_not_invent_numbers(self) -> None:
        counts = _funnel_counts({}, 'portfolio_s0')
        self.assertEqual(counts['candidates'], 0)
        self.assertIsNone(counts['candidates_per_trade'])
        self.assertIn('| S0 | 0 |', _report({}))


if __name__ == '__main__':
    unittest.main()
