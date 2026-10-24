"""Run #10b §5 — the cadence fuse must not read like a protection.

In 10a `manifest.mark_poll` carried rps_seen_at_last_check = 2.967 against a
2.5 rps limit and `adaptation_blocked = true`: the run starts at
MARK_POLL_SEC = 0.25, which is exactly the value the fuse could slow down to.
It saw the overshoot and could do nothing. The report now says that in words.
"""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from main_ab import _pace_fuse_text, _run9_summary_markdown, _safe_manifest
from tests.test_run_report import _settings
from tests.test_run9_report import RUN9_ENV


def _line(mark_poll: dict) -> str:
    for line in _report(mark_poll).splitlines():
        if line.startswith('- **предохранитель темпа:**'):
            return line
    raise AssertionError('pace fuse line is missing')


def _report(mark_poll: dict) -> str:
    with patch.dict(os.environ, {**RUN9_ENV, 'RUN_DURATION_SEC': '3600'}, clear=True):
        manifest = _safe_manifest(_settings(), 'run-10', Path('/tmp/run-10'))
    manifest['actual_duration_sec'] = 3_600
    manifest['telemetry'] = {'dropped_events': 0, 'write_errors': 0}
    summary = {
        'portfolio_s0': {'stats': {'n': 1}},
        'portfolio_s2': {'stats': {'n': 1}},
        'profiles': {'portfolio_s0': {'arm_id': 'S0'},
                     'portfolio_s2': {'arm_id': 'S2'}},
        'mark_poll': mark_poll,
        'drain': {'left_open': 0, 'waited_sec': 0.0},
    }
    return _run9_summary_markdown(manifest, {}, summary)


class PaceFuseTests(unittest.TestCase):
    def test_the_10a_state_reads_as_inactive_not_as_a_net(self) -> None:
        line = _line({
            'mark_poll_sec': 0.25, 'mark_poll_adapted': False,
            'adaptation_blocked': True, 'rps_limit_10min': 2.5,
            'rps_seen_at_last_check': 2.967,
        })
        self.assertIn('неактивен, каденция уже на полу', line)
        self.assertIn('каденция 0.25 с', line)
        self.assertIn('порог 2.5 rps', line)
        self.assertIn('видел 2.967 rps', line)
        self.assertIn('это не защита', line)

    def test_a_fuse_that_never_fired_says_so(self) -> None:
        text = _pace_fuse_text({
            'mark_poll_sec': 0.25, 'mark_poll_adapted': False,
            'adaptation_blocked': False, 'rps_limit_10min': 2.5,
            'rps_seen_at_last_check': 1.1,
        })
        self.assertIn('активен, превышения не было', text)
        self.assertNotIn('не защита', text)

    def test_an_actual_adaptation_is_named(self) -> None:
        text = _pace_fuse_text({
            'mark_poll_sec': 0.25, 'mark_poll_adapted': True,
            'adaptation_blocked': False, 'rps_limit_10min': 2.2,
            'rps_seen_at_last_check': 2.5,
        })
        self.assertIn('сработал, каденция замедлена', text)

    def test_the_line_groups_by_nothing_but_the_fuse_itself(self) -> None:
        # The cadence is run-level, not per book: exactly one line, no arm names.
        text = _report({'mark_poll_sec': 0.25, 'adaptation_blocked': True,
                        'rps_limit_10min': 2.5, 'rps_seen_at_last_check': 2.967})
        fuse = [line for line in text.splitlines() if 'предохранитель темпа' in line]
        self.assertEqual(len(fuse), 1)
        self.assertNotIn('S0', fuse[0])
        self.assertNotIn('S2', fuse[0])


if __name__ == '__main__':
    unittest.main()
