from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.preflight_budget import check, main  # noqa: E402


def _summary(*, coins: int | None, requests: int, http_429: int = 0,
             retries: int = 0, unrecovered: int = 0) -> dict:
    chain = 'candidates=100 -> security_pass=%d -> admits=3' % coins if coins else 'n/a'
    return {
        'rpc_metrics': {
            'requests': requests, 'failed_requests': http_429,
            'http_429_count': http_429, 'retry_count': retries,
            'unrecovered_failures': unrecovered, 'latency_ms_p95': 90.0,
        },
        'funnel': {'chain': chain},
    }


class PreflightBudgetTests(unittest.TestCase):
    def test_quiet_run_passes(self) -> None:
        result = check(_summary(coins=100, requests=500))
        self.assertTrue(result['go'], result['reasons'])
        self.assertEqual(result['request_budget'], 900.0)
        self.assertEqual(result['requests_per_coin'], 5.0)

    def test_budget_is_one_and_a_half_reads_per_coin_times_six(self) -> None:
        self.assertFalse(check(_summary(coins=100, requests=901))['go'])
        self.assertTrue(check(_summary(coins=100, requests=900))['go'])

    def test_recovered_429_burst_is_back_pressure_not_a_stop(self) -> None:
        result = check(_summary(coins=100, requests=600, http_429=40,
                                retries=40, unrecovered=0))
        self.assertTrue(result['go'], result['reasons'])
        self.assertEqual(result['http_429_count'], 40)

    def test_unrecovered_429s_without_retries_block_the_run(self) -> None:
        result = check(_summary(coins=100, requests=600, http_429=5))
        self.assertFalse(result['go'])
        self.assertTrue(any('back-pressure' in r for r in result['reasons']))

    def test_two_percent_of_unrecovered_errors_is_the_ceiling(self) -> None:
        # 2 % exactly is allowed, 2.01 % is not (budget is fine at 200 coins).
        self.assertTrue(check(_summary(coins=200, requests=1000,
                                       unrecovered=20))['go'])
        self.assertFalse(check(_summary(coins=200, requests=1000,
                                        unrecovered=21))['go'])

    def test_missing_funnel_is_reported_not_guessed(self) -> None:
        result = check(_summary(coins=None, requests=10))
        self.assertFalse(result['go'])
        self.assertIsNone(result['request_budget'])

    def test_cli_reads_manifest_funnel_and_exits_nonzero_on_no_go(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / 'ab_summary.json').write_text(json.dumps({
                'rpc_metrics': {'requests': 600, 'http_429_count': 0,
                                'retry_count': 3, 'unrecovered_failures': 0},
            }), encoding='utf-8')
            (run_dir / 'manifest.json').write_text(json.dumps({
                'funnel': {'chain': 'candidates=10 -> security_pass=120 -> opened=2'},
                'entry_latency_ms': 150.0,
            }), encoding='utf-8')
            self.assertEqual(main([str(run_dir)]), 0)
            (run_dir / 'ab_summary.json').write_text(json.dumps({
                'rpc_metrics': {'requests': 5000, 'http_429_count': 0,
                                'retry_count': 0, 'unrecovered_failures': 0},
            }), encoding='utf-8')
            self.assertEqual(main([str(run_dir)]), 1)


if __name__ == '__main__':
    unittest.main()
