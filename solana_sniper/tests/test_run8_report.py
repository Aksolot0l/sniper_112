"""Run #8 §3: the report must carry the seven blocks the run is about.

The point of this report is that it answers latency/cost/funnel questions and
explicitly refuses to answer the money question; the tests check presence,
arithmetic, and the absence of a run #7 paired-with-A verdict.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import os

from main_ab import _run8_summary_markdown, _is_run8
from portfolio_ab import profiles_from_settings
from tests.test_portfolio_ab import make_simulator


def _simulator_with_trades():
    """S1/S0 books with two closed trades each and a real tick stream."""
    with patch.dict(os.environ, {
        'ARMS': 'portfolio_s1,portfolio_s0', 'ALLOW_SINGLE_ARM': '1',
        'EXIT_MODE': 'fee_cover',
    }, clear=False):
        profiles = profiles_from_settings(object())
    simulator = make_simulator(Path(tempfile.mkdtemp()), profiles=profiles)
    for name, book in simulator.books.items():
        book.closed_trades.extend([
            {
                'mint': f'{name}-a', 'reason': 'Fee cover (+3.5%)',
                'pnl_rub': 12.0, 'pnl_pct': 2.4, 'held_sec': 3.0,
                'entry_price_usd': 0.0001, 'average_exit_price_usd': 0.00011,
                'entry_premium_pct': 2.9 if name == 'portfolio_s1' else 0.1,
                'entry_fill_premium_pct': 1.4, 'mfe_pct': 2.6, 'mae_pct': -0.2,
                't_detect_ms': 12.0, 't_filters_ms': 180.0, 't_read_ms': 0.0,
                't_fill_ms': 4.0, 't_total_ms': 196.0,
            },
            {
                'mint': f'{name}-b', 'reason': 'Time exit (5.0s)',
                'pnl_rub': -6.0, 'pnl_pct': -1.2, 'held_sec': 5.0,
                'entry_price_usd': 0.0001, 'average_exit_price_usd': 0.0000988,
                'entry_premium_pct': 5.0 if name == 'portfolio_s1' else 0.2,
                'entry_fill_premium_pct': 1.5, 'mfe_pct': 0.2, 'mae_pct': -0.1,
                't_detect_ms': 20.0, 't_filters_ms': 260.0, 't_read_ms': 0.0,
                't_fill_ms': 5.0, 't_total_ms': 285.0,
            },
        ])
        for mint, marks in (
            (f'{name}-a', [(0.0, 0.0), (1.1, 1.0), (2.1, 2.0), (3.1, 3.5), (5.2, 2.4)]),
            (f'{name}-b', [(0.0, 0.0), (1.1, -1.0), (2.6, -12.0), (5.1, -1.2)]),
        ):
            for at, pnl in marks:
                simulator.ticks.append({
                    'portfolio': name, 'mint': mint, 't_sec': at,
                    'pnl_pct': pnl, 'event': 'mark',
                })
    return simulator


class ManifestRun8Tests(unittest.TestCase):
    """The manifest must describe the exit that will actually run."""

    def _manifest(self, **env):
        from types import SimpleNamespace

        from main_ab import _safe_manifest

        settings = SimpleNamespace(
            http_rpc_url='https://mainnet.helius-rpc.com/?api-key=k',
            wss_url='wss://mainnet.helius-rpc.com/?api-key=k',
            entry_fee_sol=0.00008, exit_fee_sol=0.00005, usd_rub_rate=90.0,
        )
        with patch.dict(os.environ, env, clear=True):
            return _safe_manifest(settings, 'run-8', Path('/tmp/run-8'))

    def test_fee_cover_run_declares_fee_cover(self) -> None:
        manifest = self._manifest(
            RUN_DURATION_SEC='1800', ARMS='portfolio_s1,portfolio_s0',
            ALLOW_SINGLE_ARM='1', EXIT_MODE='fee_cover', ENTRY_LATENCY_MS='0',
            MARK_POLL_SEC='0.15', TELEMETRY_OBSERVATION_OFFSETS_SEC='0,5',
        )
        self.assertEqual(manifest['exit_mode'], 'fee_cover')
        self.assertIn('fee cover', manifest['exit_rule'])
        self.assertIn('no TP1', manifest['exit_rule'])
        self.assertNotIn('4-mode ladder', manifest['exit_rule'])
        self.assertEqual(manifest['entry_latency_ms'], 0.0)
        self.assertEqual(manifest['observations_sec'], [0, 5])
        self.assertIsNone(manifest['ab_test']['reference_arm'])
        self.assertIn('сравнения с A', manifest['ab_test']['reference_note'])
        self.assertIn('no money verdict', manifest['run8_out_of_scope'])
        self.assertIn('1790', manifest['run8_acceptance'])
        for strategy in manifest['strategies'].values():
            self.assertEqual(strategy['exit_rule'], manifest['exit_rule'])
            self.assertEqual(strategy['exit_mode'], 'fee_cover')

    def test_ladder_run_still_declares_the_round6_ladder(self) -> None:
        manifest = self._manifest(RUN_DURATION_SEC='28800', ENTRY_ENRICH='1')
        self.assertEqual(manifest['exit_mode'], 'ladder')
        self.assertIn('4-mode ladder', manifest['exit_rule'])
        self.assertEqual(manifest['ab_test']['reference_arm'], 'portfolio_a')


class SummaryBlocksTests(unittest.TestCase):
    def setUp(self) -> None:
        self.simulator = _simulator_with_trades()
        self.summary = self.simulator.summary()

    def test_every_required_block_exists_per_book(self) -> None:
        for name in ('portfolio_s1', 'portfolio_s0'):
            book = self.summary[name]
            for key in ('latency_chain', 'hold_replay', 'round_trip_cost',
                        'gate_price', 'exit_shares'):
                self.assertIn(key, book, (name, key))

    def test_latency_chain_reports_each_stage(self) -> None:
        chain = self.summary['portfolio_s1']['latency_chain']
        self.assertEqual(chain['n'], 2)
        self.assertEqual(chain['stages']['t_filters_ms']['median'], 220.0)
        self.assertEqual(chain['stages']['t_filters_ms']['max'], 260.0)
        # Both entries are under 200 ms on one stage and over on the other.
        self.assertEqual(chain['share_under_200ms'], 0.5)
        self.assertEqual(chain['share_under_500ms'], 1.0)

    def test_hold_replay_reads_the_tick_stream(self) -> None:
        replay = self.summary['portfolio_s1']['hold_replay']
        # "at 1 s" = the last mark not later than 1 s: both coins sit at 0.
        self.assertEqual(replay['snapshots']['pnl_pct_at_1s']['mean_pct'], 0.0)
        # "hold to 3 s" = the first mark at or after 3 s (what a poll would see):
        # -a +3.5 at 3.1 s, -b its next mark -1.2 at 5.1 s.
        self.assertEqual(replay['replay']['hold_to_3s']['mean_pct'], 1.15)
        # -b hit -12 % before 5 s, so "5 s + stop" exits there.
        self.assertEqual(replay['replay']['hold_to_5s_stop_10']['mean_pct'], -4.8)
        self.assertEqual(replay['replay']['hold_to_5s']['mean_pct'], 0.6)

    def test_round_trip_cost_uses_flat_trades_only(self) -> None:
        rt = self.summary['portfolio_s0']['round_trip_cost']
        # -b has mfe 0.2 and mae -0.1 => flat (0.3 pp); -a is not (2.8 pp).
        self.assertEqual(rt['n_flat'], 1)
        self.assertEqual(rt['round_trip_cost_pct_median'], 1.2)
        self.assertEqual(rt['entry_fill_premium_pct_median'], 1.45)

    def test_gate_price_is_the_momentum_premium(self) -> None:
        s1 = self.summary['portfolio_s1']['gate_price']
        s0 = self.summary['portfolio_s0']['gate_price']
        self.assertEqual(s1['delta_min_sol'], 0.10)
        self.assertEqual(s0['delta_min_sol'], 0.00)
        self.assertEqual(s1['entry_premium_pct_median'], 3.95)
        self.assertEqual(s0['entry_premium_pct_median'], 0.15)
        self.assertEqual(s1['share_premium_above_1pct'], 1.0)

    def test_exit_shares_and_fee_cover_flag(self) -> None:
        shares = self.summary['portfolio_s1']['exit_shares']
        self.assertTrue(shares['fee_cover_fired'])
        self.assertEqual(shares['fee_cover_n'], 1)
        self.assertEqual(shares['time_exit_n'], 1)
        self.assertEqual(shares['hard_stop_n'], 0)
        self.assertEqual(shares['shares']['fee_cover']['share'], 0.5)
        self.assertEqual(shares['ladder_kinds_present'], [])

    def test_run_line_has_the_acceptance_numbers(self) -> None:
        line = self.summary['run_line']
        self.assertTrue(line['paper_only'])
        self.assertEqual(line['signed_transactions'], 0)
        self.assertIn('early_stop_reason', line)
        self.assertIn('requests_per_sec', line)
        self.assertIn('requests_per_sec_max_10min', line)
        self.assertIn('http_429_count', line)


class Run8MarkdownTests(unittest.TestCase):
    def _manifest(self, summary) -> dict:
        return {
            'run_id': 'run-8-test', 'build': 'deadbee', 'paper_only': True,
            'signed_transactions': 0, 'actual_duration_sec': 1795.0,
            'planned_duration_sec': 1800, 'early_stop_reason': None,
            'exit_mode': 'fee_cover',
            'ab_test': {'mode': 'single_arm_smoke', 'reference_arm': None},
            'telemetry': {'dropped_events': 0, 'write_errors': 0},
            'rpc_metrics': {
                'requests_per_sec': 1.42, 'requests_per_sec_max_10min': 2.0,
                'unrecovered_rate': 0.0, 'unrecovered_failures': 0,
                'requests': 2_300, 'http_429_count': 3, 'retry_count': 4,
                'recovered_failures': 4,
            },
            'profiles': summary['profiles'],
        }

    def test_report_carries_the_seven_required_blocks(self) -> None:
        simulator = _simulator_with_trades()
        summary = simulator.summary()
        funnel = {
            'available': True, 'chain': 'candidates 120 -> security 90',
            'entry_state': 40, 'opened': {'portfolio_s1': 12, 'portfolio_s0': 30},
            'entry_skips': {'portfolio_s1': {'slots_full': 3},
                            'portfolio_s0': {'four_account_entry_snapshot_unavailable': 2}},
            'portfolio_s1_signal': {'live_momentum_entry': 12, 'no_live_momentum': 8,
                                    'momentum_too_late': 4, 'birth_out_of_band': 6},
            'portfolio_s0_signal': {'live_momentum_entry': 30, 'momentum_too_late': 4},
            'delta_buckets': {'0.1-0.5': 9},
        }
        text = _run8_summary_markdown(self._manifest(summary), funnel, summary)

        # 1. per-book table and the three exit kinds
        self.assertIn('## Результат по книгам', text)
        self.assertIn('| S1 | 2 |', text)
        self.assertIn('fee_cover', text)
        self.assertIn('time_exit', text)
        self.assertIn('hard_stop', text)
        # 2. the five latency stages with median/p90/max and the 200 ms share
        self.assertIn('## Латентность входа', text)
        for stage in ('t_detect_ms', 't_filters_ms', 't_read_ms', 't_fill_ms',
                      't_total_ms'):
            self.assertIn(stage, text)
        self.assertIn('доля входов быстрее 200 мс', text)
        # 3. cost of the round trip against the measured references
        self.assertIn('Стоимость круглого рейса', text)
        self.assertIn('3.25', text)
        self.assertIn('1.41', text)
        # 4. hold replay
        self.assertIn('hold 3 с', text)
        self.assertIn('5 с + стоп −10 %', text)
        # 5. the gate price
        self.assertIn('цена гейта', text)
        # 6. funnel with the named rejection reasons
        self.assertIn('## Воронка', text)
        self.assertIn('no_live_momentum', text)
        self.assertIn('momentum_too_late', text)
        self.assertIn('слоты заняты', text)
        self.assertIn('нет цены/снимка', text)
        # 7. the acceptance table
        self.assertIn('## Приёмка прогона #8', text)
        for check in ('early_stop_reason = null', 'dropped_events = 0',
                      'write_errors = 0', 'fee_cover сработал', 'медиана t_total_ms'):
            self.assertIn(check, text)

    def test_report_refuses_the_money_verdict(self) -> None:
        simulator = _simulator_with_trades()
        summary = simulator.summary()
        text = _run8_summary_markdown(self._manifest(summary), {}, summary)
        self.assertIn('сравнения с A в этом прогоне нет', text)
        self.assertIn('Нет вердикта по деньгам', text)
        self.assertNotIn('стало прибыльно', text)
        self.assertNotIn('парная разница с A', text)
        self.assertNotIn('## Критерии решения', text)

    def test_acceptance_rows_turn_red_when_a_rule_is_broken(self) -> None:
        simulator = _simulator_with_trades()
        summary = simulator.summary()
        # No fee_cover trade in S0 -> that acceptance row must fail.
        summary['portfolio_s0']['exit_shares']['fee_cover_fired'] = False
        manifest = self._manifest(summary)
        manifest['early_stop_reason'] = 'telemetry_loss (dropped_events=1, write_errors=0)'
        text = _run8_summary_markdown(manifest, {}, summary)
        row = [ln for ln in text.splitlines() if 'fee_cover сработал' in ln][0]
        self.assertIn('❌', row)
        row = [ln for ln in text.splitlines() if 'early_stop_reason = null' in ln][0]
        self.assertIn('❌', row)

    def test_dispatch_only_takes_the_run8_path(self) -> None:
        self.assertTrue(_is_run8({'ab_test': {'mode': 'single_arm_smoke'}}))
        self.assertTrue(_is_run8({'exit_mode': 'fee_cover'}))
        self.assertFalse(_is_run8({'ab_test': {'mode': 'ab'}, 'exit_mode': 'ladder'}))


if __name__ == '__main__':
    unittest.main()
