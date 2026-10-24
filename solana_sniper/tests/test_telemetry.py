from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
import uuid
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from models import NewCoin, SwapQuote
from telemetry import (
    CandidateCollector,
    _observation_target_monotonic,
    _security_fail_report,
    _utc_from_epoch as telemetry_utc_from_epoch,
    observation_peaks,
)


class CandidateTelemetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_candidate_decision_quote_fill_and_exports(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(),
            )
            await collector.start()
            coin = NewCoin(
                mint='MintTelemetryTest', source='Pump.fun', signature='sig-test',
                curve='curve-test', creator='creator-test',
                program_creator='32CfGsZjrkHJukZuh97YN7t2eh8EmcuLkGuEzEYPBeRm',
                tx_payer='2MRQDEBy1XfwvgwL8GbxXsD5wPq4gncn1YpsUKkosUFk',
                packet={
                    'symbol': 'TEST', 'api_key': 'must-not-export',
                    'dev_buy_sol': 3.0, 'initial_buy_raw': 97545455,
                    'private_key': 'must-not-export-private',
                    'rpc_endpoint': 'https://rpc.invalid/?api-key=must-not-export-query',
                    'debug': '{"token":"must-not-export-inline"}',
                },
            )
            collector.record_candidate(coin, {'paper_only': True})
            collector.record_creator_attribution(coin, observed_at_epoch=1_700_000_000.0)
            collector.record_creator_probe(
                mint=coin.mint, source=coin.source,
                program_creator=coin.program_creator, tx_payer=coin.tx_payer,
                status='ok', values={
                    'observed_at_utc': '2023-11-14T22:13:20Z',
                    'latency_ms': 15.0,
                    'creator_signature_count_0_25': 3,
                    'creator_signature_oldest_block_time': 1_699_999_000,
                    'creator_token_ata_exists': True,
                    'creator_token_balance_raw': '42',
                    'paper_only_read_only_rpc': True,
                }, signature_count=3, oldest_block_time=1_699_999_000,
                ata_exists=True, token_balance_raw='42', token_balance_ui=0.000042,
                latency_ms=15.0, observed_at_utc='2023-11-14T22:13:20Z',
            )
            collector.record_watchlist_event(
                coin.mint, coin.source, 'candidate_registered', 'watching',
                {'poll_interval_sec': 1.0, 'independent_of_main_slots': True},
            )
            collector.record_source_event(
                'Pump.fun', 'pumpportal_create', coin.mint, 'sig-test',
                {'mint': coin.mint, 'api_key': 'must-not-export'},
            )
            collector.record_decision(
                coin.mint, coin.source, 'filters', 'fail', ['liquidity_below'],
                {'liquidity_usd': 1000, 'threshold_usd': 3000},
            )
            quote = SwapQuote(
                input_mint='So111', output_mint=coin.mint,
                in_amount_raw=123456, out_amount_raw=987654,
                min_out_raw=980000, price_impact_pct=1.2,
                route_labels=('Pump.fun',), context_slot=42,
            )
            quote_id = collector.record_quote(
                coin.mint, coin.source, 'shadow_buy', 'buy', 'So111',
                coin.mint, 123456, quote, 'ok', latency_ms=12.5,
            )
            collector.record_fill(
                coin.mint, coin.source, quote_id, 'SL trigger', 1.0,
                0.000001, 120.0, -130.0, 0.5, 980000, 1.2,
                ['Pump.fun'], portfolio='portfolio_a', out_amount_raw=987654,
                snapshot_pnl_rub=-130.0, conservative_100bps_pnl_rub=-135.0,
                conservative_out_amount_raw=976543,
                conservative_min_out_raw=966777,
                conservative_sold_fraction=1.0,
            )
            await collector.close()

            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM candidates').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT count(*) FROM detections').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT count(*) FROM decisions').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT count(*) FROM creator_attributions').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT count(*) FROM creator_probes').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT program_creator,tx_payer FROM candidates').fetchone(), (
                    '32CfGsZjrkHJukZuh97YN7t2eh8EmcuLkGuEzEYPBeRm',
                    '2MRQDEBy1XfwvgwL8GbxXsD5wPq4gncn1YpsUKkosUFk',
                ))
                self.assertEqual(db.execute(
                    'SELECT dev_buy_sol,initial_buy_raw FROM candidates'
                ).fetchone(), (3.0, 97545455))
                self.assertEqual(db.execute('SELECT count(*) FROM watchlist_events').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT count(*) FROM quotes WHERE quote_only=1').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT count(*) FROM paper_fills WHERE paper_only=1').fetchone()[0], 1)
                fill_row = db.execute(
                    'SELECT portfolio,out_amount_raw,min_out_raw,snapshot_pnl_rub,'
                    'conservative_100bps_pnl_rub,conservative_out_amount_raw,'
                    'conservative_min_out_raw FROM paper_fills'
                ).fetchone()
                self.assertEqual(fill_row, (
                    'portfolio_a', '987654', '980000', -130.0, -135.0,
                    '976543', '966777',
                ))
                raw = db.execute('SELECT raw_json FROM candidates').fetchone()[0]
                for secret in (
                    'must-not-export', 'must-not-export-private',
                    'must-not-export-query', 'must-not-export-inline',
                ):
                    self.assertNotIn(secret, raw)

            expected = {
                'candidates.csv', 'detections.csv', 'decisions.csv',
                'creator_attributions.csv', 'creator_probes.csv', 'watchlist_events.csv',
                'quotes.csv', 'observations.csv', 'paper_fills.csv', 'collector_meta.csv',
                'raw_events.jsonl', 'README.md',
            }
            self.assertTrue(expected.issubset({p.name for p in (root / 'exports').iterdir()}))
            raw_jsonl = (root / 'exports' / 'raw_events.jsonl').read_text()
            self.assertNotIn('must-not-export', raw_jsonl)

    async def test_probe_error_detail_list_is_written_through_the_database(self) -> None:
        """Regression: a list[dict] in a TEXT column silently dropped the row.

        The old test called probe.probe() with no collector, so it passed while
        the live writer raised ProgrammingError and lost the whole probe row.
        This one goes through start() -> record -> close() -> SELECT.
        """
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(),
            )
            await collector.start()
            collector.record_creator_probe(
                mint='MintDetail', source='Pump.fun',
                program_creator='ProgramCreator', tx_payer=None,
                status='partial',
                values={
                    'status': 'partial',
                    'observed_at_utc': '2023-11-14T22:13:20Z',
                    'error_detail': [
                        {'method': 'getSignaturesForAddress', 'label': 'rpc_http_429',
                         'http_status': 429, 'message': 'rate limited'},
                    ],
                },
                signature_count=None, oldest_block_time=None, ata_exists=None,
                token_balance_raw=None, token_balance_ui=None,
                latency_ms=83.0, observed_at_utc='2023-11-14T22:13:20Z',
                error_type='RuntimeError',
                # The caller passes the raw list; the column is TEXT.
                error_detail=[{'method': 'getSignaturesForAddress',
                               'label': 'rpc_http_429', 'http_status': 429,
                               'message': 'rate limited'}],
            )
            await collector.close()
            self.assertEqual(collector.stats()['write_errors'], 0)
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                rows = db.execute(
                    'SELECT status,error_detail,values_json FROM creator_probes'
                ).fetchall()
                meta = dict(db.execute(
                    "SELECT key,value FROM collector_meta WHERE key IN "
                    "('writer_errors','dropped_events')"
                ).fetchall())
        self.assertEqual(len(rows), 1, 'the probe row must survive')
        status, detail, values_json = rows[0]
        self.assertEqual(status, 'partial')
        self.assertIsInstance(detail, str)
        self.assertIn('429', detail)
        self.assertIn('getSignaturesForAddress', detail)
        # Nothing is lost: the raw payload stays in values_json.
        self.assertIn('rpc_http_429', values_json)
        self.assertEqual(meta.get('writer_errors'), '0')
        self.assertIn('dropped_events', meta)

    async def test_writer_errors_are_durable_after_queue_drain(self) -> None:
        """close() must persist counters *after* join(), not before it."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(),
            )
            await collector.start()
            # A value sqlite3 cannot bind (the exact shape of bug 2): the
            # writer fails that row, counts it, and the queue lives on.
            collector._put('decision', (
                uuid.uuid4().hex, 'cid', 'MintBroken', 'Pump.fun', 'stage', 'pass',
                '[]', {'not': 'json'}, '2023-11-14T22:13:20Z',
            ))
            await collector.close()
            self.assertEqual(collector.stats()['write_errors'], 1)
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                meta = dict(db.execute(
                    'SELECT key,value FROM collector_meta '
                    "WHERE key IN ('writer_errors','writer_error_type')"
                ).fetchall())
        # Before the fix this artifact said 0 while the log said 1.
        self.assertEqual(meta.get('writer_errors'), '1')
        self.assertEqual(meta.get('writer_error_type'), 'ProgrammingError')

    async def test_probe_error_detail_accepts_a_string_and_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(),
            )
            await collector.start()
            for mint, detail in (('MintStr', 'rpc_http_500 body'), ('MintNone', None)):
                collector.record_creator_probe(
                    mint=mint, source='Pump.fun', program_creator=None, tx_payer=None,
                    status='unavailable', values={'status': 'unavailable'},
                    signature_count=None, oldest_block_time=None, ata_exists=None,
                    token_balance_raw=None, token_balance_ui=None, latency_ms=None,
                    observed_at_utc='2023-11-14T22:13:20Z', error_detail=detail,
                )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                got = dict(db.execute(
                    'SELECT mint,error_detail FROM creator_probes'
                ).fetchall())
        self.assertEqual(got['MintStr'], 'rpc_http_500 body')
        # P0-3 (run #3): a failed probe can no longer be written without a
        # detail line — run #2 left 21 of 75 failures NULL, i.e. unexplained.
        self.assertIsNotNone(got['MintNone'])
        self.assertIn('status=unavailable', got['MintNone'])
        self.assertEqual(collector.stats()['write_errors'], 0)

    async def test_failed_probe_without_detail_derives_one_from_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(),
            )
            await collector.start()
            collector.record_creator_probe(
                mint='MintErrors', source='Pump.fun', program_creator=None, tx_payer=None,
                status='error',
                values={'status': 'error', 'errors': [
                    {'label': 'getSignaturesForAddress', 'method': 'getSignaturesForAddress',
                     'http_status': 429, 'message': 'Too Many Requests'},
                ]},
                signature_count=None, oldest_block_time=None, ata_exists=None,
                token_balance_raw=None, token_balance_ui=None, latency_ms=83.0,
                observed_at_utc='2023-11-14T22:13:20Z',
                error_type='RuntimeError',
            )
            # A working probe keeps its detail empty: coverage only applies to
            # failures.
            collector.record_creator_probe(
                mint='MintOk', source='Pump.fun', program_creator=None, tx_payer=None,
                status='ok', values={'status': 'ok'}, signature_count=40,
                oldest_block_time=None, ata_exists=True, token_balance_raw='1',
                token_balance_ui=1.0, latency_ms=120.0,
                observed_at_utc='2023-11-14T22:13:20Z',
            )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                got = dict(db.execute(
                    'SELECT mint,error_detail FROM creator_probes'
                ).fetchall())
        self.assertIsNotNone(got['MintErrors'])
        self.assertIn('429', got['MintErrors'])
        self.assertIn('getSignaturesForAddress', got['MintErrors'])
        self.assertIsNone(got['MintOk'])

    async def test_observations_are_stamped_at_the_scheduled_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(0, 5),
            )
            await collector.start()
            now = time.time()
            collector.record_observation(
                'MintObs', 'Pump.fun', 5,
                telemetry_utc_from_epoch(now + 5), 'ok', 1e-6,
                'on_chain_pump_curve', 180.0, None,
                observed_at_utc=telemetry_utc_from_epoch(now + 5 + 0.12),
            )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                target, observed = db.execute(
                    'SELECT target_at_utc, observed_at_utc FROM observations'
                ).fetchone()
        delta = abs(
            datetime.fromisoformat(observed.replace('Z', '+00:00'))
            - datetime.fromisoformat(target.replace('Z', '+00:00'))
        ).total_seconds()
        self.assertLessEqual(delta, 0.25)

    def test_observation_deadlines_use_absolute_target_not_task_start(self) -> None:
        """P0-1: six offsets must not inherit the collector's startup delay."""
        found_at = 1_700_000_000.0
        # The collector task starts 0.4 s after the shared candidate timestamp
        # and this call happens 3.5 s later still (queue + semaphore wait).
        now_epoch = found_at + 3.9
        now_monotonic = 1_000.0
        started_monotonic = now_monotonic - 3.5
        for offset in (0, 5, 10, 15, 60, 120):
            target = _observation_target_monotonic(
                found_at, offset, now_epoch, now_monotonic,
            )
            remaining = found_at + offset - now_epoch
            # Absolute: now + remaining, i.e. exactly on the schedule.
            self.assertAlmostEqual(target - now_monotonic, max(0.0, remaining), places=9)
            # The old form (spawn time + remaining) fired 3.5 s EARLY for every
            # future offset — the drift that made run #2 rows unusable.
            old_form = started_monotonic + max(0.0, remaining)
            self.assertAlmostEqual(target - old_form, 3.5, places=9)

    async def test_legacy_candidates_table_gets_dev_buy_columns_added(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / 'telemetry.sqlite'
            # Pre-2026-10 schema: no dev-buy columns at all.
            with sqlite3.connect(db_path) as db:
                db.execute(
                    'CREATE TABLE candidates (candidate_id TEXT PRIMARY KEY, '
                    'chain_id TEXT NOT NULL, mint TEXT NOT NULL, source TEXT NOT NULL, '
                    'first_seen_at_utc TEXT NOT NULL, signature TEXT, curve TEXT, '
                    'creator TEXT, name TEXT, symbol TEXT, decimals INTEGER, '
                    'raw_json TEXT NOT NULL, config_json TEXT NOT NULL)'
                )
                db.execute(
                    "INSERT INTO candidates VALUES ('legacy','solana-mainnet','OldMint',"
                    "'Pump.fun','2026-10-01T00:00:00Z','sig',NULL,NULL,'OLD','OLD',"
                    "6,'{}','{}')"
                )
            collector = CandidateCollector(str(db_path), str(root / 'exports'))
            await collector.start()
            collector.record_candidate(
                NewCoin(
                    mint='MintNewSchema', source='Pump.fun', signature='sig-2',
                    packet={'dev_buy_sol': 2.5, 'initial_buy_raw': 250_000_000},
                ),
                {'paper_only': True},
            )
            await collector.close()
            with sqlite3.connect(db_path) as db:
                columns = {row[1] for row in db.execute('PRAGMA table_info(candidates)')}
                self.assertIn('dev_buy_sol', columns)
                self.assertIn('initial_buy_raw', columns)
                self.assertEqual(db.execute(
                    'SELECT dev_buy_sol,initial_buy_raw FROM candidates '
                    "WHERE mint='MintNewSchema'"
                ).fetchone(), (2.5, 250_000_000))
                self.assertEqual(db.execute(
                    'SELECT dev_buy_sol,initial_buy_raw FROM candidates '
                    "WHERE mint='OldMint'"
                ).fetchone(), (None, None))

    async def test_shadow_quotes_and_followups_are_scheduled_without_trades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(0,),
            )
            await collector.start()
            coin = NewCoin(mint='MintObserved', source='Pump.fun', signature='sig')
            quote_buy = SwapQuote(
                input_mint='So111', output_mint=coin.mint,
                in_amount_raw=100, out_amount_raw=1000, min_out_raw=990,
                price_impact_pct=0.5, route_labels=('Pump.fun',), context_slot=5,
            )
            quote_sell = SwapQuote(
                input_mint=coin.mint, output_mint='So111',
                in_amount_raw=990, out_amount_raw=90, min_out_raw=89,
                price_impact_pct=0.7, route_labels=('Pump.fun',), context_slot=6,
            )
            settings = SimpleNamespace(
                max_trade_rub=250, max_positions=1, min_liquidity_usd=3000,
                max_entry_price_impact_pct=2, take_profit_pct=35,
                stop_loss_pct=10, stagnation_sec=60,
                stagnation_range_pct=2, max_hold_sec=120,
                usd_rub_rate=90, http_rpc_url='https://rpc.example.invalid',
            )
            with patch('telemetry.get_sol_usd', new=AsyncMock(return_value=100.0)), \
                 patch('telemetry.get_jupiter_quote', new=AsyncMock(side_effect=[quote_buy, quote_sell])), \
                 patch('telemetry.pump_curve_price_usd', new=AsyncMock(return_value=0.000001)), \
                 patch('telemetry.resolve_real_price', new=AsyncMock(return_value=(0.000001, 'fixture'))):
                collector.observe_candidate(coin, object(), settings)
                await asyncio.gather(*list(collector._tasks))
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM candidates').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT count(*) FROM quotes').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT count(*) FROM observations WHERE status="ok"').fetchone()[0], 1)

    async def test_disabled_shadow_quotes_and_helius_key_are_not_exported(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_key = 'test-only-secret-value-never-export'
            collector = CandidateCollector(
                str(root / 'telemetry.sqlite'), str(root / 'exports'),
                observation_offsets_sec=(), enable_shadow_quotes=False,
            )
            with patch.dict(os.environ, {'HELIUS_API_KEY': fake_key}, clear=True):
                await collector.start()
                settings = SimpleNamespace(
                    max_trade_rub=500, max_positions=3, min_liquidity_usd=3000,
                    max_entry_price_impact_pct=2, take_profit_pct=30,
                    stop_loss_pct=10, stagnation_sec=60,
                    stagnation_range_pct=2, max_hold_sec=90,
                    usd_rub_rate=90, http_rpc_url='https://mainnet.helius-rpc.com/?api-key=hidden',
                )
                collector.observe_candidate(
                    NewCoin(mint='MintNoShadow', source='Pump.fun', signature='sig'),
                    object(), settings,
                )
                await asyncio.gather(*list(collector._tasks))
                await collector.close()
            manifest = (root / 'exports' / 'manifest.json').read_text()
            self.assertIn('mainnet.helius-rpc.com', manifest)
            self.assertIn('"shadow_quotes_enabled": false', manifest)
            self.assertNotIn(fake_key, manifest)
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM quotes').fetchone()[0], 0)

    async def test_queue_overflow_is_counted_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            collector = CandidateCollector(
                str(Path(temp) / 'telemetry.sqlite'), str(Path(temp) / 'exports'),
                queue_limit=1,
            )
            collector.record_decision('Mint', 'Pump.fun', 'stage', 'pass')
            collector.record_decision('Mint', 'Pump.fun', 'stage', 'fail')
            self.assertEqual(collector._dropped, 1)


if __name__ == '__main__':
    unittest.main()

class SecurityFailObservationTests(unittest.IsolatedAsyncioTestCase):
    """P0-2: a coin the security filters reject is not worth 6 RPC reads."""

    def _collector(self, root: Path, offsets=(0, 5, 10, 60, 120)):
        return CandidateCollector(
            str(root / 'telemetry.sqlite'), str(root / 'exports'),
            observation_offsets_sec=offsets,
        )

    async def test_security_fail_keeps_only_offsets_zero_and_fifteen(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = self._collector(root)
            await collector.start()
            self.assertIsNone(collector._deep_offsets_for('MintUnknown'))
            collector.set_security_verdict('MintGood', True)
            self.assertIsNone(collector._deep_offsets_for('MintGood'))
            collector.set_security_verdict('MintBad', False)
            self.assertEqual(collector._deep_offsets_for('MintBad'), (0, 15))
            await collector.close()

    async def test_followups_write_skipped_rows_instead_of_silence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = self._collector(root)
            await collector.start()
            coin = NewCoin(
                mint='MintSecurityFail', source='Pump.fun', signature='sig-bad',
                curve='curve-bad', packet={'v_sol': 32.0},
            )
            collector.set_security_verdict(coin.mint, False)
            settings = SimpleNamespace(http_rpc_url='https://rpc.invalid')
            with patch('telemetry.pump_curve_price_usd',
                       AsyncMock(return_value=1e-7)):
                await collector._collect_followups(
                    coin, object(), settings, time.monotonic(), time.time(), set(),
                )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                rows = dict(db.execute(
                    'SELECT target_offset_sec, status FROM observations '
                    'WHERE mint=?', (coin.mint,),
                ).fetchall())
        # Offsets 0 and 15 were really read (15 is not in this schedule, so it
        # is simply absent); everything deep is marked, never dropped.
        self.assertEqual(rows.get(0), 'ok')
        self.assertEqual(rows.get(5), 'skipped_security_deep')
        self.assertEqual(rows.get(10), 'skipped_security_deep')
        self.assertEqual(rows.get(60), 'skipped_security_deep')
        self.assertEqual(rows.get(120), 'skipped_security_deep')
        self.assertEqual(collector.security_fail_deep_skipped, 4)
        self.assertEqual(collector.stats()['write_errors'], 0)

    async def test_verdict_resolved_between_offsets_trims_the_rest(self) -> None:
        """The run #6 race, reproduced: the verdict lands after offset 0.

        Run #6 resolved `_deep_offsets_for` once BEFORE the offset loop, so the
        loop saw `None` forever and read all six offsets (`skipped_deep = 0`
        over 1 634 rejects). Here the security filter only fails while the
        collector is already observing, and the remaining offsets must be
        trimmed — the test fails on the old single-read code.
        """
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = self._collector(root)
            await collector.start()
            coin = NewCoin(
                mint='MintLateVerdict', source='Pump.fun', signature='sig-late',
                curve='curve-late', packet={'v_sol': 32.0},
            )
            settings = SimpleNamespace(http_rpc_url='https://rpc.invalid')
            calls = {'n': 0}

            async def price_side_effect(*args, **kwargs):
                calls['n'] += 1
                if calls['n'] == 1:
                    # The verdict arrives while offset 0 is being read.
                    collector.set_security_verdict(coin.mint, False)
                return 1e-7

            real_lookup = collector._deep_offsets_for
            lookups = {'n': 0}

            def counting_lookup(mint: str):
                lookups['n'] += 1
                return real_lookup(mint)

            collector._deep_offsets_for = counting_lookup  # type: ignore[assignment]
            with patch('telemetry.pump_curve_price_usd',
                       AsyncMock(side_effect=price_side_effect)):
                await collector._collect_followups(
                    coin, object(), settings, time.monotonic(), time.time(), set(),
                )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                rows = dict(db.execute(
                    'SELECT target_offset_sec, status FROM observations '
                    'WHERE mint=?', (coin.mint,),
                ).fetchall())
        self.assertEqual(rows.get(0), 'ok')
        for offset in (5, 10, 60, 120):
            self.assertEqual(rows.get(offset), 'skipped_security_deep', offset)
        self.assertEqual(collector.security_fail_deep_skipped, 4)
        # One lookup per offset, not one for the whole schedule.
        self.assertEqual(lookups['n'], len(collector.observation_offsets_sec))

    async def test_offset_60_is_read_for_a_hash_selected_third_only(self) -> None:
        """§8.1: the 60 s read must not depend on anything but the mint hash."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = self._collector(root, offsets=(0, 5, 15, 60))
            await collector.start()
            mints = [f'Mint{i:04d}' for i in range(300)]
            sampled = [mint for mint in mints if collector.window_sampled(mint)]
            # Deterministic: the same mint always lands in the same bucket.
            self.assertEqual(
                sampled, [mint for mint in mints if collector.window_sampled(mint)],
            )
            share = len(sampled) / len(mints)
            self.assertGreater(share, 0.25)
            self.assertLess(share, 0.42)
            # A coin outside the sample gets a skipped row for 60 and no RPC.
            chosen = next(mint for mint in mints if collector.window_sampled(mint))
            skipped = next(mint for mint in mints if not collector.window_sampled(mint))
            settings = SimpleNamespace(http_rpc_url='https://rpc.invalid')
            calls = {'n': 0}

            async def price(*args, **kwargs):
                calls['n'] += 1
                return 1e-7

            for mint in (chosen, skipped):
                coin = NewCoin(
                    mint=mint, source='Pump.fun', signature=f'sig-{mint}',
                    curve='curve', packet={'v_sol': 32.0},
                )
                with patch('telemetry.pump_curve_price_usd', AsyncMock(side_effect=price)):
                    # The run is already past every offset: no sleeping in tests.
                    await collector._collect_followups(
                        coin, object(), settings, time.monotonic() - 200.0,
                        time.time() - 200.0, set(),
                    )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                rows = dict(db.execute(
                    'SELECT mint || ":" || target_offset_sec, status FROM observations'
                ).fetchall())
        self.assertEqual(rows[f'{chosen}:60'], 'ok')
        self.assertEqual(rows[f'{skipped}:60'], 'skipped_window_sample')
        # Four reads for the sampled coin, three for the other one.
        self.assertEqual(calls['n'], 7)
        self.assertEqual(collector.window_sample_skipped, 1)

    def test_observation_peaks_measure_against_offset_zero_in_the_window(self) -> None:
        """Rocket peaks come from observations, inside a fixed 60 s window."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db_path = root / 'telemetry.sqlite'
            with sqlite3.connect(db_path) as db:
                db.execute(
                    'CREATE TABLE observations (id INTEGER PRIMARY KEY, '
                    'mint TEXT, target_offset_sec INTEGER, price_usd REAL, '
                    'excluded_from_stats INTEGER DEFAULT 0)'
                )
                rows = [
                    # Offset 0 is the reference for the peak.
                    ('Run', 0, 1.0, 0), ('Run', 15, 1.2, 0), ('Run', 60, 1.8, 0),
                    # After the window: must not count (would be +300 %).
                    ('Run', 120, 4.0, 0),
                    # Flat coin.
                    ('Flat', 0, 2.0, 0), ('Flat', 60, 2.1, 0),
                    # Excluded observations are ignored, not counted as zero.
                    ('OnlyExcluded', 0, 1.0, 1), ('OnlyExcluded', 60, 9.0, 1),
                    # No offset 0 -> no reference -> no peak.
                    ('NoBase', 60, 5.0, 0),
                ]
                db.executemany(
                    'INSERT INTO observations (mint, target_offset_sec, price_usd, '
                    'excluded_from_stats) VALUES (?,?,?,?)', rows,
                )
                db.commit()
            peaks = observation_peaks(db_path, window_sec=60.0)
            # Missing offset 0 -> no peak at all (never a fabricated zero).
            self.assertEqual(sorted(peaks), ['Flat', 'Run'])
            self.assertAlmostEqual(peaks['Run'], 80.0, places=4)
            self.assertAlmostEqual(peaks['Flat'], 5.0, places=4)
            # A missing database is an answer, not an exception at the end of a run.
            self.assertEqual(observation_peaks(root / 'nope.sqlite'), {})

    def test_security_fail_report_counts_rockets_over_observed_coins(self) -> None:
        rejected = [
            ('M1', '["creator_history_too_short"]'),
            ('M2', '["creator_history_too_short"]'),
            ('M3', '["holder_concentration"]'),
            # Blind spot: rejected, never read at all.
            ('M4', '["holder_concentration"]'),
        ]
        observations = [
            ('M1', 0, 1.0), ('M1', 15, 1.9),   # +90 % -> rocket
            ('M2', 0, 1.0), ('M2', 15, 1.1),   # +10 % -> not a rocket
            ('M3', 0, 2.0),                    # offset 15 missing -> no verdict
        ]

        def rows_of(sql, params=()):
            if 'FROM decisions' in sql:
                return rejected
            self.assertIn("status='ok'", sql)
            return observations

        report = _security_fail_report(None, rows_of, lambda sql: 7)
        self.assertEqual(report['security_fail_total'], 4)
        self.assertEqual(report['security_fail_observed'], 3)
        self.assertEqual(report['security_fail_observed_share'], 0.75)
        # One rocket out of the 3 observed, never out of the 4 rejected.
        self.assertEqual(report['security_fail_rockets'], 1)
        self.assertEqual(report['security_fail_rocket_share'], round(1 / 3, 4))
        self.assertEqual(report['security_fail_reasons'],
                         {'creator_history_too_short': 2, 'holder_concentration': 1})
        self.assertEqual(report['security_fail_skipped_deep'], 7)

    def test_security_fail_report_is_honest_when_nothing_was_rejected(self) -> None:
        report = _security_fail_report(None, lambda sql, params=(): [],
                                       lambda sql: 0)
        self.assertEqual(report['security_fail_total'], 0)
        self.assertIsNone(report['security_fail_rocket_share'])
        self.assertIsNone(report['security_fail_observed_share'])

    async def test_cancelled_rows_carry_excluded_from_stats(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = self._collector(root, offsets=(0, 5))
            await collector.start()
            collector.record_observation(
                'MintCancelled', 'Pump.fun', 5,
                '2023-11-14T22:13:25Z', 'cancelled_at_session_end',
                error_type='session_stopped',
            )
            collector.record_observation(
                'MintCancelled', 'Pump.fun', 0,
                '2023-11-14T22:13:20Z', 'ok', price_usd=1e-7,
            )
            await collector.close()
            with sqlite3.connect(root / 'telemetry.sqlite') as db:
                rows = dict(db.execute(
                    'SELECT status, excluded_from_stats FROM observations '
                    "WHERE mint='MintCancelled'"
                ).fetchall())
        self.assertEqual(rows['cancelled_at_session_end'], 1)
        # A real read is never excluded.
        self.assertEqual(rows['ok'], 0)
