from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from models import NewCoin, SwapQuote
from telemetry import CandidateCollector


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
