from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from filters import check_liquidity, fetch_mint_meta
from models import NewCoin, Position, SwapQuote
from price import WSOL, get_jupiter_quote
from trader import (
    TraderState,
    _pos_to_dict,
    manage_position,
    quote_confirms_exit_trigger,
    sell_part,
    start_paper_trade,
)
from wallet import Wallet


class FakeResponse:
    # ПЛАН: выдать тестовый HTTP-ответ Jupiter через async with.
    def __init__(self, data: dict, status: int = 200) -> None:
        self.data = data
        self.status = status

    # ПЛАН: вернуть тестовый ответ как активный контекст async with.
    async def __aenter__(self) -> 'FakeResponse':
        return self

    # ПЛАН: штатно завершить тестовый контекст ответа.
    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    # ПЛАН: вернуть заранее собранный JSON quote.
    async def json(self) -> dict:
        return self.data


class FakeSession:
    # ПЛАН: сохранить URL и параметры запроса и вернуть заранее заданный ответ.
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.url = ''
        self.params = {}

    # ПЛАН: сохранить параметры запроса и отдать fake-response.
    def get(self, url: str, **kwargs) -> FakeResponse:
        self.url = url
        self.params = kwargs.get('params', {})
        return self.response


class FakePostSession:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = 0

    def post(self, url: str, **kwargs) -> FakeResponse:
        self.calls += 1
        return self.responses.pop(0)


class PaperExecutionTests(unittest.IsolatedAsyncioTestCase):
    # ПЛАН: проверять quote-парсинг, единицы токена и запрет фиктивной выручки.

    async def test_fresh_mint_metadata_retries_transient_public_rpc_miss(self) -> None:
        info = {'decimals': 6, 'supply': '1000', 'mintAuthority': None, 'freezeAuthority': None}
        responses = [
            FakeResponse({'result': {'value': None}}),
            FakeResponse({'result': {'value': {'data': {'parsed': {'info': info}}}}}),
        ]
        session = FakePostSession(responses)
        with patch('filters.asyncio.sleep', new=AsyncMock()) as sleep_mock:
            result = await fetch_mint_meta(session, 'https://rpc.invalid', 'MintFresh')
        self.assertEqual(result, info)
        self.assertEqual(session.calls, 2)
        sleep_mock.assert_awaited_once_with(0.5)

    # ПЛАН: проверить сохранение raw-суммы, маршрута и порога slippage.
    async def test_jupiter_quote_keeps_raw_size_and_slippage_floor(self) -> None:
        payload = {
            'inputMint': WSOL,
            'outputMint': 'Mint111',
            'inAmount': '50000000',
            'outAmount': '2000000',
            'otherAmountThreshold': '1980000',
            'priceImpactPct': '0.0125',
            'contextSlot': 123,
            'routePlan': [{'swapInfo': {'label': 'Pump.fun'}}],
        }
        session = FakeSession(FakeResponse(payload))

        quote = await get_jupiter_quote(
            session, WSOL, 'Mint111', 50_000_000, slippage_bps=100
        )

        self.assertIsNotNone(quote)
        self.assertEqual(quote.in_amount_raw, 50_000_000)
        self.assertEqual(quote.out_amount_raw, 2_000_000)
        self.assertEqual(quote.min_out_raw, 1_980_000)
        self.assertEqual(quote.route_labels, ('Pump.fun',))
        self.assertEqual(quote.price_impact_pct, 1.25)
        self.assertEqual(session.params['amount'], '50000000')
        self.assertEqual(session.params['slippageBps'], '100')

    # ПЛАН: удостовериться, что ценовые выходы подтверждаются исполнимой quote.
    def test_exit_thresholds_use_quote_price_not_indexer_spot(self) -> None:
        position = Position(
            mint='MintTrigger', source='Raydium CPMM', entry_price=100.0,
            simulated=False, opened_at=1.0, deposit_rub=500.0,
            ath_price=200.0,
        )
        settings = SimpleNamespace(
            stop_loss_pct=12.0,
            take_profit_pct=40.0,
            be_offset_pct=7.0,
            trail_pct=20.0,
        )

        self.assertTrue(quote_confirms_exit_trigger(
            position, 'SL (−12%)', 88.0, settings
        ))
        self.assertFalse(quote_confirms_exit_trigger(
            position, 'SL (−12%)', 95.0, settings
        ))
        self.assertTrue(quote_confirms_exit_trigger(
            position, 'TP1 (+40%)', 140.0, settings
        ))
        self.assertFalse(quote_confirms_exit_trigger(
            position, 'TP1 (+40%)', 139.0, settings
        ))
        self.assertTrue(quote_confirms_exit_trigger(
            position, 'Break-Even (+7%)', 107.0, settings
        ))
        self.assertFalse(quote_confirms_exit_trigger(
            position, 'Break-Even (+7%)', 108.0, settings
        ))
        self.assertTrue(quote_confirms_exit_trigger(
            position, 'Trailing Stop (−20%)', 160.0, settings
        ))
        self.assertFalse(quote_confirms_exit_trigger(
            position, 'Trailing Stop (−20%)', 161.0, settings
        ))
        self.assertTrue(quote_confirms_exit_trigger(
            position, 'Timeout', 1.0, settings
        ))
        self.assertFalse(quote_confirms_exit_trigger(
            position, 'SL', 0.0, settings
        ))

    # ПЛАН: проверить перевод quote в UI-токены через decimals mint.
    async def test_entry_uses_mint_decimals_and_exact_quote_amount(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            coin = NewCoin(
                mint='Mint111', source='Pump.fun', signature='sig', decimals=6
            )
            buy_quote = SwapQuote(
                input_mint=WSOL,
                output_mint=coin.mint,
                in_amount_raw=55_555_555,
                out_amount_raw=2_000_000_000_000,
                min_out_raw=1_980_000_000_000,
                price_impact_pct=2.0,
                route_labels=('Raydium',),
                context_slot=123,
            )
            sell_quote = SwapQuote(
                input_mint=coin.mint,
                output_mint=WSOL,
                in_amount_raw=1_980_000_000_000,
                out_amount_raw=54_000_000,
                min_out_raw=53_460_000,
                price_impact_pct=1.5,
                route_labels=('Raydium',),
                context_slot=124,
            )
            state = TraderState()
            settings = SimpleNamespace(
                usd_rub_rate=90.0,
                entry_fee_sol=0.0005,
                max_trade_rub=1500.0,
                take_profit_pct=35.0,
                be_offset_pct=6.0,
                trail_pct=18.0,
                stop_loss_pct=10.0,
                max_hold_sec=120.0,
                max_entry_price_impact_pct=5.0,
            )
            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(side_effect=[buy_quote, sell_quote]),
            ) as quote_mock:
                with patch('trader._spawn', side_effect=lambda coro: coro.close()):
                    started = await start_paper_trade(
                        None, settings, coin, None, state, wallet,
                        None, sol_usd=100.0,
                    )

            self.assertTrue(started)
            position = wallet.open_positions[0]
            self.assertEqual(position['tokens_raw'], 1_980_000_000_000)
            self.assertEqual(position['decimals'], 6)
            self.assertAlmostEqual(position['tokens'], 1_980_000.0)
            self.assertLess(position['tokens'], 3_000_000.0)
            self.assertAlmostEqual(wallet.balance_rub, 995.5, places=2)
            self.assertAlmostEqual(wallet.fees_paid_rub, 4.5, places=2)
            self.assertAlmostEqual(position['invested_rub'], 504.5, places=2)
            self.assertEqual(quote_mock.await_args_list[0].args[3], 55_555_555)
            self.assertEqual(
                quote_mock.await_args_list[1].args[3],
                1_980_000_000_000,
            )
            self.assertEqual(
                position['parsed_curve_data']['exit_precheck_quote']['tokens_raw'],
                1_980_000_000_000,
            )

    # ПЛАН: повторить свежую buy-quote и затем открыть позицию только по exact-size route.
    async def test_entry_retries_missing_buy_quote_before_paper_fill(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            coin = NewCoin(
                mint='MintRetryBuy', source='Pump.fun', signature='sig', decimals=6
            )
            buy_quote = SwapQuote(
                input_mint=WSOL, output_mint=coin.mint,
                in_amount_raw=55_555_555, out_amount_raw=2_000_000_000_000,
                min_out_raw=1_980_000_000_000, price_impact_pct=2.0,
                route_labels=('Raydium',), context_slot=123,
            )
            sell_quote = SwapQuote(
                input_mint=coin.mint, output_mint=WSOL,
                in_amount_raw=1_980_000_000_000, out_amount_raw=54_000_000,
                min_out_raw=53_460_000, price_impact_pct=1.5,
                route_labels=('Raydium',), context_slot=124,
            )
            settings = SimpleNamespace(
                usd_rub_rate=90.0, entry_fee_sol=0.0005,
                max_trade_rub=1500.0, take_profit_pct=40.0,
                be_offset_pct=7.0, trail_pct=20.0,
                stop_loss_pct=12.0, max_hold_sec=120.0,
                max_entry_price_impact_pct=5.0,
                entry_quote_retries=2,
                entry_quote_retry_delay_sec=0.0,
            )
            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(side_effect=[None, buy_quote, sell_quote]),
            ) as quote_mock:
                with patch('trader._spawn', side_effect=lambda coro: coro.close()):
                    started = await start_paper_trade(
                        None, settings, coin, None, TraderState(), wallet,
                        None, sol_usd=100.0,
                    )

            self.assertTrue(started)
            self.assertEqual(quote_mock.await_count, 3)
            self.assertEqual(quote_mock.await_args_list[0].args[3], 55_555_555)
            self.assertEqual(quote_mock.await_args_list[1].args[3], 55_555_555)
            self.assertEqual(
                quote_mock.await_args_list[2].args[3], 1_980_000_000_000
            )
            self.assertEqual(len(wallet.open_positions), 1)

    # ПЛАН: повторить sell-precheck того же raw-размера до списания денег.
    async def test_entry_retries_missing_exact_sell_precheck(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            coin = NewCoin(
                mint='MintRetrySell', source='Pump.fun', signature='sig', decimals=6
            )
            buy_quote = SwapQuote(
                input_mint=WSOL, output_mint=coin.mint,
                in_amount_raw=55_555_555, out_amount_raw=2_000_000_000_000,
                min_out_raw=1_980_000_000_000, price_impact_pct=2.0,
                route_labels=('Raydium',), context_slot=123,
            )
            sell_quote = SwapQuote(
                input_mint=coin.mint, output_mint=WSOL,
                in_amount_raw=1_980_000_000_000, out_amount_raw=54_000_000,
                min_out_raw=53_460_000, price_impact_pct=1.5,
                route_labels=('Raydium',), context_slot=124,
            )
            settings = SimpleNamespace(
                usd_rub_rate=90.0, entry_fee_sol=0.0005,
                max_trade_rub=1500.0, take_profit_pct=40.0,
                be_offset_pct=7.0, trail_pct=20.0,
                stop_loss_pct=12.0, max_hold_sec=120.0,
                max_entry_price_impact_pct=5.0,
                entry_quote_retries=1,
                entry_quote_retry_delay_sec=0.0,
            )
            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(side_effect=[buy_quote, None, sell_quote]),
            ) as quote_mock:
                with patch('trader._spawn', side_effect=lambda coro: coro.close()):
                    started = await start_paper_trade(
                        None, settings, coin, None, TraderState(), wallet,
                        None, sol_usd=100.0,
                    )

            self.assertTrue(started)
            self.assertEqual(quote_mock.await_count, 3)
            self.assertEqual(
                quote_mock.await_args_list[1].args[3],
                1_980_000_000_000,
            )
            self.assertEqual(
                quote_mock.await_args_list[2].args[3],
                1_980_000_000_000,
            )
            self.assertEqual(len(wallet.open_positions), 1)

    # ПЛАН: после исчерпания повторов не списывать деньги без подтверждённой buy-quote.
    async def test_entry_retry_exhaustion_keeps_wallet_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            coin = NewCoin(
                mint='MintRetryExhausted', source='Pump.fun', signature='sig',
                decimals=6,
            )
            settings = SimpleNamespace(
                usd_rub_rate=90.0, entry_fee_sol=0.0005,
                max_trade_rub=1500.0, take_profit_pct=40.0,
                be_offset_pct=7.0, trail_pct=20.0,
                stop_loss_pct=12.0, max_hold_sec=120.0,
                max_entry_price_impact_pct=5.0,
                entry_quote_retries=2,
                entry_quote_retry_delay_sec=0.0,
            )
            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(side_effect=[None, None, None]),
            ) as quote_mock:
                started = await start_paper_trade(
                    None, settings, coin, None, TraderState(), wallet,
                    None, sol_usd=100.0,
                )

            self.assertFalse(started)
            self.assertEqual(quote_mock.await_count, 3)
            self.assertEqual(wallet.balance_rub, 1500.0)
            self.assertEqual(wallet.open_positions, [])

    # ПЛАН: не открывать позицию, если exact-size проверка продажи не прошла.
    async def test_entry_requires_exact_size_exit_route_before_debit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            coin = NewCoin(
                mint='MintExitCheck', source='Raydium CPMM', signature='sig',
                decimals=6,
            )
            buy_quote = SwapQuote(
                input_mint=WSOL,
                output_mint=coin.mint,
                in_amount_raw=100_000_000,
                out_amount_raw=200_000_000,
                min_out_raw=198_000_000,
                price_impact_pct=1.0,
                route_labels=('Raydium',),
            )
            settings = SimpleNamespace(
                usd_rub_rate=90.0,
                entry_fee_sol=0.0005,
                max_trade_rub=1500.0,
                take_profit_pct=40.0,
                be_offset_pct=7.0,
                trail_pct=20.0,
                stop_loss_pct=12.0,
                max_hold_sec=120.0,
                max_entry_price_impact_pct=5.0,
            )
            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(side_effect=[buy_quote, None]),
            ) as quote_mock:
                started = await start_paper_trade(
                    None, settings, coin, None, TraderState(), wallet,
                    None, sol_usd=100.0,
                )

            self.assertFalse(started)
            self.assertEqual(wallet.balance_rub, 1500.0)
            self.assertEqual(wallet.open_positions, [])
            self.assertEqual(quote_mock.await_args_list[1].args[3], 198_000_000)

    # ПЛАН: блокировать вход при quote-impact выше заданного предела.
    async def test_entry_rejects_excessive_price_impact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            coin = NewCoin(
                mint='MintImpact', source='Raydium CPMM', signature='sig',
                decimals=6,
            )
            quote = SwapQuote(
                input_mint=WSOL,
                output_mint=coin.mint,
                in_amount_raw=100_000_000,
                out_amount_raw=200_000_000,
                min_out_raw=198_000_000,
                price_impact_pct=100.0,
                route_labels=('Unknown route',),
            )
            settings = SimpleNamespace(
                usd_rub_rate=90.0,
                entry_fee_sol=0.0005,
                max_trade_rub=1500.0,
                take_profit_pct=40.0,
                be_offset_pct=7.0,
                trail_pct=20.0,
                stop_loss_pct=12.0,
                max_hold_sec=120.0,
                max_entry_price_impact_pct=5.0,
            )
            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(return_value=quote),
            ) as quote_mock:
                started = await start_paper_trade(
                    None, settings, coin, None, TraderState(), wallet,
                    None, sol_usd=100.0,
                )

            self.assertFalse(started)
            self.assertEqual(wallet.balance_rub, 1500.0)
            self.assertEqual(wallet.open_positions, [])
            quote_mock.assert_awaited_once()

    # ПЛАН: не продавать по stale spot-сигналу, если exact-size quote не достигла SL.
    async def test_spot_stop_is_not_filled_when_exact_quote_stays_above_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            position = Position(
                mint='MintQuoteGuard', source='Meteora DLMM',
                entry_price=0.0001, simulated=False, opened_at=1.0,
                deposit_rub=500.0, tokens=55_555.0, decimals=6,
                tokens_raw=55_555_000_000, invested_rub=504.5,
                fees_rub=4.5,
            )
            wallet.open_position(504.5, _pos_to_dict(position))
            balance_before = wallet.balance_rub
            quote = SwapQuote(
                input_mint=position.mint,
                output_mint=WSOL,
                in_amount_raw=position.tokens_raw,
                out_amount_raw=53_311_364,
                min_out_raw=52_777_250,
                price_impact_pct=0.5,
                route_labels=('Pump.fun Amm',),
            )
            settings = SimpleNamespace(
                stop_loss_pct=12.0,
                take_profit_pct=40.0,
                be_offset_pct=7.0,
                trail_pct=20.0,
                exit_fee_sol=0.0001,
                usd_rub_rate=90.0,
            )

            with patch(
                'trader.get_jupiter_quote',
                new=AsyncMock(return_value=quote),
            ) as quote_mock:
                with patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)):
                    filled = await sell_part(
                        position, 1.0, 0.0000704, 'SL (стоп-лосс −12%)',
                        None, settings, wallet, None, TraderState(open_count=1), None,
                    )

            self.assertFalse(filled)
            self.assertEqual(quote_mock.await_args.args[3], position.tokens_raw)
            self.assertEqual(position.sold_frac, 0.0)
            self.assertFalse(position.exit_pending)
            self.assertEqual(wallet.balance_rub, balance_before)
            self.assertEqual(wallet.total_pnl, 0.0)
            self.assertFalse(wallet.open_positions[0]['exit_pending'])

    # ПЛАН: убедиться, что отсутствие quote не меняет баланс и PnL.
    async def test_missing_sell_route_records_no_fill_or_revenue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            position = Position(
                mint='Mint222', source='Pump.fun', entry_price=0.0001,
                simulated=False, opened_at=1.0, deposit_rub=500.0,
                tokens=1_000.0, decimals=6, tokens_raw=1_000_000_000,
                invested_rub=504.5,
            )
            wallet.open_position(504.5, _pos_to_dict(position))
            balance_before = wallet.balance_rub
            settings = SimpleNamespace(exit_fee_sol=0.0001, usd_rub_rate=90.0)
            state = TraderState(open_count=1)

            with patch('trader.get_jupiter_quote', new=AsyncMock(return_value=None)):
                with patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)):
                    filled = await sell_part(
                        position, 1.0, 0.0001, 'Timeout',
                        None, settings, wallet, None, state, None,
                    )

            self.assertFalse(filled)
            self.assertEqual(wallet.balance_rub, balance_before)
            self.assertEqual(wallet.total_pnl, 0.0)
            self.assertEqual(position.realized_rub, 0.0)
            self.assertEqual(position.sold_frac, 0.0)
            self.assertTrue(wallet.open_positions[0]['exit_pending'])

    # ПЛАН: проверить точный размер выхода и атомарный учёт quote-fill.
    async def test_sell_quote_is_for_exact_raw_position_and_updates_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            position = Position(
                mint='Mint333', source='Pump.fun', entry_price=0.000003,
                simulated=False, opened_at=1.0, deposit_rub=500.0,
                tokens=1_980_000.0, decimals=6,
                tokens_raw=1_980_000_000_000,
                invested_rub=504.5, fees_rub=4.5,
            )
            wallet.open_position(504.5, _pos_to_dict(position))
            quote = SwapQuote(
                input_mint=position.mint,
                output_mint=WSOL,
                in_amount_raw=position.tokens_raw,
                out_amount_raw=100_000_000,
                min_out_raw=99_000_000,
                price_impact_pct=1.5,
                route_labels=('Pump.fun',),
                context_slot=456,
            )
            settings = SimpleNamespace(exit_fee_sol=0.0001, usd_rub_rate=90.0)
            state = TraderState(open_count=1)

            with patch('trader.get_jupiter_quote', new=AsyncMock(return_value=quote)) as quote_mock:
                with patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)):
                    with patch('trader._finish_position', new=AsyncMock()):
                        filled = await sell_part(
                            position, 1.0, 0.000003, 'Timeout',
                            None, settings, wallet, None, state, None,
                        )

            self.assertTrue(filled)
            self.assertEqual(quote_mock.await_args.args[3], 1_980_000_000_000)
            self.assertEqual(position.sold_tokens_raw, position.tokens_raw)
            self.assertAlmostEqual(position.sold_frac, 1.0)
            self.assertAlmostEqual(wallet.balance_rub, 1885.6, places=2)
            self.assertAlmostEqual(wallet.total_pnl, 385.6, places=2)
            self.assertAlmostEqual(wallet.fees_paid_rub, 5.4, places=2)
            self.assertEqual(wallet.open_positions, [])

    # ПЛАН: подтвердить, что истёкший MAX_HOLD сразу пытается продать по quote.
    async def test_max_hold_exit_uses_quote_without_waiting_for_spot_price(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            wallet = Wallet(str(Path(temp_dir) / 'wallet.json'), initial=1500)
            position = Position(
                mint='Mint444', source='Pump.fun', entry_price=0.000003,
                simulated=False, opened_at=1.0, deposit_rub=500.0,
                tokens=1_000_000.0, decimals=6,
                tokens_raw=1_000_000_000_000, invested_rub=504.5,
            )
            wallet.open_position(504.5, _pos_to_dict(position))
            quote = SwapQuote(
                input_mint=position.mint,
                output_mint=WSOL,
                in_amount_raw=position.tokens_raw,
                out_amount_raw=50_000_000,
                min_out_raw=49_500_000,
                price_impact_pct=2.0,
                route_labels=('Pump.fun',),
                context_slot=789,
            )
            settings = SimpleNamespace(
                max_hold_sec=120.0,
                check_min_sec=0.5,
                check_max_sec=0.5,
                usd_rub_rate=90.0,
                exit_fee_sol=0.0001,
            )
            state = TraderState(open_count=1, slots=asyncio.Semaphore(0))
            position.opened_at -= 121.0

            with patch('trader.get_jupiter_quote', new=AsyncMock(return_value=quote)):
                with patch('trader.get_sol_usd', new=AsyncMock(return_value=100.0)):
                    with patch('trader.resolve_real_price', new=AsyncMock()) as price_mock:
                        with patch('trader._finish_position', new=AsyncMock()):
                            await manage_position(
                                None, settings, position, None, state,
                                wallet, None,
                            )

            price_mock.assert_not_awaited()
            self.assertAlmostEqual(position.sold_frac, 1.0)
            self.assertEqual(state.open_count, 0)
            self.assertEqual(state.slots._value, 1)


class LiquidityThresholdTests(unittest.IsolatedAsyncioTestCase):
    # ПЛАН: проверить, что MIN_LIQUIDITY_USD сравнивается с фактическим резервом.

    # ПЛАН: повторить только пустой ответ индексаторов для свежего пула.
    async def test_liquidity_retries_when_new_pool_is_not_indexed_yet(self) -> None:
        dex_pairs = [
            [],
            [{'liquidity': {'usd': '5000'}}],
        ]
        with patch(
            'filters.fetch_dexscreener_pairs',
            new=AsyncMock(side_effect=dex_pairs),
        ) as dex_mock:
            with patch(
                'filters.fetch_geckoterminal_pools',
                new=AsyncMock(return_value=[]),
            ) as gecko_mock:
                result = await check_liquidity(
                    None, 'MintFreshPool', 5000,
                    retries=1, retry_delay_sec=0.0,
                )

        self.assertTrue(result.passed)
        self.assertEqual(dex_mock.await_count, 2)
        self.assertEqual(gecko_mock.await_count, 1)

    # ПЛАН: не пропускать пару, если liquidity ниже настройки.
    async def test_liquidity_below_threshold_is_rejected(self) -> None:
        dex_pairs = [{'liquidity': {'usd': '9999.99'}}]
        gecko_pools = [
            {'attributes': {'reserve_in_usd': '7500'}},
        ]
        with patch(
            'filters.fetch_dexscreener_pairs',
            new=AsyncMock(return_value=dex_pairs),
        ):
            with patch(
                'filters.fetch_geckoterminal_pools',
                new=AsyncMock(return_value=gecko_pools),
            ):
                result = await check_liquidity(None, 'MintLowLiq', 10_000)

        self.assertFalse(result.passed)
        self.assertIn('ниже порога', result.reasons[0])

    # ПЛАН: пропустить пару, когда DexScreener подтверждает нужную сумму.
    async def test_liquidity_at_threshold_is_accepted(self) -> None:
        dex_pairs = [{'liquidity': {'usd': '10000'}}]
        with patch(
            'filters.fetch_dexscreener_pairs',
            new=AsyncMock(return_value=dex_pairs),
        ):
            with patch(
                'filters.fetch_geckoterminal_pools',
                new=AsyncMock(),
            ) as gecko_mock:
                result = await check_liquidity(None, 'MintHighLiq', 10_000)

        self.assertTrue(result.passed)
        self.assertIn('DexScreener liquidity', result.reasons[0])
        gecko_mock.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
