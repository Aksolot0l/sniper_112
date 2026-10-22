"""
trader.py — paper trading (симуляция торговли, БЕЗ реальных денег!).

Конвейер: очередь монет → фильтры → фейк-покупка → мониторинг цены →
фейк-продажа по лестнице выхода → запись сделки в очередь для SQLite.

Лестница выхода («Челлендж 1500»):
  до TP1: настраиваемый SL → TP1 (продать 50%) → Stagnation
  после TP1: настраиваемый Break-Even → трейлинг с настраиваемым откатом от ATH
  БЕЗ потолка прибыли — «ракету» держим

Режимы (из настроек):
  'one'       — первая прошедшая фильтры монета, одна сделка, потом стоп
  'portfolio' — до max_positions позиций одновременно, работаем до Ctrl+C
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import aiohttp

from config import Settings
from console import Color, log, short
from creator_watch import creator_should_block
from filters import run_all_checks
from models import NewCoin, Position, SwapQuote, TradeRecord
from price import WSOL, get_jupiter_quote, get_sol_usd, resolve_real_price
from trade_logger import log_trade
from telemetry import get_collector
from wallet import Wallet


# ПЛАН:
# 1. TraderState       — счётчики режима (сколько позиций, взята ли цель)
# 2. manage_position() — цикл: ATH/застой + лестница выхода
# 3. sell_part()       — продажа только по свежей quote точного размера
# 4. _finish_position()— итоговый PnL, wallet.json, trades.log, SQLite
# 5. start_paper_trade — вход по Jupiter quote и фактическим decimals
# 6. _handle_coin()    — фильтры + захват слота + запуск сделки
# 7. trader_worker()   — воркер: очередь монет → сделки

@dataclass
class TraderState:
    open_count: int = 0      # сколько позиций сейчас открыто
    first_taken: bool = False  # в режиме 'one': цель уже выбрана
    # 3 слота входа (макс. 3 открытых позиции) +
    # дроссель параллельности фильтров (защита RPC)
    slots: Optional[asyncio.Semaphore] = None
    filters: Optional[asyncio.Semaphore] = None


def quote_confirms_exit_trigger(
    pos: Position,
    reason: str,
    quote_price_usd: float,
    settings: Settings,
) -> bool:
    # ПЛАН:
    # 1. Задать ценовой порог соответствующего SL/TP/BE/trailing
    # 2. Сравнить его с исполнимой Jupiter quote-ценой точного размера
    # 3. Для таймаута/застоя разрешить выход при любой положительной quote
    # 4. Неподтверждённый spot-сигнал не превращать в бумажный fill
    if quote_price_usd <= 0:
        return False

    if reason.startswith('SL'):
        threshold = pos.entry_price * (1.0 - settings.stop_loss_pct / 100.0)
        return quote_price_usd <= threshold
    if reason.startswith('TP1'):
        threshold = pos.entry_price * (1.0 + settings.take_profit_pct / 100.0)
        return quote_price_usd >= threshold
    if reason.startswith('Break-Even'):
        threshold = pos.entry_price * (1.0 + settings.be_offset_pct / 100.0)
        return quote_price_usd <= threshold
    if reason.startswith('Trailing Stop'):
        threshold = pos.ath_price * (1.0 - settings.trail_pct / 100.0)
        return quote_price_usd <= threshold
    return True


# Ссылки на фоновые задачи позиций — иначе сборщик мусора их «съест»
_position_tasks: set = set()


def _spawn(coro) -> None:
    # ПЛАН:
    # 1. Создать задачу из корутины
    # 2. Сохранить ссылку на неё (защита от сборщика мусора)
    # 3. По завершении убрать задачу из набора
    task = asyncio.create_task(coro)
    _position_tasks.add(task)
    task.add_done_callback(_position_tasks.discard)


async def cancel_position_tasks() -> None:
    """Stop monitors on shutdown without inventing a fill for open positions."""
    tasks = list(_position_tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _pos_to_dict(pos: Position) -> Dict[str, Any]:
    # ПЛАН:
    # 1. Собрать ВСЕ поля позиции в словарь (JSON-дружелюбный)
    # 2. Словарь ключ == имя поля — восстановление Position(**dict)
    return {
        'mint': pos.mint,
        'source': pos.source,
        'entry_price': pos.entry_price,
        'simulated': pos.simulated,
        'opened_at': pos.opened_at,
        'deposit_rub': pos.deposit_rub,
        'curve': pos.curve,
        'price_origin': pos.price_origin,
        'tokens': pos.tokens,
        'decimals': pos.decimals,
        'tokens_raw': pos.tokens_raw,
        'sold_tokens_raw': pos.sold_tokens_raw,
        'invested_rub': pos.invested_rub,
        'fees_rub': pos.fees_rub,
        'realized_rub': pos.realized_rub,
        'realized_exit_value_usd': pos.realized_exit_value_usd,
        'sold_frac': pos.sold_frac,
        'ath_price': pos.ath_price,
        'tp1_done': pos.tp1_done,
        'last_move_ts': pos.last_move_ts,
        'ref_price': pos.ref_price,
        'exit_pending': pos.exit_pending,
        'pending_exit_reason': pos.pending_exit_reason,
        'pending_exit_frac': pos.pending_exit_frac,
        'last_exit_attempt': pos.last_exit_attempt,
        'last_exit_log': pos.last_exit_log,
        'parsed_curve_data': pos.parsed_curve_data,
    }


def _telemetry_decision(
    mint: str, source: str, stage: str, decision: str,
    reasons: Any = None, values: Any = None,
) -> None:
    collector = get_collector()
    if collector is not None:
        collector.record_decision(mint, source, stage, decision, reasons, values)


async def _get_entry_quote_with_retries(
    session: aiohttp.ClientSession,
    settings: Settings,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    token_mint: str,
    quote_kind: str,
) -> Optional[SwapQuote]:
    # ПЛАН:
    # 1. Запрашивать точную ExactIn quote без изменения raw-размера
    # 2. Если маршрута нет — повторить с коротким линейным backoff
    # 3. При любом подтверждённом маршруте вернуть именно свежую quote
    # 4. После всех попыток вернуть None и оставить fail-closed поведение
    retries = min(
        max(int(getattr(settings, 'entry_quote_retries', 0)), 0), 5
    )
    base_delay = max(
        float(getattr(settings, 'entry_quote_retry_delay_sec', 1.0)), 0.0
    )

    for attempt in range(retries + 1):
        quote = await get_jupiter_quote(
            session, input_mint, output_mint, amount_raw, slippage_bps=100
        )
        if quote is not None:
            if attempt > 0:
                log(
                    f'✅ {short(token_mint)} — exact Jupiter quote '
                    f'{quote_kind} появилась после {attempt} повтора(ов)',
                    Color.DIM,
                )
            return quote

        if attempt < retries:
            delay = base_delay * (attempt + 1)
            log(
                f'↻ {short(token_mint)} — нет точной Jupiter quote '
                f'{quote_kind}; повтор {attempt + 1}/{retries} '
                f'через {delay:.1f}с',
                Color.DIM,
            )
            await asyncio.sleep(delay)

    return None


async def manage_position(
    session: aiohttp.ClientSession,
    settings: Settings,
    pos: Position,
    trades_queue: 'asyncio.Queue[TradeRecord]',
    state: TraderState,
    wallet: Wallet,
    stop_event: asyncio.Event,
) -> None:
    # ПЛАН:
    # 1. Выход с неподтверждённой котировкой держать pending и повторять раз в 5 секунд
    # 2. До дедлайна проверять живую цену и приоритетно применять MAX_HOLD_SEC
    # 3. Выполнять прежнюю лестницу SL → TP1 → BE/трейлинг без изменения порогов
    # 4. Освобождать слот только после полной продажи подтверждённым маршрутом
    try:
        while True:
            if pos.exit_pending:
                retry_in = 5.0 - (time.time() - pos.last_exit_attempt)
                if retry_in > 0:
                    await asyncio.sleep(retry_in)
                reason = pos.pending_exit_reason or 'Повтор выхода'
                fraction = pos.pending_exit_frac or (1.0 - pos.sold_frac)
                await sell_part(
                    pos, fraction, pos.ath_price, reason,
                    session, settings, wallet, trades_queue,
                    state, stop_event,
                )
                if pos.sold_frac >= 1.0 - 1e-9:
                    return
                continue

            held = time.time() - pos.opened_at
            remaining = settings.max_hold_sec - held
            if remaining <= 0:
                await sell_part(
                    pos, 1.0 - pos.sold_frac, pos.ath_price,
                    'Timeout (максимум держания)',
                    session, settings, wallet, trades_queue,
                    state, stop_event,
                )
                if pos.sold_frac >= 1.0 - 1e-9:
                    return
                continue

            # Если spot-сигнал не подтвердился точной quote, не спамим Jupiter.
            if pos.last_exit_attempt:
                retry_in = 5.0 - (time.time() - pos.last_exit_attempt)
                if retry_in > 0:
                    await asyncio.sleep(min(retry_in, remaining))
                    continue
                pos.last_exit_attempt = 0.0

            delay = random.uniform(settings.check_min_sec, settings.check_max_sec)
            await asyncio.sleep(min(delay, remaining))

            remaining = settings.max_hold_sec - (time.time() - pos.opened_at)
            if remaining <= 0:
                await sell_part(
                    pos, 1.0 - pos.sold_frac, pos.ath_price,
                    'Timeout (максимум держания)',
                    session, settings, wallet, trades_queue,
                    state, stop_event,
                )
                if pos.sold_frac >= 1.0 - 1e-9:
                    return
                continue

            real: Optional[float] = None
            origin = ''
            try:
                async with asyncio.timeout(remaining):
                    real, origin = await resolve_real_price(
                        session, settings.http_rpc_url, pos.mint,
                        pos.source, pos.curve,
                    )
            except TimeoutError:
                real, origin = None, ''

            held = time.time() - pos.opened_at
            if held >= settings.max_hold_sec:
                await sell_part(
                    pos, 1.0 - pos.sold_frac, pos.ath_price,
                    'Timeout (максимум держания)',
                    session, settings, wallet, trades_queue,
                    state, stop_event,
                )
                if pos.sold_frac >= 1.0 - 1e-9:
                    return
                continue

            if real is None:
                log(
                    f'⏳ {short(pos.mint)}: нет достоверной цены — ждём '
                    f'{held:.0f}с в позиции',
                    Color.DIM,
                )
                continue

            # Индексатор может случайно сопоставить чужую пару и дать фантомную цену.
            if pos.entry_price and real / pos.entry_price > 50.0:
                log(
                    f'⚠️ {short(pos.mint)}: цена ${real:.6g} выглядит гличем '
                    f'(>50× от входа) — игнорирую тик',
                    Color.YELLOW,
                )
                continue

            price = real
            pos.simulated = False
            pos.price_origin = origin
            if price > pos.ath_price:
                pos.ath_price = price

            if not pos.ref_price or abs(price / pos.ref_price - 1.0) >= (
                settings.stagnation_range_pct / 100.0
            ):
                pos.ref_price = price
                pos.last_move_ts = time.time()

            pct = (price / pos.entry_price - 1.0) * 100.0
            price_tag = pos.price_origin or 'РЕАЛ'
            log(
                f'⏳ [{price_tag}] {short(pos.mint)} '
                f'({pos.source}) цена ${price:.10g} → {pct:+.1f}% от '
                f'quote-входа | ATH ${pos.ath_price:.10g} | '
                f'остаток {(1.0 - pos.sold_frac) * 100:.0f}% | {held:.0f}с',
                Color.DIM,
            )

            # --- лестница выхода: пороги стратегии оставлены прежними ---
            if not pos.tp1_done:
                if pct <= -settings.stop_loss_pct:
                    await sell_part(
                        pos, 1.0 - pos.sold_frac, price,
                        f'SL (стоп-лосс −{settings.stop_loss_pct:.0f}%)',
                        session, settings, wallet, trades_queue,
                        state, stop_event,
                    )
                    if pos.sold_frac >= 1.0 - 1e-9:
                        return
                    continue

                if pct >= settings.take_profit_pct:
                    filled = await sell_part(
                        pos, 0.5, price,
                        f'TP1 (тейк +{settings.take_profit_pct:.0f}%)',
                        session, settings, wallet, trades_queue,
                        state, stop_event,
                    )
                    if filled and pos.tp1_done:
                        log(
                            f'🛡 TP1 подтверждён quote-маршрутом; '
                            f'SL остатка перенесён в безубыток '
                            f'+{settings.be_offset_pct:.1f}%',
                            Color.CYAN,
                        )
                    continue

                if time.time() - pos.last_move_ts >= settings.stagnation_sec:
                    await sell_part(
                        pos, 1.0 - pos.sold_frac, price,
                        'Stagnation (застой цены)',
                        session, settings, wallet, trades_queue,
                        state, stop_event,
                    )
                    if pos.sold_frac >= 1.0 - 1e-9:
                        return
                    continue
            else:
                if pct <= settings.be_offset_pct:
                    await sell_part(
                        pos, 1.0 - pos.sold_frac, price,
                        f'Break-Even (уровень +{settings.be_offset_pct:.1f}%)',
                        session, settings, wallet, trades_queue,
                        state, stop_event,
                    )
                    if pos.sold_frac >= 1.0 - 1e-9:
                        return
                    continue

                if price <= pos.ath_price * (1.0 - settings.trail_pct / 100.0):
                    await sell_part(
                        pos, 1.0 - pos.sold_frac, price,
                        f'Trailing Stop (−{settings.trail_pct:.0f}% от ATH)',
                        session, settings, wallet, trades_queue,
                        state, stop_event,
                    )
                    if pos.sold_frac >= 1.0 - 1e-9:
                        return
                    continue
    except asyncio.CancelledError:
        raise
    finally:
        if pos.sold_frac >= 1.0 - 1e-9:
            state.open_count -= 1
            state.slots.release()


async def sell_part(
    pos: Position,
    frac: float,
    trigger_price: float,
    reason: str,
    session: aiohttp.ClientSession,
    settings: Settings,
    wallet: Wallet,
    trades_queue: 'asyncio.Queue[TradeRecord]',
    state: TraderState,
    stop_event: asyncio.Event,
) -> bool:
    # ПЛАН:
    # 1. Перевести точную долю raw-токенов в quote Jupiter ExactIn
    # 2. При отсутствии маршрута не менять баланс и оставить выход pending
    # 3. SL/TP/BE/trailing подтвердить ценой exact-size quote; timeout/stagnation
    #    требуют только свежий маршрут, потому что это временные выходы
    # 4. Зачислить только нижний порог quote с 1% slippage минус сетевую комиссию
    # 5. Атомарно сохранить возврат, PnL и остаток позиции

    frac = min(max(frac, 0.0), 1.0 - pos.sold_frac)
    _telemetry_decision(
        pos.mint, pos.source, 'exit_trigger', 'triggered', [reason],
        {'spot_trigger_price': trigger_price, 'requested_fraction': frac},
    )
    if frac <= 1e-9:
        return pos.sold_frac >= 1.0 - 1e-9

    if pos.decimals < 0 or pos.decimals > 18 or pos.tokens_raw <= 0:
        pos.exit_pending = True
        pos.pending_exit_reason = reason
        pos.pending_exit_frac = frac
        pos.last_exit_attempt = time.time()
        wallet.sync_open_position(_pos_to_dict(pos))
        if time.time() - pos.last_exit_log >= 30.0:
            log(
                f'⚠️ {short(pos.mint)}: точное количество токенов отсутствует '
                f'(legacy-позиция); продажу и выручку НЕ записываю',
                Color.YELLOW,
            )
            pos.last_exit_log = time.time()
            wallet.sync_open_position(_pos_to_dict(pos))
        return False

    remaining_raw = pos.tokens_raw - pos.sold_tokens_raw
    if remaining_raw <= 0:
        return pos.sold_frac >= 1.0 - 1e-9

    remaining_frac = 1.0 - pos.sold_frac
    amount_raw = (
        remaining_raw
        if frac >= remaining_frac - 1e-9
        else min(remaining_raw, int(pos.tokens_raw * frac))
    )
    if amount_raw <= 0:
        pos.exit_pending = True
        pos.pending_exit_reason = reason
        pos.pending_exit_frac = frac
        pos.last_exit_attempt = time.time()
        wallet.sync_open_position(_pos_to_dict(pos))
        return False

    actual_frac = amount_raw / pos.tokens_raw
    was_same_pending_exit = (
        pos.exit_pending and pos.pending_exit_reason == reason
    )
    pos.exit_pending = True
    pos.pending_exit_reason = reason
    pos.pending_exit_frac = actual_frac
    pos.last_exit_attempt = time.time()
    if not was_same_pending_exit:
        wallet.sync_open_position(_pos_to_dict(pos))

    quote_started = time.monotonic()
    quote = await get_jupiter_quote(
        session, pos.mint, WSOL, amount_raw, slippage_bps=100
    )
    collector = get_collector()
    quote_event_id = None
    if collector is not None:
        quote_event_id = collector.record_quote(
            pos.mint, pos.source, 'position_exit', 'sell', pos.mint, WSOL,
            amount_raw, quote, 'ok' if quote else 'no_route', 100,
            (time.monotonic() - quote_started) * 1000,
            None if quote else 'quote_unavailable',
        )
    sol_usd = await get_sol_usd(session)
    if quote is None or sol_usd is None or sol_usd <= 0:
        _telemetry_decision(
            pos.mint, pos.source, 'position_exit', 'pending',
            ['sell_quote_unavailable' if quote is None else 'sol_price_unavailable'],
            {'amount_raw': amount_raw, 'reason': reason},
        )
        if time.time() - pos.last_exit_log >= 30.0:
            log(
                f'⚠️ {short(pos.mint)}: Jupiter не подтвердил свежий маршрут '
                f'для {amount_raw} raw-токенов; продажи/выручки нет, '
                f'позиция остаётся открытой и занятая сумма не высвобождается',
                Color.YELLOW,
            )
            pos.last_exit_log = time.time()
            wallet.sync_open_position(_pos_to_dict(pos))
        return False

    output_sol = quote.min_out_raw / 1_000_000_000
    sol_rub = sol_usd * settings.usd_rub_rate
    quote_return_rub = output_sol * sol_rub
    priority_rub = settings.exit_fee_sol * sol_rub
    net_return_rub = quote_return_rub - priority_rub
    pnl_leg = net_return_rub - pos.invested_rub * actual_frac
    token_ui = amount_raw / (10 ** pos.decimals)
    fill_price_usd = output_sol * sol_usd / token_ui

    if not quote_confirms_exit_trigger(
        pos, reason, fill_price_usd, settings
    ):
        _telemetry_decision(
            pos.mint, pos.source, 'quote_confirmation', 'rejected',
            ['quote_did_not_confirm_trigger'],
            {'reason': reason, 'quote_price_usd': fill_price_usd, 'spot_trigger_price': trigger_price},
        )
        pos.exit_pending = False
        pos.pending_exit_reason = ''
        pos.pending_exit_frac = 0.0
        if time.time() - pos.last_exit_log >= 30.0:
            log(
                f'⏸ {short(pos.mint)}: spot-триггер «{reason}» '
                f'не подтверждён исполнимой quote-ценой '
                f'${fill_price_usd:.10g} (spot ${trigger_price:.10g}); '
                f'paper-fill не записан, повтор проверки после cooldown',
                Color.YELLOW,
            )
            pos.last_exit_log = time.time()
        wallet.sync_open_position(_pos_to_dict(pos))
        return False

    if collector is not None:
        collector.record_fill(
            pos.mint, pos.source, quote_event_id, reason, actual_frac,
            fill_price_usd, net_return_rub, pnl_leg, priority_rub,
            quote.min_out_raw, quote.price_impact_pct, list(quote.route_labels),
        )
    pos.sold_tokens_raw += amount_raw
    pos.sold_frac = pos.sold_tokens_raw / pos.tokens_raw
    pos.realized_rub += net_return_rub
    pos.realized_exit_value_usd += output_sol * sol_usd
    pos.fees_rub += priority_rub
    if reason.startswith('TP1'):
        pos.tp1_done = True
    pos.exit_pending = False
    pos.pending_exit_reason = ''
    pos.pending_exit_frac = 0.0
    pos.last_exit_attempt = 0.0
    pos.last_exit_log = 0.0

    wallet.close_position(
        returned_rub=net_return_rub,
        pnl_rub=pnl_leg,
        fees_rub=priority_rub,
        pos_dict=_pos_to_dict(pos),
    )

    route = ' → '.join(quote.route_labels) or 'Jupiter'
    impact = (
        'н/д' if quote.price_impact_pct is None
        else f'{quote.price_impact_pct:.2f}%'
    )
    log(
        f'💰 Paper-fill по quote: продано {actual_frac * 100:.2f}% '
        f'({reason}), чистыми {net_return_rub:.2f} ₽ '
        f'(PnL части {pnl_leg:+.2f} ₽, сеть {priority_rub:.2f} ₽)',
        Color.CYAN,
    )
    log(
        f'   маршрут: {route} | price impact {impact} | '
        f'выручка по порогу slippage 1%: {quote.min_out_raw} lamports SOL '
        f'| средняя цена fill ${fill_price_usd:.10g}',
        Color.GRAY,
    )

    if pos.sold_frac >= 1.0 - 1e-9:
        sold_ui = pos.sold_tokens_raw / (10 ** pos.decimals)
        average_exit_price = (
            pos.realized_exit_value_usd / sold_ui if sold_ui > 0 else fill_price_usd
        )
        await _finish_position(
            pos, average_exit_price, reason, session, settings, wallet,
            trades_queue, state, stop_event,
        )
    return True


async def _finish_position(
    pos: Position,
    exit_price: float,
    reason: str,
    session: aiohttp.ClientSession,
    settings: Settings,
    wallet: Wallet,
    trades_queue: 'asyncio.Queue[TradeRecord]',
    state: TraderState,
    stop_event: asyncio.Event,
) -> None:
    # ПЛАН:
    # 1. Итоговый чистый PnL с учётом ВСЕХ комиссий сделки
    # 2. Красивая строка фейк-продажи с причиной выхода
    # 3. Запись в wallet.json, trades.log и SQLite
    # 4. В режиме 'one' — сигнал завершения (stop_event)
    if pos.tp1_done and not reason.startswith('TP1'):
        reason = f'TP1 + {reason}'

    pnl = pos.realized_rub - pos.invested_rub
    pct = pnl / pos.invested_rub * 100.0 if pos.invested_rub else 0.0

    if pct >= 0:
        log(
            f'🔴 ФЕЙК ПРОДАЖА! Итог: +{pct:.1f}% (Прибыль: {pnl:.2f} руб)',
            Color.GREEN + Color.BOLD,
        )
    else:
        log(
            f'🔴 ФЕЙК ПРОДАЖА! Итог: -{abs(pct):.1f}% (Убыток: {abs(pnl):.2f} руб)',
            Color.RED + Color.BOLD,
        )
    log(
        f'   причина: {reason} | mint: {pos.mint} | {pos.source} | '
        f'вход ${pos.entry_price:.10g} → выход ${exit_price:.10g} | '
        f'сетевые комиссии: {pos.fees_rub:.2f} ₽ | держали '
        f'{time.time() - pos.opened_at:.0f}с',
        Color.GRAY,
    )
    log(f'   💳 Баланс кошелька: {wallet.balance_rub:.2f} ₽', Color.CYAN)

    swept = await wallet.update_wallet_and_vault(
        mint=pos.mint, source=pos.source,
        entry_rub=pos.invested_rub, exit_rub=pos.realized_rub,
        pnl_rub=pnl, fees_rub=pos.fees_rub, reason=reason,
    )
    if swept > 0:
        log(
            f'🏦 СЕЙФ: {swept:.2f} ₽ унесено (50% чистой прибыли) — '
            f'vault: {wallet.vault_rub:.2f} ₽, торговать нельзя',
            Color.MAGENTA + Color.BOLD,
        )
    log_trade(
        settings.trade_log_path, pos.mint, pos.invested_rub,
        pos.realized_rub, pnl, wallet.balance_rub,
    )

    record = TradeRecord(
        mint=pos.mint,
        source=pos.source,
        entry_price=pos.entry_price,
        exit_price=exit_price,
        pct=pct,
        pnl_rub=pnl,
        reason=reason,
        hold_sec=time.time() - pos.opened_at,
        created_at=datetime.now().isoformat(timespec='seconds'),
    )
    await trades_queue.put(record)
    # Дождаться, пока журнал ЗАПИШЕТ сделку (join ждёт task_done от storage).
    # Почему это важно: иначе при быстрой остановке запись теряется.
    await trades_queue.join()

    if settings.mode == 'one':
        log('🏁 Сделка по целевой монете закрыта — завершаем работу', Color.CYAN)
        stop_event.set()


async def start_paper_trade(
    session: aiohttp.ClientSession,
    settings: Settings,
    coin: NewCoin,
    trades_queue: 'asyncio.Queue[TradeRecord]',
    state: TraderState,
    wallet: Wallet,
    stop_event: asyncio.Event,
    sol_usd: Optional[float] = None,
) -> bool:
    # ПЛАН:
    # 1. Проверить decimals и курс SOL/USD
    # 2. Получить точную Jupiter buy-quote с ограниченными повторами
    # 3. Для точного количества токенов так же подтвердить sell-route
    # 4. Списать notional и комиссию только после обеих свежих котировок
    # 5. При исчерпании попыток или превышении impact оставить вход закрытым
    entry_started_monotonic = time.monotonic()
    entry_rub = wallet.position_size()
    if entry_rub < 1.0:
        _telemetry_decision(coin.mint, coin.source, 'entry', 'skipped', ['balance_below_minimum'], {'size_rub': entry_rub})
        log(f'⏸ {short(coin.mint)} — баланс мал для позиции, вход пропущен',
            Color.YELLOW)
        return False

    if coin.decimals is None or not 0 <= coin.decimals <= 18:
        _telemetry_decision(coin.mint, coin.source, 'entry', 'blocked', ['decimals_unverified'], {'decimals': coin.decimals})
        log(
            f'⏸ {short(coin.mint)} — decimals mint не подтверждены; '
            f'покупку не симулирую',
            Color.YELLOW,
        )
        return False

    if sol_usd is None or sol_usd <= 0:
        sol_usd = await get_sol_usd(session)
    if sol_usd is None or sol_usd <= 0:
        _telemetry_decision(coin.mint, coin.source, 'entry', 'blocked', ['sol_price_unavailable'])
        log(
            f'⏸ {short(coin.mint)} — нет актуального курса SOL/USD; '
            f'вход пропущен без списания',
            Color.YELLOW,
        )
        return False

    sol_rub = sol_usd * settings.usd_rub_rate
    if sol_rub <= 0:
        _telemetry_decision(coin.mint, coin.source, 'entry', 'blocked', ['invalid_sol_rub'], {'sol_usd': sol_usd, 'usd_rub_rate': settings.usd_rub_rate})
        log(f'⏸ {short(coin.mint)} — некорректный курс SOL/RUB', Color.YELLOW)
        return False

    input_sol_raw = int(entry_rub / sol_rub * 1_000_000_000)
    if input_sol_raw <= 0:
        _telemetry_decision(coin.mint, coin.source, 'entry', 'blocked', ['amount_below_one_lamport'])
        log(f'⏸ {short(coin.mint)} — размер входа меньше 1 lamport SOL',
            Color.YELLOW)
        return False

    quote_started = time.monotonic()
    quote = await _get_entry_quote_with_retries(
        session, settings, WSOL, coin.mint, input_sol_raw,
        coin.mint, 'покупки',
    )
    buy_quote_latency_ms = (time.monotonic() - quote_started) * 1000.0
    collector = get_collector()
    quote_event_id = None
    if collector is not None:
        quote_event_id = collector.record_quote(
            coin.mint, coin.source, 'entry_buy', 'buy', WSOL, coin.mint,
            input_sol_raw, quote, 'ok' if quote else 'no_route', 100,
            buy_quote_latency_ms,
            None if quote else 'quote_unavailable',
        )
    if quote is None:
        _telemetry_decision(coin.mint, coin.source, 'entry', 'blocked', ['buy_quote_unavailable'], {'input_amount_raw': input_sol_raw})
        log(
            f'⏸ {short(coin.mint)} — Jupiter не подтвердил маршрут покупки '
            f'для фактического размера; вход и списание пропущены',
            Color.YELLOW,
        )
        return False

    max_impact_pct = settings.max_entry_price_impact_pct
    if (
        quote.price_impact_pct is None
        or quote.price_impact_pct > max_impact_pct
    ):
        _telemetry_decision(
            coin.mint, coin.source, 'entry_buy_impact', 'blocked',
            ['impact_unknown' if quote.price_impact_pct is None else 'impact_above_limit'],
            {'impact_pct': quote.price_impact_pct, 'limit_pct': max_impact_pct},
        )
        shown_impact = (
            'н/д' if quote.price_impact_pct is None
            else f'{quote.price_impact_pct:.2f}%'
        )
        log(
            f'⏸ {short(coin.mint)} — impact покупки {shown_impact} '
            f'выше лимита {max_impact_pct:.2f}% или не подтверждён; '
            f'вход пропущен',
            Color.YELLOW,
        )
        return False

    tokens_raw = quote.min_out_raw
    tokens = tokens_raw / (10 ** coin.decimals)
    if tokens_raw <= 0 or tokens <= 0:
        _telemetry_decision(coin.mint, coin.source, 'entry_buy', 'blocked', ['zero_token_output'], {'tokens_raw': tokens_raw})
        log(f'⏸ {short(coin.mint)} — quote вернул нулевой выход токенов',
            Color.YELLOW)
        return False

    exit_quote_started = time.monotonic()
    exit_quote = await _get_entry_quote_with_retries(
        session, settings, coin.mint, WSOL, tokens_raw,
        coin.mint, 'точной продажи',
    )
    sell_precheck_latency_ms = (time.monotonic() - exit_quote_started) * 1000.0
    collector = get_collector()
    precheck_quote_id = None
    if collector is not None:
        precheck_quote_id = collector.record_quote(
            coin.mint, coin.source, 'entry_sell_precheck', 'sell', coin.mint,
            WSOL, tokens_raw, exit_quote, 'ok' if exit_quote else 'no_route',
            100, sell_precheck_latency_ms,
            None if exit_quote else 'quote_unavailable',
        )
    if exit_quote is None:
        _telemetry_decision(coin.mint, coin.source, 'entry_sell_precheck', 'blocked', ['sell_route_unavailable'], {'tokens_raw': tokens_raw})
        log(
            f'⏸ {short(coin.mint)} — нет подтверждённого Jupiter-маршрута '
            f'продажи для точного количества токенов; вход пропущен',
            Color.YELLOW,
        )
        return False
    if (
        exit_quote.price_impact_pct is None
        or exit_quote.price_impact_pct > max_impact_pct
    ):
        _telemetry_decision(
            coin.mint, coin.source, 'entry_sell_precheck_impact', 'blocked',
            ['impact_unknown' if exit_quote.price_impact_pct is None else 'impact_above_limit'],
            {'impact_pct': exit_quote.price_impact_pct, 'limit_pct': max_impact_pct, 'tokens_raw': tokens_raw},
        )
        shown_impact = (
            'н/д' if exit_quote.price_impact_pct is None
            else f'{exit_quote.price_impact_pct:.2f}%'
        )
        log(
            f'⏸ {short(coin.mint)} — impact точной продажи {shown_impact} '
            f'выше лимита {max_impact_pct:.2f}% или не подтверждён; '
            f'вход пропущен',
            Color.YELLOW,
        )
        return False

    trade_cost_rub = input_sol_raw / 1_000_000_000 * sol_rub
    trade_cost_usd = trade_cost_rub / settings.usd_rub_rate
    entry_price = trade_cost_usd / tokens
    buy_priority = settings.entry_fee_sol * sol_rub
    invested = trade_cost_rub + buy_priority
    route = ' → '.join(quote.route_labels) or 'Jupiter'
    impact = (
        'н/д' if quote.price_impact_pct is None
        else f'{quote.price_impact_pct:.2f}%'
    )
    exit_route = ' → '.join(exit_quote.route_labels) or 'Jupiter'
    exit_impact = (
        'н/д' if exit_quote.price_impact_pct is None
        else f'{exit_quote.price_impact_pct:.2f}%'
    )
    opened_at = time.time()

    parsed_curve_data = dict(coin.packet or {})
    parsed_curve_data.update({
        'curve': coin.curve,
        'creator': coin.program_creator or coin.creator,
        'program_creator': coin.program_creator,
        'tx_payer': coin.tx_payer,
        'pumpportal_event_actor': (coin.packet or {}).get('pumpportal_event_actor'),
        'entry_quote': {
            'telemetry_event_id': quote_event_id,
            'route': list(quote.route_labels),
            'context_slot': quote.context_slot,
            'price_impact_pct': quote.price_impact_pct,
            'slippage_bps': 100,
        },
        'exit_precheck_quote': {
            'telemetry_event_id': precheck_quote_id,
            'tokens_raw': tokens_raw,
            'route': list(exit_quote.route_labels),
            'context_slot': exit_quote.context_slot,
            'price_impact_pct': exit_quote.price_impact_pct,
            'slippage_bps': 100,
        },
    })

    pos = Position(
        mint=coin.mint,
        source=coin.source,
        entry_price=entry_price,
        simulated=False,
        opened_at=opened_at,
        deposit_rub=trade_cost_rub,
        curve=coin.curve,
        price_origin=f'Jupiter quote: {route}',
        tokens=tokens,
        decimals=coin.decimals,
        tokens_raw=tokens_raw,
        invested_rub=invested,
        fees_rub=buy_priority,
        ath_price=entry_price,
        last_move_ts=opened_at,
        ref_price=entry_price,
        parsed_curve_data=parsed_curve_data,
    )

    balance_before = wallet.balance_rub
    wallet.open_position(invested, _pos_to_dict(pos))
    coin.packet['_paper_entry_opened_at_epoch'] = opened_at
    collector = get_collector()
    if collector is not None:
        collector.record_decision(
            coin.mint, coin.source, 'entry_quote_latency', 'confirmed',
            ['exact_buy_and_sell_precheck_quotes'],
            {
                'buy_quote_ms': round(buy_quote_latency_ms, 2),
                'sell_precheck_quote_ms': round(sell_precheck_latency_ms, 2),
                'quote_sequence_ms': round(buy_quote_latency_ms + sell_precheck_latency_ms, 2),
                'entry_execution_from_start_ms': round((time.monotonic() - entry_started_monotonic) * 1000.0, 2),
                'first_seen_to_entry_ms': round(max(0.0, (opened_at - coin.found_at) * 1000.0), 2),
                'paper_only': True,
            },
        )

    log(
        f'🟢 ФЕЙК ПОКУПКА монеты {coin.mint} на {trade_cost_rub:.2f} рублей '
        f'(баланс до входа {balance_before:.2f} ₽, '
        f'кап {settings.max_trade_rub:.0f} ₽)',
        Color.GREEN + Color.BOLD,
    )
    log(
        f'   quote Jupiter ExactIn: {input_sol_raw / 1_000_000_000:.9f} SOL '
        f'→ {tokens:.8g} токенов (decimals={coin.decimals}); маршрут {route}; '
        f'impact {impact}; нижний порог slippage 1%',
        Color.GRAY,
    )
    log(
        f'   exact-size precheck продажи: {tokens_raw} raw токенов → '
        f'{exit_route}; impact {exit_impact} (не гарантия будущего fill)',
        Color.GRAY,
    )
    log(
        f'   входная средняя цена ${entry_price:.10g} | '
        f'сеть {buy_priority:.2f} ₽ отдельно | '
        f'TP1: +{settings.take_profit_pct:.0f}% (50%) → BE '
        f'+{settings.be_offset_pct:.1f}% → трейлинг −{settings.trail_pct:.0f}% '
        f'от ATH | SL: −{settings.stop_loss_pct:.0f}% | '
        f'MAX_HOLD_SEC={settings.max_hold_sec:.0f}',
        Color.GRAY,
    )

    _spawn(
        manage_position(
            session, settings, pos, trades_queue, state, wallet, stop_event
        )
    )
    return True


async def _handle_coin(
    coin: NewCoin,
    trades_queue: 'asyncio.Queue[TradeRecord]',
    session: aiohttp.ClientSession,
    settings: Settings,
    state: TraderState,
    wallet: Wallet,
    stop_event: asyncio.Event,
    watch_stop: asyncio.Event,
    creator_watch: Any = None,
    creator_probe: Any = None,
    watchlist: Any = None,
) -> None:
    # ПЛАН:
    # 1. Прогнать фильтры безопасности (долго: RPC + Jupiter)
    # 2. Заблокированные — с честной причиной в лог
    # 3. Атомарно захватить слот (проверка + присвоение БЕЗ await между ними)
    # 4. В режиме 'one' — отключить дальнейший просмотр (watch_stop):
    #    новые монеты уже не купим, а кредиты RPC тратить зря не надо
    # 5. Прошедшей — фейк-покупка
    # Фильтры внутри — ПОПАРРЕЛЬНЫЙ набор (asyncio.gather),
    # но не более FILTER_CONCURRENCY монет одновременно:
    # защита RPC от штурма при всплеске созданий.
    handler_started = time.monotonic()
    first_seen_to_handler_ms = max(0.0, (time.time() - coin.found_at) * 1000.0)
    filter_started: Optional[float] = None
    async with state.filters:
        filter_started = time.monotonic()
        # Warmed SOL/USD cache is expected; refresh runs concurrently with checks.
        sol_task = asyncio.create_task(get_sol_usd(session))
        verdict = await run_all_checks(
            session,
            settings.http_rpc_url,
            coin,
            settings.min_liquidity_usd,
            settings.honeypot_check,
            settings.only_sol_pairs,
            settings.indexer_retries,
            settings.indexer_retry_delay_sec,
            creator_probe=creator_probe,
        )
        sol_usd = await sol_task
    filter_done = time.monotonic()
    collector = get_collector()
    filter_metrics = {
        'queue_delay_ms': round(first_seen_to_handler_ms, 2),
        'filter_semaphore_wait_ms': round(max(0.0, ((filter_started or handler_started) - handler_started) * 1000.0), 2),
        'filter_execution_ms': round(max(0.0, (filter_done - (filter_started or handler_started)) * 1000.0), 2),
        'first_seen_to_filter_done_ms': round(max(0.0, (time.time() - coin.found_at) * 1000.0), 2),
    }
    if collector is not None:
        collector.record_decision(
            coin.mint, coin.source, 'security_and_market_filters',
            'pass' if verdict.passed else 'fail', verdict.reasons,
            {'decimals': coin.decimals, 'sol_usd_available': bool(sol_usd and sol_usd > 0),
             'packet': coin.packet, **filter_metrics},
        )

    # Creator attribution/history is recorded only after the curve-derived
    # program_creator has had a chance to arrive; features are available at this
    # decision timestamp and remain warn-only under the Rocket2 run profile.
    creator_decision_epoch = time.time()
    if collector is not None:
        try:
            collector.record_creator_attribution(
                coin, observed_at_epoch=creator_decision_epoch,
            )
        except Exception:
            pass
    profile: dict[str, Any] = {}
    if creator_watch is not None:
        try:
            profile = creator_watch.register_candidate(
                coin, eligible=True, observed_at=creator_decision_epoch,
            )
        except Exception as exc:
            log(f'⚠️ Creator watch кандидата {short(coin.mint)}: {type(exc).__name__}', Color.YELLOW)
    if profile:
        risk_level = str(profile.get('risk_level') or 'identity_unknown')
        policy = str(getattr(settings, 'creator_risk_policy', 'warn') or 'warn')
        blocked, block_reason = creator_should_block(profile, policy)
        watch_signal = risk_level not in {
            'first_seen', 'identity_unknown', 'identity_time_unknown',
            'unverified_candidate',
        }
        gate_decision = 'blocked' if blocked else ('watch' if watch_signal else 'pass')
        gate_reasons = [block_reason] if block_reason else list(profile.get('risk_evidence') or [])
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, 'creator_watch', risk_level,
                profile.get('risk_evidence') or [], profile,
            )
            collector.record_decision(
                coin.mint, coin.source, 'creator_safety_gate', gate_decision,
                gate_reasons,
                {**profile, 'risk_policy': policy, 'paper_only': True,
                 'entry_blocked_by_creator': bool(blocked)},
            )
        if watch_signal:
            log(
                f"👤 Creator watch: {risk_level} | identity={profile.get('identity_type')} "
                f"| wallet={profile.get('creator') or 'unknown'} | "
                f"prior={profile.get('prior_token_count', 0)} | "
                f"adverse={profile.get('prior_adverse_token_count', 0)} | "
                f"latency={profile.get('latency_ms')}ms | mint={coin.mint}",
                Color.YELLOW if not blocked else Color.RED,
            )
        if blocked:
            log(
                f'⛔ Paper-вход заблокирован Creator watch: {block_reason} '
                f'(wallet={profile.get("creator")}, mint={coin.mint})',
                Color.RED + Color.BOLD,
            )
            return

    if not verdict.passed:
        log(f'🛑 {short(coin.mint)} — фильтры заблокировали:', Color.YELLOW)
        for reason in verdict.reasons:
            log(f'   • {reason}', Color.GRAY)
        return

    # Independent shadow portfolio: a full main-bot position semaphore does not
    # consume Watchlist capacity, and Watchlist never occupies main slots.
    if watchlist is not None and coin.source == 'Pump.fun':
        try:
            watchlist.consider_candidate(coin)
        except Exception as exc:
            log(f'⚠️ Watchlist candidate {short(coin.mint)}: {type(exc).__name__}', Color.YELLOW)

    if settings.mode == 'one' and state.first_taken:
        _telemetry_decision(coin.mint, coin.source, 'entry_capacity', 'skipped', ['single_trade_already_taken'], {**filter_metrics, 'first_seen_to_skip_ms': round(max(0.0, (time.time() - coin.found_at) * 1000.0), 2)})
        return  # уже работаем с целевой монетой
    if wallet.position_size() < 1.0:
        _telemetry_decision(coin.mint, coin.source, 'entry_capacity', 'skipped', ['balance_below_minimum'], {**filter_metrics, 'position_size_rub': wallet.position_size(), 'first_seen_to_skip_ms': round(max(0.0, (time.time() - coin.found_at) * 1000.0), 2)})
        log(f'⏸ {short(coin.mint)} — баланс мал для позиции, вход пропущен',
            Color.YELLOW)
        return

    # Атомарный захват слота: пытаемся захватить СЕЙЧАС,
    # без ожидания (asyncio.Semaphore(max_positions) = 3 слота).
    try:
        async with asyncio.timeout(0):
            await state.slots.acquire()
    except TimeoutError:
        _telemetry_decision(coin.mint, coin.source, 'entry_capacity', 'skipped', ['slot_busy'], {**filter_metrics, 'max_positions': settings.max_positions, 'first_seen_to_skip_ms': round(max(0.0, (time.time() - coin.found_at) * 1000.0), 2)})
        log(
            f'⏸ {short(coin.mint)} — все {settings.max_positions} слота '
            f'заняты, вход пропущен',
            Color.YELLOW,
        )
        return
    if settings.mode == 'one':
        # На время quote резервируем единственный вход, чтобы не открыть две цели.
        state.first_taken = True
    state.open_count += 1

    started = await start_paper_trade(
        session, settings, coin, trades_queue, state, wallet,
        stop_event, sol_usd,
    )
    if not started:
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, 'paper_entry', 'not_opened',
                ['entry_quote_or_precheck_rejected'],
                {**filter_metrics,
                 'first_seen_to_entry_attempt_finished_ms': round(max(0.0, (time.time() - coin.found_at) * 1000.0), 2),
                 'paper_only': True},
            )
        state.open_count -= 1
        state.slots.release()
        if settings.mode == 'one':
            state.first_taken = False
        return

    if collector is not None:
        entry_at = float(coin.packet.get('_paper_entry_opened_at_epoch') or time.time())
        entry_latency_ms = max(0.0, (entry_at - coin.found_at) * 1000.0)
        collector.record_decision(
            coin.mint, coin.source, 'paper_entry', 'opened', ['quote_confirmed'],
            {**filter_metrics, 'first_seen_to_entry_ms': round(entry_latency_ms, 2),
             'paper_only': True},
        )

    if settings.mode == 'one':
        # Вход подтверждён quote-маршрутом — больше новые монеты не нужны.
        watch_stop.set()


async def trader_worker(
    coins_queue: 'asyncio.Queue[NewCoin]',
    trades_queue: 'asyncio.Queue[TradeRecord]',
    session: aiohttp.ClientSession,
    settings: Settings,
    wallet: Wallet,
    stop_event: asyncio.Event,
    watch_stop: asyncio.Event,
    creator_watch: Any = None,
    creator_probe: Any = None,
    watchlist: Any = None,
) -> None:
    # ПЛАН:
    # 1. Бесконечно брать монеты из очереди (get() — СТРОГО вне try)
    # 2. Каждую монету отдать фоновой задаче: медленные фильтры НЕ должны
    #    блокировать очередь (одна долгая проверка ≠ пауза для всех)
    # 3. ВСЕГДА отмечать обработку (task_done() — в finally)
    state = TraderState()
    # 3 слота входа (макс. 3 открытых позиции) +
    # дроссель параллельности фильтров
    state.slots = asyncio.Semaphore(settings.max_positions)
    state.filters = asyncio.Semaphore(settings.filter_concurrency)

    # Восстановить позиции, открытые перед перезапуском:
    # деньги уже списаны, мониторинг продолжается
    for saved in list(wallet.open_positions):
        pos = Position(**saved)
        state.open_count += 1
        await state.slots.acquire()
        log(
            f'♻️ Восстановлена позиция {short(pos.mint)} '
            f'({pos.source}) — держали '
            f'{time.time() - pos.opened_at:.0f}с',
            Color.YELLOW,
        )
        _spawn(
            manage_position(
                session, settings, pos, trades_queue, state,
                wallet, stop_event,
            )
        )
    while True:
        coin = await coins_queue.get()
        try:
            collector = get_collector()
            if collector is not None:
                try:
                    collector.observe_candidate(coin, session, settings)
                except Exception as exc:
                    log(f'⚠️ Телеметрия кандидата {short(coin.mint)}: {type(exc).__name__}', Color.YELLOW)
            _spawn(
                _handle_coin(
                    coin, trades_queue, session, settings, state,
                    wallet, stop_event, watch_stop, creator_watch,
                    creator_probe, watchlist,
                )
            )
        finally:
            coins_queue.task_done()
