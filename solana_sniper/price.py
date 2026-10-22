"""
price.py — поиск реальной цены монеты.

Цепочка источников:
  1. Pump.fun — НАПРЯМУЮ из он-чейн бондинг-кривой (сразу при создании!)
  2. DexScreener — индексатор DEX-пар
  3. GeckoTerminal — запасной индексатор
  4. None — «цены нет» (вызывающий код решает, что делать)

Бондинг-кривая Pump.fun — аккаунт с фиксированной раскладкой байтов:
    offset  8: virtual_token_reserves (u64, little-endian)
    offset 16: virtual_sol_reserves   (u64, little-endian)
    offset 48: complete (bool) — True = монета мигрировала на DEX
Цена в SOL = (virtual_sol / 1e9) / (virtual_token / 1e6).
"""

from __future__ import annotations

import asyncio
import base64
import math
import struct
import time
from collections import Counter, deque
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from console import log
from models import SwapQuote

DEXSCREENER_URL = 'https://api.dexscreener.com/latest/dex/tokens/{mint}'
JUPITER_QUOTE_URL = 'https://lite-api.jup.ag/swap/v1/quote'
GECKOTERMINAL_URL = 'https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/pools'
COINGECKO_SOL_URL = 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd'
WSOL = 'So11111111111111111111111111111111111111112'  # обёрнутый SOL — его mint в пулах

# Кэш курса SOL/USD: (когда получили, цена). Не долбим API на каждый тик.
_sol_usd_cache: Tuple[float, Optional[float]] = (0.0, None)


# ПЛАН:
# 1. get_sol_usd()          — курс SOL→USD с кэшем (DexScreener → CoinGecko)
# 2. rpc_get_account_raw()  — сырые байты аккаунта из RPC
# 3. find_curve_owner()     — найти бондинг-кривую монеты Pump.fun
# 4. pump_curve_price_usd() — цена из резервов кривой
# 5. fetch_dexscreener_price_usd()   — цена с DexScreener
# 6. fetch_geckoterminal_price_usd() — цена с GeckoTerminal
# 7. resolve_real_price()   — полная цепочка: (цена, имя_источника)

async def get_sol_usd(session: aiohttp.ClientSession) -> Optional[float]:
    # ПЛАН:
    # 1. Если свежий курс есть в кэше — вернуть его
    # 2. Попросить DexScreener цену WSOL (пара с максимальной ликвидностью)
    # 3. Если не вышло — попросить CoinGecko
    # 4. Удачный курс запомнить в кэш и вернуть
    global _sol_usd_cache
    cached_at, cached = _sol_usd_cache
    if cached and time.time() - cached_at < 60.0:
        return cached

    price: Optional[float] = None
    try:
        async with session.get(
            DEXSCREENER_URL.format(mint=WSOL),
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status == 200:
                pairs = ((await resp.json()).get('pairs')) or []

                def score(pair: Dict[str, Any]) -> Tuple[float, float]:
                    # ПЛАН:
                    # 1. Достать ликвидность и цену из пары (с защитой от мусора)
                    # 2. Вернуть кортеж (ликвидность, цена) — для max()
                    try:
                        liq = float(((pair.get('liquidity') or {}).get('usd')) or 0.0)
                        px = float(pair.get('priceUsd') or 0.0)
                    except (TypeError, ValueError):
                        return (0.0, 0.0)
                    return (liq, px)

                best = max(pairs, key=score, default={})
                px = score(best)[1]
                if px > 0:
                    price = px
    except Exception:
        pass  # сеть барахлит — пробуем запасной источник ниже

    if not price:
        try:
            async with session.get(
                COINGECKO_SOL_URL,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status == 200:
                    price = float((await resp.json())['solana']['usd'])
        except Exception:
            pass

    if price:
        _sol_usd_cache = (time.time(), price)
    return price


class ReadOnlyRpcMetrics:
    """Small in-memory measurements for read-only JSON-RPC batch calls."""

    def __init__(self) -> None:
        self.requests = 0
        self.logical_requests = 0
        self.batch_groups = 0
        self.failed_requests = 0
        # Run #4: separate "we saw a 429" from "we lost the read". A retried
        # 429 that succeeds is back-pressure, not a telemetry failure, and the
        # stop-list must judge only the unrecovered ones.
        self.retry_count = 0
        self.recovered_requests = 0
        self.http_429_count = 0
        self.unrecovered_failures = 0
        self.accounts_requested = 0
        self.null_accounts = 0
        self.null_retries = 0
        self.errors: Counter[str] = Counter()
        self.latencies_ms: deque[float] = deque(maxlen=20_000)

    def record(
        self, latency_ms: float, account_count: int, null_count: int = 0,
        error_type: Optional[str] = None,
    ) -> None:
        self.requests += 1
        self.accounts_requested += max(0, int(account_count))
        self.null_accounts += max(0, int(null_count))
        self.latencies_ms.append(max(0.0, float(latency_ms)))
        if error_type:
            self.failed_requests += 1
            self.errors[str(error_type)] += 1
            if str(error_type) == 'http_429':
                self.http_429_count += 1

    def snapshot(self) -> dict[str, Any]:
        values = sorted(self.latencies_ms)
        def percentile(p: float) -> Optional[float]:
            if not values:
                return None
            idx = max(0, min(len(values) - 1, math.ceil(p * len(values)) - 1))
            return round(values[idx], 2)
        return {
            'rpc_method': 'getMultipleAccounts',
            'requests': self.requests,
            'logical_requests': self.logical_requests,
            'batch_groups': self.batch_groups,
            'failed_requests': self.failed_requests,
            'http_429_count': self.http_429_count,
            'retry_count': self.retry_count,
            'unrecovered_failures': self.unrecovered_failures,
            'unrecovered_rate': round(
                self.unrecovered_failures / self.requests, 6,
            ) if self.requests else 0.0,
            'recovered_failures': self.recovered_requests,
            'accounts_requested': self.accounts_requested,
            'null_accounts': self.null_accounts,
            'null_retries': self.null_retries,
            'latency_ms_median': percentile(0.50),
            'latency_ms_p95': percentile(0.95),
            'latency_samples': len(values),
            'error_types': dict(self.errors),
        }


_rpc_metrics = ReadOnlyRpcMetrics()


def reset_rpc_metrics() -> None:
    global _rpc_metrics
    _rpc_metrics = ReadOnlyRpcMetrics()


def rpc_metrics_snapshot() -> dict[str, Any]:
    return _rpc_metrics.snapshot()


async def _rpc_multiple_accounts_once(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    addresses: list[str],
    encoding: str,
    commitment: str,
) -> tuple[list[Optional[dict[str, Any]]], Optional[int], Optional[str]]:
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getMultipleAccounts',
        'params': [addresses, {'encoding': encoding, 'commitment': commitment}],
    }
    started = time.monotonic()
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            status = int(resp.status)
            if status != 200:
                error_type = f'http_{status}'
                _rpc_metrics.record((time.monotonic() - started) * 1000, len(addresses), error_type=error_type)
                return [None] * len(addresses), None, error_type
            data = await resp.json()
        if not isinstance(data, dict) or data.get('error'):
            _rpc_metrics.record((time.monotonic() - started) * 1000, len(addresses), error_type='jsonrpc_error')
            return [None] * len(addresses), None, 'jsonrpc_error'
        result = data.get('result') or {}
        values = result.get('value')
        if not isinstance(values, list) or len(values) != len(addresses):
            _rpc_metrics.record((time.monotonic() - started) * 1000, len(addresses), error_type='malformed_result')
            return [None] * len(addresses), None, 'malformed_result'
        context = result.get('context') or {}
        try:
            context_slot = int(context.get('slot')) if context.get('slot') is not None else None
        except (TypeError, ValueError):
            context_slot = None
        null_count = sum(value is None for value in values)
        _rpc_metrics.record((time.monotonic() - started) * 1000, len(addresses), null_count=null_count)
        return values, context_slot, None
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        error_type = f'exception_{type(exc).__name__}'
        _rpc_metrics.record(
            (time.monotonic() - started) * 1000, len(addresses), error_type=error_type,
        )
        return [None] * len(addresses), None, error_type


# Errors that mean "the provider pushed back", not "the request was wrong".
_RETRYABLE_RPC_ERRORS = frozenset({
    'http_429', 'http_500', 'http_502', 'http_503', 'http_504',
    'exception_ClientError', 'exception_ServerTimeoutError',
    'exception_TimeoutError', 'exception_ClientOSError',
})
RPC_MAX_ATTEMPTS = 3          # first try + 2 retries
RPC_RETRY_BACKOFF_SEC = (0.25, 0.75)


async def _rpc_multiple_accounts_with_retry(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    addresses: list[str],
    encoding: str,
    commitment: str,
) -> tuple[list[Optional[dict[str, Any]]], Optional[int], Optional[str]]:
    """One logical read with back-pressure retries (run #4).

    Helius answers in 429 bursts. Without a retry every burst turned into a
    "failed request" and the stop-list killed an 8-hour session after 49
    minutes (run #3: 16/1408 = 1.14 %). A retried 429 that succeeds is
    back-pressure, not a failure: it is counted in ``retry_count`` and stays
    visible as ``http_429_count``, while only reads that never recovered reach
    ``unrecovered_failures`` — the number the stop-list judges.
    """
    error_type: Optional[str] = None
    for attempt in range(RPC_MAX_ATTEMPTS):
        values, slot, error_type = await _rpc_multiple_accounts_once(
            session, http_rpc_url, addresses, encoding, commitment,
        )
        if error_type is None:
            if attempt:
                _rpc_metrics.recovered_requests += 1
            return values, slot, None
        if error_type not in _RETRYABLE_RPC_ERRORS or attempt == RPC_MAX_ATTEMPTS - 1:
            break
        _rpc_metrics.retry_count += 1
        backoff = RPC_RETRY_BACKOFF_SEC[min(attempt, len(RPC_RETRY_BACKOFF_SEC) - 1)]
        await asyncio.sleep(backoff)
    _rpc_metrics.unrecovered_failures += 1
    return [None] * len(addresses), None, error_type


async def _rpc_get_multiple_accounts_direct(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    addresses: list[str] | tuple[str, ...],
    *,
    encoding: str = 'base64',
    commitment: str = 'processed',
    retry_null: bool = True,
) -> tuple[dict[str, Optional[dict[str, Any]]], Optional[int]]:
    """Read up to 100 accounts per `getMultipleAccounts`, retry nulls once at 120 ms.

    The returned mapping is keyed by the requested public key. No account data,
    endpoint URL, or credential is logged or written by this helper.
    """
    unique = list(dict.fromkeys(str(address) for address in addresses if address))
    if not unique:
        return {}, None
    result: dict[str, Optional[dict[str, Any]]] = {}
    last_slot: Optional[int] = None
    for start in range(0, len(unique), 100):
        chunk = unique[start:start + 100]
        values, slot, _error = await _rpc_multiple_accounts_with_retry(
            session, http_rpc_url, chunk, encoding, commitment,
        )
        result.update(zip(chunk, values))
        if slot is not None:
            last_slot = slot
        missing = [address for address, value in zip(chunk, values) if value is None]
        if missing and retry_null:
            _rpc_metrics.null_retries += len(missing)
            await asyncio.sleep(0.120)
            retry_values, retry_slot, _retry_error = await _rpc_multiple_accounts_with_retry(
                session, http_rpc_url, missing, encoding, commitment,
            )
            for address, value in zip(missing, retry_values):
                if value is not None:
                    result[address] = value
            if retry_slot is not None:
                last_slot = retry_slot
    return result, last_slot


class ReadOnlyRpcBatcher:
    """Coalesce concurrent RPC readers for a short window into <=100-key batches."""

    def __init__(
        self, session: aiohttp.ClientSession, http_rpc_url: str,
        flush_window_sec: float = 0.008,
    ) -> None:
        self.session = session
        self.http_rpc_url = http_rpc_url
        self.flush_window_sec = max(0.0, float(flush_window_sec))
        self.queue: asyncio.Queue = asyncio.Queue()
        self.worker: Optional[asyncio.Task] = None
        self.closed = False

    def start(self) -> None:
        if self.worker is None:
            self.worker = asyncio.create_task(self._run(), name='read-only-rpc-batcher')

    async def get_multiple_accounts(
        self, addresses: list[str] | tuple[str, ...], *,
        encoding: str = 'base64', commitment: str = 'processed',
        retry_null: bool = True,
    ) -> tuple[dict[str, Optional[dict[str, Any]]], Optional[int]]:
        unique = list(dict.fromkeys(str(address) for address in addresses if address))
        if not unique:
            return {}, None
        _rpc_metrics.logical_requests += 1
        if self.closed or self.worker is None:
            _rpc_metrics.batch_groups += 1
            return await _rpc_get_multiple_accounts_direct(
                self.session, self.http_rpc_url, unique,
                encoding=encoding, commitment=commitment, retry_null=retry_null,
            )
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        await self.queue.put((unique, encoding, commitment, retry_null, future))
        return await future

    async def _run(self) -> None:
        while True:
            item = await self.queue.get()
            if item is None:
                self.queue.task_done()
                return
            requests = [item]
            if self.flush_window_sec:
                await asyncio.sleep(self.flush_window_sec)
            while True:
                try:
                    extra = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if extra is None:
                    self.queue.task_done()
                    self.closed = True
                    break
                requests.append(extra)
            groups: dict[tuple[str, str, bool], list[tuple[Any, ...]]] = {}
            for request in requests:
                key = (str(request[1]), str(request[2]), bool(request[3]))
                groups.setdefault(key, []).append(request)
            for (encoding, commitment, retry_null), group in groups.items():
                _rpc_metrics.batch_groups += 1
                all_addresses = list(dict.fromkeys(
                    address for request in group for address in request[0]
                ))
                try:
                    accounts, slot = await _rpc_get_multiple_accounts_direct(
                        self.session, self.http_rpc_url, all_addresses,
                        encoding=encoding, commitment=commitment,
                        retry_null=retry_null,
                    )
                    for addresses, _enc, _commitment, _retry, future in group:
                        if not future.done():
                            future.set_result((
                                {address: accounts.get(address) for address in addresses},
                                slot,
                            ))
                except asyncio.CancelledError:
                    for request in group:
                        future = request[4]
                        if not future.done():
                            future.cancel()
                    raise
                except Exception:
                    for request in group:
                        future = request[4]
                        if not future.done():
                            future.set_result(({}, None))
            for _request in requests:
                self.queue.task_done()
            if self.closed:
                return

    async def close(self) -> None:
        if self.worker is None or self.worker.done():
            self.closed = True
            return
        await self.queue.join()
        self.closed = True
        await self.queue.put(None)
        try:
            await asyncio.wait_for(self.worker, timeout=10.0)
        except asyncio.TimeoutError:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)


_rpc_batcher: Optional[ReadOnlyRpcBatcher] = None


def set_rpc_batcher(batcher: Optional[ReadOnlyRpcBatcher]) -> None:
    global _rpc_batcher
    _rpc_batcher = batcher


async def rpc_get_multiple_accounts(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    addresses: list[str] | tuple[str, ...],
    *,
    encoding: str = 'base64',
    commitment: str = 'processed',
    retry_null: bool = True,
) -> tuple[dict[str, Optional[dict[str, Any]]], Optional[int]]:
    if _rpc_batcher is not None and _rpc_batcher.session is session:
        return await _rpc_batcher.get_multiple_accounts(
            addresses, encoding=encoding, commitment=commitment,
            retry_null=retry_null,
        )
    _rpc_metrics.logical_requests += 1
    _rpc_metrics.batch_groups += 1
    return await _rpc_get_multiple_accounts_direct(
        session, http_rpc_url, addresses, encoding=encoding,
        commitment=commitment, retry_null=retry_null,
    )


async def rpc_get_account_raw(
    session: aiohttp.ClientSession, http_rpc_url: str, address: str
) -> Optional[bytes]:
    """Fetch one account as a one-key getMultipleAccounts batch (read-only)."""
    accounts, _slot = await rpc_get_multiple_accounts(
        session, http_rpc_url, [address], encoding='base64', retry_null=True,
    )
    value = accounts.get(address)
    raw = value.get('data') if isinstance(value, dict) else None
    if not isinstance(raw, list) or not raw or not isinstance(raw[0], str):
        return None
    try:
        return base64.b64decode(raw[0], validate=True)
    except (ValueError, TypeError):
        return None


async def get_jupiter_quote(
    session: aiohttp.ClientSession,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int = 100,
) -> Optional[SwapQuote]:
    # ПЛАН:
    # 1. Запросить у Jupiter котировку ExactIn для точного размера в raw-единицах
    # 2. Проверить mint, сумму, наличие маршрута и положительную выручку
    # 3. Вернуть ожидаемый output и нижний порог с учётом slippage
    if amount_raw <= 0 or slippage_bps < 0:
        return None

    params = {
        'inputMint': input_mint,
        'outputMint': output_mint,
        'amount': str(amount_raw),
        'slippageBps': str(slippage_bps),
        'swapMode': 'ExactIn',
    }
    try:
        async with session.get(
            JUPITER_QUOTE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    if not isinstance(data, dict):
        return None
    route_plan = data.get('routePlan')
    if (data.get('inputMint') != input_mint
            or data.get('outputMint') != output_mint
            or not isinstance(route_plan, list)
            or not route_plan):
        return None

    try:
        in_amount = int(data.get('inAmount') or 0)
        out_amount = int(data.get('outAmount') or 0)
        min_out = int(data.get('otherAmountThreshold') or out_amount)
        raw_impact = data.get('priceImpactPct')
        price_impact = (
            float(raw_impact) * 100.0 if raw_impact is not None else None
        )
        raw_slot = data.get('contextSlot')
        context_slot = int(raw_slot) if raw_slot is not None else None
    except (TypeError, ValueError):
        return None

    if in_amount != amount_raw or out_amount <= 0 or min_out <= 0:
        return None
    if min_out > out_amount:
        return None

    route_labels: List[str] = []
    for step in route_plan:
        if not isinstance(step, dict):
            continue
        swap_info = step.get('swapInfo')
        if isinstance(swap_info, dict):
            route_labels.append(str(swap_info.get('label') or 'DEX'))
    if not route_labels:
        return None
    labels = tuple(route_labels)

    return SwapQuote(
        input_mint=input_mint,
        output_mint=output_mint,
        in_amount_raw=in_amount,
        out_amount_raw=out_amount,
        min_out_raw=min_out,
        price_impact_pct=price_impact,
        route_labels=labels,
        context_slot=context_slot,
    )


async def find_curve_owner(
    session: aiohttp.ClientSession, http_rpc_url: str, mint: str
) -> Optional[str]:
    # ПЛАН:
    # 1. Узнать крупнейший токен-аккаунт монеты (getTokenLargestAccounts)
    # 2. Узнать владельца этого токен-аккаунта (getAccountInfo jsonParsed)
    # 3. Для Pump.fun владелец = бондинг-кривая — вернуть её адрес
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getTokenLargestAccounts',
        'params': [mint, {'commitment': 'processed'}],
    }
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
        value = (data.get('result') or {}).get('value') or []
        if not value:
            return None
        token_account = value[0].get('address')
    except Exception:
        return None

    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getAccountInfo',
        'params': [token_account, {'encoding': 'jsonParsed', 'commitment': 'processed'}],
    }
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
        value = ((data.get('result') or {}).get('value')) or {}
        info = (((value.get('data') or {}).get('parsed')) or {}).get('info') or {}
        return info.get('owner')
    except Exception:
        return None


async def pump_curve_price_usd(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    curve: Optional[str],
) -> Optional[float]:
    # ПЛАН:
    # 1. Если адрес кривой неизвестен — найти его
    # 2. Загрузить байты аккаунта кривой
    # 3. Распаковать резервы (u64 по смещениям 8 и 16) и флаг complete
    # 4. Проверить инвариант CPMM: vTok × vSol = const
    #    (спам/тестовые аккаунты нарушают его — цену не вернём)
    # 5. Если монета мигрировала или резервы пустые — вернуть None
    # 6. Посчитать цену в SOL и перевести в USD через курс SOL
    if not curve:
        curve = await find_curve_owner(session, http_rpc_url, mint)
    if not curve:
        return None

    raw = await rpc_get_account_raw(session, http_rpc_url, curve)
    if not raw or len(raw) < 48:
        return None

    vtok, vsol = struct.unpack_from('<QQ', raw, 8)
    complete = bool(raw[48]) if len(raw) > 48 else False
    if complete or vtok == 0:
        return None  # мигрировала с кривой — дальше по индексаторам

    # Инвариант бондинг-кривой: виртуальные резервы всегда
    # лежат на одной гиперболе vTok × vSol = k0. Начальные
    # резервы Pump.fun: 1.073e15 токенов × 30 SOL.
    # Если произведение отличается — аккаунт мусорный
    # (такие «ракеты +2395%» мы уже видели живьём).
    k = vtok * vsol
    k0 = int(1073000191279242 * 30 * 1e9)
    if k <= 0 or abs(k - k0) / k0 > 0.01:
        return None

    price_sol = (vsol / 1e9) / (vtok / 1e6)
    sol_usd = await get_sol_usd(session)
    if not sol_usd:
        return None
    return price_sol * sol_usd


async def fetch_dexscreener_pairs(
    session: aiohttp.ClientSession, mint: str
) -> List[Dict[str, Any]]:
    # ПЛАН:
    # 1. Запросить все пары монеты у DexScreener
    # 2. Вернуть список пар (пустой — данных нет)
    try:
        async with session.get(
            DEXSCREENER_URL.format(mint=mint),
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                return []
            return (await resp.json()).get('pairs') or []
    except Exception:
        return []


async def fetch_dexscreener_price_usd(
    session: aiohttp.ClientSession, mint: str
) -> Optional[float]:
    # ПЛАН:
    # 1. Загрузить все пары монеты
    # 2. Выбрать пару с максимальной ликвидностью
    # 3. Вернуть её цену в USD (или None, если данных нет)
    pairs = await fetch_dexscreener_pairs(session, mint)
    if not pairs:
        return None

    def liquidity(pair: Dict[str, Any]) -> float:
        # ПЛАН:
        # 1. Достать из пары ликвидность в USD
        # 2. При мусорных данных вернуть 0.0
        try:
            return float(((pair.get('liquidity') or {}).get('usd')) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    best = max(pairs, key=liquidity)
    try:
        price = float(best.get('priceUsd') or 0.0)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


async def fetch_geckoterminal_pools(
    session: aiohttp.ClientSession, mint: str
) -> List[Dict[str, Any]]:
    # ПЛАН:
    # 1. Запросить пулы токена у GeckoTerminal
    # 2. Вернуть список пулов (пустой — данных нет)
    try:
        async with session.get(
            GECKOTERMINAL_URL.format(mint=mint),
            headers={'Accept': 'application/json'},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                return []
            return (await resp.json()).get('data') or []
    except Exception:
        return []


async def fetch_geckoterminal_price_usd(
    session: aiohttp.ClientSession, mint: str
) -> Optional[float]:
    # ПЛАН:
    # 1. Загрузить пулы токена
    # 2. Выбрать пул с максимальным reserve_in_usd
    # 3. Определить, наш токен base или quote, и вернуть его цену
    pools = await fetch_geckoterminal_pools(session, mint)
    if not pools:
        return None

    def reserve(pool: Dict[str, Any]) -> float:
        # ПЛАН:
        # 1. Достать из пула объём ликвидности в USD
        # 2. При мусорных данных вернуть 0.0
        try:
            return float(((pool.get('attributes') or {}).get('reserve_in_usd')) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    attrs = (max(pools, key=reserve).get('attributes')) or {}
    try:
        if attrs.get('base_token_address') == mint:
            price = float(attrs.get('base_token_price_usd') or 0.0)
        elif attrs.get('quote_token_address') == mint:
            price = float(attrs.get('quote_token_price_usd') or 0.0)
        else:
            price = float(attrs.get('base_token_price_usd') or 0.0)
    except (TypeError, ValueError):
        return None
    return price if price and price > 0 else None


async def resolve_real_price(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    source: str,
    curve: Optional[str],
) -> Tuple[Optional[float], str]:
    # ПЛАН:
    # 1. Для Pump.fun попробовать он-чейн кривую (работает сразу!)
    # 2. Потом DexScreener
    # 3. Потом GeckoTerminal
    # 4. Вернуть (цена, имя_источника) или (None, '')
    if source == 'Pump.fun':
        price = await pump_curve_price_usd(session, http_rpc_url, mint, curve)
        if price:
            return price, 'он-чейн кривая'

    price = await fetch_dexscreener_price_usd(session, mint)
    if price:
        return price, 'DexScreener'

    price = await fetch_geckoterminal_price_usd(session, mint)
    if price:
        return price, 'GeckoTerminal'

    return None, ''
