"""
filters.py — фильтры безопасности (анти-скам).

Принцип: каждый фильтр — ЧИСТАЯ проверка, которая возвращает FilterResult
с честной причиной. Проверки, которым нужен RPC/сеть — async-обёртки.

Что проверяем:
  ✅ mint authority отозван   — эмитент не может допечатать монету
  ✅ freeze authority отозван — никто не может заморозить твои токены
  ✅ разумный supply          — ноль или безумие = плохо
  ✅ ликвидность              — кривая Pump.fun или $-объём у индексаторов
  ✅ honeypot-маршрут         — есть ли вообще ПУТЬ ПРОДАЖИ (lite-проверка)
"""

from __future__ import annotations

import asyncio
import base64
import struct
import time
from typing import Any, Dict, Optional

import aiohttp

from console import log, short
from models import FilterResult, NewCoin
from pumpportal_validation import (
    PUMP_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    b58encode,
    derive_pump_bonding_curve,
)
from pump_curve_quote import (
    BONDING_CURVE_DISCRIMINATOR,
    PUMP_FEE_CONFIG_PDA,
    PUMP_FEE_PROGRAM_ID,
    PUMP_GLOBAL_PDA,
    decode_account_data,
)
from entry_reads import entry_account_cache, mint_account_info
from price import (
    WSOL,
    fetch_dexscreener_pairs,
    fetch_geckoterminal_pools,
    rpc_get_account_raw,
    rpc_get_multiple_accounts,
)

JUPITER_QUOTE_URL = (
    'https://lite-api.jup.ag/swap/v1/quote?inputMint={mint}'
    '&outputMint=So11111111111111111111111111111111111111112'
    '&amount=1000000&slippageBps=500'
)
PUMP_STANDARD_SUPPLY_MIN = 990_000_000_000_000
PUMP_STANDARD_SUPPLY_MAX = 1_000_000_000_000_000


# ПЛАН:
# 1. check_mint_authority()     — authority выпуска токенов отозван?
# 2. check_freeze_authority()   — никто не сможет заморозить токены?
# 3. check_supply()             — supply и decimals выглядят разумно?
# 4. check_pump_curve()         — подтверждена ли Pump.fun кривая?
# 5. check_sol_pair()           — пара торгуется именно с SOL?
# 6. check_liquidity()          — ликвидность подтверждена индексаторами?
# 7. check_honeypot_route()     — есть ли маршрут продажи через Jupiter?
# 8. fetch_mint_meta()          — получить метаданные mint через RPC
# 9. run_all_checks()            — запустить независимые проверки параллельно


def check_mint_authority(authority: Optional[str]) -> FilterResult:
    # ПЛАН:
    # 1. None означает, что mint authority отозван
    # 2. Любой адрес authority означает риск новой эмиссии
    if authority is None:
        return FilterResult(True, ['mint authority отозван — допечатать нельзя'])
    return FilterResult(
        False,
        [f'mint authority НЕ отозван ({short(authority)}) — риск допечатки'],
    )


def check_freeze_authority(authority: Optional[str]) -> FilterResult:
    # ПЛАН:
    # 1. None означает, что freeze authority отозван
    # 2. Любой адрес authority означает риск заморозки токенов
    if authority is None:
        return FilterResult(True, ['freeze authority отозван — заморозить нельзя'])
    return FilterResult(
        False,
        [f'freeze authority НЕ отозван ({short(authority)}) — могут заморозить'],
    )


def check_supply(raw_amount: int, decimals: int) -> FilterResult:
    # ПЛАН:
    # 1. Отклонить нулевой или отрицательный raw supply
    # 2. Отклонить decimals вне безопасного диапазона
    # 3. Остальное считать разумным для дальнейшей проверки
    if raw_amount <= 0:
        return FilterResult(False, ['supply = 0 — у монеты нет токенов'])
    if not 0 <= decimals <= 18:
        return FilterResult(False, [f'подозрительные decimals={decimals}'])
    return FilterResult(True, [f'supply выглядит разумно (decimals={decimals})'])


def check_pump_supply(
    mint_supply_raw: int, decimals: int, curve_supply_raw: int,
) -> FilterResult:
    """Require canonical Pump.fun decimals/supply and exact curve agreement."""
    reasons: list[str] = []
    passed = True
    if decimals != 6:
        passed = False
        reasons.append(f'pump_decimals_not_6 (decimals={decimals})')
    if not PUMP_STANDARD_SUPPLY_MIN <= mint_supply_raw <= PUMP_STANDARD_SUPPLY_MAX:
        passed = False
        reasons.append(
            'pump_supply_outside_standard_range '
            f'({mint_supply_raw}; expected {PUMP_STANDARD_SUPPLY_MIN}..{PUMP_STANDARD_SUPPLY_MAX})'
        )
    if mint_supply_raw != curve_supply_raw:
        passed = False
        reasons.append(
            'pump_supply_mismatch_curve_token_total_supply '
            f'(mint={mint_supply_raw}, curve={curve_supply_raw})'
        )
    if passed:
        reasons.append('pump_standard_supply_verified (decimals=6; curve token_total_supply matches mint)')
    return FilterResult(passed, reasons)


async def check_pump_curve(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    curve: Optional[str],
    packet: Optional[Dict[str, Any]],
    curve_raw: Optional[bytes] = None,
) -> FilterResult:
    # Packet reserves are only hints. Read the curve account so its owner, state,
    # and custom quote mint are verified before a Pump.fun paper entry.
    if not curve:
        return FilterResult(False, ['не нашли бондинг-кривую Pump.fun'])
    expected_curve = derive_pump_bonding_curve(mint)
    if expected_curve is None or curve != expected_curve:
        return FilterResult(False, ['pump_curve_pda_mismatch'])
    raw = curve_raw
    if raw is None:
        raw = await rpc_get_account_raw(session, http_rpc_url, curve)
    if not raw or len(raw) < 166:
        return FilterResult(False, ['Pump.fun curve layout unsupported or account not read'])
    if raw[:8] != BONDING_CURVE_DISCRIMINATOR:
        return FilterResult(False, ['pump_curve_discriminator_mismatch'])

    # The exact local quote requires the full SDK 4.1.0 BondingCurve layout,
    # including creator_fee_bps and the v2 quote-mint fields.
    if bool(raw[81]):
        return FilterResult(False, ['pump_mayhem_mode_excluded'])
    vtok, vsol = struct.unpack_from('<QQ', raw, 8)
    complete = bool(raw[48])
    if complete:
        return FilterResult(False, ['монета мигрировала с кривой — торгуем не по ней'])

    # BondingCurve account v2 stores an optional quote mint at bytes 83..114;
    # all-zero is the Pump.fun default (WSOL). Explicit non-WSOL quotes are blocked.
    quote_bytes = raw[83:115]
    if len(quote_bytes) != 32:
        return FilterResult(False, ['не удалось проверить quote mint кривой'])
    if any(quote_bytes):
        quote_mint = b58encode(quote_bytes)
        if quote_mint != WSOL:
            return FilterResult(False, [f'Pump.fun кривая использует не SOL quote ({quote_mint})'])
    if vtok == 0 or vsol == 0:
        return FilterResult(False, ['пустые резервы кривой'])
    return FilterResult(
        True,
        [f'кривая Pump.fun активна, PDA привязан к mint, quote=WSOL, виртуальный SOL-резерв {vsol / 1_000_000_000:.2f}'],
    )


async def check_sol_pair(
    session: aiohttp.ClientSession, coin: NewCoin
) -> FilterResult:
    # ПЛАН:
    # 1. Pump.fun bonding curve считается SOL-парой
    # 2. Для индексированных DEX-пар найти WSOL среди base/quote mint
    # 3. Если DexScreener пуст — так же проверить GeckoTerminal
    # 4. Не принимать неизвестную пару как SOL-пару
    if coin.source == 'Pump.fun':
        return FilterResult(True, ['Pump.fun: кривая торгуется только за SOL'])

    pairs = await fetch_dexscreener_pairs(session, coin.mint)
    for pair in pairs:
        base = (pair.get('baseToken') or {}).get('address', '')
        quote = (pair.get('quoteToken') or {}).get('address', '')
        if base == WSOL or quote == WSOL:
            return FilterResult(True, ['пара с SOL (DexScreener)'])
    if pairs:
        return FilterResult(False, ['пара без SOL (USDC/USDT?) — не ищем'])

    pools = await fetch_geckoterminal_pools(session, coin.mint)
    for pool in pools:
        attrs = pool.get('attributes') or {}
        if (
            attrs.get('base_token_address') == WSOL
            or attrs.get('quote_token_address') == WSOL
        ):
            return FilterResult(True, ['пара с SOL (GeckoTerminal)'])
    if pools:
        return FilterResult(False, ['пара без SOL (GeckoTerminal) — не ищем'])
    return FilterResult(False, ['пары не найдены у индексаторов'])


async def check_liquidity(
    session: aiohttp.ClientSession,
    mint: str,
    min_usd: float,
    retries: int = 0,
    retry_delay_sec: float = 1.0,
) -> FilterResult:
    # ПЛАН:
    # 1. Сравнить DexScreener liquidity.usd и GeckoTerminal reserve_in_usd
    # 2. Если найденная ликвидность ниже порога — блокировать без послаблений
    # 3. Повторить только случай, когда оба индексатора пока не вернули данных
    # 4. После попыток без подтверждения применить fail-closed блокировку
    retry_count = min(max(int(retries), 0), 3)
    base_delay = max(float(retry_delay_sec), 0.0)

    for attempt in range(retry_count + 1):
        dex_pairs = await fetch_dexscreener_pairs(session, mint)
        dex_max_usd = 0.0
        for pair in dex_pairs:
            try:
                liquidity_usd = float((pair.get('liquidity') or {}).get('usd') or 0)
            except (TypeError, ValueError):
                continue
            dex_max_usd = max(dex_max_usd, liquidity_usd)

        if dex_max_usd >= min_usd:
            return FilterResult(
                True,
                [f'DexScreener liquidity ${dex_max_usd:,.0f} >= порога ${min_usd:,.0f}'],
            )

        gecko_pools = await fetch_geckoterminal_pools(session, mint)
        gecko_max_usd = 0.0
        for pool in gecko_pools:
            attrs = pool.get('attributes') or {}
            try:
                reserve_usd = float(attrs.get('reserve_in_usd') or 0)
            except (TypeError, ValueError):
                continue
            gecko_max_usd = max(gecko_max_usd, reserve_usd)

        if gecko_max_usd >= min_usd:
            return FilterResult(
                True,
                [f'GeckoTerminal reserve ${gecko_max_usd:,.0f} >= порога ${min_usd:,.0f}'],
            )

        if not dex_pairs and not gecko_pools and attempt < retry_count:
            delay = base_delay * (attempt + 1)
            if delay > 0:
                await asyncio.sleep(delay)
            continue

        if dex_pairs or gecko_pools:
            return FilterResult(
                False,
                [
                    'ликвидность ниже порога '
                    f'${min_usd:,.0f} '
                    f'(DexScreener max ${dex_max_usd:,.0f}; '
                    f'GeckoTerminal max ${gecko_max_usd:,.0f})'
                ],
            )
        return FilterResult(
            False,
            ['ликвидность не найдена у индексаторов — консервативно блокируем'],
        )

    return FilterResult(
        False,
        ['ликвидность не найдена у индексаторов — консервативно блокируем'],
    )


async def check_honeypot_route(
    session: aiohttp.ClientSession,
    mint: str,
    attempts: int = 3,
) -> FilterResult:
    # ПЛАН:
    # 1. Попросить Jupiter посчитать маршрут ПРОДАЖИ монеты за SOL
    # 2. Если маршрут есть — продажа технически возможна
    # 3. HTTP 400 на свежей монете трактовать как возможную задержку индексации
    # 4. После коротких повторов без результата блокировать вход
    # ВНИМАНИЕ: это lite-проверка, а не симуляция swap-транзакции
    for attempt in range(attempts):
        try:
            async with session.get(
                JUPITER_QUOTE_URL.format(mint=mint),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    quote = await resp.json()
                    if quote.get('outAmount'):
                        return FilterResult(
                            True,
                            ['маршрут продажи через Jupiter существует'],
                        )
                    return FilterResult(
                        False,
                        ['Jupiter не нашёл маршрут продажи — возможен honeypot'],
                    )
                if resp.status in (400, 429) and attempt < attempts - 1:
                    await asyncio.sleep(1.5 if resp.status == 400 else 3.0)
                    continue
                return FilterResult(
                    False,
                    [f'Jupiter не дал маршрут продажи (HTTP {resp.status})'],
                )
        except Exception:
            if attempt < attempts - 1:
                await asyncio.sleep(1.5)
                continue
            return FilterResult(
                False,
                ['Jupiter недоступен — маршрут продажи не подтверждён'],
            )
    return FilterResult(False, ['Jupiter не подтвердил маршрут продажи'])


async def fetch_mint_meta(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    retries: int = 2,
    retry_delay_sec: float = 0.5,
) -> Optional[Dict[str, Any]]:
    # Новая монета может уже прийти по stream, пока public RPC ещё не
    # проиндексировал mint. Делаем короткие ограниченные повторы, затем
    # сохраняем fail-closed: без meta paper-входа нет.
    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getAccountInfo',
        'params': [mint, {'encoding': 'jsonParsed', 'commitment': 'processed'}],
    }
    attempts = min(max(int(retries), 0), 3) + 1
    delay = max(float(retry_delay_sec), 0.0)
    for attempt in range(attempts):
        try:
            async with session.post(
                http_rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
            value = ((data.get('result') or {}).get('value')) or {}
            info = (((value.get('data') or {}).get('parsed')) or {}).get('info')
            if info:
                return info
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        if attempt < attempts - 1 and delay:
            await asyncio.sleep(delay * (attempt + 1))
    return None


async def fetch_pump_mint_curve_accounts(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    mint: str,
    curve: Optional[str],
    retries: int = 1,
    retry_delay_sec: float = 0.1,
) -> Optional[tuple[Dict[str, Any], bytes, dict[str, Any]]]:
    """Read mint, curve, Global, and FeeConfig in one consistent RPC batch.

    Run #10b §1: the batch is base64 and goes through the one-cycle entry
    cache. The mint is decoded from the same answer (SPL Mint layout) instead of
    asking the provider to parse it, because a batch has one encoding and the
    curve side has always been base64. Nothing else changes: same four
    addresses, same owner checks, same snapshot keys.

    `retries`/`retry_delay_sec` are kept for call compatibility; null values are
    retried once inside `price._rpc_get_multiple_accounts_direct`, and a 429 is
    retried by the back-pressure loop, so a second loop here would only add
    requests.
    """
    expected_curve = derive_pump_bonding_curve(mint)
    if (
        not curve or expected_curve is None or curve != expected_curve
        or not PUMP_GLOBAL_PDA or not PUMP_FEE_CONFIG_PDA
    ):
        return None
    addresses = [mint, curve, PUMP_GLOBAL_PDA, PUMP_FEE_CONFIG_PDA]
    accounts, context_slot = await entry_account_cache().accounts(
        session, http_rpc_url, str(mint), mint, curve, source='filters',
    )
    if context_slot is None:
        return None
    mint_account = accounts.get(mint)
    curve_account = accounts.get(curve)
    global_account = accounts.get(PUMP_GLOBAL_PDA)
    fee_account = accounts.get(PUMP_FEE_CONFIG_PDA)
    if not all(isinstance(a, dict) for a in (mint_account, curve_account, global_account, fee_account)):
        return None
    if mint_account.get('owner') not in {TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID}:
        return None
    if curve_account.get('owner') != PUMP_PROGRAM_ID:
        return None
    if global_account.get('owner') != PUMP_PROGRAM_ID:
        return None
    if fee_account.get('owner') != PUMP_FEE_PROGRAM_ID:
        return None
    info = mint_account_info(mint_account)
    curve_raw = decode_account_data(curve_account)
    global_raw = decode_account_data(global_account)
    fee_config_raw = decode_account_data(fee_account)
    if (
        not isinstance(info, dict) or not curve_raw or not global_raw or not fee_config_raw
        or len(curve_raw) < 166 or curve_raw[:8] != BONDING_CURVE_DISCRIMINATOR
    ):
        return None
    snapshot = {
        'mint': mint,
        'curve': curve,
        'curve_raw': curve_raw,
        'global_raw': global_raw,
        'fee_config_raw': fee_config_raw,
        'context_slot': context_slot,
        'observed_at_epoch': time.time(),
        'account_addresses': addresses,
    }
    return info, curve_raw, snapshot


async def run_all_checks(
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    coin: NewCoin,
    min_liquidity_usd: float,
    honeypot_check: bool,
    only_sol_pairs: bool,
    indexer_retries: int = 0,
    indexer_retry_delay_sec: float = 1.0,
    creator_probe: Any = None,
) -> FilterResult:
    # Fail closed for Pump.fun events without the local mint↔curve PDA proof.
    is_pump = coin.source == 'Pump.fun'
    if is_pump and coin.packet.get('pumpportal_identity_verified') is not True:
        return FilterResult(False, ['Pump.fun mint/curve identity не подтверждена PumpPortal PDA-проверкой'])

    # One getMultipleAccounts request returns both mint metadata and the curve
    # state; check curve quote mint in parallel with lightweight pair checks.
    if is_pump:
        meta_task = asyncio.create_task(
            fetch_pump_mint_curve_accounts(
                session, http_rpc_url, coin.mint, coin.curve,
                retries=indexer_retries,
                retry_delay_sec=indexer_retry_delay_sec,
            )
        )
    else:
        meta_task = asyncio.create_task(
            fetch_mint_meta(
                session, http_rpc_url, coin.mint,
                retries=indexer_retries,
                retry_delay_sec=indexer_retry_delay_sec,
            )
        )

    tasks = [asyncio.create_task(check_sol_pair(session, coin))]
    if honeypot_check:
        tasks.append(asyncio.create_task(check_honeypot_route(session, coin.mint)))
    if not is_pump:
        tasks.append(asyncio.create_task(
            check_liquidity(
                session, coin.mint, min_liquidity_usd,
                retries=indexer_retries,
                retry_delay_sec=indexer_retry_delay_sec,
            )
        ))

    result = FilterResult(True, [])
    meta_result = await meta_task
    curve_raw: Optional[bytes] = None
    if is_pump:
        if meta_result is None:
            return FilterResult(False, ['не прочитали mint и Pump.fun curve в одном RPC-запросе — вход заблокирован'])
        meta, curve_raw, entry_snapshot = meta_result
        coin.entry_snapshot = entry_snapshot
        if not coin.curve or not curve_raw:
            return FilterResult(False, ['отсутствует подтверждённый аккаунт Pump.fun curve'])
        if len(curve_raw) < 166 or curve_raw[:8] != BONDING_CURVE_DISCRIMINATOR:
            return FilterResult(False, ['pump_curve_layout_unsupported_or_discriminator_mismatch'])

        # This successful, owner-checked and mint-derived PDA read is the
        # earliest causal Watchlist sample (t0). Never serialize raw bytes.
        coin.curve_raw = curve_raw
        coin.curve_observed_at = float(entry_snapshot.get('observed_at_epoch') or time.time())
        creator_bytes = curve_raw[49:81]
        coin.program_creator = (
            b58encode(creator_bytes) if len(creator_bytes) == 32 and any(creator_bytes)
            else None
        )
        packet = dict(coin.packet or {})
        coin.tx_payer = coin.tx_payer or packet.get('tx_payer')
        packet.update({
            'program_creator': coin.program_creator,
            'tx_payer': coin.tx_payer,
            'pump_curve_observed_at_epoch': coin.curve_observed_at,
            'pump_curve_token_total_supply_raw': struct.unpack_from('<Q', curve_raw, 40)[0],
            'pump_curve_mayhem_mode': bool(curve_raw[81]),
            'pump_curve_real_sol_reserves_raw': struct.unpack_from('<Q', curve_raw, 32)[0],
            'pump_curve_r_sol': struct.unpack_from('<Q', curve_raw, 32)[0] / 1_000_000_000,
            'pump_curve_quote_mint': (
                b58encode(curve_raw[83:115]) if any(curve_raw[83:115]) else WSOL
            ),
            'creator_identity_type': 'pump_curve_creator' if coin.program_creator else 'unknown',
            'creator_identity_source': 'pump_curve_raw_49_81' if coin.program_creator else None,
        })
        coin.packet = packet
    else:
        meta = meta_result
        if meta is None:
            return FilterResult(False, ['не прочитали mint-аккаунт — пропускаем'])
        packet = dict(coin.packet or {})
        coin.tx_payer = coin.tx_payer or packet.get('tx_payer') or coin.creator
        packet['tx_payer'] = coin.tx_payer
        coin.packet = packet

    result = result.merge(check_mint_authority(meta.get('mintAuthority')))
    result = result.merge(check_freeze_authority(meta.get('freezeAuthority')))
    try:
        coin.decimals = int(meta.get('decimals'))
    except (TypeError, ValueError):
        coin.decimals = None
    try:
        mint_supply_raw = int(meta.get('supply') or 0)
    except (TypeError, ValueError, OverflowError):
        mint_supply_raw = 0

    if is_pump:
        curve_supply_raw = struct.unpack_from('<Q', curve_raw, 40)[0]
        pump_supply_result = check_pump_supply(
            mint_supply_raw,
            coin.decimals if coin.decimals is not None else -1,
            curve_supply_raw,
        )
        packet = dict(coin.packet or {})
        packet['pump_mint_supply_raw'] = mint_supply_raw
        packet['pump_standard_supply_valid'] = pump_supply_result.passed
        coin.packet = packet
        result = result.merge(pump_supply_result)
    else:
        result = result.merge(
            check_supply(
                mint_supply_raw,
                coin.decimals if coin.decimals is not None else -1,
            )
        )

    if is_pump:
        tasks.append(asyncio.create_task(
            check_pump_curve(
                session, http_rpc_url, coin.mint, coin.curve,
                coin.packet, curve_raw=curve_raw,
            )
        ))

    probe_task = None
    if is_pump and creator_probe is not None and coin.program_creator:
        # The decision-time probe runs concurrently with the remaining candidate
        # checks. Both portfolios receive this same bounded result and snapshot.
        probe_task = asyncio.create_task(creator_probe.probe(coin))
    gathered = await asyncio.gather(
        *tasks, *([probe_task] if probe_task is not None else []),
        return_exceptions=True,
    )
    check_results = gathered[:len(tasks)]
    for verdict in check_results:
        if isinstance(verdict, Exception):
            result = result.merge(FilterResult(False, [f'проверка упала: {type(verdict).__name__}']))
        else:
            result = result.merge(verdict)
    if probe_task is not None:
        probe_result = gathered[-1]
        if isinstance(probe_result, BaseException):
            coin.creator_probe_result = {
                'status': 'unavailable', 'errors': [type(probe_result).__name__],
                'signature_count': None, 'ata_exists': None, 'token_balance_raw': None,
            }
        else:
            coin.creator_probe_result = probe_result
    return result
