"""
detector.py — детектор новых монет.

Слушает события WebSocket (logsNotification), распознаёт транзакции
СОЗДАНИЯ монет/пулов по логам и извлекает Mint Address (CA).

Распознавание — по строкам вида "Program log: Instruction: <Create|Initialize...>"
в логах транзакции конкретной программы.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import random
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import websockets

from console import Color, log, short
from models import NewCoin
from pumpportal_validation import PUMP_PROGRAM_ID, b58decode, validate_pumpportal_create
from telemetry import get_collector

PUMPPORTAL_WS = 'wss://pumpportal.fun/ws'

# Программы, которые слушаем: program_id -> человекочитаемое имя
PROGRAMS: Dict[str, str] = {
    PUMP_PROGRAM_ID: 'Pump.fun',
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': 'Raydium AMM v4',
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C': 'Raydium CPMM',
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo': 'Meteora DLMM',
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB': 'Meteora DAMM v1',
    'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN': 'Meteora DBC',
}

# Какие логи означают «создали монету/пул» для каждой программы
CREATION_PATTERNS: Dict[str, List[re.Pattern]] = {
    'Pump.fun': [re.compile(r'Instruction: Create(V2)?\b')],
    'Raydium AMM v4': [re.compile(r'Instruction: Initialize2\b')],
    'Raydium CPMM': [
        re.compile(r'Instruction: Initialize\b'),
        re.compile(r'Instruction: CreatePool\b'),
    ],
    'Meteora DLMM': [
        re.compile(r'Instruction: InitializeLbPair'),
        re.compile(r'Instruction: InitializeCustomizablePermissionlessLbPair'),
        re.compile(r'Instruction: InitializePermissionLbPair'),
    ],
    'Meteora DAMM v1': [re.compile(r'Instruction: Initialize\b')],
    'Meteora DBC': [re.compile(r'Instruction: InitializeVirtualPool')],
}

WSOL = 'So11111111111111111111111111111111111111112'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
USDT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
QUOTE_MINTS: Set[str] = {WSOL, USDC, USDT}

# Известные программы/системные адреса — они точно НЕ являются монетами
KNOWN_PROGRAMS: Set[str] = {
    '11111111111111111111111111111111',
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
    'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr',
    'ComputeBudget111111111111111111111111111111',
    'SysvarRent111111111111111111111111111111111',
    'SysvarC1ock11111111111111111111111111111111',
    'Ed25519SigVerify111111111111111111111111111',
    'KeccakSecp256k11111111111111111111111111111',
    'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s',
    PUMP_PROGRAM_ID,
    'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',
    'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN',
} | QUOTE_MINTS

# Память дедупликации: уже видели эти транзакции и монеты
_seen_signatures: Set[str] = set()
_seen_mints: Set[str] = set()

# Фоновые задачи обработки созданий. Почему не по одной: если одна транзакция
# мучается с ретраями (RPC отдаёт mint не сразу), остальные НЕ должны ждать
# в очереди — иначе события копятся и приходят «завалом» с опозданием.
_creation_tasks: Set['asyncio.Task[Any]'] = set()


# Короткие идентификаторы для настройки SOURCES (какие площадки слушать):
#   pumpfun, raydium-v4, raydium-cpmm, meteora-dlmm, meteora-damm, meteora-dbc
SOURCE_IDS: Dict[str, str] = {
    'pumpfun': PUMP_PROGRAM_ID,
    'raydium-v4': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    'raydium-cpmm': 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
    'meteora-dlmm': 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
    'meteora-damm': 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',
    'meteora-dbc': 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN',
}


def select_programs(sources: str) -> Dict[str, str]:
    # ПЛАН:
    # 1. 'all' или пусто — слушать все площадки
    # 2. Иначе разобрать список коротких id через запятую (пробелы не мешают)
    # 3. Неизвестные id — честно предупредить
    # 4. Если не выбрано ни одной — выйти с понятной ошибкой
    # 5. Вернуть {program_id: имя} только для выбранных площадок
    if not sources or sources.strip().lower() == 'all':
        return dict(PROGRAMS)

    selected: Dict[str, str] = {}
    for raw_id in sources.split(','):
        source_id = raw_id.strip().lower()
        if not source_id:
            continue
        program_id = SOURCE_IDS.get(source_id)
        if program_id is None:
            print(f'⚠️ Неизвестный источник {source_id!r} — пропускаем')
            continue
        selected[program_id] = PROGRAMS[program_id]

    if not selected:
        known = ', '.join(SOURCE_IDS)
        print(f'❌ SOURCES не содержит ни одного известного источника.\n   Доступные: {known}')
        sys.exit(1)
    return selected


# ПЛАН:
# 1. select_programs()           — какие площадки слушаем (настройка SOURCES)
# 2. _spawn()                    — фоновая задача с удержанием ссылки
# 3. _process_creation()         — медленная часть: достать mint, положить в очередь
# 4. is_creation_log()           — логи похожи на создание монеты?
# 5. mint_from_token_balances()  — достать mint (+владельца) из балансов транзакции
# 6. mint_from_account_scan()    — запасной поиск: проверить аккаунты транзакции
# 7. fetch_mint_from_signature() — скачать транзакцию и разобрать mint
# 8. detector_worker()           — воркер: очередь событий -> очередь монет

def _spawn(coro) -> None:
    # ПЛАН:
    # 1. Создать задачу из корутины
    # 2. Сохранить ссылку (иначе сборщик мусора может её «съесть»)
    # 3. По завершении убрать задачу из набора
    task = asyncio.create_task(coro)
    _creation_tasks.add(task)
    task.add_done_callback(_creation_tasks.discard)


async def _process_creation(
    source: str,
    signature: str,
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    coins_queue: 'asyncio.Queue[NewCoin]',
) -> None:
    # ПЛАН:
    # 1. Достать mint из транзакции (с ретраями — это медленно, НЕ блокируем других!)
    # 2. Если не вышло — честно сказать и завершиться
    # 3. Пропустить дубликат монеты
    # 4. Запомнить кривую Pump.fun и положить NewCoin в очередь монет
    found = await fetch_mint_from_signature(session, http_rpc_url, signature)
    if not found:
        log('   не удалось извлечь mint — ждём следующую монету', Color.DIM)
        return

    mint, owner, creator = found
    if mint in _seen_mints:
        return
    _seen_mints.add(mint)

    # У Pump.fun владелец токен-аккаунта из create-транзакции —
    # это бондинг-кривая (нужна для мгновенного он-чейн прайса).
    # Первого подписанта сохраняем отдельно как инициатора транзакции;
    # для Helius-пути он не всегда равен создателю токена.
    curve = owner if source == 'Pump.fun' else None
    packet = {
        'creator_identity_type': 'solana_transaction_fee_payer' if creator else 'unknown',
        'creator_identity_source': 'first_transaction_signer' if creator else None,
        'creator_identity_confidence': 'medium' if creator else 'none',
        'tx_payer_source': 'first_transaction_signer' if creator else None,
        'tx_payer': creator,
        'program_creator': None,
    }
    coin = NewCoin(
        mint=mint, source=source, signature=signature, curve=curve,
        creator=creator, tx_payer=creator, packet=packet,
    )
    log(f'👁️ Новая монета: {mint} ({source})', Color.MAGENTA + Color.BOLD)
    await coins_queue.put(coin)


def is_creation_log(source: str, logs: List[str]) -> bool:
    # ПЛАН:
    # 1. Взять список шаблонов для этой программы
    # 2. Проверить, есть ли совпадение хотя бы в одной строке лога
    patterns = CREATION_PATTERNS.get(source) or []
    return any(pattern.search(line) for line in logs for pattern in patterns)


def mint_from_token_balances(
    tx: Dict[str, Any],
) -> Optional[Tuple[str, Optional[str]]]:
    # ПЛАН:
    # 1. Собрать токен-балансы (post + pre) из меты транзакции
    # 2. Отбросить котируемые монеты (SOL/USDC/USDT)
    # 3. Для каждой монеты запомнить максимальный «сырой» баланс и владельца
    # 4. Вернуть монету с наибольшим балансом и её владельца
    #    (для Pump.fun владелец = бондинг-кривая — пригодится для цены)
    meta = tx.get('meta') or {}
    amounts: Dict[str, Tuple[int, str]] = {}
    balances = (meta.get('postTokenBalances') or []) + (meta.get('preTokenBalances') or [])
    for bal in balances:
        mint = bal.get('mint')
        if not mint or mint in QUOTE_MINTS:
            continue
        try:
            amount = int(((bal.get('uiTokenAmount') or {}).get('amount')) or 0)
        except (TypeError, ValueError):
            amount = 0
        owner = bal.get('owner') or ''
        if amount >= amounts.get(mint, (0, ''))[0]:
            amounts[mint] = (amount, owner)
    if not amounts:
        return None
    best_mint, (_, best_owner) = max(amounts.items(), key=lambda kv: kv[1][0])
    return best_mint, (best_owner or None)


def transaction_fee_payer(tx: Dict[str, Any]) -> Optional[str]:
    """Extract the transaction's first signer as an attributed launch actor.

    For non-PumpPortal sources this is only the transaction fee payer/initiator;
    it is not guaranteed to be the token's original creator. The identity type
    is preserved so this evidence is not treated like a direct PumpPortal creator.
    """
    transaction = tx.get('transaction') or {}
    message = transaction.get('message') or {}
    keys = message.get('accountKeys') or []
    if not isinstance(keys, list) or not keys:
        return None

    signer_keys = []
    for key in keys:
        if isinstance(key, dict) and key.get('signer') is True:
            signer_keys.append(key.get('pubkey'))
    if signer_keys:
        candidate = signer_keys[0]
    else:
        # Legacy/non-jsonParsed responses may expose strings plus a header.
        header = message.get('header') or {}
        try:
            required = int(header.get('numRequiredSignatures') or 0)
        except (TypeError, ValueError):
            required = 0
        first_signer = keys[:max(required, 1)]
        candidate = None
        for key in first_signer:
            value = key.get('pubkey') if isinstance(key, dict) else key
            if isinstance(value, str) and value:
                candidate = value
                break

    decoded = b58decode(candidate) if isinstance(candidate, str) else None
    return candidate if decoded is not None and len(decoded) == 32 else None


async def mint_from_account_scan(
    session: aiohttp.ClientSession, http_rpc_url: str, tx: Dict[str, Any]
) -> Optional[Tuple[str, Optional[str]]]:
    # ПЛАН:
    # 1. Достать ключи аккаунтов транзакции, отбросить известные программы
    # 2. Пакетно проверить оставшиеся (getMultipleAccounts, jsonParsed)
    # 3. Оставить настоящие mint-аккаунты (parsed.type == 'mint')
    # 4. Вернуть первый не-котируемый (или единственный)
    message = (tx.get('transaction') or {}).get('message') or {}
    keys = [
        k.get('pubkey') if isinstance(k, dict) else k
        for k in (message.get('accountKeys') or [])
    ]
    candidates = [k for k in keys if k and k not in KNOWN_PROGRAMS and len(k) >= 32]
    if not candidates:
        return None

    payload = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'getMultipleAccounts',
        'params': [candidates[:30], {'encoding': 'jsonParsed'}],
    }
    try:
        async with session.post(
            http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
    except Exception:
        return None

    mints: List[str] = []
    for item in (data.get('result') or {}).get('value') or []:
        if not isinstance(item, dict):
            continue
        account_data = item.get('data')
        if not isinstance(account_data, dict):
            # getMultipleAccounts может вернуть base64 как [data, encoding].
            # Такой аккаунт не разобран в jsonParsed — пропускаем без падения.
            continue
        parsed = account_data.get('parsed')
        if not isinstance(parsed, dict):
            continue
        if parsed.get('type') == 'mint':
            pubkey = item.get('pubkey') or ''
            if pubkey:
                mints.append(pubkey)
    if not mints:
        return None
    non_quote = [m for m in mints if m not in QUOTE_MINTS]
    return (non_quote or mints)[0], None


async def fetch_mint_from_signature(
    session: aiohttp.ClientSession, http_rpc_url: str, signature: str
) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    # ПЛАН:
    # 1. До 5 попыток скачать транзакцию (getTransaction, jsonParsed)
    # 2. При 429 (rate limit) — подождать дольше обычного
    # 3. Разобрать mint: сначала из балансов, потом сканом аккаунтов
    # 4. Сохранить первый signer как wallet-инициатор (не всегда creator)
    # 5. Если транзакция есть, но mint не найден — не ретраить, вернуть None
    for attempt in range(5):
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'getTransaction',
            'params': [
                signature,
                {
                    'encoding': 'jsonParsed',
                    'maxSupportedTransactionVersion': 0,
                    'commitment': 'confirmed',
                },
            ],
        }
        tx = None
        try:
            async with session.post(
                http_rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(3.0 * (attempt + 1))
                    continue
                data = await resp.json()
            tx = data.get('result')
        except Exception:
            tx = None

        if tx:
            creator = transaction_fee_payer(tx)
            found = mint_from_token_balances(tx)
            if not found:
                found = await mint_from_account_scan(session, http_rpc_url, tx)
            if found:
                return found[0], found[1], creator
            return None

        await asyncio.sleep(1.5 * (attempt + 1) + random.uniform(0, 0.5))
    return None


async def detector_worker(
    events_queue: 'asyncio.Queue[Tuple[str, Dict[str, Any]]]',
    coins_queue: 'asyncio.Queue[NewCoin]',
    session: aiohttp.ClientSession,
    http_rpc_url: str,
    watch_stop: asyncio.Event,
) -> None:
    # ПЛАН:
    # 1. Бесконечно брать события из очереди (get() — СТРОГО вне try)
    # 2. Если слушать больше не нужно (watch_stop) — тихо пропускать
    #    (не тратим кредиты RPC на ненужные уже монеты)
    # 3. Проверить, что это транзакция создания (по логам)
    # 4. Пропустить дубликаты (по подписи транзакции)
    # 5. Медленную работу (достать mint) отдать фоновой задаче
    # 6. ВСЕГДА отмечать событие обработанным (task_done() — в finally)
    while True:
        source, result = await events_queue.get()
        try:
            if watch_stop.is_set():
                continue  # цель уже выбрана — новые монеты не нужны

            if result.get('err') is not None:
                continue  # упавшие транзакции — не создание

            signature = result.get('signature') or ''
            logs: List[str] = result.get('logs') or []
            if not is_creation_log(source, logs):
                continue

            collector = get_collector()
            if collector is not None:
                collector.record_source_event(
                    source, 'program_creation_log', None, signature,
                    {'signature': signature, 'logs': logs[:80], 'err': result.get('err')},
                )

            if signature in _seen_signatures:
                continue
            _seen_signatures.add(signature)
            if len(_seen_signatures) > 50_000:
                _seen_signatures.clear()

            log(f'🔎 Создание монеты/пула! {source} | tx {short(signature, 8)}')
            _spawn(_process_creation(source, signature, session, http_rpc_url, coins_queue))
        finally:
            events_queue.task_done()


# ─────────────────────────────────────────────────────────────
# БЫСТРЫЙ ПУТЬ: Pumpportal WSS (Pump.fun)
# Mint, создатель, адрес кривой и СТАРТОВЫЕ РЕЗЕРВЫ приходят
# прямо в WSS-пакете. Ни одного HTTP-запроса (никаких 429).
# ─────────────────────────────────────────────────────────────

async def probe_pumpportal(timeout: float = 6.0) -> bool:
    # ПЛАН:
    # 1. Подключиться и подписаться на создания
    # 2. Дождаться подтверждения — значит, сервис жив
    # 3. Отключиться; вернуть True/False
    try:
        async with websockets.connect(
            PUMPPORTAL_WS, open_timeout=8
        ) as ws:
            await ws.send(json.dumps({'method': 'subscribeNewToken'}))
            async with asyncio.timeout(timeout):
                async for raw in ws:
                    if 'successfully subscribed' in str(raw).lower():
                        return True
    except Exception:
        return False
    return False


def lamports_or_none(value: Any) -> Optional[int]:
    """Safe parse of a PumpPortal base-unit integer (lamports / raw tokens).

    Telemetry only: the value comes in the same create message, so this costs
    no RPC. Absent, empty, non-finite or malformed values become None instead
    of raising inside the hot listener path.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            number = int(text, 10)
        elif isinstance(value, float):
            if not math.isfinite(value):
                return None
            number = int(round(value))
        else:
            number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


async def on_transaction_signal(
    raw: Dict[str, Any],
    received_at: Optional[float] = None,
    validation: Optional[Tuple[bool, str, Dict[str, Any]]] = None,
) -> Optional[NewCoin]:
    # Fast local checks before a PumpPortal event can enter trading filters:
    # mint must match the deterministic Pump.fun bonding-curve PDA. This rejects
    # create_v2 payloads that label an old quote mint as the newly-created mint.
    valid, _reason, evidence = validation or validate_pumpportal_create(raw)
    if not valid:
        return None
    mint = raw.get('mint') or ''
    if not mint or mint in _seen_mints:
        return None

    v_tokens = float(evidence['v_tokens'])
    v_sol = float(evidence['v_sol'])
    try:
        market_cap_sol = float(raw.get('marketCapSol') or 0.0)
        if not math.isfinite(market_cap_sol):
            market_cap_sol = 0.0
    except (TypeError, ValueError, OverflowError):
        market_cap_sol = 0.0
    _seen_mints.add(mint)

    creator_value = raw.get('traderPublicKey')
    creator_bytes = b58decode(creator_value) if isinstance(creator_value, str) else None
    creator = creator_value if creator_bytes is not None and len(creator_bytes) == 32 else None
    packet = {
        'v_tokens': v_tokens,
        'v_sol': v_sol,
        'packet_price_sol': v_sol / v_tokens,
        'market_cap_sol': market_cap_sol,
        'name': raw.get('name') or '',
        'symbol': raw.get('symbol') or '',
        'pumpportal_identity_verified': True,
        'creator_identity_type': 'pumpportal_event_actor' if creator else 'unknown',
        'creator_identity_source': 'pumpportal_traderPublicKey' if creator else None,
        'creator_identity_confidence': 'medium' if creator else 'none',
        'tx_payer': None,
        'tx_payer_source': None,
        'pumpportal_event_actor': creator,
        'program_creator': None,
        'derived_bonding_curve': evidence.get('expected_curve'),
        # Dev's own buy, straight from the create message (no extra RPC).
        # `initialBuy` is raw token base units, `solAmount` is lamports;
        # both are telemetry for the 4-12 discriminator experiment.
        'initial_buy_lamports': lamports_or_none(raw.get('initialBuy')),
        'sol_amount_lamports': lamports_or_none(raw.get('solAmount')),
    }
    if evidence.get('explicit_quote_mint'):
        packet['quote_mint'] = evidence['explicit_quote_mint']
    return NewCoin(
        mint=mint,
        source='Pump.fun',
        signature=raw.get('signature') or '',
        curve=raw.get('bondingCurveKey') or None,
        creator=creator,
        packet=packet,
        found_at=received_at if received_at is not None else time.time(),
        tx_payer=None,
    )


async def pumpportal_listener(
    coins_queue: 'asyncio.Queue[NewCoin]',
    watch_stop: asyncio.Event,
) -> None:
    # ПЛАН:
    # 1. Бесконечный цикл с переподключением (backoff)
    # 2. Подключиться к Pumportal WSS, подписаться на создания
    # 3. Каждое событие мгновенно разбирать (on_transaction_signal)
    # 4. Новую монету класть в очередь — БЕЗ HTTP-запросов
    # 5. Если слушать больше не нужно (watch_stop) — выйти
    backoff = 1.0
    while True:
        if watch_stop.is_set():
            return
        try:
            async with websockets.connect(
                PUMPPORTAL_WS,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=20,
                max_size=2 ** 23,
            ) as ws:
                await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                log(
                    '🟣 Pumportal: поток созданий Pump.fun подключён '
                    '(mint прямо из WSS-пакета, без getTransaction)',
                    Color.MAGENTA,
                )
                backoff = 1.0

                async for raw in ws:
                    if watch_stop.is_set():
                        return
                    received_at = time.time()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if 'mint' not in msg:
                        continue  # подтверждение подписки и т.п.
                    if msg.get('txType') != 'create':
                        continue
                    collector = get_collector()
                    if collector is not None:
                        # Keep the untouched source payload even when validation rejects it.
                        collector.record_source_event(
                            'Pump.fun', 'pumpportal_create', msg.get('mint'),
                            msg.get('signature'), msg,
                        )

                    validation = validate_pumpportal_create(msg)
                    if not validation[0]:
                        invalid_mint = msg.get('mint')
                        if collector is not None and isinstance(invalid_mint, str) and invalid_mint:
                            rejected_candidate = NewCoin(
                                mint=invalid_mint,
                                source='Pump.fun',
                                signature=msg.get('signature') or '',
                                curve=msg.get('bondingCurveKey') or None,
                                creator=msg.get('traderPublicKey'),
                                packet={
                                    'name': msg.get('name') or '',
                                    'symbol': msg.get('symbol') or '',
                                    'raw_create_event': msg,
                                    'validation_evidence': validation[2],
                                },
                                found_at=received_at,
                            )
                            collector.record_candidate(
                                rejected_candidate,
                                {
                                    'paper_only': True,
                                    'candidate_status': 'rejected_before_trading',
                                    'validation_reason': validation[1],
                                },
                            )
                            collector.record_decision(
                                invalid_mint, 'Pump.fun', 'pumpportal_identity_validation',
                                'rejected', [validation[1]], validation[2],
                            )
                        log(
                            f'⛔ PumpPortal create отклонён: {short(str(msg.get("mint", "?")))} '
                            f'— {validation[1]} '
                            f'(curve={validation[2].get("provided_curve")}, '
                            f'expected={validation[2].get("expected_curve")})',
                            Color.YELLOW,
                        )
                        continue

                    coin = await on_transaction_signal(
                        msg, received_at=received_at, validation=validation
                    )
                    if coin is None:
                        continue
                    log(
                        f'👁️ Новая монета: {coin.mint} (Pump.fun | '
                        f'{coin.packet.get("symbol", "")})',
                        Color.MAGENTA + Color.BOLD,
                    )
                    await coins_queue.put(coin)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f'❌ Pumportal: {exc}', Color.RED)
            log(f'↻ Переподключение через {backoff:.0f} с…', Color.YELLOW)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
