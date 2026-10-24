"""
entry_reads.py — один `getMultipleAccounts` на кандидата за цикл опроса.

Зачем это здесь. В прогоне 10a `getMultipleAccounts` уходило 6 565 запросов за
3 611 с (1.817 rps). Из них 3 094 — это два чтения одного и того же набора
аккаунтов `[mint, curve, Global, FeeConfig]` на одного кандидата: секундный
снимок безопасности в `filters.run_all_checks` и наблюдение «+0 с» в
телеметрии, которые идут в один и тот же момент (цель наблюдения — это
`coin.found_at`, то есть момент обнаружения).

Правило кэша:

* ключ — mint (у пути без mint — адрес кривой), значение — ответ батча
  `[mint, curve, Global, FeeConfig]` в base64;
* запись живёт один цикл опроса (`ENTRY_CACHE_WINDOW_SEC`, по умолчанию
  `MARK_POLL_SEC = 0.25` с) и удаляется по истечении — на диск ничего не
  пишется;
* одновременные читатели одного mint ждут один и тот же запрос
  (single-flight), а не дублируют его;
* читатели, которые обязаны видеть свежие данные (опрос открытых позиций),
  идут своим батчем мимо кэша — их исполнение выхода не меняется.

Ни одно торговое решение не читает отсюда ничего, кроме снимка, которым оно
и раньше пользовалось: меняется только то, что снимок в пределах одного цикла
берётся один раз.
"""

from __future__ import annotations

import asyncio
import struct
import time
from typing import Any, Optional

from console import log
from price import rpc_get_multiple_accounts
from pump_curve_quote import (
    PUMP_FEE_CONFIG_PDA,
    PUMP_GLOBAL_PDA,
    decode_account_data,
)
from pumpportal_validation import b58encode

# One poll cycle. Run #10b §1: the entry read and the "+0 s" observation of the
# same candidate happen within milliseconds of each other, so a window of one
# cycle merges them into a single batch without ever serving an entry a price
# older than the mark stream a 5-second exit already lives with.
ENTRY_CACHE_WINDOW_SEC = 0.25


def mint_account_info(account: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Decode an SPL mint account into the shape `jsonParsed` used to return.

    Run #10b §1 replaces the `jsonParsed` batch with a base64 one, because the
    curve, Global and FeeConfig were already read as base64 and the same batch
    can only have one encoding. The fields are identical to what the filters
    and the packet used to read: `mintAuthority`, `freezeAuthority`,
    `decimals`, `supply`, `isInitialized`.
    """
    raw = decode_account_data(account)
    if not raw or len(raw) < 82:
        return None
    try:
        # SPL Mint: COption<Pubkey> 0..36, supply u64 at 36, decimals at 44,
        # is_initialized at 45, COption<Pubkey> freeze authority at 46..82.
        mint_authority_tag = struct.unpack_from('<I', raw, 0)[0]
        supply = struct.unpack_from('<Q', raw, 36)[0]
        decimals = raw[44]
        is_initialized = bool(raw[45])
        freeze_authority_tag = struct.unpack_from('<I', raw, 46)[0]
    except struct.error:
        return None
    authority = raw[4:36]
    freeze = raw[50:82]
    return {
        'mintAuthority': (
            b58encode(authority) if mint_authority_tag and any(authority) else None
        ),
        'freezeAuthority': (
            b58encode(freeze) if freeze_authority_tag and any(freeze) else None
        ),
        'decimals': int(decimals),
        'supply': str(supply),
        'isInitialized': is_initialized,
    }


def mint_curve_addresses(
    mint: Optional[str], curve: Optional[str],
) -> list[str]:
    """The four accounts one entry read is made of, in a stable order."""
    addresses: list[str] = []
    for address in (mint, curve, PUMP_GLOBAL_PDA, PUMP_FEE_CONFIG_PDA):
        if address and address not in addresses:
            addresses.append(address)
    return addresses


class EntryAccountCache:
    """In-memory, single-cycle cache of the four-account entry batch."""

    def __init__(self, window_sec: float = ENTRY_CACHE_WINDOW_SEC) -> None:
        self.window_sec = max(0.0, float(window_sec))
        self._entries: dict[str, tuple[float, dict[str, Optional[dict[str, Any]]],
                                       Optional[int]]] = {}
        self._inflight: dict[str, asyncio.Future] = {}
        # Counters. `reads` is the number of batches this cache actually sent;
        # `asked` is the number of distinct keys (mints) that wanted one.
        self.reads = 0
        self.hits = 0
        self.asked: set[str] = set()
        self.reads_by_source: dict[str, int] = {}
        self.last_cycle_epoch = 0.0

    # --- lifecycle ---------------------------------------------------------
    def reset(self) -> None:
        self._entries.clear()
        self._inflight.clear()
        self.reads = 0
        self.hits = 0
        self.asked = set()
        self.reads_by_source = {}
        self.last_cycle_epoch = 0.0

    def begin_cycle(self, now: Optional[float] = None) -> None:
        """Drop every entry — a new poll cycle must not see the old snapshot."""
        self._entries.clear()
        self.last_cycle_epoch = time.time() if now is None else float(now)

    def _purge(self, now: float) -> None:
        if not self._entries or self.window_sec <= 0:
            if self.window_sec <= 0:
                self._entries.clear()
            return
        expired = [
            key for key, (at, _accounts, _slot) in self._entries.items()
            if now - at > self.window_sec
        ]
        for key in expired:
            self._entries.pop(key, None)

    # --- reads -------------------------------------------------------------
    async def accounts(
        self,
        session: Any,
        http_rpc_url: str,
        key: str,
        mint: Optional[str],
        curve: Optional[str],
        *,
        source: str = 'entry',
    ) -> tuple[dict[str, Optional[dict[str, Any]]], Optional[int]]:
        """One batch for `key`, served from this cycle's cache when possible."""
        now = time.time()
        self._purge(now)
        self.asked.add(str(key))
        cached = self._entries.get(key)
        if cached is not None:
            self.hits += 1
            return cached[1], cached[2]
        pending = self._inflight.get(key)
        if pending is not None and not pending.done():
            # Same key, same cycle: wait for the request already in flight
            # instead of paying for a second one.
            self.hits += 1
            accounts, slot = await asyncio.shield(pending)
            return accounts, slot
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._inflight[key] = future
        addresses = mint_curve_addresses(mint, curve)
        try:
            accounts, slot = await rpc_get_multiple_accounts(
                session, http_rpc_url, addresses,
                encoding='base64', commitment='processed',
                retry_null=True, source=source,
            )
        except asyncio.CancelledError:
            self._inflight.pop(key, None)
            if not future.done():
                future.cancel()
            raise
        except Exception:
            self._inflight.pop(key, None)
            if not future.done():
                future.set_result(({}, None))
            raise
        self._inflight.pop(key, None)
        self.reads += 1
        self.reads_by_source[source] = self.reads_by_source.get(source, 0) + 1
        self._entries[key] = (time.time(), accounts, slot)
        if not future.done():
            future.set_result((accounts, slot))
        return accounts, slot

    # --- reporting ---------------------------------------------------------
    def stats(self) -> dict[str, Any]:
        issued = self.reads
        candidates = len(self.asked)
        return {
            'window_sec': round(self.window_sec, 4),
            'entry_reads_total': issued,
            'entry_reads_served_from_cache': self.hits,
            'entry_candidates': candidates,
            'entry_reads_per_candidate': (
                round(issued / candidates, 3) if candidates else None
            ),
            'entry_reads_by_source': dict(sorted(self.reads_by_source.items())),
            'persisted': False,
        }


_cache: EntryAccountCache = EntryAccountCache()


def entry_account_cache() -> EntryAccountCache:
    return _cache


def reset_entry_account_cache(window_sec: float = ENTRY_CACHE_WINDOW_SEC) -> EntryAccountCache:
    """A fresh cache for a new run (or a test)."""
    global _cache
    _cache = EntryAccountCache(window_sec)
    return _cache


def begin_entry_cycle(now: Optional[float] = None) -> None:
    """Called at the top of every mark-poll cycle: no entry survives it."""
    _cache.begin_cycle(now)


def merge_entry_stats(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Fold the cache counters into an `rpc_metrics` snapshot (run #10b §1)."""
    merged = dict(snapshot)
    merged.update(_cache.stats())
    return merged


def log_entry_reads() -> None:
    stats = _cache.stats()
    per_candidate = stats.get('entry_reads_per_candidate')
    if per_candidate is None:
        return
    log(
        f'📉 Входных чтений на кандидата: {per_candidate:.2f} '
        f'({stats["entry_reads_total"]} батчей на {stats["entry_candidates"]} '
        f'кандидатов, из кэша {stats["entry_reads_served_from_cache"]})',
    )
