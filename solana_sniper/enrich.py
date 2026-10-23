"""Passive token-intelligence collection for run #7 (P0: zero RPC).

Everything the bot knew at entry time — birth, delta, dev buy, market cap,
creator wallet history, name — has been combed through and no filter survived
the ladder test. The next class of features lives upstream of the chain:
holder concentration, organic score, how many tokens the dev has shipped.

`datapi.jup.ag` is plain HTTPS, so this module never touches the Helius quota
and never blocks a decision:

* a mint is enqueued right after it passes the security filters;
* a background task batches up to 7 mints per request (the upstream limit);
* one attempt, 2 s timeout, any failure becomes a row
  ``status=enrich_unavailable`` instead of an exception;
* the output is ``exports/enrich.csv``, read by the report only.

No entry/exit rule reads a single field from here. That is deliberate: run #7
must first *collect* these features, and only a later run may test them.
"""
from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from console import Color, log

ENRICH_URL = 'https://datapi.jup.ag/v1/assets/search'
ENRICH_BATCH = 7                 # upstream limit per query
ENRICH_TIMEOUT_SEC = 2.0
ENRICH_FLUSH_SEC = 2.0

ENRICH_FIELDS: tuple[str, ...] = (
    'mint', 'fetched_at_utc', 'status',
    'top_holders_pct', 'holder_count', 'organic_score',
    'dev_mints', 'dev_migrations', 'dev_funded_at',
    'num_buys_1h', 'num_sells_1h', 'num_traders_1h',
    'buy_volume_1h', 'sell_volume_1h', 'buy_organic_volume_1h',
    'twitter', 'twitter_followers',
    'error',
)


def _utc(epoch: float) -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(epoch))


def _first(payload: Any, *keys: str) -> Any:
    """First present key of a nested dict, in the order given."""
    for key in keys:
        current = payload
        for part in key.split('.'):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current is not None:
            return current
    return None


def parse_asset(asset: dict[str, Any]) -> dict[str, Any]:
    """Flatten one upstream asset into the flat row of ``enrich.csv``."""
    stats = asset.get('stats1h') or {}
    return {
        'top_holders_pct': _first(asset, 'topHoldersPercentage', 'audit.topHoldersPercentage'),
        'holder_count': _first(asset, 'holderCount', 'audit.holderCount'),
        'organic_score': _first(asset, 'organicScore', 'audit.organicScore'),
        'dev_mints': _first(asset, 'devMints', 'audit.devMints'),
        'dev_migrations': _first(asset, 'devMigrations', 'audit.devMigrations'),
        'dev_funded_at': _first(asset, 'devFundedAt', 'audit.devFundedAt'),
        'num_buys_1h': _first(stats, 'numBuys') if isinstance(stats, dict) else None,
        'num_sells_1h': _first(stats, 'numSells') if isinstance(stats, dict) else None,
        'num_traders_1h': _first(stats, 'numTraders') if isinstance(stats, dict) else None,
        'buy_volume_1h': _first(stats, 'buyVolume') if isinstance(stats, dict) else None,
        'sell_volume_1h': _first(stats, 'sellVolume') if isinstance(stats, dict) else None,
        'buy_organic_volume_1h': (
            _first(stats, 'buyOrganicVolume') if isinstance(stats, dict) else None
        ),
        'twitter': _first(asset, 'twitter', 'primaryTwitter.handle'),
        'twitter_followers': _first(asset, 'primaryTwitter.followerCount', 'twitterFollowers'),
    }


@dataclass
class EnrichCollector:
    """Buffer + one background task; never raises into the trading path."""

    session: aiohttp.ClientSession
    enabled: bool = True
    batch_size: int = ENRICH_BATCH
    flush_sec: float = ENRICH_FLUSH_SEC
    queue: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    ok_count: int = 0
    unavailable_count: int = 0
    _seen: set[str] = field(default_factory=set)
    _task: Any = None
    _stopping: bool = False

    def enqueue(self, mint: str) -> bool:
        """Queue one mint; returns False when it was already collected."""
        if not self.enabled or not mint or mint in self._seen:
            return False
        self._seen.add(mint)
        self.queue.append(mint)
        return True

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        import asyncio

        self._task = asyncio.create_task(self._run(), name='enrich-collector')

    async def _run(self) -> None:
        import asyncio

        while True:
            try:
                await asyncio.sleep(self.flush_sec)
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - passive collector
                log(f'⚠️ enrich: {type(exc).__name__}', Color.YELLOW)

    async def flush(self) -> None:
        """Send every buffered batch. Never raises on a network problem."""
        import asyncio

        while self.queue:
            batch = self.queue[:self.batch_size]
            del self.queue[:len(batch)]
            assets: dict[str, dict[str, Any]] = {}
            error: Optional[str] = None
            try:
                async with asyncio.timeout(ENRICH_TIMEOUT_SEC):
                    async with self.session.get(
                        ENRICH_URL,
                        params={'query': ','.join(batch)},
                        timeout=aiohttp.ClientTimeout(total=ENRICH_TIMEOUT_SEC),
                    ) as resp:
                        if resp.status != 200:
                            error = f'http_{resp.status}'
                        else:
                            payload = await resp.json()
                            assets = _index_by_mint(payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                error = f'exception_{type(exc).__name__}'
            fetched_at = _utc(time.time())
            for mint in batch:
                asset = assets.get(mint)
                if asset is None:
                    self.unavailable_count += 1
                    self.rows.append({
                        'mint': mint, 'fetched_at_utc': fetched_at,
                        'status': 'enrich_unavailable', 'error': error or 'not_in_response',
                    })
                    continue
                self.ok_count += 1
                self.rows.append({
                    'mint': mint, 'fetched_at_utc': fetched_at,
                    'status': 'ok', 'error': None, **parse_asset(asset),
                })

    async def close(self) -> None:
        import asyncio

        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        try:
            await asyncio.wait_for(self.flush(), timeout=5.0)
        except Exception:  # noqa: BLE001 - a missing artefact must not fail the run
            pass

    def stats(self) -> dict[str, Any]:
        return {
            'enabled': bool(self.enabled),
            'rows': len(self.rows),
            'ok': self.ok_count,
            'unavailable': self.unavailable_count,
            'observed_share': (
                round(self.ok_count / len(self.rows), 4) if self.rows else None
            ),
        }

    def write_csv(self, path: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=ENRICH_FIELDS, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.rows)


def _index_by_mint(payload: Any) -> dict[str, dict[str, Any]]:
    """Upstream returns a list (or a dict with a list) of assets."""
    items: Any = payload
    if isinstance(payload, dict):
        for key in ('data', 'assets', 'items', 'result'):
            if isinstance(payload.get(key), list):
                items = payload[key]
                break
    if not isinstance(items, list):
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        mint = item.get('id') or item.get('address') or item.get('mint')
        if isinstance(mint, str) and mint:
            indexed[mint] = item
    return indexed


def enrich_row_is_empty(row: dict[str, Any]) -> bool:
    """True when every feature cell is empty (used by the report)."""
    return all(
        row.get(name) in (None, '') for name in ENRICH_FIELDS
        if name not in ('mint', 'fetched_at_utc', 'status', 'error')
    )


def rows_to_json(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, ensure_ascii=False, default=str)
