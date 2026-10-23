"""Passive, paper-only candidate/quote telemetry. Never sends transactions."""
from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import aiosqlite

from console import Color, log
from models import NewCoin, SwapQuote
from price import WSOL, get_jupiter_quote, get_sol_usd, pump_curve_price_usd, resolve_real_price


_SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS candidates (
        candidate_id TEXT PRIMARY KEY, chain_id TEXT NOT NULL, mint TEXT NOT NULL,
        source TEXT NOT NULL, first_seen_at_utc TEXT NOT NULL, signature TEXT,
        curve TEXT, creator TEXT, program_creator TEXT, tx_payer TEXT,
        name TEXT, symbol TEXT, decimals INTEGER,
        dev_buy_sol REAL, initial_buy_raw INTEGER,
        raw_json TEXT NOT NULL, config_json TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS detections (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT, source TEXT NOT NULL,
        detection_type TEXT NOT NULL, detected_at_utc TEXT NOT NULL,
        signature TEXT, raw_json TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        stage TEXT NOT NULL, decision TEXT NOT NULL, reasons_json TEXT NOT NULL,
        values_json TEXT NOT NULL, occurred_at_utc TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS creator_attributions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        program_creator TEXT, tx_payer TEXT, identity_source TEXT,
        observed_at_utc TEXT NOT NULL, latency_ms REAL, values_json TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS creator_probes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        program_creator TEXT, tx_payer TEXT, status TEXT NOT NULL,
        requested_at_utc TEXT NOT NULL, observed_at_utc TEXT NOT NULL,
        latency_ms REAL, signature_count INTEGER, oldest_block_time INTEGER,
        ata_exists INTEGER, token_balance_raw TEXT, token_balance_ui REAL,
        error_type TEXT, error_detail TEXT, values_json TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS watchlist_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        event_type TEXT NOT NULL, status TEXT NOT NULL,
        occurred_at_utc TEXT NOT NULL, values_json TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS quotes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        stage TEXT NOT NULL, side TEXT NOT NULL, status TEXT NOT NULL,
        requested_at_utc TEXT NOT NULL, input_mint TEXT, output_mint TEXT,
        input_amount_raw TEXT, out_amount_raw TEXT, min_out_raw TEXT,
        price_impact_pct REAL, route_json TEXT NOT NULL, context_slot INTEGER,
        slippage_bps INTEGER NOT NULL, latency_ms REAL, error_type TEXT,
        quote_only INTEGER NOT NULL CHECK (quote_only = 1)
    )''',
    '''CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        candidate_id TEXT, mint TEXT NOT NULL, source TEXT NOT NULL,
        target_offset_sec INTEGER NOT NULL, target_at_utc TEXT NOT NULL,
        observed_at_utc TEXT NOT NULL, status TEXT NOT NULL, price_usd REAL,
        price_source TEXT, latency_ms REAL, error_type TEXT,
        excluded_from_stats INTEGER NOT NULL DEFAULT 0
    )''',
    '''CREATE TABLE IF NOT EXISTS paper_fills (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        quote_event_id TEXT, candidate_id TEXT, mint TEXT NOT NULL,
        source TEXT NOT NULL, occurred_at_utc TEXT NOT NULL, reason TEXT NOT NULL,
        sold_fraction REAL NOT NULL, fill_price_usd REAL NOT NULL,
        net_return_rub REAL NOT NULL, pnl_leg_rub REAL NOT NULL,
        fee_rub REAL NOT NULL, out_amount_raw TEXT, min_out_raw TEXT NOT NULL,
        snapshot_pnl_rub REAL, conservative_100bps_pnl_rub REAL,
        conservative_out_amount_raw TEXT, conservative_min_out_raw TEXT,
        conservative_sold_fraction REAL, conservative_error TEXT,
        price_impact_pct REAL, route_json TEXT NOT NULL,
        paper_only INTEGER NOT NULL CHECK (paper_only = 1),
        portfolio TEXT NOT NULL DEFAULT 'main'
    )''',
    '''CREATE TABLE IF NOT EXISTS collector_meta (
        key TEXT PRIMARY KEY, value TEXT NOT NULL, recorded_at_utc TEXT NOT NULL
    )''',
    'CREATE INDEX IF NOT EXISTS idx_detections_mint_time ON detections(mint, detected_at_utc)',
    'CREATE INDEX IF NOT EXISTS idx_decisions_mint_time ON decisions(mint, occurred_at_utc)',
    'CREATE INDEX IF NOT EXISTS idx_quotes_mint_time ON quotes(mint, requested_at_utc)',
    'CREATE INDEX IF NOT EXISTS idx_observations_mint_time ON observations(mint, target_at_utc)',
)

_INSERTS = {
    'candidate': '''INSERT OR IGNORE INTO candidates
        (candidate_id,chain_id,mint,source,first_seen_at_utc,signature,curve,creator,
         program_creator,tx_payer,name,symbol,decimals,dev_buy_sol,
         initial_buy_raw,raw_json,config_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
    'detection': '''INSERT OR IGNORE INTO detections
        (event_id,candidate_id,mint,source,detection_type,detected_at_utc,signature,raw_json)
        VALUES (?,?,?,?,?,?,?,?)''',
    'decision': '''INSERT OR IGNORE INTO decisions
        (event_id,candidate_id,mint,source,stage,decision,reasons_json,values_json,occurred_at_utc)
        VALUES (?,?,?,?,?,?,?,?,?)''',
    'creator_attribution': '''INSERT OR IGNORE INTO creator_attributions
        (event_id,candidate_id,mint,source,program_creator,tx_payer,identity_source,
         observed_at_utc,latency_ms,values_json) VALUES (?,?,?,?,?,?,?,?,?,?)''',
    'creator_probe': '''INSERT OR IGNORE INTO creator_probes
        (event_id,candidate_id,mint,source,program_creator,tx_payer,status,
         requested_at_utc,observed_at_utc,latency_ms,signature_count,oldest_block_time,
         ata_exists,token_balance_raw,token_balance_ui,error_type,error_detail,
         values_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
    'watchlist_event': '''INSERT OR IGNORE INTO watchlist_events
        (event_id,candidate_id,mint,source,event_type,status,occurred_at_utc,values_json)
        VALUES (?,?,?,?,?,?,?,?)''',
    'quote': '''INSERT OR IGNORE INTO quotes
        (event_id,candidate_id,mint,source,stage,side,status,requested_at_utc,input_mint,
         output_mint,input_amount_raw,out_amount_raw,min_out_raw,price_impact_pct,route_json,
         context_slot,slippage_bps,latency_ms,error_type,quote_only)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)''',
    'observation': '''INSERT OR IGNORE INTO observations
        (event_id,candidate_id,mint,source,target_offset_sec,target_at_utc,observed_at_utc,
         status,price_usd,price_source,latency_ms,error_type,excluded_from_stats)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
    'fill': '''INSERT OR IGNORE INTO paper_fills
        (event_id,quote_event_id,candidate_id,mint,source,occurred_at_utc,reason,sold_fraction,
         fill_price_usd,net_return_rub,pnl_leg_rub,fee_rub,out_amount_raw,min_out_raw,
         snapshot_pnl_rub,conservative_100bps_pnl_rub,conservative_out_amount_raw,
         conservative_min_out_raw,conservative_sold_fraction,conservative_error,
         price_impact_pct,route_json,paper_only,portfolio)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)''',
    'meta': 'INSERT OR REPLACE INTO collector_meta(key,value,recorded_at_utc) VALUES (?,?,?)',
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def _observation_target_monotonic(
    target_epoch: float, offset: float, now_epoch: float, now_monotonic: float,
) -> float:
    """Absolute monotonic deadline for one scheduled observation (P0-1).

    The deadline is anchored to *now*, never to the moment the collector task
    was spawned: anchoring to the spawn time added the collector's own startup
    latency to every offset, which is exactly the 5.06 / 50.06 s drift run #2
    showed. Pure arithmetic, so it is unit-testable without sleeping.
    """
    return float(now_monotonic) + max(0.0, float(target_epoch) + float(offset) - float(now_epoch))


def _utc_from_epoch(value: float) -> str:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat(timespec='milliseconds')
    except (ValueError, TypeError, OverflowError):
        return utc_now()


_URL_CREDENTIAL_RE = re.compile(
    r'''([?&](?:api[-_]?key|access[-_]?token|refresh[-_]?token|auth(?:orization)?|token|jwt|password|secret|private[-_]?key|key)=)[^&\s"'<>]+''',
    re.IGNORECASE,
)
_SECRET_FIELD_RE = re.compile(
    r'''(["'`]?)(api[-_]?key|helius[_-]?api[_-]?key|access[-_]?token|refresh[-_]?token|auth[_-]?token|token|jwt|authorization|private[_-]?key|secret[_-]?key|keypair|password|mnemonic|seed(?:[_-]?phrase)?)(["'`]?[\s]*[:=][\s]*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\])*`|\[[^\]]*\]|Bearer\s+[^\s,}\]]+|[^,\s}\]]+)''',
    re.IGNORECASE,
)
_SECRET_NAME_SUFFIXES = (
    'apikey', 'token', 'jwt', 'authorization', 'secret', 'secretkey',
    'privatekey', 'keypair', 'password', 'mnemonic', 'seedphrase',
)


def _redact_text(value: str) -> str:
    text = _URL_CREDENTIAL_RE.sub(lambda match: match.group(1) + '[REDACTED]', str(value))
    return _SECRET_FIELD_RE.sub(
        lambda match: match.group(1) + match.group(2) + match.group(3) + '"[REDACTED]"',
        text,
    )


def _safe_json(value: Any) -> str:
    def clean(obj: Any) -> Any:
        if isinstance(obj, dict):
            result = {}
            for key, item in obj.items():
                normalized = re.sub(r'[^a-z0-9]', '', str(key).lower())
                if normalized.endswith(_SECRET_NAME_SUFFIXES):
                    result[str(key)] = '[REDACTED]'
                else:
                    result[str(key)] = clean(item)
            return result
        if isinstance(obj, (list, tuple)):
            return [clean(item) for item in obj]
        if isinstance(obj, str):
            return _redact_text(obj)
        if obj is None or isinstance(obj, (int, float, bool)):
            return obj
        return _redact_text(str(obj))
    try:
        return json.dumps(clean(value), ensure_ascii=False, separators=(',', ':'), default=str)
    except Exception:
        return '{}'


MAX_ERROR_DETAIL_CHARS = 200


def _summarize_error_detail(details: Any) -> Optional[str]:
    """One bounded, redacted line per failed RPC (code/message/first 200 chars).

    Run #1 showed 108 of 405 probes failing in ~83 ms with nothing but
    ``RuntimeError`` recorded, so a 429 could not be told from a bad request.
    """
    if not details:
        return None
    if isinstance(details, str):
        return _redact_text(details)[:MAX_ERROR_DETAIL_CHARS] or None
    if not isinstance(details, (list, tuple)):
        return None
    parts = []
    for item in details:
        if not isinstance(item, dict):
            continue
        label = str(item.get('label') or item.get('method') or 'error')
        bits = [label]
        method = str(item.get('method') or '').strip()
        if method and method not in label:
            bits.append(method)
        for key in ('http_status', 'code'):
            if item.get(key) is not None:
                bits.append(f'{key}={item[key]}')
        message = str(item.get('message') or item.get('body') or '').strip()
        if message:
            bits.append(message[:MAX_ERROR_DETAIL_CHARS])
        parts.append(' '.join(bits))
    if not parts:
        return None
    return _redact_text('; '.join(parts))[:MAX_ERROR_DETAIL_CHARS]


def _fallback_error_detail(
    status: str, error_type: Optional[str], values: dict[str, Any],
) -> str:
    """Never write a failed probe without a detail line (P0-3, run #3).

    Run #2 left 21 of 75 failed probes with ``error_detail = NULL`` — precisely
    the rows that would explain a 429 or a timeout. The fallback is built from
    data already in the row, so it costs no RPC and can never be empty.
    """
    for key in ('errors', 'error_detail', 'error', 'message', 'error_message'):
        summary = _summarize_error_detail(values.get(key))
        if summary:
            return summary
    for key in ('http_status', 'code', 'rpc_error_code'):
        if values.get(key) is not None:
            return _redact_text(f'{key}={values[key]} status={status}')[:MAX_ERROR_DETAIL_CHARS]
    return _redact_text(
        f'error_type={error_type or "unknown"} status={status}'
    )[:MAX_ERROR_DETAIL_CHARS]


def _float_or_none(value: Any) -> Optional[float]:
    """Defensive cast for telemetry-only REAL columns (never raises)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number == number and abs(number) != float('inf') else None


def _int_or_none(value: Any) -> Optional[int]:
    """Defensive cast for telemetry-only integer columns (never raises)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number


def candidate_id_for(mint: str) -> str:
    return hashlib.sha256(f'solana-mainnet:{mint}'.encode('utf-8')).hexdigest()


EXCLUDED_OBSERVATION_STATUSES = frozenset({'cancelled_at_session_end'})


def _excluded_from_stats(status: str) -> int:
    """1 when an observation row must stay out of every statistic."""
    return 1 if str(status or '').strip() in EXCLUDED_OBSERVATION_STATUSES else 0


class CandidateCollector:
    """Bounded-queue SQLite telemetry with independent, rate-limited shadow sampling."""

    def __init__(
        self,
        db_path: str,
        export_dir: str,
        shadow_notional_rub: float = 250.0,
        observation_offsets_sec: tuple[int, ...] = (0, 5, 10, 15, 60, 120),
        queue_limit: int = 20_000,
        creator_watch: Any = None,
        enable_shadow_quotes: bool = True,
    ) -> None:
        self.db_path = str(db_path)
        self.export_dir = Path(export_dir)
        self.shadow_notional_rub = float(shadow_notional_rub)
        self.observation_offsets_sec = tuple(sorted(set(int(x) for x in observation_offsets_sec)))
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_limit)
        self._writer_task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()
        self._accepting = True
        self._failed = False
        self._writer_error = ''
        self._dropped = 0
        self._write_errors = 0
        self._tasks: set[asyncio.Task] = set()
        self._quote_sem = asyncio.Semaphore(1)
        self._observation_sem = asyncio.Semaphore(2)
        self._sol_cache: tuple[float, Optional[float]] = (0.0, None)
        self.creator_watch = creator_watch
        self.enable_shadow_quotes = bool(enable_shadow_quotes)
        # P0-2: security verdict per mint. Coins rejected by the security
        # filters are observed only at offsets 0 and 15 — enough to answer
        # "did it become a rocket in the first 15 s", a third of the reads.
        self._security_verdicts: dict[str, bool] = {}
        # Deep offsets kept for coins that failed the security filters.
        self.security_fail_deep_offsets_sec = (0, 15)
        self.security_fail_deep_skipped = 0

    async def start(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self._writer_task = asyncio.create_task(self._writer(), name='candidate-telemetry-writer')
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            self._failed = True
            self._writer_error = 'writer_start_timeout'
        if self._failed:
            log(f'⚠️ Телеметрия не готова ({self._writer_error}); торговая логика продолжает работу без неё', Color.YELLOW)
        else:
            log(f'💾 Пассивная телеметрия готова: {self.db_path}', Color.GREEN)

    def _put(self, kind: str, values: tuple[Any, ...]) -> Optional[str]:
        if not self._accepting or self._failed:
            self._dropped += 1
            return None
        event_id = str(values[0]) if values else None
        try:
            self.queue.put_nowait((kind, values))
            return event_id
        except asyncio.QueueFull:
            self._dropped += 1
            return None

    async def _writer(self) -> None:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute('PRAGMA journal_mode=WAL')
                await db.execute('PRAGMA synchronous=NORMAL')
                for statement in _SCHEMA:
                    await db.execute(statement)
                # Safe additive migrations for any explicitly reused telemetry DB.
                for table, column, definition in (
                    ('candidates', 'program_creator', 'TEXT'),
                    ('candidates', 'tx_payer', 'TEXT'),
                    ('candidates', 'dev_buy_sol', 'REAL'),
                    ('candidates', 'initial_buy_raw', 'INTEGER'),
                    ('creator_probes', 'error_detail', 'TEXT'),
                    ('paper_fills', 'portfolio', "TEXT NOT NULL DEFAULT 'main'"),
                    ('paper_fills', 'out_amount_raw', 'TEXT'),
                    ('paper_fills', 'snapshot_pnl_rub', 'REAL'),
                    ('paper_fills', 'conservative_100bps_pnl_rub', 'REAL'),
                    ('paper_fills', 'conservative_out_amount_raw', 'TEXT'),
                    ('paper_fills', 'conservative_min_out_raw', 'TEXT'),
                    ('paper_fills', 'conservative_sold_fraction', 'REAL'),
                    ('paper_fills', 'conservative_error', 'TEXT'),
                    ('observations', 'excluded_from_stats',
                     'INTEGER NOT NULL DEFAULT 0'),
                ):
                    cursor = await db.execute(f'PRAGMA table_info({table})')
                    existing_columns = {row[1] for row in await cursor.fetchall()}
                    if column not in existing_columns:
                        await db.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')
                await db.commit()
                self._ready.set()
                while True:
                    item = await self.queue.get()
                    try:
                        if item is None:
                            return
                        kind, values = item
                        try:
                            await db.execute(_INSERTS[kind], values)
                            if kind == 'creator_attribution':
                                await db.execute(
                                    'UPDATE candidates SET creator=?,program_creator=?,tx_payer=? WHERE candidate_id=?',
                                    (values[4], values[4], values[5], values[1]),
                                )
                            await db.commit()
                        except Exception as exc:
                            self._write_errors += 1
                            self._writer_error = type(exc).__name__
                    finally:
                        self.queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failed = True
            self._writer_error = type(exc).__name__
            self._ready.set()
            log(f'⚠️ Ошибка писателя телеметрии ({self._writer_error}); торговая логика не меняется', Color.YELLOW)

    def record_source_event(
        self, source: str, detection_type: str, mint: Optional[str],
        signature: Optional[str], payload: Any,
    ) -> None:
        cid = candidate_id_for(mint) if mint else None
        self._put('detection', (
            uuid.uuid4().hex, cid, mint, source, detection_type, utc_now(),
            signature or None, _safe_json(payload),
        ))

    def record_candidate(self, coin: NewCoin, config: dict[str, Any]) -> None:
        cid = candidate_id_for(coin.mint)
        first_seen = _utc_from_epoch(coin.found_at)
        packet = dict(coin.packet or {})
        program_creator = coin.program_creator or packet.get('program_creator')
        tx_payer = coin.tx_payer or packet.get('tx_payer')
        if tx_payer is None and coin.source != 'Pump.fun':
            tx_payer = coin.creator
        packet['program_creator'] = program_creator
        packet['tx_payer'] = tx_payer
        self._put('candidate', (
            cid, 'solana-mainnet', coin.mint, coin.source, first_seen,
            coin.signature or None, coin.curve, program_creator,
            program_creator, tx_payer,
            str(packet.get('name') or ''), str(packet.get('symbol') or ''),
            coin.decimals, _float_or_none(packet.get('dev_buy_sol')),
            _int_or_none(packet.get('initial_buy_raw')),
            _safe_json(packet), _safe_json(config),
        ))
        self._put('detection', (
            uuid.uuid4().hex, cid, coin.mint, coin.source, 'candidate_received',
            first_seen, coin.signature or None, _safe_json(packet),
        ))

    def record_creator_attribution(
        self,
        coin: NewCoin,
        observed_at_epoch: Optional[float] = None,
        identity_source: Optional[str] = None,
    ) -> None:
        observed_epoch = float(observed_at_epoch or time.time())
        observed_at = _utc_from_epoch(observed_epoch)
        program_creator = coin.program_creator or (coin.packet or {}).get('program_creator')
        tx_payer = coin.tx_payer or (coin.packet or {}).get('tx_payer')
        if tx_payer is None and coin.source != 'Pump.fun':
            tx_payer = coin.creator
        latency_ms = max(0.0, (observed_epoch - float(coin.found_at)) * 1000.0)
        values = {
            'program_creator': program_creator,
            'tx_payer': tx_payer,
            'identity_source': identity_source or (
                'pump_curve_raw_49_81' if program_creator else
                'first_transaction_signer' if tx_payer and coin.source != 'Pump.fun' else
                'getTransaction_first_signer' if tx_payer else 'unknown'
            ),
            'observed_at_utc': observed_at,
            'latency_ms': round(latency_ms, 2),
            'candidate_created_at_utc': _utc_from_epoch(coin.found_at),
            'causal_availability': True,
        }
        self._put('creator_attribution', (
            uuid.uuid4().hex, candidate_id_for(coin.mint), coin.mint,
            coin.source, program_creator, tx_payer, values['identity_source'],
            observed_at, latency_ms, _safe_json(values),
        ))

    def record_creator_probe(
        self,
        mint: str,
        source: str,
        program_creator: Optional[str],
        tx_payer: Optional[str],
        status: str,
        values: dict[str, Any],
        signature_count: Optional[int],
        oldest_block_time: Optional[int],
        ata_exists: Optional[bool],
        token_balance_raw: Optional[str],
        token_balance_ui: Optional[float],
        latency_ms: Optional[float],
        observed_at_utc: str,
        error_type: Optional[str] = None,
        error_detail: Any = None,
    ) -> None:
        event_id = uuid.uuid4().hex
        requested_at = str(values.get('probe_started_at_utc') or utc_now())
        # This is a public API and the column is TEXT: normalize here, not only
        # in the caller. Run #1 lost a whole probe row because a list[dict] went
        # straight into the TEXT parameter (sqlite3 ProgrammingError); the raw
        # payload is still preserved in values_json below.
        if not isinstance(error_detail, str):
            error_detail = _summarize_error_detail(
                error_detail if error_detail is not None
                else values.get('error_detail')
            )
        if not error_detail and str(status) != 'ok':
            # A failed probe without a detail is a blind spot by construction.
            error_detail = _fallback_error_detail(str(status), error_type, values)
        self._put('creator_probe', (
            event_id, candidate_id_for(mint), mint, source,
            program_creator, tx_payer, status, requested_at, observed_at_utc,
            latency_ms, signature_count, oldest_block_time,
            None if ata_exists is None else int(bool(ata_exists)),
            token_balance_raw, token_balance_ui, error_type, error_detail,
            _safe_json(values),
        ))
        self.record_decision(
            mint, source, 'creator_onchain_probe', status,
            [error_type] if error_type else [], values,
        )

    def record_watchlist_event(
        self, mint: str, source: str, event_type: str, status: str,
        values: Any = None,
    ) -> Optional[str]:
        event_id = uuid.uuid4().hex
        return self._put('watchlist_event', (
            event_id, candidate_id_for(mint), mint, source,
            event_type, status, utc_now(), _safe_json(values or {}),
        ))

    def record_decision(
        self, mint: str, source: str, stage: str, decision: str,
        reasons: Any = None, values: Any = None,
    ) -> None:
        self._put('decision', (
            uuid.uuid4().hex, candidate_id_for(mint), mint, source, stage,
            decision, _safe_json(reasons or []), _safe_json(values or {}), utc_now(),
        ))

    def record_meta(self, key: str, value: Any) -> None:
        """Persist redacted run metadata in collector_meta."""
        self._put('meta', (str(key), _safe_json(value), utc_now()))

    def record_quote(
        self, mint: str, source: str, stage: str, side: str,
        input_mint: str, output_mint: str, amount_raw: int,
        quote: Optional[SwapQuote], status: str, slippage_bps: int = 100,
        latency_ms: Optional[float] = None, error_type: Optional[str] = None,
    ) -> Optional[str]:
        event_id = uuid.uuid4().hex
        requested_at = utc_now()
        stored_id = self._put('quote', (
            event_id, candidate_id_for(mint), mint, source, stage, side, status,
            requested_at, input_mint, output_mint, str(amount_raw),
            str(quote.out_amount_raw) if quote else None,
            str(quote.min_out_raw) if quote else None,
            quote.price_impact_pct if quote else None,
            _safe_json(list(quote.route_labels) if quote else []),
            quote.context_slot if quote else None, int(slippage_bps),
            latency_ms, error_type,
        ))
        if self.creator_watch is not None:
            self.creator_watch.record_quote(
                mint, stage, side, status, requested_at, error_type, event_id,
            )
        return stored_id

    def set_security_verdict(self, mint: str, passed: bool) -> None:
        """Remember the security verdict so the deep observation can be trimmed.

        Telemetry only: nothing in the entry/exit path reads this.
        """
        self._security_verdicts[mint] = bool(passed)

    def _deep_offsets_for(self, mint: str) -> Optional[tuple[int, ...]]:
        """Offsets worth an RPC for this mint; None = no trim (verdict unknown)."""
        verdict = self._security_verdicts.get(mint)
        if verdict is None or verdict:
            return None
        return self.security_fail_deep_offsets_sec

    def record_observation(
        self, mint: str, source: str, target_offset_sec: int,
        target_at_utc: str, status: str, price_usd: Optional[float] = None,
        price_source: Optional[str] = None, latency_ms: Optional[float] = None,
        error_type: Optional[str] = None,
        observed_at_utc: Optional[str] = None,
    ) -> None:
        event_id = uuid.uuid4().hex
        # `observed_at` must be the sampling instant, not the writer's: the
        # collector stamps it when the scheduled wait ends (RPC latency is
        # already reported separately in latency_ms).
        observed_at = observed_at_utc or utc_now()
        self._put('observation', (
            event_id, candidate_id_for(mint), mint, source,
            int(target_offset_sec), target_at_utc, observed_at, status,
            price_usd, price_source, latency_ms, error_type,
            # Run #4: rows the session end interrupted are missing data, not
            # a zero. The flag travels with the row so every later query can
            # exclude them without knowing the status string.
            _excluded_from_stats(status),
        ))
        if self.creator_watch is not None:
            self.creator_watch.record_observation(
                mint, int(target_offset_sec), target_at_utc, observed_at,
                status, price_usd, price_source, event_id,
            )

    def record_fill(
        self, mint: str, source: str, quote_event_id: Optional[str], reason: str,
        sold_fraction: float, fill_price_usd: float, net_return_rub: float,
        pnl_leg_rub: float, fee_rub: float, min_out_raw: int,
        price_impact_pct: Optional[float], route_labels: Any,
        portfolio: str = 'main',
        out_amount_raw: Optional[int] = None,
        snapshot_pnl_rub: Optional[float] = None,
        conservative_100bps_pnl_rub: Optional[float] = None,
        conservative_out_amount_raw: Optional[int] = None,
        conservative_min_out_raw: Optional[int] = None,
        conservative_sold_fraction: Optional[float] = None,
        conservative_error: Optional[str] = None,
    ) -> None:
        self._put('fill', (
            uuid.uuid4().hex, quote_event_id, candidate_id_for(mint), mint, source,
            utc_now(), reason, float(sold_fraction), float(fill_price_usd),
            float(net_return_rub), float(pnl_leg_rub), float(fee_rub),
            str(out_amount_raw) if out_amount_raw is not None else None,
            str(min_out_raw),
            float(pnl_leg_rub if snapshot_pnl_rub is None else snapshot_pnl_rub),
            conservative_100bps_pnl_rub,
            str(conservative_out_amount_raw) if conservative_out_amount_raw is not None else None,
            str(conservative_min_out_raw) if conservative_min_out_raw is not None else None,
            conservative_sold_fraction,
            str(conservative_error) if conservative_error else None,
            price_impact_pct, _safe_json(route_labels or []),
            str(portfolio or 'main'),
        ))

    def observe_candidate(self, coin: NewCoin, session: Any, settings: Any) -> None:
        config = {
            'portfolio_scope': os.getenv('PORTFOLIO_SCOPE', 'legacy').strip().lower(),
            'max_trade_rub': settings.max_trade_rub,
            'max_positions': settings.max_positions,
            'min_liquidity_usd': settings.min_liquidity_usd,
            'max_entry_price_impact_pct': settings.max_entry_price_impact_pct,
            'take_profit_pct': settings.take_profit_pct,
            'stop_loss_trigger_pct': settings.stop_loss_pct,
            'stagnation_sec': settings.stagnation_sec,
            'stagnation_range_pct': settings.stagnation_range_pct,
            'max_hold_sec': settings.max_hold_sec,
            'shadow_quotes_enabled': self.enable_shadow_quotes,
            'paper_only': True,
        }
        if config['portfolio_scope'] == 'ab_only':
            shared = {
                'entry': '30.5 <= v_sol_birth <= 34.5 and 0.10 <= (v_sol_now - v_sol_birth) <= 2.5',
                'stop_loss': 'pnl_pct <= -10 before best_pnl_pct reaches +20',
                'no_progress': 'once at 10s: best_pnl_pct < +15 and pnl_pct < +1.5',
                'trailing': 'after best_pnl_pct >= +20: min(peak-15.0, 0.75*peak), sell 100%',
                'timeout_before_trailing_sec': 35,
                'timeout_after_trailing_sec': 90,
            }
            config['strategy_profiles'] = {
                'portfolio_a': {**shared, 'creator_gate': False},
                'portfolio_b': {**shared, 'creator_gate': 'signature_count_0_25 == 25 AND ata_exists AND token_balance_raw > 0'},
                'initial_balance_rub_per_portfolio': 1500,
                'position_size_rub': 500,
                'max_slots_per_portfolio': 3,
            }
            config['snapshot_fill'] = 'out_amount_raw'
            config['conservative_shadow'] = 'min_out_raw at 100 bps'
            config['duplicate_symbol_gate'] = False
            config['legacy_single_strategy_fields_active'] = False
        self.record_candidate(coin, config)
        if self._failed or not self._accepting:
            return
        task = asyncio.create_task(self._collect_candidate(coin, session, settings))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _cached_sol_usd(self, session: Any) -> Optional[float]:
        at, value = self._sol_cache
        if value and time.monotonic() - at < 30:
            return value
        value = await get_sol_usd(session)
        if value and value > 0:
            self._sol_cache = (time.monotonic(), value)
        return value

    async def _collect_shadow_quotes(self, coin: NewCoin, session: Any, settings: Any) -> None:
        async with self._quote_sem:
            sol_usd = await self._cached_sol_usd(session)
            if not sol_usd or sol_usd <= 0:
                self.record_quote(coin.mint, coin.source, 'shadow_buy', 'buy', WSOL,
                                  coin.mint, 0, None, 'not_requested', error_type='sol_price_unavailable')
                return
            sol_rub = sol_usd * settings.usd_rub_rate
            amount_raw = int(self.shadow_notional_rub / sol_rub * 1_000_000_000) if sol_rub > 0 else 0
            if amount_raw <= 0:
                self.record_quote(coin.mint, coin.source, 'shadow_buy', 'buy', WSOL,
                                  coin.mint, 0, None, 'not_requested', error_type='invalid_size')
                return
            started = time.monotonic()
            buy = await get_jupiter_quote(session, WSOL, coin.mint, amount_raw, slippage_bps=100)
            buy_latency = (time.monotonic() - started) * 1000
            self.record_quote(coin.mint, coin.source, 'shadow_buy', 'buy', WSOL, coin.mint,
                              amount_raw, buy, 'ok' if buy else 'no_route', 100,
                              buy_latency, None if buy else 'quote_unavailable')
            if not buy or buy.min_out_raw <= 0:
                self.record_quote(coin.mint, coin.source, 'shadow_sell', 'sell', coin.mint,
                                  WSOL, 0, None, 'not_requested', error_type='buy_quote_unavailable')
                return
            started = time.monotonic()
            sell = await get_jupiter_quote(session, coin.mint, WSOL, buy.min_out_raw, slippage_bps=100)
            sell_latency = (time.monotonic() - started) * 1000
            self.record_quote(coin.mint, coin.source, 'shadow_sell', 'sell', coin.mint, WSOL,
                              buy.min_out_raw, sell, 'ok' if sell else 'no_route', 100,
                              sell_latency, None if sell else 'quote_unavailable')

    async def _collect_candidate(self, coin: NewCoin, session: Any, settings: Any) -> None:
        started_monotonic = time.monotonic()
        target_epoch = float(coin.found_at)
        completed: set[int] = set()
        tasks = [
            asyncio.create_task(self._collect_followups(coin, session, settings, started_monotonic, target_epoch, completed)),
        ]
        if self.enable_shadow_quotes:
            tasks.append(asyncio.create_task(self._collect_shadow_quotes(coin, session, settings)))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for child in tasks:
                child.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        except Exception as exc:
            for child in tasks:
                if not child.done():
                    child.cancel()
            self.record_decision(coin.mint, coin.source, 'collector', 'error',
                                 [type(exc).__name__])

    async def _collect_followups(
        self, coin: NewCoin, session: Any, settings: Any,
        started_monotonic: float, target_epoch: float, completed: set[int],
    ) -> None:
        try:
            for offset in self.observation_offsets_sec:
                # P0-2 (run #7): the verdict is resolved INSIDE the loop.
                # `observe_candidate` starts before `handle_candidate` sets the
                # verdict, so a single read before the loop always saw `None`
                # and read all six offsets (run6: skipped_deep = 0 over 3 695
                # coins, 1 634 of them security failures).
                deep = self._deep_offsets_for(coin.mint)
                if deep is not None and offset not in deep:
                    # P0-2: no RPC for a coin we would never trade. The row
                    # still exists, so "not measured" cannot be read as "did
                    # not move".
                    self.security_fail_deep_skipped += 1
                    self.record_observation(
                        coin.mint, coin.source, offset,
                        _utc_from_epoch(target_epoch + offset),
                        'skipped_security_deep',
                        error_type='security_filters_failed',
                    )
                    completed.add(offset)
                    continue
                # Absolute target in the monotonic clock. The old form added
                # the remaining wall-clock delay to the moment the collector
                # started, so offsets drifted by the collector's own latency
                # (run #2: 5.06 / 50.06 / 70.00 s observed instead of 5/50/70).
                target_mono = _observation_target_monotonic(
                    target_epoch, offset, time.time(), time.monotonic(),
                )
                wait = target_mono - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                target_utc = _utc_from_epoch(target_epoch + offset)
                observed_utc = _utc_from_epoch(time.time())
                started = time.monotonic()
                try:
                    async with self._observation_sem:
                        async with asyncio.timeout(10.0):
                            if coin.source == 'Pump.fun':
                                price = await pump_curve_price_usd(
                                    session, settings.http_rpc_url, coin.mint, coin.curve,
                                )
                                origin = 'on_chain_pump_curve' if price and price > 0 else ''
                            else:
                                price, origin = await resolve_real_price(
                                    session, settings.http_rpc_url, coin.mint,
                                    coin.source, coin.curve,
                                )
                    self.record_observation(
                        coin.mint, coin.source, offset, target_utc,
                        'ok' if price and price > 0 else 'no_price', price,
                        origin or None, (time.monotonic() - started) * 1000,
                        None if price and price > 0 else 'price_unavailable',
                        observed_at_utc=observed_utc,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.record_observation(
                        coin.mint, coin.source, offset, target_utc, 'error',
                        latency_ms=(time.monotonic() - started) * 1000,
                        error_type=type(exc).__name__,
                        observed_at_utc=observed_utc,
                    )
                completed.add(offset)
        except asyncio.CancelledError:
            for offset in self.observation_offsets_sec:
                if offset not in completed:
                    self.record_observation(
                        coin.mint, coin.source, offset,
                        _utc_from_epoch(target_epoch + offset),
                        'cancelled_at_session_end', error_type='session_stopped',
                    )
            raise

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._put('meta', ('dropped_events', str(self._dropped), utc_now()))
        if self._writer_task and not self._writer_task.done() and not self._failed:
            self._accepting = False
            try:
                await asyncio.wait_for(self.queue.join(), timeout=15.0)
                await self.queue.put(None)
                await asyncio.wait_for(self._writer_task, timeout=15.0)
            except Exception as exc:
                self._writer_error = self._writer_error or type(exc).__name__
                self._writer_task.cancel()
                await asyncio.gather(self._writer_task, return_exceptions=True)
        else:
            self._accepting = False
        # Written after the queue drained: a counter queued before join() would
        # freeze the number at 0 in the artifact (run #1: log said 1, DB said 0)
        # and the only durable trace of lost rows would be one yellow log line.
        self._write_final_counters()
        if not self._failed:
            try:
                await asyncio.to_thread(self.export)
            except Exception as exc:
                log(f'⚠️ Не удалось экспортировать телеметрию ({type(exc).__name__})', Color.YELLOW)
        if self._dropped or self._write_errors or self._writer_error:
            log(
                f'⚠️ Телеметрия: dropped={self._dropped}, write_errors={self._write_errors}, '
                f'writer={self._writer_error or "ok"}', Color.YELLOW,
            )

    def stats(self) -> dict[str, Any]:
        """Durable telemetry health for the manifest / SUMMARY."""
        return {
            'dropped_events': int(self._dropped),
            'write_errors': int(self._write_errors),
            'writer_error': self._writer_error or None,
            'failed': bool(self._failed),
        }

    def _write_final_counters(self) -> None:
        """Persist counters straight into the DB after the writer stopped."""
        try:
            with sqlite3.connect(self.db_path) as db:
                for key, value in (
                    ('dropped_events', str(self._dropped)),
                    ('writer_errors', str(self._write_errors)),
                    ('writer_error_type', self._writer_error or 'ok'),
                ):
                    db.execute(
                        'INSERT OR REPLACE INTO collector_meta(key,value,recorded_at_utc) '
                        'VALUES (?,?,?)',
                        (key, value, utc_now()),
                    )
                db.commit()
        except sqlite3.Error as exc:
            log(f'⚠️ Не удалось записать счётчики телеметрии ({type(exc).__name__})', Color.YELLOW)

    def export(self) -> None:
        self.export_dir.mkdir(parents=True, exist_ok=True)
        tables = (
            'candidates', 'detections', 'decisions', 'creator_attributions',
            'creator_probes', 'watchlist_events', 'quotes', 'observations',
            'paper_fills', 'collector_meta',
        )
        with sqlite3.connect(f'file:{Path(self.db_path).resolve()}?mode=ro', uri=True) as db:
            for table in tables:
                rows = db.execute(f'SELECT * FROM {table}').fetchall()
                headers = [column[1] for column in db.execute(f'PRAGMA table_info({table})').fetchall()]
                with (self.export_dir / f'{table}.csv').open('w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(rows)
            detections = db.execute(
                'SELECT event_id,candidate_id,mint,source,detection_type,detected_at_utc,signature,raw_json FROM detections ORDER BY id'
            ).fetchall()
        with (self.export_dir / 'raw_events.jsonl').open('w', encoding='utf-8') as f:
            for row in detections:
                f.write(json.dumps({
                    'event_id': row[0], 'candidate_id': row[1], 'mint': row[2],
                    'source': row[3], 'detection_type': row[4],
                    'detected_at_utc': row[5], 'signature': row[6],
                    'raw_payload': json.loads(row[7] or '{}'),
                }, ensure_ascii=False) + '\n')
        (self.export_dir / 'README.md').write_text(
            '# Paper candidate telemetry export\n\n'
            'Read-only observation data. No transactions were signed or sent.\n'
            'All timestamps are UTC. Quote rows are observations, not fills.\n'
            'See `candidates.csv`, `detections.csv`, `decisions.csv`, '
            '`creator_attributions.csv`, `creator_probes.csv`, `watchlist_events.csv`, '
            '`quotes.csv`, `observations.csv`, and `paper_fills.csv`.\n',
            encoding='utf-8',
        )
        (self.export_dir / 'data_dictionary.md').write_text(
            '# Data dictionary\n\n'
            '- `candidates`: one stable row per chain+mint, first-seen metadata and config snapshot.\n'
            '- `detections`: append-only source creation events and candidate-received events; raw JSON is redacted.\n'
            '- `decisions`: filter, capacity, entry, quote-confirmation, and collector outcomes with reasons/values.\n'
            '- `creator_attributions`: separate program creator and transaction payer with causal observation time/latency.\n'
            '- `creator_probes`: read-only getSignaturesForAddress(limit=25) and creator ATA balance results; RPC failures are non-blocking.\n'
            '- `watchlist_events`: independent Watchlist candidate polls, quote-confirmed entries/exits, and portfolio summaries.\n'
            '- `quotes`: buy/sell observations, including shadow quotes; `quote_only=1` means no transaction/fill.\n'
            '- `observations`: price/source at a scheduled offset from first discovery; Pump.fun samples are fresh curve RPC reads (no indexer fallback or interpolation), and late/cancelled/missing rows are explicit. `excluded_from_stats=1` marks rows that must stay out of every statistic (session-end cancellation); `status=skipped_security_deep` means the coin failed the security filters, so offsets beyond 0 and 15 were deliberately not read.\n'
            '- `paper_fills`: simulated sale legs only; `out_amount_raw`/`snapshot_pnl_rub` are the primary snapshot fill, while `min_out_raw` and `conservative_100bps_*` preserve the separate 100 bps shadow; `portfolio` separates A and B; no on-chain executions.\n'
            '- `collector_meta`: queue drops and writer errors. Timestamps are UTC; raw token amounts are strings.\n'
            '- Future observations are labels/targets only; never use them as features available at discovery time.\n',
            encoding='utf-8',
        )
        # If the provider URL is synthesized from a runtime-only Helius key,
        # report only the hostname; never serialize its query string or key.
        http_url_for_host = os.getenv('SOLANA_HTTP_RPC_URL', '').strip()
        wss_url_for_host = os.getenv('SOLANA_WSS_RPC_URL', '').strip()
        if not http_url_for_host:
            http_url_for_host = (
                'https://mainnet.helius-rpc.com/' if os.getenv('HELIUS_API_KEY', '').strip()
                else 'https://api.mainnet-beta.solana.com'
            )
        if not wss_url_for_host:
            wss_url_for_host = (
                'wss://mainnet.helius-rpc.com/' if os.getenv('HELIUS_API_KEY', '').strip()
                else 'wss://api.mainnet-beta.solana.com'
            )
        http_host = urlsplit(http_url_for_host).hostname
        wss_host = urlsplit(wss_url_for_host).hostname
        with sqlite3.connect(f'file:{Path(self.db_path).resolve()}?mode=ro', uri=True) as db:
            counts = {table: db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in tables}
        (self.export_dir / 'manifest.json').write_text(json.dumps({
            'run_id': os.getenv('TELEMETRY_RUN_ID', 'unspecified'),
            'mode': 'paper_only', 'transactions_signed_or_sent': False,
            'quote_rows_are_fills': False, 'shadow_quotes_enabled': self.enable_shadow_quotes,
            'shadow_notional_rub': self.shadow_notional_rub,
            'observation_offsets_sec': self.observation_offsets_sec,
            'rpc_http_host': http_host, 'rpc_wss_host': wss_host,
            'counts': counts, 'timestamps': 'UTC ISO-8601',
            'bias_controls': ['append-only events', 'future outcomes separated from discovery-time features',
                              'filtered/skipped candidates retained'],
        }, ensure_ascii=False, indent=2), encoding='utf-8')


_COLLECTOR: Optional[CandidateCollector] = None


def set_collector(collector: Optional[CandidateCollector]) -> None:
    global _COLLECTOR
    _COLLECTOR = collector


def get_collector() -> Optional[CandidateCollector]:
    return _COLLECTOR


# --------------------------------------------------------------------------
# Post-run funnel report
# --------------------------------------------------------------------------
_SECURITY_REASON_CODES: tuple[tuple[str, str], ...] = (
    ('mint authority отозван', 'mint_authority_revoked_ok'),
    ('Pump.fun кривая использует не SOL quote', 'curve_not_sol_quoted'),
    ('pump_mayhem_mode_excluded', 'pump_mayhem_mode'),
    ('мигрировала с кривой', 'curve_migrated'),
    ('не прочитали mint и Pump.fun curve', 'mint_curve_read_failed'),
    ('отсутствует подтверждённый аккаунт', 'curve_account_missing'),
    ('проверка упала', 'check_raised'),
    ('supply выглядит разумно', 'supply_ok'),
    ('pump_standard_supply_verified', 'pump_supply_ok'),
)


def security_reason_code(text: Any) -> str:
    """Collapse one free-text filter reason into a short, stable code."""
    value = str(text or '').strip()
    if not value:
        return 'unknown'
    for marker, code in _SECURITY_REASON_CODES:
        if marker in value:
            return code
    head = value.split(' ', 1)[0].split('(', 1)[0].strip()
    if head and all(ch.isascii() and (ch.isalnum() or ch in '_.-') for ch in head):
        return head
    return 'other'


def _first_reason(reasons_json: Any) -> str:
    """Raw first reason of a single-reason row (arm signals, skip counters)."""
    try:
        parsed = json.loads(reasons_json or '[]')
    except (TypeError, ValueError):
        return 'unparsed'
    if isinstance(parsed, list) and parsed:
        return str(parsed[0])
    return 'none'


def _last_reason(reasons_json: Any) -> str:
    """The LAST reason is the verdict; the first is a passed check.

    Run #6 reported ``mint authority отозван — допечатать нельзя`` (the message
    of a check that *passed*) as the refusal reason for all 1 634 rejected
    coins, because the report read ``parsed[0]``.
    """
    try:
        parsed = json.loads(reasons_json or '[]')
    except (TypeError, ValueError):
        return 'unparsed'
    if isinstance(parsed, list) and parsed:
        return security_reason_code(parsed[-1])
    return 'none'


def _security_fail_report(db: Any, rows_of: Any, scalar: Any) -> dict[str, Any]:
    """Rocket rate among coins the security filters rejected (P0-2).

    A rocket is a coin whose price at offset 15 is at least +50 % of its
    offset-0 price. Percentages are reported over *observed* coins, never over
    all rejected ones: the unread majority is a blind spot, not a zero.
    """
    rejected = rows_of(
        "SELECT mint, reasons_json FROM decisions "
        "WHERE stage='security_and_market_filters' AND decision='fail'"
    )
    mints: dict[str, str] = {}
    for mint, reasons in rejected:
        if mint in mints:
            continue
        mints[str(mint)] = _last_reason(reasons)
    if not mints:
        return {
            'security_fail_total': 0, 'security_fail_observed': 0,
            'security_fail_rockets': 0, 'security_fail_rocket_share': None,
            'security_fail_reasons': {}, 'security_fail_observed_share': None,
        }
    prices: dict[str, dict[int, float]] = {}
    placeholders = ','.join('?' for _ in mints)
    rows = rows_of(
        f"SELECT mint, target_offset_sec, price_usd FROM observations "
        f"WHERE status='ok' AND price_usd IS NOT NULL AND mint IN ({placeholders})",
        tuple(mints),
    )
    for mint, offset, price in rows:
        try:
            offset_int = int(offset)
            value = float(price)
        except (TypeError, ValueError):
            continue
        if value > 0:
            prices.setdefault(str(mint), {})[offset_int] = value
    observed = {mint: series for mint, series in prices.items() if series}
    rockets = 0
    for series in observed.values():
        first = series.get(0)
        at15 = series.get(15)
        if first and at15 and at15 >= first * 1.5:
            rockets += 1
    reasons: dict[str, int] = {}
    for mint, reason in mints.items():
        if mint in observed:
            reasons[reason] = reasons.get(reason, 0) + 1
    return {
        'security_fail_total': len(mints),
        'security_fail_observed': len(observed),
        'security_fail_observed_share': round(len(observed) / len(mints), 4),
        'security_fail_rockets': rockets,
        'security_fail_rocket_share': (
            round(rockets / len(observed), 4) if observed else None
        ),
        'security_fail_reasons': dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
        'security_fail_skipped_deep': scalar(
            "SELECT COUNT(*) FROM observations WHERE status='skipped_security_deep'"
        ),
    }


def funnel_from_db(db_path: str | os.PathLike) -> dict[str, Any]:
    """Read-only funnel summary of one finished run.

    Answers the two questions run #1 left open without any new RPC:
    where candidates disappear between detection and an open position, and how
    many were admitted by the *current* delta cap versus the shadow caps.
    """
    path = Path(db_path)
    if not path.is_file():
        return {'available': False, 'reason': 'telemetry_db_missing'}
    report: dict[str, Any] = {'available': True, 'db': path.name}
    with sqlite3.connect(f'file:{path.resolve()}?mode=ro', uri=True) as db:
        def scalar(sql: str, params: tuple = ()) -> int:
            try:
                row = db.execute(sql, params).fetchone()
            except sqlite3.Error:
                # Trimmed/older databases may lack a table; report 0, not crash.
                return 0
            return int(row[0]) if row and row[0] is not None else 0

        def rows_of(sql: str, params: tuple = ()) -> list:
            try:
                return db.execute(sql, params).fetchall()
            except sqlite3.Error:
                return []

        report['candidates'] = scalar('SELECT COUNT(DISTINCT mint) FROM candidates')
        if not report['candidates']:
            report['candidates'] = scalar(
                "SELECT COUNT(DISTINCT mint) FROM detections WHERE detection_type='candidate_received'"
            )
        report['security_pass'] = scalar(
            "SELECT COUNT(DISTINCT mint) FROM decisions "
            "WHERE stage='security_and_market_filters' AND decision='pass'"
        )
        report['entry_state'] = scalar(
            "SELECT COUNT(DISTINCT mint) FROM decisions WHERE stage='entry_state'"
        )

        # Run #7: the arms are discovered from the telemetry, not hardcoded —
        # an added or renamed arm must appear in the funnel by itself.
        arms = sorted({
            str(stage)[: -len('_signal')]
            for (stage,) in rows_of(
                "SELECT DISTINCT stage FROM decisions WHERE stage LIKE '%_signal'"
            )
            if str(stage).endswith('_signal')
        })
        report['arms'] = arms
        signals: dict[str, int] = {}
        for arm in arms:
            rows = rows_of(
                'SELECT reasons_json FROM decisions WHERE stage=?', (f'{arm}_signal',)
            )
            counts: dict[str, int] = {}
            for (reasons,) in rows:
                reason = _first_reason(reasons)
                counts[reason] = counts.get(reason, 0) + 1
            report[f'{arm}_signal'] = dict(sorted(counts.items()))

        opened: dict[str, int] = {}
        skipped: dict[str, int] = {}
        for arm in arms:
            opened[arm] = scalar(
                "SELECT COUNT(*) FROM decisions WHERE stage=? AND decision='opened'",
                (f'{arm}_entry',),
            )
            skip_rows = rows_of(
                "SELECT reasons_json FROM decisions WHERE stage=? AND decision IN ('skipped','not_opened')",
                (f'{arm}_entry',),
            )
            per_reason: dict[str, int] = {}
            for (reasons,) in skip_rows:
                reason = _first_reason(reasons)
                per_reason[reason] = per_reason.get(reason, 0) + 1
            skipped[arm] = dict(sorted(per_reason.items(), key=lambda kv: -kv[1]))
        report['opened'] = opened
        report['entry_skips'] = skipped

        # Delta admission: how many entry_state rows pass the cap in force vs
        # the shadow caps (the "26 instead of ~40" question, answered by data).
        buckets: dict[str, int] = {}
        shadow: dict[str, int] = {}
        dev_buy: dict[str, int] = {}
        for (values_json,) in rows_of(
            "SELECT values_json FROM decisions WHERE stage='entry_state'"
        ):
            try:
                values = json.loads(values_json or '{}')
            except (TypeError, ValueError):
                continue
            bucket = str(values.get('delta_bucket') or 'unknown')
            buckets[bucket] = buckets.get(bucket, 0) + 1
            for key, flag in values.items():
                if key.startswith('pass_if_cap_') and flag:
                    shadow[key] = shadow.get(key, 0) + 1
            if values.get('dev_buy_sol') is None:
                dev_buy['unknown'] = dev_buy.get('unknown', 0) + 1
            else:
                dev_buy['bought'] = dev_buy.get('bought', 0) + 1
        report['delta_buckets'] = dict(sorted(buckets.items()))
        report['pass_if_cap'] = dict(sorted(shadow.items()))
        report['dev_buy_presence'] = dict(sorted(dev_buy.items()))
        # One-line chain: A is the reference band, so the funnel is read on it.
        a_signals = report.get('portfolio_a_signal') or {}
        report['chain'] = (
            f"candidates={report['candidates']} -> "
            f"security_pass={report['security_pass']} -> "
            f"birth_reject={a_signals.get('birth_out_of_band', 0)} -> "
            f"no_live_momentum={a_signals.get('no_live_momentum', 0)} -> "
            f"momentum_too_late={a_signals.get('momentum_too_late', 0)} -> "
            f"admits={a_signals.get('live_momentum_entry', 0)} -> "
            f"opened={opened['portfolio_a']} -> "
            f"cash_below_slot={(skipped['portfolio_a'].get('cash_below_slot_size', 0))}"
        )
        report['admitted_by_current_cap'] = scalar(
            "SELECT COUNT(*) FROM decisions WHERE stage='portfolio_a_signal' AND decision='pass'"
        )

        # Run #7: the creator gate is gone from the live path (it lost against
        # the average in 4/4 samples), so no gate funnel is reported.

        # --- P0-2: the blind spot that was never measured --------------------
        # Coins rejected by the security filters used to have no price series
        # at all, so "how many rockets do we lose before the entry" could not
        # be answered. They are now read at offsets 0 and 15 only.
        report.update(_security_fail_report(db, rows_of, scalar))

        # P0-1 acceptance: |observed_at - target_at| <= 250 ms per offset for
        # >= 99% of rows. Anything worse means the schedule, not the market,
        # is being measured.
        timing: dict[str, Any] = {}
        for offset, total, within in rows_of(
            """
            SELECT target_offset_sec, COUNT(*),
                   SUM(CASE WHEN ABS(
                       (julianday(observed_at_utc) - julianday(target_at_utc)) * 86400.0
                   ) <= 0.25 THEN 1 ELSE 0 END)
            FROM observations
            WHERE excluded_from_stats = 0
            GROUP BY target_offset_sec ORDER BY target_offset_sec
            """
        ):
            timing[str(int(offset))] = {
                'rows': int(total or 0),
                'within_250ms': int(within or 0),
                'share': round(int(within or 0) / max(1, int(total or 0)), 4),
            }
        report['observation_timing'] = timing
        # Cancelled offsets never enter any statistic: a read that the session
        # end interrupted is missing data, not a zero.
        status_counts = {
            str(status): int(count or 0)
            for status, count in rows_of(
                'SELECT status, COUNT(*) FROM observations GROUP BY status'
            )
        }
        report['observation_status'] = dict(sorted(status_counts.items()))
        report['cancelled_observations'] = status_counts.get(
            'cancelled_at_session_end', 0,
        )
        # The flag, not just the status string, is what every statistic uses.
        report['cancelled_excluded_from_stats'] = True
        report['excluded_observations'] = scalar(
            'SELECT COUNT(*) FROM observations WHERE excluded_from_stats = 1'
        )

        # P0-3 acceptance: error_detail must be present on every failed probe.
        probe_total, probe_failed, probe_with_detail = (
            rows_of(
                """
                SELECT COUNT(*),
                       SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN status != 'ok' AND error_detail IS NOT NULL
                                AND error_detail != '' THEN 1 ELSE 0 END)
                FROM creator_probes
                """
            )[0]
            if scalar('SELECT COUNT(*) FROM creator_probes')
            else (0, 0, 0)
        )
        report['error_detail_coverage'] = {
            'probes': int(probe_total or 0),
            'failed': int(probe_failed or 0),
            'with_detail': int(probe_with_detail or 0),
            'share': round(int(probe_with_detail or 0) / max(1, int(probe_failed or 0)), 4),
        }
    return report
