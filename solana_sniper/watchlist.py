"""Independent Pump.fun Watchlist paper portfolio.

This module never signs or submits transactions. It polls Pump bonding-curve
accounts via read-only RPC and uses Jupiter ExactIn quotes only for simulated
entry, exact-size sell precheck, and quote-confirmed paper exits.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp

from models import NewCoin, SwapQuote
from pumpportal_validation import PUMP_PROGRAM_ID, b58encode, derive_pump_bonding_curve
from price import WSOL, get_jupiter_quote, get_sol_usd
from telemetry import get_collector

PARTS = 3


@dataclass(frozen=True)
class WatchlistConfig:
    max_positions: int = 3
    initial_balance_rub: float = 0.0
    poll_sec: float = 1.0
    exit_poll_sec: float = 0.75
    min_age_sec: float = 3.5
    max_age_sec: float = 40.0
    rising_intervals_required: int = 2
    interval_growth_sol: float = 0.15
    current_growth_sol: float = 0.25
    total_growth_sol: float = 2.0
    min_r_sol: float = 4.0
    max_r_sol: float = 18.0
    max_retrace_sol: float = 1.5
    max_initial_r_sol: float = 10.0
    stop_loss_pct: float = 12.0
    take_profit_pct: float = 35.0
    stagnation_sec: float = 35.0
    stagnation_max_ath_pct: float = 10.0
    break_even_pct: float = 5.0
    trail_retention_pct: float = 82.0
    max_hold_before_tp1_sec: float = 120.0
    max_hold_after_tp1_sec: float = 900.0
    rpc_timeout_sec: float = 3.0
    quote_retries: int = 1
    quote_retry_delay_sec: float = 0.2

    @classmethod
    def from_env(cls) -> 'WatchlistConfig':
        defaults = cls()
        values: dict[str, Any] = {}
        for name, default in asdict(defaults).items():
            raw = os.getenv(f'WATCHLIST_{name.upper()}')
            if raw is None or not raw.strip():
                continue
            try:
                if isinstance(default, bool):
                    values[name] = raw.strip().lower() in {'1', 'true', 'yes', 'on'}
                elif isinstance(default, int):
                    values[name] = int(raw)
                else:
                    values[name] = float(raw)
            except (TypeError, ValueError, OverflowError):
                values[name] = default
        cfg = cls(**values)
        return cls(
            **{
                **asdict(cfg),
                'max_positions': min(max(1, cfg.max_positions), 3),
                'initial_balance_rub': max(0.0, cfg.initial_balance_rub),
                'poll_sec': min(max(cfg.poll_sec, 0.25), 5.0),
                'exit_poll_sec': min(max(cfg.exit_poll_sec, 0.25), 1.0),
                'min_age_sec': max(0.0, cfg.min_age_sec),
                'max_age_sec': max(cfg.min_age_sec, cfg.max_age_sec),
                'rising_intervals_required': max(1, cfg.rising_intervals_required),
                'quote_retries': min(max(cfg.quote_retries, 0), 2),
                'quote_retry_delay_sec': max(0.0, cfg.quote_retry_delay_sec),
                'rpc_timeout_sec': min(max(cfg.rpc_timeout_sec, 0.5), 8.0),
            }
        )


def watchlist_entry_signal(
    *,
    age_sec: float,
    r_sol_t0: float,
    r_sol_prev: float,
    r_sol_now: float,
    r_sol_max: float,
    rising_intervals: int,
    config: WatchlistConfig,
) -> bool:
    """Pure, deterministic Watchlist entry gate using only current/past samples."""
    delta = r_sol_now - r_sol_prev
    return all((
        config.min_age_sec <= age_sec <= config.max_age_sec,
        rising_intervals >= config.rising_intervals_required,
        delta >= config.current_growth_sol,
        r_sol_now - r_sol_t0 >= config.total_growth_sol,
        config.min_r_sol <= r_sol_now <= config.max_r_sol,
        r_sol_max - r_sol_now <= config.max_retrace_sol,
    ))


def watchlist_exit_quote_confirms(
    reason: str,
    quote_price_usd: float,
    entry_price_usd: float,
    ath_price_usd: float,
    config: WatchlistConfig,
) -> bool:
    if quote_price_usd <= 0 or entry_price_usd <= 0:
        return False
    if reason.startswith('SL'):
        return quote_price_usd <= entry_price_usd * (1.0 - config.stop_loss_pct / 100.0)
    if reason.startswith('TP1'):
        return quote_price_usd >= entry_price_usd * (1.0 + config.take_profit_pct / 100.0)
    if reason.startswith('Break-Even'):
        return quote_price_usd <= entry_price_usd * (1.0 + config.break_even_pct / 100.0)
    if reason.startswith('Trailing Stop'):
        return quote_price_usd <= ath_price_usd * (config.trail_retention_pct / 100.0)
    # Stagnation/timeouts are time-based exits; a positive exact-size quote is
    # sufficient, matching the base paper strategy's timeout safeguards.
    return True


def parse_curve_state(raw: bytes) -> Optional[dict[str, Any]]:
    if not raw or len(raw) < 115:
        return None
    try:
        virtual_tokens, virtual_sol = struct.unpack_from('<QQ', raw, 8)
        real_sol = struct.unpack_from('<Q', raw, 32)[0]
        token_total_supply = struct.unpack_from('<Q', raw, 40)[0]
    except struct.error:
        return None
    quote_bytes = raw[83:115]
    return {
        'real_sol_reserves_raw': real_sol,
        'r_sol': real_sol / 1_000_000_000,
        'virtual_token_reserves_raw': virtual_tokens,
        'virtual_sol_reserves_raw': virtual_sol,
        'token_total_supply_raw': token_total_supply,
        'complete': bool(raw[48]),
        'program_creator': b58encode(raw[49:81]) if any(raw[49:81]) else None,
        'mayhem': bool(raw[81]),
        'quote_mint': b58encode(quote_bytes) if any(quote_bytes) else WSOL,
    }


def curve_price_sol(raw: bytes) -> Optional[float]:
    state = parse_curve_state(raw)
    if not state or state['complete'] or state['mayhem'] or state['quote_mint'] != WSOL:
        return None
    v_tokens = int(state['virtual_token_reserves_raw'])
    v_sol = int(state['virtual_sol_reserves_raw'])
    if v_tokens <= 0 or v_sol <= 0:
        return None
    return (v_sol / 1_000_000_000) / (v_tokens / 1_000_000)


@dataclass
class WatchCandidate:
    coin: NewCoin
    t0_epoch: float
    r_sol_t0: float
    r_sol_prev: float
    r_sol_max: float
    rising_intervals: int = 0
    poll_count: int = 0
    poll_errors: int = 0
    latest_delta: float = 0.0
    last_poll_epoch: float = 0.0
    status: str = 'watching'


@dataclass
class WatchlistPosition:
    mint: str
    source: str
    curve: str
    opened_at: float
    entry_price_usd: float
    tokens_raw: int
    remaining_tokens_raw: int
    decimals: int
    entry_rub: float
    entry_fee_rub: float
    invested_rub: float
    ath_price_usd: float
    r_sol_t0: float
    tp1_done: bool = False
    sold_tokens_raw: int = 0
    sold_fraction: float = 0.0
    realized_rub: float = 0.0
    fees_rub: float = 0.0
    exit_pending: bool = False
    pending_exit_reason: str = ''
    pending_exit_amount_raw: int = 0
    last_exit_attempt: float = 0.0
    exit_in_progress: bool = False
    mark_quote_in_progress: bool = False
    last_mark_price_usd: Optional[float] = None
    last_mark_observed_at: Optional[float] = None
    closed: bool = False


class WatchlistSimulator:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        settings: Any,
        state_path: str | Path,
        config: Optional[WatchlistConfig] = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.state_path = Path(state_path)
        self.config = config or WatchlistConfig.from_env()
        self.candidates: dict[str, WatchCandidate] = {}
        self.positions: dict[str, WatchlistPosition] = {}
        self.closed_trades: list[dict[str, Any]] = []
        configured_balance = float(self.config.initial_balance_rub or 0.0)
        self.balance_rub = configured_balance if configured_balance > 0 else float(getattr(settings, 'deposit_rub', 1500.0))
        self.initial_balance_rub = self.balance_rub
        self.total_fees_rub = 0.0
        self.entry_count = 0
        self.pending_entries = 0
        self.pending_entry_reservations_rub = 0.0
        self.poll_errors = 0
        self._sol_cache: tuple[float, Optional[float]] = (0.0, None)
        self._rpc_sem = asyncio.Semaphore(2)
        self._stop = asyncio.Event()
        self._candidate_task: Optional[asyncio.Task[Any]] = None
        self._position_task: Optional[asyncio.Task[Any]] = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._started = False
        self._closed = False

    def start(self) -> None:
        if self._started or self._closed:
            return
        self._started = True
        self._save_state()
        self._candidate_task = asyncio.create_task(self._candidate_poll_loop(), name='watchlist-candidate-poller')
        self._position_task = asyncio.create_task(self._position_poll_loop(), name='watchlist-position-poller')

    def _spawn(self, coro: Any, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _event(self, mint: str, event_type: str, status: str, values: dict[str, Any]) -> None:
        collector = get_collector()
        if collector is not None:
            try:
                collector.record_watchlist_event(mint, 'Pump.fun', event_type, status, values)
            except Exception:
                pass

    def _eligible(self, coin: NewCoin) -> tuple[bool, str]:
        packet = coin.packet if isinstance(coin.packet, dict) else {}
        if coin.source != 'Pump.fun':
            return False, 'watchlist_pump_fun_only'
        if coin.decimals != 6:
            return False, 'watchlist_requires_decimals_6'
        if packet.get('pump_standard_supply_valid') is not True:
            return False, 'watchlist_requires_standard_supply'
        if packet.get('pump_curve_mayhem_mode') is not False:
            return False, 'watchlist_mayhem_or_mode_unverified'
        if packet.get('pump_curve_quote_mint') != WSOL:
            return False, 'watchlist_requires_wsol_curve'
        if not coin.curve_raw or len(coin.curve_raw) < 115 or not coin.curve:
            return False, 'watchlist_requires_verified_curve_sample'
        if derive_pump_bonding_curve(coin.mint) != coin.curve:
            return False, 'watchlist_curve_pda_mismatch'
        state = parse_curve_state(coin.curve_raw)
        if not state or state['complete'] or state['mayhem'] or state['quote_mint'] != WSOL:
            return False, 'watchlist_curve_state_not_eligible'
        return True, 'eligible'

    def consider_candidate(self, coin: NewCoin) -> None:
        """Enqueue a passed base-filter candidate; does not reserve main slots."""
        if self._closed:
            return
        valid, reason = self._eligible(coin)
        if not valid:
            if coin.source == 'Pump.fun':
                self._event(coin.mint, 'candidate_rejected', 'not_eligible', {
                    'reason': reason, 'program_creator': coin.program_creator,
                    'tx_payer': coin.tx_payer, 'paper_only': True,
                })
            return
        if coin.mint in self.candidates or coin.mint in self.positions:
            return
        state = parse_curve_state(coin.curve_raw or b'')
        if not state:
            return
        t0_epoch = float(coin.curve_observed_at or coin.found_at)
        r_sol_t0 = float(state['r_sol'])
        if r_sol_t0 > self.config.max_initial_r_sol:
            self._event(coin.mint, 'candidate_cancelled', 'initial_reserve_above_limit', {
                't0_observed_at_utc': _utc(t0_epoch), 'r_sol_t0': r_sol_t0,
                'limit_r_sol': self.config.max_initial_r_sol,
            })
            return
        age_at_registration = max(0.0, time.time() - t0_epoch)
        if age_at_registration > self.config.max_age_sec:
            self._event(coin.mint, 'candidate_cancelled', 'no_signal_window_expired_before_watch', {
                't0_observed_at_utc': _utc(t0_epoch), 'registration_age_sec': age_at_registration,
                'poll_count': 0,
            })
            return
        candidate = WatchCandidate(
            coin=coin,
            t0_epoch=t0_epoch,
            r_sol_t0=r_sol_t0,
            r_sol_prev=r_sol_t0,
            r_sol_max=r_sol_t0,
            last_poll_epoch=t0_epoch,
        )
        self.candidates[coin.mint] = candidate
        self._event(coin.mint, 'candidate_registered', 'watching', {
            't0_observed_at_utc': _utc(t0_epoch), 'r_sol_t0': r_sol_t0,
            'curve': coin.curve, 'program_creator': coin.program_creator,
            'tx_payer': coin.tx_payer, 'max_watch_age_sec': self.config.max_age_sec,
            'poll_interval_sec': self.config.poll_sec,
            'watchlist_slot_cap': self.config.max_positions,
            'independent_of_main_slots': True,
        })

    async def _rpc_multiple_accounts(self, candidates: list[WatchCandidate]) -> dict[str, tuple[Optional[bytes], Optional[str]]]:
        result: dict[str, tuple[Optional[bytes], Optional[str]]] = {}
        for start in range(0, len(candidates), 100):
            batch = candidates[start:start + 100]
            addresses = [candidate.coin.curve for candidate in batch]
            payload = {
                'jsonrpc': '2.0', 'id': 1, 'method': 'getMultipleAccounts',
                'params': [addresses, {'encoding': 'base64', 'commitment': 'processed'}],
            }
            try:
                async with self._rpc_sem:
                    async with asyncio.timeout(self.config.rpc_timeout_sec):
                        async with self.session.post(
                            self.settings.http_rpc_url, json=payload,
                            timeout=aiohttp.ClientTimeout(total=self.config.rpc_timeout_sec),
                        ) as response:
                            if response.status != 200:
                                raise RuntimeError(f'rpc_http_{response.status}')
                            data = await response.json()
                if not isinstance(data, dict) or data.get('error'):
                    raise RuntimeError('rpc_error_response')
                accounts = ((data.get('result') or {}).get('value')) or []
                for candidate, account in zip(batch, accounts):
                    if not isinstance(account, dict):
                        result[candidate.coin.mint] = (None, 'curve_account_missing')
                        continue
                    if account.get('owner') != PUMP_PROGRAM_ID:
                        result[candidate.coin.mint] = (None, 'curve_owner_mismatch')
                        continue
                    expected = derive_pump_bonding_curve(candidate.coin.mint)
                    if expected is None or expected != candidate.coin.curve:
                        result[candidate.coin.mint] = (None, 'curve_pda_mismatch')
                        continue
                    encoded = account.get('data') or []
                    if not isinstance(encoded, list) or not encoded or (len(encoded) > 1 and encoded[1] != 'base64'):
                        result[candidate.coin.mint] = (None, 'curve_data_encoding_invalid')
                        continue
                    try:
                        raw = base64.b64decode(encoded[0], validate=True)
                    except Exception:
                        raw = b''
                    result[candidate.coin.mint] = (raw if len(raw) >= 115 else None,
                                                   None if len(raw) >= 115 else 'curve_layout_short')
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                for candidate in batch:
                    result[candidate.coin.mint] = (None, type(exc).__name__)
        return result

    async def _candidate_poll_loop(self) -> None:
        next_tick = time.monotonic()
        try:
            while not self._stop.is_set():
                cycle_started = time.monotonic()
                active = list(self.candidates.values())
                if active:
                    account_results = await self._rpc_multiple_accounts(active)
                    observed_epoch = time.time()
                    for candidate in active:
                        if self.candidates.get(candidate.coin.mint) is not candidate:
                            continue
                        age = max(0.0, observed_epoch - candidate.t0_epoch)
                        raw, error = account_results.get(candidate.coin.mint, (None, 'rpc_result_missing'))
                        candidate.poll_count += 1
                        if raw is None:
                            candidate.poll_errors += 1
                            self.poll_errors += 1
                            if candidate.poll_count % 5 == 0 or age >= self.config.max_age_sec:
                                self._event(candidate.coin.mint, 'curve_poll', 'error', {
                                    'observed_at_utc': _utc(observed_epoch), 'age_sec': round(age, 3),
                                    'poll_count': candidate.poll_count, 'error_type': error,
                                    'poll_latency_ms': round((time.monotonic() - cycle_started) * 1000.0, 2),
                                })
                            if age > self.config.max_age_sec:
                                self._cancel_candidate(candidate, 'no_signal_within_40s')
                            continue
                        state = parse_curve_state(raw)
                        if not state or state['complete'] or state['mayhem'] or state['quote_mint'] != WSOL:
                            self._cancel_candidate(candidate, 'curve_no_longer_eligible')
                            continue
                        r_sol_now = float(state['r_sol'])
                        candidate.latest_delta = r_sol_now - candidate.r_sol_prev
                        if candidate.latest_delta > self.config.interval_growth_sol:
                            candidate.rising_intervals += 1
                        candidate.r_sol_max = max(candidate.r_sol_max, r_sol_now)
                        candidate.r_sol_prev = r_sol_now
                        candidate.last_poll_epoch = observed_epoch
                        if candidate.poll_count % 5 == 0:
                            self._event(candidate.coin.mint, 'curve_poll', 'sample', {
                                'observed_at_utc': _utc(observed_epoch), 'age_sec': round(age, 3),
                                'r_sol_t0': candidate.r_sol_t0, 'r_sol_prev': r_sol_now,
                                'delta_r_sol': round(candidate.latest_delta, 6),
                                'max_r_sol': candidate.r_sol_max,
                                'rising_intervals_over_0_15': candidate.rising_intervals,
                                'poll_count': candidate.poll_count,
                                'poll_latency_ms': round((time.monotonic() - cycle_started) * 1000.0, 2),
                            })
                        signal = watchlist_entry_signal(
                            age_sec=age,
                            r_sol_t0=candidate.r_sol_t0,
                            r_sol_prev=(r_sol_now - candidate.latest_delta),
                            r_sol_now=r_sol_now,
                            r_sol_max=candidate.r_sol_max,
                            rising_intervals=candidate.rising_intervals,
                            config=self.config,
                        )
                        if age > self.config.max_age_sec:
                            self._cancel_candidate(candidate, 'no_signal_within_40s')
                        elif signal:
                            candidate.status = 'entry_pending'
                            self._event(candidate.coin.mint, 'entry_signal', 'qualified', {
                                'observed_at_utc': _utc(observed_epoch), 'age_sec': round(age, 3),
                                'r_sol_t0': candidate.r_sol_t0, 'r_sol_now': r_sol_now,
                                'delta_r_sol': round(candidate.latest_delta, 6),
                                'total_growth_r_sol': round(r_sol_now - candidate.r_sol_t0, 6),
                                'max_r_sol': candidate.r_sol_max,
                                'retrace_r_sol': round(candidate.r_sol_max - r_sol_now, 6),
                                'rising_intervals_over_0_15': candidate.rising_intervals,
                                'poll_count': candidate.poll_count,
                                'causal': True,
                            })
                            self.candidates.pop(candidate.coin.mint, None)
                            self._spawn(self._open_from_signal(candidate), f'watchlist-entry-{candidate.coin.mint[:7]}')
                next_tick += self.config.poll_sec
                wait = next_tick - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                else:
                    # Do not spin or issue catch-up bursts if RPC is slow.
                    next_tick = time.monotonic()
        except asyncio.CancelledError:
            raise

    def _cancel_candidate(self, candidate: WatchCandidate, reason: str) -> None:
        self.candidates.pop(candidate.coin.mint, None)
        candidate.status = 'cancelled'
        self._event(candidate.coin.mint, 'candidate_cancelled', reason, {
            't0_observed_at_utc': _utc(candidate.t0_epoch), 'r_sol_t0': candidate.r_sol_t0,
            'max_r_sol': candidate.r_sol_max, 'last_r_sol': candidate.r_sol_prev,
            'rising_intervals_over_0_15': candidate.rising_intervals,
            'poll_count': candidate.poll_count, 'poll_errors': candidate.poll_errors,
            'max_age_sec': self.config.max_age_sec,
        })

    async def _cached_sol_usd(self) -> Optional[float]:
        cached_at, cached = self._sol_cache
        if cached and time.monotonic() - cached_at < 30.0:
            return cached
        try:
            value = await get_sol_usd(self.session)
        except Exception:
            value = None
        if value and value > 0:
            self._sol_cache = (time.monotonic(), float(value))
            return float(value)
        return None

    async def _quote_with_retries(
        self, input_mint: str, output_mint: str, amount_raw: int,
    ) -> tuple[Optional[SwapQuote], float]:
        quote = None
        latency_ms = 0.0
        attempts = self.config.quote_retries + 1
        for attempt in range(attempts):
            started = time.monotonic()
            quote = await get_jupiter_quote(
                self.session, input_mint, output_mint, amount_raw, slippage_bps=100,
            )
            latency_ms += (time.monotonic() - started) * 1000.0
            if quote is not None:
                return quote, latency_ms
            if attempt + 1 < attempts and self.config.quote_retry_delay_sec:
                await asyncio.sleep(self.config.quote_retry_delay_sec * (attempt + 1))
        return None, latency_ms

    def _record_quote(
        self, mint: str, stage: str, side: str, input_mint: str,
        output_mint: str, amount_raw: int, quote: Optional[SwapQuote],
        latency_ms: float, error_type: Optional[str],
    ) -> Optional[str]:
        collector = get_collector()
        if collector is None:
            return None
        try:
            return collector.record_quote(
                mint, 'Pump.fun', stage, side, input_mint, output_mint,
                amount_raw, quote, 'ok' if quote else 'no_route',
                100, latency_ms, error_type,
            )
        except Exception:
            return None

    async def _open_from_signal(self, candidate: WatchCandidate) -> None:
        coin = candidate.coin
        mint = coin.mint
        active_count = sum(1 for position in self.positions.values() if not position.closed)
        if active_count + self.pending_entries >= self.config.max_positions:
            self._event(mint, 'entry_skipped', 'watchlist_slots_full', {
                'open_positions': active_count, 'pending_entries': self.pending_entries,
                'max_positions': self.config.max_positions,
                'independent_of_main_slots': True,
            })
            return
        self.pending_entries += 1
        reservation_rub = 0.0
        try:
            entry_rub = float(self.settings.max_trade_rub)
            if not (entry_rub > 0.0):
                self._event(mint, 'entry_skipped', 'max_trade_rub_invalid', {
                    'max_trade_rub': entry_rub,
                })
                return
            sol_usd = await self._cached_sol_usd()
            if not sol_usd or sol_usd <= 0:
                self._event(mint, 'entry_skipped', 'sol_price_unavailable', {'entry_rub': entry_rub})
                return
            sol_rub = sol_usd * float(self.settings.usd_rub_rate)
            input_sol_raw = int(entry_rub / sol_rub * 1_000_000_000) if sol_rub > 0 else 0
            if input_sol_raw <= 0:
                self._event(mint, 'entry_skipped', 'amount_below_one_lamport', {'entry_rub': entry_rub})
                return
            entry_fee_rub = float(self.settings.entry_fee_sol) * sol_rub
            reservation_rub = entry_rub + entry_fee_rub
            if self.balance_rub - self.pending_entry_reservations_rub + 1e-9 < reservation_rub:
                self._event(mint, 'entry_skipped', 'watchlist_cash_below_exact_max_trade', {
                    'requested_trade_rub': entry_rub,
                    'required_with_entry_fee_rub': reservation_rub,
                    'available_rub': self.balance_rub - self.pending_entry_reservations_rub,
                    'max_trade_rub': float(self.settings.max_trade_rub),
                })
                reservation_rub = 0.0
                return
            self.pending_entry_reservations_rub += reservation_rub

            buy, buy_latency = await self._quote_with_retries(WSOL, mint, input_sol_raw)
            buy_event_id = self._record_quote(
                mint, 'watchlist_entry_buy', 'buy', WSOL, mint, input_sol_raw,
                buy, buy_latency, None if buy else 'quote_unavailable',
            )
            if not buy:
                self._event(mint, 'entry_skipped', 'buy_quote_unavailable', {
                    'requested_rub': entry_rub, 'input_sol_raw': input_sol_raw,
                    'buy_quote_latency_ms': round(buy_latency, 2),
                })
                return
            impact_limit = float(getattr(self.settings, 'max_entry_price_impact_pct', 2.0))
            if buy.price_impact_pct is None or buy.price_impact_pct > impact_limit:
                self._event(mint, 'entry_skipped', 'buy_impact_unverified_or_above_limit', {
                    'impact_pct': buy.price_impact_pct, 'limit_pct': impact_limit,
                    'buy_quote_latency_ms': round(buy_latency, 2),
                })
                return
            tokens_raw = int(buy.min_out_raw)
            if tokens_raw <= 0:
                self._event(mint, 'entry_skipped', 'zero_token_output', {})
                return

            sell_precheck, sell_latency = await self._quote_with_retries(mint, WSOL, tokens_raw)
            precheck_event_id = self._record_quote(
                mint, 'watchlist_entry_sell_precheck', 'sell', mint, WSOL,
                tokens_raw, sell_precheck, sell_latency,
                None if sell_precheck else 'quote_unavailable',
            )
            if not sell_precheck:
                self._event(mint, 'entry_skipped', 'exact_size_sell_precheck_unavailable', {
                    'tokens_raw': tokens_raw, 'sell_precheck_latency_ms': round(sell_latency, 2),
                })
                return
            if sell_precheck.price_impact_pct is None or sell_precheck.price_impact_pct > impact_limit:
                self._event(mint, 'entry_skipped', 'sell_precheck_impact_unverified_or_above_limit', {
                    'impact_pct': sell_precheck.price_impact_pct, 'limit_pct': impact_limit,
                    'tokens_raw': tokens_raw,
                })
                return

            decimals = int(coin.decimals or 6)
            tokens_ui = tokens_raw / (10 ** decimals)
            cost_rub = input_sol_raw / 1_000_000_000 * sol_rub
            cost_usd = cost_rub / float(self.settings.usd_rub_rate)
            entry_price = cost_usd / tokens_ui if tokens_ui > 0 else 0.0
            if entry_price <= 0:
                self._event(mint, 'entry_skipped', 'invalid_entry_price', {})
                return
            invested_rub = cost_rub + entry_fee_rub
            opened_at = time.time()
            position = WatchlistPosition(
                mint=mint, source=coin.source, curve=str(coin.curve or ''),
                opened_at=opened_at, entry_price_usd=entry_price,
                tokens_raw=tokens_raw, remaining_tokens_raw=tokens_raw,
                decimals=decimals, entry_rub=cost_rub,
                entry_fee_rub=entry_fee_rub, invested_rub=invested_rub,
                ath_price_usd=entry_price, r_sol_t0=candidate.r_sol_t0,
                fees_rub=entry_fee_rub,
            )
            self.balance_rub -= invested_rub
            self.total_fees_rub += entry_fee_rub
            self.positions[mint] = position
            self.entry_count += 1
            self._save_state()
            self._event(mint, 'entry_opened', 'quote_confirmed', {
                'opened_at_utc': _utc(opened_at), 'entry_rub': round(cost_rub, 4),
                'position_size_policy': 'exact MAX_TRADE_RUB per entry; fees reserved separately',
                'requested_trade_rub': float(self.settings.max_trade_rub),
                'max_trade_rub': float(self.settings.max_trade_rub),
                'watchlist_initial_balance_rub': self.initial_balance_rub,
                'entry_fee_rub': round(entry_fee_rub, 4), 'invested_rub': round(invested_rub, 4),
                'input_sol_raw': input_sol_raw, 'tokens_raw': tokens_raw,
                'entry_price_usd': entry_price, 'decimals': decimals,
                'buy_quote_event_id': buy_event_id,
                'sell_precheck_quote_event_id': precheck_event_id,
                'buy_quote_latency_ms': round(buy_latency, 2),
                'sell_precheck_latency_ms': round(sell_latency, 2),
                'r_sol_t0': candidate.r_sol_t0,
                'watchlist_open_positions': sum(1 for p in self.positions.values() if not p.closed),
                'watchlist_max_positions': self.config.max_positions,
                'main_slots_untouched': True,
                'paper_only': True,
            })
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._event(mint, 'entry_error', 'non_blocking', {
                'error_type': type(exc).__name__, 'paper_only': True,
            })
        finally:
            self.pending_entries = max(0, self.pending_entries - 1)
            self.pending_entry_reservations_rub = max(
                0.0, self.pending_entry_reservations_rub - reservation_rub,
            )

    async def _position_poll_loop(self) -> None:
        next_tick = time.monotonic()
        try:
            while not self._stop.is_set():
                cycle_started = time.monotonic()
                active = [position for position in self.positions.values() if not position.closed]
                if active:
                    # Reuse the same batched, owner/PDA-checked account reads.
                    pseudo_candidates = [
                        WatchCandidate(
                            coin=NewCoin(
                                mint=position.mint, source='Pump.fun', signature='',
                                curve=position.curve,
                            ),
                            t0_epoch=position.opened_at,
                            r_sol_t0=position.r_sol_t0,
                            r_sol_prev=0.0,
                            r_sol_max=0.0,
                        )
                        for position in active
                    ]
                    account_results = await self._rpc_multiple_accounts(pseudo_candidates)
                    observed_epoch = time.time()
                    sol_usd = await self._cached_sol_usd()
                    for position in active:
                        if position.closed:
                            continue
                        held = max(0.0, observed_epoch - position.opened_at)
                        raw, error = account_results.get(position.mint, (None, 'rpc_result_missing'))
                        mark_price: Optional[float] = None
                        if raw is not None:
                            spot_sol = curve_price_sol(raw)
                            if spot_sol and sol_usd and sol_usd > 0:
                                mark_price = spot_sol * sol_usd
                        if mark_price is None:
                            self.poll_errors += 1
                            if position.mark_quote_in_progress is False and position.remaining_tokens_raw > 0:
                                position.mark_quote_in_progress = True
                                self._spawn(
                                    self._mark_from_exact_quote(position),
                                    f'watchlist-mark-{position.mint[:7]}',
                                )
                        else:
                            position.last_mark_price_usd = mark_price
                            position.last_mark_observed_at = observed_epoch
                            position.ath_price_usd = max(position.ath_price_usd, mark_price)
                        if self._should_log_position_poll(position):
                            self._event(position.mint, 'position_poll', 'sample' if mark_price else 'price_unavailable', {
                                'observed_at_utc': _utc(observed_epoch), 'held_sec': round(held, 3),
                                'mark_price_usd': mark_price,
                                'mark_price_source': 'pump_curve' if mark_price else 'pending_exact_sell_quote',
                                'entry_price_usd': position.entry_price_usd,
                                'ath_price_usd': position.ath_price_usd,
                                'tp1_done': position.tp1_done,
                                'remaining_tokens_raw': position.remaining_tokens_raw,
                                'exit_pending': position.exit_pending,
                                'curve_poll_error': error,
                                'poll_latency_ms': round((time.monotonic() - cycle_started) * 1000.0, 2),
                            })
                        if position.exit_in_progress:
                            continue
                        if position.exit_pending:
                            if observed_epoch - position.last_exit_attempt >= self.config.exit_poll_sec:
                                self._spawn(self._try_exit(position), f'watchlist-exit-retry-{position.mint[:7]}')
                            continue

                        timeout = (
                            self.config.max_hold_after_tp1_sec if position.tp1_done
                            else self.config.max_hold_before_tp1_sec
                        )
                        reason: Optional[str] = None
                        amount_raw = position.remaining_tokens_raw
                        if held >= timeout:
                            reason = 'Timeout after TP1 (900s)' if position.tp1_done else 'Timeout before TP1 (120s)'
                        elif (
                            position.last_mark_price_usd is not None
                            and position.last_mark_observed_at is not None
                            and observed_epoch - position.last_mark_observed_at
                                <= max(2.5, 3.0 * self.config.exit_poll_sec)
                        ):
                            pct = (position.last_mark_price_usd / position.entry_price_usd - 1.0) * 100.0
                            if not position.tp1_done:
                                if pct <= -self.config.stop_loss_pct:
                                    reason = 'SL (Watchlist −12%)'
                                elif pct >= self.config.take_profit_pct:
                                    reason = 'TP1 (Watchlist +35%)'
                                    amount_raw = max(1, position.remaining_tokens_raw // 2)
                                elif (
                                    held >= self.config.stagnation_sec
                                    and (position.ath_price_usd / position.entry_price_usd - 1.0) * 100.0
                                        < self.config.stagnation_max_ath_pct
                                ):
                                    reason = 'Stagnation from opened_at (35s; ATH below +10%)'
                            else:
                                if pct <= self.config.break_even_pct:
                                    reason = 'Break-Even (Watchlist ≤+5% from entry)'
                                elif position.last_mark_price_usd <= position.ath_price_usd * (
                                    self.config.trail_retention_pct / 100.0
                                ):
                                    reason = 'Trailing Stop (Watchlist ≤82% of ATH)'
                        if reason:
                            position.exit_pending = True
                            position.pending_exit_reason = reason
                            position.pending_exit_amount_raw = min(amount_raw, position.remaining_tokens_raw)
                            position.last_exit_attempt = 0.0
                            self._event(position.mint, 'exit_trigger', 'triggered', {
                                'observed_at_utc': _utc(observed_epoch), 'reason': reason,
                                'held_sec': round(held, 3),
                                'mark_price_usd': position.last_mark_price_usd,
                                'entry_price_usd': position.entry_price_usd,
                                'ath_price_usd': position.ath_price_usd,
                                'requested_tokens_raw': position.pending_exit_amount_raw,
                                'remaining_tokens_raw': position.remaining_tokens_raw,
                                'tp1_done': position.tp1_done,
                                'requires_exact_sell_quote_confirmation': True,
                            })
                            self._spawn(self._try_exit(position), f'watchlist-exit-{position.mint[:7]}')
                next_tick += self.config.exit_poll_sec
                wait = next_tick - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                else:
                    next_tick = time.monotonic()
        except asyncio.CancelledError:
            raise

    def _should_log_position_poll(self, position: WatchlistPosition) -> bool:
        # Keep full-resolution checks in memory; persist a compact causal sample
        # every five cycles, while always recording triggers and quote outcomes.
        counter = int(getattr(position, '_poll_count', 0)) + 1
        setattr(position, '_poll_count', counter)
        return counter % 5 == 0

    async def _mark_from_exact_quote(self, position: WatchlistPosition) -> None:
        try:
            if position.remaining_tokens_raw <= 0:
                return
            quote, latency = await self._quote_with_retries(
                position.mint, WSOL, position.remaining_tokens_raw,
            )
            event_id = self._record_quote(
                position.mint, 'watchlist_mark_sell_quote', 'sell',
                position.mint, WSOL, position.remaining_tokens_raw,
                quote, latency, None if quote else 'quote_unavailable',
            )
            sol_usd = await self._cached_sol_usd()
            if quote and sol_usd and sol_usd > 0:
                token_ui = position.remaining_tokens_raw / (10 ** position.decimals)
                price = (quote.min_out_raw / 1_000_000_000) * sol_usd / token_ui if token_ui > 0 else None
                if price and price > 0:
                    position.last_mark_price_usd = price
                    position.last_mark_observed_at = time.time()
                    position.ath_price_usd = max(position.ath_price_usd, price)
                    self._event(position.mint, 'mark_quote', 'ok', {
                        'observed_at_utc': _utc(position.last_mark_observed_at),
                        'price_usd': price, 'quote_event_id': event_id,
                        'latency_ms': round(latency, 2),
                        'exact_tokens_raw': position.remaining_tokens_raw,
                    })
                else:
                    self._event(position.mint, 'mark_quote', 'invalid_price', {
                        'quote_event_id': event_id, 'latency_ms': round(latency, 2),
                    })
            else:
                self._event(position.mint, 'mark_quote', 'unavailable', {
                    'quote_event_id': event_id, 'latency_ms': round(latency, 2),
                })
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._event(position.mint, 'mark_quote', 'error', {'error_type': type(exc).__name__})
        finally:
            position.mark_quote_in_progress = False

    async def _try_exit(self, position: WatchlistPosition) -> None:
        if position.closed or position.exit_in_progress or not position.exit_pending:
            return
        position.exit_in_progress = True
        position.last_exit_attempt = time.time()
        amount_raw = min(position.pending_exit_amount_raw, position.remaining_tokens_raw)
        reason = position.pending_exit_reason
        try:
            if amount_raw <= 0:
                position.exit_pending = False
                return
            quote, latency = await self._quote_with_retries(position.mint, WSOL, amount_raw)
            quote_event_id = self._record_quote(
                position.mint, 'watchlist_position_exit', 'sell',
                position.mint, WSOL, amount_raw, quote, latency,
                None if quote else 'quote_unavailable',
            )
            sol_usd = await self._cached_sol_usd()
            if not quote or not sol_usd or sol_usd <= 0:
                self._event(position.mint, 'exit_quote_pending', 'no_exact_quote', {
                    'reason': reason, 'amount_raw': amount_raw,
                    'quote_event_id': quote_event_id,
                    'latency_ms': round(latency, 2),
                    'exit_remains_open': True,
                })
                return
            token_ui = amount_raw / (10 ** position.decimals)
            fill_price = (quote.min_out_raw / 1_000_000_000) * sol_usd / token_ui if token_ui > 0 else 0.0
            if not watchlist_exit_quote_confirms(
                reason, fill_price, position.entry_price_usd,
                position.ath_price_usd, self.config,
            ):
                self._event(position.mint, 'exit_quote_rejected', 'trigger_not_confirmed', {
                    'reason': reason, 'quote_price_usd': fill_price,
                    'entry_price_usd': position.entry_price_usd,
                    'ath_price_usd': position.ath_price_usd,
                    'quote_event_id': quote_event_id,
                    'latency_ms': round(latency, 2),
                    'paper_fill_recorded': False,
                })
                position.exit_pending = False
                position.pending_exit_reason = ''
                position.pending_exit_amount_raw = 0
                return

            sol_rub = sol_usd * float(self.settings.usd_rub_rate)
            output_sol = quote.min_out_raw / 1_000_000_000
            gross_return_rub = output_sol * sol_rub
            exit_fee_rub = float(self.settings.exit_fee_sol) * sol_rub
            net_return_rub = gross_return_rub - exit_fee_rub
            sold_fraction = amount_raw / position.tokens_raw
            cost_basis = position.invested_rub * sold_fraction
            pnl_leg = net_return_rub - cost_basis
            position.sold_tokens_raw += amount_raw
            position.remaining_tokens_raw -= amount_raw
            position.sold_fraction = position.sold_tokens_raw / position.tokens_raw
            position.realized_rub += net_return_rub
            position.fees_rub += exit_fee_rub
            self.balance_rub += net_return_rub
            self.total_fees_rub += exit_fee_rub
            if reason.startswith('TP1'):
                position.tp1_done = True
            is_closed = position.remaining_tokens_raw <= 0 or position.sold_fraction >= 1.0 - 1e-9
            if is_closed:
                position.remaining_tokens_raw = 0
                position.closed = True
                position.exit_pending = False
                position.pending_exit_reason = ''
                position.pending_exit_amount_raw = 0
                closed = {
                    'mint': position.mint,
                    'source': position.source,
                    'opened_at_utc': _utc(position.opened_at),
                    'closed_at_utc': _utc(time.time()),
                    'entry_price_usd': position.entry_price_usd,
                    'exit_price_usd': fill_price,
                    'invested_rub': position.invested_rub,
                    'realized_rub': position.realized_rub,
                    'pnl_rub': position.realized_rub - position.invested_rub,
                    'fees_rub': position.fees_rub,
                    'reason': reason,
                    'tp1_done': position.tp1_done,
                }
                self.closed_trades.append(closed)
                self.positions.pop(position.mint, None)
            else:
                position.exit_pending = False
                position.pending_exit_reason = ''
                position.pending_exit_amount_raw = 0
            collector = get_collector()
            if collector is not None:
                try:
                    collector.record_fill(
                        position.mint, position.source, quote_event_id,
                        f'Watchlist {reason}', sold_fraction, fill_price,
                        net_return_rub, pnl_leg, exit_fee_rub,
                        quote.min_out_raw, quote.price_impact_pct,
                        list(quote.route_labels), portfolio='watchlist',
                    )
                except Exception:
                    pass
            self._event(position.mint, 'paper_fill', 'quote_confirmed', {
                'reason': reason, 'sold_fraction': sold_fraction,
                'sold_tokens_raw': amount_raw, 'remaining_tokens_raw': position.remaining_tokens_raw,
                'quote_price_usd': fill_price, 'net_return_rub': net_return_rub,
                'pnl_leg_rub': pnl_leg, 'exit_fee_rub': exit_fee_rub,
                'quote_event_id': quote_event_id,
                'portfolio': 'watchlist', 'paper_only': True,
                'position_closed': is_closed,
            })
            self._save_state()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._event(position.mint, 'exit_error', 'pending_or_retry', {
                'reason': reason, 'error_type': type(exc).__name__,
                'exit_remains_open': True,
            })
        finally:
            position.exit_in_progress = False

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            'mode': 'paper_only',
            'portfolio': 'watchlist',
            'initial_balance_rub': self.initial_balance_rub,
            'balance_rub': self.balance_rub,
            'max_positions': self.config.max_positions,
            'entry_count': self.entry_count,
            'pending_entries': self.pending_entries,
            'closed_trade_count': len(self.closed_trades),
            'total_fees_rub': self.total_fees_rub,
            'open_positions': [
                {
                    'mint': p.mint, 'source': p.source, 'curve': p.curve,
                    'opened_at': p.opened_at, 'opened_at_utc': _utc(p.opened_at),
                    'entry_price_usd': p.entry_price_usd, 'tokens_raw': p.tokens_raw,
                    'remaining_tokens_raw': p.remaining_tokens_raw,
                    'sold_fraction': p.sold_fraction, 'tp1_done': p.tp1_done,
                    'invested_rub': p.invested_rub, 'realized_rub': p.realized_rub,
                    'fees_rub': p.fees_rub, 'exit_pending': p.exit_pending,
                    'pending_exit_reason': p.pending_exit_reason,
                    'pending_exit_amount_raw': p.pending_exit_amount_raw,
                    'opened_at_epoch': p.opened_at,
                }
                for p in self.positions.values() if not p.closed
            ],
            'closed_trades': self.closed_trades,
            'paper_only': True,
            'transactions_signed_or_sent': False,
        }
        tmp = self.state_path.with_name(self.state_path.name + '.tmp')
        try:
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
            os.replace(tmp, self.state_path)
        except OSError:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

    def summary(self) -> dict[str, Any]:
        open_positions = [
            {
                'mint': p.mint, 'remaining_tokens_raw': p.remaining_tokens_raw,
                'sold_fraction': p.sold_fraction, 'tp1_done': p.tp1_done,
                'exit_pending': p.exit_pending,
                'pending_exit_reason': p.pending_exit_reason or None,
                'opened_at_utc': _utc(p.opened_at),
            }
            for p in self.positions.values() if not p.closed
        ]
        pending = sum(1 for p in self.positions.values() if not p.closed and p.exit_pending)
        realized = sum(float(trade.get('pnl_rub') or 0.0) for trade in self.closed_trades)
        return {
            'portfolio': 'watchlist', 'mode': 'paper_only',
            'max_positions': self.config.max_positions,
            'entry_count': self.entry_count,
            'pending_entries': self.pending_entries,
            'closed_position_count': len(self.closed_trades),
            'open_positions': len(open_positions), 'pending_exits': pending,
            'open_position_details': open_positions,
            'candidate_watch_count_at_shutdown': len(self.candidates),
            'poll_errors': self.poll_errors,
            'initial_balance_rub': round(self.initial_balance_rub, 4),
            'balance_rub': round(self.balance_rub, 4),
            'closed_pnl_rub': round(realized, 4),
            'total_fees_rub': round(self.total_fees_rub, 4),
            'paper_only': True, 'transactions_signed_or_sent': False,
        }

    async def close(self) -> dict[str, Any]:
        if self._closed:
            return self.summary()
        self._closed = True
        self._stop.set()
        for task in (self._candidate_task, self._position_task):
            if task is not None and not task.done():
                task.cancel()
        background = [task for task in self._tasks if not task.done()]
        for task in background:
            task.cancel()
        all_tasks = [task for task in (self._candidate_task, self._position_task) if task is not None]
        all_tasks.extend(background)
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        for candidate in list(self.candidates.values()):
            self._cancel_candidate(candidate, 'session_stopped_before_signal')
        self._save_state()
        summary = self.summary()
        self._event('watchlist-portfolio', 'summary', 'final', summary)
        summary_path = os.getenv('WATCHLIST_SUMMARY_PATH', '').strip()
        if summary_path:
            path = Path(summary_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
            except OSError:
                pass
        return summary


def _utc(epoch: float) -> str:
    return datetime.fromtimestamp(float(epoch), timezone.utc).isoformat(timespec='milliseconds')
