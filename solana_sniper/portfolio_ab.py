"""Isolated dual-portfolio A/B Pump.fun paper simulator.

No wallet keys, signing, transaction construction, or transaction submission are
used. Entry/exit prices on active non-Mayhem SOL curves come from the official
Pump SDK 4.1.0 integer quote math port in pump_curve_quote.py. Jupiter is queried
only after an on-chain curve read confirms `complete=true`.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp

from config import Settings
from console import Color, log, short
from creator_watch import CreatorWatch
from creator_probe import CreatorOnchainProbe
from filters import run_all_checks
from models import NewCoin, SwapQuote, TradeRecord
from pump_curve_quote import (
    PUMP_FEE_CONFIG_PDA,
    PUMP_FEE_PROGRAM_ID,
    PUMP_GLOBAL_PDA,
    CurveQuote,
    CurveState,
    FeeContext,
    QuoteUnavailable,
    decode_account_data,
    make_fee_context,
    quote_buy_exact_quote_in,
    quote_sell_exact_tokens,
)
from pumpportal_validation import PUMP_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID, WSOL_MINT, b58decode
from price import (
    WSOL,
    get_jupiter_quote,
    get_sol_usd,
    rpc_get_multiple_accounts,
    rpc_metrics_snapshot,
)
from telemetry import CandidateCollector, get_collector, utc_now


LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_SCALE = 1_000_000  # Pump.fun standard mint is checked as decimals=6.
QUOTE_SLIPPAGE_BPS = 100


@dataclass(frozen=True)
class ABProfile:
    """Immutable strategy profile.

    Entry (v3, 2026-10-22): the old rule paired a narrow birth band with
    ``v_sol_now <= v_sol_birth + 0.8``. Solscan/Jupiter reconciliation showed
    that cap was an anti-filter: every real rocket (Turtle 33.95 -> 45.51,
    KCHAD, Shigure 33.16 -> 38.59, CAVELON 36.0 -> 40.43, PERFORMA 39.88)
    was rejected while dead launches with ``delta == 0.0`` were bought (8 of 9
    such fills were losing decoys). The replacement requires *live momentum*:
    a real inflow of curve reserves inside the first observation window, and
    no upper cap tight enough to reject early organic runners.
    """

    name: str
    # --- entry: live-momentum band (shared by both arms) ---
    v_sol_birth_min: float = 30.5
    v_sol_birth_max: float = 50.0
    delta_min_sol: float = 0.10
    delta_max_sol: float = 12.0
    # --- exit ladder (approved 4-mode logic) ---
    no_progress_pct: float = 1.5
    stop_loss_pct: float = 10.0
    no_progress_sec: float = 5.0
    no_progress_best_max_pct: float = 4.0
    trailing_activation_pct: float = 10.0
    profit_retain_ratio: float = 0.85
    tp1_pct: float = 30.0
    rocket_bypass_pct: float = 50.0
    shelf_stagnation_sec: float = 4.5
    shelf_min_pnl_pct: float = 1.5
    new_high_epsilon_pct: float = 0.4
    # Safety nets that are not part of the 4-mode ladder but prevent stuck slots.
    timeout_before_trailing_sec: float = 35.0
    timeout_after_trailing_sec: float = 90.0
    # --- treatment (B only) ---
    require_creator_gate: bool = False
    guard_min_signatures: int = 25
    initial_balance_rub: float = 1_500.0
    position_size_rub: float = 500.0
    max_slots: int = 3

    def entry_check(self, v_sol_birth: float, v_sol_now: float) -> tuple[bool, str]:
        delta = v_sol_now - v_sol_birth
        if not (self.v_sol_birth_min <= v_sol_birth <= self.v_sol_birth_max):
            return False, 'birth_out_of_band'
        if delta < self.delta_min_sol:
            return False, 'no_live_momentum'
        if delta > self.delta_max_sol:
            return False, 'momentum_too_late'
        return True, 'live_momentum_entry'


PROFILE_A = ABProfile(name='portfolio_a', require_creator_gate=False)
PROFILE_B = ABProfile(name='portfolio_b', require_creator_gate=True)


def profiles_from_settings(settings: Any) -> dict[str, 'ABProfile']:
    """Build the two arms from environment-driven settings.

    Only ``require_creator_gate`` and the guard thresholds may differ between
    the arms; every entry/exit number stays identical so the A/B comparison is
    single-factor.
    """
    shared = {
        'v_sol_birth_min': float(getattr(settings, 'entry_birth_min_sol', 30.5)),
        'v_sol_birth_max': float(getattr(settings, 'entry_birth_max_sol', 50.0)),
        'delta_min_sol': float(getattr(settings, 'entry_delta_min_sol', 0.10)),
        'delta_max_sol': float(getattr(settings, 'entry_delta_max_sol', 12.0)),
    }
    a = replace(PROFILE_A, **shared)
    b = replace(PROFILE_B, **shared,
                guard_min_signatures=int(getattr(settings, 'creator_guard_min_sig', 25)))
    return {a.name: a, b.name: b}


@dataclass
class SimPosition:
    portfolio: str
    mint: str
    source: str
    curve: str
    opened_at_epoch: float
    opened_at_utc: str
    birth_v_sol: float
    entry_v_sol: float
    entry_sol_usd: float
    entry_cost_rub: float
    entry_fee_rub: float
    remaining_entry_fee_rub: float
    entry_price_usd: float
    initial_tokens_raw: int
    remaining_tokens_raw: int
    entry_amount_lamports: int
    entry_min_out_raw: int
    entry_protocol_fee_raw: int
    entry_creator_fee_raw: int
    entry_fee_bps_protocol: int
    entry_fee_bps_creator: int
    entry_market_cap_lamports: int
    entry_fee_tier_index: int
    remaining_cost_rub: float
    entry_out_amount_raw: Optional[int] = None
    initial_conservative_tokens_raw: int = 0
    remaining_conservative_tokens_raw: int = 0
    conservative_remaining_cost_rub: float = 0.0
    conservative_remaining_entry_fee_rub: float = 0.0
    conservative_realized_pnl_rub: float = 0.0
    conservative_realized_cost_rub: float = 0.0
    conservative_realized_return_rub: float = 0.0
    conservative_realized_fee_rub: float = 0.0
    conservative_metrics_complete: bool = True
    conservative_incomplete_reason: Optional[str] = None
    conservative_incomplete_counted: bool = False
    last_mark_snapshot_pnl_rub: Optional[float] = None
    last_mark_conservative_100bps_pnl_rub: Optional[float] = None
    realized_cost_rub: float = 0.0
    realized_return_rub: float = 0.0
    realized_exit_value_usd: float = 0.0
    realized_pnl_rub: float = 0.0
    realized_fee_rub: float = 0.0
    onchain_fees_paid_rub: float = 0.0
    realized_exit_lamports: int = 0
    sold_tokens_raw: int = 0
    best_pnl_pct: float = -100.0
    best_pnl_updated_at: float = 0.0
    no_progress_checked: bool = False
    tp1_done: bool = False
    tp1_rocket_bypass: bool = False
    tp1_partial_pending: bool = False
    tp1_pending_sell_raw: int = 0
    ath_net_rub_per_token: float = 0.0
    last_mark_pnl_pct: Optional[float] = None
    last_mark_rub: Optional[float] = None
    last_mark_at_epoch: Optional[float] = None
    exit_pending_reason: Optional[str] = None
    exit_pending_since_epoch: Optional[float] = None
    last_poll_error_type: Optional[str] = None
    last_poll_error_logged_epoch: float = 0.0
    exit_reasons: list[str] = field(default_factory=list)
    status: str = 'open'

    @property
    def remaining_fraction(self) -> float:
        if self.initial_tokens_raw <= 0:
            return 0.0
        return max(0.0, min(1.0, self.remaining_tokens_raw / self.initial_tokens_raw))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data['remaining_fraction'] = self.remaining_fraction
        data['conservative_remaining_fraction'] = (
            self.remaining_conservative_tokens_raw / self.initial_conservative_tokens_raw
            if self.initial_conservative_tokens_raw > 0 else 0.0
        )
        data['snapshot_pnl_rub'] = round(self.realized_pnl_rub, 8)
        data['conservative_100bps_pnl_rub'] = round(self.conservative_realized_pnl_rub, 8)
        data['conservative_100bps_pnl_pct'] = (
            round(self.conservative_realized_pnl_rub / self.entry_cost_rub * 100.0, 8)
            if self.entry_cost_rub > 0 and self.conservative_metrics_complete else None
        )
        data['gross_curve_pnl_rub'] = round(self.realized_pnl_rub + self.realized_fee_rub, 8)
        data['conservative_100bps_complete'] = self.conservative_metrics_complete
        return data


@dataclass
class PortfolioBook:
    profile: ABProfile
    state_path: Path
    cash_balance_rub: float = 1_500.0
    realized_pnl_rub: float = 0.0
    gross_curve_pnl_rub: float = 0.0
    realized_network_fees_rub: float = 0.0
    conservative_100bps_pnl_rub: float = 0.0
    conservative_100bps_incomplete_trades: int = 0
    fees_paid_rub: float = 0.0
    onchain_fees_paid_rub: float = 0.0
    entry_count: int = 0
    closed_trade_count: int = 0
    poll_errors: int = 0
    positions: dict[str, SimPosition] = field(default_factory=dict)
    closed_mints: set[str] = field(default_factory=set)
    closed_trades: list[dict[str, Any]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def save(self, run_id: str) -> None:
        data = {
            'run_id': run_id,
            'portfolio': self.profile.name,
            'strategy': asdict(self.profile),
            'paper_only': True,
            'initial_balance_rub': self.profile.initial_balance_rub,
            'cash_balance_rub': round(self.cash_balance_rub, 8),
            'open_entry_fee_liability_rub': round(sum(p.remaining_entry_fee_rub for p in self.positions.values()), 8),
            'net_cash_after_open_entry_fee_liability_rub': round(
                self.cash_balance_rub - sum(p.remaining_entry_fee_rub for p in self.positions.values()), 8
            ),
            'realized_pnl_rub': round(self.realized_pnl_rub, 8),
            'snapshot_pnl_rub': round(self.realized_pnl_rub, 8),
            'gross_curve_pnl_rub': round(self.gross_curve_pnl_rub, 8),
            'realized_network_fees_rub': round(self.realized_network_fees_rub, 8),
            'conservative_100bps_pnl_rub': (
                round(self.conservative_100bps_pnl_rub, 8)
                if self.conservative_100bps_incomplete_trades == 0
                and all(p.conservative_metrics_complete for p in self.positions.values()) else None
            ),
            'conservative_100bps_pnl_partial_sum_rub': round(self.conservative_100bps_pnl_rub, 8),
            'conservative_100bps_incomplete_trades': self.conservative_100bps_incomplete_trades,
            'conservative_100bps_complete': (
                self.conservative_100bps_incomplete_trades == 0
                and all(p.conservative_metrics_complete for p in self.positions.values())
            ),
            'fees_paid_rub': round(self.fees_paid_rub, 8),
            'onchain_fees_paid_rub': round(self.onchain_fees_paid_rub, 8),
            'entry_count': self.entry_count,
            'closed_trade_count': self.closed_trade_count,
            'poll_errors': self.poll_errors,
            'max_slots': self.profile.max_slots,
            'position_size_rub': self.profile.position_size_rub,
            'open_positions': [p.to_dict() for p in self.positions.values() if p.status == 'open'],
            'pending_positions': [p.to_dict() for p in self.positions.values() if p.status == 'exit_pending'],
            'closed_trades': self.closed_trades,
            'updated_at_utc': utc_now(),
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + '.tmp')
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(tmp, self.state_path)


@dataclass(frozen=True)
class EntrySnapshot:
    curve: CurveState
    fee_context: FeeContext
    context_slot: Optional[int]
    observed_at_epoch: float


@dataclass(frozen=True)
class ExecutableQuote:
    swap_quote: SwapQuote
    protocol_fee_raw: int
    creator_fee_raw: int
    protocol_fee_bps: int
    creator_fee_bps: int
    market_cap_lamports: Optional[int]
    fee_tier_index: Optional[int]
    route: str
    error_type: Optional[str] = None


class ABPaperSimulator:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        settings: Settings,
        run_id: str,
        run_dir: str | Path,
        trades_queue: asyncio.Queue,
        creator_watch: CreatorWatch,
        creator_probe: CreatorOnchainProbe,
        profiles: Optional[dict[str, 'ABProfile']] = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.run_id = run_id
        self.run_dir = Path(run_dir)
        self.trades_queue = trades_queue
        self.creator_watch = creator_watch
        self.creator_probe = creator_probe
        resolved = profiles or profiles_from_settings(settings)
        self.books = {
            name: PortfolioBook(
                profile, self.run_dir / f'wallet_{name}.json',
                cash_balance_rub=profile.initial_balance_rub,
            )
            for name, profile in resolved.items()
        }
        self.filter_sem = asyncio.Semaphore(max(1, int(settings.filter_concurrency)))
        self._last_fee_log_epoch = 0.0
        self._poll_task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        self.early_stop_event = asyncio.Event()
        self.early_stop_reason: Optional[str] = None
        self.started_at_epoch = time.time()
        self.ended_at_epoch: Optional[float] = None
        self._last_rpc_error_logged = 0.0

    async def start(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        if any(book.state_path.exists() for book in self.books.values()):
            raise RuntimeError('refusing to reuse an existing A/B wallet state in this RUN_ID')
        for book in self.books.values():
            book.save(self.run_id)
        snapshot = await self._fetch_entry_snapshot_raw(curve_address=None)
        if snapshot is None:
            log('⚠️ On-chain Global/FeeConfig preflight failed; all active-curve fills remain fail-closed.', Color.YELLOW)
        else:
            self._record_fee_context(snapshot.fee_context)
            log(
                '✅ Pump Global + FeeConfig read-only preflight OK; '
                f'tiers={len(snapshot.fee_context.fee_tiers)}, slot={snapshot.context_slot}',
                Color.GREEN,
            )
        self._poll_task = asyncio.create_task(self._position_poll_loop(), name='ab-position-poller')
        log(
            '🧪 Isolated paper portfolios active: A=3×500 ₽ / 1,500 ₽; '
            'B=3×500 ₽ / 1,500 ₽; no Main/Watchlist; read-only RPC only.',
            Color.CYAN,
        )

    def _collector(self) -> Optional[CandidateCollector]:
        return get_collector()

    def _record_fee_context(self, context: FeeContext) -> None:
        collector = self._collector()
        if collector is not None:
            collector.record_meta('pump_fee_context_latest', context.manifest())
        self._last_fee_log_epoch = time.time()

    def _make_fee_context_from_accounts(
        self, accounts: dict[str, Optional[dict[str, Any]]], context_slot: Optional[int],
    ) -> Optional[FeeContext]:
        global_account = accounts.get(PUMP_GLOBAL_PDA or '')
        fee_account = accounts.get(PUMP_FEE_CONFIG_PDA or '')
        if not isinstance(global_account, dict) or not isinstance(fee_account, dict):
            return None
        if global_account.get('owner') != PUMP_PROGRAM_ID:
            return None
        if fee_account.get('owner') != PUMP_FEE_PROGRAM_ID:
            return None
        global_raw = decode_account_data(global_account)
        fee_raw = decode_account_data(fee_account)
        if not global_raw or not fee_raw:
            return None
        try:
            return make_fee_context(
                global_raw, fee_raw, context_slot=context_slot,
                observed_at_epoch=time.time(),
            )
        except QuoteUnavailable:
            return None

    async def _fetch_entry_snapshot_raw(
        self, curve_address: Optional[str],
    ) -> Optional[EntrySnapshot]:
        addresses: list[str] = []
        if curve_address:
            addresses.append(curve_address)
        if PUMP_GLOBAL_PDA:
            addresses.append(PUMP_GLOBAL_PDA)
        if PUMP_FEE_CONFIG_PDA:
            addresses.append(PUMP_FEE_CONFIG_PDA)
        if len(addresses) < 2:
            return None
        accounts, slot = await rpc_get_multiple_accounts(
            self.session, self.settings.http_rpc_url, addresses,
            encoding='base64', commitment='processed', retry_null=True,
        )
        fee_context = self._make_fee_context_from_accounts(accounts, slot)
        if fee_context is None:
            return None
        if not curve_address:
            # Startup fee-config preflight uses the same exact read path but no curve.
            return EntrySnapshot(
                curve=CurveState(
                    virtual_token_reserves=1, virtual_quote_reserves=1,
                    real_token_reserves=1, real_quote_reserves=0,
                    token_total_supply=1, complete=False, creator=bytes(32),
                    is_mayhem_mode=False, quote_mint_bytes=bytes(32),
                    creator_fee_bps=0, raw_length=166,
                ),
                fee_context=fee_context, context_slot=slot,
                observed_at_epoch=time.time(),
            )
        curve_account = accounts.get(curve_address)
        if not isinstance(curve_account, dict) or curve_account.get('owner') != PUMP_PROGRAM_ID:
            return None
        curve_raw = decode_account_data(curve_account)
        if not curve_raw:
            return None
        try:
            curve = CurveState.decode(curve_raw)
        except QuoteUnavailable:
            return None
        return EntrySnapshot(
            curve=curve, fee_context=fee_context,
            context_slot=slot, observed_at_epoch=time.time(),
        )

    def _entry_snapshot_from_coin(self, coin: NewCoin) -> Optional[EntrySnapshot]:
        raw_snapshot = coin.entry_snapshot
        expected = [coin.mint, coin.curve, PUMP_GLOBAL_PDA, PUMP_FEE_CONFIG_PDA]
        if not isinstance(raw_snapshot, dict) or raw_snapshot.get('account_addresses') != expected:
            return None
        try:
            slot = int(raw_snapshot['context_slot'])
            observed_at = float(raw_snapshot['observed_at_epoch'])
            curve_raw = raw_snapshot['curve_raw']
            global_raw = raw_snapshot['global_raw']
            fee_raw = raw_snapshot['fee_config_raw']
            if not all(isinstance(raw, bytes) and raw for raw in (curve_raw, global_raw, fee_raw)):
                return None
            curve = CurveState.decode(curve_raw)
            fee_context = make_fee_context(
                global_raw, fee_raw, context_slot=slot,
                observed_at_epoch=observed_at,
            )
        except (KeyError, TypeError, ValueError, QuoteUnavailable):
            return None
        return EntrySnapshot(curve, fee_context, slot, observed_at)

    async def handle_candidate(self, coin: NewCoin) -> None:
        if coin.source != 'Pump.fun':
            collector = self._collector()
            if collector is not None:
                collector.record_decision(coin.mint, coin.source, 'ab_scope', 'skipped', ['source_not_pumpfun'])
            return
        handler_started = time.monotonic()
        first_seen_delay_ms = max(0.0, (time.time() - coin.found_at) * 1000.0)
        async with self.filter_sem:
            filter_started = time.monotonic()
            verdict = await run_all_checks(
                self.session, self.settings.http_rpc_url, coin,
                self.settings.min_liquidity_usd, self.settings.honeypot_check,
                self.settings.only_sol_pairs,
                self.settings.indexer_retries,
                self.settings.indexer_retry_delay_sec,
                creator_probe=self.creator_probe,
            )
        filter_done = time.monotonic()
        filter_metrics = {
            'queue_delay_ms': round(first_seen_delay_ms, 2),
            'filter_semaphore_wait_ms': round(max(0.0, (filter_started - handler_started) * 1000.0), 2),
            'filter_execution_ms': round(max(0.0, (filter_done - filter_started) * 1000.0), 2),
            'first_seen_to_filter_done_ms': round(max(0.0, (time.time() - coin.found_at) * 1000.0), 2),
        }
        collector = self._collector()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, 'security_and_market_filters',
                'pass' if verdict.passed else 'fail', verdict.reasons,
                {'decimals': coin.decimals, 'packet': coin.packet, **filter_metrics,
                 'paper_only': True, 'scope': 'A/B only'},
            )
            try:
                collector.record_creator_attribution(coin, observed_at_epoch=time.time())
            except Exception:
                pass

        # Creator-history evidence remains advisory. The separate, predeclared
        # B-treatment gate uses only the bounded decision-time probe below.
        try:
            profile = self.creator_watch.register_candidate(
                coin, eligible=True, observed_at=time.time(),
            )
        except Exception as exc:
            profile = {'risk_level': 'creator_probe_unavailable', 'error_type': type(exc).__name__}
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, 'creator_watch',
                str(profile.get('risk_level') or 'identity_unknown'),
                profile.get('risk_evidence') or [], profile,
            )
            collector.record_decision(
                coin.mint, coin.source, 'creator_history_advisory', 'warn_only',
                profile.get('risk_evidence') or [],
                {**profile, 'risk_policy': 'warn', 'entry_blocked_by_creator_history': False},
            )
        if not verdict.passed:
            log(f'🛑 {short(coin.mint)} — Pump.fun security filters blocked: {"; ".join(verdict.reasons)}', Color.YELLOW)
            return
        if not coin.curve:
            self._record_candidate_fail(coin, 'missing_curve_after_security_filters')
            return

        # The filter preflight already read mint + curve + Global + FeeConfig
        # in one getMultipleAccounts context. Do not issue a second entry read.
        snapshot = self._entry_snapshot_from_coin(coin)
        if snapshot is None:
            self._record_candidate_fail(coin, 'four_account_entry_snapshot_unavailable')
            return
        if snapshot.curve.complete:
            self._record_candidate_fail(coin, 'curve_completed_before_entry')
            return
        if snapshot.curve.is_mayhem_mode:
            self._record_candidate_fail(coin, 'pump_mayhem_mode_excluded')
            return
        quote_bytes = snapshot.curve.quote_mint_bytes
        wsol_bytes = b58decode(WSOL_MINT)
        if any(quote_bytes) and quote_bytes != wsol_bytes:
            self._record_candidate_fail(coin, 'unsupported_non_sol_curve_quote_mint')
            return
        try:
            birth_v_sol = float((coin.packet or {}).get('v_sol'))
        except (TypeError, ValueError, OverflowError):
            self._record_candidate_fail(coin, 'missing_or_invalid_v_sol_birth')
            return
        if not (math.isfinite(birth_v_sol) and birth_v_sol > 0):
            self._record_candidate_fail(coin, 'missing_or_invalid_v_sol_birth')
            return
        current_v_sol = snapshot.curve.virtual_quote_reserves / LAMPORTS_PER_SOL
        sol_usd = await get_sol_usd(self.session)
        if sol_usd is None or not math.isfinite(sol_usd) or sol_usd <= 0:
            for book in self.books.values():
                self._record_portfolio_skip(book, coin, 'sol_usd_unavailable', birth_v_sol, current_v_sol)
            return

        # The common timestamp and immutable snapshot keep control/treatment
        # decisions aligned; only the B creator gate may differ.
        decision_epoch = time.time()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, 'entry_state', 'fresh_state_ok', [],
                {'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
                 'delta_sol': round(current_v_sol - birth_v_sol, 6),
                 'curve_complete': snapshot.curve.complete,
                 'is_mayhem_mode': snapshot.curve.is_mayhem_mode,
                 'curve_context_slot': snapshot.context_slot,
                 'fee_context_observed_at_epoch': snapshot.fee_context.observed_at_epoch,
                 'fee_tiers': snapshot.fee_context.manifest()['fee_tiers'],
                 'decision_at_epoch': decision_epoch,
                 'first_seen_to_decision_ms': round(max(0.0, (decision_epoch - coin.found_at) * 1000), 2),
                 'creator_probe': coin.creator_probe_result,
                 'shared_entry_snapshot': True},
            )
        await asyncio.gather(*(
            self._consider_entry(
                book, coin, snapshot, birth_v_sol, current_v_sol,
                sol_usd, filter_metrics, decision_epoch,
            )
            for book in self.books.values()
        ))

    def _record_candidate_fail(self, coin: NewCoin, reason: str) -> None:
        collector = self._collector()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, 'ab_entry_preflight', 'fail', [reason],
                {'paper_only': True, 'jupiter_used': False},
            )
        log(f'⏸ {short(coin.mint)} — A/B preflight fail-closed: {reason}', Color.YELLOW)

    def _record_portfolio_skip(
        self, book: PortfolioBook, coin: NewCoin, reason: str,
        birth_v_sol: Optional[float] = None, current_v_sol: Optional[float] = None,
    ) -> None:
        collector = self._collector()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, f'{book.profile.name}_entry', 'skipped', [reason],
                {'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
                 'cash_balance_rub': round(book.cash_balance_rub, 4),
                 'open_positions': len(book.positions), 'max_slots': book.profile.max_slots},
            )

    async def _consider_entry(
        self,
        book: PortfolioBook,
        coin: NewCoin,
        snapshot: EntrySnapshot,
        birth_v_sol: float,
        current_v_sol: float,
        sol_usd: float,
        filter_metrics: dict[str, Any],
        decision_epoch: Optional[float] = None,
    ) -> None:
        decision_epoch = float(decision_epoch or time.time())
        profile = book.profile
        qualifies, entry_rule = profile.entry_check(birth_v_sol, current_v_sol)
        collector = self._collector()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, f'{profile.name}_signal',
                'pass' if qualifies else 'skip', [entry_rule],
                {'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
                 'entry_profile': profile.name,
                 'as_of_epoch': decision_epoch, 'paper_only': True},
            )
        if not qualifies:
            return
        if profile.require_creator_gate:
            probe = coin.creator_probe_result or {}
            guard_passed = bool(probe.get('creator_guard_passed'))
            guard_reason = str(probe.get('creator_guard_reason') or 'creator_guard_unavailable')
            censored = bool(probe.get('censored')) or not probe
            decision = 'pass' if guard_passed else ('censored' if censored else 'fail')
            if collector is not None:
                collector.record_decision(
                    coin.mint, coin.source, 'portfolio_b_creator_gate', decision,
                    [] if guard_passed else [guard_reason],
                    {'creator': probe.get('creator'),
                     'creator_identity_type': probe.get('creator_identity_type'),
                     'creator_identity_resolved': probe.get('creator_identity_resolved'),
                     'program_creator': probe.get('program_creator'),
                     'pumpportal_event_actor': probe.get('pumpportal_event_actor'),
                     'program_creator_sig_count': probe.get('program_creator_sig_count'),
                     'pumpportal_event_actor_sig_count': probe.get('pumpportal_event_actor_sig_count'),
                     'signature_count_0_25': probe.get('signature_count'),
                     'creator_wallet_age_sec': probe.get('creator_wallet_age_sec'),
                     'creator_burst_activity': probe.get('creator_burst_activity'),
                     'ata_exists': probe.get('ata_exists'),
                     'token_balance_raw': probe.get('token_balance_raw'),
                     'probe_status': probe.get('status'),
                     'probe_latency_ms': probe.get('latency_ms'),
                     'probe_errors': probe.get('errors') or [],
                     'guard_reason': guard_reason,
                     'censored': censored,
                     'decision_at_epoch': decision_epoch,
                     'paper_only': True},
                )
            if not guard_passed:
                self._record_portfolio_skip(
                    book, coin,
                    'creator_guard_censored' if censored else f'creator_guard_{guard_reason}',
                    birth_v_sol, current_v_sol,
                )
                return
        async with book.lock:
            if coin.mint in book.positions or coin.mint in book.closed_mints:
                self._record_portfolio_skip(book, coin, 'mint_already_seen_in_this_portfolio', birth_v_sol, current_v_sol)
                return
            if len(book.positions) >= profile.max_slots:
                self._record_portfolio_skip(book, coin, 'three_slots_already_open_or_pending', birth_v_sol, current_v_sol)
                return
            if book.cash_balance_rub < profile.position_size_rub:
                self._record_portfolio_skip(book, coin, 'cash_below_slot_size', birth_v_sol, current_v_sol)
                return
            fx = max(0.0, float(self.settings.usd_rub_rate))
            sol_rub = sol_usd * fx
            if sol_rub <= 0:
                self._record_portfolio_skip(book, coin, 'invalid_sol_rub_conversion', birth_v_sol, current_v_sol)
                return
            amount_lamports = int(profile.position_size_rub / sol_rub * LAMPORTS_PER_SOL)
            if amount_lamports <= 0:
                self._record_portfolio_skip(book, coin, 'invalid_quote_size', birth_v_sol, current_v_sol)
                return
            started = time.monotonic()
            try:
                curve_quote = quote_buy_exact_quote_in(
                    snapshot.curve, snapshot.fee_context, amount_lamports,
                    slippage_bps=QUOTE_SLIPPAGE_BPS,
                    context_slot=snapshot.context_slot,
                )
            except QuoteUnavailable as exc:
                latency_ms = (time.monotonic() - started) * 1000.0
                if collector is not None:
                    collector.record_quote(
                        coin.mint, coin.source, 'paper_entry', 'buy', WSOL,
                        coin.mint, amount_lamports, None, 'unavailable',
                        QUOTE_SLIPPAGE_BPS, latency_ms, str(exc),
                    )
                    collector.record_decision(
                        coin.mint, coin.source, f'{profile.name}_entry', 'not_opened',
                        [str(exc)], {'paper_only': True, 'jupiter_used': False},
                    )
                return
            latency_ms = (time.monotonic() - started) * 1000.0
            swap_quote = SwapQuote(
                input_mint=WSOL, output_mint=coin.mint,
                in_amount_raw=amount_lamports,
                out_amount_raw=curve_quote.out_amount_raw,
                min_out_raw=curve_quote.min_out_raw,
                price_impact_pct=curve_quote.price_impact_pct,
                route_labels=curve_quote.route_labels(),
                context_slot=curve_quote.context_slot,
            )
            quote_event_id = None
            if collector is not None:
                quote_event_id = collector.record_quote(
                    coin.mint, coin.source, 'paper_entry', 'buy', WSOL,
                    coin.mint, amount_lamports, swap_quote, 'ok',
                    QUOTE_SLIPPAGE_BPS, latency_ms,
                )
            # Exact-quote-in reserves the caller's requested input budget;
            # integer fee rounding can leave at most dust in the fee/net split.
            actual_cost_rub = (
                curve_quote.in_amount_raw / LAMPORTS_PER_SOL * sol_usd * fx
            )
            if actual_cost_rub <= 0 or actual_cost_rub > book.cash_balance_rub + 1e-6:
                self._record_portfolio_skip(book, coin, 'insufficient_free_cash_for_entry', birth_v_sol, current_v_sol)
                return
            entry_fee_rub = max(0.0, float(self.settings.entry_fee_sol)) * sol_usd * fx
            onchain_entry_fee_rub = (
                (curve_quote.protocol_fee_raw + curve_quote.creator_fee_raw)
                / LAMPORTS_PER_SOL * sol_usd * fx
            )
            received_tokens = curve_quote.out_amount_raw
            conservative_tokens = curve_quote.min_out_raw
            if received_tokens <= 0:
                self._record_portfolio_skip(book, coin, 'entry_out_amount_zero', birth_v_sol, current_v_sol)
                return
            entry_price_usd = (
                actual_cost_rub / fx / (received_tokens / TOKEN_SCALE)
                if fx > 0 else 0.0
            )
            pos = SimPosition(
                portfolio=profile.name, mint=coin.mint, source=coin.source,
                curve=coin.curve or '', opened_at_epoch=decision_epoch,
                opened_at_utc=datetime.fromtimestamp(decision_epoch, timezone.utc).isoformat(timespec='milliseconds'),
                birth_v_sol=birth_v_sol,
                entry_v_sol=current_v_sol, entry_sol_usd=sol_usd,
                entry_cost_rub=actual_cost_rub, entry_fee_rub=entry_fee_rub,
                remaining_entry_fee_rub=entry_fee_rub,
                entry_price_usd=entry_price_usd,
                initial_tokens_raw=received_tokens,
                remaining_tokens_raw=received_tokens,
                entry_amount_lamports=amount_lamports,
                entry_min_out_raw=curve_quote.min_out_raw,
                entry_protocol_fee_raw=curve_quote.protocol_fee_raw,
                entry_creator_fee_raw=curve_quote.creator_fee_raw,
                entry_fee_bps_protocol=curve_quote.protocol_fee_bps,
                entry_fee_bps_creator=curve_quote.creator_fee_bps,
                entry_market_cap_lamports=curve_quote.market_cap_lamports,
                entry_fee_tier_index=curve_quote.tier_index,
                remaining_cost_rub=actual_cost_rub,
                entry_out_amount_raw=curve_quote.out_amount_raw,
                initial_conservative_tokens_raw=conservative_tokens,
                remaining_conservative_tokens_raw=conservative_tokens,
                conservative_remaining_cost_rub=actual_cost_rub,
                conservative_remaining_entry_fee_rub=entry_fee_rub,
                best_pnl_updated_at=decision_epoch,
            )
            book.cash_balance_rub -= actual_cost_rub
            book.fees_paid_rub += entry_fee_rub
            book.onchain_fees_paid_rub += onchain_entry_fee_rub
            book.entry_count += 1
            book.positions[coin.mint] = pos
            book.save(self.run_id)
            values = {
                'entry_rule': entry_rule,
                'v_sol_birth': birth_v_sol,
                'v_sol_now': current_v_sol,
                'delta_sol': round(current_v_sol - birth_v_sol, 6),
                'entry_size_target_rub': profile.position_size_rub,
                'entry_cost_rub': round(actual_cost_rub, 6),
                'entry_fee_rub': round(entry_fee_rub, 6),
                'onchain_protocol_fee_raw': curve_quote.protocol_fee_raw,
                'onchain_creator_fee_raw': curve_quote.creator_fee_raw,
                'protocol_fee_bps': curve_quote.protocol_fee_bps,
                'creator_fee_bps': curve_quote.creator_fee_bps,
                'fee_tier_index': curve_quote.tier_index,
                'market_cap_lamports': curve_quote.market_cap_lamports,
                'tokens_received_snapshot_out_raw': received_tokens,
                'tokens_received_conservative_min_raw': conservative_tokens,
                'snapshot_pnl_rub': 0.0,
                'conservative_100bps_pnl_rub': 0.0,
                'conservative_100bps_pnl_pct': 0.0,
                'curve_context_slot': snapshot.context_slot,
                'decision_at_epoch': decision_epoch,
                'first_seen_to_entry_ms': round(max(0.0, (decision_epoch - coin.found_at) * 1000.0), 2),
                **filter_metrics,
                'portfolio_balance_rub_after_entry': round(book.cash_balance_rub, 6),
                'max_slots': profile.max_slots,
                'paper_only': True,
                'signed_transaction': False,
                'jupiter_used': False,
            }
            if collector is not None:
                collector.record_decision(
                    coin.mint, coin.source, f'{profile.name}_entry', 'opened',
                    ['local_exact_quote_confirmed'], values,
                )
            log(
                f'🧪 [{profile.name}] OPEN {short(coin.mint)} | '
                f'birth={birth_v_sol:.2f} now={current_v_sol:.2f} '
                f'delta=+{current_v_sol - birth_v_sol:.2f} SOL | '
                f'cost≈{actual_cost_rub:.2f} ₽ | tokens(out)={received_tokens} | ' 
                f'fee={curve_quote.protocol_fee_bps}+{curve_quote.creator_fee_bps} bps | '
                f'slots={len(book.positions)}/{profile.max_slots}',
                Color.CYAN,
            )

    def _maybe_early_stop(self) -> None:
        if self.early_stop_reason is not None:
            return
        depleted = all(
            not any(pos.status != 'closed' for pos in book.positions.values())
            and book.cash_balance_rub < book.profile.position_size_rub
            for book in self.books.values()
        )
        if depleted:
            self.early_stop_reason = 'both_portfolios_below_slot_size'
            self.early_stop_event.set()

    async def _position_poll_loop(self) -> None:
        while not self._stopping.is_set():
            self._maybe_early_stop()
            if self.early_stop_event.is_set():
                return
            all_positions = [
                (book, pos)
                for book in self.books.values()
                for pos in list(book.positions.values())
                if pos.status != 'closed'
            ]
            if all_positions:
                curve_addresses = list(dict.fromkeys(
                    pos.curve for _book, pos in all_positions if pos.curve
                ))
                addresses = curve_addresses + [
                    address for address in (PUMP_GLOBAL_PDA, PUMP_FEE_CONFIG_PDA)
                    if address
                ]
                accounts, context_slot = await rpc_get_multiple_accounts(
                    self.session, self.settings.http_rpc_url, addresses,
                    encoding='base64', commitment='processed', retry_null=True,
                )
                current_fee_context = self._make_fee_context_from_accounts(accounts, context_slot)
                if current_fee_context is not None:
                    if time.time() - self._last_fee_log_epoch >= 60.0:
                        self._record_fee_context(current_fee_context)
                # Active-curve fills require Global/FeeConfig from this same
                # batch slot; stale fee snapshots are intentionally not reused.
                fee_context_for_marks = current_fee_context
                sol_usd = await get_sol_usd(self.session)
                changed_books: set[str] = set()
                for book, pos in all_positions:
                    if pos.mint not in book.positions:
                        continue
                    account = accounts.get(pos.curve)
                    if not isinstance(account, dict) or account.get('owner') != PUMP_PROGRAM_ID:
                        book.poll_errors += 1
                        await self._mark_poll_error(book, pos, 'curve_account_unavailable_or_owner_mismatch')
                        changed_books.add(book.profile.name)
                        continue
                    raw = decode_account_data(account)
                    try:
                        if not raw:
                            raise QuoteUnavailable('curve_data_empty')
                        curve = CurveState.decode(raw)
                    except QuoteUnavailable as exc:
                        book.poll_errors += 1
                        await self._mark_poll_error(book, pos, str(exc))
                        changed_books.add(book.profile.name)
                        continue
                    if sol_usd is None or not math.isfinite(sol_usd) or sol_usd <= 0:
                        book.poll_errors += 1
                        await self._mark_poll_error(book, pos, 'sol_usd_unavailable')
                        changed_books.add(book.profile.name)
                        continue
                    executable = await self._build_sell_quote(
                        pos, curve, fee_context_for_marks, sol_usd, context_slot,
                    )
                    if executable is None:
                        book.poll_errors += 1
                        await self._mark_poll_error(book, pos, 'exact_sell_quote_unavailable')
                        changed_books.add(book.profile.name)
                        continue
                    pnl_pct, mark_value_rub, unit_price_rub = self._mark_position(
                        pos, executable.swap_quote, sol_usd,
                    )
                    expected_exit_fee = max(0.0, float(self.settings.exit_fee_sol)) * sol_usd * max(0.0, float(self.settings.usd_rub_rate))
                    pos.last_mark_snapshot_pnl_rub = (
                        pos.realized_pnl_rub + mark_value_rub - pos.remaining_cost_rub
                        - pos.remaining_entry_fee_rub - expected_exit_fee
                    )
                    pos.last_mark_conservative_100bps_pnl_rub = None
                    if (
                        not curve.complete and fee_context_for_marks is not None
                        and pos.conservative_metrics_complete
                        and pos.remaining_conservative_tokens_raw > 0
                    ):
                        try:
                            shadow = quote_sell_exact_tokens(
                                curve, fee_context_for_marks,
                                pos.remaining_conservative_tokens_raw,
                                slippage_bps=QUOTE_SLIPPAGE_BPS,
                                context_slot=context_slot,
                            )
                            shadow_swap = SwapQuote(
                                input_mint=pos.mint, output_mint=WSOL,
                                in_amount_raw=pos.remaining_conservative_tokens_raw,
                                out_amount_raw=shadow.out_amount_raw,
                                min_out_raw=shadow.min_out_raw,
                                price_impact_pct=shadow.price_impact_pct,
                                route_labels=shadow.route_labels(), context_slot=context_slot,
                            )
                            pos.last_mark_conservative_100bps_pnl_rub = self._mark_conservative_position(
                                pos, shadow_swap, sol_usd,
                            )
                        except QuoteUnavailable:
                            pass
                    mark_at = time.time()
                    pos.last_mark_pnl_pct = pnl_pct
                    pos.last_mark_rub = mark_value_rub
                    pos.last_mark_at_epoch = mark_at
                    self._update_best_pnl(pos, book.profile, pnl_pct, mark_at)
                    if unit_price_rub > pos.ath_net_rub_per_token:
                        pos.ath_net_rub_per_token = unit_price_rub
                    trigger = self._exit_trigger(book.profile, pos, pnl_pct, mark_at)
                    if trigger is None:
                        if pos.exit_pending_reason and not pos.exit_pending_reason.lower().startswith('timeout'):
                            pos.exit_pending_reason = None
                            pos.exit_pending_since_epoch = None
                            pos.status = 'open'
                        changed_books.add(book.profile.name)
                        continue
                    reason, sell_amount_raw = trigger
                    await self._apply_sell_fill(
                        book, pos, sell_amount_raw, reason, executable,
                        curve, fee_context_for_marks, context_slot,
                        sol_usd, pnl_pct, mark_value_rub,
                    )
                    changed_books.add(book.profile.name)
                for book in self.books.values():
                    if book.profile.name in changed_books:
                        book.save(self.run_id)
            self._maybe_early_stop()
            if self.early_stop_event.is_set():
                return
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=0.40)
            except asyncio.TimeoutError:
                pass

    async def _build_sell_quote(
        self,
        pos: SimPosition,
        curve: CurveState,
        fee_context: Optional[FeeContext],
        sol_usd: float,
        context_slot: Optional[int],
    ) -> Optional[ExecutableQuote]:
        if pos.remaining_tokens_raw <= 0:
            return None
        if curve.complete:
            started = time.monotonic()
            quote = await get_jupiter_quote(
                self.session, pos.mint, WSOL, pos.remaining_tokens_raw,
                slippage_bps=QUOTE_SLIPPAGE_BPS,
            )
            if quote is None:
                collector = self._collector()
                if collector is not None:
                    collector.record_quote(
                        pos.mint, pos.source, 'position_exit', 'sell',
                        pos.mint, WSOL, pos.remaining_tokens_raw, None,
                        'unavailable', QUOTE_SLIPPAGE_BPS,
                        (time.monotonic() - started) * 1000.0,
                        'jupiter_quote_unavailable_after_complete',
                    )
                return None
            return ExecutableQuote(
                quote, 0, 0, 0, 0, None, None,
                'Jupiter migrated-pool fallback (on-chain complete=true)',
            )
        if curve.is_mayhem_mode:
            return None
        if fee_context is None or time.time() - fee_context.observed_at_epoch > 2.5:
            return None
        try:
            curve_quote = quote_sell_exact_tokens(
                curve, fee_context, pos.remaining_tokens_raw,
                slippage_bps=QUOTE_SLIPPAGE_BPS,
                context_slot=context_slot,
            )
        except QuoteUnavailable:
            return None
        swap = SwapQuote(
            input_mint=pos.mint, output_mint=WSOL,
            in_amount_raw=pos.remaining_tokens_raw,
            out_amount_raw=curve_quote.out_amount_raw,
            min_out_raw=curve_quote.min_out_raw,
            price_impact_pct=curve_quote.price_impact_pct,
            route_labels=curve_quote.route_labels(),
            context_slot=curve_quote.context_slot,
        )
        return ExecutableQuote(
            swap, curve_quote.protocol_fee_raw, curve_quote.creator_fee_raw,
            curve_quote.protocol_fee_bps, curve_quote.creator_fee_bps,
            curve_quote.market_cap_lamports, curve_quote.tier_index,
            'Pump.fun bonding curve local exact quote',
        )

    def _mark_position(
        self, pos: SimPosition, quote: SwapQuote, sol_usd: float,
    ) -> tuple[float, float, float]:
        fx = max(0.0, float(self.settings.usd_rub_rate))
        out_sol = quote.out_amount_raw / LAMPORTS_PER_SOL
        proceeds_rub = out_sol * sol_usd * fx
        remaining_cost = max(0.0, pos.remaining_cost_rub)
        expected_exit_fee = max(0.0, float(self.settings.exit_fee_sol)) * sol_usd * fx
        net_after_fees = proceeds_rub - pos.remaining_entry_fee_rub - expected_exit_fee
        pnl_pct = (
            (net_after_fees / remaining_cost - 1.0) * 100.0
            if remaining_cost > 0 else -100.0
        )
        units = pos.remaining_tokens_raw / TOKEN_SCALE
        unit_price_rub = proceeds_rub / units if units > 0 else 0.0
        return pnl_pct, proceeds_rub, unit_price_rub

    def _mark_conservative_position(
        self, pos: SimPosition, quote: SwapQuote, sol_usd: float,
    ) -> Optional[float]:
        if not pos.conservative_metrics_complete or pos.remaining_conservative_tokens_raw <= 0:
            return None
        fx = max(0.0, float(self.settings.usd_rub_rate))
        proceeds = quote.min_out_raw / LAMPORTS_PER_SOL * sol_usd * fx
        expected_exit_fee = max(0.0, float(self.settings.exit_fee_sol)) * sol_usd * fx
        return (
            pos.conservative_realized_pnl_rub + proceeds
            - pos.conservative_remaining_cost_rub
            - pos.conservative_remaining_entry_fee_rub
            - expected_exit_fee
        )

    @staticmethod
    def _update_best_pnl(
        pos: SimPosition, profile: ABProfile, pnl_pct: float, now: float,
    ) -> bool:
        """Persist a new peak only after a meaningful +0.4 pp improvement."""
        if pnl_pct >= pos.best_pnl_pct + profile.new_high_epsilon_pct:
            pos.best_pnl_pct = pnl_pct
            pos.best_pnl_updated_at = now
            return True
        return False

    def _exit_trigger(
        self,
        profile: ABProfile,
        pos: SimPosition,
        pnl_pct: float,
        now: Optional[float] = None,
    ) -> Optional[tuple[str, int]]:
        now = time.time() if now is None else float(now)
        held = max(0.0, now - pos.opened_at_epoch)
        if pos.remaining_tokens_raw <= 0:
            return None

        # Once a TP1 sell quote has failed, retry the same half-size paper fill
        # on every fresh mark; do not silently downgrade it to a full-position exit.
        if pos.tp1_partial_pending and not pos.tp1_done:
            retry_amount = min(
                pos.remaining_tokens_raw,
                pos.tp1_pending_sell_raw or max(1, pos.remaining_tokens_raw // 2),
            )
            if retry_amount > 0:
                return f'TP1 +{profile.tp1_pct:.0f}% partial 50% (retry)', retry_amount

        best_updated_at = pos.best_pnl_updated_at or pos.opened_at_epoch
        stalled_sec = max(0.0, now - best_updated_at)

        # A fast loss is protected only before TP1 and before the rocket bypass.
        if (
            not pos.tp1_done and not pos.tp1_rocket_bypass
            and pos.best_pnl_pct < profile.trailing_activation_pct
            and pnl_pct <= -profile.stop_loss_pct
        ):
            return f'SL (−{profile.stop_loss_pct:.1f}%)', pos.remaining_tokens_raw

        # If a mark jumps past the ordinary TP1 zone, preserve all tokens and
        # switch directly to peak-based protection (no half sale on the way).
        if (
            not pos.tp1_done and not pos.tp1_rocket_bypass
            and pnl_pct >= profile.rocket_bypass_pct
        ):
            pos.tp1_rocket_bypass = True

        # Ordinary TP1: only the [25%, 50%) band sells half. The pending flag is
        # durable, so an unavailable partial-size quote is retried on later polls.
        if (
            not pos.tp1_done and not pos.tp1_rocket_bypass
            and profile.tp1_pct <= pnl_pct < profile.rocket_bypass_pct
        ):
            sell_amount = max(1, pos.remaining_tokens_raw // 2)
            pos.tp1_partial_pending = True
            pos.tp1_pending_sell_raw = sell_amount
            return f'TP1 +{profile.tp1_pct:.0f}% partial 50%', sell_amount

        # Retain 85% of the best PnL once a position has first reached +10%.
        if pos.best_pnl_pct >= profile.trailing_activation_pct:
            floor_pnl_pct = profile.profit_retain_ratio * pos.best_pnl_pct
            if pnl_pct <= floor_pnl_pct:
                return (
                    f'Peak retain exit (peak={pos.best_pnl_pct:+.1f}%, '
                    f'floor={floor_pnl_pct:+.1f}%)',
                    pos.remaining_tokens_raw,
                )

        # A flat shelf is closed after 4.5 seconds since the last meaningful
        # high, but only when the position remains at least +2% in profit.
        if pnl_pct >= profile.shelf_min_pnl_pct and stalled_sec >= profile.shelf_stagnation_sec:
            return (
                f'Shelf stagnation ({stalled_sec:.1f}s; pnl={pnl_pct:+.1f}%)',
                pos.remaining_tokens_raw,
            )

        if not pos.no_progress_checked and held >= profile.no_progress_sec:
            pos.no_progress_checked = True
            if (
                pos.best_pnl_pct < profile.no_progress_best_max_pct
                and pnl_pct < profile.no_progress_pct
            ):
                return (
                    f'No progress ({profile.no_progress_sec:.1f}s; '
                    f'peak <+{profile.no_progress_best_max_pct:.1f}%, '
                    f'PnL <+{profile.no_progress_pct:.1f}%)',
                    pos.remaining_tokens_raw,
                )

        timeout = (
            profile.timeout_after_trailing_sec
            if pos.best_pnl_pct >= profile.trailing_activation_pct
            else profile.timeout_before_trailing_sec
        )
        if held >= timeout:
            return f'Timeout ({timeout:.1f}s)', pos.remaining_tokens_raw
        return None

    async def _apply_sell_fill(
        self,
        book: PortfolioBook,
        pos: SimPosition,
        sell_amount_raw: int,
        reason: str,
        executable: ExecutableQuote,
        curve: CurveState,
        fee_context: Optional[FeeContext],
        context_slot: Optional[int],
        sol_usd: float,
        pnl_pct: float,
        mark_value_rub: float,
    ) -> None:
        if sell_amount_raw <= 0 or sell_amount_raw > pos.remaining_tokens_raw:
            await self._mark_poll_error(book, pos, 'invalid_sell_raw_amount')
            return
        quote = executable.swap_quote
        if quote.in_amount_raw != sell_amount_raw:
            if not reason.startswith('TP1'):
                await self._mark_poll_error(book, pos, 'exit_quote_size_mismatch')
                return
            started = time.monotonic()
            try:
                if curve.complete:
                    partial = await get_jupiter_quote(
                        self.session, pos.mint, WSOL, sell_amount_raw,
                        slippage_bps=QUOTE_SLIPPAGE_BPS,
                    )
                    if partial is None:
                        raise QuoteUnavailable('jupiter_quote_unavailable_after_complete')
                    quote = partial
                    executable = ExecutableQuote(
                        quote, 0, 0, 0, 0, None, None,
                        'Jupiter migrated-pool fallback (on-chain complete=true)',
                    )
                else:
                    if fee_context is None or time.time() - fee_context.observed_at_epoch > 2.5:
                        raise QuoteUnavailable('fresh_fee_config_unavailable_for_partial_size')
                    local = quote_sell_exact_tokens(
                        curve, fee_context, sell_amount_raw,
                        slippage_bps=QUOTE_SLIPPAGE_BPS, context_slot=context_slot,
                    )
                    quote = SwapQuote(
                        input_mint=pos.mint, output_mint=WSOL,
                        in_amount_raw=sell_amount_raw,
                        out_amount_raw=local.out_amount_raw,
                        min_out_raw=local.min_out_raw,
                        price_impact_pct=local.price_impact_pct,
                        route_labels=local.route_labels(), context_slot=context_slot,
                    )
                    executable = ExecutableQuote(
                        quote, local.protocol_fee_raw, local.creator_fee_raw,
                        local.protocol_fee_bps, local.creator_fee_bps,
                        local.market_cap_lamports, local.tier_index,
                        'Pump.fun bonding curve local exact quote',
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                collector = self._collector()
                if collector is not None:
                    collector.record_quote(
                        pos.mint, pos.source, 'position_exit', 'sell',
                        pos.mint, WSOL, sell_amount_raw, None, 'unavailable',
                        QUOTE_SLIPPAGE_BPS,
                        (time.monotonic() - started) * 1000.0, type(exc).__name__,
                    )
                pos.exit_pending_reason = reason
                pos.exit_pending_since_epoch = pos.exit_pending_since_epoch or time.time()
                pos.status = 'exit_pending'
                return
        if quote.out_amount_raw <= 0:
            collector = self._collector()
            if collector is not None:
                collector.record_quote(
                    pos.mint, pos.source, 'position_exit', 'sell',
                    pos.mint, WSOL, sell_amount_raw, None, 'unavailable',
                    QUOTE_SLIPPAGE_BPS, error_type='zero_out_amount',
                )
            pos.exit_pending_reason = reason
            pos.exit_pending_since_epoch = pos.exit_pending_since_epoch or time.time()
            pos.status = 'exit_pending'
            return

        collector = self._collector()
        quote_event_id = None
        if collector is not None:
            quote_event_id = collector.record_quote(
                pos.mint, pos.source, 'position_exit', 'sell',
                pos.mint, WSOL, sell_amount_raw, quote, 'ok',
                QUOTE_SLIPPAGE_BPS,
                0.0 if executable.route.startswith('Pump.fun') else None,
            )

        fx = max(0.0, float(self.settings.usd_rub_rate))
        proceeds_sol = quote.out_amount_raw / LAMPORTS_PER_SOL
        proceeds_rub = proceeds_sol * sol_usd * fx
        frac_of_remaining = sell_amount_raw / max(1, pos.remaining_tokens_raw)
        sold_fraction_total = sell_amount_raw / max(1, pos.initial_tokens_raw)
        cost_basis = min(pos.remaining_cost_rub, pos.entry_cost_rub * sold_fraction_total)
        allocated_entry_fee = min(
            pos.remaining_entry_fee_rub,
            pos.remaining_entry_fee_rub * frac_of_remaining,
        )
        exit_fee_rub = max(0.0, float(self.settings.exit_fee_sol)) * sol_usd * fx
        net_cash_return = proceeds_rub - allocated_entry_fee - exit_fee_rub
        pnl_leg = net_cash_return - cost_basis
        exit_price_usd = (
            proceeds_sol * sol_usd / (sell_amount_raw / TOKEN_SCALE)
            if sell_amount_raw > 0 else 0.0
        )

        # Run a parallel, non-triggering conservative shadow with the same
        # sell fraction and network-fee assumptions. It never changes cash or
        # exit decisions. On migrated pools the second quote is tightly capped.
        shadow_quote: Optional[SwapQuote] = None
        shadow_sell_amount_raw = 0
        shadow_cost_basis = 0.0
        shadow_entry_fee = 0.0
        shadow_exit_fee = exit_fee_rub
        shadow_proceeds_rub = 0.0
        shadow_net_return = 0.0
        shadow_pnl_leg: Optional[float] = None
        shadow_fraction_total = 0.0
        shadow_reason: Optional[str] = None
        if pos.conservative_metrics_complete and pos.remaining_conservative_tokens_raw > 0:
            if sell_amount_raw >= pos.remaining_tokens_raw:
                shadow_sell_amount_raw = pos.remaining_conservative_tokens_raw
            else:
                shadow_sell_amount_raw = min(
                    pos.remaining_conservative_tokens_raw,
                    max(1, int(round(pos.remaining_conservative_tokens_raw * frac_of_remaining))),
                )
            if shadow_sell_amount_raw > 0:
                try:
                    if curve.complete:
                        async with asyncio.timeout(0.35):
                            shadow_quote = await get_jupiter_quote(
                                self.session, pos.mint, WSOL, shadow_sell_amount_raw,
                                slippage_bps=QUOTE_SLIPPAGE_BPS,
                            )
                    elif fee_context is not None and time.time() - fee_context.observed_at_epoch <= 2.5:
                        shadow_local = quote_sell_exact_tokens(
                            curve, fee_context, shadow_sell_amount_raw,
                            slippage_bps=QUOTE_SLIPPAGE_BPS, context_slot=context_slot,
                        )
                        shadow_quote = SwapQuote(
                            input_mint=pos.mint, output_mint=WSOL,
                            in_amount_raw=shadow_sell_amount_raw,
                            out_amount_raw=shadow_local.out_amount_raw,
                            min_out_raw=shadow_local.min_out_raw,
                            price_impact_pct=shadow_local.price_impact_pct,
                            route_labels=shadow_local.route_labels(),
                            context_slot=shadow_local.context_slot,
                        )
                    else:
                        shadow_reason = 'shadow_fee_context_unavailable'
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    shadow_reason = f'shadow_quote_{type(exc).__name__}'
                if shadow_quote is not None and shadow_quote.out_amount_raw > 0 and shadow_quote.min_out_raw > 0:
                    shadow_fraction_remaining = (
                        shadow_sell_amount_raw / max(1, pos.remaining_conservative_tokens_raw)
                    )
                    shadow_fraction_total = (
                        shadow_sell_amount_raw / max(1, pos.initial_conservative_tokens_raw)
                    )
                    shadow_cost_basis = min(
                        pos.conservative_remaining_cost_rub,
                        pos.conservative_remaining_cost_rub * shadow_fraction_remaining,
                    )
                    shadow_entry_fee = min(
                        pos.conservative_remaining_entry_fee_rub,
                        pos.conservative_remaining_entry_fee_rub * shadow_fraction_remaining,
                    )
                    shadow_proceeds_rub = (
                        shadow_quote.min_out_raw / LAMPORTS_PER_SOL * sol_usd * fx
                    )
                    shadow_net_return = shadow_proceeds_rub - shadow_entry_fee - shadow_exit_fee
                    shadow_pnl_leg = shadow_net_return - shadow_cost_basis
                else:
                    shadow_quote = None
                    shadow_reason = shadow_reason or 'shadow_quote_zero_or_unavailable'
            else:
                shadow_reason = 'shadow_sell_amount_zero'

        book.cash_balance_rub += net_cash_return
        book.realized_pnl_rub += pnl_leg
        book.gross_curve_pnl_rub += pnl_leg + allocated_entry_fee + exit_fee_rub
        book.realized_network_fees_rub += allocated_entry_fee + exit_fee_rub
        book.fees_paid_rub += exit_fee_rub
        onchain_fee_rub = (
            (executable.protocol_fee_raw + executable.creator_fee_raw)
            / LAMPORTS_PER_SOL * sol_usd * fx
        )
        book.onchain_fees_paid_rub += onchain_fee_rub
        pos.realized_pnl_rub += pnl_leg
        pos.realized_cost_rub += cost_basis
        pos.realized_return_rub += net_cash_return
        pos.realized_exit_value_usd += proceeds_sol * sol_usd
        pos.realized_fee_rub += allocated_entry_fee + exit_fee_rub
        pos.onchain_fees_paid_rub += onchain_fee_rub
        pos.realized_exit_lamports += quote.out_amount_raw
        pos.sold_tokens_raw += sell_amount_raw
        pos.remaining_tokens_raw -= sell_amount_raw
        if reason.startswith('TP1'):
            pos.tp1_done = True
            pos.tp1_partial_pending = False
            pos.tp1_pending_sell_raw = 0
        pos.remaining_cost_rub = max(0.0, pos.remaining_cost_rub - cost_basis)
        pos.remaining_entry_fee_rub = max(0.0, pos.remaining_entry_fee_rub - allocated_entry_fee)

        if shadow_pnl_leg is not None and shadow_quote is not None:
            pos.conservative_realized_pnl_rub += shadow_pnl_leg
            pos.conservative_realized_cost_rub += shadow_cost_basis
            pos.conservative_realized_return_rub += shadow_net_return
            pos.conservative_realized_fee_rub += shadow_entry_fee + shadow_exit_fee
            pos.remaining_conservative_tokens_raw = max(
                0, pos.remaining_conservative_tokens_raw - shadow_sell_amount_raw,
            )
            pos.conservative_remaining_cost_rub = max(
                0.0, pos.conservative_remaining_cost_rub - shadow_cost_basis,
            )
            pos.conservative_remaining_entry_fee_rub = max(
                0.0, pos.conservative_remaining_entry_fee_rub - shadow_entry_fee,
            )
            book.conservative_100bps_pnl_rub += shadow_pnl_leg
        else:
            pos.conservative_metrics_complete = False
            pos.conservative_incomplete_reason = shadow_reason or 'shadow_quote_unavailable'

        pos.exit_reasons.append(reason)
        pos.exit_pending_reason = None
        pos.exit_pending_since_epoch = None
        pos.status = 'open' if pos.remaining_tokens_raw > 0 else 'closed'
        if collector is not None:
            collector.record_fill(
                pos.mint, pos.source, quote_event_id, reason,
                sold_fraction_total, exit_price_usd,
                net_cash_return, pnl_leg, allocated_entry_fee + exit_fee_rub,
                quote.min_out_raw, quote.price_impact_pct,
                list(quote.route_labels) + [executable.route],
                portfolio=book.profile.name,
                out_amount_raw=quote.out_amount_raw,
                snapshot_pnl_rub=pnl_leg,
                conservative_100bps_pnl_rub=shadow_pnl_leg,
                conservative_out_amount_raw=(shadow_quote.out_amount_raw if shadow_quote else None),
                conservative_min_out_raw=(shadow_quote.min_out_raw if shadow_quote else None),
                conservative_sold_fraction=shadow_fraction_total,
                conservative_error=shadow_reason,
            )

        if pos.remaining_tokens_raw <= 0:
            book.closed_trade_count += 1
            book.closed_mints.add(pos.mint)
            book.positions.pop(pos.mint, None)
            if not pos.conservative_metrics_complete and not pos.conservative_incomplete_counted:
                book.conservative_100bps_incomplete_trades += 1
                pos.conservative_incomplete_counted = True
            total_pnl_pct = pos.realized_pnl_rub / max(1e-9, pos.entry_cost_rub) * 100.0
            conservative_pct = (
                pos.conservative_realized_pnl_rub / max(1e-9, pos.entry_cost_rub) * 100.0
                if pos.conservative_metrics_complete else None
            )
            average_exit_usd = (
                pos.realized_exit_value_usd / max(1e-9, pos.sold_tokens_raw / TOKEN_SCALE)
            )
            record = TradeRecord(
                mint=pos.mint, source=pos.source,
                entry_price=pos.entry_price_usd,
                exit_price=average_exit_usd,
                pct=total_pnl_pct, pnl_rub=pos.realized_pnl_rub,
                reason=' + '.join(pos.exit_reasons),
                hold_sec=max(0.0, time.time() - pos.opened_at_epoch),
                created_at=utc_now(), portfolio=book.profile.name,
            )
            try:
                self.trades_queue.put_nowait(record)
            except asyncio.QueueFull:
                log(f'⚠️ {book.profile.name} trade DB queue full; fill remains in telemetry.', Color.YELLOW)
            closed = {
                'mint': pos.mint,
                'reason': record.reason,
                'pnl_rub': round(pos.realized_pnl_rub, 6),
                'pnl_pct': round(total_pnl_pct, 6),
                'snapshot_pnl_rub': round(pos.realized_pnl_rub, 6),
                'snapshot_pnl_pct': round(total_pnl_pct, 6),
                'conservative_100bps_pnl_rub': (
                    round(pos.conservative_realized_pnl_rub, 6)
                    if pos.conservative_metrics_complete else None
                ),
                'conservative_100bps_pnl_pct': round(conservative_pct, 6) if conservative_pct is not None else None,
                'conservative_100bps_partial_pnl_rub': round(pos.conservative_realized_pnl_rub, 6),
                'conservative_100bps_complete': pos.conservative_metrics_complete,
                'conservative_100bps_incomplete_reason': pos.conservative_incomplete_reason,
                'gross_curve_pnl_rub': round(pos.realized_pnl_rub + pos.realized_fee_rub, 6),
                'held_sec': round(record.hold_sec, 3),
                'entry_price_usd': pos.entry_price_usd,
                'average_exit_price_usd': average_exit_usd,
                'entry_out_amount_raw': pos.entry_out_amount_raw,
                'entry_min_out_raw': pos.entry_min_out_raw,
                'network_fees_rub': round(pos.realized_fee_rub, 6),
                'onchain_fees_rub': round(pos.onchain_fees_paid_rub, 6),
            }
            book.closed_trades.append(closed)
            if collector is not None:
                collector.record_decision(
                    pos.mint, pos.source, f'{book.profile.name}_position', 'closed',
                    [record.reason],
                    {'realized_pnl_rub': round(pos.realized_pnl_rub, 6),
                     'snapshot_pnl_rub': round(pos.realized_pnl_rub, 6),
                     'conservative_100bps_pnl_rub': closed['conservative_100bps_pnl_rub'],
                     'conservative_100bps_complete': pos.conservative_metrics_complete,
                     'gross_curve_pnl_rub': closed['gross_curve_pnl_rub'],
                     'pnl_pct': round(total_pnl_pct, 6),
                     'open_positions': len(book.positions),
                     'closed_trade_count': book.closed_trade_count,
                     'paper_only': True},
                )
            log(
                f'✅ [{book.profile.name}] CLOSE {short(pos.mint)} | '
                f'{record.reason} | PnL={pos.realized_pnl_rub:+.2f} ₽ '
                f'({total_pnl_pct:+.1f}%) | cash={book.cash_balance_rub:.2f} ₽',
                Color.GREEN if pos.realized_pnl_rub >= 0 else Color.RED,
            )
        else:
            if collector is not None:
                collector.record_decision(
                    pos.mint, pos.source, f'{book.profile.name}_position',
                    'partial_fill', [reason],
                    {'sold_fraction': sold_fraction_total,
                     'remaining_fraction': pos.remaining_fraction,
                     'snapshot_pnl_rub': round(pnl_leg, 6),
                     'conservative_100bps_pnl_rub': (
                         round(shadow_pnl_leg, 6) if shadow_pnl_leg is not None else None
                     ),
                     'conservative_100bps_complete': pos.conservative_metrics_complete,
                     'remaining_pnl_pct_at_trigger': round(pnl_pct, 6),
                     'paper_only': True},
                )
            log(
                f'💰 [{book.profile.name}] PARTIAL {short(pos.mint)} | '
                f'{reason} | leg={pnl_leg:+.2f} ₽ | '
                f'remaining={pos.remaining_fraction:.0%}',
                Color.CYAN,
            )

    async def _read_curve_state_only(
        self, curve_address: str,
    ) -> Optional[tuple[CurveState, FeeContext, Optional[int]]]:
        addresses = [curve_address]
        if PUMP_GLOBAL_PDA:
            addresses.append(PUMP_GLOBAL_PDA)
        if PUMP_FEE_CONFIG_PDA:
            addresses.append(PUMP_FEE_CONFIG_PDA)
        accounts, slot = await rpc_get_multiple_accounts(
            self.session, self.settings.http_rpc_url, addresses,
            encoding='base64', commitment='processed', retry_null=True,
        )
        account = accounts.get(curve_address)
        if not isinstance(account, dict) or account.get('owner') != PUMP_PROGRAM_ID:
            return None
        raw = decode_account_data(account)
        if not raw:
            return None
        try:
            curve = CurveState.decode(raw)
        except QuoteUnavailable:
            return None
        fee_context = self._make_fee_context_from_accounts(accounts, slot)
        if fee_context is None:
            return None
        return curve, fee_context, slot

    async def _mark_poll_error(self, book: PortfolioBook, pos: SimPosition, reason: str) -> None:
        now = time.time()
        held = now - pos.opened_at_epoch
        deadline = (
            book.profile.timeout_after_trailing_sec
            if pos.best_pnl_pct >= book.profile.trailing_activation_pct
            else book.profile.timeout_before_trailing_sec
        )
        if held >= deadline:
            pos.exit_pending_reason = f'Timeout ({deadline:.0f}s) awaiting exact sell quote'
            pos.exit_pending_since_epoch = pos.exit_pending_since_epoch or now
            pos.status = 'exit_pending'
        should_record = (
            pos.last_poll_error_type != reason
            or now - pos.last_poll_error_logged_epoch >= 10.0
        )
        if should_record:
            collector = self._collector()
            if collector is not None:
                collector.record_decision(
                    pos.mint, pos.source, f'{book.profile.name}_position_poll',
                    'quote_pending' if pos.exit_pending_reason else 'mark_unavailable',
                    [reason], {'held_sec': round(held, 3), 'status': pos.status,
                               'pending_reason': pos.exit_pending_reason,
                               'paper_only': True},
                )
            log(f'⏳ [{book.profile.name}] {short(pos.mint)} mark unavailable ({reason}); no fill counted.', Color.YELLOW)
            pos.last_poll_error_type = reason
            pos.last_poll_error_logged_epoch = now

    async def close(self) -> dict[str, Any]:
        self._stopping.set()
        if self._poll_task is not None:
            try:
                await asyncio.wait_for(self._poll_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._poll_task.cancel()
                await asyncio.gather(self._poll_task, return_exceptions=True)
        self.ended_at_epoch = time.time()
        for book in self.books.values():
            book.save(self.run_id)
        summary = self.summary()
        summary_path = self.run_dir / 'ab_summary.json'
        tmp = summary_path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(tmp, summary_path)
        return summary

    def summary(self) -> dict[str, Any]:
        portfolios: dict[str, Any] = {}
        for name, book in self.books.items():
            open_positions = [p.to_dict() for p in book.positions.values() if p.status == 'open']
            pending_positions = [p.to_dict() for p in book.positions.values() if p.status == 'exit_pending']
            portfolios[name] = {
                'initial_balance_rub': book.profile.initial_balance_rub,
                'cash_balance_rub': round(book.cash_balance_rub, 6),
                'open_entry_fee_liability_rub': round(
                    sum(p.remaining_entry_fee_rub for p in book.positions.values()), 6
                ),
                'net_cash_after_open_entry_fee_liability_rub': round(
                    book.cash_balance_rub - sum(p.remaining_entry_fee_rub for p in book.positions.values()), 6
                ),
                'realized_pnl_rub': round(book.realized_pnl_rub, 6),
                'snapshot_pnl_rub': round(book.realized_pnl_rub, 6),
                'gross_curve_pnl_rub': round(book.gross_curve_pnl_rub, 6),
                'realized_network_fees_rub': round(book.realized_network_fees_rub, 6),
                'conservative_100bps_pnl_rub': (
                    round(book.conservative_100bps_pnl_rub, 6)
                    if book.conservative_100bps_incomplete_trades == 0
                    and all(p.conservative_metrics_complete for p in book.positions.values()) else None
                ),
                'conservative_100bps_pnl_partial_sum_rub': round(book.conservative_100bps_pnl_rub, 6),
                'conservative_100bps_incomplete_trades': book.conservative_100bps_incomplete_trades,
                'conservative_100bps_complete': (
                    book.conservative_100bps_incomplete_trades == 0
                    and all(p.conservative_metrics_complete for p in book.positions.values())
                ),
                'fees_paid_rub': round(book.fees_paid_rub, 6),
                'onchain_fees_paid_rub': round(book.onchain_fees_paid_rub, 6),
                'entry_count': book.entry_count,
                'closed_trade_count': book.closed_trade_count,
                'open_position_count': len(open_positions),
                'pending_exit_count': len(pending_positions),
                'open_positions': open_positions,
                'pending_positions': pending_positions,
                'poll_errors': book.poll_errors,
                'closed_trades': book.closed_trades,
                'max_slots': book.profile.max_slots,
                'position_size_rub': book.profile.position_size_rub,
            }
        return {
            'run_id': self.run_id,
            'started_at_epoch': self.started_at_epoch,
            'ended_at_epoch': self.ended_at_epoch or time.time(),
            'duration_sec': round((self.ended_at_epoch or time.time()) - self.started_at_epoch, 3),
            'paper_only': True,
            'signed_transactions': 0,
            'submitted_transactions': 0,
            'portfolio_scope': ['portfolio_a', 'portfolio_b'],
            'early_stop_reason': self.early_stop_reason,
            'fee_model': 'official @pump-fun/pump-sdk 4.1.0 exact integer curve quote; out_amount_raw is the snapshot fill; min_out_raw is the 100 bps conservative shadow; on-chain Global + FeeConfig + per-curve creator_fee_bps; fail-closed when unsupported',
            'jupiter_policy': 'GET ExactIn fallback only after on-chain complete=true; no active-curve entry/exit Jupiter quotes',
            'portfolio_a': portfolios['portfolio_a'],
            'portfolio_b': portfolios['portfolio_b'],
            'rpc_metrics': rpc_metrics_snapshot(),
        }
