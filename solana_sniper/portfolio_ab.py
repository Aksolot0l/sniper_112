"""Isolated dual-portfolio A/B Pump.fun paper simulator.

No wallet keys, signing, transaction construction, or transaction submission are
used. Entry/exit prices on active non-Mayhem SOL curves come from the official
Pump SDK 4.1.0 integer quote math port in pump_curve_quote.py. Jupiter is queried
only after an on-chain curve read confirms `complete=true`.
"""
from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import random
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
    that delta cap was an anti-filter: Turtle (33.95 -> 45.51) and Shigure
    (33.16 -> 38.59) were rejected while dead launches with ``delta == 0.0``
    were bought (8 of 9 such fills were losing decoys). The replacement
    requires *live momentum*: a real inflow of curve reserves inside the first
    observation window.

    Note on the others: CAVELON (36.0) and PERFORMA (39.88) are *not* evidence
    against the anti-filter any more - with the 34.5 birth ceiling from fix #7
    they are rejected as ``birth_out_of_band``, because ``v_sol_birth`` is the
    dev's own buy (29.96 + dev_buy_SOL, corr 0.99988).
    """

    name: str
    # --- entry: live-momentum band (shared by both arms) ---
    v_sol_birth_min: float = 30.5
    # 34.5, not 50: run #1 (build 9f2cd08) measured v_sol_birth = 29.96 + dev_buy
    # with corr 0.99988 (n=145). The 50 SOL ceiling therefore admitted launches
    # where the dev bought 4-6 SOL of his own token: of 50 such candidates 36
    # fell <= -25% (median minimum -33.2%), and in the live run they were 4 mints
    # for -538.57 RUB in A and -552.79 RUB in B. Ceiling 34.5 keeps dev buys up
    # to ~4.5 SOL. This cuts the catastrophic tail; it does not create an edge.
    v_sol_birth_max: float = 34.5
    delta_min_sol: float = 0.10
    # 2.5, not 12: across 66 archived entry_state rows joined with observations,
    # delta >= 2.5 SOL gave a median -19.8% drawdown from the entry point and
    # 10 of 15 such entries ended in the stop loss; Turtle (11.56 SOL) went
    # -18.7% within 15 s of its own entry. The 12 SOL ceiling fitted the filter
    # to a single observation.
    delta_max_sol: float = 2.5
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
    # --- treatment (run #3: B differs from A ONLY by the delta cap) ---
    require_creator_gate: bool = False
    guard_min_signatures: int = 25
    # --- shadows (run #3): virtual books, 0 RUB of real capital ---
    is_shadow: bool = False
    capital_rub: float = 1_500.0
    # Copy the entries of this arm instead of evaluating the entry rule again.
    mirror_of: Optional[str] = None
    # shadow_exit25: sell 100% on the first tick at >= this PnL; TP1/trailing/
    # shelf/no-progress are disabled, SL and the 35 s timeout stay.
    single_exit_pct: Optional[float] = None
    initial_balance_rub: float = 1_500.0
    position_size_rub: float = 500.0
    max_slots: int = 3

    def entry_check(
        self,
        v_sol_birth: float,
        v_sol_now: float,
        delta_max_override: Optional[float] = None,
    ) -> tuple[bool, str]:
        delta = v_sol_now - v_sol_birth
        cap = self.delta_max_sol if delta_max_override is None else float(delta_max_override)
        if not (self.v_sol_birth_min <= v_sol_birth <= self.v_sol_birth_max):
            return False, 'birth_out_of_band'
        if delta < self.delta_min_sol:
            return False, 'no_live_momentum'
        if delta > cap:
            return False, 'momentum_too_late'
        return True, 'live_momentum_entry'


DELTA_BUCKETS: tuple[tuple[str, float, float], ...] = (
    # label, lower bound (inclusive), upper bound (exclusive)
    ('0', float('-inf'), 0.0000001),
    ('0.1-1', 0.0000001, 1.0),
    ('1-2.5', 1.0, 2.5),
    ('2.5-4', 2.5, 4.0),
    ('4-12', 4.0, 12.0),
    ('>12', 12.0, float('inf')),
)
SHADOW_DELTA_CAPS: tuple[float, ...] = (1.0, 2.5, 3.0, 4.0, 12.0)


def delta_bucket(delta_sol: float) -> str:
    """Coarse label for telemetry; pure arithmetic, no RPC."""
    for label, lower, upper in DELTA_BUCKETS:
        if lower <= delta_sol < upper:
            return label
    return '>12'


def entry_shadow_flags(
    profile: 'ABProfile', v_sol_birth: float, v_sol_now: float,
) -> dict[str, bool]:
    """Counterfactual pass/fail for alternative delta caps (telemetry only)."""
    return {
        f'pass_if_cap_{cap:g}': profile.entry_check(
            v_sol_birth, v_sol_now, delta_max_override=cap,
        )[0]
        for cap in SHADOW_DELTA_CAPS
    }


# Guard reasons that mean "we could not see the creator" (probe not ok).
_GUARD_UNAVAILABLE_REASONS = frozenset({
    'creator_history_unavailable',
    'creator_token_ata_missing',
    'creator_guard_unavailable',
    'creator_guard_censored',
})


def gate_status(
    gate_passed: bool,
    guard_reason: str,
    probe: Optional[dict[str, Any]] = None,
) -> str:
    """Map one guard outcome to ``pass`` / ``fail_history`` / ``fail_risk`` /
    ``unavailable``.

    Run #2 called every ``creator_history_too_short`` row "unavailable", which
    produced ``unavailable_share = 46.15 %`` and a false "telemetry failure"
    line in SUMMARY.md. A successfully read page with 1-2 signatures is a
    *verdict* (``fail_history``), not a blind spot: ``unavailable`` is reserved
    for ``probe_status != ok`` or a censored probe. The payload still comes
    from the same read, so no extra RPC is involved.
    """
    if gate_passed:
        return 'pass'
    reason = str(guard_reason or '')
    data = probe or {}
    status = str(data.get('status') or 'unknown')
    censored = bool(data.get('censored'))
    identity = str(data.get('creator_identity_type') or '')
    resolved_actor = identity == 'pumpportal_event_actor' and bool(
        data.get('pumpportal_event_actor')
    )
    if reason == 'creator_history_too_short':
        if status == 'ok' and not censored:
            # The page was read: 1-2 signatures is a verdict, not a blind spot.
            return 'fail_history'
        if status == 'unknown' and resolved_actor:
            # Legacy rows without probe_status: a resolved deployer is still a
            # real verdict, the curve fee vault is not.
            return 'fail_risk'
        return 'unavailable'
    if censored or status != 'ok' or reason in _GUARD_UNAVAILABLE_REASONS:
        # Unknown status can only happen on legacy rows; keep them fail-closed.
        if status == 'unknown' and reason not in _GUARD_UNAVAILABLE_REASONS and not censored:
            return 'fail_risk'
        return 'unavailable'
    return 'fail_risk'


PROFILE_A = ABProfile(name='portfolio_a', delta_max_sol=2.5)
PROFILE_B = ABProfile(name='portfolio_b', delta_max_sol=3.0)

# Virtual books: same 1 500 RUB / 500 RUB / 3 slots accounting, 0 RUB of real
# capital, never fed into ab_summary's live arms.
SHADOW_GATE = ABProfile(
    name='shadow_gate', delta_max_sol=2.5, require_creator_gate=True,
    is_shadow=True, capital_rub=0.0, mirror_of='portfolio_a',
)
SHADOW_EXIT25 = ABProfile(
    name='shadow_exit25', delta_max_sol=2.5, is_shadow=True, capital_rub=0.0,
    mirror_of='portfolio_a', single_exit_pct=25.0,
)
SHADOW_PROFILES = (SHADOW_GATE, SHADOW_EXIT25)


def profiles_from_settings(settings: Any) -> dict[str, 'ABProfile']:
    """Build both live arms and both shadows from environment-driven settings.

    Run #3 invariant: the arms differ in exactly ONE number, ``delta_max_sol``
    (A 2.5, B 3.0). Both creator gates are off, so a difference in the result
    can only be attributed to the cap. The gate lives on as ``shadow_gate``.
    """
    shared = {
        'v_sol_birth_min': float(getattr(settings, 'entry_birth_min_sol', 30.5)),
        'v_sol_birth_max': float(getattr(settings, 'entry_birth_max_sol', 34.5)),
        'delta_min_sol': float(getattr(settings, 'entry_delta_min_sol', 0.10)),
    }
    guard_min_sig = int(getattr(settings, 'creator_guard_min_sig', 25))
    a = replace(
        PROFILE_A, **shared,
        delta_max_sol=float(getattr(settings, 'entry_delta_max_sol', 2.5)),
    )
    b = replace(
        PROFILE_B, **shared,
        delta_max_sol=float(getattr(settings, 'entry_delta_max_sol_b', 3.0)),
    )
    shadow_gate = replace(SHADOW_GATE, **shared, guard_min_signatures=guard_min_sig)
    shadow_exit25 = replace(SHADOW_EXIT25, **shared)
    return {
        a.name: a, b.name: b,
        shadow_gate.name: shadow_gate, shadow_exit25.name: shadow_exit25,
    }


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
    # --- run #3 instrumentation (own mark stream, not observations.csv) ---
    entry_lag_sec: float = 0.0
    entry_premium_pct: Optional[float] = None
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    exit_trigger_pnl_pct: Optional[float] = None

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
    # Run #3: B may see twice as many signals; when slots, not the rule, cap
    # the sample, the comparison must switch to RUB per trade.
    slots_full_count: int = 0
    cash_below_slot_count: int = 0
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


BOOTSTRAP_ITERATIONS = 20_000
BOOTSTRAP_SEED = 20261022


def trade_stats(pnls: list[float], *, iterations: int = BOOTSTRAP_ITERATIONS) -> dict[str, Any]:
    """Distribution stats for one arm, including a bootstrap CI of the mean.

    Run #3 asks for a decision ("B beats A by >= 10 RUB per trade with a CI that
    excludes 0"), so the report has to carry the uncertainty, not just a sum.
    """
    values = [float(value) for value in pnls]
    n = len(values)
    if n == 0:
        return {
            'n': 0, 'sum': 0.0, 'mean': None, 'median': None, 'sd': None,
            'ci95_mean': None, 'n_for_significance': None,
            'sum_without_top2': 0.0, 'sum_without_worst2': 0.0,
        }
    total = sum(values)
    mean = total / n
    ordered = sorted(values)
    median = (
        ordered[n // 2] if n % 2
        else (ordered[n // 2 - 1] + ordered[n // 2]) / 2.0
    )
    sd = math.sqrt(sum((v - mean) ** 2 for v in values) / (n - 1)) if n > 1 else 0.0
    ci95 = None
    if n > 1:
        rng = random.Random(BOOTSTRAP_SEED)
        means = []
        for _ in range(iterations):
            sample = rng.choices(values, k=n)
            means.append(sum(sample) / n)
        means.sort()
        ci95 = [round(means[int(0.025 * iterations)], 4),
                round(means[min(iterations - 1, int(0.975 * iterations))], 4)]
    # Per-arm n needed for 80% power at the observed effect (2.8 = 2*(1.96+0.84)).
    n_for_sig = None
    if n > 1 and mean and sd:
        n_for_sig = int(math.ceil((2.8 * sd / abs(mean)) ** 2))
    return {
        'n': n,
        'sum': round(total, 4),
        'mean': round(mean, 4),
        'median': round(median, 4),
        'sd': round(sd, 4),
        'ci95_mean': ci95,
        'n_for_significance': n_for_sig,
        'sum_without_top2': round(sum(ordered[:max(0, n - 2)]), 4),
        'sum_without_worst2': round(sum(ordered[2:]) if n > 2 else 0.0, 4),
    }


def mean_difference_ci(
    a_values: list[float], b_values: list[float],
    *, iterations: int = BOOTSTRAP_ITERATIONS,
) -> dict[str, Any]:
    """Bootstrap CI of mean(B) - mean(A); the run #3 decision statistic."""
    a = [float(v) for v in a_values]
    b = [float(v) for v in b_values]
    if not a or not b:
        return {'mean_diff': None, 'ci95': None, 'n_a': len(a), 'n_b': len(b)}
    mean_a = sum(a) / len(a)
    mean_b = sum(b) / len(b)
    rng = random.Random(BOOTSTRAP_SEED + 1)
    diffs = []
    for _ in range(iterations):
        sample_a = rng.choices(a, k=len(a))
        sample_b = rng.choices(b, k=len(b))
        diffs.append(sum(sample_b) / len(b) - sum(sample_a) / len(a))
    diffs.sort()
    return {
        'mean_a': round(mean_a, 4),
        'mean_b': round(mean_b, 4),
        'mean_diff': round(mean_b - mean_a, 4),
        'ci95': [round(diffs[int(0.025 * iterations)], 4),
                 round(diffs[min(iterations - 1, int(0.975 * iterations))], 4)],
        'n_a': len(a),
        'n_b': len(b),
    }


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
        # Creator-gate funnel for the manifest/SUMMARY (B arm only).
        self.gate_status_counts: dict[str, int] = {
            'pass': 0, 'fail_history': 0, 'fail_risk': 0, 'unavailable': 0,
        }
        self.gate_reason_counts: dict[str, int] = {}
        self.probe_identity_counts: dict[str, int] = {}
        # Per-position tick stream (own marks, not observations.csv).
        self.ticks: list[dict[str, Any]] = []

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
        self.record_entry_state(
            coin, snapshot, birth_v_sol, current_v_sol, decision_epoch,
        )
        # Live arms decide first (concurrently); mirrors then copy A's entries,
        # so a shadow can never hold a position that its source arm rejected.
        await asyncio.gather(*(
            self._consider_entry(
                book, coin, snapshot, birth_v_sol, current_v_sol,
                sol_usd, filter_metrics, decision_epoch,
            )
            for book in self.books.values() if book.profile.mirror_of is None
        ))
        mirrors = [book for book in self.books.values() if book.profile.mirror_of]
        if mirrors:
            await asyncio.gather(*(
                self._consider_entry(
                    book, coin, snapshot, birth_v_sol, current_v_sol,
                    sol_usd, filter_metrics, decision_epoch,
                )
                for book in mirrors
            ))

    def record_entry_state(
        self,
        coin: NewCoin,
        snapshot: 'EntrySnapshot',
        birth_v_sol: float,
        current_v_sol: float,
        decision_epoch: float,
        *,
        filter_metrics: Optional[dict[str, Any]] = None,
    ) -> None:
        """One shared `entry_state` row per candidate: delta bucket + shadow caps.

        Pure arithmetic on already-fetched values; no RPC. Also reused by the
        offline smoke test so its funnel matches production semantics.
        """
        collector = self._collector()
        if collector is None:
            return
        # delta_cap_used_sol must be the cap of the arm that admits, not
        # whatever book happens to be first (shadow books share A's cap).
        a_book = self.books.get('portfolio_a')
        base_profile = a_book.profile if a_book is not None else next(
            book.profile for book in self.books.values() if not book.profile.is_shadow
        )
        delta_sol = current_v_sol - birth_v_sol
        # Free create-message fields (no RPC): dev's own buy, used later to
        # discriminate rockets from corpses inside the wide delta buckets.
        packet = coin.packet or {}
        collector.record_decision(
            coin.mint, coin.source, 'entry_state', 'fresh_state_ok', [],
            {'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
             'dev_buy_sol': packet.get('dev_buy_sol'),
             'dev_buy_sol_raw': packet.get('dev_buy_sol_raw'),
             'initial_buy_raw': packet.get('initial_buy_raw'),
             'dev_bought_at_birth': (
                 bool(packet.get('dev_buy_sol'))
                 if packet.get('dev_buy_sol') is not None else None
             ),
             'delta_sol': round(delta_sol, 6),
             'delta_bucket': delta_bucket(delta_sol),
             'delta_cap_used_sol': base_profile.delta_max_sol,
             **entry_shadow_flags(base_profile, birth_v_sol, current_v_sol),
             'curve_complete': snapshot.curve.complete,
             'is_mayhem_mode': snapshot.curve.is_mayhem_mode,
             'curve_context_slot': snapshot.context_slot,
             'fee_context_observed_at_epoch': snapshot.fee_context.observed_at_epoch,
             'fee_tiers': snapshot.fee_context.manifest()['fee_tiers'],
             'decision_at_epoch': decision_epoch,
             'first_seen_to_decision_ms': round(
                 max(0.0, (decision_epoch - coin.found_at) * 1000.0), 2,
             ),
             'creator_probe': coin.creator_probe_result,
             'shared_entry_snapshot': True,
             **(filter_metrics or {}),
            },
        )

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
        if reason == 'three_slots_already_open_or_pending':
            book.slots_full_count += 1
        elif reason == 'cash_below_slot_size':
            book.cash_below_slot_count += 1
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
        if profile.mirror_of:
            source = self.books.get(profile.mirror_of)
            if source is None or coin.mint not in source.positions:
                self._record_portfolio_skip(
                    book, coin, f'mirror_not_open_in_{profile.mirror_of}',
                    birth_v_sol, current_v_sol,
                )
                return
        if profile.require_creator_gate:
            probe = coin.creator_probe_result or {}
            guard_passed = bool(probe.get('creator_guard_passed'))
            guard_reason = str(probe.get('creator_guard_reason') or 'creator_guard_unavailable')
            censored = bool(probe.get('censored')) or not probe
            decision = 'pass' if guard_passed else ('censored' if censored else 'fail')
            gate_class = gate_status(guard_passed, guard_reason, probe)
            self.gate_status_counts[gate_class] = self.gate_status_counts.get(gate_class, 0) + 1
            if not guard_passed:
                self.gate_reason_counts[guard_reason] = self.gate_reason_counts.get(guard_reason, 0) + 1
            identity_key = str(probe.get('creator_identity_type') or 'unknown')
            self.probe_identity_counts[identity_key] = self.probe_identity_counts.get(identity_key, 0) + 1
            if collector is not None:
                collector.record_decision(
                    coin.mint, coin.source, f'{profile.name}_creator_gate', decision,
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
                     'creator_prior_tokens_10m': probe.get('creator_prior_tokens_10m'),
                     'creator_repeat_deploy_in_session': probe.get('creator_repeat_deploy_in_session'),
                     'ata_exists': probe.get('ata_exists'),
                     'token_balance_raw': probe.get('token_balance_raw'),
                     'probe_status': probe.get('status'),
                     'probe_latency_ms': probe.get('latency_ms'),
                     'probe_errors': probe.get('errors') or [],
                     'guard_reason': guard_reason,
                     # pass / fail_risk / unavailable — never mix a blind spot
                     # with a real risk verdict (run #1: 8 of 12 A-only mints
                     # were telemetry failures, not gate signals).
                     'gate_status': gate_class,
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
            # Premium paid over the curve mid at the same snapshot: entry price
            # is always worse than observations t0, which is half of the
            # "zero movement becomes -6..-8%" effect. Pure arithmetic, no RPC.
            try:
                v_tokens_ui = snapshot.curve.virtual_token_reserves / TOKEN_SCALE
                mid_price_usd = (
                    current_v_sol / v_tokens_ui * sol_usd if v_tokens_ui > 0 else 0.0
                )
            except (AttributeError, TypeError, ZeroDivisionError):
                mid_price_usd = 0.0
            entry_premium_pct = (
                (entry_price_usd / mid_price_usd - 1.0) * 100.0
                if mid_price_usd > 0 and entry_price_usd > 0 else None
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
                entry_lag_sec=round(max(0.0, decision_epoch - coin.found_at), 3),
                entry_premium_pct=(
                    round(entry_premium_pct, 4) if entry_premium_pct is not None else None
                ),
            )
            book.cash_balance_rub -= actual_cost_rub
            book.fees_paid_rub += entry_fee_rub
            book.onchain_fees_paid_rub += onchain_entry_fee_rub
            book.entry_count += 1
            book.positions[coin.mint] = pos
            # t=0 of this position's own tick stream.
            self._record_tick(book, pos, 0.0, decision_epoch, entry_price_usd * fx, sol_usd, event='entry')
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
            # Shadows hold virtual money and never deplete; including them
            # would disable the early stop entirely.
            for book in self.books.values() if not book.profile.is_shadow
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
                    pos.mfe_pct = max(pos.mfe_pct, pnl_pct)
                    pos.mae_pct = min(pos.mae_pct, pnl_pct)
                    self._record_tick(book, pos, pnl_pct, mark_at, unit_price_rub, sol_usd)
                    trigger = self._exit_trigger(book.profile, pos, pnl_pct, mark_at)
                    if trigger is not None:
                        pos.exit_trigger_pnl_pct = round(pnl_pct, 6)
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

    def _record_tick(
        self,
        book: PortfolioBook,
        pos: SimPosition,
        pnl_pct: float,
        at_epoch: float,
        unit_price_rub: float,
        sol_usd: float,
        event: str = 'mark',
    ) -> None:
        """One row of this position's own tick stream (exports/ticks.csv).

        Marks come from the exit quote the bot actually sees, not from the
        scheduled observations, so MFE/MAE and the trigger price are the ones
        the ladder really acted on.
        """
        fx = max(1e-9, float(self.settings.usd_rub_rate))
        price_usd = unit_price_rub / (sol_usd * fx) if sol_usd > 0 else None
        self.ticks.append({
            'portfolio': book.profile.name,
            'mint': pos.mint,
            't_sec': round(max(0.0, at_epoch - pos.opened_at_epoch), 3),
            'price_usd': round(price_usd, 12) if price_usd else None,
            'pnl_pct': round(pnl_pct, 4),
            'event': event,
        })

    def write_ticks(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=('portfolio', 'mint', 't_sec', 'price_usd', 'pnl_pct', 'event'),
            )
            writer.writeheader()
            writer.writerows(self.ticks)

    def write_shadow_fills(self, path: Path) -> None:
        """Closed shadow trades only; the live arms stay in telemetry/paper_fills."""
        path.parent.mkdir(parents=True, exist_ok=True)
        fields = (
            'portfolio', 'mint', 'reason', 'pnl_rub', 'pnl_pct',
            'entry_lag_sec', 'entry_premium_pct', 'mfe_pct', 'mae_pct',
            'trigger_pct', 'fill_pct', 'conservative_100bps_pnl_rub',
        )
        with path.open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            for book in self.books.values():
                if not book.profile.is_shadow:
                    continue
                for trade in book.closed_trades:
                    writer.writerow({'portfolio': book.profile.name, **trade})

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

        # shadow_exit25: 100% on the first tick at or above the threshold.
        # TP1, trailing, shelf and no-progress are disabled by design; SL and
        # the 35 s timeout are kept exactly as in the live ladder.
        if profile.single_exit_pct is not None:
            if pnl_pct <= -profile.stop_loss_pct:
                return f'SL (−{profile.stop_loss_pct:.1f}%)', pos.remaining_tokens_raw
            if pnl_pct >= profile.single_exit_pct:
                return (
                    f'Exit{profile.single_exit_pct:.0f} '
                    f'(+{profile.single_exit_pct:.1f}%; pnl={pnl_pct:+.1f}%)',
                    pos.remaining_tokens_raw,
                )
            timeout = profile.timeout_before_trailing_sec
            if held >= timeout:
                return f'Timeout ({timeout:.1f}s)', pos.remaining_tokens_raw
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
                    f'SL/trail (peak={pos.best_pnl_pct:+.1f}%, '
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

        # Fill tick: realized PnL of the position so far, at the real fill price.
        realized_pct_now = pos.realized_pnl_rub / max(1e-9, pos.entry_cost_rub) * 100.0
        fx_for_tick = max(1e-9, float(self.settings.usd_rub_rate))
        self._record_tick(
            book, pos, realized_pct_now, time.time(),
            exit_price_usd * sol_usd * fx_for_tick, sol_usd, event='exit',
        )
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
                # Run #3 instrumentation, all from the position's own marks.
                'entry_lag_sec': pos.entry_lag_sec,
                'entry_premium_pct': pos.entry_premium_pct,
                'mfe_pct': round(pos.mfe_pct, 6),
                'mae_pct': round(pos.mae_pct, 6),
                'trigger_pct': pos.exit_trigger_pnl_pct,
                'fill_pct': round(total_pnl_pct, 6),
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
        # Run #3 artefacts: the per-position mark stream and the shadow books'
        # fills, both plain files next to ab_summary.json.
        export_dir = Path(os.getenv('TELEMETRY_EXPORT_DIR', str(self.run_dir / 'exports')))
        try:
            self.write_ticks(export_dir / 'ticks.csv')
        except Exception as exc:  # never let an artefact kill the finalization
            log(f'⚠️ ticks.csv не записан ({type(exc).__name__})', Color.YELLOW)
        try:
            self.write_shadow_fills(export_dir / 'shadow_fills.csv')
        except Exception as exc:
            log(f'⚠️ shadow_fills.csv не записан ({type(exc).__name__})', Color.YELLOW)
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
                'is_shadow': book.profile.is_shadow,
                'mirror_of': book.profile.mirror_of,
                'single_exit_pct': book.profile.single_exit_pct,
                'delta_max_sol': book.profile.delta_max_sol,
                'delta_min_sol': book.profile.delta_min_sol,
                'require_creator_gate': book.profile.require_creator_gate,
                # The two counters that tell a rule failure from a capacity cap.
                'slots_full': book.slots_full_count,
                'cash_below_slot': book.cash_below_slot_count,
                'stats': trade_stats([t.get('pnl_rub', 0.0) for t in book.closed_trades]),
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
            'shadow_scope': [name for name, book in self.books.items()
                             if book.profile.is_shadow],
            'early_stop_reason': self.early_stop_reason,
            'profiles': {name: {
                'delta_max_sol': book.profile.delta_max_sol,
                'delta_min_sol': book.profile.delta_min_sol,
                'require_creator_gate': book.profile.require_creator_gate,
                'is_shadow': book.profile.is_shadow,
                'mirror_of': book.profile.mirror_of,
                'single_exit_pct': book.profile.single_exit_pct,
                'capital_rub': book.profile.capital_rub,
            } for name, book in self.books.items()},
            'fee_model': 'official @pump-fun/pump-sdk 4.1.0 exact integer curve quote; out_amount_raw is the snapshot fill; min_out_raw is the 100 bps conservative shadow; on-chain Global + FeeConfig + per-curve creator_fee_bps; fail-closed when unsupported',
            'jupiter_policy': 'GET ExactIn fallback only after on-chain complete=true; no active-curve entry/exit Jupiter quotes',
            'portfolio_a': portfolios['portfolio_a'],
            'portfolio_b': portfolios['portfolio_b'],
            # Shadows: same entries as A, virtual capital, never traded.
            **{name: book for name, book in portfolios.items()
               if name not in ('portfolio_a', 'portfolio_b')},
            'ab_test': {
                'single_difference': 'delta_max_sol',
                'portfolio_a': {'delta_max_sol': 2.5, 'require_creator_gate': False},
                'portfolio_b': {'delta_max_sol': 3.0, 'require_creator_gate': False},
                'decision_rule': (
                    'B beats A iff mean(B) - mean(A) >= +10 RUB '
                    'and the 95% CI of the difference excludes 0'
                ),
                'difference': mean_difference_ci(
                    [t.get('pnl_rub', 0.0) for t in portfolios['portfolio_a']['closed_trades']],
                    [t.get('pnl_rub', 0.0) for t in portfolios['portfolio_b']['closed_trades']],
                ),
            },
            # B-arm gate funnel: a blind spot must never be counted as a risk
            # signal (run #1: 8 of 12 A-only mints were telemetry failures).
            'creator_gate': {
                'status_counts': dict(sorted(self.gate_status_counts.items())),
                'reason_counts': dict(sorted(self.gate_reason_counts.items())),
                'probe_identity_counts': dict(sorted(self.probe_identity_counts.items())),
                'unavailable_share': (
                    round(
                        self.gate_status_counts.get('unavailable', 0)
                        / max(1, sum(self.gate_status_counts.values())), 4,
                    )
                ),
            },
            'rpc_metrics': rpc_metrics_snapshot(),
        }
