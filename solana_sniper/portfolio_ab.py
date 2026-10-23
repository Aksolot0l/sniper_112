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
import shutil
import statistics
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp

from config import Settings
from console import Color, log, short
from enrich import EnrichCollector
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
from telemetry import (
    CandidateCollector,
    get_collector,
    observation_peaks,
    utc_now,
)


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
    # Run #7: arm identity for the report. `arm_id` is the short label
    # (A / B1 / B2 / B3) and `entry_band_label` is the one band that separates
    # this arm from A, printed verbatim in SUMMARY.md.
    arm_id: str = 'A'
    entry_band_label: str = '30.5 <= v_sol_birth <= 34.5; 0.10 <= delta <= 2.50'
    # --- entry: live-momentum band (shared by every arm) ---
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
    # Optional: derive the cap from the birth ceiling instead of a constant
    # (kept for the deferred `delta_min = 0` experiment, off in run #4).
    delta_max_from_birth_ceiling: bool = False
    # True only for the deferred t0-entry experiment; run #4 enters like A.
    entry_at_detection: bool = False
    # --- entry latency model (instrument, identical for every arm) ---
    # The fill price always comes from the real on-chain curve read, never from
    # the WSS packet. `entry_latency_ms` is the modelled execution delay between
    # detection and the fill; the measured delay of every trade is written as
    # `entry_lag_ms`, and a read that arrives earlier than the model is counted
    # as a floor violation instead of being silently priced at t0.
    entry_latency_ms: float = 150.0
    # --- exit rule: the approved 4-mode ladder, identical in EVERY arm ---
    # Run #4's time exit was closed by the paired test (B-A = -4.24 RUB,
    # CI [-19.2, +10.7]) and its toggles are gone: run #7 varies the entry band
    # only, so no arm can differ from A in the exit.
    tp1_sell_pct: float = 50.0
    # --- exit ladder (approved 4-mode logic, arm A) ---
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
    # --- run #8: a second, mutually exclusive exit rule -------------------
    # 'ladder'    -> the Round 6 4-mode ladder above (run #7 behaviour);
    # 'fee_cover' -> three rules only: hard stop, cover the round trip, and a
    #                short clock. TP1 / trail / shelf / no-progress are NOT
    #                called in that mode, so they cannot appear in the report.
    #
    # The threshold is measured data, not a guess: the round trip costs 3.25 %
    # of the stake (run #7, 202 trades whose price did not move during 5 s), so
    # 3.5 % covers the fees plus 0.25 pp of slippage.
    exit_mode: str = 'ladder'
    fee_cover_pct: float = 3.5
    time_exit_sec: float = 5.0
    hard_stop_pct: float = 10.0
    # --- run #9: name-copy filter, a per-book entry parameter ---------------
    # `None` (the default) means the filter is off. An integer N means: reject
    # a candidate if the same symbol (case-insensitive) was already launched
    # within `name_copies_window_sec` before it, N or more times. N=0 is the
    # strictest setting — "this symbol has been launched in the last 120 s, do
    # not enter". An empty symbol is never filtered: there is nothing to match.
    # Pure in-memory bookkeeping, no RPC.
    max_name_copies_120s: Optional[int] = None
    name_copies_window_sec: float = 120.0
    # Accounting only; a book never shares cash with another arm.
    capital_rub: float = 1_500.0
    initial_balance_rub: float = 1_500.0
    position_size_rub: float = 500.0
    max_slots: int = 3
    # Run #3 addendum: when the cash can no longer fund one 500 RUB slot, the
    # book is topped back up to `initial_balance_rub` and the trade proceeds.
    # Accounting only: no entry/exit decision reads the injected capital.
    auto_top_up: bool = True

    def delta_cap(self, v_sol_birth: float) -> float:
        """Upper bound on delta for this candidate."""
        if self.delta_max_from_birth_ceiling:
            return max(0.0, self.v_sol_birth_max - v_sol_birth)
        return self.delta_max_sol

    def entry_check(
        self,
        v_sol_birth: float,
        v_sol_now: float,
        delta_max_override: Optional[float] = None,
    ) -> tuple[bool, str]:
        delta = v_sol_now - v_sol_birth
        cap = self.delta_cap(v_sol_birth) if delta_max_override is None else float(delta_max_override)
        if not (self.v_sol_birth_min <= v_sol_birth <= self.v_sol_birth_max):
            return False, 'birth_out_of_band'
        if delta < self.delta_min_sol:
            return False, 'entry_below_delta_min' if self.delta_min_sol <= 0 else 'no_live_momentum'
        if delta > cap:
            return False, 'momentum_too_late'
        return True, 'first_read_entry' if self.entry_at_detection else 'live_momentum_entry'


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


# Run #7: four arms, one entry band each.
#
# A is the Round 6 reference and is not touched in any symbol: birth
# 30.5-34.5, delta 0.10-2.5, the 4-mode ladder, 3 slots, 500 RUB. B1/B2/B3
# each move exactly ONE boundary of that band, so every comparison against A
# is attributable and paired on the coins both arms saw.
PROFILE_A = ABProfile(name='portfolio_a', arm_id='A')

ARM_IDS: tuple[str, ...] = (
    'portfolio_a', 'portfolio_b1_narrow', 'portfolio_b2_birth', 'portfolio_b3_t0',
    # Run #8 (30-minute fee-cover experiment): S1 is exactly B1, S0 is S1 with
    # the momentum gate removed. They are a pair of their own — S0 is compared
    # with S1, not with B1 — so `ARM_REFERENCE` says who each arm is measured
    # against. A is still the reference of every run #7 arm.
    'portfolio_s1', 'portfolio_s0',
    # Run #9 (12-hour S0 vs S2 run): S0 is run #8's S0 unchanged, S2 moves two
    # entry numbers only — the birth floor 31.96 (dev buy >= 2 SOL) and the
    # name-copy filter. The exit, the stake, the slots and the cadence are
    # identical in both books.
    'portfolio_s2',
)

# Default boundaries of the three experimental arms. They live in env as well
# (ENTRY_DELTA_MAX_SOL_B1 / ENTRY_BIRTH_MAX_SOL_B2 / ENTRY_DELTA_MIN_SOL_B3)
# and are written into the manifest, so a run can never be misread.
#
# B3 is the t0 arm: it keeps A's whole band and only drops the momentum floor,
# i.e. it enters on the first curve read after detection. It is the one arm
# whose expected gain is arithmetic rather than fitted: the measured
# entry_fill_premium_pct median is 1.416 % (about +7 RUB on a 500 RUB stake).
ARM_DEFAULTS: dict[str, dict[str, float]] = {
    'portfolio_b1_narrow': {'delta_max_sol': 1.00},
    'portfolio_b2_birth': {'v_sol_birth_max': 31.00},
    'portfolio_b3_t0': {'delta_min_sol': 0.00},
    # S1 = B1 verbatim. S0 = S1 with ONE boundary moved: delta_min 0.10 -> 0.00.
    'portfolio_s1': {'delta_max_sol': 1.00},
    # Run #9: S0 keeps the run #8 band AND states its birth floor explicitly,
    # so the one thing that differs from S2 is readable at a glance.
    'portfolio_s0': {'delta_max_sol': 1.00, 'delta_min_sol': 0.00,
                     'v_sol_birth_min': 30.50, 'max_name_copies_120s': None},
    # 31.96 = 29.96 (bare launch) + 2 SOL of the dev's own buy; run #9's only
    # change to the reference book is this floor plus the name-copy rule.
    'portfolio_s2': {'v_sol_birth_min': 31.96, 'delta_max_sol': 1.00,
                     'delta_min_sol': 0.00, 'max_name_copies_120s': 0},
}
# What a launch with no ARMS named runs. It is deliberately NOT every arm in
# the registry: since run #9 the registry also holds the S-books, and starting
# all of them by accident would triple the RPC bill and mix three experiments.
DEFAULT_ARMS: tuple[str, ...] = (
    'portfolio_a', 'portfolio_b1_narrow', 'portfolio_b2_birth', 'portfolio_b3_t0',
)
ARM_LABELS: dict[str, str] = {
    'portfolio_a': 'A',
    'portfolio_b1_narrow': 'B1',
    'portfolio_b2_birth': 'B2',
    'portfolio_b3_t0': 'B3',
    'portfolio_s1': 'S1',
    'portfolio_s0': 'S0',
    'portfolio_s2': 'S2',
}
# arm -> (env key, profile field). Each experimental arm names ONE boundary
# it may move; everything else about it is an explicit ARM_DEFAULTS entry, so
# S0 (delta_max 1.00 from S1 + delta_min 0.00) is readable in one place.
ARM_ENV_KEYS: dict[str, tuple[str, str]] = {
    'portfolio_b1_narrow': ('ENTRY_DELTA_MAX_SOL_B1', 'delta_max_sol'),
    'portfolio_b2_birth': ('ENTRY_BIRTH_MAX_SOL_B2', 'v_sol_birth_max'),
    'portfolio_b3_t0': ('ENTRY_DELTA_MIN_SOL_B3', 'delta_min_sol'),
    'portfolio_s1': ('ENTRY_DELTA_MAX_SOL_S1', 'delta_max_sol'),
    'portfolio_s0': ('ENTRY_DELTA_MIN_SOL_S0', 'delta_min_sol'),
    'portfolio_s2': ('ENTRY_BIRTH_MIN_S2', 'v_sol_birth_min'),
}

# Additional per-book env keys, beyond the ONE boundary `ARM_ENV_KEYS` gives
# each arm. Kept flat (arm -> tuple of (env key, profile field)) for the same
# reason: every number a run uses is named in the manifest, and a key that is
# set in the environment is never silently ignored.
#
# `MAX_NAME_COPIES_*` takes an integer or `off` (filter disabled).
ARM_ENV_EXTRA: dict[str, tuple[tuple[str, str], ...]] = {
    'portfolio_s0': (
        ('ENTRY_BIRTH_MIN_S0', 'v_sol_birth_min'),
        ('MAX_NAME_COPIES_S0', 'max_name_copies_120s'),
    ),
    'portfolio_s2': (
        ('MAX_NAME_COPIES_S2', 'max_name_copies_120s'),
    ),
}
# Which profile each arm is compared with. S0 is a control for S1, not for A:
# the question is "does the momentum gate help", not "does S0 beat A".
ARM_REFERENCE: dict[str, str] = {
    'portfolio_a': 'portfolio_a',
    'portfolio_b1_narrow': 'portfolio_a',
    'portfolio_b2_birth': 'portfolio_a',
    'portfolio_b3_t0': 'portfolio_a',
    'portfolio_s1': 'portfolio_a',
    'portfolio_s0': 'portfolio_s1',
    # Run #9 asks whether the dev-buy floor plus the name-copy filter beat the
    # unchanged book, so S2 is measured against S0 — not against A.
    'portfolio_s2': 'portfolio_s0',
}


def entry_band_label(profile: 'ABProfile') -> str:
    """The band of one arm, printed in the report next to its name.

    Run #9 adds the name-copy rule here as well: it is an entry parameter, and
    a reader must not have to open the code to learn that S2 refuses a second
    launch of the same ticker."""
    label = (
        f'{profile.v_sol_birth_min:.2f} <= v_sol_birth <= {profile.v_sol_birth_max:.2f}; '
        f'{profile.delta_min_sol:.2f} <= delta <= {profile.delta_max_sol:.2f}'
    )
    if profile.max_name_copies_120s is None:
        return f'{label}; name copies: off'
    return (
        f'{label}; name copies {profile.name_copies_window_sec:.0f}s '
        f'<= {profile.max_name_copies_120s}'
    )


def arm_profiles(overrides: Optional[dict[str, dict[str, float]]] = None) -> dict[str, 'ABProfile']:
    """A plus every single-boundary arm (pure, no env)."""
    merged = {arm: dict(values) for arm, values in ARM_DEFAULTS.items()}
    for arm, values in (overrides or {}).items():
        merged.setdefault(arm, {}).update(values)
    profiles: dict[str, ABProfile] = {'portfolio_a': PROFILE_A}
    for arm in ARM_IDS[1:]:
        profile = replace(
            PROFILE_A, name=arm, arm_id=ARM_LABELS[arm], **merged.get(arm, {}),
        )
        profiles[arm] = replace(profile, entry_band_label=entry_band_label(profile))
    profiles['portfolio_a'] = replace(
        profiles['portfolio_a'],
        entry_band_label=entry_band_label(profiles['portfolio_a']),
    )
    return profiles


def _env_float(name: str, fallback: float) -> float:
    raw = (os.getenv(name) or '').strip()
    if not raw:
        return float(fallback)
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f'{name} must be a number, got {raw!r}') from exc


def exit_mode_from_env(default: str = 'ladder') -> str:
    """Run-level exit rule. Both books of a run always share it."""
    raw = (os.getenv('EXIT_MODE') or default).strip().lower()
    if raw not in {'ladder', 'fee_cover'}:
        raise ValueError(f"EXIT_MODE must be 'ladder' or 'fee_cover', got {raw!r}")
    return raw


def _parse_name_copies(env_key: str, raw: str) -> Optional[int]:
    """`off` disables the filter; any other value is a non-negative count."""
    text = (raw or '').strip().lower()
    if text in {'off', 'none', 'no', 'false', 'null', 'disabled'}:
        return None
    try:
        value = int(float(text))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f'{env_key} must be an integer or off, got {raw!r}'
        ) from exc
    if value < 0:
        raise ValueError(f'{env_key} must be >= 0, got {raw!r}')
    return value


def allow_single_arm() -> bool:
    return (os.getenv('ALLOW_SINGLE_ARM') or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def profiles_from_settings(settings: Any) -> dict[str, 'ABProfile']:
    """Build the live arms from settings/env.

    Invariants: `delta_min_sol`, the exit ladder, the slots and the stake are
    identical in every arm; an arm named in `ARMS` differs from A in exactly
    one boundary, and the mapping arm -> boundaries is fixed in ``ARM_ENV_KEYS``.

    Run #8 adds two deliberate exceptions to "A is always present":
    ``ALLOW_SINGLE_ARM=1`` lets a run start without the reference arm (a
    30-minute fee-cover smoke compares S1 with S0, not with A), and the exit
    rule — ``EXIT_MODE`` + its three thresholds — is read once for the whole
    run so no book can end up with a different exit than its twin.
    """
    shared = {
        'v_sol_birth_min': float(getattr(settings, 'entry_birth_min_sol', 30.5)),
        'v_sol_birth_max': float(getattr(settings, 'entry_birth_max_sol', 34.5)),
        'delta_min_sol': float(getattr(settings, 'entry_delta_min_sol', 0.10)),
        'delta_max_sol': float(getattr(settings, 'entry_delta_max_sol', 2.5)),
        'entry_latency_ms': _env_float(
            'ENTRY_LATENCY_MS', float(getattr(settings, 'entry_latency_ms', 150.0)),
        ),
        'exit_mode': exit_mode_from_env(),
        'fee_cover_pct': _env_float('EXIT_FEE_COVER_PCT', 3.5),
        'time_exit_sec': _env_float('EXIT_TIME_SEC', 5.0),
        'hard_stop_pct': _env_float('EXIT_HARD_STOP_PCT', 10.0),
    }
    settings_attr = {
        'portfolio_b1_narrow': 'entry_delta_max_sol_b1',
        'portfolio_b2_birth': 'entry_birth_max_sol_b2',
        'portfolio_b3_t0': 'entry_delta_min_sol_b3',
    }
    overrides: dict[str, dict[str, float]] = {}
    for arm, (env_key, field) in ARM_ENV_KEYS.items():
        # Non-numeric defaults (the name-copy rule: an int or None) are kept
        # as they are; the single boundary below is always a float.
        defaults = {key: val for key, val in ARM_DEFAULTS.get(arm, {}).items()}
        raw = (os.getenv(env_key) or '').strip()
        if raw:
            defaults[field] = float(raw)
        else:
            defaults[field] = float(
                getattr(settings, settings_attr.get(arm, ''), defaults[field])
            )
        overrides[arm] = defaults
    # Run #9: the extra per-book keys (birth floor of the control book, the
    # name-copy rule). Applied after the single boundary so an explicit
    # ENTRY_BIRTH_MIN_S0 always wins over the ARM_DEFAULTS value.
    for arm, extra_keys in ARM_ENV_EXTRA.items():
        for env_key, field in extra_keys:
            raw = (os.getenv(env_key) or '').strip()
            if not raw:
                continue
            value: Any = (
                _parse_name_copies(env_key, raw)
                if field == 'max_name_copies_120s' else float(raw)
            )
            overrides.setdefault(arm, {})[field] = value
    wanted = [
        name.strip() for name in
        os.getenv('ARMS', ','.join(DEFAULT_ARMS)).split(',') if name.strip()
    ]
    if 'portfolio_a' not in wanted and not allow_single_arm():
        # A is the reference every other arm is compared to; without it the
        # run cannot be analysed at all.
        raise ValueError(
            'ARMS must contain portfolio_a: it is the reference arm '
            '(set ALLOW_SINGLE_ARM=1 for a deliberate A-free run)'
        )
    built: dict[str, ABProfile] = {}
    for name in wanted:
        if name not in ARM_IDS:
            raise ValueError(f'unknown ARMS entry: {name}')
        kwargs: dict[str, Any] = {**shared, 'name': name, 'arm_id': ARM_LABELS[name]}
        if name != 'portfolio_a':
            kwargs.update(overrides.get(name, {}))
        if shared['exit_mode'] == 'fee_cover':
            for field in ('fee_cover_pct', 'time_exit_sec', 'hard_stop_pct'):
                if float(shared[field]) <= 0:
                    raise ValueError(
                        f'{field} must be > 0 in fee_cover mode, got {shared[field]}'
                    )
        profile = replace(PROFILE_A, **kwargs)
        built[name] = replace(profile, entry_band_label=entry_band_label(profile))
    return built


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
    # Run #4: premium paid for waiting for momentum — mid price at the entry
    # snapshot vs the price at the coin's creation (t0). A ~ +2.1 %, B ~ 0.
    entry_premium_pct: Optional[float] = None
    # Fees + price impact inside the same snapshot (the -2.5 %/trade line).
    entry_fill_premium_pct: Optional[float] = None
    t0_price_usd: Optional[float] = None
    # Detection -> the on-chain read used for the fill, milliseconds.
    entry_lag_ms: Optional[float] = None
    # Run #8 §1.3: the entry latency chain, stage by stage. Each value is a
    # duration in ms between two real clocks, not a model:
    #   t_detect_ms  WSS packet -> candidate row written to telemetry
    #   t_filters_ms candidate row -> the security batch verdict
    #   t_read_ms    verdict -> the curve read this fill was priced from
    #   t_fill_ms    that read -> the fill row
    #   t_total_ms   WSS packet -> the fill row
    # t_read_ms is 0 whenever the filter batch IS the entry read, which is the
    # production path: no second entry RPC is issued to make a stage look busy.
    # Run #9 §2.4: the real price 5 s after the entry, taken from the tick
    # stream while the position is open or from the post-exit ghost after it
    # closed. None when no mark is within POST_EXIT_NEAREST_TOLERANCE_SEC of
    # the 5-second mark — an absent value is honest, an interpolated one is not.
    # Mid price at the entry snapshot ((v_sol / v_token) of the curve read the
    # fill was priced from): the ghost compares like with like, without the
    # entry fee sitting inside the reference.
    entry_mid_price_usd: Optional[float] = None
    price_at_5s: Optional[float] = None
    pnl_pct_at_5s: Optional[float] = None
    price_at_5s_source: Optional[str] = None
    # Run #9 §3: how many times this ticker had already been launched in the
    # 120 s before entry (0 = first launch of that symbol we saw).
    name_copies_120s: Optional[int] = None
    # Run #9 §2.3: the post-exit ghost marks for this position.
    post_exit_marks: list[dict[str, Any]] = field(default_factory=list)
    t_detect_ms: Optional[float] = None
    t_filters_ms: Optional[float] = None
    t_read_ms: Optional[float] = None
    t_fill_ms: Optional[float] = None
    t_total_ms: Optional[float] = None
    entry_latency_ms: float = 0.0
    entry_latency_floor_met: Optional[bool] = None
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    exit_trigger_pnl_pct: Optional[float] = None
    # When the exit rule fired (epoch). The tick stream reports the latency
    # between this moment and the booked fill.
    exit_trigger_at_epoch: Optional[float] = None
    # P0-1 (run #7): the previous mark of THIS position and when it was taken,
    # so the tick stream can show how far the mark jumped over the threshold
    # and how long the gap between two marks actually was.
    last_tick_pnl_pct: Optional[float] = None
    last_tick_at_epoch: Optional[float] = None

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
    # Run #9 §3: why a candidate did not become a trade, counted per book, so
    # the funnel survives a dropped telemetry row.
    birth_band_count: int = 0
    delta_min_count: int = 0
    delta_max_count: int = 0
    name_copies_count: int = 0
    no_price_count: int = 0
    # --- run #3 addendum: bank accounting (never touches decisions) ---
    top_ups: int = 0
    capital_injected_rub: float = 0.0
    top_up_events: list[dict[str, Any]] = field(default_factory=list)
    # First moment the REAL 1500 RUB bank could no longer fund a slot, i.e.
    # initial_balance_rub + realized PnL < position_size_rub.
    bust_at_trade: Optional[int] = None
    bust_at_utc: Optional[str] = None
    bust_equity_rub: Optional[float] = None
    positions: dict[str, SimPosition] = field(default_factory=dict)
    closed_mints: set[str] = field(default_factory=set)
    closed_trades: list[dict[str, Any]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # --- run #3 addendum: bank accounting --------------------------------
    def equity_no_topups(self) -> float:
        """What the untouched 1500 RUB bank would hold in cash right now.

        Realized PnL only: open positions are not marked here, because the
        question is "could the bank still fund the next 500 RUB slot".
        """
        return float(self.profile.initial_balance_rub) + float(self.realized_pnl_rub)

    def note_bust(self, at_epoch: Optional[float] = None) -> bool:
        """Record the first moment the real bank could not fund a slot."""
        if self.bust_at_trade is not None:
            return False
        equity = self.equity_no_topups()
        if equity >= self.profile.position_size_rub:
            return False
        self.bust_at_trade = int(self.closed_trade_count)
        self.bust_at_utc = _utc_from_epoch(
            time.time() if at_epoch is None else float(at_epoch)
        )
        self.bust_equity_rub = round(equity, 6)
        return True

    def maybe_top_up(self, at_epoch: Optional[float] = None) -> Optional[dict[str, Any]]:
        """Top the book back up to 1500 RUB when cash < one slot.

        Returns the event dict (also appended to `top_up_events`) or None when
        no top-up is needed. Only the cash floor changes; a trade that fits the
        unchanged 500 RUB slot size is never resized.
        """
        if not self.profile.auto_top_up:
            return None
        if self.cash_balance_rub >= self.profile.position_size_rub:
            return None
        cash_before = float(self.cash_balance_rub)
        target = float(self.profile.initial_balance_rub)
        amount = target - cash_before
        if amount <= 0:
            return None
        self.cash_balance_rub = target
        self.top_ups += 1
        self.capital_injected_rub += amount
        event = {
            'profile': self.profile.name,
            'amount_rub': round(amount, 6),
            'cash_before': round(cash_before, 6),
            'cash_after': round(self.cash_balance_rub, 6),
            'cumulative_injected_rub': round(self.capital_injected_rub, 6),
            # Transparency: cash can be below one slot simply because capital
            # is deployed, not because the bank is dying.
            'equity_no_topups_at_event': round(self.equity_no_topups(), 6),
            'at_utc': _utc_from_epoch(
                time.time() if at_epoch is None else float(at_epoch)
            ),
            'at_trade_no': int(self.closed_trade_count),
        }
        self.top_up_events.append(event)
        return event

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
            'top_ups': self.top_ups,
            'capital_injected_rub': round(self.capital_injected_rub, 8),
            'pnl_with_topups': round(self.realized_pnl_rub, 8),
            'equity_no_topups': round(self.equity_no_topups(), 8),
            'bust_at_trade': self.bust_at_trade,
            'bust_at_utc': self.bust_at_utc,
            'bust_equity_rub': self.bust_equity_rub,
            'top_up_events': self.top_up_events,
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


# --- run #3 addendum: early-stop policy thresholds (checked every cycle) ---
# Run #4: the stop-list judges UNRECOVERED errors only. Run #3 died after 49
# minutes on 16/1408 = 1.14 % "failures" that were plain Helius 429 bursts;
# a 429 that a retry recovers is back-pressure, not a telemetry failure.
STOP_RPC_ERROR_RATE = 0.02          # > 2 % unrecovered requests over the window
STOP_RPC_WINDOW_SEC = 600.0         # ... last 10 minutes
STOP_RPC_MIN_REQUESTS = 20          # never judge a rate on 3 calls
STOP_RPC_LATENCY_MS = 1_000.0       # p95 above this ...
STOP_RPC_LATENCY_HOLD_SEC = 300.0   # ... for five minutes straight
STOP_RPC_LATENCY_WINDOW_SEC = 330.0
STOP_STALL_SEC = 1_800.0            # 30 minutes without a single entry
STOP_DISK_FREE_BYTES = 2 * 1024 ** 3
STOP_HELIUS_QUOTA_PCT = 10.0

# --- run #8 §1.4: mark polling cadence --------------------------------------
# The poll loop used to wait a hardcoded 0.40 s, so a 5 s fee-cover exit was
# sampled ~12 times and the last 0.4 s of the hold were invisible. 0.15 s is
# ~1.7 rps with three open positions in one batch; the floor is 0.05 s because
# anything faster is just a slower way to write http_429.
MARK_POLL_SEC_DEFAULT = 0.15
MARK_POLL_SEC_MIN = 0.05
# Run #7 acceptance is "no 10-minute window above 2.2 rps". Run #8 does not
# stop on it (the run must survive to the end); the cadence adapts instead.
# --- run #9 §2.3: the post-exit ghost -----------------------------------
# A closed coin stays in the mark batch for this long. It adds addresses to a
# getMultipleAccounts call that open positions already trigger, never a new
# request: the ghost costs zero RPC.
POST_EXIT_WINDOW_SEC = 10.0
# The ghost is not a second opinion on the exit — it records the nearest mark
# to the 5-second point so `price_at_5s` is a real observation. Beyond this
# distance the value stays None instead of being interpolated.
POST_EXIT_NEAREST_TOLERANCE_SEC = 0.75
# The points the run has to be able to answer for every exit.
POST_EXIT_REQUIRED_SEC: tuple[float, ...] = (0.5, 1.0, 2.0, 5.0, 10.0)
# Run #10 §2a: the ghost is read on a schedule, not on every mark cadence.
#
# Run #9 fed the ghost from every poll cycle (0.25 s, ~40 marks per exit) and
# 1 839 of 2 735 mark batches in the 30-minute smoke existed only for ghosts.
# That is what pushed the run to 2.23 rps mean and 2.83 rps in the worst
# 10-minute window. The ghost needs eight points, not forty: this schedule
# covers every required point with an exact hit (0.5/1/2/5/10) plus three
# extra points at 1.5/3/7 s. A ghost is added to a batch only when its next
# scheduled second has arrived, so a coin that no book holds costs nothing
# between those moments.
POST_EXIT_SCHEDULE_SEC: tuple[float, ...] = (0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 7.0, 10.0)
# The last scheduled point sits exactly on the window edge; the ghost has to
# outlive it by at least one poll cycle, or the 10th mark is never read.
POST_EXIT_SCHEDULE_GRACE_SEC = 0.5
MARK_POLL_RPS_LIMIT = 2.2
MARK_POLL_ADAPTED_SEC = 0.25
MARK_POLL_RPS_CHECK_SEC = 10.0
# Run #8 acceptance: 0 open positions when the run ends. A position opened in
# the last seconds before the deadline still has to leave by its own rule (the
# 5 s clock, a fee cover or a stop), so shutdown waits for the live poll loop
# for at most one time exit plus a few polls. It never books a synthetic fill.
DRAIN_OPEN_POSITIONS_MAX_SEC = 6.0


def mark_poll_rps_limit_from_env() -> float:
    """10-minute rps ceiling at which the cadence is slowed (default 2.2)."""
    raw = (os.getenv('MARK_POLL_RPS_LIMIT') or '').strip()
    if not raw:
        return MARK_POLL_RPS_LIMIT
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f'MARK_POLL_RPS_LIMIT must be a number, got {raw!r}') from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'MARK_POLL_RPS_LIMIT must be > 0, got {raw!r}')
    return value


def mark_poll_sec_from_env() -> float:
    raw = (os.getenv('MARK_POLL_SEC') or '').strip()
    if not raw:
        return MARK_POLL_SEC_DEFAULT
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f'MARK_POLL_SEC must be a number, got {raw!r}') from exc
    if not math.isfinite(value):
        raise ValueError(f'MARK_POLL_SEC must be finite, got {raw!r}')
    return max(MARK_POLL_SEC_MIN, value)


def _utc_from_epoch(value: float) -> str:
    """ISO-8601 Z timestamp from an epoch, without pulling in telemetry internals."""
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat(
        timespec='milliseconds',
    ).replace('+00:00', 'Z')


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


def median(values: list[float]) -> Optional[float]:
    """Plain median; None for an empty sample (never a fake 0)."""
    data = sorted(float(v) for v in values)
    if not data:
        return None
    middle = len(data) // 2
    if len(data) % 2:
        return data[middle]
    return (data[middle - 1] + data[middle]) / 2.0


def percentile(values: list[float], p: float) -> Optional[float]:
    data = sorted(float(v) for v in values)
    if not data:
        return None
    index = max(0, min(len(data) - 1, math.ceil(p * len(data)) - 1))
    return data[index]


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


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {'1', 'true', 'yes', 'on'}


def minute_pairs(
    reference_trades: list[dict[str, Any]], arm_trades: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run #9 §5(4): the two books compared minute by minute, not per coin.

    Both books read the same candidates at the same moment, so a minute is the
    natural block: the sum each book earned inside that minute. Minutes where
    only one book traded are counted separately and never enter the paired
    mean — an empty minute is not a zero.
    """
    def by_minute(trades: list[dict[str, Any]]) -> dict[int, float]:
        buckets: dict[int, float] = {}
        for trade in trades:
            at = trade.get('opened_at_epoch')
            if at is None:
                continue
            minute = int(float(at) // 60)
            buckets[minute] = buckets.get(minute, 0.0) + float(trade.get('pnl_rub') or 0.0)
        return buckets

    ref = by_minute(reference_trades)
    arm = by_minute(arm_trades)
    shared = sorted(set(ref) & set(arm))
    diffs = [arm[m] - ref[m] for m in shared]
    if not shared:
        return {
            'minutes_with_trades': {'reference': len(ref), 'arm': len(arm)},
            'minutes_paired': 0, 'share_arm_better': None,
            'mean_diff_rub': None, 'mean_reference_rub': None,
            'mean_arm_rub': None, 'n_better': 0, 'n_worse': 0, 'n_tie': 0,
        }
    better = sum(1 for d in diffs if d > 1e-9)
    worse = sum(1 for d in diffs if d < -1e-9)
    return {
        'unit': 'minute of the trade opening; sum of PnL inside the minute',
        'minutes_with_trades': {'reference': len(ref), 'arm': len(arm)},
        'minutes_paired': len(shared),
        'share_arm_better': round(better / len(shared), 4),
        'n_better': better,
        'n_worse': worse,
        'n_tie': len(shared) - better - worse,
        'mean_diff_rub': round(sum(diffs) / len(diffs), 4),
        'mean_reference_rub': round(sum(ref[m] for m in shared) / len(shared), 4),
        'mean_arm_rub': round(sum(arm[m] for m in shared) / len(shared), 4),
    }


def selection_decomposition(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Run #9 §5(5): the effect of each edit, inside ONE book's trades.

    `dev_buy_sol` is `v_sol_birth - 29.96`. The three cells are exactly the
    ones the decision rule names: dev 2-4.5 SOL with no copies, dev 2-4.5 SOL
    with copies, and dev below 2 SOL.
    """
    cells: dict[str, list[dict[str, Any]]] = {
        'dev_2_4.5_copies_0': [], 'dev_2_4.5_copies_pos': [],
        'dev_lt_2': [], 'unknown': [],
    }
    for trade in trades:
        dev = trade.get('dev_buy_sol')
        copies = trade.get('name_copies_120s')
        if dev is None:
            cells['unknown'].append(trade)
        elif float(dev) < 2.0:
            cells['dev_lt_2'].append(trade)
        elif copies:
            cells['dev_2_4.5_copies_pos'].append(trade)
        else:
            cells['dev_2_4.5_copies_0'].append(trade)
    return {
        'rule': (
            'dev_buy_sol = v_sol_birth - 29.96; the three cells of the '
            'pre-declared decomposition of §5(5)'
        ),
        'cells': {
            name: {
                **trade_stats([float(t.get('pnl_rub') or 0.0) for t in rows]),
                'mean_pct': round(
                    sum(float(t.get('pnl_pct') or 0.0) for t in rows) / len(rows), 4,
                ) if rows else None,
                'fee_cover_n': sum(
                    1 for t in rows if 'Fee cover' in str(t.get('reason') or '')
                ),
                'time_exit_n': sum(
                    1 for t in rows if 'Time exit' in str(t.get('reason') or '')
                ),
                'hard_stop_n': sum(
                    1 for t in rows if 'Hard stop' in str(t.get('reason') or '')
                ),
            }
            for name, rows in cells.items()
        },
    }


def paired_stats(
    a_trades: list[dict[str, Any]], arm_trades: list[dict[str, Any]],
    *, iterations: int = BOOTSTRAP_ITERATIONS,
) -> dict[str, Any]:
    """Paired comparison of one arm against A on the coins both arms traded.

    The arms see the same candidate stream, so the correct test is the mean of
    the per-coin differences, not the difference of two independent means
    (run #6 lost most of its power that way).
    """
    a_by_mint = {str(t.get('mint')): float(t.get('pnl_rub') or 0.0) for t in a_trades}
    pairs: list[float] = []
    wins = losses = ties = 0
    for trade in arm_trades:
        mint = str(trade.get('mint'))
        if mint not in a_by_mint:
            continue
        diff = float(trade.get('pnl_rub') or 0.0) - a_by_mint[mint]
        pairs.append(diff)
        if diff > 1e-9:
            wins += 1
        elif diff < -1e-9:
            losses += 1
        else:
            ties += 1
    n = len(pairs)
    if n == 0:
        return {
            'n_pairs': 0, 'mean_diff': None, 'sd_diff': None, 'ci95_diff': None,
            'wins': 0, 'losses': 0, 'ties': 0,
        }
    mean = sum(pairs) / n
    sd = math.sqrt(sum((v - mean) ** 2 for v in pairs) / (n - 1)) if n > 1 else 0.0
    ci = None
    if n > 1:
        rng = random.Random(BOOTSTRAP_SEED + 2)
        means = []
        for _ in range(iterations):
            sample = rng.choices(pairs, k=n)
            means.append(sum(sample) / n)
        means.sort()
        ci = [round(means[int(0.00625 * iterations)], 4),
              round(means[min(iterations - 1, int(0.99375 * iterations))], 4)]
    return {
        'n_pairs': n,
        'mean_diff': round(mean, 4),
        'sd_diff': round(sd, 4),
        # 98.75 % interval: three hypotheses, Bonferroni alpha 0.0125.
        'ci95_diff': ci,
        'ci_level': 0.9875,
        'wins': wins, 'losses': losses, 'ties': ties,
    }


def hourly_blocks(
    trades: list[dict[str, Any]], started_at_epoch: Optional[float],
) -> dict[str, Any]:
    """n / sum / mean per completed hour — the noise floor of the rules."""
    blocks: dict[str, dict[str, Any]] = {}
    for trade in trades:
        at = trade.get('closed_at_epoch') or trade.get('opened_at_epoch')
        if not isinstance(at, (int, float)) or not started_at_epoch:
            continue
        hour = int(max(0.0, float(at) - float(started_at_epoch)) // 3600.0)
        slot = blocks.setdefault(f'h{hour + 1}', {'n': 0, 'sum': 0.0})
        slot['n'] += 1
        slot['sum'] += float(trade.get('pnl_rub') or 0.0)
    return {
        key: {
            'n': slot['n'],
            'sum': round(slot['sum'], 2),
            'mean': round(slot['sum'] / slot['n'], 2) if slot['n'] else None,
        }
        for key, slot in sorted(blocks.items())
    }


def stability_split(
    a_trades: list[dict[str, Any]], arm_trades: list[dict[str, Any]],
    started_at_epoch: Optional[float], duration_sec: float,
) -> dict[str, Any]:
    """Paired mean difference in the first and the second half of the run.

    Run #6's -2.27 vs +1.49 gap is smaller than the between-hour noise, so a
    sign that flips between halves is a result in itself.
    """
    if not started_at_epoch:
        return {'first_half': None, 'second_half': None, 'sign_stable': None}
    middle = float(started_at_epoch) + max(1.0, float(duration_sec)) / 2.0

    def half(trades: list[dict[str, Any]], first: bool) -> list[dict[str, Any]]:
        out = []
        for trade in trades:
            at = trade.get('closed_at_epoch') or trade.get('opened_at_epoch')
            if not isinstance(at, (int, float)):
                continue
            if (float(at) < middle) == first:
                out.append(trade)
        return out

    first = paired_stats(half(a_trades, True), half(arm_trades, True))
    second = paired_stats(half(a_trades, False), half(arm_trades, False))
    means = [first.get('mean_diff'), second.get('mean_diff')]
    stable = (
        None if any(m is None for m in means)
        else bool((means[0] > 0) == (means[1] > 0))
    )
    return {'split_at_epoch': round(middle, 3), 'first_half': first,
            'second_half': second, 'sign_stable': stable}


ROCKET_PEAK_PCT = 50.0
ROCKET_WINDOW_SEC = 60.0


def rocket_accounting(
    trades: list[dict[str, Any]],
    *,
    peaks: Optional[dict[str, float]] = None,
    rocket_peak_pct: float = ROCKET_PEAK_PCT,
    window_sec: float = ROCKET_WINDOW_SEC,
) -> dict[str, Any]:
    """How much of the result rides on coins that peaked >= +50 % (run #6).

    The peak MUST come from a fixed observation window (60 s), not from the
    trade: a trade lives ~5 s, so its in-trade peak measures a different thing
    per arm and is not comparable. Run #6 on the 60 s window: 13 of 167 coins
    were rockets (7.8 %) with p* = 6.89 %, while the in-trade peak would have
    counted 3 (1.8 %) and reported a p* of 1.34 % — five times too optimistic.

    ``peaks`` maps mint -> peak % inside the window (telemetry observations).
    A trade whose mint has no observation falls back to its own in-trade peak
    and is counted in ``fallback_trades`` so the reader can see the mix.

    `break_even_rocket_share` is the fraction of trades that must be rockets
    for the strategy to break even: p* = -mean(others) / (mean(rockets) -
    mean(others)). Run #6: p* = 6.9 % against an actual 7.8 %.
    """
    resolved: list[tuple[float, Optional[float]]] = []
    fallback = 0
    for trade in trades:
        mint = str(trade.get('mint') or '')
        peak = (peaks or {}).get(mint)
        if peak is None:
            raw = trade.get('peak_pct')
            peak = float(raw) if isinstance(raw, (int, float)) else None
            fallback += 1
        resolved.append((float(trade.get('pnl_rub') or 0.0), peak))

    def is_rocket(peak: Optional[float]) -> bool:
        return peak is not None and peak >= rocket_peak_pct

    rockets = [pnl for pnl, peak in resolved if is_rocket(peak)]
    others = [pnl for pnl, peak in resolved if not is_rocket(peak)]
    n = len(rockets) + len(others)
    mean_rockets = sum(rockets) / len(rockets) if rockets else None
    mean_others = sum(others) / len(others) if others else None
    break_even = None
    if (
        mean_rockets is not None and mean_others is not None
        and mean_rockets > mean_others
    ):
        break_even = round(-mean_others / (mean_rockets - mean_others), 4)
    return {
        'rocket_pct_threshold': rocket_peak_pct,
        'window_sec': window_sec,
        'peak_source': 'observations' if peaks else 'in_trade',
        'fallback_trades': fallback,
        'n': n,
        'rockets': len(rockets),
        'rocket_share': round(len(rockets) / n, 4) if n else None,
        'rockets_sum_rub': round(sum(rockets), 2),
        'rockets_mean_rub': round(mean_rockets, 2) if mean_rockets is not None else None,
        'others_sum_rub': round(sum(others), 2),
        'others_mean_rub': round(mean_others, 2) if mean_others is not None else None,
        'break_even_rocket_share': break_even,
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
        # Run #7: passive token intelligence (plain HTTPS, zero RPC). It is
        # collected for a later run and read by no entry/exit rule.
        self.enrich = EnrichCollector(
            session, enabled=_env_flag('ENTRY_ENRICH', default=False),
        )
        # Per-position tick stream (own marks, not observations.csv).
        self.ticks: list[dict[str, Any]] = []
        # --- run #3 addendum: early-stop bookkeeping (no new RPC) ---
        self._entry_state_total = 0
        # Run #4: entry-latency accounting (acceptance #4).
        # Run #7: no 'floor violations' counter. `entry_lag_ms` already says how
        # old the curve read was; in run #6 the counter fired on 143/167 trades
        # while the median read age was 101 ms, i.e. it measured the model, not
        # a failure.
        self.last_entry_latency_wait_sec = 0.0
        self.entry_lag_ms_by_profile: dict[str, list[float]] = {}
        # --- run #9 §2.2: name-copy registry -------------------------------
        # symbol (lower-case) -> epochs of the launches we saw, pruned to the
        # widest window any book asks about. In memory only: no RPC, no file.
        self._symbol_launches: dict[str, list[float]] = {}
        self.symbols_seen = 0
        self.symbols_with_copies = 0
        # --- run #9 §2.3: post-exit ghosts --------------------------------
        self.post_exit_rows: list[dict[str, Any]] = []
        self._ghosts: list[dict[str, Any]] = []
        self.post_exit_started: dict[str, int] = {}
        self.post_exit_skipped: dict[str, int] = {}
        self._book_last_entry_epoch: dict[str, float] = {
            name: self.started_at_epoch for name in self.books
        }
        self._entry_state_at_last_entry: dict[str, int] = {
            name: 0 for name in self.books
        }
        self._poll_cycle = 0
        self.rpc_reads_by_profile: dict[str, int] = {}
        # (monotonic, requests, failed attempts, p95, unrecovered reads)
        self._rpc_samples: list[tuple[float, int, int, Optional[float], int]] = []
        self._rpc_latency_high_since: Optional[float] = None
        self.stop_policy_state: dict[str, Any] = {
            'reason': None, 'telemetry': {}, 'rpc': {}, 'stall': {}, 'resources': {},
        }
        # --- run #8 §1.4: mark poll cadence (env MARK_POLL_SEC, default 0.15) ---
        self.mark_poll_sec = mark_poll_sec_from_env()
        self.mark_poll_rps_limit = mark_poll_rps_limit_from_env()
        self.mark_poll_adaptation_blocked = False
        self.mark_poll_adapted = False
        self.mark_poll_rps_at_check: Optional[float] = None
        self._last_rps_check_epoch = 0.0
        self.drain_left_open: Optional[int] = None
        self.drain_waited_sec = 0.0
        self.ghost_drain_left_open: Optional[int] = None
        self.ghost_drain_waited_sec = 0.0

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
        await self.enrich.start()
        log(
            '🧪 Isolated paper portfolios active: ' + ', '.join(
                f'{book.profile.arm_id}={book.profile.max_slots}×'
                f'{book.profile.position_size_rub:.0f} ₽'
                for book in self.books.values()
            ) + '; no Main/Watchlist; read-only RPC only.',
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

    def _entry_latency_floor_sec(self) -> float:
        """The longest modelled execution delay across the books.

        All live arms and `shadow_t0_5s` use the same 150 ms, so the gate costs
        nothing; taking the max keeps the guard correct if one arm is ever
        given a different value."""
        values = [
            float(book.profile.entry_latency_ms) for book in self.books.values()
        ]
        return max(values) / 1000.0 if values else 0.0

    async def _await_entry_latency_floor(self, coin: NewCoin) -> float:
        """Hold the decision until `found_at + entry_latency_ms`.

        Run #4 prices fills from the on-chain curve read, so the earliest
        moment a position can exist is detection + execution delay. Waiting
        here costs no RPC and no state; it only stops the simulator from
        booking an entry that a real bot could not have sent. Returns the
        seconds actually spent waiting (0.0 when the filter stage was already
        slower than the floor, which is the normal case).
        """
        floor = self._entry_latency_floor_sec()
        if floor <= 0:
            return 0.0
        wait = (coin.found_at + floor) - time.time()
        if wait <= 0:
            return 0.0
        # A candidate older than a second is a different problem; never block
        # the loop for long, the flag on the trade records the violation.
        wait = min(wait, 1.0)
        await asyncio.sleep(wait)
        return wait

    def _name_copies_window_sec(self) -> float:
        windows = [
            float(book.profile.name_copies_window_sec)
            for book in self.books.values()
            if book.profile.max_name_copies_120s is not None
        ]
        return max(windows) if windows else 0.0

    def count_name_copies(self, coin: NewCoin, at_epoch: Optional[float] = None) -> int:
        """How many times this ticker was launched before, inside the window.

        Pure arithmetic on a dict we already hold: no RPC. The current launch
        does not count itself, and an empty symbol is never a copy of
        anything (there is nothing to compare).
        """
        symbol = str((coin.packet or {}).get('symbol') or '').strip().lower()
        if not symbol:
            return 0
        window = self._name_copies_window_sec()
        if window <= 0:
            return 0
        at = float(coin.found_at if at_epoch is None else at_epoch)
        launches = [
            epoch for epoch in self._symbol_launches.get(symbol, [])
            if at - window <= epoch < at
        ]
        return len(launches)

    def remember_symbol_launch(self, coin: NewCoin) -> None:
        """Register the launch, whatever the books decide about it afterwards.

        A coin rejected by the security filters is still a launch of that
        ticker, so a later coin with the same symbol must see it.
        """
        symbol = str((coin.packet or {}).get('symbol') or '').strip().lower()
        if not symbol:
            return
        at = float(coin.found_at)
        launches = [epoch for epoch in self._symbol_launches.get(symbol, []) if epoch <= at]
        launches.append(at)
        window = max(self._name_copies_window_sec(), 0.0)
        if window > 0:
            launches = [epoch for epoch in launches if epoch >= at - window]
        self._symbol_launches[symbol] = launches
        self.symbols_seen += 1
        if len(launches) > 1:
            self.symbols_with_copies += 1

    async def handle_candidate(self, coin: NewCoin) -> None:
        if coin.source != 'Pump.fun':
            collector = self._collector()
            if collector is not None:
                collector.record_decision(coin.mint, coin.source, 'ab_scope', 'skipped', ['source_not_pumpfun'])
            return
        # Run #8 §1.3: the SOL/USD rate used to be awaited AFTER the security
        # verdict, so on a cache miss an HTTPS round trip sat between the
        # verdict and the fill. Start it together with the security batch: it
        # is DexScreener, not Helius, so it costs neither RPC budget nor a
        # filter-semaphore slot.
        sol_usd_task = asyncio.create_task(get_sol_usd(self.session))
        # Run #9 §2.2: count the copies BEFORE registering this launch, then
        # register it — every book sees the same number, computed once.
        name_copies = self.count_name_copies(coin)
        self.remember_symbol_launch(coin)
        try:
            await self._handle_candidate_checks(coin, sol_usd_task, name_copies)
        finally:
            # Rejections return early all over the place; never leave the
            # prefetched rate running behind them.
            if not sol_usd_task.done():
                sol_usd_task.cancel()
            elif not sol_usd_task.cancelled():
                # Consume the result even when it was not needed, so a failed
                # prefetch can never surface as an unretrieved exception.
                sol_usd_task.exception()

    async def _handle_candidate_checks(
        self, coin: NewCoin, sol_usd_task: 'asyncio.Future',
        name_copies: int = 0,
    ) -> None:
        """Everything after the source check, with the rate prefetch in flight."""
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
        filter_done_epoch = time.time()
        # Stage 1 (WSS packet -> candidate row) is stamped by the collector when
        # it writes the row; the handler start is the fallback without telemetry.
        detect_epoch = float(
            coin.candidate_row_epoch
            or (coin.found_at + first_seen_delay_ms / 1000.0)
        )
        filter_metrics = {
            'queue_delay_ms': round(first_seen_delay_ms, 2),
            'filter_semaphore_wait_ms': round(max(0.0, (filter_started - handler_started) * 1000.0), 2),
            'filter_execution_ms': round(max(0.0, (filter_done - filter_started) * 1000.0), 2),
            'first_seen_to_filter_done_ms': round(max(0.0, (filter_done_epoch - coin.found_at) * 1000.0), 2),
            # --- run #8 §1.3: the chain, stages 1-2 ---
            't_detect_ms': round(max(0.0, (detect_epoch - coin.found_at) * 1000.0), 2),
            't_filters_ms': round(max(0.0, (filter_done_epoch - detect_epoch) * 1000.0), 2),
        }
        collector = self._collector()
        if collector is not None:
            # P0-2: a rejected coin is only observed at offsets 0 and 15, so the
            # "did it become a rocket" question stays answerable at 1/3 of the
            # reads. Telemetry-only; no decision reads this flag.
            collector.set_security_verdict(coin.mint, verdict.passed)
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
        # Run #7: one passive HTTPS lookup per coin that survived the filters.
        # It never blocks, never retries and no rule reads its output.
        try:
            self.enrich.enqueue(coin.mint)
        except Exception:
            pass
        if not coin.curve:
            self._record_candidate_fail(coin, 'missing_curve_after_security_filters')
            return

        # Run #4: no fill may be booked before detection + entry_latency_ms.
        # This is a sleep, not a read: the RPC budget is untouched.
        latency_wait_sec = await self._await_entry_latency_floor(coin)
        self.last_entry_latency_wait_sec = latency_wait_sec

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
        # Stage 3: how old the read this fill is priced from is, measured from
        # the verdict. It is 0 on the production path because the filter batch
        # and the entry read are the same getMultipleAccounts context.
        read_epoch = float(snapshot.observed_at_epoch or filter_done_epoch)
        filter_metrics = {
            **filter_metrics,
            't_read_ms': round(max(0.0, (read_epoch - filter_done_epoch) * 1000.0), 2),
        }
        try:
            sol_usd = await sol_usd_task
        except asyncio.CancelledError:
            raise
        except Exception:
            sol_usd = None
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
        # Every arm evaluates the same candidate on the same shared snapshot,
        # concurrently: no arm issues its own curve read and no arm waits for
        # another, so the arms stay comparable.
        await asyncio.gather(*(
            self._consider_entry(
                book, coin, snapshot, birth_v_sol, current_v_sol,
                sol_usd, filter_metrics, decision_epoch,
                name_copies=name_copies,
            )
            for book in self.books.values()
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
        # Counted even without a collector: the stall check compares candidates
        # seen against entries opened, and it must not depend on telemetry.
        self._entry_state_total += 1
        if collector is None:
            return
        # delta_cap_used_sol must be A's cap: that is the band every other arm
        # is compared against, not whatever book happens to come first.
        a_book = self.books.get('portfolio_a')
        base_profile = a_book.profile if a_book is not None else next(
            book.profile for book in self.books.values()
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
             'entry_latency_ms': base_profile.entry_latency_ms,
             'entry_latency_wait_ms': round(
                 float(getattr(self, 'last_entry_latency_wait_sec', 0.0)) * 1000.0, 2,
             ),
             'first_read_age_ms': round(
                 max(0.0, float(snapshot.observed_at_epoch or decision_epoch)
                     - coin.found_at) * 1000.0, 2,
             ),
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
        elif reason in {'sol_usd_unavailable', 'invalid_sol_rub_conversion'}:
            book.no_price_count += 1
        collector = self._collector()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, f'{book.profile.name}_entry', 'skipped', [reason],
                {'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
                 'cash_balance_rub': round(book.cash_balance_rub, 4),
                 'open_positions': len(book.positions), 'max_slots': book.profile.max_slots},
            )

    def _record_top_up(
        self, book: PortfolioBook, coin: NewCoin, event: dict[str, Any],
    ) -> None:
        """One row per injection: the bank metric must be auditable per trade."""
        collector = self._collector()
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, f'{book.profile.name}_top_up', 'applied',
                ['cash_below_slot_size'],
                {**event, 'paper_only': True,
                 'capital_is_virtual': book.profile.capital_rub == 0.0},
            )
        log(
            f'🏦 [{book.profile.name}] TOP-UP +{event["amount_rub"]:.2f} ₽ '
            f'(cash {event["cash_before"]:.2f} -> {event["cash_after"]:.2f}; '
            f'injected total {event["cumulative_injected_rub"]:.2f} ₽)',
            Color.CYAN,
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
        name_copies: int = 0,
    ) -> None:
        decision_epoch = float(decision_epoch or time.time())
        profile = book.profile
        collector = self._collector()
        symbol = str((coin.packet or {}).get('symbol') or '').strip()
        # Run #9 §2.2: the name-copy rule runs BEFORE the band, because it is
        # about the coin, not about the price. An empty ticker is never
        # filtered: there is nothing to compare it with.
        if (
            symbol and profile.max_name_copies_120s is not None
            and name_copies > int(profile.max_name_copies_120s)
        ):
            book.name_copies_count += 1
            if collector is not None:
                collector.record_decision(
                    coin.mint, coin.source, f'{profile.name}_entry', 'skipped',
                    ['name_copies_120s'],
                    {'symbol': symbol, 'name_copies_120s': name_copies,
                     'max_name_copies_120s': int(profile.max_name_copies_120s),
                     'window_sec': profile.name_copies_window_sec,
                     'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
                     'paper_only': True},
                )
            return
        qualifies, entry_rule = profile.entry_check(birth_v_sol, current_v_sol)
        if collector is not None:
            collector.record_decision(
                coin.mint, coin.source, f'{profile.name}_signal',
                'pass' if qualifies else 'skip', [entry_rule],
                {'v_sol_birth': birth_v_sol, 'v_sol_now': current_v_sol,
                 'entry_profile': profile.name,
                 'symbol': symbol, 'name_copies_120s': name_copies,
                 'max_name_copies_120s': profile.max_name_copies_120s,
                 'as_of_epoch': decision_epoch, 'paper_only': True},
            )
        if not qualifies:
            if entry_rule == 'birth_out_of_band':
                book.birth_band_count += 1
            elif entry_rule in {'no_live_momentum', 'entry_below_delta_min'}:
                book.delta_min_count += 1
            elif entry_rule == 'momentum_too_late':
                book.delta_max_count += 1
            return
        async with book.lock:
            if coin.mint in book.positions or coin.mint in book.closed_mints:
                self._record_portfolio_skip(book, coin, 'mint_already_seen_in_this_portfolio', birth_v_sol, current_v_sol)
                return
            if len(book.positions) >= profile.max_slots:
                self._record_portfolio_skip(book, coin, 'three_slots_already_open_or_pending', birth_v_sol, current_v_sol)
                return
            if book.cash_balance_rub < profile.position_size_rub:
                # Run #3 addendum: the bank is topped back up instead of the
                # trade being skipped. Decisions above this line are untouched.
                book.note_bust(decision_epoch)
                top_up = book.maybe_top_up(decision_epoch)
                if top_up is None:
                    self._record_portfolio_skip(
                        book, coin, 'cash_below_slot_size', birth_v_sol, current_v_sol,
                    )
                    return
                self._record_top_up(book, coin, top_up)
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
            # --- run #4: two different premiums, never mixed -------------
            # (1) entry_premium_pct = the price paid for WAITING. Mid price at
            #     the entry snapshot against the price at the coin's creation.
            #     Along the bonding curve price = v_sol^2 / k, so the ratio is
            #     exactly (v_now / v_birth)^2: no RPC, no interpolation, and no
            #     dependence on the stale WSS packet price. This is the -2.12 %
            #     line of the run #3 decomposition.
            # (2) entry_fill_premium_pct = fees + price impact inside the same
            #     snapshot (fill vs mid). Kept separate so the fee line is
            #     never read as "the cost of waiting".
            try:
                v_tokens_ui = snapshot.curve.virtual_token_reserves / TOKEN_SCALE
                invariant = current_v_sol * v_tokens_ui
                mid_price_usd = (
                    current_v_sol / v_tokens_ui * sol_usd if v_tokens_ui > 0 else 0.0
                )
                t0_price_usd = (
                    birth_v_sol ** 2 / invariant * sol_usd if invariant > 0 else 0.0
                )
            except (AttributeError, TypeError, ZeroDivisionError):
                mid_price_usd = 0.0
                t0_price_usd = 0.0
            entry_premium_pct = (
                (mid_price_usd / t0_price_usd - 1.0) * 100.0
                if mid_price_usd > 0 and t0_price_usd > 0 else None
            )
            entry_fill_premium_pct = (
                (entry_price_usd / mid_price_usd - 1.0) * 100.0
                if mid_price_usd > 0 and entry_price_usd > 0 else None
            )
            # Measured, not modelled: the age of the on-chain read that was
            # actually priced, and the modelled execution delay next to it.
            read_epoch = float(snapshot.observed_at_epoch or decision_epoch)
            entry_lag_ms = max(0.0, (read_epoch - coin.found_at) * 1000.0)
            # Stages 4-5: the fill is booked right here, so this clock is the
            # fill row and the whole chain can be closed with two durations.
            fill_epoch = time.time()
            t_fill_ms = round(max(0.0, (fill_epoch - read_epoch) * 1000.0), 2)
            t_total_ms = round(max(0.0, (fill_epoch - coin.found_at) * 1000.0), 2)
            entry_latency_ms = float(profile.entry_latency_ms)
            latency_floor_met = entry_lag_ms >= entry_latency_ms
            pos = SimPosition(
                portfolio=profile.name, mint=coin.mint, source=coin.source,
                curve=coin.curve or '', opened_at_epoch=decision_epoch,
                opened_at_utc=datetime.fromtimestamp(decision_epoch, timezone.utc).isoformat(timespec='milliseconds'),
                birth_v_sol=birth_v_sol,
                entry_v_sol=current_v_sol, entry_sol_usd=sol_usd,
                entry_cost_rub=actual_cost_rub, entry_fee_rub=entry_fee_rub,
                remaining_entry_fee_rub=entry_fee_rub,
                entry_price_usd=entry_price_usd,
                entry_mid_price_usd=round(mid_price_usd, 16) if mid_price_usd else None,
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
                name_copies_120s=int(name_copies),
                entry_premium_pct=(
                    round(entry_premium_pct, 4) if entry_premium_pct is not None else None
                ),
                entry_fill_premium_pct=(
                    round(entry_fill_premium_pct, 4)
                    if entry_fill_premium_pct is not None else None
                ),
                t0_price_usd=round(t0_price_usd, 16) if t0_price_usd else None,
                entry_lag_ms=round(entry_lag_ms, 3),
                entry_latency_ms=entry_latency_ms,
                entry_latency_floor_met=latency_floor_met,
                t_detect_ms=filter_metrics.get('t_detect_ms'),
                t_filters_ms=filter_metrics.get('t_filters_ms'),
                t_read_ms=filter_metrics.get('t_read_ms'),
                t_fill_ms=t_fill_ms,
                t_total_ms=t_total_ms,
            )
            book.cash_balance_rub -= actual_cost_rub
            book.fees_paid_rub += entry_fee_rub
            book.onchain_fees_paid_rub += onchain_entry_fee_rub
            book.entry_count += 1
            book.positions[coin.mint] = pos
            self._book_last_entry_epoch[book.profile.name] = decision_epoch
            self.entry_lag_ms_by_profile.setdefault(book.profile.name, []).append(entry_lag_ms)
            self._entry_state_at_last_entry[book.profile.name] = self._entry_state_total
            # t=0 of this position's own tick stream.
            self._record_tick(book, pos, 0.0, decision_epoch, entry_price_usd, event='entry')
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
                # --- run #4 instrumentation (acceptance #1, #2, #4) ---
                'entry_premium_pct': (
                    round(entry_premium_pct, 4) if entry_premium_pct is not None else None
                ),
                'entry_fill_premium_pct': (
                    round(entry_fill_premium_pct, 4)
                    if entry_fill_premium_pct is not None else None
                ),
                'entry_mid_price_usd': round(mid_price_usd, 16) if mid_price_usd else None,
                't0_price_usd': round(t0_price_usd, 16) if t0_price_usd else None,
                'entry_price_usd': round(entry_price_usd, 16),
                'entry_lag_ms': round(entry_lag_ms, 3),
                'entry_latency_ms': entry_latency_ms,
                'entry_latency_floor_met': latency_floor_met,
                # Run #8 §1.3: the whole chain lands in decisions.csv too.
                't_detect_ms': filter_metrics.get('t_detect_ms'),
                't_filters_ms': filter_metrics.get('t_filters_ms'),
                't_read_ms': filter_metrics.get('t_read_ms'),
                't_fill_ms': t_fill_ms,
                't_total_ms': t_total_ms,
                'entry_price_source': 'first_onchain_curve_read_after_detection',
                'entry_at_detection': profile.entry_at_detection,
                'delta_min_sol': profile.delta_min_sol,
                'delta_at_entry_sol': round(current_v_sol - birth_v_sol, 6),
                'name_copies_120s': int(name_copies),
                'max_name_copies_120s': profile.max_name_copies_120s,
                'v_sol_birth_min': profile.v_sol_birth_min,
                'delta_cap_sol': round(profile.delta_cap(birth_v_sol), 6),
                'wss_packet_price_used': False,
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

    # --- run #3 addendum: stop-list --------------------------------------
    def _helius_quota_remaining_pct(self) -> Optional[float]:
        """Remaining Helius quota, when the operator publishes it.

        There is no read-only RPC that reports credits, so the value comes from
        `HELIUS_QUOTA_REMAINING_PCT` or `<RUN_DIR>/helius_quota.json`
        (`{"remaining_pct": 42.0}`). Without either, the check is reported as
        unavailable instead of silently passing.
        """
        raw = os.getenv('HELIUS_QUOTA_REMAINING_PCT', '').strip()
        if raw:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = None
            if value is not None and math.isfinite(value):
                return max(0.0, min(100.0, value))
        path = self.run_dir / 'helius_quota.json'
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
            value = float(payload.get('remaining_pct'))
        except (OSError, ValueError, TypeError, AttributeError):
            return None
        return max(0.0, min(100.0, value)) if math.isfinite(value) else None

    def _rpc_error_rate_reason(self, now: float) -> Optional[str]:
        """> 2 % UNRECOVERED RPC requests over the last 10 minutes."""
        self._rpc_samples = [
            sample for sample in self._rpc_samples
            if now - sample[0] <= STOP_RPC_WINDOW_SEC
        ]
        if len(self._rpc_samples) < 2:
            return None
        first, last = self._rpc_samples[0], self._rpc_samples[-1]
        delta_requests = last[1] - first[1]
        # index 2 = raw failed attempts, index 4 = unrecovered logical reads
        delta_failed = last[2] - first[2]
        delta_unrecovered = last[4] - first[4]
        if delta_requests < STOP_RPC_MIN_REQUESTS:
            return None
        rate = delta_unrecovered / delta_requests
        self.stop_policy_state['rpc'].update({
            'window_requests': delta_requests,
            'window_failed_attempts': delta_failed,
            'window_unrecovered': delta_unrecovered,
            'unrecovered_rate': round(rate, 6),
            'raw_error_rate': round(delta_failed / delta_requests, 6),
        })
        if rate > STOP_RPC_ERROR_RATE:
            return (
                f'rpc_unrecovered_rate (unrecovered={delta_unrecovered}/{delta_requests} '
                f'= {rate:.2%} over {int(STOP_RPC_WINDOW_SEC)}s > '
                f'{STOP_RPC_ERROR_RATE:.0%})'
            )
        return None

    def _rpc_latency_reason(
        self, now: float, p95: Optional[float],
    ) -> Optional[str]:
        """p95 > 1000 ms for five minutes straight."""
        if p95 is None or p95 <= STOP_RPC_LATENCY_MS:
            self._rpc_latency_high_since = None
            return None
        if self._rpc_latency_high_since is None:
            self._rpc_latency_high_since = now
        held = now - self._rpc_latency_high_since
        self.stop_policy_state['rpc'].update({
            'latency_p95_ms': round(float(p95), 2),
            'latency_high_for_sec': round(held, 1),
        })
        if held >= STOP_RPC_LATENCY_HOLD_SEC:
            return (
                f'rpc_latency_p95 (p95={float(p95):.0f}ms > '
                f'{STOP_RPC_LATENCY_MS:.0f}ms for {held:.0f}s >= '
                f'{STOP_RPC_LATENCY_HOLD_SEC:.0f}s)'
            )
        return None

    def _stall_reason(self, now: float) -> Optional[str]:
        """A live arm opened nothing for 30 min while candidates keep coming."""
        for name, book in self.books.items():
            last = self._book_last_entry_epoch.get(name, self.started_at_epoch)
            quiet = now - last
            new_states = self._entry_state_total - self._entry_state_at_last_entry.get(name, 0)
            self.stop_policy_state['stall'][name] = {
                'sec_without_entry': round(quiet, 1),
                'entry_state_since_last_entry': new_states,
            }
            if quiet >= STOP_STALL_SEC and new_states > 0:
                return (
                    f'portfolio_stalled ({name}: 0 entries for {quiet:.0f}s >= '
                    f'{STOP_STALL_SEC:.0f}s while entry_state grew by {new_states})'
                )
        return None

    def _resource_reason(
        self, disk_free_bytes: Optional[int], quota_pct: Optional[float],
    ) -> Optional[str]:
        self.stop_policy_state['resources'] = {
            'disk_free_bytes': disk_free_bytes,
            'disk_free_gb': (
                round(disk_free_bytes / 1024 ** 3, 3)
                if disk_free_bytes is not None else None
            ),
            'helius_quota_remaining_pct': quota_pct,
        }
        if disk_free_bytes is not None and disk_free_bytes < STOP_DISK_FREE_BYTES:
            return (
                f'disk_low (free={disk_free_bytes / 1024 ** 3:.2f}GB < '
                f'{STOP_DISK_FREE_BYTES / 1024 ** 3:.0f}GB)'
            )
        if quota_pct is not None and quota_pct < STOP_HELIUS_QUOTA_PCT:
            return (
                f'helius_quota_low (remaining={quota_pct:.1f}% < '
                f'{STOP_HELIUS_QUOTA_PCT:.0f}%)'
            )
        return None

    def _evaluate_stop_reason(
        self,
        *,
        now: Optional[float] = None,
        telemetry: Optional[dict[str, Any]] = None,
        rpc: Optional[dict[str, Any]] = None,
        disk_free_bytes: Optional[int] = None,
        quota_pct: Optional[float] = None,
    ) -> Optional[str]:
        """The stop-list, in the order the addendum declares it.

        Every input can be injected, so the policy is testable without a live
        session; nothing here issues an RPC.
        """
        now = time.time() if now is None else float(now)
        if self.early_stop_reason is not None:
            return self.early_stop_reason

        stats = telemetry
        if stats is None:
            collector = self._collector()
            stats = collector.stats() if collector is not None else {}
        dropped = int(stats.get('dropped_events') or 0)
        errors = int(stats.get('write_errors') or 0)
        self.stop_policy_state['telemetry'] = {
            'dropped_events': dropped, 'write_errors': errors,
        }

        reason: Optional[str] = None
        if dropped or errors:
            reason = (
                f'telemetry_loss (dropped_events={dropped}, '
                f'write_errors={errors})'
            )
        if reason is None:
            metrics = rpc if rpc is not None else rpc_metrics_snapshot()
            self._rpc_samples.append((
                now, int(metrics.get('requests') or 0),
                int(metrics.get('failed_requests') or 0),
                metrics.get('latency_ms_p95'),
                int(metrics.get('unrecovered_failures') or 0),
            ))
            if len(self._rpc_samples) > 4_000:
                self._rpc_samples = self._rpc_samples[-4_000:]
            reason = self._rpc_error_rate_reason(now)
            if reason is None:
                reason = self._rpc_latency_reason(now, metrics.get('latency_ms_p95'))
        if reason is None:
            reason = self._stall_reason(now)
        if reason is None:
            free = disk_free_bytes
            if free is None:
                try:
                    free = int(shutil.disk_usage(self.run_dir).free)
                except OSError:
                    free = None
            quota = quota_pct if quota_pct is not None else self._helius_quota_remaining_pct()
            reason = self._resource_reason(free, quota)
        return reason

    def negative_edge_reason(self, now: Optional[float] = None) -> Optional[str]:
        """Run #9's ONE pre-agreed money stop, and the only one.

        At `EARLY_STOP_NEGATIVE_EDGE_AT_SEC` after the start, if the 95 % CI of
        the mean PnL of `EARLY_STOP_NEGATIVE_EDGE_BOOK` is entirely below zero,
        the run stops as `negative_edge_S2`. Both values come from the
        environment and are declared in the launcher, so the rule is fixed
        before the first trade and cannot be re-read after the fact. With the
        variable unset (every other run) this returns None and PnL still never
        stops a run.
        """
        try:
            threshold = float(os.getenv('EARLY_STOP_NEGATIVE_EDGE_AT_SEC') or 0.0)
        except ValueError:
            return None
        if threshold <= 0:
            return None
        now = time.time() if now is None else float(now)
        if now - self.started_at_epoch < threshold:
            return None
        name = (os.getenv('EARLY_STOP_NEGATIVE_EDGE_BOOK') or 'portfolio_s2').strip()
        book = self.books.get(name)
        if book is None:
            return None
        stats = trade_stats([t.get('pnl_rub', 0.0) for t in book.closed_trades])
        ci = stats.get('ci95_mean')
        self.stop_policy_state['negative_edge'] = {
            'book': name,
            'checked_at_sec': round(now - self.started_at_epoch, 1),
            'threshold_sec': threshold,
            'n': stats.get('n'),
            'mean_rub': stats.get('mean'),
            'ci95_mean': ci,
        }
        if ci is None or int(stats.get('n') or 0) < 2:
            return None
        if float(ci[1]) < 0.0:
            return (
                f'negative_edge_S2 (CI95 of the mean [{ci[0]:.2f}, {ci[1]:.2f}] '
                f'< 0 after {int(now - self.started_at_epoch)} s, n={stats["n"]})'
            )
        return None

    def _maybe_early_stop(self) -> None:
        if self.early_stop_reason is not None:
            return
        # Run #9: the single declared exception to "PnL never stops the run".
        money_stop = self.negative_edge_reason()
        if money_stop:
            self.early_stop_reason = money_stop
            self.stop_policy_state['reason'] = money_stop
            self.early_stop_event.set()
            log(f'⏹ Pre-agreed stop: {money_stop}', Color.YELLOW)
            return
        # Run #3 addendum: only the four declared reasons may stop the run;
        # a PnL-based stop would be result fitting. The bank never ends the
        # session any more, because the book tops itself back up.
        stop_reason = self._evaluate_stop_reason()
        if stop_reason:
            self.early_stop_reason = stop_reason
            self.stop_policy_state['reason'] = stop_reason
            self.early_stop_event.set()
            log(f'⏹ Stop-list fired: {stop_reason}', Color.YELLOW)
            return
        # With top-ups enabled a depleted book refills itself, so this legacy
        # net only applies to books that opted out of the top-up rule.
        eligible = [
            book for book in self.books.values() if not book.profile.auto_top_up
        ]
        depleted = bool(eligible) and all(
            not any(pos.status != 'closed' for pos in book.positions.values())
            and book.cash_balance_rub < book.profile.position_size_rub
            for book in eligible
        )
        if depleted:
            self.early_stop_reason = 'both_portfolios_below_slot_size'
            self.early_stop_event.set()

    def _adapt_mark_poll(self, *, now: Optional[float] = None, rpc: Optional[dict[str, Any]] = None) -> None:
        """Slow the mark cadence down when a 10-minute window passes 2.2 rps.

        Run #8 asked for the run to survive to the end rather than stop on the
        rps ceiling, so the only lever left is the cadence. It only ever goes
        up (0.15 -> 0.25 s), once, and the change is reported as
        `mark_poll_adapted: true` — a silent slowdown would make the latency
        report a lie.
        """
        now = time.time() if now is None else float(now)
        if self.mark_poll_adapted:
            return
        if now - self._last_rps_check_epoch < MARK_POLL_RPS_CHECK_SEC:
            return
        self._last_rps_check_epoch = now
        metrics = rpc if rpc is not None else rpc_metrics_snapshot()
        peak = metrics.get('requests_per_sec_max_10min')
        if peak is None:
            return
        self.mark_poll_rps_at_check = float(peak)
        if float(peak) > self.mark_poll_rps_limit:
            if self.mark_poll_sec >= MARK_POLL_ADAPTED_SEC:
                # Run #9 polls at 0.25 s from the start: there is nothing left
                # to slow down, and claiming an adaptation would be a lie.
                if not self.mark_poll_adaptation_blocked:
                    self.mark_poll_adaptation_blocked = True
                    log(
                        f'🐢 {peak:.2f} rps in a 10-min window > '
                        f'{self.mark_poll_rps_limit:.1f}, but the cadence is '
                        f'already {self.mark_poll_sec:.2f} s; no adaptation '
                        'is available and none is claimed.', Color.YELLOW,
                    )
                return
            self.mark_poll_sec = MARK_POLL_ADAPTED_SEC
            self.mark_poll_adapted = True
            log(
                f'🐢 Mark poll adapted: {peak:.2f} rps in a 10-min window '
                f'> {self.mark_poll_rps_limit:.1f}; cadence '
                f'{MARK_POLL_SEC_DEFAULT:.2f} -> {MARK_POLL_ADAPTED_SEC:.2f} s.',
                Color.YELLOW,
            )

    async def _position_poll_loop(self) -> None:
        while not self._stopping.is_set():
            self._maybe_early_stop()
            if self.early_stop_event.is_set():
                return
            # Every arm is polled at the same frequency: an arm that is read
            # less often would be measured on a different price stream, which
            # is exactly what invalidated the run #6 shadows.
            self._poll_cycle += 1
            all_positions = [
                (book, pos)
                for book in self.books.values()
                for pos in list(book.positions.values())
                if pos.status != 'closed'
            ]
            # Per-profile RPC accounting: one batch serves every open position,
            # so each book is charged one read per cycle it participated in.
            if all_positions:
                for name in {book.profile.name for book, _pos in all_positions}:
                    self.rpc_reads_by_profile[name] = (
                        self.rpc_reads_by_profile.get(name, 0) + 1
                    )
            # Run #9 §2.3: ghosts ride along in the same batch. They add
            # addresses, never a request — the RPC bill is unchanged.
            #
            # Run #10 §2a: they ride along only when a scheduled second is
            # due. Between the eight points a ghost costs nothing at all, and a
            # cycle with no open position and no due ghost issues no request.
            now_epoch = time.time()
            self._ghosts = [
                ghost for ghost in self._ghosts
                if now_epoch < ghost['until_epoch']
                and int(ghost.get('schedule_index', 0)) < len(POST_EXIT_SCHEDULE_SEC)
            ]
            due_ghosts = self._due_post_exit_ghosts(now_epoch)
            if all_positions or due_ghosts:
                curve_addresses = list(dict.fromkeys(
                    [pos.curve for _book, pos in all_positions if pos.curve]
                    + [ghost['curve'] for ghost in due_ghosts if ghost['curve']]
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
                    fx_now = max(1e-9, float(self.settings.usd_rub_rate))
                    self._record_tick(
                        book, pos, pnl_pct, mark_at,
                        unit_price_rub / fx_now if unit_price_rub else None,
                    )
                    trigger = self._exit_trigger(book.profile, pos, pnl_pct, mark_at)
                    if trigger is not None:
                        if pos.exit_trigger_at_epoch is None:
                            pos.exit_trigger_at_epoch = mark_at
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
                    pos.exit_trigger_at_epoch = None
                    changed_books.add(book.profile.name)
                # Run #9 §2.3: the post-exit marks, from the batch above.
                # Run #10 §2a: only the ghosts whose point is due, one mark
                # each, then the schedule advances.
                for ghost in due_ghosts:
                    account = accounts.get(ghost['curve'])
                    if not isinstance(account, dict) or account.get('owner') != PUMP_PROGRAM_ID:
                        continue
                    raw = decode_account_data(account)
                    if not raw:
                        continue
                    try:
                        ghost_curve = CurveState.decode(raw)
                    except QuoteUnavailable:
                        continue
                    price = self._mid_price_usd(ghost_curve, sol_usd)
                    if price is None:
                        continue
                    self._record_post_exit_mark(ghost, time.time(), price)
                    # The point is taken; the next one is minutes away. A
                    # failed read above leaves the index untouched, so a lost
                    # mark is retried on the next cycle instead of being
                    # silently dropped from the schedule.
                    ghost['schedule_index'] = int(ghost.get('schedule_index', 0)) + 1
                for book in self.books.values():
                    if book.profile.name in changed_books:
                        book.save(self.run_id)
            self._maybe_early_stop()
            if self.early_stop_event.is_set():
                return
            self._adapt_mark_poll()
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self.mark_poll_sec,
                )
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
        price_usd: Optional[float],
        event: str = 'mark',
        exit_kind: Optional[str] = None,
    ) -> None:
        """One row of this position's own tick stream (exports/ticks.csv).

        Marks come from the exit quote the bot actually sees, not from the
        scheduled observations, so MFE/MAE and the trigger price are the ones
        the ladder really acted on.

        ``price_usd`` is a USD unit price and is written as-is. Run #6 divided
        a rouble price by ``sol_usd * fx`` for the entry and the marks (so
        every such row was off by exactly ``1 / sol_usd``) while the exit
        passed a rouble value and cancelled out. The conversion now happens at
        the call sites, once.
        """
        peak_at = pos.best_pnl_updated_at or pos.opened_at_epoch
        previous_pnl = pos.last_tick_pnl_pct
        previous_at = pos.last_tick_at_epoch
        trigger_pct = (
            round(pos.exit_trigger_pnl_pct, 4)
            if pos.exit_trigger_pnl_pct is not None else None
        )
        # P0-1 (run #7): trigger_pct == fill_pct on all 668 run6 exits, so the
        # latency columns measured a structural zero. The useful question is
        # how far the mark that fired the rule had already flown past it.
        step_gap_pp = (
            round(trigger_pct - previous_pnl, 4)
            if trigger_pct is not None and previous_pnl is not None else None
        )
        mark_gap_ms = (
            round(max(0.0, at_epoch - previous_at) * 1000.0, 2)
            if previous_at is not None else None
        )
        self.ticks.append({
            # Private, not a CSV column: the epoch of this mark, so a lookup by
            # "nearest to entry+N s" never has to re-derive it from t_sec.
            '_at_epoch': float(at_epoch),
            'portfolio': book.profile.name,
            'mint': pos.mint,
            't_sec': round(max(0.0, at_epoch - pos.opened_at_epoch), 3),
            'price_usd': round(price_usd, 16) if price_usd else None,
            'pnl_pct': round(pnl_pct, 4),
            'event': event,
            'trigger_pct': trigger_pct,
            'fill_pct': round(pnl_pct, 4) if event == 'exit' else None,
            'exit_latency_ms': (
                round(max(0.0, at_epoch - pos.exit_trigger_at_epoch) * 1000.0, 2)
                if event == 'exit' and pos.exit_trigger_at_epoch else None
            ),
            'peak_pct': round(pos.best_pnl_pct, 4),
            'peak_t_sec': round(max(0.0, peak_at - pos.opened_at_epoch), 3),
            # Run #7: the overshoot of the rule, measured mark to mark.
            'mark_prev_pct': (
                round(previous_pnl, 4) if previous_pnl is not None else None
            ),
            'step_gap_pp': step_gap_pp,
            'mark_gap_ms': mark_gap_ms,
            # Run #7: which rule closed it, so fly-through can be split by
            # exit type instead of averaged over everything.
            'exit_kind': exit_kind,
        })
        pos.last_tick_pnl_pct = float(pnl_pct)
        pos.last_tick_at_epoch = float(at_epoch)

    # --- run #9 §2.3: post-exit ghost -------------------------------------
    @staticmethod
    def _mid_price_usd(curve: CurveState, sol_usd: float) -> Optional[float]:
        """(v_sol / v_token) * SOL price — the same mid the entry reports."""
        try:
            v_tokens_ui = curve.virtual_token_reserves / TOKEN_SCALE
            v_sol = curve.virtual_quote_reserves / LAMPORTS_PER_SOL
        except (AttributeError, TypeError):
            return None
        if v_tokens_ui <= 0 or v_sol <= 0 or sol_usd <= 0:
            return None
        return v_sol / v_tokens_ui * sol_usd

    def _start_post_exit_ghost(
        self,
        book: PortfolioBook,
        pos: SimPosition,
        exit_epoch: float,
        exit_mid_price_usd: Optional[float],
        trade: Optional[dict[str, Any]] = None,
    ) -> None:
        """Follow the coin for POST_EXIT_WINDOW_SEC after it left the book."""
        if not pos.curve or exit_mid_price_usd is None:
            name = book.profile.name
            self.post_exit_skipped[name] = self.post_exit_skipped.get(name, 0) + 1
            return
        self.post_exit_started[book.profile.name] = (
            self.post_exit_started.get(book.profile.name, 0) + 1
        )
        self._ghosts.append({
            'portfolio': book.profile.name,
            'mint': pos.mint,
            'curve': pos.curve,
            'exit_at_epoch': float(exit_epoch),
            'entry_at_epoch': float(pos.opened_at_epoch),
            'entry_mid_price_usd': float(
                pos.entry_mid_price_usd or pos.entry_price_usd or 0.0
            ),
            'exit_mid_price_usd': float(exit_mid_price_usd),
            # Run #10 §2a: which point of the schedule is due next.
            'schedule_index': 0,
            'until_epoch': (
                float(exit_epoch) + POST_EXIT_WINDOW_SEC
                + max(POST_EXIT_SCHEDULE_GRACE_SEC, 2.0 * self.mark_poll_sec)
            ),
            # Run #9 §2.4: the closed trade is still being written, so the
            # ghost patches its price_at_5s instead of a second lookup.
            'trade': trade,
            'marks': 0,
        })

    def _price_at_offset(
        self, book_name: str, mint: str, entry_epoch: float, offset_sec: float,
    ) -> Optional[tuple[float, str]]:
        """Nearest real mark to entry+offset, from ticks or the ghost.

        No interpolation: the value is a price the bot actually read, and the
        distance to the requested moment is bounded by
        POST_EXIT_NEAREST_TOLERANCE_SEC.
        """
        target = entry_epoch + offset_sec
        best: Optional[tuple[float, float, str]] = None
        for tick in self.ticks:
            if (tick.get('portfolio') != book_name or tick.get('mint') != mint
                    or tick.get('event') != 'mark' or not tick.get('price_usd')):
                continue
            at = tick.get('_at_epoch')
            if at is None:
                continue
            distance = abs(float(at) - target)
            if best is None or distance < best[0]:
                best = (distance, float(tick['price_usd']), 'tick')
        for ghost in self._ghosts:
            if ghost['portfolio'] != book_name or ghost['mint'] != mint:
                continue
            for mark in ghost.setdefault('_rows', []):
                distance = abs(float(mark['_at_epoch']) - target)
                if best is None or distance < best[0]:
                    best = (distance, float(mark['price_usd']), 'post_exit')
        if best is None or best[0] > POST_EXIT_NEAREST_TOLERANCE_SEC:
            return None
        return best[1], best[2]

    def _due_post_exit_ghosts(self, now_epoch: float) -> list[dict[str, Any]]:
        """Ghosts whose next scheduled second has arrived (run #10 §2a).

        The schedule is a list of seconds since the exit, so a ghost is due
        when the clock passed `exit_at + schedule[schedule_index]`. Once the
        schedule is exhausted the ghost is not due again — it is pruned by the
        window check in the poll loop.
        """
        due: list[dict[str, Any]] = []
        for ghost in self._ghosts:
            index = int(ghost.get('schedule_index', 0))
            if index >= len(POST_EXIT_SCHEDULE_SEC):
                continue
            if now_epoch >= float(ghost['exit_at_epoch']) + POST_EXIT_SCHEDULE_SEC[index]:
                due.append(ghost)
        return due

    def _record_post_exit_mark(
        self, ghost: dict[str, Any], at_epoch: float, price_usd: float,
    ) -> None:
        sec_since_exit = at_epoch - ghost['exit_at_epoch']
        entry_mid = ghost['entry_mid_price_usd']
        exit_mid = ghost['exit_mid_price_usd']
        row = {
            'portfolio': ghost['portfolio'],
            'mint': ghost['mint'],
            'exit_at_epoch': round(ghost['exit_at_epoch'], 3),
            'sec_since_exit': round(sec_since_exit, 3),
            'price_usd': round(price_usd, 16),
            'pnl_vs_entry_pct': (
                round((price_usd / entry_mid - 1.0) * 100.0, 6) if entry_mid else None
            ),
            'pnl_vs_exit_pct': (
                round((price_usd / exit_mid - 1.0) * 100.0, 6) if exit_mid else None
            ),
        }
        ghost['marks'] += 1
        ghost.setdefault('_rows', []).append({**row, '_at_epoch': at_epoch})
        self.post_exit_rows.append(row)
        # §2.4: fill price_at_5s as soon as a mark close to entry+5 s lands.
        trade = ghost.get('trade')
        if trade is not None:
            target = ghost['entry_at_epoch'] + 5.0
            distance = abs(at_epoch - target)
            if distance <= POST_EXIT_NEAREST_TOLERANCE_SEC:
                current = trade.get('price_at_5s_distance_sec')
                if current is None or distance < float(current):
                    trade['price_at_5s'] = round(price_usd, 16)
                    trade['price_at_5s_source'] = 'post_exit'
                    trade['price_at_5s_distance_sec'] = round(distance, 3)
                    if entry_mid:
                        trade['pnl_pct_at_5s'] = round(
                            (price_usd / entry_mid - 1.0) * 100.0, 6,
                        )

    def write_post_exit(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=(
                    'portfolio', 'mint', 'exit_at_epoch', 'sec_since_exit',
                    'price_usd', 'pnl_vs_entry_pct', 'pnl_vs_exit_pct',
                ),
            )
            writer.writeheader()
            writer.writerows(self.post_exit_rows)

    def write_ticks(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=(
                    'portfolio', 'mint', 't_sec', 'price_usd', 'pnl_pct', 'event',
                    'trigger_pct', 'fill_pct', 'exit_latency_ms',
                    'peak_pct', 'peak_t_sec',
                    'mark_prev_pct', 'step_gap_pp', 'mark_gap_ms',
                    'exit_kind',
                ),
                extrasaction='ignore',
            )
            writer.writeheader()
            writer.writerows(self.ticks)

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

        # Run #8: `fee_cover` is a different exit, not a variant of the ladder.
        # It is decided first and returns in every case, so none of the Round 6
        # rules below (TP1, trail, shelf, no-progress, 35/90 s timeout) can
        # fire — and so none of them can appear in exit_breakdown.
        if profile.exit_mode == 'fee_cover':
            return self._fee_cover_trigger(profile, pos, pnl_pct, held)

        # Run #7: one exit ladder for every arm (the run #4 time exit and its
        # toggles were deleted, not parked).
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

        # The Round 6 stop: it applies before TP1 and before the rocket bypass,
        # while the best PnL is still below +10%.
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
            sell_pct = max(1.0, min(100.0, profile.tp1_sell_pct))
            sell_amount = max(
                1, int(round(pos.remaining_tokens_raw * sell_pct / 100.0)),
            )
            sell_amount = min(sell_amount, pos.remaining_tokens_raw)
            pos.tp1_partial_pending = True
            pos.tp1_pending_sell_raw = sell_amount
            return (
                f'TP1 +{profile.tp1_pct:.0f}% partial {profile.tp1_sell_pct:.0f}%',
                sell_amount,
            )

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
        if (
            pnl_pct >= profile.shelf_min_pnl_pct
            and stalled_sec >= profile.shelf_stagnation_sec
        ):
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

    def _fee_cover_trigger(
        self,
        profile: ABProfile,
        pos: SimPosition,
        pnl_pct: float,
        held: float,
    ) -> Optional[tuple[str, int]]:
        """Run #8's exit rule — always the whole remaining position.

        Order matters: a hard stop outranks a profit target when a mark jumps
        over both, and the clock only runs when neither price rule fired. The
        three thresholds are measured, not tuned: the round trip costs 3.25 %
        of the stake, so +3.5 % covers it with 0.25 pp of slack, and the replay
        of run #7's real B1 trades says a 5 s clock is the least bad of the
        short holds. `tp1_sell_pct` is deliberately unused: a partial exit
        cannot both cover the fee and leave the book flat.
        """
        if pnl_pct <= -abs(float(profile.hard_stop_pct)):
            return (
                f'Hard stop (−{abs(float(profile.hard_stop_pct)):.1f}%)',
                pos.remaining_tokens_raw,
            )
        if pnl_pct >= float(profile.fee_cover_pct):
            return (
                f'Fee cover (+{float(profile.fee_cover_pct):.1f}%)',
                pos.remaining_tokens_raw,
            )
        if held >= float(profile.time_exit_sec):
            return (
                f'Time exit ({float(profile.time_exit_sec):.1f}s)',
                pos.remaining_tokens_raw,
            )
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
        self._record_tick(
            book, pos, realized_pct_now, time.time(),
            exit_price_usd, event='exit',
            exit_kind=self.exit_reason_kind(str(reason).split(' + ')[-1]),
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
            book.note_bust(time.time())
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
            # Run #9: the mid at the exit snapshot, not the fee-loaded proceeds
            # price. Comparing it with the entry mid is what makes "the price
            # did not move" a decidable statement, and it is the right
            # reference for the post-exit ghost.
            exit_mid_price_usd = self._mid_price_usd(curve, sol_usd)
            entry_mid_price_usd = pos.entry_mid_price_usd
            flat_move = (
                abs(exit_mid_price_usd / entry_mid_price_usd - 1.0) <= 1e-4
                if exit_mid_price_usd and entry_mid_price_usd else False
            )
            # The cost of one round trip, measured on the trades where the
            # market price did not move at all: there the PnL IS the cost.
            round_trip_cost_pct = (
                round(-total_pnl_pct, 6) if flat_move else None
            )
            price_at_5s = self._price_at_offset(
                book.profile.name, pos.mint, pos.opened_at_epoch, 5.0,
            )
            if price_at_5s is not None and pos.price_at_5s is None:
                pos.price_at_5s, pos.price_at_5s_source = price_at_5s[0], price_at_5s[1]
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
                # Epoch, not just a UTC string: the hourly blocks and the
                # first/second-half stability split need arithmetic.
                'opened_at_epoch': round(pos.opened_at_epoch, 3),
                'closed_at_epoch': round(time.time(), 3),
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
                'entry_lag_ms': pos.entry_lag_ms,
                # Run #8 §1.3: the chain survives into closed_trades, so the
                # report can be built from trades alone.
                't_detect_ms': pos.t_detect_ms,
                't_filters_ms': pos.t_filters_ms,
                't_read_ms': pos.t_read_ms,
                't_fill_ms': pos.t_fill_ms,
                't_total_ms': pos.t_total_ms,
                'entry_latency_ms': pos.entry_latency_ms,
                'entry_latency_floor_met': pos.entry_latency_floor_met,
                'entry_premium_pct': pos.entry_premium_pct,
                'entry_fill_premium_pct': pos.entry_fill_premium_pct,
                'mfe_pct': round(pos.mfe_pct, 6),
                'mae_pct': round(pos.mae_pct, 6),
                'peak_pct': round(pos.best_pnl_pct, 6),
                'peak_t_sec': round(
                    max(0.0, (pos.best_pnl_updated_at or pos.opened_at_epoch)
                        - pos.opened_at_epoch), 3,
                ),
                'captured_pct_of_peak': (
                    round(total_pnl_pct / pos.best_pnl_pct, 4)
                    if pos.best_pnl_pct > 0 else None
                ),
                'trigger_pct': pos.exit_trigger_pnl_pct,
                'fill_pct': round(total_pnl_pct, 6),
                # --- run #9 §3 ------------------------------------------
                'delta_at_entry_sol': (
                    round(pos.entry_v_sol - pos.birth_v_sol, 6)
                    if pos.entry_v_sol is not None and pos.birth_v_sol is not None
                    else None
                ),
                'v_sol_birth': round(pos.birth_v_sol, 6) if pos.birth_v_sol else None,
                # 29.96 SOL is a bare launch; the difference is the dev's own
                # buy, which is what S2's 31.96 floor selects.
                'dev_buy_sol': (
                    round(pos.birth_v_sol - 29.96, 6) if pos.birth_v_sol else None
                ),
                'name_copies_120s': pos.name_copies_120s,
                'entry_mid_price_usd': (
                    round(entry_mid_price_usd, 16) if entry_mid_price_usd else None
                ),
                'exit_mid_price_usd': (
                    round(exit_mid_price_usd, 16) if exit_mid_price_usd else None
                ),
                'flat_move': flat_move,
                'round_trip_cost_pct': round_trip_cost_pct,
                'price_at_5s': (
                    round(pos.price_at_5s, 16) if pos.price_at_5s else None
                ),
                'pnl_pct_at_5s': (
                    round((pos.price_at_5s / entry_mid_price_usd - 1.0) * 100.0, 6)
                    if pos.price_at_5s and entry_mid_price_usd else None
                ),
                'price_at_5s_source': pos.price_at_5s_source,
                'network_fees_rub': round(pos.realized_fee_rub, 6),
                'onchain_fees_rub': round(pos.onchain_fees_paid_rub, 6),
            }
            book.closed_trades.append(closed)
            # Run #9 §2.3: keep following the coin for 10 s. The ghost patches
            # price_at_5s on this very dict when a mark near entry+5 s lands.
            self._start_post_exit_ghost(
                book, pos, time.time(), exit_mid_price_usd, trade=closed,
            )
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

    def snapshot_artifacts(self, *, reason: str = 'periodic') -> dict[str, Any]:
        """Write every artefact that does not need the run to be over (P0-5).

        Run #5 lost its telemetry DB, log, summary and wallets because they
        were only written in the final handler. This is called every 60 s from
        the session, so a killed process still leaves readable numbers.
        """
        export_dir = Path(os.getenv('TELEMETRY_EXPORT_DIR', str(self.run_dir / 'exports')))
        written: list[str] = []
        for name, writer in (
            ('ticks.csv', self.write_ticks),
            ('post_exit.csv', self.write_post_exit),
            ('enrich.csv', self.write_enrich),
        ):
            try:
                writer(export_dir / name)
                written.append(name)
            except Exception as exc:  # an artefact must never kill the run
                log(f'⚠️ {name} не записан ({type(exc).__name__})', Color.YELLOW)
        summary = self.summary()
        summary_path = self.run_dir / 'ab_summary.json'
        tmp = summary_path.with_suffix('.json.tmp')
        try:
            tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
            os.replace(tmp, summary_path)
            written.append('ab_summary.json')
        except Exception as exc:
            log(f'⚠️ ab_summary.json не записан ({type(exc).__name__})', Color.YELLOW)
        for book in self.books.values():
            try:
                book.save(self.run_id)
            except Exception:
                pass
        try:
            (self.run_dir / 'STATUS.txt').write_text(
                f'snapshot={reason}\nwritten_at_utc={utc_now()}\n'
                f'duration_sec={round(time.time() - self.started_at_epoch, 1)}\n'
                'paper_only=true\nsigned_transactions=0\n'
                + ''.join(
                    f'{name}: entries={book.entry_count} '
                    f'closed={book.closed_trade_count} '
                    f'cash={book.cash_balance_rub:.2f} '
                    f'pnl={book.realized_pnl_rub:.2f}\n'
                    for name, book in self.books.items()
                ),
                encoding='utf-8',
            )
        except OSError:
            pass
        return {'written': written, 'reason': reason}

    def write_enrich(self, path: Path) -> None:
        """exports/enrich.csv — collected features, never read by a rule."""
        self.enrich.write_csv(path)

    async def drain_open_positions(
        self, max_wait_sec: Optional[float] = None,
    ) -> int:
        """Let the real poll loop close what is still open; return the leftovers.

        Waiting is not an exit: no fill is fabricated here. The caller either
        gets 0 (every position left by fee_cover / time_exit / hard_stop) or the
        honest count of positions that outlived the wait, which the report
        prints as-is.
        """
        limit = (
            DRAIN_OPEN_POSITIONS_MAX_SEC if max_wait_sec is None
            else float(max_wait_sec)
        )
        deadline = time.time() + max(0.0, limit)
        while time.time() < deadline:
            if not any(book.positions for book in self.books.values()):
                break
            await asyncio.sleep(max(0.01, self.mark_poll_sec))
        self.drain_left_open = sum(len(book.positions) for book in self.books.values())
        self.drain_waited_sec = max(0.0, limit - max(0.0, deadline - time.time()))
        if self.drain_left_open:
            log(
                f'⚠️ {self.drain_left_open} positions still open after the '
                f'{limit:.1f} s drain; they are reported as open, not closed.',
                Color.YELLOW,
            )
        return self.drain_left_open

    async def drain_post_exit_ghosts(
        self, max_wait_sec: Optional[float] = None,
    ) -> int:
        """Let ghosts that are still running collect their required points.

        Run #9 §2.3: a ghost lasts POST_EXIT_WINDOW_SEC after the exit, so an
        exit in the last ten seconds of the run would otherwise be written with
        one or two marks and would silently fail the "5 marks per exit"
        acceptance. Waiting here is not a trade: no fill is fabricated, the poll
        loop simply keeps reading the coins it is already reading.
        """
        limit = (
            POST_EXIT_WINDOW_SEC + POST_EXIT_SCHEDULE_GRACE_SEC
            + 2.0 * max(0.05, self.mark_poll_sec)
            if max_wait_sec is None else float(max_wait_sec)
        )
        started = time.time()
        deadline = started + max(0.0, limit)

        def unfinished() -> int:
            # A ghost is unfinished only while it still owes a scheduled point:
            # the window itself may outlive the schedule by the grace second.
            now = time.time()
            return sum(
                1 for ghost in self._ghosts
                if now < ghost['until_epoch']
                and int(ghost.get('schedule_index', 0)) < len(POST_EXIT_SCHEDULE_SEC)
            )

        while time.time() < deadline:
            if not unfinished():
                break
            await asyncio.sleep(max(0.01, self.mark_poll_sec))
        self.ghost_drain_waited_sec = round(time.time() - started, 3)
        self.ghost_drain_left_open = unfinished()
        if self.ghost_drain_left_open:
            log(
                f'⚠️ {self.ghost_drain_left_open} post-exit ghosts still open '
                f'after the {limit:.1f} s drain', Color.YELLOW,
            )
        return self.ghost_drain_left_open

    async def close(self) -> dict[str, Any]:
        # Run #8: give the live rules a bounded moment to finish their trade
        # before the poll loop is stopped.
        try:
            await self.drain_open_positions()
        except Exception as exc:  # never block finalization on the drain
            log(f'⚠️ drain open positions failed ({type(exc).__name__})', Color.YELLOW)
        try:
            await self.drain_post_exit_ghosts()
        except Exception as exc:  # never block finalization on the ghosts
            log(f'⚠️ post-exit ghost drain failed ({type(exc).__name__})', Color.YELLOW)
        self._stopping.set()
        try:
            await self.enrich.close()
        except Exception as exc:  # noqa: BLE001
            log(f'⚠️ enrich не остановлен ({type(exc).__name__})', Color.YELLOW)
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
            self.write_post_exit(export_dir / 'post_exit.csv')
        except Exception as exc:
            log(f'⚠️ post_exit.csv не записан ({type(exc).__name__})', Color.YELLOW)
        try:
            self.write_enrich(export_dir / 'enrich.csv')
        except Exception as exc:
            log(f'⚠️ enrich.csv не записан ({type(exc).__name__})', Color.YELLOW)
        summary = self.summary()
        summary_path = self.run_dir / 'ab_summary.json'
        tmp = summary_path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(tmp, summary_path)
        return summary

    @staticmethod
    def _exit_rule_label(profile: ABProfile) -> str:
        """One line that identifies the exit rule (identical in every arm)."""
        if profile.exit_mode == 'fee_cover':
            return (
                f'fee cover: hard stop −{profile.hard_stop_pct:g}%, '
                f'sell 100% at +{profile.fee_cover_pct:g}%, else time exit '
                f'{profile.time_exit_sec:g}s; no TP1, no trail, no shelf, '
                f'no no-progress'
            )
        return (
            f'4-mode ladder: SL −{profile.stop_loss_pct:.0f}%, '
            f'TP1 +{profile.tp1_pct:.0f}%={profile.tp1_sell_pct:.0f}%, '
            f'TP1/rocket bypass +{profile.rocket_bypass_pct:g}%, '
            f'trail 0.85x after +{profile.trailing_activation_pct:.0f}%, '
            f'shelf {profile.shelf_stagnation_sec:g}s, '
            f'no-progress {profile.no_progress_sec:g}s'
        )

    @staticmethod
    def _changed_boundaries(reference: ABProfile, arm: ABProfile) -> list[str]:
        """Entry-boundary fields this arm moves relative to A (must be one)."""
        fields = (
            'v_sol_birth_min', 'v_sol_birth_max',
            'delta_min_sol', 'delta_max_sol',
        )
        # Two decimals: a band edge is money-adjacent, and "0.1 -> 0" reads like
        # a bug in a report that also prints 0.10 elsewhere.
        changed = [
            f'{field} {getattr(reference, field):.2f} -> {getattr(arm, field):.2f}'
            for field in fields
            if getattr(reference, field) != getattr(arm, field)
        ]
        return changed or ['none']

    @staticmethod
    def exit_reason_kind(reason: str) -> str:
        """Collapse a free-text exit reason into a category for the report."""
        text = str(reason or '').strip()
        head = text.split('(')[0].strip()
        for prefix, label in (
            # Run #8's three rules. They must be classified here: an unknown
            # head lands in its own bucket, which is how a run #8 report would
            # end up with an "unknown" exit category.
            ('Hard stop', 'hard_stop'),
            ('Fee cover', 'fee_cover'),
            ('Time exit', 'time_exit'),
            ('SL/trail', 'trail'),
            ('SL', 'stop_loss'),
            ('TP1', 'tp'),
            ('Shelf', 'shelf'),
            ('No progress', 'no_progress'),
            ('Timeout', 'timeout'),
        ):
            if head.startswith(prefix):
                return label
        return head or 'unknown'

    def tick_quality(self) -> dict[str, Any]:
        """How far past its trigger the SL mark already was (P0-1, run #7).

        Run #6 measured `trigger_pct == fill_pct` on all 668 exits, because the
        trigger and the fill were the same object. With the mark-to-mark
        columns we can answer the real question: the SL fires at -10 %, but at
        which price did the trade actually leave?
        """
        def stats(values: list[float], *, largest_is_worst: bool = False) -> dict[str, Any]:
            # `worst` is direction-aware: for a fill % the minimum is the worst
            # fill, for a gap in milliseconds the maximum is. Printing the
            # minimum of the mark gaps ("median 5553 ms, worst 714 ms") was a
            # straight mislabel.
            if not values:
                return {'n': 0, 'median': None, 'mean': None, 'p90': None,
                        'worst': None}
            ordered = sorted(values)
            middle = len(ordered) // 2
            median = (
                ordered[middle] if len(ordered) % 2
                else (ordered[middle - 1] + ordered[middle]) / 2.0
            )
            p90_index = max(0, min(len(ordered) - 1, math.ceil(0.9 * len(ordered)) - 1))
            return {
                'n': len(values),
                'median': round(median, 4),
                'mean': round(sum(values) / len(values), 4),
                'p90': round(ordered[p90_index], 4),
                'worst': round(ordered[-1] if largest_is_worst else ordered[0], 4),
            }

        exits = [t for t in self.ticks if t.get('event') == 'exit']
        stop_exits = [
            float(t['fill_pct']) for t in exits
            if isinstance(t.get('trigger_pct'), (int, float))
            and float(t['trigger_pct']) <= -10.0
            and isinstance(t.get('fill_pct'), (int, float))
        ]
        gaps = [
            float(t['mark_gap_ms']) for t in self.ticks
            if isinstance(t.get('mark_gap_ms'), (int, float))
        ]
        # Fly-through per exit type: how far past its own trigger the exit mark
        # already was, in percentage points of PnL. Positive = the rule fired
        # late (the fill is worse than the trigger); the median is the number
        # the ladder can actually be blamed for.
        by_kind: dict[str, list[float]] = {}
        for tick in exits:
            trigger = tick.get('trigger_pct')
            fill = tick.get('fill_pct')
            if not isinstance(trigger, (int, float)) or not isinstance(fill, (int, float)):
                continue
            kind = str(tick.get('exit_kind') or 'unknown')
            by_kind.setdefault(kind, []).append(float(fill) - float(trigger))
        return {
            'stop_loss_fills': stats(stop_exits),
            'stop_loss_worse_than_minus15': sum(1 for v in stop_exits if v < -15.0),
            'stop_loss_trigger_pct': -10.0,
            'mark_gap_ms': stats(gaps, largest_is_worst=True),
            'fly_through_by_exit': {
                kind: {'n': len(values), **stats(values)}
                for kind, values in sorted(by_kind.items())
            },
            'rows': len(self.ticks),
        }

    def _exit_breakdown(self, book: PortfolioBook) -> dict[str, Any]:
        """n / sum / mean / median hold per exit category (P0-4)."""
        buckets: dict[str, dict[str, Any]] = {}
        for trade in book.closed_trades:
            reasons = str(trade.get('reason') or '')
            kind = self.exit_reason_kind(reasons.split(' + ')[-1] if reasons else '')
            slot = buckets.setdefault(kind, {'n': 0, 'sum_rub': 0.0, 'holds': []})
            slot['n'] += 1
            slot['sum_rub'] += float(trade.get('pnl_rub') or 0.0)
            slot['holds'].append(float(trade.get('held_sec') or 0.0))
        return {
            kind: {
                'n': slot['n'],
                'sum_rub': round(slot['sum_rub'], 2),
                'mean_rub': round(slot['sum_rub'] / slot['n'], 2),
                'median_hold_sec': round(median(slot['holds']) or 0.0, 2),
            }
            for kind, slot in sorted(buckets.items(), key=lambda kv: -kv[1]['n'])
        }

    def _entry_quality(self, book: PortfolioBook) -> dict[str, Any]:
        """Premium paid for waiting and the age of the read that was priced."""
        trades = list(book.closed_trades)
        open_positions = [p for p in book.positions.values()]
        premiums = [
            float(t['entry_premium_pct']) for t in trades
            if isinstance(t.get('entry_premium_pct'), (int, float))
        ]
        fill_premiums = [
            float(t['entry_fill_premium_pct']) for t in trades
            if isinstance(t.get('entry_fill_premium_pct'), (int, float))
        ]
        lags = [
            float(t['entry_lag_ms']) for t in trades
            if isinstance(t.get('entry_lag_ms'), (int, float))
        ] + [
            float(p.entry_lag_ms) for p in open_positions
            if isinstance(p.entry_lag_ms, (int, float))
        ]
        return {
            'n': len(premiums),
            'entry_premium_pct_median': (
                round(median(premiums), 4) if premiums else None
            ),
            'entry_premium_pct_mean': (
                round(sum(premiums) / len(premiums), 4) if premiums else None
            ),
            'entry_premium_pct_p90': (
                round(percentile(premiums, 0.90), 4) if premiums else None
            ),
            'entry_fill_premium_pct_median': (
                round(median(fill_premiums), 4) if fill_premiums else None
            ),
            'entry_lag_ms_median': round(median(lags), 2) if lags else None,
            'entry_lag_ms_p95': round(percentile(lags, 0.95), 2) if lags else None,
            'entry_lag_ms_max': round(max(lags), 2) if lags else None,
            'entry_lag_ms_over_1000': sum(1 for lag in lags if lag > 1_000.0),
            'entry_latency_ms_model': float(book.profile.entry_latency_ms),
            # Run #7: `entry_lag_ms_*` above is the honest version of the old
            # `entry_latency_floor_violations` (143/167 in run #6, which only
            # said the on-chain read was older than the modelled 150 ms).
            'entry_at_detection': book.profile.entry_at_detection,
        }

    # --- run #8 §3: the five blocks the report must contain ---------------
    LATENCY_STAGES = ('t_detect_ms', 't_filters_ms', 't_read_ms', 't_fill_ms', 't_total_ms')

    def _latency_chain(self, book: PortfolioBook) -> dict[str, Any]:
        """Median/p90/max of every stage, and the share of sub-200 ms entries."""
        trades = list(book.closed_trades)
        stages: dict[str, Any] = {}
        for field in self.LATENCY_STAGES:
            values = [
                float(t[field]) for t in trades
                if isinstance(t.get(field), (int, float))
            ]
            stages[field] = {
                'n': len(values),
                'median': round(float(median(values)), 2) if values else None,
                'p90': (round(float(percentile(values, 0.90)), 2) if values else None),
                'max': round(max(values), 2) if values else None,
            }
        totals = [
            float(t['t_total_ms']) for t in trades
            if isinstance(t.get('t_total_ms'), (int, float))
        ]
        return {
            'n': len(totals),
            'stages': stages,
            'share_under_200ms': (
                round(sum(1 for v in totals if v < 200.0) / len(totals), 4)
                if totals else None
            ),
            'share_under_500ms': (
                round(sum(1 for v in totals if v < 500.0) / len(totals), 4)
                if totals else None
            ),
            'entry_latency_ms_model': float(book.profile.entry_latency_ms),
        }

    def _hold_replay(self, book: PortfolioBook) -> dict[str, Any]:
        """What the same fills would have paid at 1/2/3/5 s, and with a stop.

        Read from this position's own tick stream (`exports/ticks.csv`), so the
        numbers are the marks the bot really saw — not an interpolated series.
        Everything is in percent of the stake: the trade rows do not carry the
        cost basis, and inventing one would turn a measurement into a model.
        """
        marks_by_mint: dict[str, list[tuple[float, float]]] = {}
        for tick in self.ticks:
            if tick.get('portfolio') != book.profile.name:
                continue
            if tick.get('event') == 'exit':
                continue
            pnl = tick.get('pnl_pct')
            at = tick.get('t_sec')
            if not isinstance(pnl, (int, float)) or not isinstance(at, (int, float)):
                continue
            marks_by_mint.setdefault(str(tick.get('mint')), []).append(
                (float(at), float(pnl))
            )
        for marks in marks_by_mint.values():
            marks.sort()

        def pnl_at_or_before(marks, target: float) -> Optional[float]:
            last = None
            for at, pnl in marks:
                if at > target:
                    break
                last = pnl
            return last

        def first_at_or_after(marks, target: float) -> Optional[float]:
            for at, pnl in marks:
                if at >= target:
                    return pnl
            return None

        def stop_first(marks, upto: float, stop_pct: float) -> Optional[float]:
            for at, pnl in marks:
                if at > upto:
                    break
                if pnl <= stop_pct:
                    return pnl
            return first_at_or_after(marks, upto)

        snapshots: dict[str, list[float]] = {f'pnl_pct_at_{h}s': [] for h in (1, 2, 3, 5)}
        replays: dict[str, list[float]] = {
            'hold_to_3s': [], 'hold_to_5s': [], 'hold_to_5s_stop_10': [],
        }
        for trade in book.closed_trades:
            marks = marks_by_mint.get(str(trade.get('mint')))
            if not marks:
                continue
            entry = pnl_at_or_before(marks, 0.0)
            for hold in (1, 2, 3, 5):
                value = pnl_at_or_before(marks, float(hold))
                snapshots[f'pnl_pct_at_{hold}s'].append(
                    value if value is not None else (entry if entry is not None else 0.0)
                )
            for key, value in (
                ('hold_to_3s', first_at_or_after(marks, 3.0)),
                ('hold_to_5s', first_at_or_after(marks, 5.0)),
                ('hold_to_5s_stop_10', stop_first(marks, 5.0, -10.0)),
            ):
                if value is not None:
                    replays[key].append(value)

        def summarise(values: list[float]) -> dict[str, Any]:
            return {
                'n': len(values),
                'mean_pct': round(sum(values) / len(values), 4) if values else None,
                'median_pct': round(float(median(values)), 4) if values else None,
            }

        actual = [float(t.get('pnl_pct') or 0.0) for t in book.closed_trades]
        return {
            'n': len(actual),
            'actual': summarise(actual),
            'snapshots': {key: summarise(values) for key, values in snapshots.items()},
            'replay': {key: summarise(values) for key, values in replays.items()},
            'source': 'ticks.csv of each closed trade; percent of stake, not RUB',
        }

    def _round_trip_cost(self, book: PortfolioBook) -> dict[str, Any]:
        """The measured cost of one round trip on trades whose price stood still.

        A flat trade pays fees + impact and nothing else, so its PnL *is* the
        round-trip cost. Run #7 measured 3.25 % this way over 202 trades, which
        is why fee_cover aims at +3.5 %.
        """
        flat: list[float] = []
        for trade in book.closed_trades:
            mfe = trade.get('mfe_pct')
            mae = trade.get('mae_pct')
            if not isinstance(mfe, (int, float)) or not isinstance(mae, (int, float)):
                continue
            if float(mfe) - float(mae) <= 1.0:
                flat.append(float(trade.get('pnl_pct') or 0.0))
        premiums = [
            float(t['entry_fill_premium_pct']) for t in book.closed_trades
            if isinstance(t.get('entry_fill_premium_pct'), (int, float))
        ]
        # Run #9 §3: a stricter, per-trade definition written into the trade
        # itself — the exit mid equals the entry mid within 0.01 %. It is the
        # gate of stage B ("do not start B if the median is above 4.0 %"), so
        # it must come from the trade rows, not from a replay of mfe/mae.
        exact = [
            float(t['round_trip_cost_pct']) for t in book.closed_trades
            if isinstance(t.get('round_trip_cost_pct'), (int, float))
        ]
        return {
            'flat_definition': 'mfe_pct - mae_pct <= 1.0 pp (price did not move)',
            'n_flat': len(flat),
            'round_trip_cost_pct_median': (
                round(-float(median(flat)), 4) if flat else None
            ),
            'round_trip_cost_pct_mean': (
                round(-sum(flat) / len(flat), 4) if flat else None
            ),
            'exact_definition': 'exit mid / entry mid - 1 within 0.01 %',
            'n_flat_exact': len(exact),
            'round_trip_exact_pct_median': (
                round(float(median(exact)), 4) if exact else None
            ),
            'round_trip_exact_pct_mean': (
                round(sum(exact) / len(exact), 4) if exact else None
            ),
            'reference_round_trip_pct': 3.25,
            'entry_fill_premium_pct_median': (
                round(float(median(premiums)), 4) if premiums else None
            ),
            'reference_entry_fill_premium_pct': 1.41,
        }

    def _gate_price(self, book: PortfolioBook) -> dict[str, Any]:
        """What the momentum gate costs: the fill vs the price at detection.

        `entry_premium_pct` is (v_now / v_birth)^2 - 1 on the same read, i.e.
        exactly the price paid for waiting until delta reached delta_min. For S0
        (delta_min 0.00) the entry read IS the detection read, so this is the
        number the gate is responsible for.
        """
        premiums = [
            float(t['entry_premium_pct']) for t in book.closed_trades
            if isinstance(t.get('entry_premium_pct'), (int, float))
        ]
        birth = [
            float(t['entry_price_usd']) for t in book.closed_trades
            if isinstance(t.get('entry_price_usd'), (int, float))
        ]
        return {
            'n': len(premiums),
            'delta_min_sol': float(book.profile.delta_min_sol),
            'entry_premium_pct_median': (
                round(float(median(premiums)), 4) if premiums else None
            ),
            'entry_premium_pct_p90': (
                round(float(percentile(premiums, 0.90)), 4) if premiums else None
            ),
            'share_premium_above_1pct': (
                round(sum(1 for v in premiums if v > 1.0) / len(premiums), 4)
                if premiums else None
            ),
            'n_entry_price': len(birth),
            'definition': (
                'entry_premium_pct = (v_sol at the entry read / v_sol_birth)^2 - 1'
            ),
        }

    def _exit_shares(self, book: PortfolioBook) -> dict[str, Any]:
        """Shares of the exit categories, with run #8's three named explicitly."""
        breakdown = self._exit_breakdown(book)
        total = sum(int(slot.get('n') or 0) for slot in breakdown.values())
        shares = {
            kind: {
                'n': int(slot.get('n') or 0),
                'share': round(int(slot.get('n') or 0) / total, 4) if total else None,
                'sum_rub': slot.get('sum_rub'),
            }
            for kind, slot in breakdown.items()
        }
        return {
            'n': total,
            'shares': shares,
            # Run #8 acceptance: fee_cover must fire at least once per book.
            'fee_cover_fired': int(shares.get('fee_cover', {}).get('n') or 0) > 0,
            'fee_cover_n': int(shares.get('fee_cover', {}).get('n') or 0),
            'time_exit_n': int(shares.get('time_exit', {}).get('n') or 0),
            'hard_stop_n': int(shares.get('hard_stop', {}).get('n') or 0),
            'ladder_kinds_present': sorted(
                kind for kind in shares
                if kind in {'tp', 'trail', 'stop_loss', 'shelf', 'no_progress', 'timeout'}
            ),
        }

    def _post_exit_stats(self) -> dict[str, Any]:
        """§2.3-2.4: how complete the ghost is, and what it measured.

        Nothing here is interpolated: a required point counts as covered only
        when a real mark landed within POST_EXIT_NEAREST_TOLERANCE_SEC of it.
        """
        by_exit: dict[tuple[str, str, float], list[dict[str, Any]]] = {}
        for row in self.post_exit_rows:
            key = (row['portfolio'], row['mint'], float(row['exit_at_epoch']))
            by_exit.setdefault(key, []).append(row)
        required = POST_EXIT_REQUIRED_SEC
        per_book: dict[str, Any] = {}
        for name, book in self.books.items():
            exits = [
                rows for key, rows in by_exit.items() if key[0] == name
            ]
            marks_per_exit = [len(rows) for rows in exits]
            coverage: dict[str, float] = {}
            for target in required:
                covered = 0
                for rows in exits:
                    if any(abs(float(r['sec_since_exit']) - target)
                           <= POST_EXIT_NEAREST_TOLERANCE_SEC for r in rows):
                        covered += 1
                coverage[f'+{target:g}s'] = (
                    round(covered / len(exits), 4) if exits else 0.0
                )

            def metric(offset: float) -> Optional[float]:
                picked: list[float] = []
                for rows in exits:
                    nearest = min(
                        rows, key=lambda r: abs(float(r['sec_since_exit']) - offset),
                        default=None,
                    )
                    if nearest is not None and abs(
                        float(nearest['sec_since_exit']) - offset
                    ) <= POST_EXIT_NEAREST_TOLERANCE_SEC:
                        if nearest.get('pnl_vs_exit_pct') is not None:
                            picked.append(float(nearest['pnl_vs_exit_pct']))
                if not picked:
                    return None
                return round(sum(picked) / len(picked), 4)

            five_s = [
                round(float(t['pnl_pct_at_5s']), 4)
                for t in book.closed_trades
                if t.get('pnl_pct_at_5s') is not None
            ]
            per_book[name] = {
                'exits_followed': self.post_exit_started.get(name, 0),
                'exits_not_followed': self.post_exit_skipped.get(name, 0),
                'exits_with_marks': len(exits),
                'rows': sum(len(rows) for rows in exits),
                'marks_per_exit_min': min(marks_per_exit) if marks_per_exit else 0,
                'marks_per_exit_median': (
                    statistics.median(marks_per_exit) if marks_per_exit else 0
                ),
                'required_point_coverage': coverage,
                'mean_pnl_vs_exit_pct_at': {
                    f'+{target:g}s': metric(target) for target in required
                },
                'price_at_5s_trades': len(five_s),
                'price_at_5s_coverage': (
                    round(len(five_s) / len(book.closed_trades), 4)
                    if book.closed_trades else 0.0
                ),
                'pnl_pct_at_5s_median': (
                    round(statistics.median(five_s), 4) if five_s else None
                ),
                'pnl_pct_at_5s_mean': (
                    round(sum(five_s) / len(five_s), 4) if five_s else None
                ),
            }
        return {
            'window_sec': POST_EXIT_WINDOW_SEC,
            'nearest_tolerance_sec': POST_EXIT_NEAREST_TOLERANCE_SEC,
            'required_points_sec': list(required),
            'source': (
                'post_exit.csv: the same getMultipleAccounts batch the open '
                'positions trigger, so no extra RPC. price_usd is the curve mid '
                '(v_sol / v_token * SOL price); pnl_vs_exit_pct is mid-to-mid'
            ),
            'per_book': per_book,
        }

    def _run_line(self, rpc: dict[str, Any]) -> dict[str, Any]:
        """Everything the acceptance line needs, in one dict (run #8 §3(7))."""
        return {
            'paper_only': True,
            'signed_transactions': 0,
            'early_stop_reason': self.early_stop_reason,
            'requests_per_sec': rpc.get('requests_per_sec'),
            'requests_per_sec_max_10min': rpc.get('requests_per_sec_max_10min'),
            'http_429_count': int(rpc.get('http_429_count') or 0),
            'unrecovered_rate': rpc.get('unrecovered_rate'),
            'mark_poll_sec': round(self.mark_poll_sec, 4),
            'mark_poll_adapted': self.mark_poll_adapted,
        }

    def _rocket_peaks(self) -> dict[str, float]:
        """Peak % per mint on the 60 s observation window (empty if absent)."""
        collector = get_collector()
        db_path = getattr(collector, 'db_path', None) or os.getenv('TELEMETRY_DB_PATH')
        if not db_path:
            return {}
        return observation_peaks(db_path, window_sec=ROCKET_WINDOW_SEC)

    def summary(self) -> dict[str, Any]:
        peaks = self._rocket_peaks()
        post_exit_stats = self._post_exit_stats()
        portfolios: dict[str, Any] = {}
        # Which arm each book is measured against: A when it runs, otherwise
        # the arm's declared pair partner if that partner is running too.
        reference_name = 'portfolio_a' if 'portfolio_a' in self.books else None
        reference_for: dict[str, Optional[str]] = {}
        for name in self.books:
            candidate = ARM_REFERENCE.get(name, 'portfolio_a')
            reference_for[name] = (
                candidate if candidate != name and candidate in self.books else None
            )
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
                'arm_id': book.profile.arm_id,
                'entry_band_label': book.profile.entry_band_label,
                'exit_rule': self._exit_rule_label(book.profile),
                'stop_loss_pct': book.profile.stop_loss_pct,
                'tp1_pct': book.profile.tp1_pct,
                'delta_max_sol': book.profile.delta_max_sol,
                'delta_min_sol': book.profile.delta_min_sol,
                'v_sol_birth_max': book.profile.v_sol_birth_max,
                # The two counters that tell a rule failure from a capacity cap.
                'slots_full': book.slots_full_count,
                'cash_below_slot': book.cash_below_slot_count,
                # Run #9 §3: the funnel of THIS book, counted where the
                # decision is made, so a lost telemetry row cannot hide it.
                'reject_counts': {
                    'name_copies': book.name_copies_count,
                    'birth_band': book.birth_band_count,
                    'delta_min': book.delta_min_count,
                    'delta_max': book.delta_max_count,
                    'slots_full': book.slots_full_count,
                    'no_price': book.no_price_count,
                },
                'max_name_copies_120s': book.profile.max_name_copies_120s,
                'v_sol_birth_min': book.profile.v_sol_birth_min,
                # --- run #3 addendum: bank metrics ---
                'top_ups': book.top_ups,
                'capital_injected_rub': round(book.capital_injected_rub, 6),
                'pnl_with_topups': round(book.realized_pnl_rub, 6),
                'equity_no_topups': round(book.equity_no_topups(), 6),
                'bust_at_trade': book.bust_at_trade,
                'bust_at_utc': book.bust_at_utc,
                'bust_equity_rub': book.bust_equity_rub,
                'top_up_events': book.top_up_events,
                'stats': trade_stats([t.get('pnl_rub', 0.0) for t in book.closed_trades]),
                # --- run #4: the number the run is about (acceptance #1, #4) ---
                'entry_quality': self._entry_quality(book),
                # --- run #8 §3: the five required blocks ---
                'latency_chain': self._latency_chain(book),
                'hold_replay': self._hold_replay(book),
                'round_trip_cost': self._round_trip_cost(book),
                'gate_price': self._gate_price(book),
                'exit_shares': self._exit_shares(book),
                # P0-4: what closed the trade, not just the total.
                'exit_breakdown': self._exit_breakdown(book),
                # Run #7 (§6): paired test vs the arm's own reference, hourly
                # noise floor, the first/second-half sign, and how much rides
                # on rockets. In run #8 the reference of S1 (A) is absent, so
                # the value is None instead of silently pairing with S0.
                'paired_vs_a': (
                    {'reference': True} if reference_name == name
                    else (
                        paired_stats(
                            portfolios[reference_for[name]]['closed_trades'],
                            book.closed_trades,
                        ) if reference_for[name] else None
                    )
                ),
                'paired_against': reference_for[name],
                'blocks': hourly_blocks(book.closed_trades, self.started_at_epoch),
                'stability': (
                    {'reference': True} if reference_name == name
                    else (
                        stability_split(
                            portfolios[reference_for[name]]['closed_trades'],
                            book.closed_trades,
                            self.started_at_epoch,
                            (self.ended_at_epoch or time.time()) - self.started_at_epoch,
                        ) if reference_for[name] else None
                    )
                ),
                'rocket_accounting': rocket_accounting(
                    book.closed_trades, peaks=peaks,
                ),
                # --- run #9 §2.3: the ghost, per book (the report reads here) --
                'post_exit': (post_exit_stats.get('per_book') or {}).get(name),
                # --- run #9 §5(4)-(5) ---
                'minute_pairs_vs_reference': (
                    minute_pairs(
                        portfolios[reference_for[name]]['closed_trades'],
                        book.closed_trades,
                    )
                    if reference_for.get(name) and reference_for[name] != name
                    else None
                ),
                'selection_decomposition': selection_decomposition(book.closed_trades),
            }
        return {
            'run_id': self.run_id,
            'started_at_epoch': self.started_at_epoch,
            'ended_at_epoch': self.ended_at_epoch or time.time(),
            'duration_sec': round((self.ended_at_epoch or time.time()) - self.started_at_epoch, 3),
            'paper_only': True,
            'signed_transactions': 0,
            'submitted_transactions': 0,
            'portfolio_scope': list(self.books),
            'arms': [book.profile.arm_id for book in self.books.values()],
            'early_stop_reason': self.early_stop_reason,
            # Run #7 §1.4: the fly-through of the SL rule, mark to mark.
            'tick_quality': self.tick_quality(),
            'mark_poll': {
                'mark_poll_sec': round(self.mark_poll_sec, 4),
                'mark_poll_adapted': self.mark_poll_adapted,
                'adapted_from_sec': MARK_POLL_SEC_DEFAULT if self.mark_poll_adapted else None,
                'adapted_to_sec': MARK_POLL_ADAPTED_SEC if self.mark_poll_adapted else None,
                'rps_limit_10min': self.mark_poll_rps_limit,
                'adaptation_blocked': self.mark_poll_adaptation_blocked,
                'rps_seen_at_last_check': (
                    round(self.mark_poll_rps_at_check, 3)
                    if self.mark_poll_rps_at_check is not None else None
                ),
            },
            'stop_policy': {
                **self.stop_policy_state,
                'thresholds': {
                    'rpc_error_rate': STOP_RPC_ERROR_RATE,
                    'rpc_window_sec': STOP_RPC_WINDOW_SEC,
                    'rpc_latency_ms': STOP_RPC_LATENCY_MS,
                    'rpc_latency_hold_sec': STOP_RPC_LATENCY_HOLD_SEC,
                    'stall_sec': STOP_STALL_SEC,
                    'disk_free_gb': round(STOP_DISK_FREE_BYTES / 1024 ** 3, 2),
                    'helius_quota_pct': STOP_HELIUS_QUOTA_PCT,
                },
                'pnl_never_stops_the_run': True,
            },
            'profiles': {name: {
                # Run #7: four arms, each with the ONE boundary it moves.
                'arm_id': book.profile.arm_id,
                'entry_band_label': book.profile.entry_band_label,
                'v_sol_birth_min': book.profile.v_sol_birth_min,
                'v_sol_birth_max': book.profile.v_sol_birth_max,
                'delta_min_sol': book.profile.delta_min_sol,
                'delta_max_sol': book.profile.delta_max_sol,
                'max_name_copies_120s': book.profile.max_name_copies_120s,
                'name_copies_window_sec': book.profile.name_copies_window_sec,
                'reject_counts': portfolios[name].get('reject_counts'),
                'post_exit': (post_exit_stats.get('per_book') or {}).get(name),
                'entry_latency_ms': book.profile.entry_latency_ms,
                'exit_rule': self._exit_rule_label(book.profile),
                'stop_loss_pct': book.profile.stop_loss_pct,
                'tp1_pct': book.profile.tp1_pct,
                'tp1_sell_pct': book.profile.tp1_sell_pct,
                'max_slots': book.profile.max_slots,
                'capital_rub': book.profile.capital_rub,
                'entry_quality': portfolios[name].get('entry_quality'),
                'exit_breakdown': portfolios[name].get('exit_breakdown'),
                'paired_vs_a': portfolios[name].get('paired_vs_a'),
                'blocks': portfolios[name].get('blocks'),
                'stability': portfolios[name].get('stability'),
                'rocket_accounting': portfolios[name].get('rocket_accounting'),
            } for name, book in self.books.items()},
            'fee_model': 'official @pump-fun/pump-sdk 4.1.0 exact integer curve quote; out_amount_raw is the snapshot fill; min_out_raw is the 100 bps conservative shadow; on-chain Global + FeeConfig + per-curve creator_fee_bps; fail-closed when unsupported',
            'jupiter_policy': 'GET ExactIn fallback only after on-chain complete=true; no active-curve entry/exit Jupiter quotes',
            **portfolios,
            'ab_test': {
                # Run #7: every arm moves exactly ONE entry boundary of A.
                'single_difference': 'entry_band',
                'reference_arm': reference_name,
                'arm_reference': dict(ARM_REFERENCE),
                'entry_latency_ms': float(
                    next(iter(self.books.values())).profile.entry_latency_ms
                ),
                'decision_rule': (
                    'an arm confirms its hypothesis iff the PAIRED mean(arm - A) '
                    '>= +8 RUB on the coins both arms traded, the 98.75% bootstrap '
                    'CI (Bonferroni over three hypotheses, alpha 0.0125) excludes '
                    '0, and the sign is the same in both halves of the run'
                ),
                'arms': {
                    name: {
                        'arm_id': book.profile.arm_id,
                        'entry_band_label': book.profile.entry_band_label,
                        # Run #8 has no A: S0 is compared with S1 instead, and
                        # S1 has no reference at all in that run.
                        'single_boundary_vs_a': (
                            self._changed_boundaries(
                                self.books[reference_for[name]].profile, book.profile,
                            ) if reference_for.get(name) else ['none']
                        ),
                        'compared_with': reference_for.get(name),
                        'exit_rule': self._exit_rule_label(book.profile),
                    }
                    for name, book in self.books.items()
                },
                'expectation': {
                    'portfolio_b1_narrow': '+1..+6 RUB/trade vs A (better in 3/3 '
                                           'runs on a fixed exit, 2/2 on the ladder)',
                    'portfolio_b2_birth': '-8..+8 RUB (model better in 2/3, the '
                                          'run #6 ladder lost 15.7 RUB/trade)',
                    'portfolio_b3_t0': '+3..+9 RUB from removing the 1.416 % entry '
                                       'premium; EV itself is neutral',
                },
                'power_note': (
                    'paired sd ~98 RUB and ~56 trades/hour per arm: the CI half '
                    'width is ~15 RUB at ~165 pairs, so a 3-hour run cannot '
                    'resolve anything below ~15 RUB/trade, and an 8 RUB effect '
                    'needs ~1 700 pairs after Bonferroni (~21 h at this flow)'
                ),
            },
            'enrich': self.enrich.stats(),
            'rpc_metrics': {
                **rpc_metrics_snapshot(),
                'requests_per_profile': dict(sorted(self.rpc_reads_by_profile.items())),
            },
            # --- run #8 §3(7): the one line a reviewer reads first ---
            'run_line': self._run_line(rpc_metrics_snapshot()),
            'post_exit': post_exit_stats,
            'drain': {
                'max_wait_sec': DRAIN_OPEN_POSITIONS_MAX_SEC,
                'waited_sec': round(self.drain_waited_sec, 3),
                'left_open': self.drain_left_open,
                # Run #9 §2.3: the ghosts get their own bounded drain, so an
                # exit in the last seconds of the run still reaches +10 s.
                'ghost_waited_sec': round(self.ghost_drain_waited_sec, 3),
                'ghosts_left_open': self.ghost_drain_left_open,
            },
        }
