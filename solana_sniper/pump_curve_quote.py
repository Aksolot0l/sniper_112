"""Exact integer Pump.fun bonding-curve quote math, ported from the official
@pump-fun/pump-sdk 4.1.0 `bondingCurve.ts` and `fees.ts`.

This module is deliberately read-only. It decodes the current Global, FeeConfig,
and BondingCurve accounts and computes only the active, non-Mayhem, SOL/WSOL
bonding-curve leg. It fails closed for unknown layouts, unsupported quote mints,
missing fee state, completed curves, or a buy that would cross migration.

No Jupiter quote is used here. A caller may use Jupiter only after an on-chain
read confirms `complete=true`.
"""
from __future__ import annotations

import base64
import struct
import time
from dataclasses import dataclass
from typing import Any, Optional

from pumpportal_validation import (
    PUMP_PROGRAM_ID,
    WSOL_MINT,
    b58decode,
    b58encode,
    find_program_address,
)

PUMP_FEE_PROGRAM_ID = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'
GLOBAL_DISCRIMINATOR = bytes([167, 232, 232, 177, 200, 108, 114, 127])
BONDING_CURVE_DISCRIMINATOR = bytes([23, 183, 248, 55, 96, 216, 172, 96])
FEE_CONFIG_DISCRIMINATOR = bytes([143, 52, 146, 187, 219, 123, 76, 155])
LAMPORTS_PER_SOL = 1_000_000_000
ONE_BILLION_SUPPLY_RAW = 1_000_000_000_000_000  # 1,000,000,000 tokens at 6 decimals
BPS_DENOMINATOR = 10_000

PUMP_GLOBAL_PDA = find_program_address([b'global'], PUMP_PROGRAM_ID)
_pump_program_bytes = b58decode(PUMP_PROGRAM_ID)
PUMP_FEE_CONFIG_PDA = (
    find_program_address([b'fee_config', _pump_program_bytes], PUMP_FEE_PROGRAM_ID)
    if _pump_program_bytes is not None else None
)


class QuoteUnavailable(ValueError):
    """A quote cannot be represented exactly from the observed state."""


@dataclass(frozen=True)
class CurveState:
    virtual_token_reserves: int
    virtual_quote_reserves: int
    real_token_reserves: int
    real_quote_reserves: int
    token_total_supply: int
    complete: bool
    creator: bytes
    is_mayhem_mode: bool
    quote_mint_bytes: bytes
    creator_fee_bps: int
    raw_length: int

    @property
    def quote_mint(self) -> str:
        return b58encode(self.quote_mint_bytes) if any(self.quote_mint_bytes) else '11111111111111111111111111111111'

    @property
    def has_creator(self) -> bool:
        return any(self.creator)

    @classmethod
    def decode(cls, raw: bytes) -> 'CurveState':
        if not isinstance(raw, (bytes, bytearray)) or len(raw) < 166:
            raise QuoteUnavailable('unsupported_bonding_curve_layout')
        raw = bytes(raw)
        if raw[:8] != BONDING_CURVE_DISCRIMINATOR:
            raise QuoteUnavailable('bonding_curve_discriminator_mismatch')
        if raw[48] not in (0, 1) or raw[81] not in (0, 1):
            raise QuoteUnavailable('invalid_bonding_curve_boolean')
        vtok, vquote, rtok, rquote, supply = struct.unpack_from('<QQQQQ', raw, 8)
        creator = raw[49:81]
        quote_mint = raw[83:115]
        creator_fee_bps = struct.unpack_from('<Q', raw, 115)[0]
        if len(creator) != 32 or len(quote_mint) != 32:
            raise QuoteUnavailable('invalid_bonding_curve_pubkey_layout')
        complete = bool(raw[48])
        if supply <= 0 or (not complete and (vtok <= 0 or vquote <= 0)):
            raise QuoteUnavailable('empty_or_invalid_curve_reserves')
        return cls(
            virtual_token_reserves=vtok,
            virtual_quote_reserves=vquote,
            real_token_reserves=rtok,
            real_quote_reserves=rquote,
            token_total_supply=supply,
            complete=bool(raw[48]),
            creator=creator,
            is_mayhem_mode=bool(raw[81]),
            quote_mint_bytes=quote_mint,
            creator_fee_bps=creator_fee_bps,
            raw_length=len(raw),
        )


@dataclass(frozen=True)
class FeeTier:
    market_cap_lamports_threshold: int
    lp_fee_bps: int
    protocol_fee_bps: int
    creator_fee_bps: int


@dataclass(frozen=True)
class FeeContext:
    global_fee_basis_points: int
    global_creator_fee_basis_points: int
    creator_fee_configurable: bool
    max_configurable_creator_fee_bps: int
    fee_tiers: tuple[FeeTier, ...]
    flat_fees: tuple[int, int, int]
    stable_fee_tiers: tuple[FeeTier, ...]
    exotic_flat_fees: tuple[int, int, int]
    global_account: str
    fee_config_account: str
    observed_at_epoch: float
    context_slot: Optional[int] = None

    def manifest(self) -> dict[str, Any]:
        return {
            'source': 'on_chain_Global_and_FeeConfig',
            'global_account': self.global_account,
            'fee_config_account': self.fee_config_account,
            'observed_at_epoch': self.observed_at_epoch,
            'context_slot': self.context_slot,
            'global_fee_basis_points': self.global_fee_basis_points,
            'global_creator_fee_basis_points': self.global_creator_fee_basis_points,
            'creator_fee_configurable': self.creator_fee_configurable,
            'max_configurable_creator_fee_bps': self.max_configurable_creator_fee_bps,
            'fee_tiers': [
                {
                    'market_cap_lamports_threshold': tier.market_cap_lamports_threshold,
                    'lp_fee_bps': tier.lp_fee_bps,
                    'protocol_fee_bps': tier.protocol_fee_bps,
                    'creator_fee_bps': tier.creator_fee_bps,
                }
                for tier in self.fee_tiers
            ],
            'note': 'Pump SDK 4.1.0 curve quotes charge protocol+creator fees; LP fee is not charged on the bonding-curve leg.',
        }


def _read_u64(raw: bytes, offset: int) -> int:
    if offset < 0 or offset + 8 > len(raw):
        raise QuoteUnavailable('truncated_account_layout')
    return struct.unpack_from('<Q', raw, offset)[0]


def _read_fees(raw: bytes, offset: int) -> tuple[int, int, int]:
    if offset < 0 or offset + 24 > len(raw):
        raise QuoteUnavailable('truncated_fee_config_fees')
    return struct.unpack_from('<QQQ', raw, offset)


def _read_fee_tiers(raw: bytes, offset: int) -> tuple[tuple[FeeTier, ...], int]:
    if offset < 0 or offset + 4 > len(raw):
        raise QuoteUnavailable('truncated_fee_config_vector')
    count = struct.unpack_from('<I', raw, offset)[0]
    offset += 4
    # Avoid trusting a corrupt account's vector length or doing unbounded work.
    if count > 256 or offset + count * 40 > len(raw):
        raise QuoteUnavailable('invalid_fee_tier_vector')
    tiers: list[FeeTier] = []
    for _ in range(count):
        threshold = int.from_bytes(raw[offset:offset + 16], 'little', signed=False)
        lp_bps, protocol_bps, creator_bps = struct.unpack_from('<QQQ', raw, offset + 16)
        tiers.append(FeeTier(threshold, lp_bps, protocol_bps, creator_bps))
        offset += 40
    if any(
        tier.protocol_fee_bps > 100_000 or tier.creator_fee_bps > 100_000
        or tier.lp_fee_bps > 100_000 for tier in tiers
    ):
        raise QuoteUnavailable('fee_tier_rate_out_of_range')
    return tuple(tiers), offset


def decode_global_account(raw: bytes) -> tuple[int, int, bool, int]:
    """Return Global legacy bps and creator-fee configurability fields."""
    if len(raw) < 1054 or raw[:8] != GLOBAL_DISCRIMINATOR:
        raise QuoteUnavailable('global_account_layout_or_discriminator_mismatch')
    if raw[1045] not in (0, 1):
        raise QuoteUnavailable('invalid_global_creator_fee_configurable')
    return (
        _read_u64(raw, 105),
        _read_u64(raw, 154),
        bool(raw[1045]),
        _read_u64(raw, 1046),
    )


def decode_fee_config_account(raw: bytes) -> tuple[
    tuple[int, int, int], tuple[FeeTier, ...], tuple[FeeTier, ...], tuple[int, int, int]
]:
    """Borsh-decode the FeeConfig account layout from the official Pump IDL."""
    if len(raw) < 69 or raw[:8] != FEE_CONFIG_DISCRIMINATOR:
        raise QuoteUnavailable('fee_config_layout_or_discriminator_mismatch')
    # discriminator(8), bump(1), admin(32), then flat_fees(3*u64)
    flat_fees = _read_fees(raw, 41)
    fee_tiers, offset = _read_fee_tiers(raw, 65)
    stable_fee_tiers, offset = _read_fee_tiers(raw, offset)
    exotic_flat_fees = _read_fees(raw, offset)
    if not fee_tiers:
        raise QuoteUnavailable('empty_on_chain_fee_tier_schedule')
    return flat_fees, fee_tiers, stable_fee_tiers, exotic_flat_fees


def decode_account_data(account: Optional[dict[str, Any]]) -> Optional[bytes]:
    if not isinstance(account, dict):
        return None
    encoded = account.get('data')
    if isinstance(encoded, list) and len(encoded) >= 1 and isinstance(encoded[0], str):
        try:
            return base64.b64decode(encoded[0], validate=True)
        except (ValueError, TypeError):
            return None
    return None


def make_fee_context(
    global_raw: bytes,
    fee_config_raw: bytes,
    context_slot: Optional[int] = None,
    observed_at_epoch: Optional[float] = None,
) -> FeeContext:
    global_protocol, global_creator, configurable, max_creator = decode_global_account(global_raw)
    flat, tiers, stable_tiers, exotic = decode_fee_config_account(fee_config_raw)
    if max_creator > 100_000:
        raise QuoteUnavailable('global_creator_fee_limit_out_of_range')
    return FeeContext(
        global_fee_basis_points=global_protocol,
        global_creator_fee_basis_points=global_creator,
        creator_fee_configurable=configurable,
        max_configurable_creator_fee_bps=max_creator,
        fee_tiers=tiers,
        flat_fees=flat,
        stable_fee_tiers=stable_tiers,
        exotic_flat_fees=exotic,
        global_account=PUMP_GLOBAL_PDA or '',
        fee_config_account=PUMP_FEE_CONFIG_PDA or '',
        observed_at_epoch=float(observed_at_epoch or time.time()),
        context_slot=context_slot,
    )


def curve_fee_rates(curve: CurveState, fees: FeeContext) -> tuple[int, int, int, int]:
    """Return protocol bps, creator bps, market-cap lamports, and selected tier."""
    zero_key = bytes(32)
    wsol = b58decode(WSOL_MINT)
    if curve.quote_mint_bytes not in (zero_key, wsol):
        raise QuoteUnavailable('unsupported_non_sol_curve_quote_mint')
    market_cap = (
        curve.virtual_quote_reserves * ONE_BILLION_SUPPLY_RAW
        // curve.virtual_token_reserves
    )
    tiers = fees.fee_tiers
    if not tiers:
        raise QuoteUnavailable('missing_fee_tier_schedule')
    selected_index = 0
    if market_cap >= tiers[0].market_cap_lamports_threshold:
        for index in range(len(tiers) - 1, -1, -1):
            if market_cap >= tiers[index].market_cap_lamports_threshold:
                selected_index = index
                break
    tier = tiers[selected_index]
    protocol_bps = tier.protocol_fee_bps
    creator_bps = tier.creator_fee_bps
    if (
        fees.creator_fee_configurable
        and curve.creator_fee_bps > 0
    ):
        if curve.creator_fee_bps > fees.max_configurable_creator_fee_bps:
            raise QuoteUnavailable('curve_creator_fee_exceeds_global_limit')
        creator_bps = curve.creator_fee_bps
    if not curve.has_creator:
        creator_bps = 0
    return protocol_bps, creator_bps, market_cap, selected_index


def _ceil_fee(amount: int, bps: int) -> int:
    if amount < 0 or bps < 0:
        raise QuoteUnavailable('negative_fee_input')
    if bps == 0 or amount == 0:
        return 0
    return (amount * bps + BPS_DENOMINATOR - 1) // BPS_DENOMINATOR


def _v3_exact_quote_in_net(budget: int, protocol_bps: int, creator_bps: int) -> tuple[int, int, int]:
    if budget <= 0:
        return 0, 0, 0
    total_bps = protocol_bps + creator_bps
    net = budget if total_bps == 0 else budget * BPS_DENOMINATOR // (BPS_DENOMINATOR + total_bps)
    protocol_fee = _ceil_fee(net, protocol_bps)
    creator_fee = _ceil_fee(net, creator_bps)
    cost = net + protocol_fee + creator_fee
    if cost > budget:
        net -= cost - budget
        protocol_fee = _ceil_fee(net, protocol_bps)
        creator_fee = _ceil_fee(net, creator_bps)
    return net, protocol_fee, creator_fee


def _min_out(out_amount: int, slippage_bps: int) -> int:
    if slippage_bps < 0 or slippage_bps >= BPS_DENOMINATOR:
        raise QuoteUnavailable('invalid_slippage_bps')
    return out_amount * (BPS_DENOMINATOR - slippage_bps) // BPS_DENOMINATOR


@dataclass(frozen=True)
class CurveQuote:
    side: str
    in_amount_raw: int
    out_amount_raw: int
    min_out_raw: int
    gross_quote_raw: int
    net_quote_raw: int
    protocol_fee_raw: int
    creator_fee_raw: int
    protocol_fee_bps: int
    creator_fee_bps: int
    market_cap_lamports: int
    tier_index: int
    price_impact_pct: float
    slippage_bps: int
    context_slot: Optional[int] = None

    def route_labels(self) -> tuple[str, ...]:
        return (
            'Pump.fun bonding curve; official @pump-fun/pump-sdk 4.1.0 integer math',
            f'protocol_fee_bps={self.protocol_fee_bps}',
            f'creator_fee_bps={self.creator_fee_bps}',
            f'fee_tier_index={self.tier_index}',
        )


def quote_buy_exact_quote_in(
    curve: CurveState,
    fee_context: FeeContext,
    amount_quote_raw: int,
    slippage_bps: int = 100,
    context_slot: Optional[int] = None,
) -> CurveQuote:
    """Mirror SDK `getBuyV3TokenAmountFromQuoteAmount` on a non-crossing curve."""
    if amount_quote_raw <= 0:
        raise QuoteUnavailable('invalid_buy_budget')
    if curve.complete:
        raise QuoteUnavailable('curve_complete_use_migrated_fallback')
    if curve.is_mayhem_mode:
        raise QuoteUnavailable('mayhem_curve_unsupported')
    protocol_bps, creator_bps, market_cap, tier_index = curve_fee_rates(curve, fee_context)
    net, protocol_fee, creator_fee = _v3_exact_quote_in_net(
        amount_quote_raw, protocol_bps, creator_bps,
    )
    if net <= 1:
        raise QuoteUnavailable('buy_net_quote_too_small')
    input_to_curve = net - 1
    tokens = input_to_curve * curve.virtual_token_reserves // (
        curve.virtual_quote_reserves + input_to_curve
    )
    if tokens <= 0:
        raise QuoteUnavailable('buy_quote_zero_token_output')
    if tokens > curve.real_token_reserves:
        # Crossing graduation needs the SDK's synthetic post-migration pool
        # state and curve-base ATA balance. A/B entry gates do not need it; do
        # not invent a curve-only fill if an unexpected state reaches here.
        raise QuoteUnavailable('buy_would_cross_curve_migration')
    gross_quote = net + protocol_fee + creator_fee
    average_price = amount_quote_raw / tokens
    marginal_price = curve.virtual_quote_reserves / curve.virtual_token_reserves
    impact = max(0.0, (average_price / marginal_price - 1.0) * 100.0) if marginal_price > 0 else 0.0
    return CurveQuote(
        side='buy', in_amount_raw=amount_quote_raw, out_amount_raw=tokens,
        min_out_raw=_min_out(tokens, slippage_bps), gross_quote_raw=gross_quote,
        net_quote_raw=net, protocol_fee_raw=protocol_fee, creator_fee_raw=creator_fee,
        protocol_fee_bps=protocol_bps, creator_fee_bps=creator_bps,
        market_cap_lamports=market_cap, tier_index=tier_index,
        price_impact_pct=impact, slippage_bps=slippage_bps,
        context_slot=context_slot,
    )


def quote_sell_exact_tokens(
    curve: CurveState,
    fee_context: FeeContext,
    amount_tokens_raw: int,
    slippage_bps: int = 100,
    context_slot: Optional[int] = None,
) -> CurveQuote:
    """Mirror SDK sell gross-quote floor and per-component ceiling fees."""
    if amount_tokens_raw <= 0:
        raise QuoteUnavailable('invalid_sell_amount')
    if curve.complete:
        raise QuoteUnavailable('curve_complete_use_migrated_fallback')
    if curve.is_mayhem_mode:
        raise QuoteUnavailable('mayhem_curve_unsupported')
    protocol_bps, creator_bps, market_cap, tier_index = curve_fee_rates(curve, fee_context)
    gross = amount_tokens_raw * curve.virtual_quote_reserves // (
        curve.virtual_token_reserves + amount_tokens_raw
    )
    if gross <= 0:
        raise QuoteUnavailable('sell_quote_zero_gross_output')
    protocol_fee = _ceil_fee(gross, protocol_bps)
    creator_fee = _ceil_fee(gross, creator_bps)
    net = gross - protocol_fee - creator_fee
    if net <= 0:
        raise QuoteUnavailable('sell_quote_zero_net_output')
    average_price = net / amount_tokens_raw
    marginal_price = curve.virtual_quote_reserves / curve.virtual_token_reserves
    impact = max(0.0, (1.0 - average_price / marginal_price) * 100.0) if marginal_price > 0 else 0.0
    return CurveQuote(
        side='sell', in_amount_raw=amount_tokens_raw, out_amount_raw=net,
        min_out_raw=_min_out(net, slippage_bps), gross_quote_raw=gross,
        net_quote_raw=net, protocol_fee_raw=protocol_fee, creator_fee_raw=creator_fee,
        protocol_fee_bps=protocol_bps, creator_fee_bps=creator_bps,
        market_cap_lamports=market_cap, tier_index=tier_index,
        price_impact_pct=impact, slippage_bps=slippage_bps,
        context_slot=context_slot,
    )
