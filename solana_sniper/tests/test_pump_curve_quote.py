from __future__ import annotations

import struct
import unittest

from pump_curve_quote import (
    BONDING_CURVE_DISCRIMINATOR,
    FEE_CONFIG_DISCRIMINATOR,
    GLOBAL_DISCRIMINATOR,
    PUMP_FEE_CONFIG_PDA,
    PUMP_GLOBAL_PDA,
    QuoteUnavailable,
    CurveState,
    decode_fee_config_account,
    make_fee_context,
    quote_buy_exact_quote_in,
    quote_sell_exact_tokens,
)


def make_global(*, configurable: bool = False, max_creator_bps: int = 1_000) -> bytes:
    raw = bytearray(1_088)
    raw[:8] = GLOBAL_DISCRIMINATOR
    struct.pack_into('<Q', raw, 105, 95)
    struct.pack_into('<Q', raw, 154, 30)
    raw[1045] = int(configurable)
    struct.pack_into('<Q', raw, 1046, max_creator_bps)
    return bytes(raw)


def make_fee_config() -> bytes:
    raw = bytearray()
    raw.extend(FEE_CONFIG_DISCRIMINATOR)
    raw.extend(b'\x01')  # bump
    raw.extend(bytes(32))  # admin
    raw.extend(struct.pack('<QQQ', 0, 95, 30))  # flat_fees (fallback)
    raw.extend(struct.pack('<I', 2))  # fee_tiers length
    raw.extend((0).to_bytes(16, 'little'))
    raw.extend(struct.pack('<QQQ', 0, 95, 30))
    raw.extend((40_000_000_000).to_bytes(16, 'little'))
    raw.extend(struct.pack('<QQQ', 0, 120, 45))
    raw.extend(struct.pack('<I', 0))  # stable_fee_tiers length
    raw.extend(struct.pack('<QQQ', 0, 0, 0))  # exotic_flat_fees
    return bytes(raw)


def make_curve(
    *,
    complete: bool = False,
    mayhem: bool = False,
    quote_mint: bytes = bytes(32),
    creator: bytes = bytes([7]) * 32,
    creator_fee_bps: int = 0,
    real_token_reserves: int = 793_100_000_000_000,
) -> CurveState:
    raw = bytearray(166)
    raw[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into('<QQQQQ', raw, 8,
                     1_073_000_000_000_000, 30_000_000_000,
                     real_token_reserves, 0, 1_000_000_000_000_000)
    raw[48] = int(complete)
    raw[49:81] = creator
    raw[81] = int(mayhem)
    raw[83:115] = quote_mint
    struct.pack_into('<Q', raw, 115, creator_fee_bps)
    return CurveState.decode(bytes(raw))


class PumpCurveQuoteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = make_fee_context(make_global(), make_fee_config(), context_slot=123)
        self.curve = make_curve()

    def test_pdas_are_derived_and_nonempty(self) -> None:
        self.assertEqual(PUMP_GLOBAL_PDA, '4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf')
        self.assertEqual(PUMP_FEE_CONFIG_PDA, '8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt')

    def test_fee_config_borsh_vectors_decode(self) -> None:
        flat, tiers, stable, exotic = decode_fee_config_account(make_fee_config())
        self.assertEqual(flat, (0, 95, 30))
        self.assertEqual(len(tiers), 2)
        self.assertEqual(tiers[0].protocol_fee_bps, 95)
        self.assertEqual(tiers[1].market_cap_lamports_threshold, 40_000_000_000)
        self.assertEqual(stable, ())
        self.assertEqual(exotic, (0, 0, 0))

    def test_exact_buy_quote_matches_sdk_integer_rounding_vector(self) -> None:
        quote = quote_buy_exact_quote_in(self.curve, self.context, 28_000_000, 100, 123)
        self.assertEqual(quote.protocol_fee_bps, 95)
        self.assertEqual(quote.creator_fee_bps, 30)
        self.assertEqual(quote.net_quote_raw, 27_654_320)
        self.assertEqual(quote.protocol_fee_raw, 262_717)
        self.assertEqual(quote.creator_fee_raw, 82_963)
        self.assertEqual(quote.gross_quote_raw, 28_000_000)
        self.assertEqual(quote.out_amount_raw, 988_191_883_780)
        self.assertEqual(quote.min_out_raw, 978_309_964_942)
        self.assertEqual(quote.context_slot, 123)

    def test_exact_sell_quote_uses_per_component_ceil_fees(self) -> None:
        quote = quote_sell_exact_tokens(self.curve, self.context, 494_095_941_890, 100)
        self.assertEqual(quote.gross_quote_raw, 13_808_066)
        self.assertEqual(quote.protocol_fee_raw, 131_177)
        self.assertEqual(quote.creator_fee_raw, 41_425)
        self.assertEqual(quote.out_amount_raw, 13_635_464)
        self.assertEqual(quote.min_out_raw, 13_499_109)

    def test_tier_and_creator_fee_override_follow_global_gate(self) -> None:
        high_cap_curve = make_curve()
        high = make_fee_context(make_global(), make_fee_config(), context_slot=1)
        # A virtual cap above the second market-cap boundary selects tier 1.
        raw = bytearray(166)
        raw[:8] = BONDING_CURVE_DISCRIMINATOR
        struct.pack_into('<QQQQQ', raw, 8,
                         1_000_000_000_000_000, 50_000_000_000,
                         793_100_000_000_000, 0, 1_000_000_000_000_000)
        raw[49:81] = bytes([7]) * 32
        raw[83:115] = bytes(32)
        tiered_curve = CurveState.decode(bytes(raw))
        quote = quote_buy_exact_quote_in(tiered_curve, high, 28_000_000)
        self.assertEqual(quote.tier_index, 1)
        self.assertEqual(quote.protocol_fee_bps, 120)
        self.assertEqual(quote.creator_fee_bps, 45)

        configurable = make_fee_context(
            make_global(configurable=True, max_creator_bps=100),
            make_fee_config(), context_slot=2,
        )
        custom_curve = make_curve(creator_fee_bps=50)
        custom_quote = quote_buy_exact_quote_in(custom_curve, configurable, 28_000_000)
        self.assertEqual(custom_quote.creator_fee_bps, 50)

        gated_off_quote = quote_buy_exact_quote_in(custom_curve, high, 28_000_000)
        self.assertEqual(gated_off_quote.creator_fee_bps, 30)

    def test_fail_closed_for_unknown_or_unsupported_state(self) -> None:
        with self.assertRaises(QuoteUnavailable):
            CurveState.decode(b'\x00' * 166)
        with self.assertRaises(QuoteUnavailable):
            quote_buy_exact_quote_in(make_curve(complete=True), self.context, 28_000_000)
        with self.assertRaises(QuoteUnavailable):
            quote_sell_exact_tokens(make_curve(mayhem=True), self.context, 1_000_000)
        with self.assertRaises(QuoteUnavailable):
            quote_buy_exact_quote_in(
                make_curve(real_token_reserves=1), self.context, 28_000_000,
            )


if __name__ == '__main__':
    unittest.main()
