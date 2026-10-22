from __future__ import annotations

import base64
import json
import struct
import unittest
from unittest.mock import patch

from detector import on_transaction_signal
from filters import check_pump_curve, check_pump_supply, fetch_pump_mint_curve_accounts, run_all_checks
from models import NewCoin
from pumpportal_validation import (
    PUMP_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    WSOL_MINT,
    b58decode,
    b58encode,
    derive_pump_bonding_curve,
    validate_pumpportal_create,
)
from pump_curve_quote import (
    BONDING_CURVE_DISCRIMINATOR,
    PUMP_FEE_CONFIG_PDA,
    PUMP_FEE_PROGRAM_ID,
    PUMP_GLOBAL_PDA,
)


VALID_CREATE = {
    'signature': '2rUMkSY4WC1LXTqkj1Xzbe5LLKBkYd2hydGyYnjeKiWbsRKicPWnUDgdxEkdRQi5VRoqErizS5YvjW6vUoWnc5vB',
    'mint': 'onQyCAYjDfNfhALuDwDtip3Xmi6cdCqPAi2JGwYpump',
    'traderPublicKey': '2MRQDEBy1XfwvgwL8GbxXsD5wPq4gncn1YpsUKkosUFk',
    'txType': 'create',
    'initialBuy': 97545454.545454,
    'solAmount': 3,
    'bondingCurveKey': 'HSAcqKvSEFo7sHWgim7bRheRNrpLrJsMmwuQR75BCNQx',
    'vTokensInBondingCurve': 975454545.454546,
    'vSolInBondingCurve': 32.999999999999986,
    'marketCapSol': 33.830382106244144,
    'name': 'Dawkoin',
    'symbol': 'DAWKOIN',
}


class FakeResponse:
    def __init__(self, data: dict, status: int = 200) -> None:
        self.data = data
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def json(self):
        return self.data


class FakePostSession:
    def __init__(self, data: dict) -> None:
        self.data = data
        self.calls = 0
        self.payloads = []

    def post(self, url: str, **kwargs):
        self.calls += 1
        self.payloads.append(kwargs.get('json'))
        return FakeResponse(self.data)


def curve_bytes(
    quote_mint: str | None = None,
    *,
    token_total_supply: int = 1_000_000_000_000_000,
    mayhem: bool = False,
) -> bytes:
    raw = bytearray(166)
    raw[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into('<QQ', raw, 8, 975_454_545, 33_000_000_000)
    struct.pack_into('<Q', raw, 32, 3_000_000_000)
    struct.pack_into('<Q', raw, 40, token_total_supply)
    creator = b58decode('32CfGsZjrkHJukZuh97YN7t2eh8EmcuLkGuEzEYPBeRm')
    assert creator is not None and len(creator) == 32
    raw[49:81] = creator
    raw[48] = 0
    raw[81] = int(mayhem)
    if quote_mint:
        quote_bytes = b58decode(quote_mint)
        assert quote_bytes is not None and len(quote_bytes) == 32
        raw[83:115] = quote_bytes
    return bytes(raw)


def pump_accounts_data(meta: dict, raw: bytes, slot: int = 12345) -> dict:
    global_raw = b'global-fixture-bytes'
    fee_config_raw = b'fee-config-fixture-bytes'
    return {
        'result': {
            'context': {'slot': slot},
            'value': [
                {'owner': TOKEN_PROGRAM_ID, 'data': {'parsed': {'info': meta}}},
                {'owner': PUMP_PROGRAM_ID, 'data': [base64.b64encode(raw).decode('ascii'), 'base64']},
                {'owner': PUMP_PROGRAM_ID, 'data': [base64.b64encode(global_raw).decode('ascii'), 'base64']},
                {'owner': PUMP_FEE_PROGRAM_ID, 'data': [base64.b64encode(fee_config_raw).decode('ascii'), 'base64']},
            ],
        },
    }


class PumpPortalValidationTests(unittest.IsolatedAsyncioTestCase):
    def test_known_curve_pda_matches_event_mint(self) -> None:
        ok, reason, evidence = validate_pumpportal_create(VALID_CREATE)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, 'ok')
        self.assertEqual(derive_pump_bonding_curve(VALID_CREATE['mint']), VALID_CREATE['bondingCurveKey'])
        self.assertTrue(evidence['curve_pda_matches_mint'])

    def test_mislabeled_quote_mint_is_rejected_before_network_or_trade(self) -> None:
        event = dict(VALID_CREATE)
        event['mint'] = b58encode(bytes(range(1, 33)))
        ok, reason, evidence = validate_pumpportal_create(event)
        self.assertFalse(ok)
        self.assertEqual(reason, 'bonding_curve_pda_mismatch')
        self.assertNotEqual(evidence['expected_curve'], event['bondingCurveKey'])

    def test_missing_signature_non_sol_quote_and_bad_reserves_fail_closed(self) -> None:
        missing_signature = dict(VALID_CREATE, signature='')
        self.assertEqual(validate_pumpportal_create(missing_signature)[1], 'invalid_or_missing_signature')

        non_sol_quote = dict(VALID_CREATE, quoteMint=b58encode(bytes([7]) * 32))
        self.assertEqual(validate_pumpportal_create(non_sol_quote)[1], 'non_sol_quote_mint')

        zero_reserves = dict(VALID_CREATE, vSolInBondingCurve=0)
        self.assertEqual(validate_pumpportal_create(zero_reserves)[1], 'invalid_curve_reserves')

    async def test_listener_signal_stamps_receive_time_and_verified_identity(self) -> None:
        import detector

        detector._seen_mints.discard(VALID_CREATE['mint'])
        with patch('detector.validate_pumpportal_create', return_value=validate_pumpportal_create(VALID_CREATE)):
            coin = await on_transaction_signal(VALID_CREATE, received_at=1234.5)
        self.assertIsInstance(coin, NewCoin)
        self.assertEqual(coin.found_at, 1234.5)
        self.assertTrue(coin.packet['pumpportal_identity_verified'])
        self.assertEqual(coin.packet['derived_bonding_curve'], VALID_CREATE['bondingCurveKey'])
        self.assertIsNone(coin.tx_payer)  # actor is not mislabeled as the first signer
        self.assertEqual(coin.packet['pumpportal_event_actor'], VALID_CREATE['traderPublicKey'])
        # Dev's own buy straight from the create message: no extra RPC.
        self.assertEqual(coin.packet['initial_buy_lamports'], 97545455)
        self.assertEqual(coin.packet['sol_amount_lamports'], 3)
        detector._seen_mints.discard(VALID_CREATE['mint'])

    async def test_dev_buy_fields_are_none_when_absent_or_malformed(self) -> None:
        import detector
        from detector import lamports_or_none

        self.assertEqual(lamports_or_none(1_000_000_000), 1_000_000_000)
        self.assertEqual(lamports_or_none('250000000'), 250_000_000)
        self.assertEqual(lamports_or_none(12.6), 13)
        for bad in (None, '', '   ', 'abc', '1e9', True, float('nan'), float('inf'), -5, {}, []):
            self.assertIsNone(lamports_or_none(bad), bad)

        detector._seen_mints.discard(VALID_CREATE['mint'])
        without_dev_buy = {k: v for k, v in VALID_CREATE.items() if k not in ('initialBuy', 'solAmount')}
        with patch('detector.validate_pumpportal_create', return_value=validate_pumpportal_create(VALID_CREATE)):
            coin = await on_transaction_signal(without_dev_buy, received_at=1234.5)
        self.assertIsInstance(coin, NewCoin)
        self.assertIsNone(coin.packet['initial_buy_lamports'])
        self.assertIsNone(coin.packet['sol_amount_lamports'])
        detector._seen_mints.discard(VALID_CREATE['mint'])

    async def test_curve_quote_mint_is_checked_from_live_account_bytes(self) -> None:
        sol_quote = await check_pump_curve(
            None, '', VALID_CREATE['mint'], VALID_CREATE['bondingCurveKey'],
            {}, curve_raw=curve_bytes(WSOL_MINT),
        )
        self.assertTrue(sol_quote.passed, sol_quote.reasons)

        non_sol_quote = await check_pump_curve(
            None, '', VALID_CREATE['mint'], VALID_CREATE['bondingCurveKey'],
            {}, curve_raw=curve_bytes(b58encode(bytes([9]) * 32)),
        )
        self.assertFalse(non_sol_quote.passed)
        self.assertIn('не SOL quote', non_sol_quote.reasons[0])

        mayhem = await check_pump_curve(
            None, '', VALID_CREATE['mint'], VALID_CREATE['bondingCurveKey'],
            {}, curve_raw=curve_bytes(WSOL_MINT, mayhem=True),
        )
        self.assertFalse(mayhem.passed)
        self.assertEqual(mayhem.reasons, ['pump_mayhem_mode_excluded'])

    def test_pump_supply_requires_six_decimals_standard_range_and_curve_match(self) -> None:
        ok = check_pump_supply(1_000_000_000_000_000, 6, 1_000_000_000_000_000)
        self.assertTrue(ok.passed, ok.reasons)
        wrong_decimals = check_pump_supply(1_000_000_000_000_000, 9, 1_000_000_000_000_000)
        self.assertFalse(wrong_decimals.passed)
        self.assertTrue(any('pump_decimals_not_6' in reason for reason in wrong_decimals.reasons))
        wrong_curve_supply = check_pump_supply(995_000_000_000_000, 6, 1_000_000_000_000_000)
        self.assertFalse(wrong_curve_supply.passed)
        self.assertTrue(any('pump_supply_mismatch_curve_token_total_supply' in reason for reason in wrong_curve_supply.reasons))
        out_of_range = check_pump_supply(989_999_999_999_999, 6, 989_999_999_999_999)
        self.assertFalse(out_of_range.passed)
        self.assertTrue(any('pump_supply_outside_standard_range' in reason for reason in out_of_range.reasons))

    async def test_mint_and_curve_are_read_in_one_owner_checked_rpc_request(self) -> None:
        meta = {
            'decimals': 6,
            'supply': '1000000000000000',
            'mintAuthority': None,
            'freezeAuthority': None,
        }
        raw = curve_bytes()
        global_raw = b'global-fixture-bytes'
        fee_config_raw = b'fee-config-fixture-bytes'
        addresses = [VALID_CREATE['mint'], VALID_CREATE['bondingCurveKey'], PUMP_GLOBAL_PDA, PUMP_FEE_CONFIG_PDA]
        data = {
            'result': {
                'context': {'slot': 12345},
                'value': [
                    {'owner': TOKEN_PROGRAM_ID, 'data': {'parsed': {'info': meta}}},
                    {'owner': PUMP_PROGRAM_ID, 'data': [base64.b64encode(raw).decode('ascii'), 'base64']},
                    {'owner': PUMP_PROGRAM_ID, 'data': [base64.b64encode(global_raw).decode('ascii'), 'base64']},
                    {'owner': PUMP_FEE_PROGRAM_ID, 'data': [base64.b64encode(fee_config_raw).decode('ascii'), 'base64']},
                ],
            }
        }
        session = FakePostSession(data)
        result = await fetch_pump_mint_curve_accounts(
            session, 'https://rpc.invalid', VALID_CREATE['mint'],
            VALID_CREATE['bondingCurveKey'], retries=0,
        )
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result[0], meta)
        self.assertEqual(result[1], raw)
        self.assertEqual(result[2]['context_slot'], 12345)
        self.assertEqual(result[2]['curve_raw'], raw)
        self.assertEqual(result[2]['global_raw'], global_raw)
        self.assertEqual(result[2]['fee_config_raw'], fee_config_raw)
        self.assertEqual(session.calls, 1)
        self.assertEqual(session.payloads[0]['method'], 'getMultipleAccounts')
        self.assertEqual(session.payloads[0]['params'][0], addresses)
        self.assertEqual(session.payloads[0]['params'][1]['commitment'], 'processed')

        data['result']['value'][2]['owner'] = 'wrong-owner'
        rejected = await fetch_pump_mint_curve_accounts(
            FakePostSession(data), 'https://rpc.invalid', VALID_CREATE['mint'],
            VALID_CREATE['bondingCurveKey'], retries=0,
        )
        self.assertIsNone(rejected)

    async def test_pump_filter_requires_local_pda_proof_and_checks_on_chain_quote(self) -> None:
        unverified = NewCoin(
            mint=VALID_CREATE['mint'], source='Pump.fun',
            signature=VALID_CREATE['signature'], curve=VALID_CREATE['bondingCurveKey'],
        )
        unused_session = FakePostSession({})
        rejected = await run_all_checks(
            unused_session, 'https://rpc.invalid', unverified,
            min_liquidity_usd=3000, honeypot_check=False,
            only_sol_pairs=True, indexer_retries=0,
        )
        self.assertFalse(rejected.passed)
        self.assertIn('не подтверждена', rejected.reasons[0])
        self.assertEqual(unused_session.calls, 0)

        meta = {
            'decimals': 6, 'supply': '1000000000000000',
            'mintAuthority': None, 'freezeAuthority': None,
        }
        raw = curve_bytes(WSOL_MINT)
        session = FakePostSession(pump_accounts_data(meta, raw))
        verified = NewCoin(
            mint=VALID_CREATE['mint'], source='Pump.fun',
            signature=VALID_CREATE['signature'], curve=VALID_CREATE['bondingCurveKey'],
            creator=VALID_CREATE['traderPublicKey'],
            tx_payer=None,
            packet={
                'pumpportal_identity_verified': True, 'tx_payer': None,
                'pumpportal_event_actor': VALID_CREATE['traderPublicKey'],
            },
        )
        passed = await run_all_checks(
            session, 'https://rpc.invalid', verified,
            min_liquidity_usd=3000, honeypot_check=False,
            only_sol_pairs=True, indexer_retries=0,
        )
        self.assertTrue(passed.passed, passed.reasons)
        self.assertEqual(verified.decimals, 6)
        self.assertEqual(verified.program_creator, '32CfGsZjrkHJukZuh97YN7t2eh8EmcuLkGuEzEYPBeRm')
        self.assertIsNone(verified.tx_payer)
        self.assertEqual(verified.packet['program_creator'], verified.program_creator)
        self.assertIsNone(verified.packet['tx_payer'])
        self.assertEqual(verified.packet['pumpportal_event_actor'], VALID_CREATE['traderPublicKey'])
        self.assertTrue(verified.packet['pump_standard_supply_valid'])
        self.assertEqual(verified.packet['pump_curve_r_sol'], 3.0)
        self.assertIsInstance(verified.curve_observed_at, float)
        self.assertEqual(session.calls, 1)

        wrong_quote_session = FakePostSession(
            pump_accounts_data(meta, curve_bytes(b58encode(bytes([4]) * 32)))
        )
        blocked = await run_all_checks(
            wrong_quote_session, 'https://rpc.invalid', verified,
            min_liquidity_usd=3000, honeypot_check=False,
            only_sol_pairs=True, indexer_retries=0,
        )
        self.assertFalse(blocked.passed)
        self.assertTrue(any('не SOL quote' in reason for reason in blocked.reasons))


if __name__ == '__main__':
    unittest.main()
