"""Run #10b §1 — one `getMultipleAccounts` per candidate per poll cycle.

The three questions this file answers with fixtures, not estimates:

1. two readers of the same mint inside one cycle cost exactly ONE request,
   and a new cycle costs a new one;
2. the local base64 mint decode returns exactly the fields `jsonParsed` used to
   return, so no filter can change its verdict because of the encoding;
3. the same account bytes give the same snapshot, the same filter verdict and
   the same entry price — the cache changes the number of reads, not decisions.
"""

from __future__ import annotations

import base64
import unittest
from unittest.mock import patch

from entry_reads import (
    ENTRY_CACHE_WINDOW_SEC,
    begin_entry_cycle,
    entry_account_cache,
    merge_entry_stats,
    mint_account_info,
    reset_entry_account_cache,
)
from filters import fetch_pump_mint_curve_accounts, run_all_checks
from models import NewCoin
from price import WSOL
from pumpportal_validation import (
    PUMP_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    b58encode,
)
from pump_curve_quote import (
    PUMP_FEE_CONFIG_PDA,
    PUMP_FEE_PROGRAM_ID,
    PUMP_GLOBAL_PDA,
)

import struct

from pump_curve_quote import BONDING_CURVE_DISCRIMINATOR

from tests.test_pumpportal_validation import (
    FakePostSession,
    VALID_CREATE,
    mint_bytes,
    pump_accounts_data,
)

MINT = VALID_CREATE['mint']
CURVE = VALID_CREATE['bondingCurveKey']
META = {
    'decimals': 6,
    'supply': '1000000000000000',
    'mintAuthority': None,
    'freezeAuthority': None,
}


def curve_bytes_with_real_reserves() -> bytes:
    """A curve that satisfies the CPMM invariant, so a price can be computed."""
    raw = bytearray(166)
    raw[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into('<QQ', raw, 8, 1_073_000_191_279_242, 30_000_000_000)
    struct.pack_into('<Q', raw, 32, 30_000_000_000)
    struct.pack_into('<Q', raw, 40, 1_073_000_191_279_242)
    raw[48] = 0
    raw[81] = 0
    return bytes(raw)


def _accounts_data() -> dict:
    return pump_accounts_data(META, curve_bytes_with_real_reserves())


class EntryCacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_entry_account_cache()

    async def test_two_readers_of_one_mint_in_one_cycle_cost_one_request(self) -> None:
        session = FakePostSession(_accounts_data())
        first = await fetch_pump_mint_curve_accounts(
            session, 'https://rpc.invalid', MINT, CURVE, retries=0,
        )
        self.assertIsNotNone(first)
        self.assertEqual(session.calls, 1)

        # Second reader of the SAME mint inside the same cycle: no new request.
        accounts, slot = await entry_account_cache().accounts(
            session, 'https://rpc.invalid', MINT, MINT, CURVE,
            source='observation',
        )
        self.assertEqual(session.calls, 1)
        self.assertIsNotNone(slot)
        self.assertIsInstance(accounts.get(CURVE), dict)

        stats = entry_account_cache().stats()
        self.assertEqual(stats['entry_reads_total'], 1)
        self.assertEqual(stats['entry_reads_served_from_cache'], 1)
        self.assertEqual(stats['entry_candidates'], 1)
        self.assertEqual(stats['entry_reads_per_candidate'], 1.0)
        self.assertFalse(stats['persisted'])

    async def test_new_cycle_reads_again(self) -> None:
        session = FakePostSession(_accounts_data())
        await fetch_pump_mint_curve_accounts(
            session, 'https://rpc.invalid', MINT, CURVE, retries=0,
        )
        self.assertEqual(session.calls, 1)
        begin_entry_cycle()
        await fetch_pump_mint_curve_accounts(
            session, 'https://rpc.invalid', MINT, CURVE, retries=0,
        )
        self.assertEqual(session.calls, 2)
        self.assertEqual(entry_account_cache().stats()['entry_reads_total'], 2)

    async def test_entry_reads_per_candidate_is_reads_over_candidates(self) -> None:
        session = FakePostSession(_accounts_data())
        other = b58encode(bytes(range(1, 33)))
        for mint in (MINT, other):
            await entry_account_cache().accounts(
                session, 'https://rpc.invalid', mint, mint, CURVE, source='filters',
            )
            await entry_account_cache().accounts(
                session, 'https://rpc.invalid', mint, mint, CURVE, source='observation',
            )
        stats = entry_account_cache().stats()
        self.assertEqual(session.calls, 2)
        self.assertEqual(stats['entry_candidates'], 2)
        self.assertEqual(stats['entry_reads_per_candidate'], 1.0)
        # Only the read that was actually paid for is attributed: the second
        # reader of each mint was served from the cycle cache.
        self.assertEqual(stats['entry_reads_by_source'], {'filters': 2})

    async def test_manifest_snapshot_carries_the_entry_budget(self) -> None:
        session = FakePostSession(_accounts_data())
        await fetch_pump_mint_curve_accounts(
            session, 'https://rpc.invalid', MINT, CURVE, retries=0,
        )
        merged = merge_entry_stats({'requests': 7})
        self.assertEqual(merged['requests'], 7)
        self.assertEqual(merged['entry_reads_total'], 1)
        self.assertEqual(merged['entry_reads_per_candidate'], 1.0)
        self.assertEqual(merged['window_sec'], ENTRY_CACHE_WINDOW_SEC)


class MintDecodeTests(unittest.TestCase):
    def test_decoded_fields_match_the_old_json_parsed_answer(self) -> None:
        account = {
            'owner': TOKEN_PROGRAM_ID,
            'data': [base64.b64encode(mint_bytes(META)).decode('ascii'), 'base64'],
        }
        self.assertEqual(mint_account_info(account), {
            'mintAuthority': None,
            'freezeAuthority': None,
            'decimals': 6,
            'supply': '1000000000000000',
            'isInitialized': True,
        })

    def test_live_authorities_are_returned_as_addresses(self) -> None:
        authority = b58encode(bytes(range(1, 33)))
        account = {
            'owner': TOKEN_PROGRAM_ID,
            'data': [base64.b64encode(mint_bytes({
                **META, 'mintAuthority': authority, 'freezeAuthority': authority,
            })).decode('ascii'), 'base64'],
        }
        info = mint_account_info(account)
        self.assertEqual(info['mintAuthority'], authority)
        self.assertEqual(info['freezeAuthority'], authority)

    def test_unreadable_account_is_not_guessed(self) -> None:
        self.assertIsNone(mint_account_info({'owner': TOKEN_PROGRAM_ID, 'data': []}))
        self.assertIsNone(mint_account_info(None))


class SameSnapshotSameDecisionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_entry_account_cache()

    def _coin(self) -> NewCoin:
        return NewCoin(
            mint=MINT, source='Pump.fun',
            signature=VALID_CREATE['signature'], curve=CURVE,
            creator=VALID_CREATE['traderPublicKey'], tx_payer=None,
            packet={
                'pumpportal_identity_verified': True, 'tx_payer': None,
                'pumpportal_event_actor': VALID_CREATE['traderPublicKey'],
            },
        )

    async def test_cached_and_uncached_reads_give_the_same_verdict_and_snapshot(self) -> None:
        first_session = FakePostSession(_accounts_data())
        first_coin = self._coin()
        first = await run_all_checks(
            first_session, 'https://rpc.invalid', first_coin,
            min_liquidity_usd=3000, honeypot_check=False,
            only_sol_pairs=True, indexer_retries=0,
        )
        snapshot = dict(first_coin.entry_snapshot)
        self.assertEqual(first_session.calls, 1)

        # Same bytes, second cycle: the read happens again but must produce a
        # byte-identical snapshot and the same verdict.
        begin_entry_cycle()
        second_session = FakePostSession(_accounts_data())
        second_coin = self._coin()
        second = await run_all_checks(
            second_session, 'https://rpc.invalid', second_coin,
            min_liquidity_usd=3000, honeypot_check=False,
            only_sol_pairs=True, indexer_retries=0,
        )
        self.assertEqual(second_session.calls, 1)
        self.assertEqual(first.passed, second.passed)
        self.assertEqual(first.reasons, second.reasons)
        self.assertEqual(snapshot['curve_raw'], second_coin.entry_snapshot['curve_raw'])
        self.assertEqual(snapshot['global_raw'], second_coin.entry_snapshot['global_raw'])
        self.assertEqual(snapshot['fee_config_raw'], second_coin.entry_snapshot['fee_config_raw'])
        self.assertEqual(snapshot['account_addresses'], second_coin.entry_snapshot['account_addresses'])
        self.assertEqual(first_coin.decimals, second_coin.decimals)
        self.assertEqual(
            first_coin.packet['pump_mint_supply_raw'],
            second_coin.packet['pump_mint_supply_raw'],
        )

    async def test_the_observation_reader_is_served_by_the_filter_batch(self) -> None:
        from price import pump_curve_price_usd

        session = FakePostSession(_accounts_data())
        await fetch_pump_mint_curve_accounts(
            session, 'https://rpc.invalid', MINT, CURVE, retries=0,
        )
        self.assertEqual(session.calls, 1)
        with patch('price.get_sol_usd', return_value=100.0):
            price = await pump_curve_price_usd(
                session, 'https://rpc.invalid', MINT, CURVE, source='observation',
            )
        self.assertEqual(session.calls, 1, 'the "+0 s" observation must ride along')
        self.assertIsNotNone(price)
        self.assertGreater(price, 0)


if __name__ == '__main__':
    unittest.main()
