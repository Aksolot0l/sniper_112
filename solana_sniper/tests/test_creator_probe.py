from __future__ import annotations

import asyncio
import time
import unittest
from types import SimpleNamespace

from creator_probe import CreatorOnchainProbe
from pumpportal_validation import (
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    b58decode,
    derive_associated_token_address,
)

CREATOR = '2MRQDEBy1XfwvgwL8GbxXsD5wPq4gncn1YpsUKkosUFk'
MINT = '32CfGsZjrkHJukZuh97YN7t2eh8EmcuLkGuEzEYPBeRm'


class FakeResponse:
    status = 200

    def __init__(self, value, session=None):
        self.value = value
        self.session = session

    async def __aenter__(self):
        if self.session is not None:
            self.session.active += 1
            self.session.max_active = max(self.session.max_active, self.session.active)
            await asyncio.sleep(self.session.delay_sec)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        if self.session is not None:
            self.session.active -= 1
        return None

    async def json(self):
        return self.value


class FakeSession:
    def __init__(self, *, ata_pubkey=None, delay_sec=0.0):
        self.calls = []
        self.ata_pubkey = ata_pubkey or derive_associated_token_address(CREATOR, MINT, TOKEN_PROGRAM_ID)
        self.delay_sec = delay_sec
        self.active = 0
        self.max_active = 0

    def post(self, _url, **kwargs):
        payload = kwargs['json']
        self.calls.append(payload)
        if payload['method'] == 'getSignaturesForAddress':
            result = [
                {'signature': f'public-sig-{i}', 'blockTime': 1_720_000_100 - i}
                for i in range(25)
            ]
        elif payload['method'] == 'getTokenAccountsByOwner':
            result = {'value': [{
                'pubkey': self.ata_pubkey,
                'account': {
                    'owner': TOKEN_PROGRAM_ID,
                    'data': {'parsed': {'info': {
                        'owner': CREATOR,
                        'mint': MINT,
                        'tokenAmount': {'amount': '1234567', 'decimals': 6},
                    }}},
                },
            }]}
        else:
            raise AssertionError(f'unexpected RPC method {payload["method"]}')
        return FakeResponse({'jsonrpc': '2.0', 'id': payload['id'], 'result': result}, self)


class CreatorProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_signature_count_is_uncached_capped_page_stat(self) -> None:
        session = FakeSession()
        probe = CreatorOnchainProbe(session, 'https://rpc.example.invalid')
        result, error = await probe._signatures(CREATOR)
        self.assertIsNone(error)
        self.assertIsNotNone(result)
        self.assertEqual(result['count'], 25)
        self.assertEqual(result['oldest_block_time'], 1_720_000_076)
        second, error = await probe._signatures(CREATOR)
        self.assertIsNone(error)
        self.assertEqual(second['count'], 25)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual({call['method'] for call in session.calls}, {'getSignaturesForAddress'})

    async def test_probe_runs_signature_and_ata_reads_in_parallel_without_gettransaction(self) -> None:
        session = FakeSession(delay_sec=0.04)
        probe = CreatorOnchainProbe(session, 'https://rpc.example.invalid', timeout_sec=0.35)
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None,
            source='Pump.fun', packet={}, signature='public-create-signature',
        )
        started = time.monotonic()
        result = await probe.probe(coin)
        elapsed = time.monotonic() - started
        self.assertEqual(result['status'], 'ok')
        self.assertTrue(result['creator_gate_passed'])
        self.assertEqual(result['signature_count'], 25)
        self.assertTrue(result['ata_exists'])
        self.assertEqual(result['token_balance_raw'], '1234567')
        self.assertEqual(session.max_active, 2)
        self.assertLess(elapsed, 0.25)
        self.assertEqual(
            {call['method'] for call in session.calls},
            {'getSignaturesForAddress', 'getTokenAccountsByOwner'},
        )
        self.assertTrue(all(call['method'] != 'getTransaction' for call in session.calls))

    async def test_non_associated_token_account_does_not_satisfy_ata_gate(self) -> None:
        ata = derive_associated_token_address(CREATOR, MINT, TOKEN_PROGRAM_ID)
        ata_2022 = derive_associated_token_address(CREATOR, MINT, TOKEN_2022_PROGRAM_ID)
        self.assertIsNotNone(ata)
        self.assertIsNotNone(ata_2022)
        self.assertEqual(len(b58decode(ata) or b''), 32)
        self.assertEqual(len(b58decode(ata_2022) or b''), 32)
        self.assertNotEqual(ata, ata_2022)

        # The owner has a positive mint balance, but the returned token-account
        # address is deliberately not either locally-derived associated ATA.
        session = FakeSession(ata_pubkey=MINT)
        probe = CreatorOnchainProbe(session, 'https://rpc.example.invalid')
        balance, error = await probe._creator_token_accounts(CREATOR, MINT)
        self.assertIsNone(error)
        self.assertIsNotNone(balance)
        self.assertFalse(balance['ata_exists'])
        self.assertEqual(balance['balance_raw'], '0')
        self.assertEqual(balance['total_owner_mint_balance_raw'], '1234567')

        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None,
            source='Pump.fun', packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertFalse(result['creator_gate_passed'])


if __name__ == '__main__':
    unittest.main()


ACTOR = '9xQeWvG816bUx9EPfM5WYnNwX4y6Ef6yJ4v1o1r1n1Ab'


class ScriptedFakeSession:
    """Fake RPC session driven by explicit per-wallet script data."""

    def __init__(self, *, sigs, balances):
        # sigs: wallet -> list of blockTime ints (already newest-first)
        # balances: wallet -> token amount raw string
        self.sigs = sigs
        self.balances = balances
        self.calls = []
        self.mints = []

    def post(self, _url, **kwargs):
        payload = kwargs['json']
        self.calls.append(payload)
        method = payload['method']
        if method == 'getSignaturesForAddress':
            wallet = payload['params'][0]
            result = [
                {'signature': f'sig-{wallet[:4]}-{i}', 'blockTime': block_time}
                for i, block_time in enumerate(self.sigs.get(wallet, []))
            ]
        elif method == 'getTokenAccountsByOwner':
            wallet, mint = payload['params'][0], payload['params'][1]['mint']
            self.mints.append((wallet, mint))
            ata = derive_associated_token_address(wallet, mint, TOKEN_PROGRAM_ID)
            result = {'value': [{
                'pubkey': ata,
                'account': {
                    'owner': TOKEN_PROGRAM_ID,
                    'data': {'parsed': {'info': {
                        'owner': wallet,
                        'mint': mint,
                        'tokenAmount': {
                            'amount': self.balances.get(wallet, '0'), 'decimals': 6,
                        },
                    }}},
                },
            }]}
        else:
            raise AssertionError(f'unexpected RPC method {method}')
        return FakeResponse({'jsonrpc': '2.0', 'id': payload['id'], 'result': result})


def _old_sig_times(count: int = 25) -> list[int]:
    now = int(time.time())
    return [now - 86_400 * 30 - i for i in range(count)]


class CreatorIdentityAndGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_zero_history_fee_wallet_falls_back_to_event_actor(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: [], ACTOR: _old_sig_times(25)},
            balances={ACTOR: '9000000000000'},
        )
        probe = CreatorOnchainProbe(session, 'https://rpc.example.invalid')
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={'pumpportal_event_actor': ACTOR}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertEqual(result['creator'], ACTOR)
        self.assertEqual(result['creator_identity_type'], 'pumpportal_event_actor')
        self.assertTrue(result['creator_identity_resolved'])
        self.assertEqual(result['program_creator_sig_count'], 0)
        self.assertEqual(result['pumpportal_event_actor_sig_count'], 25)
        self.assertTrue(result['creator_guard_passed'])
        self.assertFalse(result['creator_burst_activity'])
        self.assertFalse(result['censored'])
        # The token account is read for the resolved identity, not the vault.
        self.assertIn((ACTOR, MINT), session.mints)

    async def test_burst_wallet_is_rejected_without_extra_rpc(self) -> None:
        now = int(time.time())
        # 25 signatures inside 26 minutes: a minting bot (DIAMOND/CONSENT shape).
        burst = [now - 60 * index for index in range(25)]
        session = ScriptedFakeSession(
            sigs={CREATOR: burst}, balances={CREATOR: '1000000000000'},
        )
        probe = CreatorOnchainProbe(session, 'https://rpc.example.invalid')
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertTrue(result['creator_burst_activity'])
        self.assertFalse(result['creator_guard_passed'])
        self.assertEqual(result['creator_guard_reason'], 'creator_burst_activity')
        self.assertLessEqual(result['creator_wallet_age_sec'], 3600)
        self.assertNotIn('getTransaction', {call['method'] for call in session.calls})

    async def test_wallet_cache_keeps_rps_flat_for_repeat_launchers(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: _old_sig_times(25)}, balances={CREATOR: '1000000000000'},
        )
        probe = CreatorOnchainProbe(
            session, 'https://rpc.example.invalid', signature_cache_sec=300.0,
        )
        for index in range(3):
            coin = SimpleNamespace(
                mint=MINT, program_creator=CREATOR, tx_payer=None,
                source='Pump.fun', packet={}, signature=f'sig-{index}',
            )
            result = await probe.probe(coin)
            self.assertTrue(result['creator_guard_passed'])
        signature_calls = [
            call for call in session.calls if call['method'] == 'getSignaturesForAddress'
        ]
        self.assertEqual(len(signature_calls), 1)
        stats = probe.cache_stats()
        self.assertEqual(stats['hits'], 2)
        self.assertEqual(stats['misses'], 1)

    async def test_missing_probe_data_is_censored_not_silently_clean(self) -> None:
        class FailingSession:
            def post(self, _url, **_kwargs):
                raise RuntimeError('rpc_down')

        probe = CreatorOnchainProbe(FailingSession(), 'https://rpc.example.invalid')
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertTrue(result['censored'])
        self.assertFalse(result['creator_guard_passed'])
        self.assertEqual(result['creator_guard_reason'], 'creator_history_unavailable')
