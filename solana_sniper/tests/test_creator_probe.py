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


class BusyResponse(FakeResponse):
    """HTTP 429 with a JSON body, as seen once in run #1."""

    status = 429

    async def text(self):
        return '{"error":{"code":-32429,"message":"rate limited"}}'


class BusySession(FakeSession):
    def post(self, _url, **kwargs):
        payload = kwargs['json']
        self.calls.append(payload)
        return BusyResponse({'jsonrpc': '2.0', 'id': payload['id']}, self)


class RpcErrorDetailTests(unittest.IsolatedAsyncioTestCase):
    """Run #1 lost 108/405 probes to a bare RuntimeError; keep the payload."""

    async def test_http_429_keeps_status_and_body(self) -> None:
        probe = CreatorOnchainProbe(BusySession(), 'https://rpc.example.invalid')
        result, error = await probe._signatures(CREATOR)
        self.assertIsNone(result)
        # The guard still fails closed on the same exception type...
        self.assertEqual(error, 'RuntimeError')
        # ...and the reason is now recoverable.
        detail = probe._error_details[CREATOR]
        self.assertEqual(detail['http_status'], 429)
        self.assertEqual(detail['method'], 'getSignaturesForAddress')
        self.assertIn('rate limited', detail['body'])
        self.assertIn('429', detail['label'])

    async def test_rpc_error_object_keeps_code_and_message(self) -> None:
        class ErrorSession(FakeSession):
            def post(self, _url, **kwargs):
                payload = kwargs['json']
                self.calls.append(payload)
                return FakeResponse({
                    'jsonrpc': '2.0', 'id': payload['id'],
                    'error': {'code': -32602, 'message': 'Invalid param: not a hash'},
                }, self)

        probe = CreatorOnchainProbe(ErrorSession(), 'https://rpc.example.invalid')
        data, error = await probe._creator_token_accounts(CREATOR, MINT)
        self.assertIsNone(data)
        self.assertEqual(error, 'RuntimeError')
        detail = probe._error_details[CREATOR]
        self.assertEqual(detail['code'], -32602)
        self.assertIn('Invalid param', detail['message'])

    async def test_successful_read_clears_previous_failure_of_same_wallet(self) -> None:
        """Stale detail must not ride along on a probe with empty errors[]."""
        class FlakySession(FakeSession):
            def __init__(self) -> None:
                super().__init__()
                self.calls_left = 1

            def post(self, _url, **kwargs):
                if self.calls_left > 0:
                    self.calls_left -= 1
                    payload = kwargs['json']
                    self.calls.append(payload)
                    return BusyResponse({'jsonrpc': '2.0', 'id': payload['id']}, self)
                return super().post(_url, **kwargs)

        probe = CreatorOnchainProbe(FlakySession(), 'https://rpc.example.invalid')
        first, error = await probe._signatures(CREATOR)
        self.assertIsNone(first)
        self.assertEqual(error, 'RuntimeError')
        self.assertIn(CREATOR, probe._error_details)

        second, error = await probe._signatures(CREATOR)
        self.assertIsNone(error)
        self.assertIsNotNone(second)
        self.assertNotIn(CREATOR, probe._error_details)

    async def test_token_account_success_also_clears_the_stale_detail(self) -> None:
        """The ATA read has its own success path; both must clear the wallet."""

        class FlakyAtaSession(FakeSession):
            def __init__(self) -> None:
                super().__init__()
                self.fail_ata = True

            def post(self, _url, **kwargs):
                if self.fail_ata and kwargs['json']['method'] == 'getTokenAccountsByOwner':
                    self.fail_ata = False
                    payload = kwargs['json']
                    self.calls.append(payload)
                    return BusyResponse({'jsonrpc': '2.0', 'id': payload['id']}, self)
                return super().post(_url, **kwargs)

        probe = CreatorOnchainProbe(FlakyAtaSession(), 'https://rpc.example.invalid')
        first, error = await probe._creator_token_accounts(CREATOR, MINT)
        self.assertIsNone(first)
        self.assertEqual(error, 'RuntimeError')
        self.assertIn(CREATOR, probe._error_details)
        second, error = await probe._creator_token_accounts(CREATOR, MINT)
        self.assertIsNone(error)
        self.assertIsNotNone(second)
        self.assertNotIn(CREATOR, probe._error_details)

    async def test_probe_result_carries_error_detail_and_never_gates_on_it(self) -> None:
        probe = CreatorOnchainProbe(BusySession(), 'https://rpc.example.invalid')
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None,
            packet={'pumpportal_event_actor': None}, signature='sig',
        )
        values = await probe.probe(coin)
        self.assertFalse(values['creator_guard_passed'])
        self.assertTrue(values['censored'])
        self.assertTrue(values['error_detail'])
        self.assertIn('rpc_http_429', values['error_codes'])
        # The failure detail never turns into a pass.
        self.assertEqual(values['creator_guard_reason'], 'creator_history_unavailable')

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


class CreatorRepeatDeployTests(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_also_applies_when_curve_read_failed(self) -> None:
        class FlakySession(ScriptedFakeSession):
            def post(self, _url, **kwargs):
                payload = kwargs['json']
                if payload['method'] == 'getSignaturesForAddress':
                    if payload['params'][0] == CREATOR:
                        raise RuntimeError('rpc_timeout_curve')
                return super().post(_url, **kwargs)

        session = FlakySession(
            sigs={ACTOR: _old_sig_times(25)}, balances={ACTOR: '9000000000000'},
        )
        probe = CreatorOnchainProbe(session, 'https://rpc.example.invalid')
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={'pumpportal_event_actor': ACTOR}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertEqual(result['creator'], ACTOR)
        self.assertTrue(result['creator_identity_resolved'])
        self.assertIsNone(result['program_creator_sig_count'])
        self.assertTrue(result['creator_guard_passed'])

    async def test_repeat_deploy_in_session_uses_local_telemetry_only(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: _old_sig_times(25)}, balances={CREATOR: '1000000000000'},
        )

        class FakeWatch:
            def __init__(self, prior):
                self.prior = prior
                self.calls = []

            def prior_tokens_10m(self, creator, as_of=None, current_mint=None):
                self.calls.append((creator, current_mint))
                return self.prior

        watch = FakeWatch(prior=2)
        rpc_before = len(session.calls)
        probe = CreatorOnchainProbe(
            session, 'https://rpc.example.invalid', creator_watch=watch,
        )
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertFalse(result['creator_guard_passed'])
        self.assertEqual(result['creator_guard_reason'], 'creator_repeat_deploy_in_session')
        self.assertEqual(result['creator_prior_tokens_10m'], 2)
        self.assertTrue(result['creator_repeat_deploy_in_session'])
        self.assertFalse(result['creator_burst_activity'])
        self.assertEqual(watch.calls, [(CREATOR, MINT)])
        # The local signal must not add a single RPC request: exactly the
        # signature page + one token-account read, nothing more.
        self.assertEqual(len(session.calls) - rpc_before, 2)
        self.assertEqual(
            sorted(call['method'] for call in session.calls[rpc_before:]),
            ['getSignaturesForAddress', 'getTokenAccountsByOwner'],
        )

    async def test_single_prior_launch_is_not_enough_to_reject(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: _old_sig_times(25)}, balances={CREATOR: '1000000000000'},
        )

        class FakeWatch:
            def prior_tokens_10m(self, *_args, **_kwargs):
                return 1

        probe = CreatorOnchainProbe(
            session, 'https://rpc.example.invalid', creator_watch=FakeWatch(),
        )
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertTrue(result['creator_guard_passed'])
        self.assertEqual(result['creator_guard_reason'], 'creator_guard_met')

    async def test_broken_watch_object_does_not_break_the_probe(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: _old_sig_times(25)}, balances={CREATOR: '1000000000000'},
        )

        class BrokenWatch:
            def prior_tokens_10m(self, *_args, **_kwargs):
                raise RuntimeError('telemetry_corrupt')

        probe = CreatorOnchainProbe(
            session, 'https://rpc.example.invalid', creator_watch=BrokenWatch(),
        )
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertEqual(result['creator_prior_tokens_10m'], 0)
        self.assertTrue(result['creator_guard_passed'])


class CreatorActorDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_actor_prior_tokens_recorded_but_never_gating(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: [], ACTOR: _old_sig_times(25)},
            balances={ACTOR: '9000000000000'},
        )

        class FakeWatch:
            def __init__(self):
                self.seen = []

            def prior_tokens_10m(self, creator, as_of=None, current_mint=None):
                self.seen.append(creator)
                # Real deployer in the actor path: 3 launches in 10 minutes.
                return 3 if creator == ACTOR else 0

        watch = FakeWatch()
        probe = CreatorOnchainProbe(
            session, 'https://rpc.example.invalid', creator_watch=watch,
        )
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={'pumpportal_event_actor': ACTOR}, signature='sig',
        )
        result = await probe.probe(coin)
        # Identity resolved to the actor, so the gating count itself is the actor's.
        self.assertEqual(result['creator'], ACTOR)
        self.assertEqual(result['creator_prior_tokens_10m'], 3)
        self.assertEqual(result['creator_prior_tokens_10m_actor'], 3)
        self.assertIn(ACTOR, watch.seen)

    async def test_actor_field_is_none_when_curve_creator_is_the_real_wallet(self) -> None:
        session = ScriptedFakeSession(
            sigs={CREATOR: _old_sig_times(25)}, balances={CREATOR: '1000000000000'},
        )

        class FakeWatch:
            def prior_tokens_10m(self, *_args, **_kwargs):
                return 0

        probe = CreatorOnchainProbe(
            session, 'https://rpc.example.invalid', creator_watch=FakeWatch(),
        )
        coin = SimpleNamespace(
            mint=MINT, program_creator=CREATOR, tx_payer=None, source='Pump.fun',
            packet={}, signature='sig',
        )
        result = await probe.probe(coin)
        self.assertIsNone(result['creator_prior_tokens_10m_actor'])
        self.assertEqual(result['creator_prior_tokens_10m_actor_path'], 'not_applicable')
        self.assertTrue(result['creator_guard_passed'])
