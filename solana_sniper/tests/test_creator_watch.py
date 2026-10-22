from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from creator_watch import CreatorWatch, creator_should_block
from detector import transaction_fee_payer
from models import NewCoin

CREATOR = '2MRQDEBy1XfwvgwL8GbxXsD5wPq4gncn1YpsUKkosUFk'
CREATOR_2 = '32CfGsZjrkHJukZuh97YN7t2eh8EmcuLkGuEzEYPBeRm'


def iso(epoch: float) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec='milliseconds')


def coin(mint: str, creator: str = CREATOR, found_at: float = 1_000.0, identity_type: str = 'pump_curve_creator') -> NewCoin:
    program_creator = creator if identity_type == 'pump_curve_creator' else None
    tx_payer = creator
    return NewCoin(
        mint=mint,
        source='Pump.fun',
        signature=f'sig-{mint}',
        creator=creator,
        found_at=found_at,
        program_creator=program_creator,
        tx_payer=tx_payer,
        packet={
            'program_creator': program_creator,
            'tx_payer': tx_payer,
            'creator_identity_type': identity_type,
        },
    )


class CreatorWatchTests(unittest.TestCase):
    def make_watch(self, root: Path) -> CreatorWatch:
        watch = CreatorWatch(root / 'runs', root / 'runs' / 'known_scam_sellers.txt')
        watch.load_history()
        return watch

    def add_sell_route_loss(self, watch: CreatorWatch, mint: str, start: float) -> None:
        watch.record_quote(mint, 'entry_sell_precheck', 'sell', 'ok', iso(start), event_id=f'{mint}-precheck')
        for index, seconds_after in enumerate((5, 20, 35), start=1):
            watch.record_quote(
                mint, 'position_exit', 'sell', 'no_route',
                iso(start + seconds_after), 'quote_unavailable',
                event_id=f'{mint}-failed-{index}',
            )

    def test_repeat_wallet_and_burst_are_timestamped_and_do_not_self_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            first = coin('MintOne', found_at=1_000)
            first_profile = watch.register_candidate(first)
            self.assertEqual(first_profile['risk_level'], 'first_seen')
            self.assertEqual(first_profile['prior_token_count'], 0)

            second = coin('MintTwo', found_at=1_001)
            second_profile = watch.register_candidate(second)
            self.assertEqual(second_profile['risk_level'], 'repeat_creator')
            self.assertEqual(second_profile['prior_token_count'], 1)

            third = coin('MintThree', found_at=1_002)
            third_profile = watch.register_candidate(third)
            self.assertEqual(third_profile['risk_level'], 'rapid_launch_burst')
            self.assertEqual(third_profile['prior_token_count'], 2)
            self.assertEqual(third_profile['prior_tokens_10m'], 2)

    def test_future_quote_failures_are_not_used_before_they_happen(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            first = coin('MintPrior', found_at=1_000)
            watch.register_candidate(first)
            watch.record_quote('MintPrior', 'entry_sell_precheck', 'sell', 'ok', iso(1_001), event_id='pre')
            watch.record_quote('MintPrior', 'position_exit', 'sell', 'no_route', iso(1_010), event_id='f1')
            watch.record_quote('MintPrior', 'position_exit', 'sell', 'no_route', iso(1_020), event_id='f2')
            watch.record_quote('MintPrior', 'position_exit', 'sell', 'no_route', iso(1_040), event_id='f3')

            before_final_quote = watch.register_candidate(coin('MintSecond', found_at=1_030))
            self.assertEqual(before_final_quote['prior_exit_route_loss_count'], 0)
            self.assertEqual(before_final_quote['risk_level'], 'repeat_creator')

    def test_shadow_sell_quotes_alone_do_not_count_as_entry_route_loss_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            prior = coin('MintShadowOnly', found_at=1_000)
            watch.register_candidate(prior)
            watch.record_quote('MintShadowOnly', 'shadow_sell', 'sell', 'ok', iso(1_001), event_id='shadow-precheck')
            for index, seconds_after in enumerate((5, 20, 35), start=1):
                watch.record_quote(
                    'MintShadowOnly', 'position_exit', 'sell', 'no_route',
                    iso(1_001 + seconds_after), 'quote_unavailable',
                    event_id=f'shadow-failed-{index}',
                )

            profile = watch.register_candidate(coin('MintAfterShadow', found_at=1_100))
            self.assertEqual(profile['prior_exit_route_loss_count'], 0)
            self.assertEqual(profile['risk_level'], 'repeat_creator')

    def test_two_independent_prior_sell_route_losses_can_be_paper_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            first = coin('MintLossOne', found_at=1_000)
            watch.register_candidate(first)
            self.add_sell_route_loss(watch, 'MintLossOne', 1_001)

            second = coin('MintLossTwo', found_at=1_050)
            second_profile = watch.register_candidate(second)
            self.assertEqual(second_profile['prior_adverse_token_count'], 1)
            self.assertFalse(creator_should_block(second_profile, 'block')[0])
            self.add_sell_route_loss(watch, 'MintLossTwo', 1_051)

            third = coin('MintAfterTwoLosses', found_at=1_200)
            third_profile = watch.register_candidate(third)
            self.assertEqual(third_profile['risk_level'], 'suspected_repeat_adverse')
            self.assertEqual(third_profile['automatic_block_evidence_count'], 2)
            self.assertTrue(creator_should_block(third_profile, 'block')[0])
            self.assertFalse(creator_should_block(third_profile, 'warn')[0])

    def test_manual_blocklist_is_observed_in_warn_mode_and_blocks_only_in_explicit_block_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            blocklist = root / 'runs' / 'known_scam_sellers.txt'
            blocklist.parent.mkdir(parents=True)
            blocklist.write_text(f'{CREATOR} # manually reviewed\nnot-a-wallet\n', encoding='utf-8')
            watch = CreatorWatch(root / 'runs', blocklist)
            watch.load_history()
            profile = watch.register_candidate(coin('MintListed', found_at=2_000))
            self.assertEqual(profile['risk_level'], 'known_scam_manual')
            self.assertFalse(creator_should_block(profile, 'warn')[0])
            self.assertFalse(creator_should_block(profile, 'off')[0])
            self.assertTrue(creator_should_block(profile, 'block')[0])

    def test_observed_price_collapse_is_only_counted_after_its_observation_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            prior = coin('MintCollapse', found_at=1_000)
            watch.register_candidate(prior)
            watch.record_observation('MintCollapse', 0, iso(1_000), iso(1_001), 'ok', 1.0, 'fixture', 'obs0')
            watch.record_observation('MintCollapse', 15, iso(1_015), iso(1_016), 'ok', 1.1, 'fixture', 'obs15')
            watch.record_observation('MintCollapse', 60, iso(1_060), iso(1_061), 'ok', 0.05, 'fixture', 'obs60')

            before_observation = watch.register_candidate(coin('MintBeforeCollapse', found_at=1_050))
            self.assertEqual(before_observation['prior_rapid_collapse_count'], 0)
            after_observation = watch.register_candidate(coin('MintAfterCollapse', found_at=1_100))
            self.assertEqual(after_observation['prior_rapid_collapse_count'], 1)
            self.assertEqual(after_observation['risk_level'], 'prior_adverse_review')

    def test_fifty_percent_drawdown_is_review_only_and_never_a_single_mint_auto_block(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            prior = coin('MintExactly50Drop', found_at=1_000)
            watch.register_candidate(prior, observed_at=1_000)
            watch.record_observation('MintExactly50Drop', 0, iso(1_000), iso(1_001), 'ok', 1.0, 'fixture', 'drop-0')
            watch.record_observation('MintExactly50Drop', 15, iso(1_015), iso(1_016), 'ok', 1.0, 'fixture', 'drop-15')
            watch.record_observation('MintExactly50Drop', 60, iso(1_060), iso(1_061), 'ok', 0.5, 'fixture', 'drop-60')

            candidate = coin('MintAfter50Drop', found_at=1_100)
            profile = watch.register_candidate(candidate, observed_at=1_100)
            self.assertEqual(profile['prior_rapid_collapse_count'], 1)
            self.assertEqual(profile['risk_level'], 'prior_adverse_review')
            self.assertFalse(creator_should_block(profile, 'warn')[0])
            self.assertFalse(creator_should_block(profile, 'block')[0])

    def test_identity_observation_latency_is_causal_and_payer_is_not_curve_creator(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            late_identity = coin('MintLateIdentity', found_at=1_000)
            watch.register_candidate(late_identity, observed_at=1_020)
            before_identity_available = watch.register_candidate(
                coin('MintDecisionBeforeIdentity', found_at=1_010),
                observed_at=1_015,
            )
            self.assertEqual(before_identity_available['prior_token_count'], 0)
            self.assertEqual(before_identity_available['latency_ms'], 5_000.0)

            payer_only = NewCoin(
                mint='MintPayerOnly', source='Raydium CPMM', signature='payer-sig',
                creator=CREATOR, tx_payer=CREATOR,
                packet={'tx_payer': CREATOR, 'creator_identity_type': 'solana_transaction_fee_payer'},
                found_at=1_030,
            )
            payer_profile = watch.register_candidate(payer_only, observed_at=1_035)
            self.assertEqual(payer_profile['identity_type'], 'solana_transaction_fee_payer')
            self.assertEqual(payer_profile['identity_confidence'], 'medium')
            self.assertEqual(payer_profile['prior_token_count'], 0)

    def test_observations_arriving_before_creator_registration_are_retained(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            watch.record_observation('MintPendingObs', 0, iso(1_000), iso(1_001), 'ok', 1.0, 'fixture', 'pending-0')
            watch.record_observation('MintPendingObs', 15, iso(1_015), iso(1_016), 'ok', 1.0, 'fixture', 'pending-15')
            watch.record_observation('MintPendingObs', 60, iso(1_060), iso(1_061), 'ok', 0.5, 'fixture', 'pending-60')
            watch.register_candidate(coin('MintPendingObs', found_at=1_000), observed_at=1_000)
            after = watch.register_candidate(coin('MintAfterPendingObs', found_at=1_100), observed_at=1_100)
            self.assertEqual(after['prior_rapid_collapse_count'], 1)
            self.assertEqual(after['risk_level'], 'prior_adverse_review')

    def test_generic_transaction_identity_is_only_fee_payer_attribution(self) -> None:
        tx = {
            'transaction': {
                'message': {
                    'accountKeys': [
                        {'pubkey': CREATOR_2, 'signer': True},
                        {'pubkey': CREATOR, 'signer': False},
                    ]
                }
            }
        }
        self.assertEqual(transaction_fee_payer(tx), CREATOR_2)
        legacy = {
            'transaction': {
                'message': {
                    'header': {'numRequiredSignatures': 1},
                    'accountKeys': [CREATOR_2, CREATOR],
                }
            }
        }
        self.assertEqual(transaction_fee_payer(legacy), CREATOR_2)
        self.assertIsNone(transaction_fee_payer({'transaction': {'message': {'accountKeys': ['bad']}}}))

    def test_archive_scan_deduplicates_same_mint_across_run_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'runs'
            for folder in ('run-a', 'run-b'):
                path = root / folder / 'telemetry.sqlite'
                path.parent.mkdir(parents=True)
                with sqlite3.connect(path) as db:
                    db.execute('''CREATE TABLE candidates (
                        mint TEXT, creator TEXT, source TEXT, first_seen_at_utc TEXT, raw_json TEXT
                    )''')
                    db.execute('INSERT INTO candidates VALUES (?,?,?,?,?)', (
                        'MintDuplicate', CREATOR, 'Pump.fun', iso(1_000),
                        json.dumps({'pumpportal_identity_verified': True}),
                    ))
                    db.commit()

            watch = CreatorWatch(root, root / 'known_scam_sellers.txt')
            summary = watch.load_history()
            self.assertEqual(summary['files_scanned'], 2)
            self.assertEqual(summary['unique_tokens_with_creator'], 1)
            self.assertEqual(summary['creator_wallets'], 1)
            self.assertEqual(summary['repeat_creator_wallets'], 0)

    def test_rejected_identity_candidate_is_not_added_to_creator_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            watch = self.make_watch(Path(temp))
            profile = watch.register_candidate(coin('MintRejected', found_at=1_000), eligible=False)
            self.assertEqual(profile['risk_level'], 'unverified_candidate')
            self.assertEqual(watch.summary()['unique_tokens_with_creator'], 0)


if __name__ == '__main__':
    unittest.main()
