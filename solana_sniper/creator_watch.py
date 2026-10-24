"""Paper-only creator/deployer wallet watch built from timestamped local telemetry.

This module never sends transactions. It links a public wallet address to token
creation events and prior, observable outcomes; an address is not a person and a
repeat launch is not, by itself, proof of a scam.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pumpportal_validation import b58decode

# Keep these conservative. A price collapse is only supporting evidence; the
# stronger quote-unavailability signal requires a prior successful exact-size
# sell precheck followed by repeated missing exit quotes over time. The quote
# provider does not distinguish a missing route from every transient API error.
RAPID_COLLAPSE_DROP_PCT = 50.0
RAPID_COLLAPSE_HORIZON_SEC = 120
MIN_EXIT_ROUTE_FAILURES = 3
MIN_EXIT_ROUTE_FAILURE_SPAN_SEC = 20.0
# Window for counting a wallet's own repeat launches (used by
# CreatorWatch.prior_tokens_10m). Distinct from creator_probe's
# creator_burst_window_sec = 3600, which measures signature-page density.
REPEAT_DEPLOY_WINDOW_SEC = 10 * 60
BURST_PRIOR_TOKENS = 2
MIN_PRIOR_ADVERSE_TOKENS_TO_FLAG = 2

_HIGH_CONFIDENCE_CREATOR_TYPES = {
    'pump_curve_creator',
}


def is_solana_pubkey(value: Any) -> bool:
    """Return True only for a base58 string decoding to a 32-byte public key."""
    if not isinstance(value, str) or not value:
        return False
    decoded = b58decode(value)
    return decoded is not None and len(decoded) == 32


def parse_utc_epoch(value: Any) -> Optional[float]:
    """Parse an ISO-8601 UTC timestamp or epoch, returning None if unknown."""
    if isinstance(value, (int, float)):
        try:
            result = float(value)
            return result if result >= 0 else None
        except (TypeError, ValueError, OverflowError):
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        result = float(raw)
        return result if result >= 0 else None
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def utc_iso(value: float) -> str:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat(timespec='milliseconds')
    except (TypeError, ValueError, OverflowError, OSError):
        return ''


def creator_identity_type(creator: Optional[str], source: str, packet: Any) -> str:
    """Describe attribution provenance; only the curve field is a Pump creator."""
    data = packet if isinstance(packet, dict) else {}
    program_creator = data.get('program_creator')
    if is_solana_pubkey(program_creator):
        return 'pump_curve_creator'
    tx_payer = data.get('tx_payer')
    if is_solana_pubkey(tx_payer):
        return 'solana_transaction_fee_payer'
    explicit = data.get('creator_identity_type')
    if isinstance(explicit, str) and explicit.strip():
        if explicit.strip() in {'pumpportal_creator', 'pumpfun_creator_legacy'}:
            return 'legacy_attributed_wallet'
        return explicit.strip()
    if creator and source == 'Pump.fun':
        return 'legacy_attributed_wallet'
    if creator:
        return 'solana_transaction_fee_payer'
    return 'unknown'


@dataclass
class ObservationPoint:
    event_id: str
    offset_sec: int
    target_epoch: Optional[float]
    observed_epoch: Optional[float]
    status: str
    price_usd: Optional[float]
    price_source: Optional[str] = None


@dataclass
class QuotePoint:
    event_id: str
    stage: str
    side: str
    status: str
    requested_epoch: Optional[float]
    error_type: Optional[str] = None


@dataclass
class CreatorToken:
    mint: str
    source: str
    creator: Optional[str]
    identity_type: str
    created_epoch: Optional[float]
    identity_observed_epoch: Optional[float] = None
    observations: dict[str, ObservationPoint] = field(default_factory=dict)
    quotes: dict[str, QuotePoint] = field(default_factory=dict)
    identity_conflict: bool = False


class CreatorWatch:
    """Read-only archive scan plus in-memory, append-only watch for this run."""

    def __init__(self, history_dir: str | Path, blocklist_path: str | Path) -> None:
        self.history_dir = Path(history_dir)
        self.blocklist_path = Path(blocklist_path)
        self.known_scam_wallets: set[str] = set()
        self._tokens: dict[str, CreatorToken] = {}
        # Identity groups are typed: a transaction payer never silently merges
        # with a verified Pump bonding-curve program creator.
        self._by_creator: dict[tuple[str, str], set[str]] = {}
        self._pending_observations: dict[str, dict[str, ObservationPoint]] = {}
        self._pending_quotes: dict[str, dict[str, QuotePoint]] = {}
        self._load_summary: dict[str, int] = {
            'files_scanned': 0,
            'files_skipped': 0,
            'candidate_rows': 0,
            'unique_tokens_with_creator': 0,
            'creator_wallets': 0,
            'repeat_creator_wallets': 0,
        }

    def _ensure_blocklist_file(self) -> None:
        self.blocklist_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.blocklist_path.exists():
            self.blocklist_path.write_text(
                '# One complete Solana public creator/deployer wallet address per line.\n'
                '# Add only manually confirmed addresses; use # for optional notes.\n'
                '# Never put a private key or seed phrase in this file.\n',
                encoding='utf-8',
            )

    def _load_blocklist(self) -> None:
        self._ensure_blocklist_file()
        wallets: set[str] = set()
        try:
            lines = self.blocklist_path.read_text(encoding='utf-8', errors='replace').splitlines()
        except OSError:
            lines = []
        for line in lines:
            candidate = line.split('#', 1)[0].strip().split()
            if not candidate:
                continue
            address = candidate[0].strip()
            if is_solana_pubkey(address):
                wallets.add(address)
        self.known_scam_wallets = wallets

    def _put_candidate(
        self,
        mint: str,
        creator: Optional[str],
        source: str,
        created_epoch: Optional[float],
        identity_type: str,
        identity_observed_epoch: Optional[float] = None,
    ) -> None:
        if not mint:
            return
        candidate_creator = creator if is_solana_pubkey(creator) else None
        identity_type = identity_type or 'unknown'
        observed_epoch = identity_observed_epoch if identity_observed_epoch is not None else created_epoch
        existing = self._tokens.get(mint)
        if existing is None:
            record = CreatorToken(
                mint=mint,
                source=source or 'unknown',
                creator=candidate_creator,
                identity_type=identity_type,
                created_epoch=created_epoch,
                identity_observed_epoch=observed_epoch,
            )
            self._tokens[mint] = record
        else:
            if existing.created_epoch is None or (
                created_epoch is not None and created_epoch < existing.created_epoch
            ):
                existing.created_epoch = created_epoch
                existing.source = source or existing.source
            if candidate_creator and not existing.identity_conflict:
                current_is_high = existing.identity_type in _HIGH_CONFIDENCE_CREATOR_TYPES
                new_is_high = identity_type in _HIGH_CONFIDENCE_CREATOR_TYPES
                if existing.creator is None or (new_is_high and not current_is_high):
                    if existing.creator:
                        self._by_creator.get((existing.identity_type, existing.creator), set()).discard(mint)
                    existing.creator = candidate_creator
                    existing.identity_type = identity_type
                    existing.identity_observed_epoch = observed_epoch
                elif existing.creator == candidate_creator:
                    if new_is_high:
                        existing.identity_type = identity_type
                    if (
                        observed_epoch is not None
                        and (existing.identity_observed_epoch is None or observed_epoch < existing.identity_observed_epoch)
                    ):
                        existing.identity_observed_epoch = observed_epoch
                elif current_is_high and not new_is_high:
                    # Keep the verified curve creator; fee-payer attribution is a
                    # separate, lower-confidence role, not a conflict.
                    pass
                elif new_is_high and not current_is_high:
                    self._by_creator.get((existing.identity_type, existing.creator), set()).discard(mint)
                    existing.creator = candidate_creator
                    existing.identity_type = identity_type
                    existing.identity_observed_epoch = observed_epoch
                elif existing.identity_type == identity_type:
                    # Conflicting same-role attribution for one mint is ambiguous.
                    self._by_creator.get((existing.identity_type, existing.creator), set()).discard(mint)
                    existing.identity_conflict = True
                    existing.creator = None

        record = self._tokens[mint]
        if record.creator and not record.identity_conflict:
            self._by_creator.setdefault((record.identity_type, record.creator), set()).add(mint)
        for event_id, point in self._pending_observations.pop(mint, {}).items():
            record.observations[event_id] = point
        for event_id, point in self._pending_quotes.pop(mint, {}).items():
            record.quotes[event_id] = point

    def load_history(self) -> dict[str, int]:
        """Load old run telemetry, deduplicating mint/event IDs across snapshots."""
        self._tokens.clear()
        self._by_creator.clear()
        self._load_blocklist()
        scanned = 0
        skipped = 0
        candidate_rows = 0
        explicit_identity_observations: dict[tuple[str, str, str], float] = {}

        if self.history_dir.exists():
            db_paths = sorted(self.history_dir.rglob('telemetry.sqlite'))
        else:
            db_paths = []

        for db_path in db_paths:
            try:
                uri = f'file:{db_path.resolve()}?mode=ro'
                with sqlite3.connect(uri, uri=True, timeout=1.5) as db:
                    tables = {
                        row[0] for row in db.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        ).fetchall()
                    }
                    if 'candidates' not in tables:
                        skipped += 1
                        continue
                    columns = {
                        row[1] for row in db.execute('PRAGMA table_info(candidates)').fetchall()
                    }
                    required = {'mint', 'source', 'first_seen_at_utc'}
                    if not required.issubset(columns):
                        skipped += 1
                        continue
                    select_program = 'program_creator' if 'program_creator' in columns else 'NULL'
                    select_payer = 'tx_payer' if 'tx_payer' in columns else 'NULL'
                    select_legacy = 'creator' if 'creator' in columns else 'NULL'
                    select_raw = 'raw_json' if 'raw_json' in columns else "'{}'"
                    query = (
                        f'SELECT mint,{select_program},{select_payer},{select_legacy},'
                        f'source,first_seen_at_utc,{select_raw} FROM candidates'
                    )
                    candidates = db.execute(query).fetchall()
                    candidate_rows += len(candidates)
                    for mint, program_creator, tx_payer, legacy_creator, source, created_at, raw_json in candidates:
                        if not mint:
                            continue
                        packet: Any = {}
                        try:
                            packet = json.loads(raw_json or '{}')
                        except (TypeError, ValueError, json.JSONDecodeError):
                            packet = {}
                        program_creator = program_creator or packet.get('program_creator')
                        tx_payer = tx_payer or packet.get('tx_payer')
                        if is_solana_pubkey(program_creator):
                            identity, identity_type = str(program_creator), 'pump_curve_creator'
                        elif is_solana_pubkey(tx_payer):
                            identity, identity_type = str(tx_payer), 'solana_transaction_fee_payer'
                        elif is_solana_pubkey(legacy_creator):
                            identity, identity_type = str(legacy_creator), 'legacy_attributed_wallet'
                        else:
                            continue
                        created_epoch = parse_utc_epoch(created_at)
                        identity_observed_epoch = parse_utc_epoch(
                            packet.get('pump_curve_observed_at_epoch')
                        ) or created_epoch
                        self._put_candidate(
                            str(mint), identity, str(source or 'unknown'),
                            created_epoch, identity_type, identity_observed_epoch,
                        )

                    if 'creator_attributions' in tables:
                        attribution_columns = {
                            row[1] for row in db.execute('PRAGMA table_info(creator_attributions)').fetchall()
                        }
                        if {'mint', 'program_creator', 'tx_payer', 'observed_at_utc'}.issubset(attribution_columns):
                            rows = db.execute(
                                'SELECT mint,program_creator,tx_payer,observed_at_utc '
                                'FROM creator_attributions ORDER BY rowid'
                            ).fetchall()
                            for mint, program_creator, tx_payer, observed_at in rows:
                                token = self._tokens.get(str(mint or ''))
                                if token is None:
                                    continue
                                observed_epoch = parse_utc_epoch(observed_at)
                                if is_solana_pubkey(program_creator):
                                    self._put_candidate(
                                        token.mint, str(program_creator), token.source,
                                        token.created_epoch, 'pump_curve_creator', observed_epoch,
                                    )
                                    if observed_epoch is not None:
                                        key = (token.mint, str(program_creator), 'pump_curve_creator')
                                        prior_observed = explicit_identity_observations.get(key)
                                        explicit_identity_observations[key] = (
                                            observed_epoch if prior_observed is None
                                            else min(prior_observed, observed_epoch)
                                        )
                                elif is_solana_pubkey(tx_payer) and token.creator is None:
                                    self._put_candidate(
                                        token.mint, str(tx_payer), token.source,
                                        token.created_epoch, 'solana_transaction_fee_payer', observed_epoch,
                                    )

                    if 'observations' in tables:
                        observation_columns = {
                            row[1] for row in db.execute('PRAGMA table_info(observations)').fetchall()
                        }
                        if {'mint', 'target_offset_sec', 'target_at_utc', 'observed_at_utc', 'status', 'price_usd'}.issubset(observation_columns):
                            obs_query = 'SELECT '
                            obs_query += 'event_id,' if 'event_id' in observation_columns else "'' AS event_id,"
                            obs_query += 'mint,target_offset_sec,target_at_utc,observed_at_utc,status,price_usd'
                            obs_query += ',price_source' if 'price_source' in observation_columns else ", NULL"
                            rows = db.execute(obs_query + ' FROM observations').fetchall()
                            for event_id, mint, offset, target, observed, status, price, source in rows:
                                if not mint or str(mint) not in self._tokens:
                                    continue
                                point = ObservationPoint(
                                    event_id=str(event_id or f'{mint}:{offset}:{observed}'),
                                    offset_sec=int(offset or 0),
                                    target_epoch=parse_utc_epoch(target),
                                    observed_epoch=parse_utc_epoch(observed),
                                    status=str(status or ''),
                                    price_usd=_optional_float(price),
                                    price_source=str(source) if source else None,
                                )
                                self._tokens[str(mint)].observations[point.event_id] = point

                    if 'quotes' in tables:
                        quote_columns = {
                            row[1] for row in db.execute('PRAGMA table_info(quotes)').fetchall()
                        }
                        if {'mint', 'stage', 'side', 'status', 'requested_at_utc'}.issubset(quote_columns):
                            quote_query = 'SELECT '
                            quote_query += 'event_id,' if 'event_id' in quote_columns else "'' AS event_id,"
                            quote_query += 'mint,stage,side,status,requested_at_utc'
                            quote_query += ',error_type' if 'error_type' in quote_columns else ',NULL'
                            rows = db.execute(quote_query + ' FROM quotes').fetchall()
                            for event_id, mint, stage, side, status, requested_at, error_type in rows:
                                if not mint or str(mint) not in self._tokens:
                                    continue
                                point = QuotePoint(
                                    event_id=str(event_id or f'{mint}:{stage}:{side}:{requested_at}:{status}'),
                                    stage=str(stage or ''),
                                    side=str(side or ''),
                                    status=str(status or ''),
                                    requested_epoch=parse_utc_epoch(requested_at),
                                    error_type=str(error_type) if error_type else None,
                                )
                                self._tokens[str(mint)].quotes[point.event_id] = point
                scanned += 1
            except (OSError, sqlite3.Error, ValueError):
                skipped += 1

        for (mint, identity, identity_type), observed_epoch in explicit_identity_observations.items():
            token = self._tokens.get(mint)
            if (
                token is not None and token.creator == identity
                and token.identity_type == identity_type
            ):
                token.identity_observed_epoch = observed_epoch
        self._rebuild_index()
        repeated = sum(1 for mints in self._by_creator.values() if len(mints) > 1)
        self._load_summary = {
            'files_scanned': scanned,
            'files_skipped': skipped,
            'candidate_rows': candidate_rows,
            'unique_tokens_with_creator': sum(
                1 for token in self._tokens.values() if token.creator and not token.identity_conflict
            ),
            'creator_wallets': len({address for _identity_type, address in self._by_creator}),
            'repeat_creator_wallets': repeated,
        }
        return dict(self._load_summary)

    def _rebuild_index(self) -> None:
        self._by_creator.clear()
        for mint, token in self._tokens.items():
            if token.creator and not token.identity_conflict:
                self._by_creator.setdefault((token.identity_type, token.creator), set()).add(mint)

    def prior_tokens_10m(
        self,
        creator: Optional[str],
        as_of: Optional[float] = None,
        current_mint: Optional[str] = None,
    ) -> int:
        """Lookahead-safe count of this wallet's other launches in the last 10 min.

        Runs purely on locally loaded telemetry: zero RPC, zero new state.
        Used by the B guard as a second serial-deploy signal.
        """
        if not creator or as_of is None:
            return 0
        count = 0
        for token in self._tokens.values():
            if token.identity_conflict or token.creator != creator:
                continue
            if current_mint and token.mint == current_mint:
                continue
            created = token.created_epoch
            if created is None or created >= as_of:
                continue
            observed = token.identity_observed_epoch
            if observed is not None and observed > as_of:
                continue
            if 0.0 <= as_of - float(created) <= REPEAT_DEPLOY_WINDOW_SEC:
                count += 1
        return count

    def register_candidate(
        self,
        coin: Any,
        eligible: bool = True,
        observed_at: Optional[float] = None,
    ) -> dict[str, Any]:
        """Profile with information observable by the supplied decision time."""
        packet = coin.packet if isinstance(getattr(coin, 'packet', None), dict) else {}
        program_creator = getattr(coin, 'program_creator', None) or packet.get('program_creator')
        tx_payer = getattr(coin, 'tx_payer', None) or packet.get('tx_payer')
        source = str(getattr(coin, 'source', '') or 'unknown')
        if tx_payer is None and source != 'Pump.fun':
            tx_payer = getattr(coin, 'creator', None)
        mint = str(getattr(coin, 'mint', '') or '')
        created_epoch = parse_utc_epoch(getattr(coin, 'found_at', None))
        as_of = parse_utc_epoch(observed_at) if observed_at is not None else created_epoch
        if is_solana_pubkey(program_creator):
            creator = str(program_creator)
            identity_type = 'pump_curve_creator'
        elif is_solana_pubkey(tx_payer):
            creator = str(tx_payer)
            identity_type = 'solana_transaction_fee_payer'
        else:
            creator = None
            identity_type = 'unknown'
        creator_valid = is_solana_pubkey(creator)
        decision_latency_ms = (
            max(0.0, (as_of - created_epoch) * 1000.0)
            if as_of is not None and created_epoch is not None else None
        )
        profile: dict[str, Any] = {
            'creator': creator if creator_valid else None,
            'program_creator': str(program_creator) if is_solana_pubkey(program_creator) else None,
            'tx_payer': str(tx_payer) if is_solana_pubkey(tx_payer) else None,
            'identity_type': identity_type,
            'identity_confidence': 'high' if identity_type in _HIGH_CONFIDENCE_CREATOR_TYPES else ('medium' if creator_valid else 'none'),
            'risk_level': 'identity_unknown',
            'known_scam': bool(
                identity_type == 'pump_curve_creator'
                and creator_valid and creator in self.known_scam_wallets
            ),
            'prior_token_count': 0,
            'prior_tokens_10m': 0,
            'prior_adverse_token_count': 0,
            'prior_exit_route_loss_count': 0,
            'prior_rapid_collapse_count': 0,
            'prior_token_mints': [],
            'prior_adverse_mints': [],
            'risk_evidence': [],
            'candidate_created_at_utc': utc_iso(created_epoch) if created_epoch is not None else None,
            'as_of_utc': utc_iso(as_of) if as_of is not None else None,
            'observed_at_utc': utc_iso(as_of) if as_of is not None else None,
            'latency_ms': round(decision_latency_ms, 2) if decision_latency_ms is not None else None,
            'lookahead_safe': True,
            'history_files_scanned': self._load_summary['files_scanned'],
            'candidate_eligible': bool(eligible),
        }

        if not creator_valid:
            profile['risk_evidence'].append('wallet_identity_missing_or_invalid')
        elif as_of is None:
            if profile['known_scam']:
                profile['risk_level'] = 'known_scam_manual'
                profile['risk_evidence'].append('creator_wallet_present_in_local_manual_blocklist')
            else:
                profile['risk_level'] = 'identity_time_unknown'
            profile['risk_evidence'].append('candidate_timestamp_missing')
        else:
            prior: list[CreatorToken] = []
            identity_key = (identity_type, str(creator))
            for prior_mint in self._by_creator.get(identity_key, set()):
                if prior_mint == mint:
                    continue
                token = self._tokens.get(prior_mint)
                if (
                    token is not None
                    and not token.identity_conflict
                    and token.created_epoch is not None
                    and token.created_epoch < as_of
                    and token.identity_observed_epoch is not None
                    and token.identity_observed_epoch <= as_of
                ):
                    prior.append(token)
            prior.sort(key=lambda token: (token.created_epoch or 0.0, token.mint))
            recent = [
                token for token in prior
                if 0 <= as_of - float(token.created_epoch or 0.0) <= REPEAT_DEPLOY_WINDOW_SEC
            ]

            adverse_mints: list[str] = []
            route_loss_mints: list[dict[str, Any]] = []
            collapse_mints: list[dict[str, Any]] = []
            high_confidence_adverse: set[str] = set()
            for token in prior:
                route_loss = _exit_route_loss(token, as_of)
                collapse = _rapid_collapse(token, as_of)
                if route_loss:
                    route_loss_mints.append({'mint': token.mint, **route_loss})
                    adverse_mints.append(token.mint)
                    if token.identity_type in _HIGH_CONFIDENCE_CREATOR_TYPES:
                        high_confidence_adverse.add(token.mint)
                if collapse:
                    collapse_mints.append({'mint': token.mint, **collapse})
                    adverse_mints.append(token.mint)
                    if token.identity_type in _HIGH_CONFIDENCE_CREATOR_TYPES:
                        high_confidence_adverse.add(token.mint)
            adverse_unique = sorted(set(adverse_mints))
            high_confidence_adverse_unique = {
                mint_value for mint_value in adverse_unique
                if mint_value in high_confidence_adverse
            }

            profile.update({
                'prior_token_count': len(prior),
                'prior_tokens_10m': len(recent),
                'prior_adverse_token_count': len(adverse_unique),
                'prior_exit_route_loss_count': len(route_loss_mints),
                'prior_rapid_collapse_count': len(collapse_mints),
                'prior_token_mints': [
                    {'mint': token.mint, 'source': token.source, 'created_at_utc': utc_iso(token.created_epoch)}
                    for token in reversed(prior[-10:])
                ],
                'prior_adverse_mints': adverse_unique[:10],
                'prior_exit_route_loss_evidence': route_loss_mints[:10],
                'prior_rapid_collapse_evidence': collapse_mints[:10],
                'automatic_block_evidence_count': len(high_confidence_adverse_unique),
            })

            if profile['known_scam']:
                profile['risk_level'] = 'known_scam_manual'
                profile['risk_evidence'].append('creator_wallet_present_in_local_manual_blocklist')
            elif (
                len(high_confidence_adverse_unique)
                >= MIN_PRIOR_ADVERSE_TOKENS_TO_FLAG
            ):
                profile['risk_level'] = 'suspected_repeat_adverse'
                profile['risk_evidence'].append(
                    'at_least_two_prior_creator_tokens_have_separate_adverse_evidence'
                )
            elif adverse_unique:
                profile['risk_level'] = 'prior_adverse_review'
                profile['risk_evidence'].append(
                    f'{len(adverse_unique)} prior token(s) have review-only adverse telemetry '
                    f'(rapid drawdown review threshold={RAPID_COLLAPSE_DROP_PCT:.0f}%)'
                )
            elif len(recent) >= BURST_PRIOR_TOKENS:
                profile['risk_level'] = 'rapid_launch_burst'
                profile['risk_evidence'].append(
                    f'{len(recent)} earlier token launches by the same wallet within 10 minutes; '
                    'launch count alone is review-only, not a block signal'
                )
            elif prior:
                profile['risk_level'] = 'repeat_creator'
                profile['risk_evidence'].append(
                    f'{len(prior)} earlier token(s) attributed to the same wallet'
                )
            else:
                profile['risk_level'] = 'first_seen'
                profile['risk_evidence'].append('no earlier token from this wallet in local telemetry')

        if not eligible:
            profile['candidate_eligible'] = False
            profile['risk_level'] = 'unverified_candidate'
            profile['risk_evidence'].append('excluded_from_creator_history_due_to_identity_rejection')

        if eligible and creator_valid and created_epoch is not None:
            self._put_candidate(
                mint, str(creator), source, created_epoch, identity_type,
                identity_observed_epoch=as_of,
            )
        packet['_creator_reputation'] = profile
        coin.packet = packet
        return profile

    def record_observation(
        self,
        mint: str,
        offset_sec: int,
        target_at_utc: str,
        observed_at_utc: str,
        status: str,
        price_usd: Optional[float],
        price_source: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> None:
        identity = event_id or f'{mint}\0{offset_sec}\0{observed_at_utc}'
        point = ObservationPoint(
            event_id=f'{mint}\0{identity}',
            offset_sec=int(offset_sec),
            target_epoch=parse_utc_epoch(target_at_utc),
            observed_epoch=parse_utc_epoch(observed_at_utc),
            status=str(status or ''),
            price_usd=_optional_float(price_usd),
            price_source=price_source,
        )
        token = self._tokens.get(mint)
        if token is None:
            self._pending_observations.setdefault(mint, {})[identity] = point
            return
        token.observations[identity] = point

    def record_quote(
        self,
        mint: str,
        stage: str,
        side: str,
        status: str,
        requested_at_utc: str,
        error_type: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> None:
        identity = event_id or f'{mint}\0{stage}\0{side}\0{requested_at_utc}\0{status}'
        point = QuotePoint(
            event_id=identity,
            stage=str(stage or ''),
            side=str(side or ''),
            status=str(status or ''),
            requested_epoch=parse_utc_epoch(requested_at_utc),
            error_type=error_type,
        )
        token = self._tokens.get(mint)
        if token is None:
            self._pending_quotes.setdefault(mint, {})[identity] = point
            return
        token.quotes[identity] = point

    def summary(self) -> dict[str, Any]:
        repeats = [
            (identity_key, mints) for identity_key, mints in self._by_creator.items()
            if len(mints) > 1
        ]
        return {
            **self._load_summary,
            'unique_tokens_with_creator': sum(
                1 for token in self._tokens.values() if token.creator and not token.identity_conflict
            ),
            'creator_wallets': len({address for _identity_type, address in self._by_creator}),
            'repeat_creator_wallets': len(repeats),
            'known_scam_wallets': len(self.known_scam_wallets),
        }

    def top_creators(self, limit: int = 20) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for (identity_type, creator), mint_set in self._by_creator.items():
            tokens = [self._tokens[mint] for mint in mint_set if mint in self._tokens]
            timestamps = [token.created_epoch for token in tokens if token.created_epoch is not None]
            if len(tokens) < 2:
                continue
            rows.append({
                'creator': creator,
                'identity_type': identity_type,
                'token_count': len(tokens),
                'first_seen_utc': utc_iso(min(timestamps)) if timestamps else None,
                'last_seen_utc': utc_iso(max(timestamps)) if timestamps else None,
                'mints_sample': [token.mint for token in sorted(
                    tokens, key=lambda item: item.created_epoch or 0.0, reverse=True
                )[:5]],
                'identity_types': sorted({token.identity_type for token in tokens}),
                'manual_blocklisted': identity_type == 'pump_curve_creator' and creator in self.known_scam_wallets,
            })
        rows.sort(key=lambda row: (-int(row['token_count']), str(row['creator'])))
        return rows[:max(0, int(limit))]


def creator_should_block(profile: Any, policy: str = 'warn') -> tuple[bool, Optional[str]]:
    """Return a block only in explicit block mode; warn/off never gate entry."""
    if not isinstance(profile, dict):
        return False, None
    normalized = str(policy or 'warn').strip().lower()
    if normalized != 'block':
        return False, None
    if profile.get('known_scam'):
        return True, 'creator_wallet_on_manual_scam_blocklist'
    if (
        profile.get('risk_level') == 'suspected_repeat_adverse'
        and bool(profile.get('identity_confidence') == 'high')
        and int(profile.get('automatic_block_evidence_count') or 0)
            >= MIN_PRIOR_ADVERSE_TOKENS_TO_FLAG
    ):
        return True, 'multiple_prior_tokens_from_creator_have_adverse_evidence'
    return False, None


def _exit_route_loss(token: CreatorToken, as_of: float) -> Optional[dict[str, Any]]:
    quotes = [
        quote for quote in token.quotes.values()
        if quote.side.lower() == 'sell'
        and quote.requested_epoch is not None
        and quote.requested_epoch <= as_of
    ]
    prechecks = [
        quote for quote in quotes
        if quote.status.lower() == 'ok'
        and quote.stage == 'entry_sell_precheck'
    ]
    exit_quotes = [quote for quote in quotes if quote.stage == 'position_exit']
    if not prechecks or not exit_quotes:
        return None

    latest_precheck = max(float(quote.requested_epoch or 0.0) for quote in prechecks)
    attempts = sorted(
        (quote for quote in exit_quotes if float(quote.requested_epoch or 0.0) > latest_precheck),
        key=lambda quote: float(quote.requested_epoch or 0.0),
    )
    if not attempts:
        return None

    last_success = max(
        (float(quote.requested_epoch or 0.0) for quote in attempts if quote.status.lower() == 'ok'),
        default=latest_precheck,
    )
    failures = [
        quote for quote in attempts
        if quote.status.lower() != 'ok'
        and float(quote.requested_epoch or 0.0) > last_success
    ]
    if len(failures) < MIN_EXIT_ROUTE_FAILURES:
        return None
    first_failed = float(failures[0].requested_epoch or 0.0)
    last_failed = float(failures[-1].requested_epoch or 0.0)
    span = max(0.0, last_failed - first_failed)
    if span < MIN_EXIT_ROUTE_FAILURE_SPAN_SEC:
        return None
    return {
        'failed_quote_count': len(failures),
        'failure_span_sec': round(span, 2),
        'first_failure_utc': utc_iso(first_failed),
        'last_failure_utc': utc_iso(last_failed),
        'evidence_type': 'sell_quote_unavailable_after_successful_entry_precheck',
    }


def _rapid_collapse(token: CreatorToken, as_of: float) -> Optional[dict[str, Any]]:
    points = [
        point for point in token.observations.values()
        if point.offset_sec <= RAPID_COLLAPSE_HORIZON_SEC
        and point.observed_epoch is not None
        and point.observed_epoch <= as_of
        and point.status.lower() == 'ok'
        and point.price_usd is not None
        and point.price_usd > 0
    ]
    by_offset: dict[int, ObservationPoint] = {}
    for point in sorted(points, key=lambda item: float(item.observed_epoch or 0.0)):
        # Keep the earliest real observation at an offset across copied run DBs;
        # later snapshots of the same mint must not rewrite an earlier feature.
        by_offset.setdefault(point.offset_sec, point)
    ordered = sorted(by_offset.values(), key=lambda item: item.offset_sec)
    if len(ordered) < 2 or ordered[0].offset_sec > 15 or ordered[-1].offset_sec < 60:
        return None

    final = ordered[-1]
    prior_peak = max(
        (float(point.price_usd or 0.0) for point in ordered[:-1]),
        default=0.0,
    )
    final_price = float(final.price_usd or 0.0)
    if prior_peak <= 0 or final_price > prior_peak * (1.0 - RAPID_COLLAPSE_DROP_PCT / 100.0):
        return None

    decline_pct = (1.0 - final_price / prior_peak) * 100.0
    return {
        'drop_pct': round(decline_pct, 2),
        'from_peak_usd': prior_peak,
        'last_observed_usd': final_price,
        'last_offset_sec': final.offset_sec,
        'observed_at_utc': utc_iso(float(final.observed_epoch or 0.0)),
        'evidence_type': 'observed_rapid_price_collapse_not_proof_of_fraud',
    }


def _optional_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
        return result if result == result and abs(result) != float('inf') else None
    except (TypeError, ValueError, OverflowError):
        return None


if __name__ == '__main__':
    project = Path(__file__).resolve().parent
    watcher = CreatorWatch(project / 'runs', project / 'runs' / 'known_scam_sellers.txt')
    summary = watcher.load_history()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print('\nTop repeated creator wallets (review candidates only; not scam labels):')
    for row in watcher.top_creators(20):
        print(
            f"{row['token_count']:>4} tokens | {row['first_seen_utc']} .. {row['last_seen_utc']} | "
            f"{row['creator']}"
        )
