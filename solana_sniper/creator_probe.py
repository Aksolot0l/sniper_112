"""Bounded, read-only creator probe for A/B telemetry and the B treatment gate.

Only public RPC reads are used. The candidate-time result is capped at 350 ms;
no transaction, signature, or private key is ever constructed or submitted.

Design notes (2026-10-22 revision):
- Identity: the Pump curve stores a creator field at raw[49:81]. Fresh rug
  factories point it at an empty fee-vault wallet. When that wallet has zero
  transaction history we fall back to the PumpPortal create actor
  (``traderPublicKey``) which is the wallet that actually paid for the launch.
- RPS: every ``getSignaturesForAddress`` answer is memoised per wallet for
  ``wallet_cache_sec`` seconds (in-process only, no disk, no cross-run reuse).
  Repeat launchers - exactly the wallets that matter for the spam check - cost
  zero extra RPC calls after the first sighting.
- Spam/burst detection: a wallet that produced 25+ transactions inside a
  ``burst_window_sec`` window is a minting bot, not an organic creator. The
  evidence is already inside the signature page (``oldest_block_time``), so no
  extra request is made.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

from creator_watch import is_solana_pubkey
from pumpportal_validation import (
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    derive_associated_token_address,
)
from telemetry import get_collector

DEFAULT_GUARD_MIN_SIGNATURES = 25
DEFAULT_BURST_WINDOW_SEC = 3600.0
MAX_SIG_CACHE_ENTRIES = 4000
MAX_ERROR_DETAIL_ENTRIES = 512


class RpcCallError(RuntimeError):
    """RPC failure that keeps the payload needed to explain itself.

    Run #1 lost 108 of 405 probes to a fail-closed ``RuntimeError`` in ~83 ms
    with no trace of the cause; the label alone cannot distinguish HTTP 429
    from a malformed request or an RPC-level error object.
    """

    def __init__(
        self,
        label: str,
        *,
        http_status: Optional[int] = None,
        code: Any = None,
        message: str = '',
        body: str = '',
        method: str = '',
    ) -> None:
        super().__init__(label)
        self.label = label
        self.http_status = http_status
        self.code = code
        self.message = str(message or '')[:200]
        self.body = str(body or '')[:200]
        self.method = method


class CreatorOnchainProbe:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        http_rpc_url: str,
        timeout_sec: float = 0.35,
        concurrency: int = 2,
        signature_cache_sec: float = 0.0,
        *,
        guard_min_signatures: int = DEFAULT_GUARD_MIN_SIGNATURES,
        burst_window_sec: float = DEFAULT_BURST_WINDOW_SEC,
        creator_watch: Any = None,
    ) -> None:
        self.session = session
        self.http_rpc_url = http_rpc_url
        # A strict cap prevents a caller from silently turning the gate into a
        # long wait.
        self.timeout_sec = min(0.35, max(0.05, float(timeout_sec)))
        self._sem = asyncio.Semaphore(max(2, int(concurrency)))
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        # Optional local telemetry index: gives a second serial-deploy signal
        # (prior_tokens_10m) with zero extra RPC calls.
        self.creator_watch = creator_watch
        self.wallet_cache_sec = max(0.0, float(signature_cache_sec))
        self.guard_min_signatures = max(0, int(guard_min_signatures))
        self.burst_window_sec = max(1.0, float(burst_window_sec))
        # wallet -> (observed_at_monotonic, signature summary, cached flag)
        self._sig_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        # wallet -> last failure description (diagnostics only, bounded).
        self._error_details: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------ RPC
    def _remember_error(self, method: str, target: str, exc: BaseException) -> None:
        """Keep one bounded description of the last failure per wallet.

        Behaviour-neutral: the guard still fails closed on the same exceptions;
        this only preserves *why* (HTTP status / JSON-RPC code / message) so a
        post-run review can separate a 429 from a malformed request.
        """
        detail = {
            'method': method,
            'target': target,
            'label': str(getattr(exc, 'label', '') or type(exc).__name__),
            'http_status': getattr(exc, 'http_status', None),
            'code': getattr(exc, 'code', None),
            'message': str(getattr(exc, 'message', '') or '')[:200],
            'body': str(getattr(exc, 'body', '') or '')[:200],
            'error_type': type(exc).__name__,
        }
        if len(self._error_details) >= MAX_ERROR_DETAIL_ENTRIES:
            self._error_details.clear()
        self._error_details[target] = detail

    async def _rpc(self, method: str, params: list[Any]) -> dict[str, Any]:
        payload = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
        async with asyncio.timeout(self.timeout_sec):
            async with self._sem:
                async with self.session.post(
                    self.http_rpc_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
                ) as response:
                    if response.status != 200:
                        body = ''
                        try:
                            body = (await response.text())[:200]
                        except Exception:
                            body = ''
                        raise RpcCallError(
                            f'rpc_http_{response.status}',
                            http_status=response.status, body=body, method=method,
                        )
                    data = await response.json()
        if not isinstance(data, dict) or data.get('error'):
            error = data.get('error') if isinstance(data, dict) else None
            if isinstance(error, dict):
                raise RpcCallError(
                    'rpc_error_response',
                    code=error.get('code'),
                    message=str(error.get('message') or ''),
                    body=str(error.get('data') or ''),
                    method=method,
                )
            raise RpcCallError('rpc_error_response', method=method)
        return data

    @staticmethod
    def _error_label(exc: BaseException) -> str:
        """Stable label used by reports; RpcCallError stays "RuntimeError".

        Run #1 reports count failures by exception class, so keeping the label
        identical lets run #2 be compared without re-learning the columns.
        """
        if isinstance(exc, RuntimeError):
            return 'RuntimeError'
        return type(exc).__name__

    async def _signatures_uncached(
        self, wallet: str,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        try:
            data = await self._rpc('getSignaturesForAddress', [wallet, {'limit': 25}])
            rows = (data.get('result') or [])
            if not isinstance(rows, list):
                raise RuntimeError('signature_result_not_list')
            block_times: list[int] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    block_time = int(row.get('blockTime'))
                    if block_time >= 0:
                        block_times.append(block_time)
                except (TypeError, ValueError, OverflowError):
                    pass
            observed_epoch = time.time()
            return {
                # This is a capped page count, not a wallet-age proof.
                'count': min(len(rows), 25),
                'oldest_block_time': min(block_times) if block_times else None,
                'observed_at_utc': _utc_iso(observed_epoch),
                'observed_at_epoch': observed_epoch,
                'cached': False,
            }, None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._remember_error('getSignaturesForAddress', wallet, exc)
            return None, self._error_label(exc)

    async def _signatures(
        self, wallet: str,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        """Signature page for one wallet, memoised for ``wallet_cache_sec``."""
        now = time.monotonic()
        if self.wallet_cache_sec > 0:
            cached = self._sig_cache.get(wallet)
            if cached is not None and (now - cached[0]) <= self.wallet_cache_sec:
                self.cache_hits += 1
                # A success must not inherit the previous failure of this wallet.
                self._error_details.pop(wallet, None)
                return dict(cached[1], cached=True), None
        result, error = await self._signatures_uncached(wallet)
        if result is not None:
            # Stale detail would make a clean probe look censored in the report.
            self._error_details.pop(wallet, None)
        if result is not None and self.wallet_cache_sec > 0:
            self.cache_misses += 1
            if len(self._sig_cache) >= MAX_SIG_CACHE_ENTRIES:
                # Drop the oldest quarter; cheap and bounded.
                for key, _ in sorted(self._sig_cache.items(), key=lambda item: item[1][0])[:MAX_SIG_CACHE_ENTRIES // 4]:
                    self._sig_cache.pop(key, None)
            self._sig_cache[wallet] = (now, result)
        return result, error

    async def _creator_token_accounts(
        self, creator: str, mint: str,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        expected_atas = {
            address for address in (
                derive_associated_token_address(creator, mint, TOKEN_PROGRAM_ID),
                derive_associated_token_address(creator, mint, TOKEN_2022_PROGRAM_ID),
            ) if address
        }
        if not expected_atas:
            return None, 'ata_derivation_failed'
        try:
            data = await self._rpc(
                'getTokenAccountsByOwner',
                [creator, {'mint': mint}, {
                    'encoding': 'jsonParsed', 'commitment': 'processed',
                }],
            )
            rows = ((data.get('result') or {}).get('value'))
            if not isinstance(rows, list):
                raise RuntimeError('token_accounts_result_not_list')
            ata_rows: list[tuple[int, int, str]] = []
            all_balance_raw = 0
            all_decimals: Optional[int] = None
            for row in rows:
                if not isinstance(row, dict):
                    continue
                pubkey = row.get('pubkey')
                account = row.get('account') or {}
                program_owner = account.get('owner') if isinstance(account, dict) else None
                parsed = ((account.get('data') or {}).get('parsed') or {}) if isinstance(account, dict) else {}
                info = parsed.get('info') if isinstance(parsed, dict) else None
                if (
                    program_owner not in {TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID}
                    or not isinstance(info, dict)
                    or info.get('owner') != creator
                    or info.get('mint') != mint
                ):
                    continue
                token_amount = info.get('tokenAmount') or {}
                try:
                    amount = int(token_amount.get('amount') or 0)
                    decimals = int(token_amount.get('decimals'))
                except (TypeError, ValueError, OverflowError):
                    continue
                if amount < 0 or decimals < 0 or decimals > 18:
                    continue
                all_balance_raw += amount
                all_decimals = decimals if all_decimals is None else all_decimals
                if pubkey in expected_atas:
                    ata_rows.append((amount, decimals, str(pubkey)))

            # A successful read clears any earlier failure of the same wallet,
            # so error_detail is never attached to a probe with empty errors[].
            self._error_details.pop(creator, None)
            ata_exists = bool(ata_rows)
            ata_balance_raw = sum(item[0] for item in ata_rows)
            decimals = ata_rows[0][1] if ata_rows else all_decimals
            divisor = 10 ** decimals if decimals is not None else 1
            return {
                # `ata_exists` is true only when the returned account address
                # matches the locally derived SPL or Token-2022 associated ATA.
                'ata_exists': ata_exists,
                'ata': ata_rows[0][2] if ata_rows else None,
                'balance_raw': str(ata_balance_raw),
                'balance_ui': ata_balance_raw / divisor if decimals is not None else 0.0,
                'total_owner_mint_balance_raw': str(all_balance_raw),
                'decimals': decimals,
                'accounts_count': len(rows),
            }, None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._remember_error('getTokenAccountsByOwner', creator, exc)
            return None, self._error_label(exc)

    # ------------------------------------------------------------- decision
    def _burst(self, summary: Optional[dict[str, Any]], now_epoch: float) -> tuple[Optional[float], bool]:
        """Return (wallet_age_sec, burst_activity) from an already-paid page."""
        if not summary:
            return None, False
        oldest = summary.get('oldest_block_time')
        try:
            oldest_epoch = float(oldest)
        except (TypeError, ValueError, OverflowError):
            return None, False
        age_sec = max(0.0, now_epoch - oldest_epoch)
        count = summary.get('count')
        try:
            saturated_page = int(count) >= 25
        except (TypeError, ValueError):
            saturated_page = False
        return age_sec, bool(saturated_page and age_sec < self.burst_window_sec)

    def _guard(
        self,
        *,
        signature_count: Optional[int],
        burst: bool,
        ata_exists: Optional[bool],
        balance_raw: Optional[str],
        prior_tokens_10m: int = 0,
    ) -> tuple[bool, str]:
        try:
            positive_balance = int(balance_raw or 0) > 0
        except (TypeError, ValueError, OverflowError):
            positive_balance = False
        if signature_count is None:
            return False, 'creator_history_unavailable'
        if signature_count < self.guard_min_signatures:
            return False, 'creator_history_too_short'
        if burst:
            return False, 'creator_burst_activity'
        # Second, RPC-free serial-deploy signal from local telemetry: the same
        # wallet already launched >= 2 other tokens inside the last 10 minutes.
        if prior_tokens_10m >= 2:
            return False, 'creator_repeat_deploy_in_session'
        if ata_exists is not True:
            return False, 'creator_token_ata_missing'
        if not positive_balance:
            return False, 'creator_token_balance_zero'
        return True, 'creator_guard_met'

    async def probe(self, coin: Any) -> dict[str, Any]:
        """Return one candidate-time probe result; either RPC may fail closed."""
        if self._closed:
            return {
                'status': 'unavailable', 'errors': ['probe_closed'],
                'signature_count': None, 'ata_exists': None,
                'token_balance_raw': None, 'creator_gate_passed': False,
                'creator_guard_passed': False, 'censored': True,
            }
        task = asyncio.create_task(self._probe(coin), name=f'creator-probe-{str(getattr(coin, "mint", ""))[:7]}')
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return await task

    def schedule(self, coin: Any) -> Optional[asyncio.Task[Any]]:
        """Compatibility helper for telemetry-only callers; does not gate them."""
        if self._closed:
            return None
        task = asyncio.create_task(self.probe(coin), name=f'creator-probe-scheduled-{str(getattr(coin, "mint", ""))[:7]}')
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _probe(self, coin: Any) -> dict[str, Any]:
        mint = str(getattr(coin, 'mint', '') or '')
        curve_creator = str(getattr(coin, 'program_creator', '') or '')
        packet = getattr(coin, 'packet', {})
        packet = packet if isinstance(packet, dict) else {}
        tx_payer = getattr(coin, 'tx_payer', None)
        event_actor = packet.get('pumpportal_event_actor')
        signature = str(getattr(coin, 'signature', '') or '')
        started_epoch = time.time()
        started_mono = time.monotonic()

        if not is_solana_pubkey(curve_creator) or not is_solana_pubkey(mint):
            return self._record_result(
                coin, {
                    'status': 'unavailable', 'errors': ['creator_or_mint_invalid'],
                    'signature_count': None, 'oldest_block_time': None,
                    'ata_exists': None, 'token_balance_raw': None,
                    'token_balance_ui': None, 'creator_gate_passed': False,
                    'creator_guard_passed': False, 'censored': True,
                    'latency_ms': round((time.monotonic() - started_mono) * 1000.0, 2),
                }, started_epoch, time.time(), tx_payer, event_actor, signature,
            )

        actor_valid = (
            is_solana_pubkey(event_actor) and str(event_actor) != curve_creator
        )
        # Stage 1: three reads in parallel. The actor's signature page is
        # prefetched only when it can possibly be the real creator, and it is
        # cached, so a repeat rug wallet costs nothing.
        stage1: dict[str, Any] = {
            'curve_sig': asyncio.create_task(self._signatures(curve_creator)),
            'curve_ata': asyncio.create_task(self._creator_token_accounts(curve_creator, mint)),
        }
        if actor_valid:
            stage1['actor_sig'] = asyncio.create_task(self._signatures(str(event_actor)))
        gathered = await asyncio.gather(*stage1.values(), return_exceptions=True)
        results = {}
        for name, value in zip(stage1, gathered):
            if isinstance(value, BaseException):
                results[name] = (None, type(value).__name__)
            else:
                results[name] = value

        curve_sig, curve_sig_error = results['curve_sig']
        curve_ata, curve_ata_error = results['curve_ata']
        actor_sig, actor_sig_error = results.get('actor_sig', (None, None))

        identity = 'pump_curve_creator'
        identity_source = 'pump_curve_raw_49_81'
        resolved = curve_creator
        sig_data, sig_error = curve_sig, curve_sig_error
        token_data, token_error = curve_ata, curve_ata_error

        curve_count = curve_sig.get('count') if curve_sig else None
        actor_count = actor_sig.get('count') if actor_sig else None

        # Stage 2: the curve creator is an empty fee wallet and the PumpPortal
        # actor is a real wallet -> treat the actor as the creator and read its
        # token account (one extra call, only in this situation).
        # curve_count is None when that read failed; treat it like "no usable
        # history" so a fresh fee-vault wallet does not hide the real deployer.
        if (
            actor_valid
            and curve_count in (0, None)
            and actor_count is not None and actor_count > 0
        ):
            identity = 'pumpportal_event_actor'
            identity_source = 'pumpportal_traderPublicKey_fallback'
            resolved = str(event_actor)
            sig_data, sig_error = actor_sig, actor_sig_error
            token_data, token_error = await self._creator_token_accounts(resolved, mint)

        errors = [error for error in (sig_error, token_error) if error]
        # Diagnostics only: why the reads failed (HTTP status / RPC code /
        # message), one entry per wallet touched by this probe.
        error_detail = [
            self._error_details[wallet]
            for wallet in dict.fromkeys(
                w for w in (curve_creator, str(event_actor or ''), resolved)
                if w and w in self._error_details
            )
        ]
        signature_count = sig_data.get('count') if sig_data else None
        ata_exists = token_data.get('ata_exists') if token_data else None
        balance_raw = token_data.get('balance_raw') if token_data else None
        observed_epoch = time.time()
        wallet_age_sec, burst = self._burst(sig_data, observed_epoch)
        prior_tokens_10m = 0
        actor_prior_tokens_10m: Optional[int] = None
        if self.creator_watch is not None:
            lookup = getattr(self.creator_watch, 'prior_tokens_10m', None)
            if callable(lookup):
                try:
                    prior_tokens_10m = int(
                        lookup(resolved, as_of=observed_epoch, current_mint=mint) or 0
                    )
                except Exception:
                    prior_tokens_10m = 0
                # Diagnostic only: the local index is keyed by program_creator,
                # so this number is usually 0 when the real deployer sits in
                # pumpportal_event_actor. Recorded (never used for gating) so a
                # post-run review can tell "no signal" from "signal is blind".
                if actor_valid:
                    try:
                        actor_prior_tokens_10m = int(
                            lookup(str(event_actor), as_of=observed_epoch,
                                   current_mint=mint) or 0
                        )
                    except Exception:
                        actor_prior_tokens_10m = None
        guard_passed, guard_reason = self._guard(
            signature_count=signature_count, burst=burst,
            ata_exists=ata_exists, balance_raw=balance_raw,
            prior_tokens_10m=prior_tokens_10m,
        )
        # Historical key kept for dashboards/exports: same semantics as before.
        gate_passed = guard_passed
        if not errors:
            status = 'ok'
        elif sig_data is not None or token_data is not None:
            status = 'partial'
        else:
            status = 'unavailable'
        censored = bool(
            errors or signature_count is None or ata_exists is None
        )
        values = {
            'status': status,
            'program_creator': curve_creator,
            'creator': resolved,
            'creator_identity_type': identity,
            'creator_identity_source': identity_source,
            'creator_identity_resolved': identity == 'pumpportal_event_actor',
            'tx_payer': tx_payer if is_solana_pubkey(tx_payer) else None,
            'tx_payer_source': packet.get('tx_payer_source'),
            'pumpportal_event_actor': event_actor if is_solana_pubkey(event_actor) else None,
            'transaction_signature': signature or None,
            'first_signer_probe_status': 'not_requested',
            'probe_started_at_utc': _utc_iso(started_epoch),
            'observed_at_utc': _utc_iso(observed_epoch),
            'latency_ms': round((time.monotonic() - started_mono) * 1000.0, 2),
            'getSignaturesForAddress_limit': 25,
            'signature_count': signature_count,
            'creator_signature_count_0_25': signature_count,
            'program_creator_sig_count': curve_count,
            'program_creator_sig_error': curve_sig_error,
            'pumpportal_event_actor_sig_count': actor_count,
            'oldest_block_time': sig_data.get('oldest_block_time') if sig_data else None,
            'creator_signature_oldest_block_time': sig_data.get('oldest_block_time') if sig_data else None,
            'signature_data_observed_at_utc': sig_data.get('observed_at_utc') if sig_data else None,
            'signature_data_cached': bool(sig_data.get('cached')) if sig_data else False,
            'creator_wallet_age_sec': round(wallet_age_sec, 3) if wallet_age_sec is not None else None,
            'creator_burst_activity': burst,
            'creator_burst_window_sec': self.burst_window_sec,
            'creator_prior_tokens_10m': prior_tokens_10m,
            'creator_repeat_deploy_in_session': prior_tokens_10m >= 2,
            # log-only diagnostic; deliberately NOT passed to _guard this run
            'creator_prior_tokens_10m_actor': actor_prior_tokens_10m,
            'creator_prior_tokens_10m_actor_path': (
                'log_only' if actor_prior_tokens_10m is not None else 'not_applicable'
            ),
            'ata_exists': ata_exists,
            'creator_token_ata_exists': ata_exists,
            'creator_token_ata': token_data.get('ata') if token_data else None,
            'token_balance_raw': balance_raw,
            'creator_token_balance_raw': balance_raw,
            'creator_token_balance_ui': token_data.get('balance_ui') if token_data else None,
            'creator_total_owner_mint_balance_raw': token_data.get('total_owner_mint_balance_raw') if token_data else None,
            'creator_token_decimals': token_data.get('decimals') if token_data else None,
            'creator_token_accounts_count': token_data.get('accounts_count') if token_data else None,
            'creator_guard_passed': guard_passed,
            'creator_guard_reason': guard_reason,
            'creator_gate_passed': gate_passed,
            'creator_gate_reason': 'creator_gate_met' if gate_passed else guard_reason,
            'censored': censored,
            'errors': errors,
            # run #1 blind spot: 108/405 probes failed with only the exception
            # class recorded, so a 429 could not be told from a bad request.
            'error_detail': error_detail or None,
            'error_codes': sorted({
                str(item.get('label')) for item in error_detail if item.get('label')
            }),
            'paper_only_read_only_rpc': True,
        }
        return self._record_result(
            coin, values, started_epoch, observed_epoch, tx_payer, event_actor, signature,
            signature_count=signature_count,
            oldest_block_time=values['oldest_block_time'],
            ata_exists=ata_exists,
            token_balance_raw=balance_raw,
            token_balance_ui=token_data.get('balance_ui') if token_data else None,
            error_detail=error_detail or None,
        )

    def _record_result(
        self, coin: Any, values: dict[str, Any], started_epoch: float,
        observed_epoch: float, tx_payer: Any, event_actor: Any, signature: str,
        signature_count: Optional[int] = None,
        oldest_block_time: Optional[int] = None,
        ata_exists: Optional[bool] = None,
        token_balance_raw: Optional[str] = None,
        token_balance_ui: Optional[float] = None,
        error_detail: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        mint = str(getattr(coin, 'mint', '') or '')
        source = str(getattr(coin, 'source', 'Pump.fun'))
        if signature_count is None:
            signature_count = values.get('signature_count')
        if oldest_block_time is None:
            oldest_block_time = values.get('oldest_block_time')
        if ata_exists is None:
            ata_exists = values.get('ata_exists')
        if token_balance_raw is None:
            token_balance_raw = values.get('token_balance_raw')
        if token_balance_ui is None:
            token_balance_ui = values.get('creator_token_balance_ui')
        collector = get_collector()
        if collector is not None:
            try:
                collector.record_creator_probe(
                    mint=mint, source=source,
                    program_creator=str(getattr(coin, 'program_creator', '') or '') or None,
                    tx_payer=tx_payer if is_solana_pubkey(tx_payer) else None,
                    status=str(values.get('status') or 'unavailable'),
                    values=values,
                    signature_count=signature_count,
                    oldest_block_time=oldest_block_time,
                    ata_exists=ata_exists,
                    token_balance_raw=token_balance_raw,
                    token_balance_ui=token_balance_ui,
                    latency_ms=values.get('latency_ms'),
                    observed_at_utc=values.get('observed_at_utc') or _utc_iso(observed_epoch),
                    error_type=','.join(values.get('errors') or []) or None,
                    error_detail=error_detail,
                )
            except Exception:
                pass
        return values

    def cache_stats(self) -> dict[str, Any]:
        return {
            'wallet_cache_sec': self.wallet_cache_sec,
            'entries': len(self._sig_cache),
            'hits': self.cache_hits,
            'misses': self.cache_misses,
        }

    async def close(self) -> None:
        self._closed = True
        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


def _utc_iso(epoch: float) -> str:
    return datetime.fromtimestamp(float(epoch), timezone.utc).isoformat(timespec='milliseconds')
