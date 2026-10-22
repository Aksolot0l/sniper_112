"""Fast, dependency-free validation of PumpPortal Pump.fun create events.

The bonding-curve PDA must be derived from the mint named by the event. This
catches create_v2 payloads where PumpPortal labels a quote mint as the new mint.
All validation is local; no RPC or trading calls are made here.
"""
from __future__ import annotations

import hashlib
import math
from typing import Any

PUMP_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
WSOL_MINT = 'So11111111111111111111111111111111111111112'
_B58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
_B58_INDEX = {ch: i for i, ch in enumerate(_B58_ALPHABET)}
_P = 2**255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P
_PDA_MARKER = b'ProgramDerivedAddress'
ASSOCIATED_TOKEN_PROGRAM_ID = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'


def b58decode(value: str) -> bytes | None:
    """Decode base58, returning None for malformed input."""
    if not isinstance(value, str) or not value:
        return None
    number = 0
    try:
        for char in value:
            number = number * 58 + _B58_INDEX[char]
    except KeyError:
        return None
    body = number.to_bytes((number.bit_length() + 7) // 8, 'big') if number else b''
    leading_zeroes = len(value) - len(value.lstrip('1'))
    return b'\x00' * leading_zeroes + body


def b58encode(value: bytes) -> str:
    """Encode bytes as base58."""
    if not value:
        return ''
    number = int.from_bytes(value, 'big')
    result = ''
    while number:
        number, remainder = divmod(number, 58)
        result = _B58_ALPHABET[remainder] + result
    leading_zeroes = len(value) - len(value.lstrip(b'\x00'))
    if not result and not leading_zeroes:
        result = '1'
    return '1' * leading_zeroes + result


def _is_ed25519_point(compressed: bytes) -> bool:
    """Match Solana's compressed Edwards-Y decompression/on-curve test."""
    if len(compressed) != 32:
        return False
    encoded = int.from_bytes(compressed, 'little')
    sign = encoded >> 255
    y = encoded & ((1 << 255) - 1)
    if y >= _P:
        return False
    y_squared = (y * y) % _P
    denominator = (_D * y_squared + 1) % _P
    if denominator == 0:
        return False
    x_squared = ((y_squared - 1) * pow(denominator, _P - 2, _P)) % _P
    if x_squared == 0:
        return sign == 0
    return pow(x_squared, (_P - 1) // 2, _P) == 1


def find_program_address(seeds: list[bytes] | tuple[bytes, ...], program_id: str) -> str | None:
    """Derive a Solana PDA using the canonical descending-bump search."""
    program_bytes = b58decode(program_id)
    if program_bytes is None or len(program_bytes) != 32 or len(seeds) > 15:
        return None
    normalized: list[bytes] = []
    for seed in seeds:
        if not isinstance(seed, bytes) or len(seed) > 32:
            return None
        normalized.append(seed)
    seed_bytes = b''.join(normalized)
    for bump in range(255, -1, -1):
        digest = hashlib.sha256(seed_bytes + bytes([bump]) + program_bytes + _PDA_MARKER).digest()
        if not _is_ed25519_point(digest):
            return b58encode(digest)
    return None


def derive_pump_bonding_curve(mint: str) -> str | None:
    """Derive Pump.fun's `bonding-curve` PDA for `mint` (no RPC required)."""
    mint_bytes = b58decode(mint)
    if mint_bytes is None or len(mint_bytes) != 32:
        return None
    return find_program_address([b'bonding-curve', mint_bytes], PUMP_PROGRAM_ID)


def derive_associated_token_address(
    owner: str, mint: str, token_program_id: str = TOKEN_PROGRAM_ID,
) -> str | None:
    """Derive the ATA, including the token-program seed (SPL or Token-2022)."""
    owner_bytes = b58decode(owner)
    mint_bytes = b58decode(mint)
    token_program_bytes = b58decode(token_program_id)
    if any(item is None or len(item) != 32 for item in (owner_bytes, mint_bytes, token_program_bytes)):
        return None
    return find_program_address(
        [owner_bytes, token_program_bytes, mint_bytes], ASSOCIATED_TOKEN_PROGRAM_ID,
    )


def _explicit_quote_mint(raw: dict[str, Any]) -> str | None:
    for key in ('quoteMint', 'quote_mint', 'quoteMintAddress', 'quote_mint_address', 'quoteTokenMint'):
        value = raw.get(key)
        if isinstance(value, dict):
            value = value.get('mint') or value.get('address')
        if isinstance(value, str) and value.strip():
            normalized = value.strip()
            if normalized.upper() in {'SOL', 'WSOL'}:
                return WSOL_MINT
            return normalized
    return None


def validate_pumpportal_create(raw: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    """Return (valid, reason, evidence) for a PumpPortal create payload."""
    if not isinstance(raw, dict) or raw.get('txType') != 'create':
        return False, 'not_a_create_event', {}

    mint = raw.get('mint') if isinstance(raw.get('mint'), str) else ''
    curve = raw.get('bondingCurveKey') if isinstance(raw.get('bondingCurveKey'), str) else ''
    signature = raw.get('signature') if isinstance(raw.get('signature'), str) else ''
    quote_mint = _explicit_quote_mint(raw)
    evidence: dict[str, Any] = {
        'signature': signature or None,
        'event_mint': mint or None,
        'provided_curve': curve or None,
        'explicit_quote_mint': quote_mint,
    }

    mint_bytes = b58decode(mint)
    curve_bytes = b58decode(curve)
    signature_bytes = b58decode(signature)
    if mint_bytes is None or len(mint_bytes) != 32:
        return False, 'invalid_event_mint', evidence
    if mint == WSOL_MINT or mint in {
        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    }:
        return False, 'event_mint_is_quote_asset', evidence
    if curve_bytes is None or len(curve_bytes) != 32:
        return False, 'invalid_bonding_curve_address', evidence
    if signature_bytes is None or len(signature_bytes) != 64:
        return False, 'invalid_or_missing_signature', evidence

    if quote_mint is not None and quote_mint != WSOL_MINT:
        return False, 'non_sol_quote_mint', evidence

    expected_curve = derive_pump_bonding_curve(mint)
    evidence['expected_curve'] = expected_curve
    evidence['curve_pda_matches_mint'] = expected_curve == curve
    if expected_curve is None:
        return False, 'could_not_derive_curve_pda', evidence
    if expected_curve != curve:
        return False, 'bonding_curve_pda_mismatch', evidence

    try:
        v_tokens = float(raw.get('vTokensInBondingCurve') or 0.0)
        v_sol = float(raw.get('vSolInBondingCurve') or 0.0)
    except (TypeError, ValueError, OverflowError):
        return False, 'invalid_curve_reserves', evidence
    if not math.isfinite(v_tokens) or not math.isfinite(v_sol) or v_tokens <= 0 or v_sol <= 0:
        return False, 'invalid_curve_reserves', evidence

    evidence['v_tokens'] = v_tokens
    evidence['v_sol'] = v_sol
    return True, 'ok', evidence
