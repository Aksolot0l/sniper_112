#!/usr/bin/env python3
"""Offline end-to-end smoke test for the A/B paper engine.

This exercises the *real* code paths - entry rule, creator guard, exact Pump
curve quotes, the four approved exit modes, wallet persistence and SQLite
telemetry - against synthetic candidates, without any network access, without
Helius, and without a second bot process.

It is the only smoke test that can run inside a restricted sandbox. On a real
PC use ``run_local_session.py --smoke 150`` instead: that one talks to the live
Pumpportal stream and Helius read-only RPC.

Usage:
    python smoke_test.py --seconds 130
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import shutil
import struct
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from config import load_settings
from console import Color, log
from models import NewCoin, SwapQuote
from portfolio_ab import (
    ABPaperSimulator,
    EntrySnapshot,
    ExecutableQuote,
    profiles_from_settings,
)
from pump_curve_quote import (
    BONDING_CURVE_DISCRIMINATOR,
    FEE_CONFIG_DISCRIMINATOR,
    GLOBAL_DISCRIMINATOR,
    CurveState,
    make_fee_context,
    quote_buy_exact_quote_in,
    quote_sell_exact_tokens,
)
from pumpportal_validation import WSOL_MINT
from telemetry import CandidateCollector, set_collector

PROJECT = Path(__file__).resolve().parent
RUNS = PROJECT / 'runs'
LAMPORTS = 1_000_000_000
SOL_USD = 200.0


# --------------------------------------------------------------------- curves
def _fee_context():
    global_raw = bytearray(1_088)
    global_raw[:8] = GLOBAL_DISCRIMINATOR
    struct.pack_into('<Q', global_raw, 105, 95)
    struct.pack_into('<Q', global_raw, 154, 30)
    global_raw[1045] = 0
    struct.pack_into('<Q', global_raw, 1046, 1_000)

    fee_raw = bytearray(FEE_CONFIG_DISCRIMINATOR)
    fee_raw.extend(b'\x01' + bytes(32))
    fee_raw.extend(struct.pack('<QQQ', 0, 95, 30))
    fee_raw.extend(struct.pack('<I', 1))
    fee_raw.extend((0).to_bytes(16, 'little'))
    fee_raw.extend(struct.pack('<QQQ', 0, 95, 30))
    fee_raw.extend(struct.pack('<I', 0))
    fee_raw.extend(struct.pack('<QQQ', 0, 0, 0))
    return make_fee_context(bytes(global_raw), bytes(fee_raw), context_slot=12)


def _curve(v_quote_sol: float, v_tokens: int = 1_073_000_000_000_000) -> CurveState:
    raw = bytearray(166)
    raw[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into('<QQQQQ', raw, 8, v_tokens, int(v_quote_sol * LAMPORTS),
                     793_100_000_000_000, 0, 1_000_000_000_000_000)
    raw[48] = 0
    raw[49:81] = bytes([7]) * 32
    raw[83:115] = bytes(32)
    return CurveState.decode(bytes(raw))


# ------------------------------------------------------------------ scenarios
class Scenario:
    def __init__(self, name: str, birth: float, delta: float, *, probe: str = 'green'):
        self.name = name
        self.birth = birth
        self.delta = delta
        self.probe = probe  # green | rugfarm | censored | thin


SCENARIOS = [
    # Dead launches (delta == 0): 8 of 9 such fills lost money in live data.
    Scenario('dead_at_30', 30.0, 0.0),
    Scenario('dead_at_31', 31.0, 0.0),
    # Live early momentum: the new entry rule.
    Scenario('live_small', 31.0, 0.35),
    Scenario('live_medium', 32.4, 2.10),
    # Big-delta launches are now late chases (cap 2.5 SOL) and must be skipped.
    Scenario('rocket_turtle', 33.95, 6.60),
    Scenario('rocket_performa', 39.88, 0.53),
    # Live momentum just under the cap, and the cap boundary itself.
    Scenario('live_edge', 32.0, 2.49),
    Scenario('late_chase_boundary', 32.0, 2.51),
    # Out of band.
    Scenario('too_late_birth', 55.0, 1.0),
    Scenario('late_chase', 31.5, 15.0),
]

PRICE_PATHS = {
    # name -> list of (seconds, price move factor)
    'live_small': [(0, 0.0), (2, 0.03), (4, 0.05), (10, 0.04), (20, 0.10), (40, 0.32)],
    'live_medium': [(0, 0.0), (2, -0.05), (4, -0.12), (6, -0.14)],
    'rocket_turtle': [(0, 0.0), (2, 0.18), (4, 0.42), (6, 0.61), (8, 0.55), (10, 0.70)],
    'rocket_performa': [(0, 0.0), (2, 0.02), (4, 0.01), (8, 0.03), (15, 0.02)],
    'live_edge': [(0, 0.0), (2, 0.08), (4, 0.15), (6, 0.34), (8, 0.62), (10, 0.55)],
    'late_chase_boundary': [(0, 0.0), (4, -0.04), (10, -0.06)],
}


def _probe_for(kind: str) -> dict[str, Any]:
    if kind == 'rugfarm':
        return {
            'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '1000000',
            'creator_guard_passed': False, 'creator_guard_reason': 'creator_burst_activity',
            'creator_burst_activity': True, 'creator_wallet_age_sec': 900.0,
            'creator_identity_type': 'pump_curve_creator', 'censored': False,
        }
    if kind == 'censored':
        return {
            'signature_count': None, 'ata_exists': None, 'token_balance_raw': None,
            'creator_guard_passed': False, 'creator_guard_reason': 'creator_history_unavailable',
            'censored': True, 'errors': ['RuntimeError'],
        }
    if kind == 'thin':
        return {
            'signature_count': 3, 'ata_exists': True, 'token_balance_raw': '500000',
            'creator_guard_passed': False, 'creator_guard_reason': 'creator_history_too_short',
            'censored': False,
        }
    return {
        'signature_count': 25, 'ata_exists': True, 'token_balance_raw': '9000000',
        'creator_guard_passed': True, 'creator_guard_reason': 'creator_guard_met',
        'creator_wallet_age_sec': 86_400 * 40, 'creator_burst_activity': False,
        'creator_identity_type': 'pump_curve_creator', 'censored': False,
    }


def _coin(mint: str, scenario: Scenario, fees) -> NewCoin:
    coin = NewCoin(
        mint=mint, source='Pump.fun', signature=f'smoke-sig-{mint[-6:]}',
        curve=f'curve-{mint[-6:]}', packet={'v_sol': scenario.birth},
    )
    coin.decimals = 6
    coin.creator_probe_result = _probe_for(scenario.probe)
    return coin


def _executable(curve: CurveState, fees, tokens_raw: int) -> tuple[ExecutableQuote, Any]:
    local = quote_sell_exact_tokens(curve, fees, tokens_raw, slippage_bps=100, context_slot=12)
    swap = SwapQuote(
        input_mint='mint', output_mint=WSOL_MINT, in_amount_raw=tokens_raw,
        out_amount_raw=local.out_amount_raw, min_out_raw=local.min_out_raw,
        price_impact_pct=local.price_impact_pct,
        route_labels=local.route_labels(), context_slot=12,
    )
    return ExecutableQuote(
        swap, local.protocol_fee_raw, local.creator_fee_raw,
        local.protocol_fee_bps, local.creator_fee_bps,
        local.market_cap_lamports, local.tier_index,
        'Pump.fun bonding curve local exact quote',
    ), local


async def run(seconds: float) -> int:
    random.seed(7)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-smoke'
    run_dir = RUNS / f'smoke-{run_id}'
    if run_dir.exists():
        shutil.rmtree(run_dir)
    (run_dir / 'exports').mkdir(parents=True, exist_ok=True)

    settings = load_settings()
    settings = SimpleNamespace(**{**vars(settings), 'honeypot_check': False})
    profiles = profiles_from_settings(settings)
    fees = _fee_context()
    trades_queue: asyncio.Queue = asyncio.Queue()

    collector = CandidateCollector(
        str(run_dir / 'telemetry.sqlite'), str(run_dir / 'exports'),
        shadow_notional_rub=0.0, enable_shadow_quotes=False,
    )
    await collector.start()
    set_collector(collector)

    simulator = ABPaperSimulator(
        session=object(), settings=settings, run_id=run_id, run_dir=run_dir,
        trades_queue=trades_queue, creator_watch=object(), creator_probe=object(),
        profiles=profiles,
    )
    kept: list[tuple[Any, Any, Scenario, Any]] = []  # (book, position, scenario, fee ctx)
    counters = {'candidates': 0, 'entries_a': 0, 'entries_b': 0, 'closed': 0}
    started = time.monotonic()
    next_candidate = started
    tick = 0

    log(f'SMOKE: offline A/B engine run for {seconds:.0f}s in a single foreground process',
        Color.CYAN)
    while True:
        now = time.monotonic()
        elapsed = now - started
        if elapsed >= seconds:
            break

        # --- new synthetic candidate roughly every 1.2 s -------------------
        if now >= next_candidate:
            next_candidate = now + 1.2
            scenario = SCENARIOS[counters['candidates'] % len(SCENARIOS)]
            probe_kind = scenario.probe
            if scenario.name.startswith('rocket_turtle'):
                probe_kind = 'rugfarm'
            elif scenario.name.startswith('rocket_performa'):
                probe_kind = 'censored'
            elif scenario.name == 'live_medium':
                probe_kind = 'thin'
            scenario.probe = probe_kind
            counters['candidates'] += 1
            mint = f'SMOKE{counters["candidates"]:04d}mint'.ljust(32, 'x')
            coin = _coin(mint, scenario, fees)
            v_now = scenario.birth + scenario.delta
            snapshot = EntrySnapshot(_curve(v_now), fees, 12, time.time())
            simulator.record_entry_state(
                coin, snapshot, scenario.birth, v_now, time.time(),
            )
            before_a = len(simulator.books['portfolio_a'].positions)
            before_b = len(simulator.books['portfolio_b'].positions)
            # Live arms first, then the mirrors (they copy A's entries), the
            # same order _on_candidate uses in production.
            await asyncio.gather(*(
                simulator._consider_entry(
                    book, coin, snapshot, scenario.birth, v_now, SOL_USD, {},
                    time.time(),
                )
                for book in simulator.books.values() if not book.profile.mirror_of
            ))
            await asyncio.gather(*(
                simulator._consider_entry(
                    book, coin, snapshot, scenario.birth, v_now, SOL_USD, {},
                    time.time(),
                )
                for book in simulator.books.values() if book.profile.mirror_of
            ))
            shadow_names = [name for name in simulator.books if simulator.books[name].profile.is_shadow]
            for name, before in (('portfolio_a', before_a), ('portfolio_b', before_b)):
                book = simulator.books[name]
                if len(book.positions) > before:
                    counters['entries_a' if name == 'portfolio_a' else 'entries_b'] += 1
                    for pos in book.positions.values():
                        if pos.mint == mint:
                            kept.append((book, pos, scenario, _curve(v_now)))
            # Shadows are marked and exited too; they are not A/B counters.
            for name in shadow_names:
                book = simulator.books[name]
                for pos in book.positions.values():
                    if pos.mint == mint:
                        kept.append((book, pos, scenario, _curve(v_now)))

        # --- mark open positions every 0.5 s -------------------------------
        tick += 1
        if tick % 5 == 0:
            for book, pos, scenario, entry_curve in list(kept):
                if pos.mint not in book.positions:
                    continue
                held = time.time() - pos.opened_at_epoch
                path = PRICE_PATHS.get(scenario.name) or [(0, 0.0), (10, -0.03), (30, -0.08)]
                move = path[-1][1]
                for at, value in path:
                    if held >= at:
                        move = value
                curve = _curve(scenario.birth + scenario.delta + (0.0), v_tokens=1_073_000_000_000_000)
                # Shift the quote reserve to emulate a price move.
                curve = _curve(max(1.0, (scenario.birth + scenario.delta) * (1.0 + move)))
                executable, local = _executable(curve, fees, pos.remaining_tokens_raw)
                proceeds_rub = (
                    local.out_amount_raw / LAMPORTS * SOL_USD * settings.usd_rub_rate
                )
                pnl_rub = proceeds_rub - pos.remaining_cost_rub
                pnl_pct = (
                    pnl_rub / pos.remaining_cost_rub * 100.0
                    if pos.remaining_cost_rub else 0.0
                )
                ABPaperSimulator._update_best_pnl(pos, book.profile, pnl_pct, time.time())
                # Same instrumentation the live poller applies.
                pos.mfe_pct = max(pos.mfe_pct, pnl_pct)
                pos.mae_pct = min(pos.mae_pct, pnl_pct)
                trigger = ABPaperSimulator._exit_trigger(
                    simulator, book.profile, pos, pnl_pct, time.time(),
                )
                if trigger is not None:
                    reason, amount = trigger
                    pos.exit_trigger_pnl_pct = round(pnl_pct, 6)
                    executable, _ = _executable(curve, fees, min(amount, pos.remaining_tokens_raw))
                    await simulator._apply_sell_fill(
                        book, pos, min(amount, pos.remaining_tokens_raw), reason,
                        executable, curve, fees, 12, SOL_USD, pnl_pct,
                        pnl_rub + pos.remaining_cost_rub,
                    )
                    book.save(run_id)
                    if pos.mint not in book.positions:
                        if not book.profile.is_shadow:
                            counters['closed'] += 1
                        kept = [row for row in kept if row[1] is not pos]
        await asyncio.sleep(0.1)

    # --- close everything (paper accounting, not synthetic exits) ---------
    for book, pos, scenario, curve in list(kept):
        if pos.mint not in book.positions:
            continue
        executable, local = _executable(curve, fees, pos.remaining_tokens_raw)
        proceeds_rub = local.out_amount_raw / LAMPORTS * SOL_USD * settings.usd_rub_rate
        pnl_rub = proceeds_rub - pos.remaining_cost_rub
        pnl_pct = pnl_rub / pos.remaining_cost_rub * 100.0 if pos.remaining_cost_rub else 0.0
        await simulator._apply_sell_fill(
            book, pos, pos.remaining_tokens_raw, 'SMOKE end-of-run close',
            executable, curve, fees, 12, SOL_USD, pnl_pct,
            pnl_rub + pos.remaining_cost_rub,
        )
        book.save(run_id)
        if not book.profile.is_shadow:
            counters['closed'] += 1

    # Run #3 artefacts: own mark stream + shadow fills.
    export_dir = run_dir / 'exports'
    simulator.write_ticks(export_dir / 'ticks.csv')
    simulator.write_shadow_fills(export_dir / 'shadow_fills.csv')
    summary = simulator.summary()
    (run_dir / 'ab_summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    (run_dir / 'manifest.json').write_text(json.dumps({
        'run_id': run_id, 'status': 'completed', 'smoke': True, 'paper_only': True,
        'duration_sec': round(seconds, 1), 'started_at_utc': datetime.now(timezone.utc).isoformat(),
        'entry': (
            '30.5 <= v_sol_birth <= 34.5 and '
            'A: 0.10 <= delta <= 2.5 ; B: 0.00 <= delta <= 34.5 - v_sol_birth'
        ),
        'entry_latency_ms': 150.0,
        'ab_test': {
            'single_difference': 'delta_min',
            'decision_rule': (
                'B beats A iff mean(B) - mean(A) >= +8 RUB and the 95% CI '
                'of the difference excludes 0'
            ),
        },
        'strategies': {name: asdict(profile) for name, profile in profiles.items()},
    }, ensure_ascii=False, indent=2), encoding='utf-8')

    await collector.close()
    set_collector(None)

    # Prove the dashboard can build its payload for this run.
    from live_ab_dashboard import make_snapshot
    snapshot_view = make_snapshot(run_dir)
    portfolio_lines = []
    for portfolio in snapshot_view['portfolios']:
        portfolio_lines.append(
            f"  {portfolio['name']}: кэш={portfolio['cash_balance_rub']} ₽ "
            f"realized={portfolio['realized_pnl_rub']} ₽ "
            f"open={portfolio['open_count']} closed={portfolio['closed_trade_count']}"
        )

    print()
    log('SMOKE RESULT', Color.GREEN)
    print(f"  кандидатов: {counters['candidates']}  "
          f"входов A={counters['entries_a']} B={counters['entries_b']}  "
          f"закрыто сделок: {counters['closed']}")
    print('\n'.join(portfolio_lines))
    funnel = snapshot_view.get('funnel', {})
    print('  A вход:', funnel.get('entry_reasons', {}).get('portfolio_a'))
    print('  B guard:', funnel.get('guard_reasons'))
    # Bank addendum: top-ups and the moment a real 1500 RUB bank would die.
    for name, book in summary.items():
        if not isinstance(book, dict) or 'top_ups' not in book:
            continue
        bust = book.get('bust_at_trade')
        print(f"  банк {name}: пополнений={book['top_ups']}, "
              f"влито={book.get('capital_injected_rub')} ₽, "
              f"equity без пополнений={book.get('equity_no_topups')} ₽, "
              f"умер бы={'нет' if bust is None else '№' + str(bust)}")
    for line in _bucket_report(run_dir / 'telemetry.sqlite'):
        print(line)
    print(f'  run_dir: {run_dir}')
    print(f'  dashboard freshness: {snapshot_view["freshness"]}')
    print('\nSMOKE OK' if counters['entries_a'] else '\nSMOKE WARNING: no entries happened')
    return 0 if counters['entries_a'] else 2


def _bucket_report(db_path: Path) -> list[str]:
    """Funnel by delta bucket and counterfactual cap, straight from telemetry."""
    import sqlite3
    if not db_path.exists():
        return ['  бакеты delta: телеметрия недоступна']
    buckets: dict[str, list[int]] = {}
    caps = ('pass_if_cap_1', 'pass_if_cap_2.5', 'pass_if_cap_3', 'pass_if_cap_4', 'pass_if_cap_12')
    cap_hits = {cap: 0 for cap in caps}
    total = 0
    try:
        with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True) as db:
            rows = db.execute(
                "SELECT values_json FROM decisions WHERE stage='entry_state'"
            ).fetchall()
    except sqlite3.Error as exc:
        return [f'  бакеты delta: ошибка чтения ({type(exc).__name__})']
    for (raw,) in rows:
        try:
            values = json.loads(raw or '{}')
        except ValueError:
            continue
        bucket = str(values.get('delta_bucket') or '?')
        # "прошли бы" = то же правило входа при текущем потолке 2.5 SOL
        entered = bool(values.get('pass_if_cap_2.5'))
        slot = buckets.setdefault(bucket, [0, 0])
        slot[0] += 1
        slot[1] += int(entered)
        total += 1
        for cap in caps:
            if values.get(cap):
                cap_hits[cap] += 1
    order = ['0', '0.1-1', '1-2.5', '2.5-4', '4-12', '>12']
    lines = [f'  бакеты delta (всего entry_state: {total}):']
    for label in order + sorted(set(buckets) - set(order)):
        if label not in buckets:
            continue
        seen, passed = buckets[label]
        lines.append(f'    {label:>7}: {seen:>4} кандидатов, прошли бы вход: {passed}')
    lines.append('  shadow-потолки (сколько кандидатов прошли бы): '
                 + ', '.join(f'{cap}={cap_hits[cap]}' for cap in caps))
    return lines


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--seconds', type=float, default=130.0)
    args = parser.parse_args()
    return asyncio.run(run(args.seconds))


if __name__ == '__main__':
    sys.exit(main())
