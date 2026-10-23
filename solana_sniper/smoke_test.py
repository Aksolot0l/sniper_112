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
from main_ab import _observation_offsets
from models import NewCoin, SwapQuote
from portfolio_ab import (
    POST_EXIT_SCHEDULE_SEC,
    POST_EXIT_WINDOW_SEC,
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
from telemetry import CandidateCollector, funnel_from_db, set_collector

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
    def __init__(
        self, name: str, birth: float, delta: float, *,
        probe: str = 'green', symbol: str | None = None,
    ):
        self.name = name
        self.birth = birth
        self.delta = delta
        self.probe = probe  # green | rugfarm | censored | thin
        # Run #9: the ticker is an entry parameter now, so the smoke has to be
        # able to launch the same symbol twice.
        self.symbol = symbol or name.upper()
        # Only an explicit ticker repeats across cycles; the rest get a unique
        # one per launch so the copy counter measures the scenario, not the
        # smoke loop's own period.
        self.symbol_explicit = symbol is not None


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
    # Run #9: a launch inside S2's band (birth >= 31.96, delta <= 1.00). The
    # untouched S0 buys it too — the floor is the whole difference between the
    # books, and the smoke must be able to exercise both.
    Scenario('live_s2_band', 32.4, 0.40),
    # Run #9 §2.2: the same ticker launched twice inside 120 s. S0 buys both
    # (they are different mints); S2 buys the first and refuses the second
    # with name_copies_120s. No RPC is involved.
    Scenario('copy_first', 32.4, 0.40, symbol='SAMECOIN'),
    Scenario('copy_second', 32.4, 0.40, symbol='SAMECOIN'),
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
    'live_s2_band': [(0, 0.0), (1, 0.02), (3, 0.05), (6, 0.03), (12, -0.01)],
    'copy_first': [(0, 0.0), (2, 0.03), (5, 0.01)],
    'copy_second': [(0, 0.0), (2, -0.02), (5, -0.03)],
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


def _coin(mint: str, scenario: Scenario, fees, symbol: str | None = None) -> NewCoin:
    coin = NewCoin(
        mint=mint, source='Pump.fun', signature=f'smoke-sig-{mint[-6:]}',
        curve=f'curve-{mint[-6:]}',
        packet={'v_sol': scenario.birth, 'symbol': symbol or scenario.symbol},
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
    exited_scenarios: dict[str, Scenario] = {}  # mint -> its price path, for ghosts
    counters = {'candidates': 0, 'entries': {}, 'closed': 0}
    started = time.monotonic()
    next_candidate = started
    tick = 0

    def feed_ghosts() -> None:
        """Run #9 §2.3 + run #10 §2a: the ghost, one scheduled point at a time.

        The live poll loop reads a ghost only when its next scheduled second is
        due, so the smoke does the same — otherwise it would prove a cadence
        the run does not use and report 20 marks per exit instead of 8.
        """
        now_epoch = time.time()
        due = simulator._due_post_exit_ghosts(now_epoch)
        for ghost in due:
            scenario = exited_scenarios.get(ghost['mint'])
            if scenario is None:
                continue
            held = time.time() - ghost['entry_at_epoch']
            path = PRICE_PATHS.get(scenario.name) or [(0, 0.0), (10, -0.03), (30, -0.08)]
            move = path[-1][1]
            for at, value in path:
                if held >= at:
                    move = value
            ghost_curve = _curve(
                max(1.0, (scenario.birth + scenario.delta) * (1.0 + move)),
                v_tokens=1_073_000_000_000_000,
            )
            ghost_price = ABPaperSimulator._mid_price_usd(ghost_curve, SOL_USD)
            if ghost_price:
                simulator._record_post_exit_mark(ghost, now_epoch, ghost_price)
                ghost['schedule_index'] = int(ghost.get('schedule_index', 0)) + 1
        simulator._ghosts = [
            ghost for ghost in simulator._ghosts
            if now_epoch < ghost['until_epoch']
            and int(ghost.get('schedule_index', 0)) < len(POST_EXIT_SCHEDULE_SEC)
        ]

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
            symbol = (
                scenario.symbol if scenario.symbol_explicit
                else f'{scenario.symbol}{counters["candidates"]}'
            )
            coin = _coin(mint, scenario, fees, symbol)
            v_now = scenario.birth + scenario.delta
            snapshot = EntrySnapshot(_curve(v_now), fees, 12, time.time())
            simulator.record_entry_state(
                coin, snapshot, scenario.birth, v_now, time.time(),
            )
            before = {name: len(book.positions) for name, book in simulator.books.items()}
            # Run #9 §2.2: the ticker counter is advanced once per candidate,
            # before any book decides, exactly as handle_candidate does it.
            name_copies = simulator.count_name_copies(coin)
            simulator.remember_symbol_launch(coin)
            # Every arm sees the same candidate and the same shared snapshot,
            # exactly as _on_candidate dispatches it in production.
            await asyncio.gather(*(
                simulator._consider_entry(
                    book, coin, snapshot, scenario.birth, v_now, SOL_USD, {},
                    time.time(), name_copies=name_copies,
                )
                for book in simulator.books.values()
            ))
            for name, book in simulator.books.items():
                if len(book.positions) > before[name]:
                    counters['entries'][name] = counters['entries'].get(name, 0) + 1
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
                # Same instrumentation the live poller applies, including the
                # tick stream: run #9's price_at_5s is read from it.
                pos.mfe_pct = max(pos.mfe_pct, pnl_pct)
                pos.mae_pct = min(pos.mae_pct, pnl_pct)
                mark_price = ABPaperSimulator._mid_price_usd(curve, SOL_USD)
                simulator._record_tick(
                    book, pos, pnl_pct, time.time(), mark_price,
                )
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
                        counters['closed'] += 1
                        kept = [row for row in kept if row[1] is not pos]
                        exited_scenarios[pos.mint] = scenario
            feed_ghosts()
        await asyncio.sleep(0.1)

    # Run #9 §2.3, the production drain: an exit in the last seconds of the
    # run still gets its +10 s ghost, otherwise the smoke would report a
    # truncated ghost and hide the defect it exists to catch.
    ghost_deadline = time.time() + POST_EXIT_WINDOW_SEC + 2.0
    while time.time() < ghost_deadline and simulator._ghosts:
        feed_ghosts()
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
        counters['closed'] += 1
        # The ghost needs the scenario to know which price path to walk.
        exited_scenarios[pos.mint] = scenario

    # The end-of-run close starts ghosts of its own; they need the same drain,
    # otherwise the last exits are reported with zero marks.
    ghost_deadline = time.time() + POST_EXIT_WINDOW_SEC + 2.0
    while time.time() < ghost_deadline and simulator._ghosts:
        feed_ghosts()
        await asyncio.sleep(0.1)

    # Run #7 artefacts: own mark stream + the periodic snapshot. The snapshot
    # helper is the same one the live run calls every 60 s (P0-5), so the smoke
    # proves it. There are no shadow books and no shadow fills any more.
    simulator.snapshot_artifacts(reason='smoke_end')
    summary = simulator.summary()
    (run_dir / 'ab_summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    from main_ab import _build_commit, _run_number, _summary_markdown
    smoke_manifest = {
        'run_id': run_id, 'status': 'completed', 'smoke': True, 'paper_only': True,
        'portfolio_scope': list(profiles),
        'duration_sec': round(seconds, 1), 'started_at_utc': datetime.now(timezone.utc).isoformat(),
        'entry': 'each experimental arm moves one entry boundary of its reference',
        'telemetry': {'observation_offsets_sec': list(_observation_offsets())},
        'entry_latency_ms': 150.0,
        'run7_single_difference': 'entry_band',
        'ab_test': {
            # Run #7: every experimental arm moves ONE entry boundary.
            'single_difference': 'entry_band',
            'reference_arm': 'portfolio_a',
            'arms': {
                name: {
                    'arm_id': profile.arm_id,
                    'entry_band_label': profile.entry_band_label,
                }
                for name, profile in profiles.items()
            },
            'decision_rule': (
                'an arm confirms its hypothesis iff the PAIRED mean(arm - A) '
                '>= +8 RUB, the 98.75% bootstrap CI (Bonferroni alpha 0.0125) '
                'excludes 0, and the sign is the same in both halves'
            ),
        },
        'strategies': {name: asdict(profile) for name, profile in profiles.items()},
        # Run #9: the report page must render offline, before a 12-hour run,
        # so the smoke writes the same fields the live manifest does.
        'run_number': _run_number(),
        'build': _build_commit(),
        'planned_duration_sec': int(seconds),
        'actual_duration_sec': summary.get('duration_sec', round(seconds, 1)),
        'signed_transactions': 0,
        'exit_mode': next(iter(profiles.values())).exit_mode,
        'exit_rule': ABPaperSimulator._exit_rule_label(next(iter(profiles.values()))),
        'early_stop_reason': summary.get('early_stop_reason'),
        'rpc_metrics': summary.get('rpc_metrics') or {},
        'entry_parameters': {
            name: {
                'arm_id': profile.arm_id,
                'v_sol_birth_min': profile.v_sol_birth_min,
                'v_sol_birth_max': profile.v_sol_birth_max,
                'delta_min_sol': profile.delta_min_sol,
                'delta_max_sol': profile.delta_max_sol,
                'max_name_copies_120s': profile.max_name_copies_120s,
                'name_copies_window_sec': profile.name_copies_window_sec,
                'entry_band_label': profile.entry_band_label,
                'exit_mode': profile.exit_mode,
                'entry_latency_ms': profile.entry_latency_ms,
            }
            for name, profile in profiles.items()
        },
    }
    (run_dir / 'manifest.json').write_text(
        json.dumps(smoke_manifest, ensure_ascii=False, indent=2), encoding='utf-8',
    )
    # The one-page report, rendered from the same data the live run writes.
    # A smoke that cannot render its own report is a broken entry point.
    try:
        (run_dir / 'SUMMARY.md').write_text(
            _summary_markdown(
                smoke_manifest, funnel_from_db(run_dir / 'telemetry.sqlite'), summary,
            ),
            encoding='utf-8',
        )
    except Exception as exc:  # the smoke must still say SMOKE OK/WARNING
        log(f'⚠️ SUMMARY.md not rendered in the smoke ({type(exc).__name__})', Color.YELLOW)

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
    entries_text = ', '.join(
        f"{simulator.books[name].profile.arm_id}={count}"
        for name, count in sorted(counters['entries'].items())
    )
    print(f"  кандидатов: {counters['candidates']}  входов: {entries_text}  "
          f"закрыто сделок: {counters['closed']}")
    print('\n'.join(portfolio_lines))
    funnel = snapshot_view.get('funnel', {})
    for arm in profiles:
        print(f"  {profiles[arm].arm_id} вход:",
              funnel.get('entry_reasons', {}).get(arm))
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
    entered = sum(counters['entries'].values())
    print('\nSMOKE OK' if entered else '\nSMOKE WARNING: no entries happened')
    return 0 if entered else 2


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
