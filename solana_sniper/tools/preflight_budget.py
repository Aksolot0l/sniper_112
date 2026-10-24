#!/usr/bin/env python3
"""Run #4 pre-launch budget check (10-minute run).

Reads one finished run directory and answers three questions that must all be
"yes" before the 2-hour run is allowed to start:

1. RPC budget. Every candidate that passes the security filters costs one
   ``getMultipleAccounts`` read of four accounts (mint, curve, Global,
   FeeConfig) plus the bounded creator probe. The budget is
   ``requests <= 1.5 * (security_passing_coins * 6)`` — 6 reads per candidate
   is the honest ceiling, 1.5x is the noise allowance for null retries.

2. Back-pressure. Either no 429 was seen at all, or every 429 was recovered by
   the retry layer (``unrecovered_failures == 0`` with a non-empty
   ``retry_count``). A run that keeps 429s and still recovers them is fine.

3. Not the old failure mode. The stop-list threshold is 2 % on UNRECOVERED
   errors; a raw error rate above 2 % with zero unrecovered reads is *not* a
   reason to stay on the bench, but it must be reported.

Usage:
    python tools/preflight_budget.py runs/rocket2-ab-paper-<RUN_ID>
    python tools/preflight_budget.py runs/<dir> --json

Exit code 0 = go, 1 = no-go (the reason is printed).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BUDGET_READS_PER_COIN = 6.0
BUDGET_ALLOWANCE = 1.5
STOP_UNRECOVERED_RATE = 0.02


def _load(run_dir: Path) -> dict:
    """Summary + the funnel from manifest.json (the run writes both)."""
    path = run_dir / 'ab_summary.json'
    if not path.exists():
        raise SystemExit(f'no ab_summary.json in {run_dir}')
    summary = json.loads(path.read_text(encoding='utf-8'))
    manifest_path = run_dir / 'manifest.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        for key in ('funnel', 'rpc_metrics', 'entry_latency_ms'):
            if key not in summary and manifest.get(key) is not None:
                summary[key] = manifest[key]
    return summary


def _security_passing(summary: dict) -> int | None:
    """Coins that survived the security/market filters, from the funnel chain."""
    funnel = summary.get('funnel') or {}
    for key in ('security_pass', 'security_passed'):
        value = funnel.get(key)
        if isinstance(value, int):
            return value
    chain = str(funnel.get('chain') or '')
    # "candidates=6 -> security_pass=5 -> ..."
    for token in chain.replace('\n', ' -> ').split('->'):
        name, _, value = token.strip().partition('=')
        if name.strip() in ('security_pass', 'security_passed'):
            try:
                return int(value.strip())
            except ValueError:
                return None
    return None


def check(summary: dict) -> dict:
    rpc = summary.get('rpc_metrics') or {}
    requests = int(rpc.get('requests') or 0)
    http_429 = int(rpc.get('http_429_count') or 0)
    retries = int(rpc.get('retry_count') or 0)
    unrecovered = int(rpc.get('unrecovered_failures') or 0)
    coins = _security_passing(summary)

    budget = None if coins is None else BUDGET_ALLOWANCE * coins * BUDGET_READS_PER_COIN
    budget_ok = budget is None or requests <= budget
    backpressure_ok = http_429 == 0 or (unrecovered == 0 and retries > 0)
    rate = unrecovered / requests if requests else 0.0
    stop_ok = rate <= STOP_UNRECOVERED_RATE

    reasons: list[str] = []
    if coins is None:
        reasons.append('security_passing_coins unavailable in the funnel')
    elif not budget_ok:
        # One read per security-passing coin is the filter batch itself; the
        # rest is the exit ladder polling open slots (~1 read/s per slot) and
        # the scheduled observations. If this fires, look at the breakdown
        # before blaming the entry path: entries are the cheapest part.
        reasons.append(
            f'requests {requests} > budget {budget:.0f} '
            f'({BUDGET_ALLOWANCE}x of {coins} coins x '
            f'{BUDGET_READS_PER_COIN:.0f}); excess {requests - int(budget)} '
            f'reads — check position polling before the entry path'
        )
    if not backpressure_ok:
        reasons.append(
            f'http_429_count={http_429} with unrecovered={unrecovered}, '
            f'retry_count={retries}: back-pressure is not being recovered'
        )
    if not stop_ok:
        reasons.append(
            f'unrecovered_rate {rate:.2%} > {STOP_UNRECOVERED_RATE:.0%}: '
            f'the 2-hour run would hit the stop-list'
        )
    return {
        'go': not reasons,
        'reasons': reasons,
        'security_passing_coins': coins,
        'requests': requests,
        'request_budget': None if budget is None else round(budget, 1),
        'requests_per_coin': None if not coins else round(requests / coins, 2),
        'http_429_count': http_429,
        'retry_count': retries,
        'unrecovered_failures': unrecovered,
        'unrecovered_rate': round(rate, 6),
        'raw_failed_requests': int(rpc.get('failed_requests') or 0),
        'latency_ms_p95': rpc.get('latency_ms_p95'),
    }


def render(result: dict) -> str:
    lines = [
        'PREFLIGHT (run #4)',
        f"  go: {'YES' if result['go'] else 'NO'}",
        f"  security-passing coins: {result['security_passing_coins']}",
        f"  rpc requests: {result['requests']} / budget {result['request_budget']}"
        f" ({result['requests_per_coin']} per coin)",
        f"  http_429: {result['http_429_count']}, retries: {result['retry_count']}, "
        f"unrecovered: {result['unrecovered_failures']} "
        f"({result['unrecovered_rate']:.2%}), raw failures: "
        f"{result['raw_failed_requests']}",
        f"  rpc p95: {result['latency_ms_p95']} ms",
    ]
    for reason in result['reasons']:
        lines.append(f'  - {reason}')
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_dir', type=Path)
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)
    result = check(_load(args.run_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2) if args.json else render(result))
    return 0 if result['go'] else 1


if __name__ == '__main__':
    sys.exit(main())
