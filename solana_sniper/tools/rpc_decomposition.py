#!/usr/bin/env python3
"""Where the RPC requests of one finished run actually went.

Run #10a spent 6 565 `getMultipleAccounts` in 3 611 s (1.817 rps, 2.967 in the
worst 10-minute window). This script rebuilds that total from the run's own
artifacts — telemetry.sqlite and manifest.json — instead of estimating it, and
then applies the run #10b model to the same window:

  before the fix, one candidate cost
      filter read          [mint, curve, Global, FeeConfig], jsonParsed
    + observation "+0 s"   the same four accounts, moments later
    + observation "+5 s"   one curve read, a genuinely different moment
  after the fix the "+0 s" observation prices the snapshot the filter already
  read (same accounts, same slot) and costs no request at all; when a coin has
  no snapshot the ordinary read is taken. `observation_0_would_merge` reports
  how many of them the 0.25 s entry cache would have merged anyway, so the two
  mechanisms are visible side by side.

The poll loop (open positions + due ghosts) has no per-call timestamps in the
artifacts, so for a window it is taken from `rpc_metrics.requests_per_5sec` —
the run's own five-second histogram — minus the candidate-driven reads counted
from timestamps in that same window. Nothing is scaled: the window's total is
the histogram's sum, and the poll part is the remainder inside the window.

Usage:
    python3 tools/rpc_decomposition.py <run_dir> [--from-min 21 --to-min 31]
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

ENTRY_CACHE_WINDOW_SEC = 0.25


def _epoch(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return None


def _rows(db: sqlite3.Connection, sql: str, params: tuple = ()) -> list[tuple]:
    try:
        return db.execute(sql, params).fetchall()
    except sqlite3.Error:
        return []


def decompose(run_dir: Path, from_min: Optional[float], to_min: Optional[float]) -> dict[str, Any]:
    manifest = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
    rpc = manifest.get('rpc_metrics') or {}
    total = int(rpc.get('requests') or 0)
    duration = float(manifest.get('actual_duration_sec') or 0.0)
    db = sqlite3.connect(f'file:{(run_dir / "telemetry.sqlite").resolve()}?mode=ro', uri=True)

    started = _epoch(manifest.get('started_at_utc'))
    window_start = None
    window_end = None
    if from_min is not None and started is not None:
        window_start = started + 60.0 * from_min
        window_end = started + 60.0 * (to_min if to_min is not None else from_min + 10)

    def in_window(epoch: Optional[float]) -> bool:
        if window_start is None:
            return True
        return epoch is not None and window_start <= epoch < window_end

    # --- candidate-driven reads ------------------------------------------------
    filter_times: dict[str, float] = {}
    for mint, occurred in _rows(
        db, "SELECT mint, occurred_at_utc FROM decisions "
            "WHERE stage='security_and_market_filters'",
    ):
        epoch = _epoch(occurred)
        if epoch is not None:
            filter_times.setdefault(str(mint), epoch)

    filters = 0
    for mint, epoch in filter_times.items():
        if in_window(epoch):
            filters += 1
    filters_total = len(filter_times)

    obs_zero = 0
    obs_zero_merged = 0
    obs_five = 0
    obs_zero_total = 0
    obs_five_total = 0
    for mint, offset, observed, status in _rows(
        db, "SELECT mint, target_offset_sec, observed_at_utc, status FROM observations",
    ):
        if str(status).startswith('skipped'):
            continue
        epoch = _epoch(observed)
        if int(offset) == 0:
            obs_zero_total += 1
        elif int(offset) == 5:
            obs_five_total += 1
        if not in_window(epoch):
            continue
        if int(offset) == 0:
            obs_zero += 1
            filter_epoch = filter_times.get(str(mint))
            if filter_epoch is not None and epoch is not None:
                if abs(epoch - filter_epoch) <= ENTRY_CACHE_WINDOW_SEC:
                    obs_zero_merged += 1
        elif int(offset) == 5:
            obs_five += 1

    # --- poll loop ------------------------------------------------------------
    per_profile = rpc.get('requests_per_profile') or {}
    poll_with_positions = sum(int(v) for v in per_profile.values())
    # The remainder is taken from the WHOLE run: a window cannot be used to
    # derive a per-run rest, and scaling it afterwards keeps one assumption.
    known_total = filters_total + obs_zero_total + obs_five_total
    retries = int(rpc.get('retry_count') or 0)
    poll_remainder = max(0, total - known_total - retries)

    buckets = list(rpc.get('requests_per_5sec') or [])
    bucket_sec = float(rpc.get('requests_per_5sec_bucket_sec') or 5.0)

    def bucket_sum(start: Optional[float], end: Optional[float]) -> Optional[int]:
        if not buckets or start is None or end is None:
            return None
        # Half-open [start, end) — the same convention as the peak itself, so
        # a 600-second window is 120 buckets and not 121.
        first = max(0, int((start - started) // bucket_sec))
        last = min(len(buckets), math.ceil((end - started) / bucket_sec))
        return int(sum(buckets[first:last]))

    candidate_reads = filters + obs_five + max(0, obs_zero - obs_zero_merged)
    if window_start is None:
        window_total = int(sum(buckets)) if buckets else total
        poll_estimate = poll_remainder
        poll_note = 'remainder of the manifest total'
    else:
        measured = bucket_sum(window_start, window_end)
        if measured is None:
            # Run #10c §1 introduced the histogram; a run built before it has
            # none, and the poll part can only be scaled. Said out loud.
            share = (window_end - window_start) / duration if duration else 0.0
            window_total = round(total * share, 1)
            poll_estimate = round(poll_remainder * share, 1)
            poll_note = (
                f'legacy run without requests_per_5sec: scaled by {share:.3f}'
            )
        else:
            # The window total comes from the histogram, so the poll part is a
            # difference of measured numbers, not a rescale.
            window_total = measured
            poll_estimate = max(0, measured - candidate_reads)
            poll_note = 'histogram total minus candidate reads counted in the window'

    after = filters + obs_five + poll_estimate
    before = filters + obs_zero + obs_five + poll_estimate
    seconds = (window_end - window_start) if window_start is not None else duration
    return {
        'run_dir': run_dir.name,
        'window': (
            'whole run' if window_start is None
            else f'{from_min:g}-{(to_min if to_min is not None else from_min + 10):g} min'
        ),
        'seconds': round(seconds, 1),
        'manifest_requests': total,
        'known_sources_total': known_total,
        'filters': filters,
        'observation_0': obs_zero,
        'observation_0_merged_with_filter': obs_zero_merged,
        'observation_0_served_from_t0_snapshot': obs_zero,
        'observation_0_served_share': (
            round(obs_zero / obs_zero_total, 4) if obs_zero_total else None
        ),
        'observation_5': obs_five,
        'window_total_from_histogram': window_total,
        'poll_estimate': round(poll_estimate, 1),
        'poll_note': poll_note,
        'peak_10min_rps': rpc.get('requests_per_sec_max_10min'),
        'peak_10min_window_sec': rpc.get('requests_per_sec_max_10min_window_sec'),
        'peak_to_mean_ratio': rpc.get('requests_per_sec_peak_to_mean_ratio'),
        'buckets_available': len(buckets),
        'poll_with_positions_measured': poll_with_positions,
        'retry_count': retries,
        'before': round(before, 1),
        'after': round(after, 1),
        'before_rps': round(before / seconds, 3) if seconds else None,
        'after_rps': round(after / seconds, 3) if seconds else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_dir', type=Path)
    parser.add_argument('--from-min', type=float, default=None)
    parser.add_argument('--to-min', type=float, default=None)
    args = parser.parse_args()
    result = decompose(args.run_dir, args.from_min, args.to_min)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
