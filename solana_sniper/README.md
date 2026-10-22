# Solana Sniper — paper A/B workspace

## Scope and safety

- **Paper A/B only** (`portfolio_scope=ab_only`). No signing, no submitting, no real funds.
- RPC access is read-only. Never put Helius/API credentials in source files, manifests, logs, or archives.
- One bot process at a time (Helius RPS budget). `run_local_session.py` refuses a second session.

## Quick start (local PC, 2h30m)

```
Windows:      START_WINDOWS.bat
macOS/Linux:  bash START_MAC_LINUX.sh
```

Both scripts create `.venv`, install `requirements.txt`, ask for `HELIUS_API_KEY`
(kept in memory only) and run **in the foreground** — never with `nohup ... &`,
which is what froze all six previous runs inside the cloud sandbox.

Short live check first: `bash START_MAC_LINUX.sh --smoke 150`.

Dashboard: <http://127.0.0.1:8767/> (served by the runner from the same process).
`paper_ab_dashboard.html` also works when opened straight from disk.

Offline engine smoke test (no network, no key): `python smoke_test.py --seconds 130`.

## Active code and tests

- Launcher (Linux/macOS, Helius preflight): `run_rocket2_ab_paper.sh`
- Cross-platform runner + dashboard + heartbeat: `run_local_session.py`
- Entry point: `main_ab.py`
- A/B portfolio, entry rule, exit ladder: `portfolio_ab.py`, `pump_curve_quote.py`, `price.py`
- Creator identity / guard / wallet cache: `creator_probe.py`
- Telemetry: `telemetry.py`; monitor: `live_ab_dashboard.py` + `paper_ab_dashboard.html`
- Tests: `tests/` (165 tests). Bundle builder: `tools/build_bundle.py`

## Strategy state (2026-10-22)

Entry v3 — live momentum, shared by both arms:

```
30.5 <= v_sol_birth <= 34.5   and   0.10 <= (v_sol_now - v_sol_birth) <= 2.5
```

Birth ceiling 34.5 (fix #7, was 50): run #1 measured
`v_sol_birth = 29.96 + dev_buy_SOL` (corr 0.99988, n=145), so a 50 SOL ceiling
admitted launches where the dev bought 4–6 SOL of his own token — 36 of 50 such
candidates fell ≤ −25% (median minimum −33.2%), and in the live run they were 4
mints for −538.57 ₽ in A and −552.79 ₽ in B. 34.5 keeps dev buys up to ~4.5 SOL.
This cuts the catastrophic tail; it does **not** create an edge.

Cap 2.5 (not 12): across the 66 archived `entry_state` rows joined with
`observations`, `delta >= 2.5 SOL` gave a median -19.8% drawdown from the entry
point and 10 of 15 such entries went to the stop; Turtle (11.56 SOL) ran -18.7%
within 15 s of its own entry. The 12 SOL ceiling fitted the filter to a single
observation.

The previous `v_sol_now <= v_sol_birth + 0.8` cap was an anti-filter: it rejected
real runners seen in the Solscan/Jupiter reconciliation (Turtle 33.95→45.51,
Shigure 33.16→38.59) while buying dead `delta == 0` decoys (8 of 9 such fills
lost money). CAVELON (36.0) and PERFORMA (39.88) are **not** affected by that
anti-filter any more: the 34.5 birth ceiling rejects them as `birth_out_of_band`.

Exits — approved 4-mode ladder, 1:1 (unchanged this run; next experiment): SL −10% before TP1/rocket; no-progress
(5 s, peak < +4%, PnL < +1.5%); positive stagnation (4.5 s, PnL ≥ +1.5%);
TP1 +30% sells 50%; peak retain `pnl <= 0.85 * best`; rocket bypass at +50%
keeps 100%. Timeouts 35 s / 90 s remain as slot-safety nets.

**Run #4 arms (active).** The live arms again differ in exactly one number,
but this time it is `delta_min`: `portfolio_a = 0.10`, `portfolio_b = 0.00`.
B therefore enters on the first on-chain curve read after detection and pays
no premium for waiting for momentum; A is the unchanged run-#3 config. Because
B never waits, its delta cap is derived from the same birth ceiling
(`34.5 − v_sol_birth`, i.e. 2.5 SOL at the 32 SOL mid-band) so that the band
can never be the second difference. Exits, birth band, 3 slots and the 500 ₽
stake are frozen.

`entry_latency_ms = 150` is a declared parameter (manifest + every trade): no
fill may be booked earlier than `found_at + 150 ms`, and the fill price always
comes from the first curve read, never from the WSS packet. Each trade records
the measured `entry_lag_ms`, whether the floor was met, and two separate
premiums: `entry_premium_pct` (entry mid vs the coin's t0 mid — the price of
waiting, expected ≤ 0.3 % for B and ≈ 2.1 % for A) and
`entry_fill_premium_pct` (fees + impact inside the same snapshot), so the fee
line is never read as the cost of waiting. The shadow `shadow_t0_5s` mirrors B
with zero capital and sells 100 % at market exactly 5 s after entry — no SL, no
TP, no shelf, no timeout — to price the drift on its own.

The pre-registered rule is: B beats A iff `mean(B) − mean(A) >= +8 ₽` per trade
and the 95 % bootstrap CI excludes 0. **Honest expectation:** even in the best
case B stays negative (≈ −1.06 %/trade, i.e. round-trip fees plus adverse
drift). Run #4 measures what the 5-second wait costs; it does not look for
profit, and no number from it may be described as "being profitable now".

Pre-launch gate for the 10-minute check:
`python tools/preflight_budget.py runs/<RUN_DIR>` — RPC requests ≤ 1.5 ×
(security-passing coins × 6), and either `http_429_count = 0` or zero
unrecovered failures with a non-empty retry counter.

**Run #3 arms (closed).** The live arms differ in exactly one number, `delta_max_sol`:
`portfolio_a = 2.5`, `portfolio_b = 3.0`; both have the creator gate switched
off. Everything else (birth band 30.5–34.5, `delta_min 0.10`, the 4-mode exit
ladder, 3 × 500 ₽, 1500 ₽) is identical. The creator guard (below) now lives on
a zero-capital shadow `shadow_gate` that mirrors A's entries, and a second
shadow `shadow_exit25` mirrors the same entries but sells 100 % at the first
tick with `pnl_pct >= +25 %` (TP1, trailing, shelf and no-progress disabled; SL
and the 35 s timeout kept). Shadows never touch real capital and are reported
separately in `ab_summary.json`, `exports/shadow_fills.csv` and `SUMMARY.md`.

The run #3 rule was: B beats A iff `mean(B) − mean(A) >= +10 ₽` per trade
**and** the 95 % bootstrap CI excludes 0. Result: −4.76 ₽, CI [−35.04, 24.44] —
criterion not met. The decomposition of A (−16.41 ₽/trade) gave market drift
0→5 s +1.44 %, the momentum-wait premium −2.12 % and round-trip fees −2.50 %,
which is exactly what run #4 attacks.

**Bank (addendum to run #3).** Every book keeps its own cash. If cash is below
one 500 ₽ slot at the moment a trade would open, the book is topped back up to
1500 ₽ and the trade opens; nothing else changes (500 ₽ stake, 3 slots, every
entry and exit). Each injection is recorded as a `top_up` telemetry row and in
`ab_summary.json` per profile: `top_ups`, `capital_injected_rub`,
`pnl_with_topups`, `equity_no_topups = 1500 + PnL`, `bust_at_trade`,
`bust_at_utc`. `bust_at_trade` answers "would the real 1500 ₽ bank have
survived": it is the closed-trade number at which `equity_no_topups` first fell
below 500 ₽. `top_ups` is the second metric — a bank that dies twice costs
roughly two injections.

**Stop-list (addendum to run #3).** The run may stop early for exactly four
reasons, checked every cycle: telemetry loss (`dropped_events`/`write_errors`
> 0); unrecovered RPC error rate above 2 % over 10 minutes or p95 above 1000 ms for five
minutes straight; a live arm with no entries for 30 minutes while
`entry_state` keeps growing; Helius quota below 10 % or less than 2 GB of free
disk. Since run #4 the RPC branch counts **unrecovered** reads only
(`rpc_unrecovered_rate`, threshold 2 % over 10 minutes): a Helius 429 that the
retry layer recovers after 0.25 s / 0.75 s is back-pressure, not a telemetry
failure, and it is still visible as `http_429_count` / `retry_count`. The reason is written to `manifest.early_stop_reason`, `SUMMARY.md` and
`ab_summary.json`, and the artefacts are finalized — never zeroed. PnL is not a
reason to stop. The Helius quota is read from `HELIUS_QUOTA_REMAINING_PCT` or
`<RUN_DIR>/helius_quota.json`; with neither, that check is reported as
unavailable rather than silently passing.

Session length is a parameter, not a constant: `run_local_session.py
--duration-sec 28800` (8 h default), e.g. `--duration-sec 10800` for 3 h.

Creator guard v2 (now `shadow_gate` only): identity resolved to
`pumpportal_event_actor` when the curve creator is an empty fee wallet,
`creator_burst_activity` rejects 25+ signatures inside one hour,
`creator_repeat_deploy_in_session` rejects a wallet already launching 2+ other
tokens in the last 10 minutes (pure local telemetry, zero RPC), token ATA must
exist with a positive balance. The peak-retain exit is labelled `SL/trail
(peak=…)`, not a take-profit, so trailing losses are not read as wins. `getSignaturesForAddress` answers are memoised
per wallet for 300 s, so repeat launchers cost no extra RPC.

## Dependencies

`requirements.txt` (`websockets`, `aiohttp`, `aiosqlite`); `.venv` is created by the
START scripts.
