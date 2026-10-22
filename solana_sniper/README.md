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
- Tests: `tests/` (187 tests). Bundle builder: `tools/build_bundle.py`
- Report preview before launch: `RUN4_SUMMARY_PREVIEW.md` (synthetic numbers, shows the exact SUMMARY.md layout and criteria)

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

Exits — approved 4-mode ladder, A only (B is the run #4 experiment): SL −10% before TP1/rocket; no-progress
(5 s, peak < +4%, PnL < +1.5%); positive stagnation (4.5 s, PnL ≥ +1.5%);
TP1 +30% sells 50%; peak retain `pnl <= 0.85 * best`; rocket bypass at +50%
keeps 100%. Timeouts 35 s / 90 s remain as slot-safety nets.

**Run #4 arms (active): the exit rule is the single difference.**

Same entry in both live arms — `30.5 ≤ v_sol_birth ≤ 34.5` and
`0.10 ≤ v_sol_now − v_sol_birth ≤ 2.5`, priced from the first on-chain curve
read after detection. Only the exit differs:

| | A (Round 6, frozen) | B (run #4) |
|---|---|---|
| time exit | 35 s / 90 s safety nets | **100 % at t ≥ 15.0 s** (`time_exit_15s`) |
| stop loss | −10 % before TP1/rocket | **−20 %, for the whole life of the position** |
| take profit | TP1 +30 % sells 50 % | **+100 % sells 50 %** |
| trail 0.85× best | on after +10 % | **off** |
| shelf / no-progress | 4.5 s / 5 s | **off** |

Why: on the same 84 trades, holding to 15 s without SL/TP/trail gives −265 ₽
instead of −1 344 ₽ (+12.8 ₽/trade), and the loss comes from the take-profits
(SL/trail +674 ₽ on 13 trades, SL +175 ₽ on 21, no-progress −81 ₽ on 44). The
horizon is sharp: 5 s −4.9 ₽, **15 s −3.2 ₽**, 60 s −32.6 ₽, 120 s −46.7 ₽.
Slots (3), stake (500 ₽), bank (1500 ₽) and the entry are unchanged.

Shadows, both zero capital, 1 position each, polled at 0.5 Hz on A's entries:
`shadow_gate` (Round 6 creator gate) and `shadow_no_trail` (A's entry with
TP1 +30 %/50 % kept, trail/shelf/no-progress off, remainder sold at 15 s).
`shadow_exit25` and `shadow_t0_5s` are deleted. A shadow can never change A/B
fills; `shadow_poll_dropped` in the report counts polls skipped by the budget.

The pre-registered rule stays: B beats A iff `mean(B) − mean(A) ≥ +8 ₽` per
trade and the 95 % bootstrap CI of the difference excludes 0 (20 000
resamples); `shadow_no_trail` beats A iff ≥ +6 ₽ with a CI excluding 0;
several comparisons make the result exploratory (Bonferroni α = 0.0125).
**Honest expectation: B stays negative**, −8…−4 ₽/trade against −15…−17 ₽ for
A. The run measures what the current exit costs, not profit; no number from it
may be described as "being profitable now". If B ≈ A, say so and go to the
deferred `delta_min = 0` experiment (§5.1) in a separate run.

Run #4 instrumentation (no behaviour change): per tick `trigger_pct`,
`fill_pct`, `exit_latency_ms`, `peak_pct`, `peak_t_sec` (`exports/ticks.csv`);
per trade `entry_premium_pct`, `entry_lag_ms`, `held_sec`, `mfe_pct`, `mae_pct`,
`peak_pct`, `captured_pct_of_peak`; per arm/shadow `n, sum, mean, median, sd,
ci95_mean`, `n_for_significance`, `sum_without_top2`, `sum_without_worst2`;
`exit_reason × portfolio` table; `ab_test.single_difference = exit_rule`;
security-rejected coins read only at offsets 0 and 15 (others stay visible as
`status=skipped_security_deep`) and `security_fail_rockets` reports how many of
them ran ≥ +50 % in 15 s; 429s retried (0.2/0.5/1.2 s + jitter, `Retry-After`
honoured, ≤ 3 retries, concurrency ≤ 6) and only `unrecovered_failures`
(≤ 2 % over 600 s) can trip the stop-list; artefacts (`ticks.csv`,
`shadow_fills.csv`, `ab_summary.json`, wallets, `STATUS.txt`) are rewritten
every 60 s and once more in the stop handler; session-end cancellations carry
`excluded_from_stats = 1`.

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
