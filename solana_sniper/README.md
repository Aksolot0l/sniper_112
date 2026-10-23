# Solana Sniper — paper A/B workspace

## Scope and safety

- **Paper A/B only** (`portfolio_scope=ab_only`). No signing, no submitting, no real funds.
- RPC access is read-only. Never put Helius/API credentials in source files, manifests, logs, or archives.
- One bot process at a time (Helius RPS budget). `run_local_session.py` refuses a second session.

## Run #10 (active): S0 vs S2 — dev selection and ticker copies

Base `cb3f7b8` + the run #10 blocks below. **One run, one process, paper only.**
Both stages carry the same number — `RUN_NUMBER=run10` — and differ only in the
run directory and the duration:

| stage | duration | run id | what it decides |
|---|---|---|---|
| 10a | 3 600 s | `run10a-1h-<UTC>` | technique only: rps, ghost coverage, funnel |
| 10b | 43 200 s | `run10b-12h-<UTC>` | the money question, 12 h |

10b is started only after 10a passes its acceptance and the median
`round_trip_cost_pct` is ≤ 4.0 %. The numbers of the books, the exit, the stake,
the slots and the cadence are identical in both stages — not one of them moves.

| book | `v_sol_birth` | `delta` | name copies (120 s) | what it is |
|---|---|---|---|---|
| S0 `portfolio_s0` | 30.50 … 34.50 | 0.00 … 1.00 | off | run #8's book verbatim (+558.02 ₽ in 30 min) |
| S2 `portfolio_s2` | **31.96** … 34.50 | 0.00 … 1.00 | **≤ 0** | S0 + dev selection (`v_sol_birth = 29.96 + dev_buy`, so ≥ 31.96 means the dev bought ≥ 2 SOL) |

The two books differ in exactly two things — the birth floor and the copy
filter. The exit, the entry timing, the slots, the stake and the cadence are
identical: any other difference would make the comparison meaningless.
`ALLOW_SINGLE_ARM=1` lets the pair start; the manifest carries the truthful
per-book band of each book.

Exit — one rule for both books, `EXIT_MODE=fee_cover`, before any Round 6 rule:
hard stop **−10 %** → sell 100 % at **+3.5 %** → otherwise the **5 s** clock.
All three always sell the whole remaining position; TP1, trail, shelf,
no-progress and the 35/90 s timeouts are not called in this mode and cannot
appear in `exit_breakdown`. The 3.5 % is measured, not tuned: the round trip
costs 3.25 % (202 run #7 trades whose price did not move), leaving 0.25 pp of
slack. **The exit is untouchable in run #9** — it is not the experiment.

The name-copy filter is pure arithmetic on a dict the process already holds
(`symbol_lower → [found_at]`, 120 s window, no RPC). An empty ticker is never
filtered; the reject reason is `name_copies_120s`.

Entry latency is measured, not modelled: `t_detect_ms` (WSS packet → candidate
row), `t_filters_ms` (row → security verdict), `t_read_ms` (verdict → the curve
read the fill is priced from), `t_fill_ms` (read → fill row), `t_total_ms`
(packet → fill). `ENTRY_LATENCY_MS=0` removes the artificial floor only.

Marks are polled every `MARK_POLL_SEC=0.25` s under `MARK_POLL_RPS_LIMIT=2.5`:
the cadence starts where the 12-hour budget needs it, so no mid-run adaptation
decision exists. The ghost does **not** ride along on every poll — it is read on
`POST_EXIT_SCHEDULE_SEC = 0.5, 1, 1.5, 2, 3, 5, 7, 10` s, one mark per due
point. In the 30-minute smoke 1 839 of 2 735 mark batches existed only for
ghosts (2.23 rps mean, 2.83 rps in the worst window); the schedule turns those
into eight reads per exit and leaves a cycle with no open position and no due
ghost issuing no request at all. A point that cannot be read is retried at most
three times and then written off, so an undecodable curve can never turn a dead
ghost into a request per cycle; the report carries `planned / written / missed /
recovered` per book so a lost point is visible rather than inferred.

`price_at_5s` stores its distance from entry+5.0 s (`price_at_5s_distance_sec`),
so the ghost only replaces a tick when its own mark is genuinely closer. Before
run #10 the distance was not stored and any ghost mark within 0.75 s overwrote
the tick: in the 30-minute smoke 32 of 32 `post_exit` picks had a closer tick in
the stream (0.64 s vs 0.13 s median), which quietly moved `pnl_pct_at_5s` from
≈5.1 s in run #9 to ≈5.6 s. The run does not stop on rps, and raising the rate to "survive
to the end" is forbidden. `accountSubscribe` is deliberately **not** used: a
second mark source would make exit timing depend on which stream arrived first.

After every exit the run keeps a **10 s ghost**: `exports/post_exit.csv` carries
one mark per poll (`portfolio, mint, exit_at_epoch, sec_since_exit, price_usd,
pnl_vs_entry_pct, pnl_vs_exit_pct`) with the mandatory `+0.5 / +1 / +2 / +5 /
+10` s points taken from the nearest mark. It costs no extra RPS — it is fed
from the batch that already reads. It answers the question the paper fill
distorts: it books the trigger price in the same snapshot, while a real sell
arrives later. Every closed trade also carries `price_at_5s`, the real price
5.0 s after entry (from the ghost when the position has already exited).

Observation offsets in this run: **only `0, 5`** s (`TELEMETRY_OBSERVATION_OFFSETS_SEC=0,5`).

Acceptance 10a (§3): paper_only and 0 signatures; ≥ 3 500 s of real polling;
`early_stop_reason = null`; unrecovered RPC errors ≤ 1 %; **mean rps ≤ 1.8**
and **worst 10-min window ≤ 2.2** (the smoke measured 2.23 / 2.83); 429 ≤ 0.5 %
of requests; `dropped_events = 0`; `write_errors = 0`; 0 open positions at the
finish; **ghost coverage ≥ 0.97** of the required points (+0.5/+1/+2/+5/+10),
**median 8 marks per exit, minimum ≥ 7**; manifest bands S0 30.50–34.50 /
S2 31.96–34.50, copies off / 0;
**S0 ≥ 80 trades, S2 ≥ 8**; median `round_trip_cost_pct` recorded —
**> 4.0 % and 10b does not start**. Money is not read at 10a: an hour gives
S2 a CI95 of roughly ±51 ₽, which cannot separate the expected +15…+25 ₽ from
noise, and gating on it would change the config on a coin flip.

Acceptance 10b (§4): ≥ 43 000 s of real polling; `early_stop_reason = null`;
mean rps ≤ 1.8, worst 10-min window ≤ 2.2; unrecovered RPC errors ≤ 1 %;
429 ≤ 0.5 %; 0 open positions; **ghost coverage ≥ 0.95** with the median at 8
and the minimum at 7 — a 12-hour stage plans ≈14 200 points, so demanding 8 on
every single exit would fail the stage if 0.007 % of them were lost; S0 ≥ 400
and S2 ≥ 80 trades. A 30-minute smoke keeps the lighter bar (coverage ≥ 0.90,
minimum ≥ 5 marks).

Verdict, fixed before the start: S2's CI95 excludes 0 and is ≥ +2 % → the dev
selection is accepted; includes 0 → not proven, the config is not repeated;
entirely < 0 → rejected. S0 vs S2 is a **separate** conclusion, paired by
minute (the share of minutes where S2 > S0, plus the mean ₽ difference), and
inside S0 the pre-declared decomposition is `dev 2–4.5 & copies = 0`,
`dev 2–4.5 & copies > 0`, `dev < 2`. The only pre-agreed money stop: at hour 8,
if S2's CI95 is entirely below zero → `negative_edge_S2`.

Out of scope: any direct comparison with run #8's numbers, any "it is
profitable now", and any PnL statement without `n`, mean, median, CI95 and the
exit breakdown.


## Quick start (local PC) — run #10 in two stages

```
# stage 10a, one hour, technique only
DURATION_SEC=3600 RUN_NUMBER=run10 bash START_MAC_LINUX.sh \
    --run-id run10a-1h-$(date -u +%Y%m%dT%H%M%SZ)

# stage 10b, twelve hours — only after 10a passes its acceptance
DURATION_SEC=43200 RUN_NUMBER=run10 bash START_MAC_LINUX.sh \
    --run-id run10b-12h-$(date -u +%Y%m%dT%H%M%SZ)
```

Environment for both stages (identical, not one number moves between them):

```
ARMS=portfolio_s0,portfolio_s2
ENTRY_BIRTH_MIN_SOL=30.50     # shared; S0 uses it — never 31.96
ENTRY_BIRTH_MIN_S0=30.50 ; MAX_NAME_COPIES_S0=off
ENTRY_BIRTH_MIN_S2=31.96 ; MAX_NAME_COPIES_S2=0
ENTRY_DELTA_MIN_SOL=0.00 ; ENTRY_DELTA_MAX_SOL=1.00
EXIT_MODE=fee_cover ; EXIT_FEE_COVER_PCT=3.5 ; EXIT_TIME_SEC=5.0
EXIT_HARD_STOP_PCT=10.0 ; MARK_POLL_SEC=0.25
TELEMETRY_OBSERVATION_OFFSETS_SEC=0,5
```

## Quick start (offline smoke)

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

- Launchers: `START_WINDOWS.bat` / `START_MAC_LINUX.sh` (they create `.venv`,
  ask for the key and start one foreground process). `run_rocket2_ab_paper.sh`
  is the bare Linux/macOS entry point with the run #7 preflight, and
  `run_local_session.py` adds the dashboard + heartbeat on top of it.
- Cross-platform runner + dashboard + heartbeat: `run_local_session.py`
- Entry point: `main_ab.py`
- A/B portfolio, entry rule, exit ladder: `portfolio_ab.py`, `pump_curve_quote.py`, `price.py`
- Creator identity / guard / wallet cache: `creator_probe.py`
- Telemetry: `telemetry.py`; monitor: `live_ab_dashboard.py` + `paper_ab_dashboard.html`
- Tests: `tests/` (`python -m unittest discover -s tests`). Bundle builder: `tools/build_bundle.py`
- Report template: `../run-logs/run7-TEMPLATE/RUN_NOTES.md` (run #7 comparison table);
  run #9/#10 write their own page (`_run9_summary_markdown` in `main_ab.py`, kept
  under the old name), template in `../run-logs/run10-TEMPLATE/RUN_NOTES.md`

## Strategy state (2026-10-22)

The run #7 arms below are **closed**: 8 h, build `98552c9`, A 296 trades
−2 849.90 ₽, B1 167 / −1 107.10 ₽, B2 163 / −2 101.73 ₽, B3 758 / −4 161.36 ₽;
no arm reached the pre-registered `paired mean(arm − A) ≥ +8 ₽`, and the paired
column is structurally 0 (identical entry moment and ladder). Kept for the
record; the live configuration is run #9 above.

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

Exits — one ladder for every arm (Round 6, unchanged): SL −10% before
TP1/rocket; no-progress (5 s, peak < +4%, PnL < +1.5%); positive stagnation
(4.5 s, PnL ≥ +1.5%); TP1 +30% sells 50%; peak retain `pnl <= 0.85 * best`;
rocket bypass at +50% keeps 100%. Timeouts 35 s / 90 s remain as slot-safety
nets. The exit is **not** an experiment any more: run #6 showed the 15 s time
exit indistinguishable from the ladder (paired B−A −4.24 ₽, CI [−19.2; +10.7]).

**Run #7 arms (active): each arm moves ONE entry boundary of A.**

A is the untouched reference. Every experimental arm changes exactly one band
edge; exit, entry timing, slots, stake and poll rate are identical. Any other
change would make the paired comparison meaningless.

| arm | `v_sol_birth` | `delta` (`v_sol_now − v_sol_birth`) | what it moves |
|---|---|---|---|
| A `portfolio_a` | 30.50 … 34.50 | 0.10 … 2.50 | reference |
| B1 `portfolio_b1_narrow` | 30.50 … 34.50 | 0.10 … **1.00** | delta ceiling ↓ |
| B2 `portfolio_b2_birth` | 30.50 … **31.00** | 0.10 … 2.50 | birth ceiling ↓ |
| B3 `portfolio_b3_t0` | 30.50 … 34.50 | **0.00** … 2.50 | momentum floor ↓ (t0 entry) |

B3 is the t0 arm: it keeps A's whole band and only drops the momentum floor,
so it enters on the first curve read after detection. Its expected gain is
arithmetic, not fitted: A pays a measured `entry_fill_premium_pct` median of
1.416 % (~7 ₽ on a 500 ₽ stake) for waiting for the impulse, and B3 does not.
Inside the same birth window B3 sees 438 coins instead of 167 with the same
6.6 % rocket share (60 s), so the flow grows without diluting the tail.
600 RPC is the bottleneck, so the observation schedule shrank to
`0, 5, 15, 60` s (was `0, 5, 10, 15, 60, 120`); coins that failed the security
filter are read only at 0 and 15 s (`status=skipped_security_deep`), which is
the P0-2 fix — in run #6 the verdict was resolved before the loop ran, so this
trim never happened (`security_fail_skipped_deep = 0` over 1 634 rejects).
The 60 s read itself is taken for a **deterministic third of the coins**, chosen
by the mint hash (never by a bot decision, or the sample would be selected by the
thing being measured; unselected coins get `status=skipped_window_sample`). That
makes ≈47 000 requests over 8 h ≈ 1.64 rps.

Pre-registered decision rule:

1. an arm confirms its hypothesis iff the **paired** `mean(arm − A) ≥ +8 ₽`
   (mean over the coins both books traded), the 98.75 % bootstrap CI
   (20 000 resamples) excludes 0, and the sign matches in both halves;
2. three hypotheses → Bonferroni, α = 0.0125 (98.75 % interval);
3. a stable sign with a CI covering 0 is written up as "разведочно, нужен
   повтор" — the live config does not change;
4. every arm inside ±5 ₽ means band tuning is exhausted.

Honest expectations, written before the start: B1 **+1…+6 ₽**, B2 **−8…+8 ₽**,
B3 **+3…+9 ₽** per trade. A band above the 2.5 SOL delta ceiling is deliberately
absent: its EV@15 is −9.25 / −5.84 / −5.54 % in three independent runs and it
holds no extra rockets inside A's birth band. Run #6's paired CI95 was
[−19.20; +10.72] at n=165 pairs, i.e. a ±15 ₽ half-width and a ~21 ₽ minimum
detectable effect at 80 % power, so any difference below ~15 ₽ on a 3-hour run
is undecidable and is reported as such. No number from this run may be described as "being
profitable now": arm A's median trade in run #6 was −15.20 ₽ and the whole
plus rested on two pump tails.

Run #7 instrumentation (no behaviour change):
`exports/ticks.csv` gained `mark_prev_pct`, `step_gap_pp`, `mark_gap_ms` and
`price_usd` is now a plain USD unit price (run #6 divided a rouble price by
`sol_usd × fx`, so every entry/mark row was off by `1 / sol_usd`);
`security_fail_reasons` is read from the **last** reason of the verdict with
short codes (run #6 read `parsed[0]` and mis-split 1 634 rejects); `SUMMARY.md`
gained a fly-through section that answers "the SL sits at −10 %, at what price
did the trade actually leave?" with the median fill and the mark-to-mark gap.

`exports/enrich.csv` (block 3, `ENTRY_ENRICH=1`): one passive call per
security-passing coin to `datapi.jup.ag/v1/assets/search` (batches of seven,
2 s timeout, one attempt, zero RPC) with `topHoldersPercentage`, `holderCount`,
`organicScore`, `devMints`, `devMigrations`, `devFundedAt`, `stats1h.*`,
`twitter`, `primaryTwitter.followerCount`. It never influences an entry or an
exit — it is data collection for run #8. `status = ok | enrich_unavailable`.

**Run #3 arms (closed).** The live arms differed in exactly one number,
`delta_max_sol`: `portfolio_a = 2.5`, `portfolio_b = 3.0`. The creator guard
used to sit on a zero-capital shadow (`shadow_gate`) and a second shadow
(`shadow_exit25`) sold 100 % of the first +25 % tick. **Both shadows are gone**
in run #7: they ran at a different poll rate (2.34 s vs 0.467 s) with
`max_slots = 1`, so their numbers were never comparable with the live arms.
`PROFILE_B` (the `delta_max = 3.0` arm) is gone as well — a delta band above
2.5 is closed on data (EV@15 −9.25 / −5.84 / −5.54 % in three runs).

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
--duration-sec 1800` (30 min default since run #8), e.g. `--duration-sec 10800`
for 3 h. `DURATION_SEC` does the same through the START scripts.

Creator guard v2 — **telemetry only, no arm filters on it** (run #7: the
gate lost against the average in 4/4 independent samples, -4.0…-6.4 %):
identity resolved to
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
