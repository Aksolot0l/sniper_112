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
- Tests: `tests/` (119 tests). Bundle builder: `tools/build_bundle.py`

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

**Run #3 arms.** The live arms differ in exactly one number, `delta_max_sol`:
`portfolio_a = 2.5`, `portfolio_b = 3.0`; both have the creator gate switched
off. Everything else (birth band 30.5–34.5, `delta_min 0.10`, the 4-mode exit
ladder, 3 × 500 ₽, 1500 ₽) is identical. The creator guard (below) now lives on
a zero-capital shadow `shadow_gate` that mirrors A's entries, and a second
shadow `shadow_exit25` mirrors the same entries but sells 100 % at the first
tick with `pnl_pct >= +25 %` (TP1, trailing, shelf and no-progress disabled; SL
and the 35 s timeout kept). Shadows never touch real capital and are reported
separately in `ab_summary.json`, `exports/shadow_fills.csv` and `SUMMARY.md`.

The pre-registered decision rule for run #3 is: B beats A iff
`mean(B) − mean(A) >= +10 ₽` per trade **and** the 95 % bootstrap CI of the
difference excludes 0 (20 000 resamples). Otherwise the next candidate is
`delta_min 0.6`, and if that fails too the honest conclusion is that the 5-second
scalp is dead — not that it "became profitable".

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
