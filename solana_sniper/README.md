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
- Tests: `tests/` (100 tests). Bundle builder: `tools/build_bundle.py`

## Strategy state (2026-10-22)

Entry v3 — live momentum, shared by both arms:

```
30.5 <= v_sol_birth <= 50.0   and   0.10 <= (v_sol_now - v_sol_birth) <= 2.5
```

Cap 2.5 (not 12): across the 66 archived `entry_state` rows joined with
`observations`, `delta >= 2.5 SOL` gave a median -19.8% drawdown from the entry
point and 10 of 15 such entries went to the stop; Turtle (11.56 SOL) ran -18.7%
within 15 s of its own entry. The 12 SOL ceiling fitted the filter to a single
observation.

The previous `v_sol_now <= v_sol_birth + 0.8` cap was an anti-filter: it rejected
every real runner seen in the Solscan/Jupiter reconciliation (Turtle 33.95→45.51,
Shigure 33.16→38.59, CAVELON, PERFORMA, SKILLSPECTOR) while buying dead
`delta == 0` decoys (8 of 9 such fills lost money).

Exits — approved 4-mode ladder, 1:1 (unchanged this run; next experiment): SL −10% before TP1/rocket; no-progress
(5 s, peak < +4%, PnL < +1.5%); positive stagnation (4.5 s, PnL ≥ +1.5%);
TP1 +30% sells 50%; peak retain `pnl <= 0.85 * best`; rocket bypass at +50%
keeps 100%. Timeouts 35 s / 90 s remain as slot-safety nets.

Treatment (B only) — creator guard v2: identity resolved to
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
