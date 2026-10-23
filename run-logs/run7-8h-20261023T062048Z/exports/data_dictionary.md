# Data dictionary

- `candidates`: one stable row per chain+mint, first-seen metadata and config snapshot.
- `detections`: append-only source creation events and candidate-received events; raw JSON is redacted.
- `decisions`: filter, capacity, entry, quote-confirmation, and collector outcomes with reasons/values.
- `creator_attributions`: separate program creator and transaction payer with causal observation time/latency.
- `creator_probes`: read-only getSignaturesForAddress(limit=25) and creator ATA balance results; RPC failures are non-blocking.
- `watchlist_events`: independent Watchlist candidate polls, quote-confirmed entries/exits, and portfolio summaries.
- `quotes`: buy/sell observations, including shadow quotes; `quote_only=1` means no transaction/fill.
- `observations`: price/source at a scheduled offset from first discovery; Pump.fun samples are fresh curve RPC reads (no indexer fallback or interpolation), and late/cancelled/missing rows are explicit. `excluded_from_stats=1` marks rows that must stay out of every statistic (session-end cancellation); `status=skipped_security_deep` means the coin failed the security filters, so offsets beyond 0 and 15 were deliberately not read; `status=skipped_window_sample` means the coin is outside the deterministic one-in-three hash sample for the 60 s read.
- `paper_fills`: simulated sale legs only; `out_amount_raw`/`snapshot_pnl_rub` are the primary snapshot fill, while `min_out_raw` and `conservative_100bps_*` preserve the separate 100 bps shadow; `portfolio` separates A and B; no on-chain executions.
- `collector_meta`: queue drops and writer errors. Timestamps are UTC; raw token amounts are strings.
- Future observations are labels/targets only; never use them as features available at discovery time.
