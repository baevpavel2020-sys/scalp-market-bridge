# Scan+ V4 Final Combat Audit Specification

Frozen sequence:
1. Data & Realtime Infrastructure
2. Market Context & PreScan
3. Analytical Core
4. Liquidity & Leverage Event Engine
5. Scenario Engine & Opportunity Funnel
6. Trade Construction, Execution & Risk
7. State / Check / Learning / Production
8. Multi-market + Final Zero/Chaos Audit

Dependency rule:
DATA -> CONTEXT -> ANALYSIS -> EVENTS -> SCENARIOS -> OPPORTUNITY -> TRIGGER -> ENTRY/TARGET -> EXECUTION -> RISK -> STATE/WATCH -> OUTCOME/LEARNING -> MULTI-MARKET VALIDATION.

A downstream block may consume or veto upstream facts, but must not rewrite them.
If a downstream audit exposes an upstream defect: return to the owning block, fix it, rerun its regression, then rerun all affected dependent blocks.
A block closes only after: fix -> deploy -> regression -> real runtime test -> repeat verification.

## Block 1 closure record
Status: PASS (production runtime)
Production regression endpoint: HTTP 200 before final run.
Final runtime job: d4ffda9968de4bad, state DONE.
Deep scan capacity: 8 selected / 8 activated.
Observed live evidence: perpetual trades, spot trades where available, OI coverage, ticker/orderbook freshness, spot/perp/perp-only execution modes.
Futures-only path verified in runtime (e.g. TAOUSDT perp_only with trade_data_ready=true).
Sparse-spot degradation threshold bounded within warmup budget.
Realtime manager capacity >= V4 deep-scan capacity; activation eviction ordering fixed.
Terminal Redis payload compacted; final run produced no Redis OOM/write errors.
Distributed duplicate lease made authoritative under SET NX race.
Data quality contract distinguishes PASS / DEGRADED / FAIL / UNAVAILABLE.
Cross-provider history validation rejects stale and price-dislocated fallback data.

Block 1 contract is frozen. Changes to Block 1 code from later blocks require Block 1 regression and affected downstream reruns.
