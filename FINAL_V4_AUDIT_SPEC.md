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


## Block 2 closure record
Status: PASS (production regression + runtime)
Production regression endpoint: HTTP 200 before final run.
Final runtime job: 144b847d82ad484f, state DONE.
Market context remains context-only and cannot authorize or rewrite a trade.
Canonical regime supports TREND / RANGE_TRANSITION / COMPRESSION / EXPANSION plus explicit TRANSITION / PRICE_DISCOVERY event context.
PRICE_DISCOVERY is context-only and never rewrites confirmed Structure direction.
xStocks retain 24/7 secondary-market availability while exposing the underlying US session layer (regular/premarket/after-hours/closed).
Calendar/news state is UNAVAILABLE unless an external feed is actually supplied; no news is fabricated.
PreScan uses top-turnover ranking, closed/fresh history and market-quality gates.
STRICT keeps the HOT/WARMING gate. RANKED_FALLBACK fills unused deep-analysis capacity only and explicitly remains eligible_for_scan_plus=false / trade_decision=false.
Relative-strength ranking is downstream context/ranking only, not trade authority.
Final production scan completed with 8 selected candidates; Block 1 realtime/data contract remained healthy, including spot_perp and perp_only paths.

Block 2 contract is frozen. Later changes to context or PreScan require Block 2 regression plus all affected downstream blocks.


## Block 3 closure record
Status: PASS (production regression + runtime)
Production regression endpoint: HTTP 200 after Block 3 mathematical regressions.
Final runtime job: eb96c719d6514754, state DONE.
Structure remains fact authority; indicators/patterns cannot override it and hard invalidation beats confluence.
MTF price discovery remains transition/context until structure confirms.
Elliott is evaluated recursively across major/intermediate/minor degrees; higher-degree major count is retained across stream rescans until its own invalidation is breached.
Corrective Elliott direction is canonicalized to bullish/bearish end-to-end, preventing valid ABC candidates from being dropped or mis-compared.
Impulse overlap is a hard impulse-rule failure; diagonal is evaluated as a separate candidate rather than used to excuse an invalid impulse.
Fibonacci retracements/extensions and multi-leg clusters are structural-context modules.
Fib confluence now requires independent structural anchors; multiple nearby ratios from one leg cannot inflate confirmation.
Harmonic families include Gartley, Bat, Alternate Bat, Butterfly, Crab, Deep Crab, Cypher, Shark, 5-0, AB=CD and Extended AB=CD.
XABCD D geometry was corrected to measure A-to-D relative to XA; a synthetic Gartley regression locks the corrected ratio.
RSI/MACD/Stochastic divergences are pivot-based, deduplicated and freshness/TTL aware.
Signal freshness is timeframe-aware, so 1H/4H/1D evidence is not expired by a fixed lower-timeframe wall-clock threshold.
Liquidity map now includes lifecycle-aware structural/equal pools plus PDH/PDL/PWH/PWL reference liquidity; taken/accepted pools are excluded from actionable nearest objectives.
SMC remains causal: liquidity -> displacement -> MSS -> POI, and cannot rewrite Structure.
Final production scan completed with 8 deep-scan candidates while frozen Blocks 1 and 2 remained operational.

Block 3 contract is frozen. Later analytical-core changes require Block 3 regression and affected downstream reruns.
