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

## Block 4 closure record
Status: PASS (production regression + runtime)
Production regression endpoint: HTTP 200 after Block 4 event regressions.
Final runtime job: 1a02231bf5e44e33, state DONE.
Deterministic audit: 62 tests, 0 failures, 0 errors; release gates PASS.
Unified Liquidity & Leverage Event Engine consumes pump exhaustion, crowded positioning, leverage fragility, liquidation cascades and Block-3 liquidity mechanics.
Block-3 liquidity grabs are normalized into FAILED_BREAKOUT / FAILED_BREAKDOWN with LIQUIDITY_SWEEP attached as related evidence, preventing one causal episode from voting twice.
Pump Exhaustion remains event context only: trade_authority=false and no immediate short is authorized.
Causal reversal confirmation consumes the actual Block-3 smart_money.mss contract; UNKNOWN failed-retest state never becomes PASS.
Only an explicit failed retest together with structural break/MSS can complete event confirmation.
x25 remains disabled inside the event detector and can only be considered downstream by the risk/execution layer.
Runtime showed the new unified event path on live crypto candidates (including FAILED_BREAKDOWN/FAILED_BREAKOUT with deduplicated LIQUIDITY_SWEEP evidence) while 8/8 deep-scan candidates completed and live invariants passed.
The first runtime attempt remained QUEUED behind an occupied single worker; the service was restarted and the persisted Redis job 1a02231bf5e44e33 resumed under the same job_id, then completed successfully.

Block 4 contract is frozen. Later changes to liquidity/leverage event semantics require Block 4 regression and affected downstream reruns.

## Block 5 closure record
Status: PASS (production regression + runtime)
Final runtime job: 44091587b3fd4dc0, state DONE.
Deterministic audit: 66 tests, 0 failures, 0 errors; release gates PASS.
Scenario Engine always evaluates both LONG and SHORT hypotheses before selecting a primary direction; neutral/ambiguous structure remains neutral instead of being forced into a side.
Opportunity Funnel is now explicitly downstream of scenario authority: a setup direction cannot reach DEVELOPING/READY/TRADE unless it matches the authoritative primary scenario.
A neutral scenario, missing primary scenario, or primary scenario in the opposite direction fails closed with scenario_authority missing.
analysis_ready=false also fails closed to EARLY and cannot retain stale trade authorization.
Hard invalidations remain absolute and downstream scenario/funnel logic cannot override them.
Legacy alert regression fixtures were updated to include an explicitly authorized primary scenario, so alerts cannot bypass the same scenario gate.
The production runtime preserved the same Redis-backed job_id across the worker recovery and completed 8/8 crypto deep-scan candidates with live invariants PASS.
Live external-market degradation remained fail-closed: unavailable/stale xStock frames produced analysis_ready=false, EARLY funnel state and trade_authorized=false rather than fabricated readiness.

Block 5 contract is frozen. Later changes to scenario selection or opportunity-state authority require Block 5 regression and affected downstream reruns.



## Block 6 closure record
Status: PASS (production regression + runtime)
Final runtime job: f2dfcbb9917d494e, state DONE.
Deterministic audit: 66 tests, 0 failures, 0 errors; release gates PASS.
Trade geometry is directional and complete before authorization: bullish requires stop < entry < target; bearish requires target < entry < stop.
Opportunity Funnel no longer treats limit_plan.eligible alone as sufficient geometry; entry, stop and target must form a valid directional construction before TRADE authorization.
Execution-cost validation rejects wrong-side stop/target geometry instead of turning it into positive RR via absolute distances.
Execution friction (spread/slippage/commission/funding) is included in net RR and can only reduce gross RR.
Limit construction keeps target structurally independent from entry and retains the absolute RR floor 1.5; RR 1.5-1.99 requires elevated setup quality.
Core leverage remains capped at x10. The x25 profile is isolated to pump_exhaustion_x25 and risk is capped at 1%; leverage never increases fixed money risk.
Execution contract remains analysis-first and broker-independent: missing broker execution cannot fabricate an order and does not destroy valid analysis.
Production runtime completed 8/8 crypto deep-scan candidates, including a futures-only perp_only path, with telemetry error_count=0 and all live invariants PASS.

Block 6 contract is frozen. Later changes to trigger, entry/target geometry, execution cost, RR or risk/leverage semantics require Block 6 regression and affected downstream reruns.


## Block 7 closure record
Status: PASS (production regression + runtime)
Final runtime job: 7666f81eb0614295, state DONE.
Deterministic audit: 73 tests, 0 failures, 0 errors; release gates PASS.
Watchlist state is keyed by market+symbol, fingerprinted by setup/event geometry, TTL-bounded and emits state_changed only for a real state/fingerprint transition.
The canonical /scan-check endpoint is read-only and never starts or duplicates a scan; Redis-backed unified job state remains recoverable across process restart under the same job_id.
Outcome logging accepts only READY/TRADE records and now reloads persisted fingerprints before append, preventing duplicate learning samples after a Render process restart.
Backtest remains no-lookahead with explicit limit fill before TP/SL resolution; MAE/MFE are measured only over the post-fill evaluation interval.
Edge discovery consumes only resolved TP/SL outcomes; unresolved and not-filled observations cannot inflate hit-rate or expectancy.
Learning remains downstream observational evidence and does not rewrite historical Structure, Scenario, Opportunity or execution facts.
A regression exposed an outdated frozen Block-5 fixture after Block-6 tightened geometry; the fixture was aligned to the frozen Block-6 requirement (entry+stop+target) and Blocks 5->6->7 were rerun together.
Production runtime completed 8/8 crypto deep-scan candidates with telemetry error_count=0 and live invariants PASS.

Block 7 contract is frozen. Later changes to state/watch, check endpoints, outcome logging, backtest/learning or production job persistence require Block 7 regression and affected downstream reruns.

## Block 8 closure record
Status: PASS (deterministic regression + four-market production runtime)
Deterministic audit after the final Block-8 fix: 79 tests, 0 failures, 0 errors; release gates PASS.
Final production job: 5a7da9c3228a4859, state DONE; the same job was retained through the queue delay and completed without replacement.
Four-market runtime covered crypto, stocks, forex and commodities with production telemetry error_count=0.
Crypto deep scan completed 6 candidates; external runtime completed stocks=15, forex=10, commodities=5.
A Block-8 defect was found in the unified external batch path: Forex/Commodities bypassed the existing Yahoo fallback when Twelve Data was not configured. The fix routes those markets through the per-symbol Yahoo fallback when the Twelve Data key is absent, and a regression test now locks this contract.
Post-fix production verification confirmed Forex provider=yahoo_finance for all 10 candidates and Commodities provider=yahoo_finance for all 5 candidates. Data-quality failures in external markets remained fail-closed: no trade authorization was produced from unavailable/degraded data.
The production workflow wrapper reached its 15-minute CI timeout while the same Render job was still running; the job itself subsequently completed DONE and was read directly by a separate verification step. No replacement scan was started for the final verification.
Temporary audit-only workflows used to execute and finalize the same production job were removed after verification.

Block 8 contract is frozen. Later changes to multi-market provider fallback, four-market orchestration, production invariants or zero-chaos semantics require Block 8 regression and affected downstream reruns.
