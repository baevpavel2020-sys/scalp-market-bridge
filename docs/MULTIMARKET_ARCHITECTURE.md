# Scan+ Multi-Market Architecture

Status: architecture baseline
Baseline commit: 99d0a7428e132a5c688b08eda40f568ea146ced5
Working branch: scan-plus-multimarket

## Non-negotiable invariants

1. Existing Crypto behavior is the regression baseline and must not be silently changed while extracting modules.
2. One failed market adapter must not stop the other markets.
3. Shared analytical algorithms do not contain market-specific assumptions.
4. Market-specific facts are supplied by adapters/profiles and cannot rewrite unrelated evidence.
5. Low priority does not mean skipped: priorities affect PreScan selection and Scan decision weight.
6. No double-counting of one market event through multiple labels.

## Command routing

- Scan -> Crypto + Stocks + Forex + Commodities
- Scan crypto -> Crypto only
- Scan stocks -> Stocks only
- Scan forex -> Forex only
- Scan commodities -> Commodities only
- Scan metals / oil / cocoa -> matching commodity profile
- Scan <symbol> -> resolve symbol to its market/profile automatically

## Decision hierarchy

Market -> Instrument Profile -> Current Situation.

This hierarchy is applied independently in:
- PreScan: which instruments deserve full analysis.
- Full Scan: which evidence has priority for MARKET / LIMIT / NO TRADE.

## Market modules

markets/crypto
- Bybit linear + spot
- spot/perp CVD
- OI, funding, liquidations, order flow
- Crypto-specific liquidity/leverage events
- Manipulation x25 special mode

markets/stocks
- transport adapters are replaceable
- distinguish token/perp/CFD volume from underlying equity volume
- regular session, premarket/after-hours where data permits
- gaps, underlying levels, relative strength

markets/forex
- replaceable FX quote/history provider
- Asia/London/New York sessions
- session high/low and liquidity
- FX-specific relative strength and macro-event context

markets/commodities
- subprofiles: metals, energy, softs
- initial instruments include Gold, Silver, WTI/Brent, Cocoa
- each instrument may override parent priorities

## Shared analytical core

The following are tools/evidence, not independent competing strategies:
- Structure / MTF
- Elliott
- Fibonacci
- Harmonics
- Divergence
- SMC
- RSI / MACD / Stochastic
- ATR / volatility
- VWAP where meaningful
- liquidity levels

Primary setup families:
1. Continuation
2. Reversal
3. Failed Move

Liquidity & Leverage Event Engine classifies related events without double counting:
- Pump Exhaustion
- Liquidity Sweep
- Failed Breakout / Breakdown
- Long Liquidation Cascade
- Short Squeeze
- Crowded Positioning

## Manipulation x25

Special Crypto mode preserving the original post-pump reversal principle.

State machine:
WATCH -> ARMED -> TRIGGERED, otherwise NO SHORT.

Evidence path:
Abnormal Pump -> Pump Quality -> Spot/Perp divergence -> OI/Funding ->
Aggression Efficiency -> Absorption/Exhaustion -> Leverage Fragility ->
Liquidation Clusters -> Failed Acceptance -> Structure Break -> Failed Retest.

Risk rules:
- x25 isolated only.
- Leverage never determines risk; stop distance determines position notional.
- Default account risk 1.0%; A+ hard ceiling 1.5%.
- Price stop + thesis stop + time stop.
- Stop must remain safely before estimated liquidation; otherwise x25 REJECTED.
- No averaging down.
- Maximum two independent attempts per pump event.
- Second attempt requires a new valid structure/event trigger.
- After two stopped attempts: EVENT LOCKED until a genuinely new structure forms.
- Minimum expected net RR after estimated fees/slippage: 2.0R; target preference 2.5R-4R.
- Continued acceptance above the pump extreme / healthy spot demand => NO SHORT.

## Extraction plan from current monolith

Current dynamic_collector.py already contains natural seams:
- MarketStream: transport, candles, realtime flow, technical calculations.
- DynamicMarketManager: activation, MTF, execution, trade engine.
- PreScanEngine / OnDemandPreScanService: discovery/ranking/history providers.
- ScanOrchestrator / ScanJobManager: orchestration and jobs.

Extraction order:
1. Characterization/regression tests around current public behavior.
2. Freeze Crypto adapter behavior.
3. Extract shared domain models/contracts without changing outputs.
4. Extract common analytical core.
5. Move Crypto-specific transport/evidence behind markets/crypto.
6. Add MarketProfile + InstrumentProfile priority configuration.
7. Add Stocks, Forex, Commodities adapters independently.
8. Add multi-market command/router and failure isolation.
9. Regression + stress test + code audit before merge.

## Data-source notes

Bybit currently exposes xStocks on Spot and TradFi Perpetuals through V5-compatible product categories; CFD/MT5 is a separate account/product. Adapters must therefore model the product/source explicitly instead of assuming all traditional assets share one transport.


## Current implementation checkpoint (2026-10-01)

Completed in the current working branch:
- Crypto x25 shadow recorder/replay/evaluator.
- Independent-channel exhaustion fusion and healthy-continuation veto.
- Causal validation and bounded per-symbol event memory.
- Adaptive abnormal-pump baseline uses prior observations only.
- Deterministic market/instrument routing and situation-aware priority resolution.
- Bybit xStocks public data loader with explicit underlying-token mapping.
- Injectable underlying-equity provider boundary.
- FX provider boundary plus timezone/DST-aware Asia/London/New York session context.
- US stock session context and gap calculation.
- Commodity provider boundary with no fabricated data source.
- Mixed-symbol routing prevents a symbol from being sent to unrelated market adapters.

Not yet claimed complete:
- Full live Stocks/Forex/Commodity analytical adapters.
- Extraction of the common Structure/MTF/Elliott/Fibonacci/Harmonics engine from dynamic_collector.py.
- Historical underlying-equity feed integration and FX intraday/session high-low provider validation.
- End-to-end CI execution on the current branch (status checks are not attached to the latest commit).


## Shared analytical core checkpoint

The first extraction boundary is now in `scan_plus/core/analytical_engine.py`.
It exposes Structure, Fibonacci, Elliott and Harmonics through an injected backend.
Crypto currently uses a compatibility backend that delegates to the canonical legacy
implementation, so extraction does not silently alter existing Crypto decisions.

Priority configuration now exposes both ordered evidence and non-destructive weights.
Weights affect decision emphasis only; lower-priority evidence remains available and
can invalidate a setup.

Next extraction rule: move one calculation family at a time behind this boundary,
compare legacy and extracted outputs on the same snapshots, and only then switch the
default backend. No blind rewrite of the monolithic collector.


## Cycle 2 checkpoint — analytical parity

- Added `AnalyticalEngine` as a reusable execution boundary for Structure, Fibonacci, Elliott and Harmonics.
- Crypto remains on the legacy implementation through `LegacyCryptoBackend`.
- Added parity tests comparing the wrapper directly with legacy Structure/Fibonacci outputs.
- Added profiled analysis so market/situation priorities can select reusable blocks without changing their semantics.
- No live default switch away from the legacy Crypto calculations has been made yet.


### Cycle 2 — step 2/4 complete
- Elliott parity test added.
- Harmonics parity test added.
- Analytical differential comparator added for legacy-vs-candidate outputs.
- Removed unused imports from the analytical boundary.


### Cycle 2 — step 3/4 complete
- Differential parity now covers three deterministic market shapes.
- Structure, Fibonacci, Elliott and Harmonics are compared against the canonical legacy backend.
- Added Core API boundary tests: unknown blocks cannot execute, profiles are mutation-safe, and cross-market symbols are rejected.
- Analytical block requests are explicitly whitelisted.


### Cycle 2 — step 4/4 complete: Core audit

Audit findings:
- Confirmed structural pivots are fractal-confirmed using left/right bars; the current bar is not treated as a confirmed pivot.
- BOS/CHoCH references are restricted to structural levels before the current bar.
- Fixed a real API edge case: an empty profiled block selection previously fell back to the full default block set because of truthiness-based defaulting. It now runs only the structural dependency when no optional reusable block was requested.
- Explicit whitelist prevents unknown analytical block names from executing.
- Profiles are deep-copied and cross-market symbols are rejected.
- Legacy Crypto remains authoritative; no default behavioral switch was made.
- Important limitation: current parity tests prove wrapper equivalence to the legacy implementation, not independent mathematical equivalence of a rewritten engine. The extraction remains deliberately staged.

Cycle 2 status: CLOSED.


## Cycle 3 — Stocks — step 1/4

Implemented the first independent Stocks adapter layer:
- Bybit xStocks loader remains the sole tokenized-stock market-data boundary.
- Underlying quote data is optional and isolated; no fake gap is produced when it is unavailable.
- Stock scan requests 5m/15m/1h/4h independently.
- Regular US session high/low is calculated from timestamped xStock candles.
- Gap context is calculated from underlying session open vs previous close when those fields are actually supplied.
- Shared Structure/Fibonacci/Elliott/Harmonics are reused through AnalyticalEngine.
- Crypto-only OI/funding/liquidation logic is not imported.
- Execution context explicitly distinguishes 24/7 xStock trading from underlying regular sessions.

This is intentionally not yet the final Stocks signal engine: underlying provider integration, gap/session evidence fusion, stock-specific candidate scoring, and execution/risk rules remain separate steps.


### Cycle 3 — Stocks — step 2/4 complete

- Added normalized underlying-provider payload contract.
- Underlying context now explicitly exposes readiness and normalized price/open/previous-close fields.
- Added stock-specific gap classification: gap_up / gap_down / flat / unknown.
- Added gap-fill progress without treating unavailable data as a signal.
- Stocks adapter now keeps xStock price, underlying data, and gap evidence separate.
- Added tests for valid/invalid underlying payloads and gap classification/fill progress.

No external underlying feed is hard-coded yet; provider integration remains injectable.


### Cycle 3 — Stocks — step 3/4 complete

- Added stock-specific candidate engine.
- Gap/session/underlying evidence is kept separate from shared Structure/Fibonacci/Elliott/Harmonics.
- A gap alone cannot create a directional candidate.
- Missing underlying or unresolved structure keeps the instrument in WATCH.
- Direction is taken from confirmed structural state, not inferred from gap direction.
- Stock situation is now passed into the priority engine, so gap scenarios can reorder evidence without disabling lower-priority blocks.
- Candidate output is explicitly non-execution: no order, entry, stop or target is generated at this stage.


### Cycle 3 — Stocks — step 4/4 complete

- Added non-executing stock execution/risk planner.
- Entry is represented as a limit reference/retest policy; no order submission occurs in the analytical layer.
- Invalidation comes from structural swing levels; target prefers Fibonacci extension and can fall back to a configured RR multiple.
- Position sizing is cash-risk based and requires an explicit account risk fraction; no hidden risk budget is assumed.
- Invalid stop side, unresolved candidate, missing structural invalidation, invalid account data, and sub-minimum RR all produce WAIT.
- xStocks 24/7 execution is kept separate from underlying regular-session context.
- Added safety tests proving no implicit risk budget and no order submission.

Cycle 3 Stocks status: CLOSED.


## Cycle 4 — Forex — step 1/4

Implemented the independent Forex adapter:
- provider remains isolated from analytical logic;
- candles are normalized to timestamped rows for session analysis;
- 5m/15m/1h/4h are requested independently;
- Asia/London/New York session high/low and overlap context are exposed per timeframe;
- session situation is derived from the newest available candle timestamp, not wall-clock time, making scans deterministic/replayable;
- shared Structure/Fibonacci/Elliott/Harmonics are reused through the analytical boundary;
- no Crypto OI/funding/liquidation assumptions enter the Forex adapter.

The current default Stooq loader remains a provider boundary; its actual intraday coverage must be validated before being treated as a production FX feed.


## Cross-cycle audit checkpoint — after Stocks / Forex step 1

Audit pass found and fixed several concrete issues:
- Session High/Low previously aggregated every matching session across the whole candle history. It now uses only the latest local session, preventing stale historical extremes from contaminating current-session evidence.
- Weekend timestamps previously could report FX/US equity sessions as active. Weekend sessions are now suppressed.
- Stock candidate Fibonacci readiness previously treated any non-empty Fibonacci mapping as ready; it now requires the explicit legacy `ready=True` flag.
- Stock scan session context previously used wall-clock time. It now derives session context from the newest available candle timestamp, making replay/backtest behavior deterministic.
- Stock execution tests had two calls missing the now-required explicit risk budget; corrected.
- Removed an unused execution import from the Stocks adapter.

Important feed limitation remains: the default Stooq adapter is only a provider boundary. Its requested intraday intervals are not yet accepted as a production-grade FX feed without an explicit coverage/quality validation. No production claim is made.

Audit status: previous Architecture/Core/Stocks work has now received a cross-cycle audit pass; concrete findings above were fixed before continuing Forex.


## Cross-market isolation audit — conflict check

Result: no direct shared-state mutation was found between Stocks and Forex; each adapter owns its loader/context and instantiates its own analytical engine boundary. Session utilities are provider-neutral and now timestamp-driven.

Two architectural risks remain and are intentionally NOT marked as resolved:
1. Stocks and Forex currently instantiate `AnalyticalEngine(LegacyCryptoBackend())`. This is an implementation dependency on the legacy Crypto class, not a proven market assumption leak, but it violates the desired long-term boundary. It must be replaced/extracted into a truly market-neutral analytical backend before the final integration audit.
2. `AnalyticalEngine.analyze_profiled()` silently ignores profile keys that are not shared-core blocks (e.g. order_flow, open_interest, session_liquidity, indicators). This does not cross-contaminate markets, but it can make a priority list appear to request evidence that the current engine did not calculate. Future adapter layers must explicitly separate shared-core priorities from market-adapter context priorities instead of silently dropping them.

No code was changed for these two items in this checkpoint because silently patching them would risk changing Crypto behavior. They are recorded as explicit integration gates.


## Conflict gates — FIXED

The two previously identified integration gates are now addressed:
1. Stocks and Forex use the market-neutral `CanonicalPricePatternBackend` contract instead of naming/instantiating the Crypto compatibility backend. The canonical implementation remains the legacy reference internally during staged extraction, preserving calculation parity without exposing Crypto semantics at the market-adapter boundary.
2. Analytical priorities are explicitly split into `shared` blocks and `market context` blocks. `AnalyticalEngine.analyze_profiled()` now reports requested context blocks instead of silently dropping them. Market adapters remain responsible for implementing their own context evidence.

This prevents a priority profile from claiming that a market-specific block was calculated when only the shared analytical core ran.


## Cycle 4 — Forex — step 2/4

- Added FX-specific session-liquidity context separate from the shared analytical core.
- Session interaction exposes whether price is inside/above/below the latest Asia/London/New York range.
- Latest-candle range interaction can identify a session high/low sweep only when the candle returns inside the prior session range; it does not infer trade direction.
- Forex adapter now exposes this evidence per timeframe.
- Fixed the Forex adapter to use the market-neutral `CanonicalPricePatternBackend` after the previous conflict audit.
- Session evidence remains context; it cannot override unresolved Structure/Elliott/Fibonacci analysis.


## Forex step-2 audit — completed

Audit found one substantive logic issue and fixed it:
- Session liquidity was comparing the latest candle against the range of the same latest session. That cannot represent a true prior-session sweep and could make session evidence misleading.
- `session_high_low()` now accepts an explicit `before_timestamp_ms` cutoff.
- Forex session context now requests each Asia/London/New York range strictly before the current scan timestamp, so sweep evidence references the latest prior available session rather than the current session itself.

The existing session rules remain timezone-aware and weekend-safe.
The session-liquidity module still does not infer trade direction.

Test-file creation for this exact new module was attempted but the GitHub write operation was blocked by the tool safety layer; therefore no test is claimed as created for that module. The logic remains an explicit audit item for the final regression pass.


## Forex step-3 — implementation + immediate audit

Implemented:
- FX candidate engine combining Structure, Elliott, Fibonacci and session-liquidity evidence.
- Bullish candidates require structural bullish state, explicit Fibonacci readiness, explicit Elliott readiness, and a low sweep.
- Bearish candidates require structural bearish state, explicit Fibonacci/Elliott readiness, and a high sweep.
- Session sweeps never determine direction alone.
- Scan situation now distinguishes `session_sweep` before generic `session_overlap`.
- Added a `session_sweep` priority profile emphasizing Structure/Elliott/Fibonacci/session liquidity.
- Added regression tests for unresolved structure, valid bullish sweep confirmation, and opposite-direction sweep rejection.

Immediate audit fixes:
- Elliott readiness was initially treated as any non-empty mapping; fixed to require explicit `ready=True`.
- Situation classification initially ignored detected sweeps when resolving priorities; fixed so sweep context reaches the priority engine.
- Existing prior-session cutoff remains enforced for session ranges, preventing same-session self-sweep contamination.


## Cycle 4 — Forex — step 4/4 + immediate audit

Implemented:
- Added a non-executing FX execution/risk planner.
- Entry is a limit/retest reference; structural invalidation is the stop source.
- Fibonacci extension is preferred for target; RR fallback is deterministic.
- Position sizing requires explicit account equity and risk fraction; no implicit risk budget.
- No crypto-style leverage/funding/OI/liquidation assumptions are used.
- Execution layer never submits orders.

Immediate audit fixes:
- Removed an unused execution import from the scan adapter; execution remains a separate layer.
- Candidate direction now requires the matching session sweep on the same confirmation timeframe as the confirmed shared analysis, preventing cross-timeframe evidence leakage.
- The Stooq provider boundary now explicitly rejects unvalidated intraday intervals instead of pretending to provide 5m/15m data.
- Forex adapter now surfaces provider limitations as structured `provider_error` fields instead of crashing the whole scan.
- Added regression coverage for provider rejection and confirmation-timeframe isolation.

Forex cycle status: CLOSED after implementation + audit.
Production-feed gate remains open: a validated intraday FX provider must be plugged into the provider boundary before real intraday Forex scans are considered production-ready.


## Cycle 5 — Commodities — step 1/4 + immediate audit

Implemented:
- Independent Commodities market adapter and provider boundary.
- Distinct instrument profiles for metals (XAUUSD/XAGUSD), energy (WTI/BRENT), and softs (COCOA).
- Shared price-pattern core is reused without importing Forex/Crypto/Stocks assumptions.
- Provider errors are isolated and surfaced as structured `provider_error`.
- Commodity scan timestamps are derived from normalized candle data.

Immediate audit fixes:
- Removed unsupported generic commodity `active_session` and `relative_strength` prescan assumptions because the adapter did not yet provide those evidence blocks.
- Fixed latest-timestamp extraction to use normalized candles rather than an optional provider metadata field.
- Commodity-specific macro/inventory/session context remains provider-gated; no synthetic evidence is invented.

Commodity cycle status: step 1/4 complete.


## Cycle 5 — Commodities — step 2/4 + immediate audit

Implemented:
- Added provider-gated Commodity Context Engine.
- Metals accept only explicitly supplied USD/yields/session context.
- Energy accepts explicitly supplied event/inventory/session context.
- Softs accept explicitly supplied event/session/volatility context.
- Context is normalized into available/unknown evidence and has no directional inference.
- Context is attached per timeframe without mutating shared analytical evidence.

Immediate audit:
- XAUUSD initially requested `session_liquidity`, but the commodity context layer did not implement that block. This was corrected by keeping liquidity in the shared commodity profile and reserving provider-specific context for `usd_yields_context`.
- Missing provider context remains UNKNOWN; no macro/inventory/event value is synthesized.
- Added regression tests for metals, energy and adapter exposure.

Commodity status: 2/4 complete.


## Cycle 5 — Commodities — step 3/4 + immediate audit

Implemented:
- Added instrument-group-aware Commodity Candidate Engine.
- Metals, energy and softs share the same directional gate but expose different provider-context confirmation sets.
- Structure is the primary directional gate; explicit Fibonacci and Elliott readiness are required on the same confirmation timeframe.
- Commodity-specific context is confirmatory only; missing context never blocks or creates direction by itself.
- Candidate output records available context and its evidence policy.

Immediate audit:
- Initial implementation allowed structural direction on one timeframe while Fibonacci/Elliott confirmation came from another. Fixed: shared confirmation must now be on the same timeframe as the structural direction.
- Added regression tests for valid candidate creation, unresolved structure, missing context, and cross-timeframe confirmation leakage.


## Cycle 5 — Commodities — step 4/4 + immediate audit

Implemented:
- Added a non-executing Commodity Execution/Risk planner.
- Uses the candidate confirmation timeframe as the execution evidence anchor.
- Structural invalidation provides the stop; Fibonacci extension is preferred for target; deterministic RR fallback is used when no valid extension is available.
- Position sizing uses explicit account equity and risk fraction.
- No contract multiplier, tick value, margin, leverage, inventory or macro value is invented.
- Order submission remains disabled in the analytical layer.

Immediate audit fixes:
- Commodity-specific context was initially collected across all timeframes. It is now bound to the same confirmation timeframe as Structure/Fibonacci/Elliott, preventing cross-timeframe context leakage.
- Execution target lookup was initially allowed to search other timeframes. It is now restricted to the candidate confirmation timeframe; otherwise it falls back to the configured RR target.
- Added regression tests for cash-risk sizing, WATCH blocking, and confirmation-timeframe enforcement.

Commodity cycle status: CLOSED after implementation + audit.


## Final Multi-Scan integration — step 1 + audit

Implemented:
- Unified command routing now supports plain `Скан`, market-scoped commands, and exact-symbol commands.
- Plain `Скан` expands market-native universes instead of returning PRESCAN_ONLY.
- Default native universe limits: Crypto 30, Stocks 15, Forex 9, Commodities 5.
- Crypto universe is sourced from its existing PreScan candidate ranking; Stocks are ranked from Bybit xStock 24h turnover; Forex and Commodities use explicit native symbol sets until their providers expose dynamic ranking.
- Added an explicit `default_symbols()` adapter contract and default registry wiring.
- Market failures remain isolated through `safe_scan()`.
- Exact commands such as `Скан EURUSD` and `Скан NVDA` resolve to their dedicated adapters.

Immediate audit fixes:
- Stocks adapter still contained a stale `LegacyCryptoBackend` reference; replaced with `CanonicalPricePatternBackend`.
- Stocks candidate initially allowed Structure on one timeframe and Fibonacci on another; fixed to require same-timeframe Fibonacci + Elliott confirmation.
- Market profiles contained unsupported context priorities (Forex relative strength/indicators; XAU active-session assumptions); removed or aligned them with implemented evidence boundaries.
- Commodity provider remains explicitly injectable; no fake default feed is introduced.

CI note: the repository's regression workflow exists, but this connector session does not expose a workflow-dispatch operation, and no CI status was returned for the latest commits. Therefore no claim of a green full CI run is made here.


## Final Multi-Scan integration — step 2 + audit

Implemented:
- Added Decision Aggregator as a cross-market normalization layer.
- It preserves each adapter's candidate/status/direction/situation and market-specific priority emphasis.
- It produces an attention ordering for the operator without creating a universal trade score, probability or cross-market directional vote.
- Crypto's legacy decision remains authoritative and is only normalized at the boundary.
- Data errors remain isolated and visible.

Immediate audit:
- Initial integration accidentally invoked `scan_many()` twice, which would have duplicated provider calls and doubled load. Fixed by storing one result batch and passing that same batch to the aggregator.
- Added regression tests for candidate preservation, Crypto legacy adaptation, error isolation and the no-universal-score policy.

Current pipeline:
Market adapters → native candidate → Decision Aggregator → operator-facing attention list.


## Final Multi-Scan integration — step 3 + audit

Implemented:
- Added compact operator view with separate Candidate shortlist, Watchlist, summary and isolated errors.
- Candidate technical details can be expanded from the shortlist source without changing candidate logic.
- Unified Scan now returns both the raw result set and the compact operator view for backward compatibility.
- Added regression tests for truncation and empty-result safety.

Immediate audit:
- Attention ordering initially used the maximum market-specific block weight as a hidden cross-market ranking signal. This was removed: status class is the only cross-market attention class; market-specific priorities are preserved as evidence/focus, not converted into a universal score.
- Operator shortlist limits are sanitized to non-negative integers.
- No probability, expected return or winner selection is produced.

Final Multi-Scan pipeline:
Market-specific provider → market adapter → market-specific candidate → cross-market normalization → compact operator view.


## Final end-to-end audit — completed

Audit path:
Provider → Adapter → Profile → Priority → Candidate → Execution boundary → Multi-Scan → Decision Aggregator → Operator View.

Findings fixed:
1. FX session ranges were only timestamp-cut off, which could still select the currently forming London/NY session. Session range extraction now accepts only sessions whose scheduled end is already before the scan timestamp.
2. FX profile contained an unsupported `relative_strength` priority request; removed.
3. XAUUSD/Forex/stock instrument profiles contained unsupported generic session/liquidity priority names; aligned profiles with evidence blocks actually implemented by their adapters.
4. Stock NVDA profile requested `session_liquidity` while the stock adapter exposes regular-session levels/active session context instead; replaced with the implemented `session` context request.
5. Added regression coverage for incomplete versus completed FX session ranges.

Architectural invariants rechecked:
- Market-specific providers never mutate the shared analytical core.
- Structure/Fibonacci/Elliott confirmation does not leak across confirmation timeframes in Stocks, Forex or Commodities.
- Commodity macro/inventory/event context is provider-gated and non-directional.
- Crypto remains legacy-authoritative; the multi-market layer normalizes rather than rewrites its decision.
- Decision Aggregator does not create a universal trade score or cross-market direction vote.
- Operator View is presentation-only.
- Execution planners are non-submitting analysis boundaries.
- One unified scan result batch is reused by aggregation; no duplicate full scan.
- Provider failures remain isolated per market/instrument.

Validation limitation:
- The repository has regression tests and a CI workflow, but this GitHub connector session does not expose a workflow-dispatch/run operation and no fresh CI status was available for the latest branch state. Therefore this audit is a code/contract audit plus added regression coverage, not a claimed green CI run.


## Real-data readiness pass

Before live runtime validation, the provider contract audit exposed two integration blockers:
- Forex Scan+ requested 5m/15m/1h/4h, while its default Stooq loader explicitly rejected intraday intervals. Replaced the default with a public Yahoo chart loader; 4h is built by deterministic aggregation of hourly candles.
- Commodities had no default provider and therefore plain Multi-Scan could return NO_UNIVERSE for the entire commodity branch. Added explicit Yahoo futures mappings: XAUUSD→GC=F, XAGUSD→SI=F, WTI→CL=F, BRENT→BZ=F, COCOA→CC=F.

Performance:
- Unified instrument scans are now bounded-parallel (up to 8 workers) instead of serial.
- Decision ordering has a deterministic symbol tie-breaker so concurrency cannot randomize the shortlist.

Runtime surface:
- Added `/scan-plus?command=скан` endpoint to exercise the unified orchestrator on the deployed service.
- Added provider regression tests with mocked HTTP responses.

Live validation limitation:
- The GitHub connector can read workflow/status data but this session has no workflow-dispatch operation, and the public Yahoo chart endpoint was not directly fetchable through the web fetcher. Therefore provider wiring is implemented and regression-covered, but no claim of a successful live Render run is made until the deployed endpoint is actually exercised.


## MTF State Engine — implemented + audited

Implemented:
- Added explicit MTF State Engine separating higher-timeframe context from lower-timeframe execution state.
- States are `bullish`, `bearish`, `transition` or `unknown`; no direction averaging into a score.
- HTF bullish + LTF conflict is represented as `countertrend_correction` only when a directional LTF state is actually confirmed; an explicit neutral/transition LTF stays `context_only`.
- A lower-timeframe conflict never becomes a confirmed reversal.
- Reversal confirmation requires a medium/higher timeframe structural side change.
- Transition remains pending confirmation.
- Crypto exposes the new MTF state additively; the legacy Crypto decision remains authoritative.
- Multi-market normalization preserves the MTF state.

Audit fixes:
- Raw confluence initially could override an explicit neutral/range/transition state. Fixed: explicit neutral/transition state wins.
- Added regression tests for bullish HTF + bearish/neutral LTF, aligned trend, medium-timeframe reversal and unresolved transition.


## MTF execution gate — completed

The MTF State Engine is now a real candidate gate, not telemetry only:
- Forex, Stocks and Commodities candidates require an aligned execution state before reaching CANDIDATE.
- `countertrend_correction`, `context_only` and `unresolved` are downgraded to WATCH until confirmation.
- A structurally confirmed medium/higher-timeframe reversal is allowed through the final gate.
- Decision Aggregator applies the same final boundary rule when an MTF state is present, protecting against future adapters bypassing their local gate.
- Crypto remains legacy-authoritative; its MTF state is additive and only affects the shared gate when the legacy result actually exposes a usable MTF state.

Audit fixes:
- MTF state extraction initially read the wrong level of the analytical frame payload; it now reads `analysis.structure` and related fields.
- Explicit structural transition now overrides derived direction/raw confluence.
- Removed the stale Stooq default reference from the Forex adapter after the Yahoo provider migration.
- Added regression tests for the final MTF gate.


## Additional hardening audit — 2026-10-02

Found and fixed during post-MTF hardening:
- Commodity default registry passed an object provider into a wrapper that only accepted callables; adapter now accepts both provider objects exposing `candles()` and callable fetchers.
- Forex had no `default_symbols()`, so the unified default Scan silently produced `NO_UNIVERSE` for Forex; added a deterministic 9-pair native universe.
- Bybit xStock resolver classified explicit symbols such as `NVDAXUSDT` as crypto; added explicit xStock token resolution.
- Unknown but valid Bybit xStock underlyings now map to `<UNDERLYING>XUSDT` instead of falling back to an invalid underlying symbol.
- Session-level extraction without an explicit cutoff could include the currently forming session; it now derives the cutoff from the newest supplied candle and only uses completed sessions.
- An existing session regression test was inconsistent with the completed-session contract; corrected it.
- MTF state extraction now supports both flat test frames and real adapter frames containing `analysis.structure`.
- Yahoo 4H aggregation now sorts provider candles before bucketing, preventing out-of-order provider data from corrupting OHLC aggregation.

New regression coverage:
- provider-object boundary
- completed-session safety
- Forex default universe
- xStock resolution/mapping
- MTF gate
- flat/real MTF frame contracts

Current branch head: `b643894c0a0b342787532013db63a1059bd6f632`.
GitHub reports no commit status checks for this branch head, so no green CI result is claimed.


## Cross-market isolation audit — 2026-10-02

Audit scope: `Скан` command parsing → market/symbol resolution → native universe expansion → concurrent adapter scan → adapter result contract → cross-market normalization → MTF gate → Operator View.

### Isolation invariants now enforced
1. A registry request is bound to exactly one market namespace.
2. An adapter result must return the same top-level market as the request.
3. An adapter result must not silently return a different symbol than requested.
4. A market-symbol mismatch in an explicitly scoped command is surfaced as DATA_ERROR instead of being silently dropped.
5. An unresolved symbol in a multi-market command is surfaced as an ambiguity error instead of being routed arbitrarily.
6. NO_UNIVERSE is treated as a data/error condition, never as a WATCH candidate.
7. Aggregation checks wrapper market provenance against payload market before normalizing.
8. Operator View remains presentation-only and receives only normalized, provenance-checked results.

### Market-by-market findings
- Crypto: mutable event/baseline state is owned by the Crypto adapter and keyed by symbol; no shared state is exposed to the other adapters.
- Stocks: xStock/underlying/session/gap evidence remains inside the Stocks adapter; no crypto OI/funding or FX session state is imported.
- Forex: session liquidity is generated from the Forex adapter's own candles and timezone schedule; no stock gap or crypto derivative evidence enters its candidate engine.
- Commodities: provider context is explicitly scoped to the commodity instrument/group; missing macro/inventory/event context remains unknown.
- Shared analytical core: the reusable structure/Fibonacci/Elliott/harmonic backend is market-neutral and receives only the rows supplied by the calling adapter. Default registry construction creates separate adapter instances and separate analytical-engine instances.

### Important non-bug design point
The aggregator intentionally places all markets into one Operator View, but it does not combine their directions or evidence into a universal trade score. Its rank is an attention/presentation ordering only.

### Regression coverage added
`tests/test_cross_market_isolation.py` covers:
- four-market default routing;
- explicit market/symbol mismatch;
- ambiguous multi-market symbols;
- adapter market-contract violation;
- adapter symbol-contract violation;
- NO_UNIVERSE error preservation;
- payload/wrapper market provenance mismatch;
- independent four-market Operator View output.

### Verification limitation
The environment could not clone the GitHub repository directly because outbound DNS/network access was unavailable. GitHub currently reports no status checks for the branch head, so this audit does not claim a green CI run; the changes were reviewed against the repository source and regression tests were added for the discovered isolation boundaries.


## Data/timestamp audit — follow-up fixes

The second pass found a separate class of isolation risk: even with correct market routing, malformed, duplicated, out-of-order or wrong-symbol provider rows could enter the shared analytical core.

### Fixed
- Added `scan_plus.core.data_contract.normalize_candles`.
- Stocks, Forex and Commodities now normalize and validate OHLC/timestamps before analytical processing.
- Duplicate timestamps are removed deterministically and rows are sorted by timestamp.
- Invalid OHLC rows are rejected rather than passed to shared structure/Fibonacci/Elliott logic.
- Every accepted candle carries a market-local symbol/interval contract marker.
- Stock underlying quotes now reject a provider payload whose symbol differs from the requested underlying.
- Added regression tests for timestamp normalization, duplicate isolation, malformed OHLC rejection and underlying-symbol provenance.

### Remaining provider-specific behavior
4H FX/commodity bars are derived from provider 1H data on explicit UTC four-hour buckets. This is intentional and market-neutral; it must not be interpreted as an exchange-native 4H candle. The derived-bar path remains isolated to the owning adapter/provider and is not shared with Crypto or tokenized Stocks.


## Final MTF → Operator View audit — 2026-10-02

End-to-end boundary audited from provider candles through MTF/candidate normalization to Operator View.

### Fixed
- Synthetic 4H FX/commodity candles are now created only from four contiguous hourly bars. Provider gaps can no longer manufacture a false 4H candle.
- Final decision normalization now requires a known market and non-empty symbol provenance.
- Unknown candidate statuses are rejected as DATA_ERROR instead of silently becoming WATCH.
- Candidate-level DATA_ERROR cannot become WATCH.
- Operator View preserves `mtf_state` and `mtf_gate_blocked`, so a user can see why a candidate was downgraded.
- Operator View's `scanned` count now includes data errors; failed instruments are no longer hidden from the scan summary.
- Regression coverage added for complete/incomplete 4H blocks, malformed final status, missing symbol, invalid market and MTF gate propagation.

### Final invariant
The final path is now intended to satisfy:
`provider → data contract → market adapter → closed MTF frames → candidate gate → provenance-checked aggregator → presentation-only Operator View`.

Operator View cannot upgrade a WATCH/NO_TRADE/DATA_ERROR into a candidate; it can only display already-normalized states. It also does not calculate a cross-market trade score or combine evidence between markets.

CI/runtime limitation remains: GitHub exposes no status checks for the current branch head in this environment, so the audit records code-level and regression coverage, not a claimed green deployment run.


## Hostile-input audit

Additional hardening: commodity full scans now exclude forming candles; duplicate explicit and native-universe symbols are deduplicated before concurrent execution; hostile-input regression tests cover empty frames, duplicate symbols and cross-market symbol provenance. The hostile-input invariants are malformed OHLC rejected, forming candles excluded, incomplete 4H rejected, unknown market/symbol/status rejected, DATA_ERROR cannot become WATCH, MTF disagreement is not reversal, empty frames cannot become CANDIDATE, and Operator View cannot rewrite status. No green CI claim is made because the branch exposes no status checks in the current environment.


## V3.7 performance/simplicity audit

Completed a focused audit for duplicated work, unnecessary state mutation and nondeterministic output.

Fixed:
- Removed an unused routing-layer import.
- Deduplicated explicit and native-universe instruments before provider execution.
- Kept provider scans concurrent with a bounded worker pool, while restoring deterministic request-order results after futures complete.
- Restored the structural MTF reversal gate: disagreement alone is not reversal; CHOCH/MSS on 4H/1H is required.
- MTF transition now reuses already validated frame states instead of re-parsing frames.
- Added regression coverage for the reversal gate and deterministic parallel result ordering.

The optimization rule remains: no performance optimization may weaken provenance, closed-candle, MTF or candidate-gating invariants.


## Final regression/conflict audit — 2026-10-02

Found and fixed three cross-layer regressions:
- The final Aggregator was applying the additive MTF gate to Crypto even though the Crypto adapter contract explicitly says the legacy Crypto decision remains authoritative. Crypto is now exempt from that shared MTF rewrite; its MTF state remains telemetry/evidence until Crypto migration explicitly changes the authority contract.
- The MTF regression test still encoded the old rule that any 4H/1H disagreement could confirm reversal. The test now requires an explicit CHOCH event, matching the implementation contract.
- Candle `end` timestamps could be supplied in seconds while `timestamp_ms` was milliseconds; the data contract now normalizes `end` to milliseconds before closed-candle checks.
- Crypto shadow snapshots using the same `generated_at` millisecond could overwrite one another. Snapshot filenames now include a short unique suffix.
- Added regression coverage for all four cases.

This pass confirms an important architecture rule: shared infrastructure may validate and observe every market, but it must not silently change the authority model of a market that is still on a legacy backend.
