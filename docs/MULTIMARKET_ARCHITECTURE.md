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
