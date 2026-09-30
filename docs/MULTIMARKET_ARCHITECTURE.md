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
