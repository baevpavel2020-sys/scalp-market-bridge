"""Liquidity & Leverage Event Engine.

Pure classification layer: it explains *why* a setup may exist. It never rewrites
Structure/MTF/Elliott/Fibonacci evidence and it never creates a trade by itself.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    family: str
    direction: Optional[str]
    confidence: float
    evidence: tuple = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)


def _clamp(value):
    return max(0.0, min(1.0, float(value or 0.0)))


def _event_id(family, direction, anchor=None):
    return f"{family}:{direction or 'neutral'}:{anchor or 'current'}"


def deduplicate_events(events: Iterable[MarketEvent]) -> List[MarketEvent]:
    """Keep one canonical event per family/direction/anchor.

    Multiple indicators may support one event, but must not become independent votes.
    """
    best: Dict[str, MarketEvent] = {}
    for event in events:
        previous = best.get(event.event_id)
        if previous is None or event.confidence > previous.confidence:
            best[event.event_id] = event
    return list(best.values())


def classify_liquidity_leverage(snapshot: Mapping[str, Any]) -> List[MarketEvent]:
    """Classify normalized evidence supplied by a market adapter.

    Expected inputs are intentionally generic so Crypto, Stocks, FX and Commodities
    can reuse the engine while omitting evidence they do not possess.
    """
    events = []
    direction = snapshot.get("direction")
    anchor = snapshot.get("event_anchor") or "current"

    sweep = snapshot.get("liquidity_sweep") or {}
    if sweep.get("active"):
        events.append(MarketEvent(
            _event_id("liquidity_sweep", sweep.get("direction"), anchor),
            "liquidity_sweep", sweep.get("direction"),
            _clamp(sweep.get("confidence", 0.6)),
            tuple(sweep.get("evidence") or ("liquidity_sweep",)),
        ))

    failed = snapshot.get("failed_acceptance") or {}
    if failed.get("active"):
        events.append(MarketEvent(
            _event_id("failed_move", failed.get("direction"), anchor),
            "failed_move", failed.get("direction"),
            _clamp(failed.get("confidence", 0.7)),
            tuple(failed.get("evidence") or ("failed_acceptance",)),
        ))

    cascade = snapshot.get("liquidation_cascade") or {}
    if cascade.get("active"):
        side = cascade.get("side")
        family = "long_liquidation_cascade" if side == "longs" else "short_squeeze" if side == "shorts" else "liquidation_event"
        events.append(MarketEvent(
            _event_id(family, direction, anchor), family, direction,
            _clamp(cascade.get("confidence", 0.7)),
            tuple(cascade.get("evidence") or ("liquidations",)),
        ))

    crowded = snapshot.get("crowded_positioning") or {}
    if crowded.get("active"):
        events.append(MarketEvent(
            _event_id("crowded_positioning", crowded.get("direction"), anchor),
            "crowded_positioning", crowded.get("direction"),
            _clamp(crowded.get("confidence", 0.55)),
            tuple(crowded.get("evidence") or ("positioning",)),
        ))

    pump = snapshot.get("pump_exhaustion") or {}
    if pump.get("active"):
        events.append(MarketEvent(
            _event_id("pump_exhaustion", "bearish", anchor),
            "pump_exhaustion", "bearish",
            _clamp(pump.get("confidence", 0.65)),
            tuple(pump.get("evidence") or ("pump_exhaustion",)),
        ))

    return deduplicate_events(events)
