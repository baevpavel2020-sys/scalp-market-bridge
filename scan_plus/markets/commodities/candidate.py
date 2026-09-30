"""Commodity candidate engine.

Shared price structure is the directional gate. Commodity-specific context is
confirmatory only and can never manufacture a direction when the provider did not
supply the relevant evidence.
"""
from typing import Mapping


GROUP_RULES = {
    "metals": {"context_bonus": ("usd_context","usd_yields_context")},
    "energy": {"context_bonus": ("event_context","inventory_context")},
    "softs": {"context_bonus": ("event_context","volatility_context")},
}


def _direction(frames):
    for tf in ("15m","1h","4h"):
        analysis=(frames.get(tf) or {}).get("analysis") or {}
        structure=analysis.get("structure") or {}
        state=str(structure.get("state") or structure.get("trend") or "").lower()
        if state in ("bullish","uptrend"): return "bullish",tf
        if state in ("bearish","downtrend"): return "bearish",tf
    return "unknown",None


def _shared_confirmation(frames):
    for tf in ("15m","1h","4h"):
        analysis=(frames.get(tf) or {}).get("analysis") or {}
        fib=analysis.get("fibonacci") or {}
        ell=analysis.get("elliott") or {}
        if fib.get("ready") is True and ell.get("ready") is True:
            return True,tf
    return False,None


def _context_available(frames, keys):
    found=[]
    for frame in (frames or {}).values():
        ctx=frame.get("commodity_context") or {}
        available=set(ctx.get("available") or [])
        found.extend(k for k in keys if k in available)
    return sorted(set(found))


def build_commodity_candidate(*, symbol, group, frames):
    direction,structure_tf=_direction(frames)
    shared_ready,confirmation_tf=_shared_confirmation(frames)
    rules=GROUP_RULES.get(str(group or "").lower(),{})
    context=_context_available(frames,rules.get("context_bonus",()))

    blockers=[]
    if direction=="unknown": blockers.append("structure_direction_unresolved")
    if not shared_ready: blockers.append("fibonacci_or_elliott_unavailable")

    # External commodity context is confirmatory, not a hard gate.
    status="WATCH" if blockers else "CANDIDATE"
    reason=",".join(blockers) if blockers else "structure_plus_shared_confirmation"

    return {
        "status":status,
        "reason":reason,
        "direction":direction,
        "symbol":str(symbol).upper(),
        "group":group,
        "structure_timeframe":structure_tf,
        "confirmation_timeframe":confirmation_tf,
        "context_available":context,
        "evidence_policy":{
            "structure_primary":True,
            "elliott_required":True,
            "fibonacci_required":True,
            "commodity_context_confirmatory":True,
            "missing_context_never_directional":True,
        },
    }
