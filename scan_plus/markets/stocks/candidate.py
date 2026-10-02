"""Stock-specific situation and candidate engine.

This layer fuses evidence; it does not place orders. Evidence remains traceable and
unknown/missing data never becomes a bullish or bearish vote.
"""
from typing import Mapping
from scan_plus.markets.stocks.session_gap import classify_gap


def _structure_state(frames):
    for tf in ("15m","1h","4h"):
        structure=((frames.get(tf) or {}).get("analysis") or {}).get("structure") or {}
        state=str(structure.get("state") or structure.get("trend") or "").lower()
        if state:
            return state,tf
    return "unknown",None


def _shared_confirmation(frames, structure_tf):
    if structure_tf is None:
        return False,None
    analysis=((frames.get(structure_tf) or {}).get("analysis") or {})
    fib=analysis.get("fibonacci") or {}
    ell=analysis.get("elliott") or {}
    return (
        fib.get("ready") is True and ell.get("ready") is True,
        structure_tf if fib.get("ready") is True and ell.get("ready") is True else None,
    )


def build_stock_candidate(*, frames, gap, underlying, ticker, session=None):
    gap_info=classify_gap(gap or {})
    structure,structure_tf=_structure_state(frames or {})
    fibo,fibo_tf=_shared_confirmation(frames or {},structure_tf)
    underlying_ready=bool(underlying and underlying.get("ready",True))
    price=float((ticker or {}).get("last_price") or 0)

    evidence={
        "gap":{"state":gap_info.get("state","unknown"),"pct":gap_info.get("pct"),
               "ready":gap_info.get("state")!="unknown"},
        "underlying":{"ready":underlying_ready},
        "structure":{"state":structure,"timeframe":structure_tf},
        "fibonacci":{"ready":fibo,"timeframe":fibo_tf},
        "session":{"active":bool(session)},
    }

    blockers=[]
    if structure=="unknown": blockers.append("structure_unavailable")
    if not fibo: blockers.append("fibonacci_or_elliott_unavailable")
    if not underlying_ready: blockers.append("underlying_unavailable")

    gap_state=gap_info.get("state")
    situation="gap" if gap_state in ("gap_up","gap_down") else "continuation"
    # No direction is inferred from a gap alone. Direction becomes available only
    # when structural evidence itself is directional.
    direction = structure if structure in ("bullish","bearish","uptrend","downtrend") else "unknown"
    if direction=="uptrend": direction="bullish"
    if direction=="downtrend": direction="bearish"

    if blockers:
        status="WATCH"
        reason=",".join(blockers)
    elif direction=="unknown":
        status="WATCH"
        reason="structure_direction_unresolved"
    else:
        status="CANDIDATE"
        reason="multi_block_confirmation_pending_execution"

    return {
        "status":status,
        "reason":reason,
        "situation":situation,
        "direction":direction,
        "price":price,
        "evidence":evidence,
        "decision_rule":"gap/session context cannot override unresolved structure",
    }
