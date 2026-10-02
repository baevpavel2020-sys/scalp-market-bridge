"""Forex situation/candidate engine.

Session liquidity is contextual evidence. Structure, Elliott and Fibonacci remain the
primary directional gate; a session sweep alone never creates a trade candidate.
"""
from typing import Mapping
from scan_plus.core.mtf_state import build_mtf_state


def _structure_state(frames):
    for tf in ("15m","1h","4h"):
        structure=((frames.get(tf) or {}).get("analysis") or {}).get("structure") or {}
        state=str(structure.get("state") or structure.get("trend") or "").lower()
        if state in ("uptrend","bullish"): return "bullish",tf
        if state in ("downtrend","bearish"): return "bearish",tf
    return "unknown",None


def _shared_ready(frames, structure_tf):
    if structure_tf is None:
        return False,None
    analysis=(frames.get(structure_tf) or {}).get("analysis") or {}
    fib=analysis.get("fibonacci") or {}
    ell=analysis.get("elliott") or {}
    if fib.get("ready") is True and ell.get("ready") is True:
        return True,structure_tf
    return False,None


def _sweeps(frames):
    found=[]
    for tf,frame in (frames or {}).items():
        liq=frame.get("session_liquidity") or {}
        sweeps=liq.get("latest_candle_sweeps") or {}
        for session,data in sweeps.items():
            if isinstance(data,Mapping) and data.get("state") in ("high_sweep","low_sweep"):
                found.append((tf,session,data["state"]))
    return found


def build_forex_candidate(*, frames, active_sessions=None, overlap=False):
    direction,structure_tf=_structure_state(frames)
    shared_ready,confirmation_tf=_shared_ready(frames,structure_tf)
    mtf_state=build_mtf_state(frames)
    sweeps=_sweeps(frames)

    bullish_sweep=any(state=="low_sweep" and tf==confirmation_tf for tf,_,state in sweeps)
    bearish_sweep=any(state=="high_sweep" and tf==confirmation_tf for tf,_,state in sweeps)

    blockers=[]
    if direction=="unknown": blockers.append("structure_direction_unresolved")
    if mtf_state["regime"]=="countertrend_correction" or (mtf_state["regime"] in ("context_only","unresolved") and mtf_state.get("context_direction") is not None):
        blockers.append("mtf_execution_not_aligned")
    if not shared_ready: blockers.append("fibonacci_or_elliott_unavailable")

    # Sweep is confirmation/context, never the source of direction.
    if direction=="bullish" and not bullish_sweep:
        blockers.append("bullish_session_sweep_unconfirmed")
    if direction=="bearish" and not bearish_sweep:
        blockers.append("bearish_session_sweep_unconfirmed")

    if blockers:
        status="WATCH"
        reason=",".join(blockers)
    else:
        status="CANDIDATE"
        reason="structure_plus_session_liquidity_confirmation"

    return {
        "status":status,
        "reason":reason,
        "direction":direction,
        "situation":"session_overlap" if overlap else ("session_sweep" if sweeps else "continuation"),
        "structure_timeframe":structure_tf,
        "confirmation_timeframe":confirmation_tf,
        "active_sessions":list(active_sessions or []),
        "sweeps":sweeps,
        "mtf_state":mtf_state,
        "evidence_policy":{
            "structure_primary":True,
            "elliott_required":True,
            "fibonacci_required":True,
            "session_liquidity_confirmatory":True,
            "sweep_alone_never_directional":True,
        },
    }
