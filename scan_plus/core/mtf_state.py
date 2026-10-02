"""Market-timeframe state engine.

Separates higher-timeframe context from lower-timeframe execution state. It never
averages directions into a probability and never promotes a lower-timeframe conflict
to a confirmed reversal without structural confirmation.
"""
from typing import Mapping
import time

TF_ORDER=("1d","4h","1h","15m","5m","1m")
BULL={"bullish","uptrend"}
BEAR={"bearish","downtrend"}
TRANSITION={"range_or_transition","transition","pending_confirmation","unknown","neutral"}


def _state(frame):
    if not isinstance(frame,Mapping):
        return "unknown"
    analysis=frame.get("analysis") if isinstance(frame.get("analysis"),Mapping) else frame
    raw_structure=analysis.get("structure")
    if isinstance(raw_structure,Mapping):
        structure_state=str(raw_structure.get("state") or raw_structure.get("trend") or "").lower()
    else:
        structure_state=str(raw_structure or analysis.get("structure_state") or "").lower()
    raw=str(analysis.get("raw_confluence") or frame.get("raw_confluence") or "").lower()
    direction=str(analysis.get("direction") or frame.get("direction") or "").lower()

    # Explicit structural transition/neutral state has priority over derived direction
    # and raw confluence. Structure is the authoritative state at its own timeframe.
    if structure_state in TRANSITION or structure_state in ("range","range_or_transition"):
        return "transition"
    if structure_state in BULL:
        return "bullish"
    if structure_state in BEAR:
        return "bearish"
    if direction in TRANSITION or direction in ("neutral","range","sideways"):
        return "transition"
    if direction in BULL:
        return "bullish"
    if direction in BEAR:
        return "bearish"
    if raw in BULL:
        return "bullish"
    if raw in BEAR:
        return "bearish"
    return "unknown"



def _frame_fresh(frame):
    if not isinstance(frame,Mapping):
        return False
    quality=frame.get('data_quality') or {}
    if quality and quality.get('closed_only') and quality.get('last_closed') is not True:
        return False
    latest=frame.get('latest_timestamp_ms') or quality.get('last_timestamp_ms')
    if latest is None:
        return True
    try:
        return int(latest) <= int(time.time()*1000)
    except (TypeError,ValueError):
        return False

def build_mtf_state(frames):
    normalized={str(k).lower():v for k,v in (frames or {}).items()}
    states={tf:(_state(normalized[tf]) if _frame_fresh(normalized[tf]) else 'transition') for tf in TF_ORDER if tf in normalized}

    htf=[tf for tf in ("1d","4h") if states.get(tf) in ("bullish","bearish")]
    mtf=[tf for tf in ("4h","1h") if states.get(tf) in ("bullish","bearish")]
    ltf=[tf for tf in ("15m","5m","1m") if states.get(tf) in ("bullish","bearish")]

    context=None
    for tf in ("1d","4h","1h"):
        if states.get(tf) in ("bullish","bearish"):
            context=states[tf]
            break

    execution=None
    for tf in ("15m","5m","1m"):
        if states.get(tf) in ("bullish","bearish"):
            execution=states[tf]
            break

    if context is None:
        regime="unresolved"
    elif execution is None:
        regime="context_only"
    elif execution==context:
        regime="aligned"
    else:
        regime="countertrend_correction"

    # A disagreement is not a reversal. Require a structural CHOCH/MSS
    # event on 4H/1H in the opposing direction.
    reversal_confirmed=False
    for tf in ("4h","1h"):
        frame=normalized.get(tf) or {}
        analysis=frame.get("analysis") if isinstance(frame.get("analysis"),Mapping) else frame
        structure=analysis.get("structure") if isinstance(analysis,Mapping) else {}
        if not isinstance(structure,Mapping):
            continue
        event=structure.get("last_event") or {}
        event_type=str(event.get("type") or "").upper()
        event_direction=str(event.get("direction") or "").lower()
        if event_type in ("CHOCH","MSS") and event_direction in ("bullish","bearish"):
            if context and event_direction != context:
                reversal_confirmed=True
                break

    transition = any(states.get(tf)=="transition" for tf in TF_ORDER if tf in states)

    return {
        "states":states,
        "context_direction":context,
        "execution_direction":execution,
        "regime":regime,
        "transition":transition,
        "reversal_confirmed":reversal_confirmed,
        "policy":{
            "no_direction_averaging":True,
            "ltf_conflict_is_not_reversal":True,
            "transition_requires_confirmation":True,
            "context_and_execution_are_separate":True,
        },
    }
