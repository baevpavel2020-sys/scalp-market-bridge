"""Market-timeframe state engine.

Separates higher-timeframe context from lower-timeframe execution state. It never
averages directions into a probability and never promotes a lower-timeframe conflict
to a confirmed reversal without structural confirmation.
"""
from typing import Mapping

TF_ORDER=("1d","4h","1h","15m","5m","1m")
BULL={"bullish","uptrend"}
BEAR={"bearish","downtrend"}
TRANSITION={"range_or_transition","transition","pending_confirmation","unknown","neutral"}


def _state(frame):
    if not isinstance(frame,Mapping):
        return "unknown"
    structure=str(frame.get("structure") or "").lower()
    raw=str(frame.get("raw_confluence") or "").lower()
    direction=str(frame.get("direction") or "").lower()
    if structure in BULL or direction in BULL:
        return "bullish"
    if structure in BEAR or direction in BEAR:
        return "bearish"
    if structure in TRANSITION or direction in TRANSITION:
        return "transition"
    if raw in BULL:
        return "bullish"
    if raw in BEAR:
        return "bearish"
    return "unknown"


def build_mtf_state(frames):
    normalized={str(k).lower():v for k,v in (frames or {}).items()}
    states={tf:_state(normalized[tf]) for tf in TF_ORDER if tf in normalized}

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

    # A reversal is only structurally confirmed when the medium/higher structure
    # itself agrees with the new side. A single LTF conflict is not a reversal.
    reversal_confirmed=False
    for tf in ("4h","1h"):
        if states.get(tf) and states.get(tf)!=context and states.get(tf) in ("bullish","bearish"):
            reversal_confirmed=True
            break

    transition = any(
        _state(normalized.get(tf))=="transition" for tf in TF_ORDER if tf in normalized
    )

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
