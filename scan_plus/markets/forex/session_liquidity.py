"""FX session-liquidity evidence.

This module detects interaction with the latest completed/available session range.
It does not infer trade direction by itself.
"""
from typing import Mapping


def _range(session):
    if not isinstance(session,Mapping) or not session.get("ready"):
        return None
    try:
        return float(session["high"]),float(session["low"])
    except (KeyError,TypeError,ValueError):
        return None


def session_interaction(price, session):
    rng=_range(session)
    if rng is None:
        return {"ready":False,"state":"unknown"}
    high,low=rng
    try: px=float(price)
    except (TypeError,ValueError):
        return {"ready":False,"state":"unknown"}
    if px>high: state="above_high"
    elif px<low: state="below_low"
    else: state="inside"
    return {"ready":True,"state":state,"high":high,"low":low,"price":px}


def session_sweep(candles, session):
    rng=_range(session)
    if rng is None:
        return {"ready":False,"state":"unknown"}
    high,low=rng
    if not candles:
        return {"ready":False,"state":"unknown"}
    last=candles[-1]
    try:
        h=float(last["high"]); l=float(last["low"]); close=float(last["close"])
    except (KeyError,TypeError,ValueError):
        return {"ready":False,"state":"unknown"}
    swept_high=h>high and close<=high
    swept_low=l<low and close>=low
    if swept_high and not swept_low: state="high_sweep"
    elif swept_low and not swept_high: state="low_sweep"
    elif swept_high and swept_low: state="both_sides_sweep"
    else: state="none"
    return {"ready":True,"state":state,"high":high,"low":low,"close":close}
