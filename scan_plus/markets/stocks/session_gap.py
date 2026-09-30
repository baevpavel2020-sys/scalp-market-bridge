"""Stock-specific session and gap evidence."""
from typing import Mapping


def classify_gap(gap: Mapping, threshold_pct=0.15):
    if not gap or not gap.get("ready"):
        return {"state":"unknown","reason":gap.get("reason","gap_unavailable") if gap else "gap_unavailable"}
    pct=float(gap.get("pct",0))
    if abs(pct) < float(threshold_pct):
        return {"state":"flat","pct":pct}
    return {"state":"gap_up" if pct>0 else "gap_down","pct":pct}


def gap_fill_progress(gap_open, previous_close, current_price):
    try:
        go=float(gap_open); pc=float(previous_close); px=float(current_price)
    except (TypeError,ValueError):
        return {"ready":False,"reason":"invalid_gap_values"}
    distance=go-pc
    if distance == 0:
        return {"ready":True,"progress":1.0,"state":"flat"}
    progress=(px-pc)/distance
    return {
        "ready":True,
        "progress":progress,
        "filled":progress >= 1.0 if distance > 0 else progress >= 1.0,
        "overshot":progress > 1.0,
        "direction":"up" if distance>0 else "down",
    }
