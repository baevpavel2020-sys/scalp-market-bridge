"""Session high/low extraction from timestamped candles."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from scan_plus.core.sessions import SCHEDULES

def session_high_low(candles, market="forex", session="london"):
    schedule=(SCHEDULES.get(market) or {}).get(session)
    if not schedule: return {"ready":False,"reason":"unknown_session"}
    tz_name,start,end=schedule
    selected=[]
    for candle in candles or []:
        ts=candle.get("timestamp_ms",candle.get("timestamp"))
        try:
            dt=datetime.fromtimestamp(float(ts)/(1000 if float(ts)>1e11 else 1),tz=timezone.utc).astimezone(ZoneInfo(tz_name))
        except (TypeError,ValueError,OverflowError):
            continue
        if start <= dt.time() < end:
            try: selected.append(candle)
            except Exception: pass
    if not selected: return {"ready":False,"reason":"no_candles_in_session"}
    return {"ready":True,"session":session,"high":max(float(x["high"]) for x in selected),
            "low":min(float(x["low"]) for x in selected),"count":len(selected)}
