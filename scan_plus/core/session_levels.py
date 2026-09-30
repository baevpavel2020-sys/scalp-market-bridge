"""Session high/low extraction for the latest local trading session only."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from scan_plus.core.sessions import SCHEDULES

def _to_utc(ts):
    value=float(ts)
    return datetime.fromtimestamp(value/(1000 if value>1e11 else 1),tz=timezone.utc)

def session_high_low(candles, market="forex", session="london"):
    schedule=(SCHEDULES.get(market) or {}).get(session)
    if not schedule:
        return {"ready":False,"reason":"unknown_session"}
    tz_name,start,end=schedule
    prepared=[]
    for candle in candles or []:
        ts=candle.get("timestamp_ms",candle.get("timestamp"))
        try:
            dt=_to_utc(ts)
            local=dt.astimezone(ZoneInfo(tz_name))
            if start <= local.time() < end:
                prepared.append((local.date(),candle))
        except (TypeError,ValueError,OverflowError,OSError):
            continue
    if not prepared:
        return {"ready":False,"reason":"no_candles_in_session"}
    latest_date=max(day for day,_ in prepared)
    selected=[c for day,c in prepared if day==latest_date]
    try:
        high=max(float(x["high"]) for x in selected)
        low=min(float(x["low"]) for x in selected)
    except (KeyError,TypeError,ValueError):
        return {"ready":False,"reason":"invalid_candle_values"}
    return {"ready":True,"session":session,"local_date":str(latest_date),
            "high":high,"low":low,"count":len(selected)}
