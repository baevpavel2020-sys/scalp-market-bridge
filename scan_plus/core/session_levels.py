"""Session range extraction using explicit timestamp cutoffs."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from scan_plus.core.sessions import SCHEDULES


def _to_utc(ts):
    value=float(ts)
    return datetime.fromtimestamp(value/(1000 if value>1e11 else 1),tz=timezone.utc)


def session_high_low(candles, market="forex", session="london", before_timestamp_ms=None):
    schedule=(SCHEDULES.get(market) or {}).get(session)
    if not schedule:
        return {"ready":False,"reason":"unknown_session"}
    tz_name,start,end=schedule
    tz=ZoneInfo(tz_name)
    prepared=[]
    cutoff=None
    explicit_cutoff=before_timestamp_ms is not None
    if before_timestamp_ms is None:
        timestamps=[]
        for candle in candles or []:
            try:
                timestamps.append(_to_utc(candle.get("timestamp_ms",candle.get("timestamp"))))
            except (TypeError,ValueError,OverflowError,OSError):
                continue
        if timestamps:
            cutoff=max(timestamps)
    else:
        try:
            cutoff=_to_utc(before_timestamp_ms)
        except (TypeError,ValueError,OverflowError,OSError):
            return {"ready":False,"reason":"invalid_cutoff"}

    for candle in candles or []:
        ts=candle.get("timestamp_ms",candle.get("timestamp"))
        try:
            dt=_to_utc(ts)
            local=dt.astimezone(tz)
            if not (start <= local.time() < end):
                continue
            session_end_local=datetime.combine(local.date(),end,tzinfo=tz)
            if explicit_cutoff and cutoff is not None and session_end_local > cutoff:
                continue
            prepared.append((local.date(),candle))
        except (TypeError,ValueError,OverflowError,OSError):
            continue

    if not prepared:
        return {"ready":False,"reason":"no_completed_session_available"}
    latest_date=max(day for day,_ in prepared)
    selected=[c for day,c in prepared if day==latest_date]
    try:
        high=max(float(x["high"]) for x in selected)
        low=min(float(x["low"]) for x in selected)
    except (KeyError,TypeError,ValueError):
        return {"ready":False,"reason":"invalid_candle_values"}
    return {"ready":True,"session":session,"local_date":str(latest_date),
            "high":high,"low":low,"count":len(selected)}
