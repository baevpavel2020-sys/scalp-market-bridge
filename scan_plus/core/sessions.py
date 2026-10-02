"""Timezone-aware session context utilities.

Session boundaries are expressed in each market's local timezone, then evaluated
from an aware UTC timestamp. DST is therefore handled by the standard library.
"""
from datetime import datetime, timezone, time
from zoneinfo import ZoneInfo

SCHEDULES={
    "forex":{
        "asia":("Asia/Tokyo",time(9),time(18)),
        "london":("Europe/London",time(8),time(17)),
        "new_york":("America/New_York",time(8),time(17)),
    },
    "stocks_us":{
        "premarket":("America/New_York",time(4),time(9,30)),
        "regular":("America/New_York",time(9,30),time(16)),
        "after_hours":("America/New_York",time(16),time(20)),
    },
}

def _aware_utc(value=None):
    if value is None: return datetime.now(timezone.utc)
    if isinstance(value,datetime):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
    numeric=float(value)
    if abs(numeric)>1e11: numeric/=1000.0
    return datetime.fromtimestamp(numeric,tz=timezone.utc)

def active_sessions(market,timestamp=None):
    dt=_aware_utc(timestamp)
    # FX and US equity sessions are closed on weekends.
    if dt.weekday() >= 5 and market in {"forex","stocks_us"}:
        return []
    result=[]
    for name,(tz_name,start,end) in SCHEDULES.get(market,{}).items():
        local=dt.astimezone(ZoneInfo(tz_name)).time()
        if start <= local < end: result.append(name)
    return result

def session_overlap(market,timestamp=None):
    return len(active_sessions(market,timestamp))>=2
