"""Market-session utilities shared by non-crypto adapters.

All times are UTC to keep the data layer deterministic. The presentation layer may
convert them to local exchange time; no market assumption is hidden in analytics.
"""
from datetime import datetime, timezone

SESSIONS={
    "forex":{"asia":(0,8),"london":(7,16),"new_york":(12,21)},
    "stocks_us":{"premarket":(8,14.5),"regular":(14.5,21),"after_hours":(21,24)},
}

def utc_hour_minute(value=None):
    if value is None: dt=datetime.now(timezone.utc)
    elif isinstance(value,datetime):
        dt=value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        dt=dt.astimezone(timezone.utc)
    else:
        dt=datetime.fromtimestamp(float(value),tz=timezone.utc)
    return dt.hour+dt.minute/60.0

def active_sessions(market, timestamp=None):
    h=utc_hour_minute(timestamp)
    result=[]
    for name,(start,end) in SESSIONS.get(market,{}).items():
        if start <= h < end: result.append(name)
    return result

def session_overlap(market,timestamp=None):
    active=active_sessions(market,timestamp)
    return len(active)>=2
