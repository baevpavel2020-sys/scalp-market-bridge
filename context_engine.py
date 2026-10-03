"""Scan+ V4 Block 2: canonical market-context contract."""
import math, time
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo=None

VERSION="context_engine_v4_block2"

PROFILES={
 "crypto":{"session_model":"24_7","benchmark":"BTCUSDT","flow_model":"spot_perp","gap_sensitive":False},
 "stocks":{"session_model":"xstocks_24_7","benchmark":"SPY","flow_model":"spot","gap_sensitive":True},
 "forex":{"session_model":"asia_london_newyork","benchmark":"DXY","flow_model":"price_session","gap_sensitive":False},
 "commodities":{"session_model":"instrument_session","benchmark":"DXY","flow_model":"price_session","gap_sensitive":True},
}

def _num(v):
    try:
        x=float(v); return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError): return None

def _closed(rows):
    return [r for r in (rows or []) if r.get("confirm",True) is not False and all(_num(r.get(k)) is not None for k in ("open","high","low","close"))]

def _regime(rows):
    r=_closed(rows)
    if len(r)<30:return {"state":"UNKNOWN","confidence":0.0,"reason":"insufficient_history"}
    r=r[-30:]; closes=[float(x["close"]) for x in r]; ranges=[float(x["high"])-float(x["low"]) for x in r]
    atr=sum(ranges)/len(ranges); mean=sum(closes)/len(closes)
    if atr<=0 or mean<=0:return {"state":"UNKNOWN","confidence":0.0,"reason":"invalid_volatility"}
    slope=((closes[-1]-closes[0])/29.0)*10.0/atr
    recent=sum(ranges[-5:])/5.0; base=sum(ranges[:-5])/25.0; expansion=recent/max(base,1e-12)
    if expansion>=1.6: state="EXPANSION"
    elif expansion<=0.65: state="COMPRESSION"
    elif abs(slope)>=1.2: state="TREND"
    else: state="RANGE"
    direction="bullish" if slope>0.35 else "bearish" if slope<-0.35 else "neutral"
    return {"state":state,"direction":direction,"confidence":round(min(1.0,.45+min(.35,abs(slope)/4)+min(.2,abs(expansion-1)*.35)),3),
            "slope_atr":round(slope,3),"range_expansion":round(expansion,3),"atr_pct":round(atr/mean*100,4)}

def regime_context(frames, events=None):
    aliases={"1D":("1D","D"),"4h":("4h","240"),"1h":("1h","60"),"15m":("15m","15"),"5m":("5m","5")}
    by_tf={}
    for tf,keys in aliases.items():
        rows=[]
        for k in keys:
            if (frames or {}).get(k): rows=frames[k]; break
        if rows: by_tf[tf]=_regime(rows)
    senior=[by_tf[x] for x in ("1D","4h","1h") if x in by_tf and by_tf[x]["state"]!="UNKNOWN"]
    states=[x["state"] for x in senior]; dirs=[x["direction"] for x in senior if x["direction"]!="neutral"]
    event_modes=[]
    if isinstance(events,dict):
        for bundle in events.values():
            if not isinstance(bundle,dict): continue
            for e in bundle.get("events",[]) or []:
                name=str(e.get("event") or e.get("type") or "").lower()
                if "price_discovery_up" in name or "breakout" in name: event_modes.append("PRICE_DISCOVERY_UP")
                elif "price_discovery_down" in name or "breakdown" in name: event_modes.append("PRICE_DISCOVERY_DOWN")
                elif "transition" in name or "choch" in name: event_modes.append("TRANSITION")
    if "PRICE_DISCOVERY_UP" in event_modes and "PRICE_DISCOVERY_DOWN" not in event_modes: composite="PRICE_DISCOVERY"
    elif "PRICE_DISCOVERY_DOWN" in event_modes and "PRICE_DISCOVERY_UP" not in event_modes: composite="PRICE_DISCOVERY"
    elif "TRANSITION" in event_modes: composite="TRANSITION"
    elif states.count("EXPANSION")>=2: composite="EXPANSION"
    elif states.count("COMPRESSION")>=2: composite="COMPRESSION"
    elif states.count("TREND")>=2 or (len(dirs)>=2 and len(set(dirs))==1): composite="TREND"
    elif senior: composite="RANGE_TRANSITION"
    else: composite="UNKNOWN"
    direction=dirs[0] if len(dirs)>=2 and len(set(dirs))==1 else "neutral"
    confidence=round(sum(x["confidence"] for x in senior)/len(senior),3) if senior else 0.0
    return {"engine_version":VERSION,"state":composite,"direction":direction,"confidence":confidence,"timeframes":by_tf,"event_modes":event_modes,"authority":"context_only_price_discovery_never_rewrites_confirmed_structure"}

def session_profile(market, now_epoch=None):
    m=str(market).lower(); now=float(now_epoch or time.time()); utc=datetime.fromtimestamp(now,timezone.utc)
    if m=="forex" and ZoneInfo:
        l=utc.astimezone(ZoneInfo("Europe/London")); n=utc.astimezone(ZoneInfo("America/New_York"))
        lh=l.hour+l.minute/60; nh=n.hour+n.minute/60
        if 8<=lh<13 and 8<=nh<17:s="LONDON_NY_OVERLAP"
        elif 8<=lh<17:s="LONDON"
        elif 8<=nh<17:s="NEW_YORK"
        elif 0<=utc.hour<8:s="ASIA"
        else:s="ROLLOVER"
    elif m=="stocks":
        s="XSTOCKS_24_7"
        if ZoneInfo:
            ny=utc.astimezone(ZoneInfo("America/New_York"))
            nh=ny.hour+ny.minute/60
            underlying="US_REGULAR" if (ny.weekday()<5 and 9.5<=nh<16) else "US_PREMARKET" if (ny.weekday()<5 and 4<=nh<9.5) else "US_AFTER_HOURS" if (ny.weekday()<5 and 16<=nh<20) else "US_CLOSED"
        else: underlying="UNKNOWN"
    elif m=="commodities":s="INSTRUMENT_SESSION"
    else:s="24_7"
    return {"session":s,"utc_hour":round(utc.hour+utc.minute/60,2),"model":PROFILES.get(m,{}).get("session_model","unknown"),"underlying_session":underlying if m=="stocks" else None}

def event_risk(market, events=None, external_calendar=None):
    """Price events are observable. Calendar/news risk is UNKNOWN unless explicitly supplied."""
    observed=[]
    for bundle in (events or {}).values() if isinstance(events,dict) else []:
        for e in (bundle or {}).get("events",[]) if isinstance(bundle,dict) else []:
            if e.get("confidence")=="high": observed.append(e.get("event"))
    calendar_state="AVAILABLE" if external_calendar is not None else "UNAVAILABLE"
    upcoming=list(external_calendar or [])
    severity="HIGH" if any(str(x.get("impact","")).upper()=="HIGH" for x in upcoming if isinstance(x,dict)) else ("ELEVATED" if observed else "NORMAL")
    return {"severity":severity,"observed_price_events":sorted(set(x for x in observed if x)),
            "calendar_news":{"state":calendar_state,"events":upcoming,
                             "rule":"never_infer_news_or_macro_event_when_calendar_feed_is_unavailable"}}

def build_context(market,symbol,frames,events=None,relative_strength=None,external_calendar=None):
    m=str(market).lower()
    return {"context_version":VERSION,"market":m,"symbol":symbol,"regime":regime_context(frames,events),
            "instrument_profile":{**PROFILES.get(m,{}),"market":m},
            "session_profile":session_profile(m),
            "event_risk":event_risk(m,events,external_calendar),
            "relative_strength":relative_strength,
            "authority":"context_only_not_trade_direction"}

def prescan_context(symbol,histories):
    frames={"5m":histories.get("5",[]),"15m":histories.get("15",[]),"1h":histories.get("60",[])}
    ctx=build_context("crypto",symbol,frames)
    return {"regime":ctx["regime"],"instrument_profile":ctx["instrument_profile"],"session_profile":ctx["session_profile"],
            "event_risk":ctx["event_risk"],"authority":ctx["authority"]}
