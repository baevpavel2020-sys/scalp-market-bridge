"""Market-specific event engine for Scan+ external markets."""
import math,time

def _dedupe_events(events):
    groups={}
    priority={"failed_breakout":4,"failed_breakdown":4,"gap":3,"range_expansion":2,"volume_expansion":1}
    for e in events or []:
        d=e.get("direction"); level=e.get("level")
        key=(e.get("event"),d,level) if d is None and level is None else (d,level)
        if key not in groups:
            groups[key]=dict(e); continue
        cur=groups[key]
        if priority.get(e.get("event"),0)>priority.get(cur.get("event"),0):
            primary=dict(e); primary["confirmations"]=list(cur.get("confirmations") or [])+[cur.get("event")]; groups[key]=primary
        else:
            cur.setdefault("confirmations",[]).append(e.get("event"))
    return list(groups.values())
from datetime import datetime,timezone
from scan_architecture import MIN_RR, market_block_policy
try:
 from zoneinfo import ZoneInfo
except ImportError:
 ZoneInfo=None

VERSION="multimarket_event_engine_v1"

def _atr(r,n=14):
 if len(r)<2:return None
 s=r[-min(len(r),n+1):]
 tr=[max(c["high"]-c["low"],abs(c["high"]-p["close"]),abs(c["low"]-p["close"])) for p,c in zip(s,s[1:])]
 return sum(tr)/len(tr) if tr else None

def _session(market,ts):
 d=datetime.fromtimestamp(ts/1000,timezone.utc); m=str(market).lower()
 if m=="forex" and ZoneInfo:
  l=d.astimezone(ZoneInfo("Europe/London")); n=d.astimezone(ZoneInfo("America/New_York"))
  lh=l.hour+l.minute/60; nh=n.hour+n.minute/60
  if 8<=lh<13 and 8<=nh<17:return "LONDON_NY_OVERLAP"
  if 8<=lh<17:return "LONDON"
  if 8<=nh<17:return "NEW_YORK"
  if 0<=d.hour<8:return "ASIA"
  return "ROLLOVER"
 if m=="stocks":return "GLOBAL_XSTOCKS_24_7"
 if m=="commodities":return "INSTRUMENT_SESSION"
 return "24_7"

def _forex_session_levels(rows, target):
 d=[]
 for x in rows:
  if _session("forex",int(x.get("start",0)))==target:d.append(x)
 if not d:return None,None
 return max(x["high"] for x in d),min(x["low"] for x in d)

def detect_events(market,symbol,rows):
 r=[x for x in rows or [] if all(math.isfinite(float(x.get(k))) for k in ("open","high","low","close"))]
 if len(r)<30:return {"ready":False,"events":[],"reason":"need_at_least_30_valid_closed_candles"}
 atr=_atr(r)
 if not atr or atr<=0:return {"ready":False,"events":[],"reason":"atr_unavailable"}
 c,p=r[-1],r[-2]; base=r[-21:-1]
 hi=max(x["high"] for x in base); lo=min(x["low"] for x in base)
 ev=[]
 if c["high"]>hi and c["close"]<hi:ev.append({"event":"failed_breakout","direction":"bearish","confidence":"high","level":hi,"excursion_atr":round((c["high"]-hi)/atr,3)})
 if c["low"]<lo and c["close"]>lo:ev.append({"event":"failed_breakdown","direction":"bullish","confidence":"high","level":lo,"excursion_atr":round((lo-c["low"])/atr,3)})
 rg=c["high"]-c["low"]
 if rg>=1.5*atr and c["close"]!=c["open"]:ev.append({"event":"range_expansion","direction":"bullish" if c["close"]>c["open"] else "bearish","confidence":"context","range_atr":round(rg/atr,3)})
 gap=c["open"]-p["close"]
 if abs(gap)>=.35*atr:ev.append({"event":"gap","direction":"bullish" if gap>0 else "bearish","confidence":"high","gap_atr":round(abs(gap)/atr,3),"level":c["open"]})
 vols=[float(x.get("volume") or 0) for x in base]; av=sum(vols)/len(vols) if vols else 0
 if av and c.get("volume",0)>=2*av:ev.append({"event":"volume_expansion","direction":"bullish" if c["close"]>c["open"] else "bearish","confidence":"context","volume_ratio":round(c["volume"]/av,2)})
 sess=_session(market,int(c.get("start",time.time()*1000)))
 if market=="forex":
  ev.append({"event":"session_context","session":sess,"confidence":"context"})
  if sess in ("LONDON","LONDON_NY_OVERLAP","NEW_YORK"):
   prev_session="ASIA" if sess=="LONDON" else "LONDON"
   sh,sl=_forex_session_levels(r[:-1],prev_session)
   if sh is not None and c["high"]>sh and c["close"]<sh:ev.append({"event":"session_failed_high","direction":"bearish","confidence":"high","session":prev_session,"level":sh})
   if sl is not None and c["low"]<sl and c["close"]>sl:ev.append({"event":"session_failed_low","direction":"bullish","confidence":"high","session":prev_session,"level":sl})
 elif market=="commodities":
  ev += [{"event":"instrument_session_context","confidence":"context"},{"event":"inventory_event","confidence":"unavailable","status":"fundamental_calendar_not_connected"},{"event":"contract_rollover","confidence":"unavailable","status":"contract_calendar_not_connected"}]
 elif market=="stocks":ev.append({"event":"xstock_24_7_context","confidence":"context"})
 ev=_dedupe_events(ev)
 return {"ready":True,"engine_version":VERSION,"market":market,"symbol":symbol,"events":ev,"session":sess,"reference":{"atr":atr,"range_high":hi,"range_low":lo}}

def _shared_direction(analysis_core):
    analysis=(analysis_core or {}).get("analysis") or {}
    def direction(tf):
        a=analysis.get(tf) or {}
        state=(a.get("structure") or {}).get("state")
        if state=="uptrend": return "bullish"
        if state=="downtrend": return "bearish"
        return None
    d1=direction("1h"); d15=direction("15m")
    if d1 and d15 and d1==d15:
        return d1, "confirmed"
    if d1 and not d15:
        return d1, "provisional"
    if d15 and not d1:
        return d15, "provisional"
    return None, "uncertain"


def _shared_limit_geometry(market, symbol, analysis_core):
    analysis=(analysis_core or {}).get("analysis") or {}
    frame15=analysis.get("15m") or {}
    price=float(frame15.get("last_confirmed_close") or 0)
    if price<=0:
        return None
    direction,state=_shared_direction(analysis_core)
    if direction not in ("bullish","bearish") or state!="confirmed":
        return None

    def levels(tf, side):
        aliases={"D":"1D","240":"4h","60":"1h","15":"15m","5":"5m","1":"1m"}
        key=aliases.get(tf,tf)
        r=((analysis.get(key) or {}).get("regime_levels") or {})
        return [float(x) for x in (r.get(side) or []) if isinstance(x,(int,float)) and math.isfinite(float(x))]

    # Structural location is the entry source. Events remain confirmation/trigger,
    # never an alternative direction authority.
    if direction=="bullish":
        supports=sorted([x for x in levels("5", "supports")+levels("15", "supports")+levels("60", "supports") if x < price], reverse=True)
        resistances=sorted([x for x in levels("15", "resistances")+levels("60", "resistances")+levels("240", "resistances") if x > price])
        entry=supports[0] if supports else None
        lower=[x for x in supports[1:] if x < entry] if entry is not None else []
        stop_level=lower[0] if lower else None
        target=resistances[0] if resistances else None
    else:
        resistances=sorted([x for x in levels("5", "resistances")+levels("15", "resistances")+levels("60", "resistances") if x > price])
        supports=sorted([x for x in levels("15", "supports")+levels("60", "supports")+levels("240", "supports") if x < price], reverse=True)
        entry=resistances[0] if resistances else None
        upper=[x for x in resistances[1:] if x > entry] if entry is not None else []
        stop_level=upper[0] if upper else None
        target=supports[0] if supports else None

    atr=((analysis.get("15m") or {}).get("technical") or {}).get("atr14")
    atr=float(atr) if atr else None
    if entry is None:
        return None
    buffer=(0.20*atr) if atr else abs(price-entry)*0.15
    if direction=="bullish":
        stop=(stop_level-buffer) if stop_level is not None else entry-buffer
    else:
        stop=(stop_level+buffer) if stop_level is not None else entry+buffer
    risk=abs(entry-stop)
    if risk<=0:
        return None

    if target is None:
        return None
    reward=abs(target-entry)
    rr=reward/risk
    if rr < MIN_RR:
        return None
    side="BUY_LIMIT" if direction=="bullish" else "SELL_LIMIT"
    zone=max(buffer*0.25, abs(entry)*0.0002)
    if direction=="bullish":
        entry_zone=[entry-zone,entry+zone]
    else:
        entry_zone=[entry-zone,entry+zone]
    return {
        "market":market,"symbol":symbol,"direction":direction,
        "execution_ready":False,"execution_mode":"conditional_limit",
        "tradeable":True,"ready":True,
        "trigger":{"type":"structural_retest","level":round(entry,10),
                   "condition":"price_retests_structural_level_and_confirms_rejection"},
        "limit_plan":{
            "eligible":True,"state":"LIMIT_PLAN","side":side,
            "entry":round(entry,10),"entry_zone":[round(min(entry_zone),10),round(max(entry_zone),10)],
            "stop":round(stop,10),"take_profit":round(target,10),
            "rr":round(rr,3),"minimum_rr":MIN_RR,
            "rr_basis":"limit_entry_to_stop_and_take_profit",
            "risk_distance":round(risk,10),
            "entry_basis":{"source":"shared_structure","timeframe":"5m/15m/1h"},
            "place_now":False,
            "requires_before_fill":["direction_confirmation","structural_retest","market_data_ready"],
            "cancel_if":["direction_invalidated","structural_invalidation","target_or_scale_invalid"],
        },
        "event_basis":[],
        "notes":[
            "Shared analytical core supplies direction and structural geometry.",
            "Market-specific event engine only adds contextual confirmation.",
            "No broker order is sent."
        ],
    }


def build_setup_plan(market,symbol,rows,result,analysis_core=None):
    if not result.get("ready"):
        return {"ready":False,"tradeable":False,"reason":"event_engine_not_ready"}

    # Prefer the shared analytical core. This makes xStocks/Forex/Commodities use
    # the same structural discipline as crypto without importing crypto-only flow.
    shared=_shared_limit_geometry(market,symbol,analysis_core)
    if shared:
        directional=[e for e in result.get("events",[]) if e.get("direction")==shared["direction"]]
        priority=market_block_policy(market).get("event_priority") or []
        directional.sort(key=lambda e: priority.index(e.get("event")) if e.get("event") in priority else len(priority))
        shared["event_basis"]=directional[:4]
        return shared

    # Conservative fallback when the shared MTF core is not ready.
    atr=float(result["reference"]["atr"]); c=rows[-1]
    ds=[e for e in result["events"] if e.get("confidence")=="high" and e.get("direction") in ("bullish","bearish")]
    priority=market_block_policy(market).get("event_priority") or []
    ds.sort(key=lambda e: priority.index(e.get("event")) if e.get("event") in priority else len(priority))
    if not ds:
        return {"ready":True,"tradeable":False,"reason":"no_confirmed_direction_or_high_confidence_event"}
    e=next((x for x in ds if x["event"] in ("failed_breakout","failed_breakdown")),ds[0])
    direction=e["direction"]; entry=float(e.get("level",c["close"]))
    if direction=="bearish":
        stop=max(c["high"],entry+.15*atr); risk=stop-entry; target=entry-MIN_RR*risk
    else:
        stop=min(c["low"],entry-.15*atr); risk=entry-stop; target=entry+MIN_RR*risk
    if risk<=0:return {"ready":True,"tradeable":False,"reason":"invalid_geometry"}
    return {
        "ready":True,"tradeable":True,"execution_ready":False,
        "execution_mode":"conditional_limit_or_trigger","market":market,"symbol":symbol,
        "direction":direction,
        "trigger":{"type":"retest","level":round(entry,10),"condition":"price_retests_level_and_confirms_rejection"},
        "limit_plan":{"eligible":True,"entry":round(entry,10),"stop":round(stop,10),"take_profit":round(target,10),
                      "rr":MIN_RR,"minimum_rr":MIN_RR,"rr_basis":"limit_entry_to_stop_and_take_profit",
                      "risk_distance":round(risk,10)},
        "event_basis":[e],
        "notes":["Closed-candle structural level only.","No broker order is sent.","OI/funding/liquidation/CVD/orderbook are never inferred."]
    }

