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
from scan_architecture import MIN_RR
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

def build_setup_plan(market,symbol,rows,result):
 if not result.get("ready"):return {"ready":False,"tradeable":False,"reason":"event_engine_not_ready"}
 atr=float(result["reference"]["atr"]); c=rows[-1]
 ds=[e for e in result["events"] if e.get("confidence")=="high" and e.get("direction") in ("bullish","bearish")]
 if not ds:return {"ready":True,"tradeable":False,"reason":"no_high_confidence_directional_event"}
 e=next((x for x in ds if x["event"] in ("failed_breakout","failed_breakdown")),ds[0])
 direction=e["direction"]; entry=float(e.get("level",c["close"]))
 if direction=="bearish":
  stop=max(c["high"],entry+.15*atr); risk=stop-entry; target=entry-MIN_RR*risk
 else:
  stop=min(c["low"],entry-.15*atr); risk=entry-stop; target=entry+MIN_RR*risk
 if risk<=0:return {"ready":True,"tradeable":False,"reason":"invalid_geometry"}
 return {"ready":True,"tradeable":True,"execution_ready":False,"execution_mode":"conditional_limit_or_trigger","market":market,"symbol":symbol,"direction":direction,"trigger":{"type":"retest","level":round(entry,10),"condition":"price_retests_level_and_confirms_rejection"},"limit_plan":{"entry":round(entry,10),"stop":round(stop,10),"take_profit":round(target,10),"rr":MIN_RR,"minimum_rr":MIN_RR,"rr_basis":"limit_entry_to_stop_and_take_profit","risk_distance":round(risk,10)},"event_basis":[e],"notes":["Closed-candle structural level only.","No broker order is sent.","OI/funding/liquidation/CVD/orderbook are never inferred."]}
