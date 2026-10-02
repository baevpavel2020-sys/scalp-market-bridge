"""Scan+ V4 Block 4 — unified Liquidity & Leverage Event Engine.

Events describe market mechanics. They never have trade-direction authority.
Pump/squeeze/sweep is context until causal confirmation reaches structure + retest.
"""
import hashlib,json,time,math

VERSION="liquidity_leverage_event_engine_v4"
STAGES=("DETECTED","DEVELOPING","CONFIRMED","ACTIVE","FAILED","EXPIRED")
RELATED={
 "PUMP_EXHAUSTION":{"CROWDED_LONGS","LEVERAGE_FRAGILITY","LIQUIDITY_SWEEP","FAILED_BREAKOUT"},
 "SHORT_SQUEEZE":{"CROWDED_SHORTS","LEVERAGE_FRAGILITY","LIQUIDITY_SWEEP"},
 "LONG_LIQUIDATION_CASCADE":{"CROWDED_LONGS","LEVERAGE_FRAGILITY","LIQUIDITY_SWEEP"},
 "FAILED_BREAKOUT":{"LIQUIDITY_SWEEP"},"FAILED_BREAKDOWN":{"LIQUIDITY_SWEEP"},
}
def _n(x):
 try:
  v=float(x); return v if math.isfinite(v) else None
 except (TypeError,ValueError,OverflowError): return None
def _fp(e):
 p={k:e.get(k) for k in ("type","direction","level","timeframe","anchor_start")}
 return hashlib.sha256(json.dumps(p,sort_keys=True,default=str,separators=(",",":")).encode()).hexdigest()[:20]
def _event(t,d=None,stage="DETECTED",score=0,**kw):
 e={"type":t,"direction":d,"stage":stage,"score":score,"trade_authority":False,**kw}; e["fingerprint"]=_fp(e); return e

def _structure_confirmation(analysis,direction):
 """Reversal confirmation requires a fresh structural break/MSS and failed retest."""
 a=(analysis or {}).get("15") or (analysis or {}).get("15m") or {}
 s=a.get("structure") or {}; smc=a.get("smart_money") or a.get("smc") or {}
 ev=s.get("last_event") or s.get("event") or {}
 mss=smc.get("mss") or {}
 target="bearish" if direction=="bearish" else "bullish"
 break_ok=bool((ev.get("type") in ("CHOCH","BOS") and ev.get("direction")==target and ev.get("confirmed_by_close")) or
               (isinstance(mss,dict) and mss.get("direction")==target and (mss.get("confirmed") is not False)))
 retest=smc.get("failed_retest") or smc.get("retest")
 retest_ok=bool(isinstance(retest,dict) and retest.get("direction")==target and
                (retest.get("failed") is True or retest.get("confirmed") is True))
 return {"structure_break":break_ok,"failed_retest":retest_ok,
         "confirmed":bool(break_ok and retest_ok),"timeframe":"15m"}

def _classify_move(price5,delta5,spot_price5,spot_delta5,oi5,funding,liq_long,liq_short):
 perp_led=price5 is not None and spot_price5 is not None and spot_price5 < price5*.40
 spot_ok=spot_price5 is not None and spot_price5>0 and (spot_delta5 is None or delta5 is None or spot_delta5>=delta5*.55)
 short_squeeze=liq_short is not None and liq_long is not None and liq_short>max(liq_long*2,1)
 crowded=bool((oi5 is not None and oi5>=1) or (funding is not None and funding>=.0005))
 if short_squeeze:return "short_squeeze"
 if perp_led and crowded:return "leveraged_perp_pump"
 if spot_ok:return "organic_or_spot_led"
 if perp_led:return "perp_led_low_confirmation"
 return "unclassified"

def detect(linear,spot=None,analysis=None,now=None):
 now=now or time.time(); linear=linear or {}; spot=spot or {}; flow=linear.get("flow") or {}; sf=spot.get("flow") or {}
 f1=flow.get("1m") or {}; f5=flow.get("5m") or {}; sf5=sf.get("5m") or {}
 p5=_n(f5.get("price_change_pct")); d5=_n(f5.get("delta_ratio")); d1=_n(f1.get("delta_ratio"))
 sp5=_n(sf5.get("price_change_pct")); sd5=_n(sf5.get("delta_ratio"))
 oi5=_n((((linear.get("open_interest") or {}).get("windows") or {}).get("5m") or {}).get("change_pct"))
 funding=_n(linear.get("funding_rate")); book=linear.get("orderbook") or {}; imbalance=_n(book.get("imbalance_50"))
 liq=linear.get("liquidations") or linear.get("liquidation") or {}
 ll=_n(liq.get("long_usd") or liq.get("long")); ls=_n(liq.get("short_usd") or liq.get("short"))
 ticker=linear.get("ticker") or {}; price=_n(ticker.get("lastPrice"))
 events=[]; reasons=[]; diag={}
 if not linear:return {"engine_version":VERSION,"status":"NO_DATA","signal":"NONE","events":[],"reasons":[],"trade_authority":False}

 move_class=_classify_move(p5,d5,sp5,sd5,oi5,funding,ll,ls)
 driver="unknown"
 if p5 is not None and sp5 is not None: driver="perp" if sp5<p5*.40 else "spot_confirmed" if sp5>0 else "mixed"
 diag.update({"move_classification":move_class,"spot_perp_driver":driver,
              "liquidations":{"long_usd":ll,"short_usd":ls},"oi_5m_pct":oi5,"funding":funding})

 # Positioning / leverage events.
 if funding is not None and funding>=.0005: events.append(_event("CROWDED_LONGS","bearish",score=1,funding=funding))
 if funding is not None and funding<=-.0005: events.append(_event("CROWDED_SHORTS","bullish",score=1,funding=funding))
 if oi5 is not None and abs(oi5)>=1: events.append(_event("LEVERAGE_FRAGILITY",None,score=1,oi_change_pct=oi5))
 if ll is not None and ls is not None and ll>max(ls*2,1):
  events.append(_event("LONG_LIQUIDATION_CASCADE","bearish","DEVELOPING",3,ratio=round(ll/max(ls,1),3)))
 if ls is not None and ll is not None and ls>max(ll*2,1):
  events.append(_event("SHORT_SQUEEZE","bullish","DEVELOPING",3,ratio=round(ls/max(ll,1),3)))

 pump=bool(p5 is not None and d5 is not None and p5>=1.5 and d5>=.25)
 exhaustion=bool(pump and d1 is not None and d5 is not None and d1<d5*.65)
 absorption=bool(pump and imbalance is not None and imbalance<0)
 failed_acceptance=bool(pump and exhaustion and absorption)
 efficiency=None if d5 in (None,0) or p5 is None else p5/abs(d5)
 diag.update({"pump":pump,"exhaustion":exhaustion,"absorption":absorption,
              "failed_acceptance":failed_acceptance,"aggression_efficiency":efficiency})

 if pump:
  score=2+int(driver=="perp")*2+int(oi5 is not None and oi5>=1)+int(funding is not None and funding>=.0005)+int(absorption)+int(exhaustion)
  chain={"abnormal_move":True,"exhaustion":exhaustion,"absorption":absorption,"failed_acceptance":failed_acceptance}
  conf=_structure_confirmation(analysis,"bearish"); chain.update(conf)
  if conf["confirmed"] and failed_acceptance: stage="CONFIRMED"
  elif failed_acceptance or conf["structure_break"]: stage="DEVELOPING"
  else: stage="DETECTED"
  events.append(_event("PUMP_EXHAUSTION","bearish",stage,score,classification=move_class,causal_chain=chain,
                       next_confirmation=None if stage=="CONFIRMED" else "failed_acceptance_then_structure_break_and_failed_retest",
                       no_short_until_confirmed=True))
  reasons.append("abnormal_5m_up_move")
  if exhaustion: reasons.append("aggression_efficiency_falling")
  if absorption: reasons.append("offer_absorption")
  if driver=="perp": reasons.append("perp_led_spot_not_confirming")

 # Deduplicate related mechanics so one causal episode cannot vote many times.
 events=dedupe_related(events)
 confirmed=[e for e in events if e.get("stage") in ("CONFIRMED","ACTIVE")]
 pump_ev=next((e for e in events if e["type"]=="PUMP_EXHAUSTION"),None)
 signal="NONE"; execution="NONE"
 if pump_ev:
  if pump_ev["stage"]=="CONFIRMED": signal="REVERSAL_EVENT_CONFIRMED"; execution="ELIGIBLE_FOR_SCENARIO_ENGINE"
  else: signal="SHORT_CANDIDATE"; execution="NO_SHORT_UNTIL_CAUSAL_CONFIRMATION"
 status="CONFIRMED" if confirmed else "DEVELOPING" if events else "NO_EVENT"
 return {"engine_version":VERSION,"status":status,"signal":signal,"direction":"NONE","events":events,
         "diagnostics":diag,"reasons":reasons,"price":price,"execution":execution,
         "trade_authority":False,"x25_profile":{"allowed":False,"reason":"risk_profile_applied_only_after_confirmed_pump_exhaustion_and_block9_risk_engine"}}

def dedupe_related(events):
 ranked={"PUMP_EXHAUSTION":100,"LONG_LIQUIDATION_CASCADE":90,"SHORT_SQUEEZE":90,
         "FAILED_BREAKOUT":80,"FAILED_BREAKDOWN":80,"LIQUIDITY_SWEEP":70,
         "CROWDED_LONGS":40,"CROWDED_SHORTS":40,"LEVERAGE_FRAGILITY":30}
 out=[]; consumed=set()
 ordered=sorted(events or [],key=lambda e:(ranked.get(e.get("type"),10),e.get("score",0)),reverse=True)
 for e in ordered:
  t=e.get("type")
  if t in consumed: continue
  q=dict(e); related=[]
  rel=RELATED.get(t,set())
  for other in ordered:
   ot=other.get("type")
   if other is e or ot in consumed or ot not in rel: continue
   # Directionless fragility may attach to either direction; directional mechanics must agree.
   if other.get("direction") in (None,q.get("direction")):
    related.append({"type":ot,"fingerprint":other.get("fingerprint"),"score":other.get("score")}); consumed.add(ot)
  if related:q["related_evidence"]=related
  out.append(q); consumed.add(t)
 return out

def advance_lifecycle(event,*,failed_acceptance=False,structure_break=False,failed_retest=False,invalidated=False,expired=False):
 e=dict(event or {})
 if invalidated:e["stage"]="FAILED"
 elif expired:e["stage"]="EXPIRED"
 elif failed_acceptance and structure_break and failed_retest:e["stage"]="CONFIRMED"
 elif failed_acceptance or structure_break:e["stage"]="DEVELOPING"
 else:e["stage"]=e.get("stage") or "DETECTED"
 e["trade_authority"]=False
 return e
