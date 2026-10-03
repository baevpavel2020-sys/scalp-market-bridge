"""Scan+ V4 Blocks 5-14 contracts and zero-audit invariants."""
import hashlib,json,time,math
VERSION="scanplus_v4_blocks5_14"

def _dir(item):
 s=(item or {}).get("structure") or {}; st=s.get("state")
 return "bullish" if st=="uptrend" else "bearish" if st=="downtrend" else None

def dual_scenarios(analysis,events=None):
 """Always build LONG and SHORT hypotheses before selecting primary."""
 analysis=analysis or {}; events=events or []
 d1=_dir(analysis.get("1h")); d15=_dir(analysis.get("15m")); d4=_dir(analysis.get("4h"))
 def hypothesis(direction):
  aligned=sum(x==direction for x in (d4,d1,d15) if x)
  opposed=sum(x not in (None,direction) for x in (d4,d1,d15))
  support=[e for e in events if e.get("direction")==direction and e.get("stage") in ("CONFIRMED","ACTIVE")]
  invalid=[]
  if d1 and d1!=direction: invalid.append("1h_structure_opposes")
  if d4 and d4!=direction: invalid.append("4h_structure_opposes")
  state="INVALID" if invalid else "READY" if aligned>=2 else "DEVELOPING" if aligned or support else "EARLY"
  return {"direction":direction,"state":state,"structural_alignment":aligned,"opposition":opposed,
          "event_support":support,"invalidations":invalid}
 long= hypothesis("bullish"); short=hypothesis("bearish")
 viable=[x for x in (long,short) if x["state"]!="INVALID"]
 if len(viable)==1: primary=viable[0]
 elif len(viable)==2 and viable[0]["structural_alignment"]!=viable[1]["structural_alignment"]:
  primary=max(viable,key=lambda x:x["structural_alignment"])
 else: primary=None
 return {"long":long,"short":short,"primary":primary,"alternative":short if primary is long else long if primary is short else None,
         "neutral":primary is None,"rule":"both_sides_before_direction"}

def opportunity_funnel(setup):
 s=setup or {}; hard=s.get("hard_invalidations") or []; trigger=bool(s.get("trigger_confirmed"))
 direction=s.get("direction") or s.get("side"); lp=s.get("limit_plan") or {}
 entry=lp.get("entry",s.get("entry")); stop=lp.get("stop",s.get("stop")); target=lp.get("take_profit",lp.get("target"))
 geometry=False
 try:
  e,st,t=map(float,(entry,stop,target))
  geometry=(st<e<t) if direction=="bullish" else (t<e<st) if direction=="bearish" else False
 except (TypeError,ValueError): pass
 if hard:return {"stage":"EARLY","trade_authorized":False,"missing":["hard_invalidation"]}
 if direction and trigger and geometry:return {"stage":"TRADE","trade_authorized":True,"missing":[]}
 if direction and geometry:return {"stage":"READY","trade_authorized":False,"missing":["trigger"]}
 if direction:return {"stage":"DEVELOPING","trade_authorized":False,"missing":["geometry","trigger"]}
 return {"stage":"EARLY","trade_authorized":False,"missing":["direction","geometry","trigger"]}

def execution_contract(market,analysis_ready,market_data_ready=False,broker_ready=False,order_submission=False):
 if not analysis_ready: status="ANALYSIS_NOT_READY"
 elif not market_data_ready: status="TRADE_READY_ANALYSIS"
 elif not broker_ready: status="EXECUTION_UNAVAILABLE"
 else: status="EXECUTABLE"
 return {"market":market,"analysis_execution_ready":bool(analysis_ready),"market_execution_data_ready":bool(market_data_ready),
         "broker_execution_ready":bool(broker_ready),"order_submission_supported":bool(order_submission),
         "status":status,"order_sent":False}

def net_rr(entry,stop,target,cost_abs=0):
 try:e,s,t,c=map(float,(entry,stop,target,cost_abs))
 except (TypeError,ValueError):return None
 risk=abs(e-s)+abs(c); reward=max(0,abs(t-e)-abs(c))
 return reward/risk if risk else None

def risk_contract(equity,risk_pct,entry,stop,leverage=5,strategy="core"):
 try:eq=float(equity); rp=float(risk_pct); e=float(entry); s=float(stop); lev=float(leverage)
 except (TypeError,ValueError):return {"ready":False,"reason":"invalid_input"}
 cap=.01 if strategy=="pump_exhaustion_x25" else .05
 rp=min(max(rp,0),cap); money=eq*rp; dist=abs(e-s)
 if not dist:return {"ready":False,"reason":"zero_stop_distance"}
 qty=money/dist; notional=qty*e; margin=notional/max(lev,1)
 return {"ready":True,"money_risk":money,"risk_pct":rp,"position_qty":qty,"notional":notional,"margin":margin,
         "leverage":lev,"rule":"leverage_never_increases_money_risk"}

def setup_fingerprint(market,symbol,direction,entry,stop,target):
 p={"market":market,"symbol":symbol,"direction":direction,"entry":entry,"stop":stop,"target":target}
 return hashlib.sha256(json.dumps(p,sort_keys=True,default=str,separators=(",",":")).encode()).hexdigest()[:20]

def shadow_record(scan):
 s=(scan or {}).get("setup") or {}; lp=s.get("limit_plan") or {}
 return {"ts":time.time(),"market":scan.get("market","crypto"),"symbol":scan.get("symbol"),
         "stage":(scan.get("opportunity_funnel") or {}).get("stage"),"direction":s.get("direction") or s.get("side"),
         "entry":lp.get("entry") or s.get("entry"),"stop":lp.get("stop") or s.get("stop"),
         "target":lp.get("take_profit"),"expected_fill":None,"actual_fill":None,"mae":None,"mfe":None,
         "fees":None,"funding":None,"outcome":None}

def explainability(scan):
 s=(scan or {}).get("setup") or {}; sc=scan.get("scenario") or {}
 return {"observed":{"context":scan.get("market_context"),"events":scan.get("manipulation")},
         "long_hypothesis":sc.get("long"),"short_hypothesis":sc.get("short"),
         "chosen":sc.get("primary"),"waiting_for":(scan.get("opportunity_funnel") or {}).get("missing",[]),
         "decision":"TRADE" if (scan.get("opportunity_funnel") or {}).get("trade_authorized") else "NO_TRADE",
         "score_role":"ranking_only","unknown_semantics":"UNKNOWN_is_neither_PASS_nor_FAIL"}

def zero_audit_contract():
 return {"version":VERSION,"pipeline":["DATA","CONTEXT","ANALYSIS","EVENTS","SCENARIOS","OPPORTUNITY","TRIGGER","ENTRY_TARGET","EXECUTION","RISK","DECISION","WATCH","OUTCOME","LEARNING"],
         "vertical":["DATA_QUALITY","HARD_INVALIDATIONS","AUDIT_TRAIL","VERSIONING","TESTS_MONITORING"],
         "invariants":["both_sides_before_direction","hard_invalidation_absolute","event_has_no_trade_authority",
                       "target_independent_from_entry","net_rr_before_trade","broker_missing_does_not_kill_analysis",
                       "leverage_does_not_increase_money_risk","unknown_not_pass","no_duplicate_scan","no_real_order_by_default"]}
