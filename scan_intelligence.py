"""Scan+ Intelligence Layer.
Market-agnostic services that sit downstream of market-specific data/analysis.
They do not create or rewrite trading facts; they classify, rank, remember and measure them.
"""
import json, math, os, threading, time
from collections import deque

INTELLIGENCE_VERSION="intelligence_v1"
WATCHLIST_TTL=6*3600
OUTCOME_PATH=os.environ.get("SCAN_OUTCOME_PATH","/tmp/scalp-market-bridge/outcomes.jsonl")

def _num(x):
    try:
        v=float(x)
        return v if math.isfinite(v) else None
    except (TypeError,ValueError):
        return None

def classify_regime_from_analysis(timeframes):
    states=[]; dirs=[]
    for a in (timeframes or {}).values():
        levels=a.get("levels") or {}
        if levels.get("volatility_regime"): states.append(str(levels["volatility_regime"]).upper())
        d=(a.get("confluence") or {}).get("direction")
        if d in ("bullish","bearish"): dirs.append(d)
    if "EXPANSION" in states: state="EXPANSION"
    elif "COMPRESSION" in states: state="COMPRESSION"
    elif dirs and len(dirs)>=2 and len(set(dirs))==1: state="TREND"
    elif dirs: state="TRANSITION"
    else: state="UNKNOWN"
    return {"state":state,"confidence":round(min(1.0,0.45+0.1*min(len(dirs),5)),3),"source":"analysis_bundles"}

def classify_regime(rows):
    rows=[r for r in rows or [] if all(_num(r.get(k)) is not None for k in ("open","high","low","close"))]
    if len(rows)<30: return {"state":"UNKNOWN","confidence":0.0,"reason":"insufficient_history"}
    closes=[_num(r["close"]) for r in rows[-30:]]
    ranges=[max(0.0,_num(r["high"])-_num(r["low"])) for r in rows[-30:]]
    atr=sum(ranges)/len(ranges)
    mean=sum(closes)/len(closes)
    if not atr or not mean: return {"state":"UNKNOWN","confidence":0.0,"reason":"invalid_volatility"}
    slope=(closes[-1]-closes[0])/max(1,len(closes)-1)
    slope_atr=abs(slope*10)/atr
    recent=sum(ranges[-5:])/5
    baseline=sum(ranges[:-5])/max(1,len(ranges)-5)
    expansion=recent/max(baseline,1e-12)
    if expansion>=1.6: state="EXPANSION"
    elif expansion<=0.65: state="COMPRESSION"
    elif slope_atr>=1.2: state="TREND"
    else: state="RANGE"
    return {"state":state,"confidence":round(min(1.0,0.45+min(0.5,abs(slope_atr)/3)+min(0.25,abs(expansion-1)*0.5)),3),
            "slope_atr":round(slope_atr,3),"range_expansion":round(expansion,3),"atr":round(atr,10),"atr_pct":round(atr/mean*100,4)}

def mtf_state_matrix(timeframes):
    out={}
    for tf,a in (timeframes or {}).items():
        st=a.get("structure") or {}
        conf=a.get("confluence") or {}
        out[tf]={
            "ready":bool(a.get("ready")),
            "structure":st.get("state"),
            "direction":conf.get("direction"),
            "phase":st.get("phase"),
            "event":(st.get("event") or {}).get("type") if isinstance(st.get("event"),dict) else st.get("event"),
            "volatility":(a.get("levels") or {}).get("volatility_regime"),
            "signal_freshness":a.get("signal_freshness"),
        }
    return out

def performance_snapshot(frames, lookbacks=("5m","15m","1h","4h")):
    out={}
    for tf in lookbacks:
        rows=frames.get(tf) or frames.get({"5m":"5","15m":"15","1h":"60","4h":"240"}.get(tf,"")) or []
        if len(rows)>=2:
            p0=_num(rows[-min(len(rows),6)]["close"]); p1=_num(rows[-1]["close"])
            if p0: out[tf]={"change_pct":round((p1/p0-1)*100,4),"price":p1}
    return out

def relative_strength(results, market_key=None, lookback="15m"):
    vals=[]
    for r in results or []:
        perf=r.get("performance") or {}
        row=perf.get(lookback) or perf.get("15m") or {}
        ch=_num(row.get("change_pct"))
        if ch is not None: vals.append((r.get("symbol"),ch))
    vals.sort(key=lambda x:x[1],reverse=True)
    if not vals: return []
    lo=min(v for _,v in vals); hi=max(v for _,v in vals); span=max(hi-lo,1e-9)
    return [{"symbol":s,"change_pct":round(v,4),"relative_strength":round((v-lo)/span,3),"rank":i+1,"universe":market_key} for i,(s,v) in enumerate(vals)]

def alert_payload(scan):
    setup=scan.get("setup") or {}
    state=setup.get("opportunity_state")
    if state not in ("MARKET_READY","LIMIT_READY"): return {"eligible":False,"state":state or "WATCH"}
    lp=setup.get("limit_plan") or {}
    return {"eligible":True,"state":state,"symbol":scan.get("symbol"),"direction":setup.get("side") or scan.get("direction"),
            "entry":setup.get("entry") or lp.get("entry"),"stop":setup.get("stop") or lp.get("stop"),
            "target":(setup.get("targets") or [None])[0] if setup.get("targets") else lp.get("take_profit"),
            "rr":setup.get("risk_reward") or lp.get("rr"),"reason":"setup_state_changed"}

class WatchlistStore:
    def __init__(self,ttl=WATCHLIST_TTL):
        self.ttl=ttl; self.lock=threading.RLock(); self.items={}
    def upsert(self,scan):
        sym=str(scan.get("symbol") or "").upper()
        if not sym:return None
        setup=scan.get("setup") or {}
        state=setup.get("opportunity_state") or "WATCH"
        with self.lock:
            prev=self.items.get(sym)
            changed=prev is None or prev.get("state")!=state
            self.items[sym]={"symbol":sym,"market":scan.get("market","crypto"),"state":state,"direction":scan.get("direction"),
                             "rr":(setup.get("risk_reward") or (setup.get("limit_plan") or {}).get("rr")),
                             "updated_at":time.time(),"state_changed":changed}
            return dict(self.items[sym])
    def snapshot(self,market=None):
        now=time.time()
        with self.lock:
            for s,v in list(self.items.items()):
                if now-v["updated_at"]>self.ttl:self.items.pop(s,None)
            vals=list(self.items.values())
        if market: vals=[v for v in vals if v.get("market")==market]
        return sorted(vals,key=lambda x:(x["state"]!="MARKET_READY",x["state"]!="LIMIT_READY",-float(x.get("rr") or 0)))

class OutcomeLogger:
    def __init__(self,path=OUTCOME_PATH):
        self.path=path; self.lock=threading.Lock()
    def record(self,scan):
        setup=scan.get("setup") or {}
        state=setup.get("opportunity_state")
        if state not in ("MARKET_READY","LIMIT_READY"): return False
        event={"ts":time.time(),"symbol":scan.get("symbol"),"market":scan.get("market","crypto"),"state":state,
               "direction":setup.get("side") or scan.get("direction"),"entry":setup.get("entry") or (setup.get("limit_plan") or {}).get("entry"),
               "stop":setup.get("stop") or (setup.get("limit_plan") or {}).get("stop"),"target":(setup.get("limit_plan") or {}).get("take_profit"),
               "rr":setup.get("risk_reward") or (setup.get("limit_plan") or {}).get("rr"),
               "regime":scan.get("regime"),"event_basis":setup.get("event_basis") or [],
               "features":scan.get("mtf_matrix") or {}}
        try:
            os.makedirs(os.path.dirname(self.path),exist_ok=True)
            with self.lock:
                with open(self.path,"a",encoding="utf-8") as f:f.write(json.dumps(event,separators=(",",":"),default=str)+"\n")
            return True
        except OSError:return False

WATCHLIST=WatchlistStore()
OUTCOMES=OutcomeLogger()

def enrich_external_result(result, market):
    frames=result.get("frames") or {}
    analysis=result.get("analysis_core",{}).get("analysis") or {}
    tf={}
    for tf_name,a in analysis.items():
        st=a.get("structure") or {}
        tf[tf_name]={"ready":a.get("ready"),"structure":{"state":st.get("state"),"phase":st.get("phase"),"event":st.get("last_event")},
                     "confluence":a.get("confluence") or {},"levels":a.get("regime_levels") or {}}
    setup_plans=result.get("setup_plans") or {}
    setup=setup_plans.get("15m") or setup_plans.get("1h") or next(iter(setup_plans.values()),{})
    out=dict(result); out["regime"]={tf_name:classify_regime(rows) for tf_name,rows in frames.items()}
    out["mtf_matrix"]=mtf_state_matrix(tf); out["performance"]=performance_snapshot(frames)
    out["setup"]=setup; out["opportunity_state"]=("MARKET_READY" if setup.get("tradeable") and setup.get("execution_ready") else "LIMIT_READY" if setup.get("tradeable") else "WATCH")
    out["alert"]=alert_payload({"symbol":result.get("symbol"),"direction":setup.get("direction"),"setup":{**setup,"opportunity_state":out["opportunity_state"]}})
    out["intelligence_version"]=INTELLIGENCE_VERSION
    WATCHLIST.upsert({"symbol":result.get("symbol"),"market":market,"direction":setup.get("direction"),"setup":{"opportunity_state":out["opportunity_state"],"limit_plan":setup.get("limit_plan")}})
    OUTCOMES.record({"symbol":result.get("symbol"),"market":market,"direction":setup.get("direction"),"setup":{**setup,"opportunity_state":out["opportunity_state"]},"regime":out["regime"],"mtf_matrix":out["mtf_matrix"]})
    return out

def enrich_scan(scan, market="crypto"):
    out=dict(scan or {})
    out.pop("_frames",None)
    frames=(scan.get("_frames") or {})
    if frames:
        out["regime"]={tf:classify_regime(rows) for tf,rows in frames.items() if tf in ("1D","D","4h","240","1h","60","15m","15","5m","5")}
    if not out["regime"]: out["regime"]={"composite":classify_regime_from_analysis(out.get("timeframes") or {})}
    out["mtf_matrix"]=mtf_state_matrix(scan.get("timeframes") or {})
    out["performance"]=performance_snapshot(frames)
    out["intelligence_version"]=INTELLIGENCE_VERSION
    WATCHLIST.upsert(out)
    out["alert"]=alert_payload(out)
    OUTCOMES.record(out)
    return out

def backtest_event_setups(market,symbol,rows,detect_fn,plan_fn,max_checkpoints=100):
    rows=list(rows or [])
    results=[]
    start=max(30,len(rows)-max_checkpoints-1)
    for i in range(start,len(rows)-1):
        prefix=rows[:i+1]
        detected=detect_fn(market,symbol,prefix)
        plan=plan_fn(market,symbol,prefix,detected)
        if not plan.get("tradeable"): continue
        lp=plan.get("limit_plan") or {}
        entry=_num(lp.get("entry")); stop=_num(lp.get("stop")); target=_num(lp.get("take_profit"))
        if None in (entry,stop,target): continue
        outcome="UNRESOLVED"
        end=i+1
        for j in range(i+1,len(rows)):
            hi=_num(rows[j].get("high")); lo=_num(rows[j].get("low"))
            if hi is None or lo is None: continue
            if plan.get("direction")=="bearish":
                if hi>=stop: outcome="SL"; end=j; break
                if lo<=target: outcome="TP"; end=j; break
            else:
                if lo<=stop: outcome="SL"; end=j; break
                if hi>=target: outcome="TP"; end=j; break
        results.append({"checkpoint":rows[i].get("end",rows[i].get("start",i)),"entry":entry,"stop":stop,"target":target,"rr":lp.get("rr"),"outcome":outcome,"bars_to_resolution":end-i})
    total=len(results); tp=sum(x["outcome"]=="TP" for x in results); sl=sum(x["outcome"]=="SL" for x in results)
    return {"version":INTELLIGENCE_VERSION,"market":market,"symbol":symbol,"samples":total,"tp":tp,"sl":sl,
            "unresolved":total-tp-sl,"hit_rate":round(tp/max(1,tp+sl),4),"results":results}

def edge_summary(records):
    rec=[r for r in records or [] if r.get("outcome") in ("TP","SL")]
    if not rec:return {"samples":0}
    return {"samples":len(rec),"tp":sum(r["outcome"]=="TP" for r in rec),"sl":sum(r["outcome"]=="SL" for r in rec),
            "hit_rate":round(sum(r["outcome"]=="TP" for r in rec)/len(rec),4)}
