"""Multi-source LIMIT candidate engine for Scan+.

The engine is deliberately market-agnostic. Market-specific policy decides which
sources are available; this module only ranks structural candidates and validates
execution geometry. It never chooses a direction.
"""
import math
from scan_architecture import MIN_RR, market_block_policy

_TF_ALIASES={"D":"1D","240":"4h","60":"1h","15":"15m","5":"5m","1":"1m"}

def _finite(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError):
        return None

def _analysis(core):
    return (core or {}).get("analysis") or {}

def _levels(analysis, tf, side):
    key=_TF_ALIASES.get(tf,tf)
    raw=((analysis.get(key) or {}).get("regime_levels") or {}).get(side) or []
    out=[]
    for x in raw:
        v=_finite(x)
        if v is not None: out.append(v)
    return out

def _atr(analysis):
    for tf in ("15m","5m","1h"):
        v=_finite(((analysis.get(tf) or {}).get("technical") or {}).get("atr14"))
        if v and v>0: return v
    return None

def _source_levels(analysis, direction, price, enabled=None):
    """Return candidates from structure plus only policy-enabled level families."""
    enabled=set(enabled or ())
    if direction=="bullish":
        pairs=[("support","5m"),("support","15m"),("support","1h"),("support","4h")]
        side="supports"
        candidates=[]
        for label,tf in pairs:
            for v in _levels(analysis,tf,side):
                if v < price: candidates.append((v,label,tf))
    else:
        pairs=[("resistance","5m"),("resistance","15m"),("resistance","1h"),("resistance","4h")]
        side="resistances"
        candidates=[]
        for label,tf in pairs:
            for v in _levels(analysis,tf,side):
                if v > price: candidates.append((v,label,tf))
    # Optional precomputed analytical levels. These are evidence, never direction.
    for tf,item in analysis.items():
        for container_name in ("fibonacci","harmonics","liquidity","smart_money","levels"):
            if container_name not in enabled and container_name!="levels":
                continue
            container=item.get(container_name) if isinstance(item,dict) else None
            if not isinstance(container,dict): continue
            for key in ("entry","prz","retest","fvg","ob","breaker","mitigation","support","resistance","level"):
                raw=container.get(key)
                values=raw if isinstance(raw,(list,tuple)) else [raw]
                for raw_v in values:
                    v=_finite(raw_v if not isinstance(raw_v,dict) else raw_v.get("price") or raw_v.get("level"))
                    if v is None: continue
                    valid=(v<price) if direction=="bullish" else (v>price)
                    if valid: candidates.append((v,key,tf))
    return candidates

def _dedupe(candidates, tolerance):
    out=[]
    for v,source,tf in sorted(candidates,key=lambda x:x[0],reverse=True):
        if any(abs(v-x[0])<=tolerance for x in out): 
            # preserve the richest source set through a confirmation counter
            for i,x in enumerate(out):
                if abs(v-x[0])<=tolerance:
                    out[i]=(x[0],x[1]+"+"+source,x[2])
                    break
        else: out.append((v,source,tf))
    return out

def _freshness_ok(analysis):
    seen=False
    for tf in ("1h","15m","5m"):
        item=analysis.get(tf) or {}
        fresh=item.get("signal_freshness")
        if isinstance(fresh,dict):
            age=_finite(fresh.get("age_seconds"))
            limit=_finite(fresh.get("stale_limit_seconds"))
            if age is not None: seen=True
            if fresh.get("state") in ("STALE","INVALID"): return False
            if age is not None and limit is not None and age > limit: return False
        elif isinstance(fresh,str) and fresh.upper() in ("STALE","INVALID"):
            return False
    return True

def generate_candidates(market,symbol,analysis_core,direction,max_candidates=3):
    analysis=_analysis(analysis_core)
    frame=analysis.get("15m") or {}
    price=_finite(frame.get("last_confirmed_close"))
    if price is None or direction not in ("bullish","bearish"): return []
    if not _freshness_ok(analysis): return []
    atr=_atr(analysis)
    tolerance=max((atr or price*0.001)*0.12,price*0.0001)
    policy=market_block_policy(market)
    enabled=set(policy.get("enabled") or ())
    raw=_source_levels(analysis,direction,price,enabled=enabled)
    candidates=_dedupe(raw,tolerance)
    # Prefer the closest valid structural retest, then deeper liquidity/cluster zones.
    if direction=="bullish":
        candidates=sorted(candidates,key=lambda x:(price-x[0],-x[0]))
    else:
        candidates=sorted(candidates,key=lambda x:(x[0]-price,-x[0]))
    out=[]
    policy=market_block_policy(market)
    enabled=set(policy.get("enabled") or [])
    for rank,(entry,source,tf) in enumerate(candidates[:max_candidates],1):
        buffer=max((atr or abs(price-entry)*0.15)*0.20,price*0.0002)
        nearby=_levels(analysis,"15m","supports" if direction=="bullish" else "resistances")
        if direction=="bullish":
            below=sorted([x for x in nearby if x<entry],reverse=True)
            stop_level=below[0] if below else entry-buffer
            stop=stop_level-buffer
            targets=sorted([x for x in _levels(analysis,"15m","resistances")+_levels(analysis,"1h","resistances")+_levels(analysis,"4h","resistances") if x>entry])
        else:
            above=sorted([x for x in nearby if x>entry])
            stop_level=above[0] if above else entry+buffer
            stop=stop_level+buffer
            targets=sorted([x for x in _levels(analysis,"15m","supports")+_levels(analysis,"1h","supports")+_levels(analysis,"4h","supports") if x<entry],reverse=True)
        target=targets[0] if targets else None
        risk=abs(entry-stop)
        reward=abs(target-entry) if target is not None else 0
        rr=reward/risk if risk>0 else 0
        if rr<MIN_RR: continue
        confirmations=sum(1 for x in str(source).split("+") if x)
        score=confirmations*2
        if tf in ("15m","1h"): score+=2
        if "liquidity" in enabled and "liquidity" in source: score+=1
        if "smart_money" in enabled and any(x in source for x in ("fvg","ob","breaker","mitigation")): score+=1
        score+=min(3,rr/MIN_RR)
        zone=max(buffer*0.25,price*0.0001)
        out.append({
            "candidate_id":f"{market}:{symbol}:{direction}:{rank}",
            "rank":rank,"score":round(score,3),"source":source,"timeframe":tf,
            "entry":entry,"entry_zone":[entry-zone,entry+zone],
            "stop":stop,"take_profit":target,"rr":rr,
            "risk_distance":risk,"reward_distance":reward,
            "validation":{"minimum_rr":MIN_RR,"rr_ok":rr>=MIN_RR,"structure_source":source},
        })
    return sorted(out,key=lambda x:(-x["score"],-x["rr"]))[:max_candidates]

def choose_candidate(candidates):
    return dict(candidates[0]) if candidates else None
