"""Execution-cost, exposure-cluster and trade-stat helpers."""
import math

def execution_cost(market,entry,stop,target,spread=None,slippage_bps=0.0,commission_bps=0.0,funding_bps=0.0):
    try:
        entry=float(entry); stop=float(stop); target=float(target)
    except (TypeError,ValueError):
        return {"ready":False,"reason":"invalid_geometry"}
    gross_risk=abs(entry-stop); gross_reward=abs(target-entry)
    if gross_risk<=0:return {"ready":False,"reason":"zero_risk"}
    spread_abs=abs(float(spread)) if spread is not None else 0.0
    friction=entry*(abs(float(slippage_bps))+abs(float(commission_bps))+abs(float(funding_bps)))/10000.0
    net_reward=max(0.0,gross_reward-spread_abs-friction)
    net_risk=gross_risk+spread_abs+friction
    return {"ready":True,"market":market,"gross_rr":gross_reward/gross_risk,
            "net_rr":net_reward/max(net_risk,1e-12),"friction":friction+spread_abs,
            "spread":spread_abs,"slippage_bps":slippage_bps,"commission_bps":commission_bps,
            "funding_bps":funding_bps}

def mae_mfe(direction,entry,candles):
    try: entry=float(entry)
    except (TypeError,ValueError): return {"mae":None,"mfe":None}
    adverse=0.0; favorable=0.0
    for c in candles or []:
        try: hi=float(c["high"]); lo=float(c["low"])
        except (KeyError,TypeError,ValueError): continue
        if direction=="bullish":
            favorable=max(favorable,hi-entry); adverse=max(adverse,entry-lo)
        else:
            favorable=max(favorable,entry-lo); adverse=max(adverse,hi-entry)
    return {"mae":adverse,"mfe":favorable}

def exposure_cluster(setups,correlations=None,threshold=0.70):
    correlations=correlations or {}
    clusters=[]
    used=set()
    for i,a in enumerate(setups or []):
        if i in used: continue
        cluster=[a]; used.add(i)
        for j,b in enumerate(setups or []):
            if j in used: continue
            if a.get("market")!=b.get("market"): continue
            key=(a.get("symbol"),b.get("symbol"))
            corr=correlations.get(key,correlations.get((key[1],key[0]),0.0))
            same_dir=a.get("direction")==b.get("direction")
            if same_dir and float(corr or 0)>=threshold:
                cluster.append(b); used.add(j)
        clusters.append({"market":a.get("market"),"direction":a.get("direction"),
                         "symbols":[x.get("symbol") for x in cluster],"size":len(cluster)})
    return clusters

def exposure_buckets(setups):
    """Conservative gross-exposure buckets without pretending assets are correlated."""
    groups={}
    for setup in setups or []:
        key=(setup.get("market"),setup.get("direction"))
        bucket=groups.setdefault(key,{"market":key[0],"direction":key[1],"symbols":[],"size":0})
        if setup.get("symbol") not in bucket["symbols"]:
            bucket["symbols"].append(setup.get("symbol"))
            bucket["size"]+=1
    return list(groups.values())

def position_size(equity,risk_pct,entry,stop,leverage=5,strategy="core"):
    """V4: money risk is fixed before leverage. x25 is isolated to pump profile."""
    from v4_core import risk_contract
    if strategy!="pump_exhaustion_x25" and float(leverage)>10:
        return {"ready":False,"reason":"leverage_above_core_limit"}
    if strategy=="pump_exhaustion_x25" and float(leverage)>25:
        return {"ready":False,"reason":"leverage_above_pump_profile_limit"}
    return risk_contract(equity,risk_pct,entry,stop,leverage,strategy)

def portfolio_risk(setups,max_simultaneous_risk_pct=0.10,daily_loss_limit_pct=0.10,consecutive_losses=0):
    total=sum(float(x.get("risk_pct") or 0) for x in setups or [])
    blocked=[]
    if total>max_simultaneous_risk_pct: blocked.append("max_simultaneous_risk")
    if consecutive_losses>=3: blocked.append("consecutive_loss_protection")
    return {"ready":not blocked,"gross_risk_pct":total,"max_simultaneous_risk_pct":max_simultaneous_risk_pct,
            "daily_loss_limit_pct":daily_loss_limit_pct,"blocked_by":blocked,
            "rule":"daily_profit_goal_never_forces_a_trade"}
