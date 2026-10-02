"""Scan+ V3.8 analytical-depth contracts.

Evidence is represented as a causal graph rather than a vote. Structure is
fact authority; interpretation layers enrich it; hard invalidations always
outrank confirmations.
"""
import hashlib

VERSION = "analytical_depth_v3_8_1"
DEFAULT_TTLS = {"structure":12,"elliott":12,"fibonacci":12,"harmonics":8,"divergence":6,"liquidity":6,"smc":6,"flow":3}
AUTHORITY = {"structure":100,"market":100,"data_quality":95,"event":80,"liquidity":70,"smc":60,"elliott":55,"fibonacci":50,"harmonics":50,"divergence":40,"flow":30,"momentum":20}

def fingerprint(*parts):
    return hashlib.sha256(repr(parts).encode("utf-8")).hexdigest()[:20]

def evidence_node(source, kind, value=None, *, timeframe=None, fresh=None, age_bars=None, ttl_bars=None, causal_to=None, hard_invalidation=False, invalidation_reason=None, metadata=None):
    source=str(source or "unknown").lower()
    ttl=ttl_bars if ttl_bars is not None else DEFAULT_TTLS.get(source)
    stale=(fresh is False)
    if not stale and age_bars is not None and ttl is not None:
        try: stale=float(age_bars)>float(ttl)
        except (TypeError,ValueError): pass
    return {"id":fingerprint(source,kind,timeframe,value,causal_to),"source":source,"kind":str(kind or "evidence"),
            "value":value,"timeframe":timeframe,"authority":AUTHORITY.get(source,10),"fresh":fresh,"stale":stale,
            "age_bars":age_bars,"ttl_bars":ttl,"causal_to":list(causal_to or []),"hard_invalidation":bool(hard_invalidation),
            "invalidation_reason":invalidation_reason,"metadata":dict(metadata or {})}

def build_evidence_graph(evidence=None):
    nodes=[evidence_node(i.get("source") or i.get("layer"),i.get("kind") or i.get("type") or "evidence",i.get("value"),
        timeframe=i.get("timeframe") or i.get("tf"),fresh=i.get("fresh"),age_bars=i.get("age_bars"),ttl_bars=i.get("ttl_bars"),
        causal_to=i.get("causal_to"),hard_invalidation=i.get("hard_invalidation") or i.get("invalidated") is True,
        invalidation_reason=i.get("invalidation_reason") or i.get("reason"),metadata=i.get("metadata"))
        for i in (evidence or []) if isinstance(i,dict)]
    active=[n for n in nodes if not n["stale"]]
    hard=[n for n in active if n["hard_invalidation"]]
    return {"version":VERSION,"nodes":nodes,"active_nodes":active,"hard_invalidations":hard,
            "has_hard_invalidation":bool(hard),"node_count":len(nodes)}

def _direction(value):
    value=str(value or "").lower()
    return {"long":"bullish","short":"bearish","buy":"bullish","sell":"bearish"}.get(value,value)

def resolve_conflicts(graph, authoritative_direction=None):
    """Resolve conflicts by authority, never by counting confirmations."""
    candidates=[]
    for n in graph.get("active_nodes",[]):
        value=n.get("value")
        d=_direction(value.get("direction") if isinstance(value,dict) else value)
        if d in ("bullish","bearish"): candidates.append((n["authority"],n,d))
    candidates.sort(key=lambda x:x[0],reverse=True)
    selected_direction=_direction(authoritative_direction)
    selected=candidates[0][1] if candidates else None
    if selected_direction not in ("bullish","bearish") and selected:
        selected_direction=candidates[0][2]
    conflicts=[{"node_id":n["id"],"source":n["source"],"direction":d,"authority":a}
               for a,n,d in candidates if d!=selected_direction]
    return {"direction":selected_direction,"authority_source":selected.get("source") if selected else None,
            "authority":selected.get("authority") if selected else 0,"conflicts":conflicts,
            "conflict_count":len(conflicts),"hard_invalidations":graph.get("hard_invalidations",[])}

def derive_hard_invalidations(setup,timeframes=None):
    """Consume explicit invalidation contracts without inventing market facts."""
    setup=setup or {}; found=[]
    def add(reason,source="setup",metadata=None):
        found.append({"source":source,"reason":str(reason),"hard":True,"metadata":dict(metadata or {})})
    for item in setup.get("hard_invalidations") or setup.get("invalidations") or []:
        if isinstance(item,str): add(item)
        elif isinstance(item,dict) and item.get("hard",True): add(item.get("reason") or item.get("type") or "explicit_invalidation",item.get("source") or "setup",item.get("metadata"))
    for tf,a in (timeframes or {}).items():
        if not isinstance(a,dict): continue
        s=a.get("structure") or {}
        if s.get("hard_invalidated") is True: add(s.get("invalidation_reason") or "structure_hard_invalidated","structure",{"timeframe":tf})
        for key in ("invalidation","hard_invalidation"):
            item=s.get(key)
            if isinstance(item,dict) and item.get("hard",True): add(item.get("reason") or key,"structure",{"timeframe":tf})
    lp=setup.get("limit_plan") or {}
    for key in ("target","take_profit"):
        target=lp.get(key)
        if isinstance(target,dict) and target.get("swept") is True: add("target_liquidity_already_swept","liquidity",{"target":target})
        if isinstance(target,dict) and target.get("invalidated") is True: add(target.get("invalidation_reason") or "target_invalidated","liquidity")
    return found

def apply_hard_invalidations(setup,invalidations):
    out=dict(setup or {}); items=list(out.get("hard_invalidations") or [])
    keys={(str(x.get("source")),str(x.get("reason"))) for x in items if isinstance(x,dict)}
    for item in invalidations or []:
        key=(str(item.get("source")),str(item.get("reason")))
        if key not in keys: items.append(item); keys.add(key)
    out["hard_invalidations"]=items; out["invalidated"]=bool(items)
    if items:
        out.update({"tradeable":False,"execution_ready":False,"place_now":False,"opportunity_state":"WATCH","invalidation_priority":"HARD",
                    "invalidation_reason":items[0].get("reason")})
    return out

def analytical_depth_snapshot(setup,timeframes=None,evidence=None):
    graph=build_evidence_graph(evidence)
    invalidations=derive_hard_invalidations(setup,timeframes)
    graph["hard_invalidations"].extend(evidence_node(x["source"],"hard_invalidation",x["reason"],hard_invalidation=True,
        invalidation_reason=x["reason"],metadata=x.get("metadata")) for x in invalidations)
    graph["has_hard_invalidation"]=bool(graph["hard_invalidations"])
    return {"version":VERSION,"evidence_graph":graph,
            "conflict_resolution":resolve_conflicts(graph,(setup or {}).get("direction")),
            "hard_invalidations":invalidations,"state":"INVALIDATED" if invalidations else "VALID"}
