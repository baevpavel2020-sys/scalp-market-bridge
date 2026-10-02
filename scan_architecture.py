"""Shared Scan+ architecture contracts.

Order of authority:
Market -> Data Quality -> Regime/Structure -> Event -> Evidence -> Setup -> Execution.

Modules may enrich downstream state, but may not rewrite upstream facts.
"""
VERSION="scan_architecture_v1"

MARKETS=("crypto","stocks","forex","commodities")
OPPORTUNITY_STATES=("MARKET_READY","LIMIT_READY","WATCH")

def opportunity_state(setup):
    setup=setup or {}
    if setup.get("status")=="SETUP":
        return "MARKET_READY"
    lp=setup.get("limit_plan") or {}
    if lp.get("eligible") is True:
        return "LIMIT_READY"
    return "WATCH"

def data_quality(frames, required=("D","4h","1h","15m","5m"), min_closed=50):
    frames=frames or {}
    counts={tf:len(frames.get(tf) or []) for tf in required}
    missing=[tf for tf in required if counts[tf] < min_closed]
    if not frames:
        state="BLOCKED"
    elif not missing:
        state="READY"
    else:
        state="PARTIAL"
    return {
        "state":state,
        "required_timeframes":list(required),
        "closed_candle_counts":counts,
        "missing_or_short":missing,
        "analysis_allowed":state in ("READY","PARTIAL"),
        "decision_confidence_cap":0.0 if state=="BLOCKED" else 0.65 if state=="PARTIAL" else 1.0,
    }

def dedupe_events(events):
    """Collapse multiple names describing one causal event into one event record."""
    priority={"pump_exhaustion":5,"failed_breakout":4,"failed_breakdown":4,
              "liquidity_sweep":3,"range_expansion":2,"gap":2,"volume_expansion":1}
    groups={}
    for event in events or []:
        direction=event.get("direction")
        level=event.get("level")
        key=(event.get("event"),direction,level) if direction is None and level is None else (direction,level)
        if key not in groups:
            groups[key]=dict(event)
        else:
            cur=groups[key]
            if priority.get(event.get("event"),0)>priority.get(cur.get("event"),0):
                primary=dict(event)
                primary["confirmations"]=list(cur.get("confirmations") or [])+[cur.get("event")]
                groups[key]=primary
            else:
                cur.setdefault("confirmations",[]).append(event.get("event"))
    return list(groups.values())

def pipeline_contract():
    return {
        "version":VERSION,
        "authority":["market","data_quality","regime_structure","event","evidence","setup","execution"],
        "rule":"downstream blocks enrich upstream facts; they never rewrite them",
        "opportunity_states":list(OPPORTUNITY_STATES),
    }
