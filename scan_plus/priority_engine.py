"""Situation-aware analytical priorities.

Weights are evidence emphasis, not hard filters. A block with lower weight remains
available and can still invalidate a setup.
"""
from copy import deepcopy
from scan_plus.market_profiles import get_profile

SITUATION_OVERRIDES={
    "reversal":{"boost":["structure_mtf","liquidity","elliott","fibonacci"],"deemphasize":["continuation"]},
    "continuation":{"boost":["structure_mtf","volume","order_flow","session_liquidity"],"deemphasize":["harmonics"]},
    "session_sweep":{"boost":["structure_mtf","elliott","fibonacci","session_liquidity"],"deemphasize":[]},
    "gap":{"boost":["gaps","underlying_volume","fibonacci","structure_mtf"],"deemphasize":[]},
    "manipulation":{"boost":["order_flow","open_interest","liquidations","structure_mtf","liquidity"],"deemphasize":[]},
}

def resolve_priorities(market,symbol=None,situation=None):
    profile=get_profile(market,symbol)
    base=list(profile.get("scan") or [])
    override=SITUATION_OVERRIDES.get(str(situation or "").lower(),{})
    boost=override.get("boost",[])
    ordered=[]
    for key in boost+base:
        if key not in ordered: ordered.append(key)
    weights={key:1.0 for key in ordered}
    # Earlier entries carry more decision weight, but no block is disabled.
    for index,key in enumerate(ordered):
        weights[key]=round(1.0 + max(0, len(ordered)-index-1)*0.10,3)
    for key in override.get("deemphasize",[]):
        if key in weights: weights[key]=min(weights[key],0.5)
    return {
        "market":profile["market"],"symbol":profile.get("symbol"),
        "situation":situation,"ordered":ordered,"weights":weights,
        "profile":deepcopy(profile),
    }
