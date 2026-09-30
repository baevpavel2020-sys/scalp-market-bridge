"""Commodity-specific context extraction.

This module only normalizes context explicitly supplied by the provider. Missing
macro, inventory, session or event data stays UNKNOWN and never becomes a signal.
"""
from typing import Mapping


CONTEXT_BY_GROUP = {
    "metals": ("usd_context","usd_yields_context","session_context"),
    "energy": ("event_context","inventory_context","session_context"),
    "softs": ("event_context","session_context","volatility_context"),
}


def build_commodity_context(*, group, provider_context=None):
    raw=provider_context if isinstance(provider_context,Mapping) else {}
    allowed=CONTEXT_BY_GROUP.get(str(group or "").lower(),())
    context={}
    for key in allowed:
        value=raw.get(key)
        context[key] = {
            "ready": value is not None,
            "value": value,
            "source": "provider" if value is not None else None,
        }
    return {
        "group":group,
        "context":context,
        "available": [k for k,v in context.items() if v["ready"]],
        "unknown": [k for k,v in context.items() if not v["ready"]],
        "directional_inference": False,
    }
