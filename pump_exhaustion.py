"""Compatibility facade for the V4 Liquidity & Leverage Event Engine.

Legacy consumers may continue importing pump_exhaustion.detect. The actual
implementation is unified in liquidity_leverage_engine.
"""
from liquidity_leverage_engine import (
    VERSION, STAGES, detect as _detect, dedupe_related, advance_lifecycle,
)

def detect(linear, spot=None, analysis=None):
    return _detect(linear, spot, analysis)

__all__=["VERSION","STAGES","detect","dedupe_related","advance_lifecycle"]
