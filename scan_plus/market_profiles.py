"""Declarative Scan+ market/instrument priorities.

This module contains policy/configuration only. It must not fetch market data and must
not mutate shared analytical evidence.
"""
from copy import deepcopy

MARKET_PROFILES = {
    "crypto": {
        "prescan": ["volume", "open_interest", "volatility", "liquidity", "structure"],
        "scan": ["order_flow", "open_interest", "liquidations", "structure_mtf", "elliott", "fibonacci", "harmonics"],
    },
    "stocks": {
        "prescan": ["underlying_volume", "gaps", "volatility", "levels", "session"],
        "scan": ["structure_mtf", "fibonacci", "underlying_volume", "gaps", "elliott", "harmonics"],
    },
    "forex": {
        "prescan": ["volatility", "active_session", "session_high_low", "structure", "relative_strength"],
        "scan": ["structure_mtf", "elliott", "fibonacci", "session_liquidity", "harmonics", "indicators"],
    },
    "commodities": {
        "prescan": ["volatility", "active_session", "structure", "liquidity", "relative_strength"],
        "scan": ["structure_mtf", "liquidity", "fibonacci", "elliott", "volume", "harmonics"],
    },
}

INSTRUMENT_PROFILES = {
    "NVDA": {
        "market": "stocks", "group": "tech_equity", "product": "bybit_tokenized_stock",
        "prescan": ["underlying_volume", "gaps", "session", "volatility", "levels"],
        "scan": ["gaps", "fibonacci", "structure_mtf", "underlying_volume", "session_liquidity", "elliott", "harmonics"],
    },
    "AAPL": {"market":"stocks","group":"tech_equity","product":"bybit_tokenized_stock"},
    "MSFT": {"market":"stocks","group":"tech_equity","product":"bybit_tokenized_stock"},
    "EURUSD": {
        "market":"forex","group":"major_fx","product":"spot_fx",
        "prescan":["active_session","volatility","session_high_low","structure","relative_strength"],
        "scan":["structure_mtf","elliott","session_liquidity","fibonacci","relative_strength","harmonics"],
    },
    "GBPUSD": {"market":"forex","group":"major_fx","product":"spot_fx"},
    "USDJPY": {"market":"forex","group":"major_fx","product":"spot_fx"},

    "XAUUSD": {
        "market": "commodities", "group": "metals",
        "prescan": ["volatility", "active_session", "session_high_low", "structure", "liquidity"],
        "scan": ["structure_mtf", "session_liquidity", "fibonacci", "elliott", "usd_yields_context", "harmonics"],
    },
    "XAGUSD": {"market": "commodities", "group": "metals"},
    "WTI": {"market": "commodities", "group": "energy"},
    "BRENT": {"market": "commodities", "group": "energy"},
    "COCOA": {"market": "commodities", "group": "softs"},
}


def get_profile(market, symbol=None):
    market = str(market).lower()
    if market not in MARKET_PROFILES:
        raise ValueError(f"unknown market: {market}")
    result = deepcopy(MARKET_PROFILES[market])
    result["market"] = market
    if symbol:
        key = str(symbol).upper()
        override = INSTRUMENT_PROFILES.get(key)
        if override:
            if override.get("market", market) != market:
                raise ValueError(f"{key} does not belong to {market}")
            result.update(deepcopy(override))
        result["symbol"] = key
    return result
