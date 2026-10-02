"""Deterministic market/instrument resolution for Scan+ commands."""
import re

EXPLICIT_SYMBOL_MARKETS = {
    "EURUSD":"forex","GBPUSD":"forex","USDJPY":"forex","AUDUSD":"forex","USDCAD":"forex",
    "USDCHF":"forex","NZDUSD":"forex","EURGBP":"forex","EURJPY":"forex",
    "XAUUSD":"commodities","XAGUSD":"commodities","WTI":"commodities","BRENT":"commodities","COCOA":"commodities",
    "NVDA":"stocks","AAPL":"stocks","MSFT":"stocks","AMZN":"stocks","META":"stocks","TSLA":"stocks","GOOGL":"stocks",
    "NVDAXUSDT":"stocks","AAPLXUSDT":"stocks","MSFTXUSDT":"stocks","TSLAXUSDT":"stocks","METAXUSDT":"stocks",
    "GOOGLXUSDT":"stocks","AMZNXUSDT":"stocks","COINXUSDT":"stocks","HOODXUSDT":"stocks","CRCLXUSDT":"stocks","MSTRXUSDT":"stocks",
}

def normalize_symbol(symbol):
    return re.sub(r"[^A-Za-z0-9._-]","",str(symbol or "")).upper()

def resolve_market(symbol):
    key=normalize_symbol(symbol)
    if key in EXPLICIT_SYMBOL_MARKETS:
        return EXPLICIT_SYMBOL_MARKETS[key]
    if key.endswith("USDT") or key.endswith("USDC") or key.endswith("USD"):
        # FX exceptions are explicit above; generic crypto pairs remain crypto.
        return "crypto"
    return None
