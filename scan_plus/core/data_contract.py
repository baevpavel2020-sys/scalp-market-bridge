"""Market-local candle validation and timestamp normalization.

This boundary prevents provider payloads from silently crossing analytical scales:
timestamps are normalized to epoch milliseconds, candles are ordered/deduplicated,
and malformed OHLC rows are rejected before entering the shared analytical core.
"""
from __future__ import annotations
from typing import Any, Mapping


def normalize_candles(rows, *, symbol: str, interval: str):
    cleaned=[]
    seen=set()
    rejected=0
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            rejected += 1
            continue
        item=dict(raw)
        try:
            ts=item.get("timestamp_ms", item.get("timestamp"))
            ts=float(ts)
            if ts < 1e11:
                ts *= 1000
            ts=int(ts)
            o=float(item["open"]); h=float(item["high"])
            l=float(item["low"]); c=float(item["close"])
            if not all(v == v and abs(v) != float("inf") for v in (o,h,l,c)):
                raise ValueError
            if h < max(o,c) or l > min(o,c) or l > h:
                raise ValueError
        except (KeyError,TypeError,ValueError,OverflowError):
            rejected += 1
            continue
        if ts in seen:
            rejected += 1
            continue
        seen.add(ts)
        item["timestamp_ms"]=ts
        item["open"]=o; item["high"]=h; item["low"]=l; item["close"]=c
        if "volume" in item and item["volume"] is not None:
            try: item["volume"]=float(item["volume"])
            except (TypeError,ValueError): item["volume"]=0.0
        item["_market_contract"]={"symbol":str(symbol).upper(),"interval":str(interval)}
        cleaned.append(item)

    cleaned.sort(key=lambda x:x["timestamp_ms"])
    return cleaned, {
        "ready": bool(cleaned),
        "symbol": str(symbol).upper(),
        "interval": str(interval),
        "bars": len(cleaned),
        "rejected_bars": rejected,
        "deduplicated": rejected > 0,
        "first_timestamp_ms": cleaned[0]["timestamp_ms"] if cleaned else None,
        "last_timestamp_ms": cleaned[-1]["timestamp_ms"] if cleaned else None,
    }
