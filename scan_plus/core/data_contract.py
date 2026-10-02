"""Market-local candle validation and timestamp normalization.

This boundary prevents provider payloads from silently crossing analytical scales:
timestamps are normalized to epoch milliseconds, candles are ordered/deduplicated,
and malformed OHLC rows are rejected before entering the shared analytical core.
"""
from __future__ import annotations
from typing import Any, Mapping
import time


def interval_ms(interval):
    key=str(interval).lower()
    return {"1":60000,"1m":60000,"5":300000,"5m":300000,"15":900000,"15m":900000,
            "60":3600000,"1h":3600000,"240":14400000,"4h":14400000,"d":86400000,"1d":86400000}.get(key)


def normalize_candles(rows, *, symbol: str, interval: str, as_of_ms=None, closed_only=False):
    cleaned=[]
    seen=set()
    rejected=0
    closed_rejected=0
    now_ms=int(as_of_ms if as_of_ms is not None else time.time()*1000)
    duration=interval_ms(interval)
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
        end_raw=item.get("end")
        try:
            end_ms=int(float(end_raw)) if end_raw is not None else (ts + duration if duration else ts)
            if end_ms < 1e11:
                end_ms *= 1000
        except (TypeError,ValueError):
            end_ms=ts + duration if duration else ts
        is_closed=end_ms <= now_ms
        if closed_only and not is_closed:
            closed_rejected += 1
            continue
        seen.add(ts)
        item["timestamp_ms"]=ts
        item["end_timestamp_ms"]=end_ms
        item["closed"]=is_closed
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
        "closed_rejected_bars": closed_rejected,
        "deduplicated": rejected > 0,
        "closed_only": bool(closed_only),
        "last_closed": bool(cleaned[-1].get("closed")) if cleaned else False,
        "first_timestamp_ms": cleaned[0]["timestamp_ms"] if cleaned else None,
        "last_timestamp_ms": cleaned[-1]["timestamp_ms"] if cleaned else None,
    }
