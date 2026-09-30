"""Single-pass extraction of observable Crypto evidence for Scan+ event engines."""
from typing import Any, Mapping


def num(value):
    try: return float(value)
    except (TypeError, ValueError): return None


def tf(scan, name):
    return ((scan.get("timeframes") or {}).get(name) or {})


def extract_crypto_observables(scan: Mapping[str, Any]):
    ex=scan.get("execution") or {}
    windows=ex.get("windows") or {}
    usable=[(name,w) for name,w in windows.items()
            if w.get("perp_flow_usable") and num(w.get("perp_price_change_pct")) is not None]
    usable.sort(key=lambda x: {"1m":1,"5m":2,"15m":3,"1h":4}.get(x[0],99))
    name,w=(usable[0] if usable else (None,{}))

    sweeps=[]
    for timeframe in ("1","5","15","60"):
        sweep=(tf(scan,timeframe).get("liquidity") or {}).get("sweep")
        if sweep: sweeps.append((timeframe,sweep))
    sweep_direction=("bearish" if any(v=="buy_side_swept" for _,v in sweeps)
                     else "bullish" if any(v=="sell_side_swept" for _,v in sweeps)
                     else None)

    divergences=ex.get("flow_divergences") or []
    spot_not_confirming_up=any(
        isinstance(d,dict) and d.get("type")=="price_up_perp_led_spot_not_confirming"
        for d in divergences
    )
    return {
        "window_name":name,"window":w,"sweeps":sweeps,"sweep_direction":sweep_direction,
        "price_change_pct":num(w.get("perp_price_change_pct")),
        "perp_delta_ratio":num(w.get("perp_delta_ratio")),
        "spot_delta_ratio":num(w.get("spot_delta_ratio")),
        "oi_change_pct":num(w.get("oi_change_pct")),
        "sample_quality":num(w.get("sample_quality")) or 0.0,
        "funding_rate":num(ex.get("funding_rate")),
        "book_imbalance":num(ex.get("book_imbalance")),
        "spot_not_confirming_up":spot_not_confirming_up,
        "trade_state":scan.get("trade_state"),
        "setup":scan.get("setup") or {},
        "direction":scan.get("direction") or {},
    }
