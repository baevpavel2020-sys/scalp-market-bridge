"""Separate Pump Exhaustion / Post-Pump Reversal event profile for crypto Scan+.

This detector is deliberately conservative: it identifies a reversal *candidate*
but never turns a pump into an immediate short. Entry remains gated by structure
break + retest in the main execution engine.
"""
VERSION = "pump_exhaustion_v2"

def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def detect(linear, spot=None):
    if not linear:
        return {"status": "NO_DATA", "signal": "NONE", "reasons": []}

    spot = spot or {}
    flow = linear.get("flow") or {}
    oi = linear.get("open_interest") or {}
    ticker = linear.get("ticker") or {}
    book = linear.get("orderbook") or {}
    spot_flow = spot.get("flow") or {}
    spot_ticker = spot.get("ticker") or {}

    price = _num(ticker.get("lastPrice"))
    f1 = flow.get("1m") or {}
    f5 = flow.get("5m") or {}
    sf5 = spot_flow.get("5m") or {}

    delta5 = _num(f5.get("delta_ratio"))
    delta1 = _num(f1.get("delta_ratio"))
    price5 = _num(f5.get("price_change_pct"))
    spot_price5 = _num(sf5.get("price_change_pct"))
    spot_delta5 = _num(sf5.get("delta_ratio"))

    o5 = (oi.get("windows") or {}).get("5m") or {}
    oi5 = _num(o5.get("change_pct"))
    funding = _num(linear.get("funding_rate"))
    imbalance = _num(book.get("imbalance_50"))

    reasons = []
    score = 0

    # Abnormal pump proxy. Normalized delta is used instead of raw volume so the
    # detector is not biased by instrument size.
    pump = bool(
        price5 is not None
        and delta5 is not None
        and price5 >= 1.5
        and delta5 >= 0.25
    )
    if not pump:
        return {
            "status": "NO_EVENT",
            "signal": "NONE",
            "score": 0,
            "direction": "NONE",
            "reasons": [],
            "price": price,
            "execution": "NONE",
        }

    score += 2
    reasons.append("5m_price_and_aggressive_buy_expansion")

    # Spot/perp leadership is a core manipulation filter. If spot data is absent
    # or stale, do not manufacture a driver verdict.
    spot_confirming = (
        spot_price5 is not None
        and spot_price5 > 0
        and (
            delta5 is None
            or spot_delta5 is None
            or spot_delta5 >= delta5 * 0.55
        )
    )
    perp_led = (
        spot_price5 is not None
        and price5 is not None
        and spot_price5 < price5 * 0.40
    )
    if perp_led:
        score += 2
        reasons.append("perp_led_move_spot_not_confirming")
    elif spot_confirming:
        reasons.append("spot_confirms_move")
    else:
        reasons.append("spot_confirmation_unavailable")

    # Leverage fragility: rising OI and/or crowded positive funding into the pump.
    if oi5 is not None and o5.get("usable") and oi5 >= 1:
        score += 1
        reasons.append("open_interest_rising_into_move")
    if funding is not None and funding >= 0.0005:
        score += 1
        reasons.append("crowded_positive_funding")

    # Offer-side pressure / absorption.
    if imbalance is not None and imbalance < 0:
        score += 1
        reasons.append("orderbook_offer_pressure")

    # Exhaustion: normalized aggressive-buy delta weakens on the latest minute while
    # price is still positive. This is comparable across instruments.
    if (
        price5 > 0
        and delta1 is not None
        and delta5 is not None
        and delta1 < delta5 * 0.65
    ):
        score += 1
        reasons.append("aggression_efficiency_falling")

    if score >= 4:
        return {
            "status": "DETECTED",
            "signal": "SHORT_CANDIDATE",
            "score": score,
            "direction": "SHORT",
            "reasons": reasons,
            "price": price,
            "execution": "WAIT_STRUCTURE_BREAK_RETEST",
        }

    return {
        "status": "WATCH",
        "signal": "PUMP_NO_EXHAUSTION_CONFIRMATION",
        "score": score,
        "direction": "WATCH",
        "reasons": reasons,
        "price": price,
        "execution": "NO_SHORT_UNTIL_EXHAUSTION",
    }
