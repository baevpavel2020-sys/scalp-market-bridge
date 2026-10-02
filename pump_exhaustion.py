"""Crypto Pump Exhaustion / Post-Pump Reversal detector.

Uses only evidence actually present in the live linear/spot snapshots.
It is a separate event profile, not a replacement for the MTF technical engine.
"""
VERSION="pump_exhaustion_v1"

def _num(x):
    try:return float(x)
    except (TypeError,ValueError):return None

def detect(linear,spot=None):
    if not linear:return {"status":"NO_DATA","signal":"NONE","reasons":[]}
    flow=linear.get("flow") or {}
    oi=linear.get("open_interest") or {}
    ticker=linear.get("ticker") or {}
    book=linear.get("orderbook") or {}
    price=_num(ticker.get("lastPrice"))
    f1=flow.get("1m") or {}; f5=flow.get("5m") or {}
    o1=(oi.get("windows") or {}).get("1m") or {}; o5=(oi.get("windows") or {}).get("5m") or {}
    funding=_num(linear.get("funding_rate"))
    delta5=_num(f5.get("delta_ratio")); vol5=_num(f5.get("total_volume"))
    pc5=_num(f5.get("price_change_pct"))
    oi5=_num(o5.get("change_pct"))
    imb=_num(book.get("imbalance_50"))
    reasons=[]; score=0; direction="NONE"
    # Abnormal pump proxy: positive price + aggressive buying + meaningful volume.
    pump = bool(pc5 is not None and delta5 is not None and pc5>=1.5 and delta5>=0.25)
    if pump:
        direction="SHORT"; score+=2; reasons.append("5m_price_and_aggressive_buy_expansion")
    if vol5 is not None and f1.get("total_volume") is not None and _num(f1.get("total_volume")) and vol5>=2*_num(f1.get("total_volume")):
        score+=1; reasons.append("5m_volume_expansion")
    # Leverage fragility: OI rising into a pump, when the required coverage is usable.
    if oi5 is not None and o5.get("usable") and oi5>=1:
        score+=1; reasons.append("open_interest_rising_into_move")
    if funding is not None and funding>=0.0005:
        score+=1; reasons.append("crowded_positive_funding")
    if imb is not None and imb<0:
        score+=1; reasons.append("orderbook_absorption_or_offer_pressure")
    # Second leg / exhaustion clue: price rises while aggressive delta weakens.
    d1=_num(f1.get("delta_ratio"))
    if pc5 is not None and pc5>0 and d1 is not None and delta5 is not None and d1<delta5*0.65:
        score+=1; reasons.append("aggression_efficiency_falling")
    if pump and score>=4:
        return {"status":"DETECTED","signal":"SHORT_CANDIDATE","score":score,"direction":direction,"reasons":reasons,"price":price,"execution":"WAIT_STRUCTURE_BREAK_RETEST"}
    if pump:
        return {"status":"WATCH","signal":"PUMP_NO_EXHAUSTION_CONFIRMATION","score":score,"direction":"WATCH","reasons":reasons,"price":price,"execution":"NO_SHORT_UNTIL_EXHAUSTION"}
    return {"status":"NO_EVENT","signal":"NONE","score":score,"direction":"NONE","reasons":reasons,"price":price,"execution":"NONE"}
