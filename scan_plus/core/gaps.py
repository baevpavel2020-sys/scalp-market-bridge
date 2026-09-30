"""Gap calculations are pure and provider-agnostic."""
def gap_from_previous(close, previous_close):
    close=float(close); previous_close=float(previous_close)
    if previous_close == 0: return None
    return {"absolute":close-previous_close,
            "pct":(close-previous_close)/previous_close*100.0,
            "direction":"up" if close>previous_close else "down" if close<previous_close else "flat"}
