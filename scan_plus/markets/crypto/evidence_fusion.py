"""Evidence fusion for post-pump exhaustion without double-counting correlated clues."""
from typing import Mapping


CHANNELS={
    "price_aggression":"aggression_inefficiency",
    "orderbook":"absorption",
    "spot_perp":"spot_perp_divergence",
}


def fuse_exhaustion(pipeline: Mapping, *, min_independent_channels=2):
    channels={}
    confirmed=[]
    observed=[]
    scores=[]
    for channel,stage_name in CHANNELS.items():
        stage=pipeline.get(stage_name) or {}
        item={"stage":stage_name,"observed":bool(stage.get("observed")),
              "confirmed":bool(stage.get("confirmed")),"score":float(stage.get("score") or 0.0)}
        channels[channel]=item
        if item["observed"]: observed.append(channel)
        if item["confirmed"]:
            confirmed.append(channel); scores.append(item["score"])

    # Two independent information channels are the normal confirmation path.
    # Exception: one very strong channel may keep WATCH context but cannot ARMED.
    confirmed_ok=len(confirmed)>=int(min_independent_channels)
    confidence=(sum(scores)/len(scores)) if scores else 0.0
    return {
        "observed":bool(observed),
        "confirmed":confirmed_ok,
        "confidence":round(confidence,4),
        "independent_channels_confirmed":confirmed,
        "independent_channel_count":len(confirmed),
        "channels":channels,
        "reason":"independent_exhaustion_channels_confirmed" if confirmed_ok
                 else "insufficient_independent_exhaustion_evidence",
    }


def healthy_continuation_veto(observables: Mapping, pipeline: Mapping):
    spot=observables.get("spot_delta_ratio")
    perp=observables.get("perp_delta_ratio")
    oi=observables.get("oi_change_pct")
    price=observables.get("price_change_pct")
    spot_not_confirming=bool(observables.get("spot_not_confirming_up"))

    reasons=[]
    # Healthy continuation requires participation, not merely a positive Spot tick.
    if spot is not None and spot>=0.20 and not spot_not_confirming:
        reasons.append("strong_spot_confirmation")
    if oi is not None and oi>0 and price is not None and price>0 and perp is not None and perp>0:
        reasons.append("oi_expansion_with_price_and_perp_confirmation")

    failed=(pipeline.get("failed_acceptance") or {}).get("confirmed")
    veto=bool(reasons and not failed)
    return {"active":veto,"reasons":reasons,
            "failed_acceptance_confirmed":bool(failed)}
