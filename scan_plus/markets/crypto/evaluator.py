"""Evaluation/reporting for recorded Manipulation x25 shadow snapshots."""
from collections import Counter
from statistics import median
from typing import Mapping, Sequence

from scan_plus.markets.crypto.event_adapter import observe_crypto_events
from scan_plus.markets.crypto.replay import replay_snapshots


STAGES=("abnormal_pump","aggression_inefficiency","absorption",
        "spot_perp_divergence","leverage_fragility","failed_acceptance")


def _pct(n,d):
    return round(n/d*100.0,2) if d else None


def evaluate_shadow(snapshots: Sequence[Mapping], *, min_triggers=30, forward_bars=12, stop_pct=0.01):
    observations=[]
    states=Counter()
    block_reasons=Counter()
    stage_observed=Counter()
    stage_confirmed=Counter()
    trigger_parts=Counter()

    for snap in snapshots:
        obs=observe_crypto_events(snap)
        observations.append(obs)
        manip=obs.get("manipulation_x25") or {}
        state=manip.get("state") or "UNKNOWN"
        states[state]+=1
        reason=manip.get("reason")
        if reason: block_reasons[reason]+=1

        pipe=obs.get("post_pump_pipeline") or {}
        for stage in STAGES:
            data=pipe.get(stage) or {}
            if data.get("observed"): stage_observed[stage]+=1
            if data.get("confirmed"): stage_confirmed[stage]+=1

        best=(obs.get("reversal_trigger") or {}).get("best") or {}
        for key in ("structure_break","bearish_displacement","failed_retest"):
            if (best.get(key) or {}).get("confirmed"): trigger_parts[key]+=1

    replay=replay_snapshots(snapshots,forward_bars=forward_bars,stop_pct=stop_pct)
    outcomes=replay.get("outcomes") or []
    trigger_count=replay.get("trigger_count",0)
    mfe=[float(x["mfe_pct"]) for x in outcomes]
    mae=[float(x["mae_pct"]) for x in outcomes]
    bars=[int(x["bars_to_mfe"]) for x in outcomes if int(x["bars_to_mfe"])>0]
    hit2=sum(bool(x["hit_2r"]) for x in outcomes)
    hitstop=sum(bool(x["hit_stop"]) for x in outcomes)

    # Funnel transitions count first entry into a stricter state, not every snapshot.
    transitions=Counter()
    prev=None
    for obs in observations:
        state=(obs.get("manipulation_x25") or {}).get("state")
        if state!=prev and state in ("WATCH","ARMED","TRIGGERED"):
            transitions[state]+=1
        prev=state

    status="SUFFICIENT_SAMPLE" if trigger_count>=min_triggers else "INSUFFICIENT_SAMPLE"
    return {
        "status":status,
        "sample":{"snapshots":len(snapshots),"triggers":trigger_count,
                  "completed_outcomes":len(outcomes),"minimum_triggers":min_triggers},
        "funnel":{"state_snapshots":dict(states),"state_entries":dict(transitions)},
        "blocking_reasons":dict(block_reasons.most_common()),
        "detectors":{
            s:{"observed":stage_observed[s],"confirmed":stage_confirmed[s],
               "confirmation_rate_pct":_pct(stage_confirmed[s],stage_observed[s])}
            for s in STAGES
        },
        "execution_confirmations":dict(trigger_parts),
        "outcomes":{
            "hit_2r":hit2,"hit_2r_pct":_pct(hit2,len(outcomes)),
            "hit_stop":hitstop,"hit_stop_pct":_pct(hitstop,len(outcomes)),
            "median_mfe_pct":round(median(mfe),4) if mfe else None,
            "median_mae_pct":round(median(mae),4) if mae else None,
            "median_bars_to_mfe":median(bars) if bars else None,
        },
        "interpretation_allowed":status=="SUFFICIENT_SAMPLE",
        "warning":None if status=="SUFFICIENT_SAMPLE" else
            "Do not tune or promote x25 from shadow mode from this sample.",
    }
