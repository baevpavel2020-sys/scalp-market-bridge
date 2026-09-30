"""No-lookahead replay and outcome scoring for Manipulation x25 shadow signals."""
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from scan_plus.markets.crypto.event_adapter import observe_crypto_events


@dataclass(frozen=True)
class TriggerOutcome:
    index: int
    entry: float
    mfe_pct: float
    mae_pct: float
    bars_to_mfe: int
    hit_2r: bool
    hit_stop: bool


def _price(snapshot):
    try:
        return float((snapshot.get("execution") or {}).get("price"))
    except (TypeError, ValueError):
        return None


def replay_snapshots(snapshots: Sequence[Mapping], *, forward_bars=12, stop_pct=0.01):
    """Replay pre-built point-in-time snapshots.

    Contract: caller must supply snapshots built from prefixes only. This function
    never reads a future snapshot until after a trigger has already occurred.
    """
    timeline=[]
    trigger_indices=[]
    for i,snapshot in enumerate(snapshots):
        observed=observe_crypto_events(snapshot)
        state=(observed.get("manipulation_x25") or {}).get("state")
        timeline.append({"index":i,"state":state,"action":(observed.get("manipulation_x25") or {}).get("action"),
                         "price":_price(snapshot)})
        if state=="TRIGGERED" and (i==0 or timeline[i-1]["state"]!="TRIGGERED"):
            trigger_indices.append(i)

    outcomes=[]
    for idx in trigger_indices:
        entry=_price(snapshots[idx])
        if entry is None: continue
        future=snapshots[idx+1:idx+1+forward_bars]
        future_prices=[_price(x) for x in future]
        future_prices=[x for x in future_prices if x is not None]
        if not future_prices: continue
        # Short trade: favorable excursion is downward, adverse is upward.
        favorable=[(entry-p)/entry*100 for p in future_prices]
        adverse=[(p-entry)/entry*100 for p in future_prices]
        mfe=max(favorable+[0.0]); mae=max(adverse+[0.0])
        bars_to_mfe=(favorable.index(mfe)+1) if mfe>0 else 0
        reward_2r=2.0*float(stop_pct)*100.0
        outcomes.append(TriggerOutcome(idx,entry,round(mfe,6),round(mae,6),bars_to_mfe,
                                       mfe>=reward_2r,mae>=float(stop_pct)*100.0))

    counts={"INACTIVE":0,"WATCH":0,"ARMED":0,"TRIGGERED":0}
    for row in timeline:
        if row["state"] in counts: counts[row["state"]]+=1
    return {
        "timeline":timeline,
        "trigger_count":len(trigger_indices),
        "state_counts":counts,
        "outcomes":[o.__dict__ for o in outcomes],
        "hit_2r_count":sum(o.hit_2r for o in outcomes),
        "hit_stop_count":sum(o.hit_stop for o in outcomes),
        "limitations":[
            "snapshot_replay_requires_point_in_time_inputs",
            "mfe_mae_use_snapshot_prices_not_intrabar_high_low",
            "fees_slippage_funding_not_in_outcome_labels",
        ],
    }
