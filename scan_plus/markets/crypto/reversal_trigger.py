"""Execution trigger detector for bearish post-pump reversals.

Uses existing Structure/SMC authority. Break, displacement and retest are separate
observations and must occur in causal order.
"""
from typing import Any, Mapping


def _tf(scan,tf):
    return ((scan.get("timeframes") or {}).get(tf) or {})


def detect_bearish_reversal_trigger(scan: Mapping[str, Any]):
    candidates=[]
    for tf in ("1","5","15"):
        frame=_tf(scan,tf)
        structure=frame.get("structure") or {}
        smc=frame.get("smc") or {}
        ev=structure.get("event") or {}
        mss=(smc.get("mss") or {})
        mss_ev=mss.get("event") or {}
        displacement=smc.get("displacement") or {}

        break_confirmed=bool(
            (ev.get("type") in ("BOS","CHoCH") and ev.get("direction")=="bearish" and ev.get("confirmed_by_close"))
            or (mss.get("confirmed") and mss_ev.get("direction")=="bearish" and mss_ev.get("confirmed_by_close"))
        )
        break_start=mss_ev.get("start") or ev.get("start")

        displacement_confirmed=bool(
            displacement.get("direction")=="bearish"
            and float(displacement.get("body_atr") or 0) >= 0.9
            and float(displacement.get("efficiency") or 0) >= 0.55
        )
        disp_start=displacement.get("start")
        disp_end=displacement.get("end")

        scenario=smc.get("scenario") or {}
        stage=scenario.get("stage")
        pois=smc.get("poi") or []
        # Existing SMC engine labels a retrace into the causal POI as IN_POI.
        # Failed retest requires price to have returned to the POI and then rejected;
        # WAIT_RETRACE alone is NOT a failed retest.
        in_poi=stage=="IN_POI"
        bearish_rejection=False
        rejection_evidence=[]
        if in_poi:
            trigger_state=scan.get("trigger_state")
            setup=scan.get("setup") or {}
            bearish_rejection=bool(
                trigger_state=="aligned"
                and setup.get("trigger_direction_aligned") is True
                and (scan.get("direction") or {}).get("bias","bearish") in ("bearish",None)
            )
            if bearish_rejection:
                rejection_evidence=["causal_poi_touched","bearish_trigger_aligned"]

        # Unknown timestamps are not proof of ordering. MSS may establish the
        # structural relationship, but it must not silently override contradictory
        # timestamps when both are available.
        timestamps_known=break_start is not None and disp_end is not None
        timestamp_order=(disp_end <= break_start) if timestamps_known else None
        causal=bool(
            break_confirmed and displacement_confirmed
            and (timestamp_order is not False)
            and (timestamp_order is True or mss.get("confirmed"))
        )
        candidates.append({
            "timeframe":tf,
            "structure_break":{"observed":bool(ev or mss_ev),"confirmed":break_confirmed,
                               "event":mss_ev if mss.get("confirmed") else ev},
            "bearish_displacement":{"observed":bool(displacement),"confirmed":displacement_confirmed,
                                    "event":displacement},
            "failed_retest":{"observed":in_poi,"confirmed":bool(in_poi and bearish_rejection),
                             "evidence":rejection_evidence},
            "causal_order_confirmed":causal,
            "causal_timestamps_known":timestamps_known,
            "causal_timestamp_order":timestamp_order,
        })

    # Prefer the lowest execution timeframe that has the most complete chain.
    candidates.sort(key=lambda x:(
        -(int(x["structure_break"]["confirmed"])+int(x["bearish_displacement"]["confirmed"])+int(x["failed_retest"]["confirmed"])),
        {"1":1,"5":2,"15":3}.get(x["timeframe"],9)
    ))
    best=candidates[0] if candidates else {}
    triggered=bool(
        best
        and best.get("causal_order_confirmed")
        and best["structure_break"]["confirmed"]
        and best["bearish_displacement"]["confirmed"]
        and best["failed_retest"]["confirmed"]
    )
    return {"version":"bearish_reversal_trigger_v1","triggered":triggered,
            "best_timeframe":best.get("timeframe"),"best":best,"candidates":candidates}
