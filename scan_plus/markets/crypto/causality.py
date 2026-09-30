"""Causal validation for Manipulation x25 evidence.

Unknown timestamps never become proof. The layer strengthens confirmed signals but
does not discard useful WATCH context when legacy data lacks event time metadata.
"""
from typing import Mapping


def _ts(event):
    if not isinstance(event,Mapping): return None
    for key in ("end","timestamp","time","start"):
        value=event.get(key)
        try:
            if value is not None: return int(value)
        except (TypeError,ValueError):
            pass
    return None


def validate_reversal_causality(pipeline: Mapping, reversal: Mapping):
    best=reversal.get("best") or {}
    disp=(best.get("bearish_displacement") or {}).get("event") or {}
    brk=(best.get("structure_break") or {}).get("event") or {}
    disp_t=_ts(disp); break_t=_ts(brk)

    # Existing reversal detector owns displacement->break ordering. Here we expose
    # timestamp evidence explicitly so x25 can distinguish proven from unknown.
    structural_known=disp_t is not None and break_t is not None
    structural_order=bool(structural_known and disp_t <= break_t)

    pump=bool((pipeline.get("abnormal_pump") or {}).get("confirmed"))
    failed=bool((pipeline.get("failed_acceptance") or {}).get("confirmed"))
    triggered=bool(reversal.get("triggered"))

    # Pump/exhaustion stages currently share rolling-window snapshots and have no
    # precise event timestamps. Do not fabricate their internal ordering.
    return {
        "pump_context_confirmed":pump,
        "failed_acceptance_confirmed":failed,
        "reversal_triggered":triggered,
        "structural_timestamps_known":structural_known,
        "displacement_timestamp":disp_t,
        "break_timestamp":break_t,
        "structural_order_confirmed":structural_order if structural_known else None,
        "full_temporal_order_confirmed":None,
        "eligible_for_trigger":bool(triggered and (structural_order if structural_known else True)),
        "limitations":[] if structural_known else ["structure_event_timestamps_incomplete"],
    }
