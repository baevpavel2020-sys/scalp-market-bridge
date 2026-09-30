"""Manipulation x25 decision state machine.

The module returns eligibility and risk math. It does not place orders.
"""
from typing import Any, Mapping


def manipulation_x25_state(evidence: Mapping[str, Any]):
    abnormal = bool(evidence.get("abnormal_pump"))
    if not abnormal:
        return {"state": "INACTIVE", "action": "NO SHORT", "reason": "no_abnormal_pump"}

    healthy_acceptance = (bool(evidence.get("acceptance_above_high")) or
                          bool(evidence.get("healthy_spot_demand")) or
                          bool(evidence.get("healthy_continuation")))
    if healthy_acceptance:
        return {"state": "WATCH", "action": "NO SHORT", "reason": "healthy_acceptance_spot_or_continuation"}

    armed = all(bool(evidence.get(k)) for k in (
        "exhaustion", "leverage_fragility", "failed_acceptance",
    ))
    if not armed:
        return {"state": "WATCH", "action": "NO SHORT", "reason": "exhaustion_not_confirmed"}

    triggered = all(bool(evidence.get(k)) for k in (
        "structure_break", "bearish_displacement", "failed_retest",
    ))
    if not triggered:
        return {"state": "ARMED", "action": "NO SHORT", "reason": "execution_trigger_missing"}

    return {"state": "TRIGGERED", "action": "SHORT_CANDIDATE", "reason": "confirmed_post_pump_reversal"}


def x25_position_plan(account_equity, entry, stop, target, *,
                      risk_pct=0.01, leverage=25.0,
                      fee_slippage_pct=0.0015,
                      liquidation_price=None,
                      min_net_rr=2.0):
    equity=float(account_equity); entry=float(entry); stop=float(stop); target=float(target)
    risk_pct=float(risk_pct); leverage=float(leverage)
    if not (equity > 0 and entry > 0 and stop > entry and 0 < target < entry):
        return {"eligible": False, "reason": "invalid_short_price_geometry"}
    if not (0 < risk_pct <= 0.015):
        return {"eligible": False, "reason": "risk_above_x25_cap"}
    stop_pct=(stop-entry)/entry
    total_loss_fraction=stop_pct + max(0.0, float(fee_slippage_pct))
    allowed_loss=equity*risk_pct
    notional=allowed_loss/total_loss_fraction
    margin=notional/leverage
    gross_rr=(entry-target)/(stop-entry)
    cost_abs=notional*max(0.0, float(fee_slippage_pct))
    reward_abs=notional*((entry-target)/entry)-cost_abs
    risk_abs=notional*stop_pct+cost_abs
    net_rr=reward_abs/risk_abs if risk_abs > 0 else 0.0

    liq_buffer_ok=None
    stop_to_liq_pct=None
    if liquidation_price is not None:
        liquidation_price=float(liquidation_price)
        stop_to_liq_pct=(liquidation_price-stop)/stop if stop else None
        # Liquidation must be beyond the stop, with at least the same fee/slippage
        # reserve used in position sizing.
        liq_buffer_ok=bool(liquidation_price > stop and stop_to_liq_pct > max(float(fee_slippage_pct), 0.001))

    eligible=bool(net_rr >= min_net_rr and liq_buffer_ok is not False)
    reason="ok" if eligible else "net_rr_below_minimum" if net_rr < min_net_rr else "liquidation_buffer_insufficient"
    return {
        "eligible": eligible, "reason": reason, "leverage": leverage,
        "allowed_loss": round(allowed_loss, 8), "position_notional": round(notional, 8),
        "required_margin": round(margin, 8), "stop_pct": round(stop_pct*100, 6),
        "gross_rr": round(gross_rr, 4), "expected_net_rr": round(net_rr, 4),
        "estimated_liquidation": liquidation_price,
        "stop_to_liquidation_pct": None if stop_to_liq_pct is None else round(stop_to_liq_pct*100, 6),
        "liquidation_buffer_ok": liq_buffer_ok,
    }
