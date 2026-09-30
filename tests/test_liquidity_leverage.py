import unittest

from scan_plus.core.liquidity_leverage import MarketEvent, deduplicate_events, classify_liquidity_leverage
from scan_plus.markets.crypto.manipulation_x25 import manipulation_x25_state, x25_position_plan


class EventEngineTests(unittest.TestCase):
    def test_same_event_is_not_double_counted(self):
        a=MarketEvent("pump_exhaustion:bearish:1","pump_exhaustion","bearish",0.6,("rsi",))
        b=MarketEvent("pump_exhaustion:bearish:1","pump_exhaustion","bearish",0.9,("cvd",))
        out=deduplicate_events([a,b])
        self.assertEqual(len(out),1)
        self.assertEqual(out[0].confidence,0.9)

    def test_classifier_keeps_related_families_separate(self):
        out=classify_liquidity_leverage({
            "event_anchor":"abc",
            "pump_exhaustion":{"active":True,"confidence":0.8},
            "crowded_positioning":{"active":True,"direction":"bullish","confidence":0.7},
        })
        self.assertEqual({x.family for x in out},{"pump_exhaustion","crowded_positioning"})


class ManipulationTests(unittest.TestCase):
    def test_never_shorts_healthy_acceptance(self):
        state=manipulation_x25_state({
            "abnormal_pump":True,"exhaustion":True,"leverage_fragility":True,
            "failed_acceptance":True,"structure_break":True,"bearish_displacement":True,
            "failed_retest":True,"acceptance_above_high":True,
        })
        self.assertEqual(state["action"],"NO SHORT")

    def test_healthy_continuation_is_a_short_veto(self):
        state=manipulation_x25_state({
            "abnormal_pump":True,"exhaustion":True,"leverage_fragility":True,
            "failed_acceptance":False,"healthy_continuation":True,
        })
        self.assertEqual(state["action"],"NO SHORT")
        self.assertEqual(state["state"],"WATCH")

    def test_requires_break_displacement_and_failed_retest(self):
        base={"abnormal_pump":True,"exhaustion":True,"leverage_fragility":True,"failed_acceptance":True}
        self.assertEqual(manipulation_x25_state(base)["state"],"ARMED")
        base.update(structure_break=True,bearish_displacement=True,failed_retest=True)
        self.assertEqual(manipulation_x25_state(base)["state"],"TRIGGERED")

    def test_x25_leverage_does_not_increase_account_risk(self):
        plan=x25_position_plan(1000,100,102,94,risk_pct=.01,fee_slippage_pct=.001,liquidation_price=104)
        self.assertAlmostEqual(plan["allowed_loss"],10.0)
        self.assertLess(plan["required_margin"],plan["position_notional"])
        self.assertTrue(plan["eligible"])

    def test_risk_cap_is_hard(self):
        plan=x25_position_plan(1000,100,102,94,risk_pct=.02,liquidation_price=104)
        self.assertFalse(plan["eligible"])
        self.assertEqual(plan["reason"],"risk_above_x25_cap")

    def test_liquidation_before_safe_buffer_rejects_x25(self):
        plan=x25_position_plan(1000,100,102,94,risk_pct=.01,fee_slippage_pct=.001,liquidation_price=102.05)
        self.assertFalse(plan["eligible"])
        self.assertEqual(plan["reason"],"liquidation_buffer_insufficient")


if __name__=="__main__":
    unittest.main()
