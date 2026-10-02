import unittest

from scan_plus.markets.stocks.adapter import StocksMarketAdapter
from scan_plus.markets.forex.adapter import ForexMarketAdapter
from scan_plus.markets.commodities.adapter import CommoditiesMarketAdapter

from dynamic_collector import DynamicMarketManager
from scan_plus.markets.stocks.execution import build_stock_execution_plan
from scan_plus.markets.forex.execution import build_forex_execution_plan
from scan_plus.markets.commodities.execution import build_commodity_execution_plan
from scan_plus.markets.crypto.post_pump import detect_post_pump_pipeline


def frame(state="uptrend"):
    return {
        "analysis": {
            "structure": {
                "state": state,
                "last_swing_low": {"price": 99.0},
                "last_swing_high": {"price": 101.0},
                "degrees": {},
            },
            "fibonacci": {"ready": True, "extensions": {"1.618": 104.0}},
            "elliott": {"ready": True},
        },
        "data_quality": {"closed_only": True, "last_closed": True},
    }



class FakeEngine:
    def analyze_profiled(self, rows, profile):
        return {
            "structure": {
                "state": "uptrend",
                "last_swing_low": {"price": 99.0},
                "last_swing_high": {"price": 101.0},
                "degrees": {},
            },
            "fibonacci": {"ready": True, "extensions": {"1.618": 104.0}},
            "elliott": {"ready": True},
        }


class FakeXLoader:
    def default_symbols(self, limit=15): return ["NVDA"]
    def ticker(self, symbol): return {"product":"xstock_spot","symbol":"NVDAXUSDT","last_price":100.0,"volume_24h":1000000}
    def klines(self, symbol, interval="5", limit=240):
        return {"candles":[{"timestamp_ms":i*300000,"open":99,"high":101,"low":98,"close":100,"volume":100} for i in range(240)]}


class FakeFXLoader:
    def candles(self, symbol="EURUSD", interval="60"):
        step={"5":300000,"15":900000,"60":3600000,"240":14400000,"d":86400000}[str(interval)]
        return {"candles":[{"timestamp_ms":i*step,"open":1.1,"high":1.2,"low":1.0,"close":1.15,"volume":0} for i in range(240)]}


class FakeCommodityLoader:
    def candles(self, symbol, **kwargs):
        interval=str(kwargs.get("interval","60"))
        step={"5":300000,"15":900000,"60":3600000,"240":14400000,"D":86400000}[interval]
        return {"candles":[{"timestamp_ms":i*step,"open":100,"high":101,"low":99,"close":100,"volume":100} for i in range(240)]}


class Phase1ProductionAuditTests(unittest.TestCase):
    def test_price_discovery_does_not_override_confirmed_direction(self):
        linear = {"analysis": {
            "D": {"confluence": {"direction": "bearish"}, "structure": {
                "state": "downtrend",
                "degrees": {"intermediate": {"last_points": [
                    {"price": 100, "kind": "low", "start": 1},
                    {"price": 110, "kind": "high", "start": 2},
                ]},
                },
            }},
            "240": {}, "60": {}, "15": {}, "5": {}, "1": {},
        }}
        live = DynamicMarketManager._live_structure_context_v33(
            linear, {"price": 120}
        )
        self.assertEqual(live["D"]["mode"], "price_discovery_up")
        self.assertEqual(live["D"]["effective_direction"], "bearish")
        self.assertEqual(live["D"]["price_discovery_direction"], "bullish")
        self.assertEqual(live["D"]["direction_confirmation"], "pending_structure_confirmation")

    def test_stock_execution_exposes_conditional_limit_without_account_context(self):
        candidate={"status":"CANDIDATE","direction":"bullish"}
        frames={"15m":frame("uptrend")}
        plan=build_stock_execution_plan(candidate=candidate,frames=frames,price=100,equity=None,risk_fraction=None)
        self.assertEqual(plan["status"],"PLAN")
        self.assertTrue(plan["eligible"])
        self.assertEqual(plan["entry"]["type"],"LIMIT")
        self.assertEqual(plan["risk"]["sizing_status"],"ACCOUNT_CONTEXT_REQUIRED")
        self.assertGreaterEqual(plan["rr"],2.0)

    def test_forex_execution_refuses_unjustified_quote_conversion_for_sizing(self):
        candidate={"status":"CANDIDATE","direction":"bullish"}
        frames={"15m":frame("uptrend")}
        plan=build_forex_execution_plan(
            candidate=candidate,frames=frames,price=150,
            equity=100000,risk_fraction=0.01,symbol="USDJPY"
        )
        self.assertEqual(plan["status"],"WAIT")
        self.assertEqual(plan["reason"],"fx_quote_to_account_conversion_required")

    def test_forex_execution_can_still_publish_conditional_limit_without_sizing(self):
        candidate={"status":"CANDIDATE","direction":"bullish"}
        frames={"15m":frame("uptrend")}
        plan=build_forex_execution_plan(
            candidate=candidate,frames=frames,price=150,equity=None,risk_fraction=None,symbol="USDJPY"
        )
        self.assertEqual(plan["status"],"PLAN")
        self.assertTrue(plan["eligible"])
        self.assertEqual(plan["entry"]["type"],"LIMIT")

    def test_commodity_execution_exposes_conditional_limit(self):
        candidate={"status":"CANDIDATE","direction":"bullish","group":"metals","confirmation_timeframe":"15m"}
        frames={"15m":frame("uptrend")}
        plan=build_commodity_execution_plan(candidate=candidate,frames=frames,price=100,equity=None,risk_fraction=None)
        self.assertEqual(plan["status"],"PLAN")
        self.assertTrue(plan["eligible"])
        self.assertEqual(plan["entry"]["type"],"LIMIT")
        self.assertGreaterEqual(plan["rr"],2.0)


    def test_xstocks_adapter_has_daily_context(self):
        out=StocksMarketAdapter(loader=FakeXLoader(),engine=FakeEngine()).scan("NVDA")
        self.assertIn("1d",out["frames"])
        self.assertIn("limit_plan",out["candidate"])

    def test_forex_adapter_has_daily_context(self):
        out=ForexMarketAdapter(loader=FakeFXLoader(),engine=FakeEngine()).scan("EURUSD")
        self.assertIn("1d",out["frames"])

    def test_commodity_adapter_has_daily_context(self):
        out=CommoditiesMarketAdapter(loader=FakeCommodityLoader(),engine=FakeEngine()).scan("XAUUSD")
        self.assertIn("1d",out["frames"])
        self.assertIn("limit_plan",out["candidate"])

    def test_manipulation_accepts_native_liquidation_evidence_as_corrobation(self):
        scan={
            "execution":{
                "windows":{
                    "5m":{
                        "perp_price_change_pct":1.8,
                        "perp_delta_ratio":0.5,
                        "spot_delta_ratio":0.0,
                        "sample_quality":0.8,
                        "perp_flow_usable":True,
                    }
                },
                "funding_rate":0.01,
                "book_imbalance":-0.2,
                "liquidations":{
                    "5m":{
                        "event_count":4,
                        "long_liquidation_volume":120.0,
                        "short_liquidation_volume":20.0,
                    }
                },
            },
            "trade_state":"WAIT_FLOW",
            "setup":{},
            "timeframes":{},
        }
        out=detect_post_pump_pipeline(scan)
        self.assertTrue(out["liquidation_cascade"]["observed"])
        self.assertTrue(out["liquidation_cascade"]["confirmed"])

if __name__=="__main__":
    unittest.main()
