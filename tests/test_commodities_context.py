import unittest
from scan_plus.markets.commodities.context import build_commodity_context
from scan_plus.markets.commodities.adapter import CommoditiesMarketAdapter
from scan_plus.markets.commodities.loader import CommodityLoader


class Provider:
    def __call__(self,symbol,interval="60"):
        return {"candles":[
            {"timestamp_ms":1760000000000,"open":100,"high":102,"low":99,"close":101,"volume":1000}
        ],"usd_yields_context":{"10y":4.1},"event_context":{"status":"none"}}


class CommodityContextTests(unittest.TestCase):
    def test_metals_use_only_provider_context(self):
        out=build_commodity_context(
            group="metals",
            provider_context={"usd_yields_context":{"10y":4.1}})
        self.assertIn("usd_yields_context",out["available"])
        self.assertIn("usd_context",out["unknown"])
        self.assertFalse(out["directional_inference"])

    def test_missing_energy_inventory_stays_unknown(self):
        out=build_commodity_context(group="energy",provider_context={})
        self.assertEqual(out["available"],[])
        self.assertIn("inventory_context",out["unknown"])

    def test_adapter_exposes_context(self):
        adapter=CommoditiesMarketAdapter(CommodityLoader(Provider()))
        out=adapter.scan("XAUUSD")
        self.assertIn("commodity_context",out["frames"]["1h"])
        self.assertIn("usd_yields_context",out["frames"]["1h"]["commodity_context"]["available"])


if __name__=="__main__":
    unittest.main()
