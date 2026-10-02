import unittest
from scan_plus.markets.commodities.adapter import CommoditiesMarketAdapter
from scan_plus.markets.commodities.loader import CommodityLoader


class FakeProvider:
    def __call__(self,symbol,interval="60"):
        return {
            "source":"fake","product":"commodity","symbol":symbol,"interval":interval,
            "candles":[
                {"timestamp_ms":1760000000000,"open":100,"high":102,"low":99,"close":101,"volume":1000},
                {"timestamp_ms":1760000060000,"open":101,"high":103,"low":100,"close":102,"volume":1200},
            ],
        }


class CommoditiesAdapterTests(unittest.TestCase):
    def setUp(self):
        self.adapter=CommoditiesMarketAdapter(CommodityLoader(FakeProvider()))

    def test_instrument_profiles_are_distinct(self):
        gold=self.adapter.profile("XAUUSD")
        oil=self.adapter.profile("WTI")
        cocoa=self.adapter.profile("COCOA")
        self.assertEqual(gold["group"],"metals")
        self.assertEqual(oil["group"],"energy")
        self.assertEqual(cocoa["group"],"softs")
        self.assertNotEqual(gold["scan"],oil["scan"])

    def test_scan_keeps_provider_context_separate(self):
        out=self.adapter.scan("XAUUSD")
        self.assertEqual(out["market"],"commodities")
        self.assertEqual(out["instrument_group"],"metals")
        self.assertEqual(set(out["frames"]),{"1d","5m","15m","1h","4h"})
        self.assertTrue(out["execution_context"]["provider_context_only"])

    def test_provider_error_isolated(self):
        class Bad:
            def __call__(self,symbol,interval="60"):
                raise LookupError("feed unavailable")
        adapter=CommoditiesMarketAdapter(CommodityLoader(Bad()))
        out=adapter.prescan("WTI")
        self.assertEqual(out["bars"],0)
        self.assertIn("provider_error",out)

if __name__=="__main__":
    unittest.main()
