import unittest
from datetime import datetime, timezone
from scan_plus.core.session_levels import session_high_low
from scan_plus.markets.commodities.adapter import CommoditiesMarketAdapter


def ms(s):
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()*1000)


class ProviderBoundaryAuditTests(unittest.TestCase):
    def test_session_levels_do_not_use_current_incomplete_session_without_cutoff(self):
        candles=[
            {"timestamp_ms":ms("2026-10-01T08:30:00+00:00"),"high":1.20,"low":1.10},
            {"timestamp_ms":ms("2026-10-01T11:30:00+00:00"),"high":1.30,"low":1.05},
        ]
        out=session_high_low(candles,market="forex",session="london")
        self.assertFalse(out["ready"])

    def test_commodity_adapter_accepts_provider_object(self):
        class Provider:
            def candles(self,symbol,interval="60"):
                return {"symbol":symbol,"interval":interval,"candles":[]}
        adapter=CommoditiesMarketAdapter(Provider())
        self.assertIsInstance(adapter,CommoditiesMarketAdapter)

if __name__=="__main__":
    unittest.main()
