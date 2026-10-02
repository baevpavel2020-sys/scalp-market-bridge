import unittest
from scan_plus.markets.forex.yahoo import YahooFXLoader
from scan_plus.markets.commodities.yahoo import YahooCommodityLoader


class FakeResponse:
    def __init__(self,payload):
        self.payload=payload
    def raise_for_status(self): pass
    def json(self): return self.payload


class FakeSession:
    def get(self,url,params=None,timeout=None,headers=None):
        timestamps=[0,3600,7200,10800]
        quote={"open":[1,2,3,4],"high":[2,3,4,5],"low":[0,1,2,3],"close":[1.5,2.5,3.5,4.5],
               "volume":[10,20,30,40]}
        return FakeResponse({"chart":{"result":[{"timestamp":timestamps,
            "indicators":{"quote":[quote]}}],"error":None}})


class YahooProviderTests(unittest.TestCase):
    def test_fx_4h_is_aggregated_from_hourly(self):
        out=YahooFXLoader(session=FakeSession()).candles("EURUSD","240")
        self.assertEqual(len(out["candles"]),1)
        row=out["candles"][0]
        self.assertEqual(row["open"],1)
        self.assertEqual(row["high"],5)
        self.assertEqual(row["low"],0)
        self.assertEqual(row["close"],4.5)
        self.assertEqual(row["volume"],100)

    def test_commodity_mapping_is_explicit(self):
        out=YahooCommodityLoader(session=FakeSession()).candles("WTI","60")
        self.assertEqual(out["provider_symbol"],"CL=F")
        self.assertEqual(out["product"],"commodity_future")


if __name__=="__main__":
    unittest.main()
