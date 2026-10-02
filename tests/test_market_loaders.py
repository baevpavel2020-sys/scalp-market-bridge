import unittest
from unittest.mock import Mock
from scan_plus.instrument_resolver import resolve_market
from scan_plus.markets.stocks.bybit_xstocks import BybitXStocksLoader


class InstrumentResolutionTests(unittest.TestCase):
    def test_named_instruments_resolve(self):
        self.assertEqual(resolve_market("NVDA"),"stocks")
        self.assertEqual(resolve_market("EURUSD"),"forex")
        self.assertEqual(resolve_market("XAUUSD"),"commodities")
        self.assertEqual(resolve_market("BTCUSDT"),"crypto")


class BybitXStocksTests(unittest.TestCase):
    def test_nvda_maps_to_nvdax(self):
        loader=BybitXStocksLoader(session=Mock())
        self.assertEqual(loader.token_symbol("NVDA"),"NVDAXUSDT")
        self.assertEqual(loader.token_symbol("NVDAXUSDT"),"NVDAXUSDT")

    def test_default_symbols_falls_back_when_universe_request_fails(self):
        session=Mock()
        session.get.side_effect=RuntimeError("provider unavailable")
        out=BybitXStocksLoader(session=session).default_symbols(15)
        self.assertGreaterEqual(len(out), 11)
        self.assertEqual(out[:3], ["NVDA","AAPL","MSFT"])

    def test_default_symbols_fills_partial_live_universe(self):
        session=Mock()
        response=Mock()
        response.json.return_value={"retCode":0,"result":{"list":[
            {"symbol":"NVDAXUSDT","turnover24h":"100"},
            {"symbol":"AAPLXUSDT","turnover24h":"90"},
        ]}}
        response.raise_for_status.return_value=None
        session.get.return_value=response
        out=BybitXStocksLoader(session=session).default_symbols(5)
        self.assertEqual(out[:2],["NVDA","AAPL"])
        self.assertEqual(len(out),5)

    def test_ticker_normalizes_bybit_payload(self):
        session=Mock()
        response=Mock()
        response.json.return_value={"retCode":0,"result":{"list":[
            {"symbol":"NVDAXUSDT","lastPrice":"180.1","bid1Price":"180","ask1Price":"180.2","volume24h":"123"}
        ]}}
        response.raise_for_status.return_value=None
        session.get.return_value=response
        out=BybitXStocksLoader(session=session).ticker("NVDA")
        self.assertEqual(out["symbol"],"NVDAXUSDT")
        self.assertEqual(out["underlying"],"NVDA")
        self.assertAlmostEqual(out["last_price"],180.1)


if __name__=="__main__":
    unittest.main()
