import unittest
from scan_plus.markets.forex.adapter import ForexMarketAdapter
from scan_plus.markets.stocks.bybit_xstocks import BybitXStocksLoader
from scan_plus.instrument_resolver import resolve_market


class UniverseResolverAuditTests(unittest.TestCase):
    def test_forex_has_native_default_universe(self):
        symbols=ForexMarketAdapter(loader=object()).default_symbols(9)
        self.assertEqual(len(symbols),9)
        self.assertIn("EURUSD",symbols)

    def test_unknown_xstock_underlying_gets_xstock_symbol(self):
        loader=BybitXStocksLoader(session=object())
        self.assertEqual(loader.token_symbol("ABC"),"ABCXUSDT")

    def test_explicit_xstock_token_resolves_to_stocks(self):
        self.assertEqual(resolve_market("NVDAXUSDT"),"stocks")


if __name__=="__main__":
    unittest.main()
