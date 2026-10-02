import unittest
from scan_plus.multimarket import parse_scan_command, MultiMarketOrchestrator
from scan_plus.market_registry import MarketRegistry
from scan_plus.contracts import MarketAdapter


class FakeAdapter(MarketAdapter):
    def __init__(self,market,symbols):
        self.market=market
        self._symbols=symbols
        self.calls=[]
    def default_symbols(self,limit=30):
        return self._symbols[:limit]
    def prescan(self,**kwargs):
        return {"market":self.market}
    def scan(self,symbol):
        self.calls.append(symbol)
        return {"market":self.market,"symbol":symbol}
    def diagnostics(self,symbol):
        return {"market":self.market,"symbol":symbol}


class MultiMarketRoutingTests(unittest.TestCase):
    def setUp(self):
        self.registry=MarketRegistry()
        self.crypto=FakeAdapter("crypto",["C1","C2"])
        self.stocks=FakeAdapter("stocks",["S1","S2"])
        self.registry.register(self.crypto)
        self.registry.register(self.stocks)
        self.orch=MultiMarketOrchestrator(self.registry)

    def test_plain_scan_expands_native_universes(self):
        parsed=parse_scan_command("скан")
        self.assertEqual(parsed["markets"],("crypto","stocks","forex","commodities"))
        out=self.orch.scan_many([("crypto",None),("stocks",None)])
        self.assertEqual([x["result"]["symbol"] for x in out],["C1","C2","S1","S2"])

    def test_market_command_keeps_scope(self):
        parsed=parse_scan_command("Скан акции")
        self.assertEqual(parsed["markets"],("stocks",))
        self.assertEqual(parsed["symbols"],[])

    def test_named_symbol_resolves_exact_market(self):
        parsed=parse_scan_command("Скан EURUSD")
        self.assertEqual(parsed["markets"],("forex",))
        self.assertEqual(parsed["symbols"],["EURUSD"])

    def test_one_market_failure_does_not_abort_batch(self):
        class Broken(FakeAdapter):
            def scan(self,symbol):
                raise RuntimeError("boom")
        self.registry.register(Broken("stocks",["S1"]))
        out=self.orch.scan_many([("crypto","C1"),("stocks","S1")])
        self.assertEqual(out[0]["status"],"OK")
        self.assertEqual(out[1]["status"],"DATA_ERROR")


if __name__=="__main__":
    unittest.main()
