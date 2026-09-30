import unittest
from scan_plus.core.analytical_engine import AnalyticalEngine, LegacyCryptoBackend
from dynamic_collector import MarketStream


def candles(n=80, mode=0):
    rows=[]
    price=100.0
    for i in range(n):
        # deterministic alternating structure with enough pivots.
        drift=((i % 10)-5)*0.35
        open_=price
        bias=(0.8 if (i//5)%2==0 else -0.65)
        if mode == 1:
            bias *= -1
        elif mode == 2:
            bias += 0.35 if i % 7 < 3 else -0.25
        close=price+drift+bias
        high=max(open_,close)+1.0+(i%3)*0.1
        low=min(open_,close)-1.0-((i+1)%3)*0.1
        rows.append({"start":i*60_000,"open":open_,"high":high,"low":low,
                     "close":close,"volume":1000+i*3})
        price=close
    return rows


class AnalyticalParityTests(unittest.TestCase):
    def test_structure_backend_matches_legacy(self):
        rows=candles()
        stream=object.__new__(MarketStream)
        direct=stream._structure_metrics(rows)
        wrapped=LegacyCryptoBackend().structure(rows)
        self.assertEqual(direct,wrapped)

    def test_engine_fibonacci_matches_legacy_backend(self):
        rows=candles()
        stream=object.__new__(MarketStream)
        structure=stream._structure_metrics(rows)
        direct=stream._fib_metrics(rows,structure)
        wrapped=LegacyCryptoBackend().fibonacci(rows,structure)
        self.assertEqual(direct,wrapped)

    def test_engine_elliott_matches_legacy(self):
        rows=candles()
        stream=object.__new__(MarketStream)
        structure=stream._structure_metrics(rows)
        direct=stream._elliott_metrics(rows,structure)
        wrapped=LegacyCryptoBackend().elliott(rows,structure)
        self.assertEqual(direct,wrapped)

    def test_engine_harmonics_matches_legacy(self):
        rows=candles()
        stream=object.__new__(MarketStream)
        structure=stream._structure_metrics(rows)
        direct=stream._harmonic_metrics(structure)
        wrapped=LegacyCryptoBackend().harmonics(structure)
        self.assertEqual(direct,wrapped)

    def test_parity_holds_across_multiple_deterministic_market_shapes(self):
        stream=object.__new__(MarketStream)
        for mode in (0,1,2):
            rows=candles(mode=mode)
            structure=stream._structure_metrics(rows)
            pairs=(
                (stream._structure_metrics(rows),
                 LegacyCryptoBackend().structure(rows)),
                (stream._fib_metrics(rows,structure),
                 LegacyCryptoBackend().fibonacci(rows,structure)),
                (stream._elliott_metrics(rows,structure),
                 LegacyCryptoBackend().elliott(rows,structure)),
                (stream._harmonic_metrics(structure),
                 LegacyCryptoBackend().harmonics(structure)),
            )
            for reference,candidate in pairs:
                self.assertTrue(AnalyticalEngine.compare(reference,candidate)["equal"],
                                f"parity failed for mode {mode}")

    def test_engine_keeps_selective_block_contract(self):
        out=AnalyticalEngine(LegacyCryptoBackend()).analyze(candles(),requested=["structure"])
        self.assertEqual(set(out),{"structure"})


if __name__=="__main__":
    unittest.main()
