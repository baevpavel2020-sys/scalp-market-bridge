import json
import os
import tempfile
import unittest

from scripts import seed_builder


class FakeBybit:
    calls = 0

    @staticmethod
    def request(params):
        FakeBybit.calls += 1
        end = int(params["end"])
        base = end - 4 * 60_000
        rows = []
        for i in range(5):
            start = base + i * 60_000
            rows.append([
                str(start), "100", "101", "99", "100.5", "10", "1005"
            ])
        return {"retCode": 0, "retMsg": "OK", "result": {"list": rows}}


class Phase2SeedTests(unittest.TestCase):
    def test_fetch_candles_deduplicates_and_returns_oldest_to_newest(self):
        original = seed_builder._request
        try:
            seed_builder._request = FakeBybit.request
            rows = seed_builder.fetch_candles("BTCUSDT", "linear", "1", 8)
        finally:
            seed_builder._request = original

        self.assertEqual(len(rows), 8)
        self.assertEqual(rows, sorted(rows, key=lambda x: x["start"]))
        self.assertEqual(rows[0]["source"], "bybit_seed")
        self.assertEqual(FakeBybit.calls, 2)

    def test_seed_builder_writes_expected_contract(self):
        original = seed_builder._request
        try:
            seed_builder._request = FakeBybit.request
            payload = seed_builder.build_seed("BTCUSDT", "linear", 5)
        finally:
            seed_builder._request = original

        self.assertEqual(payload["version"], 2)
        self.assertEqual(payload["source"], "bybit_historical_market_data")
        self.assertEqual(set(payload["candles"]), {"1", "5", "15", "60", "240", "D"})
        self.assertEqual(len(payload["candles"]["1"]), 5)

    def test_dynamic_collector_reads_offline_seed_without_network(self):
        import dynamic_collector

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "BTCUSDT_linear_seed.json")
            candle = {
                "start": 1_800_000_000_000,
                "end": 1_800_000_059_999,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10.0,
                "turnover": 1005.0,
                "confirm": True,
                "source": "bybit_seed",
            }
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({
                    "version": 2,
                    "symbol": "BTCUSDT",
                    "market": "linear",
                    "candles": {"1": [candle]},
                }, fh)

            old_dir = os.environ.get("CANDLE_SEED_DIR")
            os.environ["CANDLE_SEED_DIR"] = tmp
            try:
                stream = dynamic_collector.MarketStream("BTCUSDT", "linear")
            finally:
                if old_dir is None:
                    os.environ.pop("CANDLE_SEED_DIR", None)
                else:
                    os.environ["CANDLE_SEED_DIR"] = old_dir

        self.assertEqual(len(stream.candles["1"]), 1)
        self.assertEqual(stream.candles["1"][0]["source"], "bybit_seed")
        self.assertEqual(stream.history_source, "bybit_historical_seed+bybit_websocket")


if __name__ == "__main__":
    unittest.main()
