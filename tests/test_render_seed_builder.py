import json
import os
import tempfile
import unittest

from scripts import build_render_seed


class RenderSeedBuilderTests(unittest.TestCase):
    def test_discover_top_symbols_sorts_by_turnover_and_filters(self):
        original = build_render_seed._request_tickers
        try:
            build_render_seed._request_tickers = lambda: [
                {"symbol": "AAAUSDT", "status": "Trading", "turnover24h": "300"},
                {"symbol": "BBBUSDT", "status": "Trading", "turnover24h": "900"},
                {"symbol": "CCCUSDC", "status": "Trading", "turnover24h": "9999"},
                {"symbol": "DDDUSDT", "status": "PreLaunch", "turnover24h": "9999"},
                {"symbol": "EEEUSDT", "status": "Trading", "turnover24h": "100"},
            ]
            symbols, ranked = build_render_seed.discover_top_symbols(2)
        finally:
            build_render_seed._request_tickers = original
        self.assertEqual(symbols, ["BBBUSDT", "AAAUSDT"])
        self.assertEqual(ranked[0][1], "BBBUSDT")

    def test_build_one_uses_all_scan_timeframes(self):
        original = build_render_seed.fetch_candles
        try:
            build_render_seed.fetch_candles = lambda symbol, category, interval, bars: [{
                "start": 1, "end": 2, "open": 1, "high": 2, "low": 1, "close": 1.5,
                "volume": 10, "turnover": 15, "confirm": True, "source": "bybit_seed"
            }]
            payload = build_render_seed.build_one("AAAUSDT", 500)
        finally:
            build_render_seed.fetch_candles = original
        self.assertEqual(payload["symbol"], "AAAUSDT")
        self.assertEqual(set(payload["candles"]), set(build_render_seed.INTERVALS))
        self.assertTrue(all(payload["candles"][tf] for tf in build_render_seed.INTERVALS))


if __name__ == "__main__":
    unittest.main()
