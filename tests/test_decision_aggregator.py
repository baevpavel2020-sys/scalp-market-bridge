import unittest
from scan_plus.decision_aggregator import normalize_result, aggregate_results


class DecisionAggregatorTests(unittest.TestCase):
    def test_market_candidate_preserves_direction_and_context(self):
        out=normalize_result({
            "market":"forex","symbol":"EURUSD",
            "candidate":{"status":"CANDIDATE","direction":"bullish",
                         "situation":"session_sweep","reason":"confirmed"},
            "priority":{"ordered":["structure_mtf","elliott"],
                        "weights":{"structure_mtf":1.2,"elliott":1.1}},
        })
        self.assertEqual(out["status"],"CANDIDATE")
        self.assertEqual(out["direction"],"bullish")
        self.assertEqual(out["situation"],"session_sweep")
        self.assertTrue(out["attention_rank"][0] > 0)

    def test_crypto_legacy_result_is_adapted_without_rewriting_strategy(self):
        out=normalize_result({
            "market":"crypto","symbol":"BTCUSDT",
            "trade_state":"BLOCKED","direction":"bearish",
            "block_reasons":["no_entry"],
        })
        self.assertEqual(out["status"],"NO_TRADE")
        self.assertEqual(out["direction"],"bearish")
        self.assertTrue(out["source"]["candidate"]["legacy"])

    def test_errors_are_isolated(self):
        out=aggregate_results([
            {"status":"DATA_ERROR","market":"stocks","symbol":"NVDA","error":"provider"},
            {"status":"OK","market":"commodities","result":{
                "market":"commodities","symbol":"XAUUSD",
                "candidate":{"status":"WATCH","direction":"unknown"},
            }},
        ])
        self.assertEqual(out["error_count"],1)
        self.assertEqual(out["watch_count"],1)
        self.assertEqual(out["candidate_count"],0)

    def test_no_universal_score_or_cross_market_vote(self):
        out=aggregate_results([])
        self.assertTrue(out["policy"]["no_universal_trade_score"])
        self.assertTrue(out["policy"]["no_cross_market_direction_vote"])


if __name__=="__main__":
    unittest.main()
