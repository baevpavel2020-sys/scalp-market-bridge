import unittest
from scan_plus.core.data_contract import normalize_candles
from scan_plus.decision_aggregator import normalize_result, aggregate_results
from scan_plus.operator_view import build_operator_view
from scan_plus.markets.forex.yahoo import _aggregate_4h as fx_4h
from scan_plus.markets.commodities.yahoo import _aggregate_4h as com_4h


def h(ts):
    return {"timestamp_ms":ts,"open":1,"high":2,"low":0.5,"close":1.5,"volume":1}


class EndToEndAuditTests(unittest.TestCase):
    def test_incomplete_4h_block_is_not_manufactured(self):
        rows=[h(i*3600000) for i in (0,1,2)]
        self.assertEqual(fx_4h(rows),[])
        self.assertEqual(com_4h(rows),[])

    def test_complete_4h_block_is_aggregated(self):
        rows=[h(i*3600000) for i in (0,1,2,3)]
        out=fx_4h(rows)
        self.assertEqual(len(out),1)
        self.assertEqual(out[0]["source_bars"],4)
        self.assertTrue(out[0]["derived_4h"])

    def test_invalid_market_cannot_reach_operator_view(self):
        decision=aggregate_results([{
            "status":"OK","market":"mars","symbol":"ABC",
            "candidate":{"status":"CANDIDATE","direction":"bullish"}
        }])
        self.assertEqual(decision["count"],0)
        self.assertEqual(decision["error_count"],1)

    def test_mtf_gate_reason_survives_to_operator_view(self):
        result={
            "market":"forex","symbol":"EURUSD",
            "candidate":{"status":"CANDIDATE","direction":"bullish","reason":"ok",
                "mtf_state":{"regime":"countertrend_correction","reversal_confirmed":False}},
        }
        normalized=normalize_result(result)
        self.assertEqual(normalized["status"],"WATCH")
        self.assertTrue(normalized["mtf_gate_blocked"])
        view=build_operator_view({"count":1,"results":[normalized],"errors":[]})
        self.assertTrue(view["watchlist"][0]["mtf_gate_blocked"])
        self.assertEqual(view["watchlist"][0]["mtf_state"]["regime"],"countertrend_correction")

    def test_data_error_candidate_does_not_become_watch(self):
        decision=aggregate_results([{
            "status":"OK","market":"crypto","symbol":"BTCUSDT",
            "candidate":{"status":"DATA_ERROR","direction":"unknown"}
        }])
        self.assertEqual(decision["count"],0)
        self.assertEqual(decision["error_count"],1)


if __name__=="__main__":
    unittest.main()
