import unittest
from scan_plus.markets.forex.execution import build_forex_execution_plan


def frames(direction="bullish"):
    return {"15m":{"analysis":{
        "structure":{
            "last_swing_low":{"price":1.098},
            "last_swing_high":{"price":1.104},
        },
        "fibonacci":{"ready":True,"extensions":{"1.618":1.11 if direction=="bullish" else 1.092}}
    }}}


class ForexExecutionTests(unittest.TestCase):
    def test_bullish_plan_uses_cash_risk(self):
        out=build_forex_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish"},
            frames=frames(),price=1.1,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"PLAN")
        self.assertEqual(out["side"],"BUY")
        self.assertAlmostEqual(out["risk"]["risk_cash"],100)
        self.assertFalse(out["execution_policy"]["submit"])

    def test_watch_never_creates_plan(self):
        out=build_forex_execution_plan(
            candidate={"status":"WATCH","direction":"bullish"},
            frames=frames(),price=1.1,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")

    def test_invalid_stop_side_blocks(self):
        bad={"15m":{"analysis":{
            "structure":{"last_swing_low":{"price":1.101}},
            "fibonacci":{"ready":True,"extensions":{"1.618":1.11}}
        }}}
        out=build_forex_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish"},
            frames=bad,price=1.1,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")


if __name__=="__main__":
    unittest.main()


class YahooFXProviderHardeningTests(unittest.TestCase):
    def test_loader_uses_injected_session_and_normalizes_payload(self):
        from unittest.mock import Mock
        from scan_plus.markets.forex.yahoo import YahooFXLoader
        session=Mock()
        response=Mock()
        response.status_code=200
        response.json.return_value={"chart":{"result":[{
            "timestamp":[1704103200],
            "indicators":{"quote":[{"open":[1.1],"high":[1.2],"low":[1.0],"close":[1.15],"volume":[0]}]}
        }]}}
        session.get.return_value=response
        out=YahooFXLoader(session=session).candles("EURUSD","60")
        self.assertEqual(len(out["candles"]),1)
        self.assertEqual(out["symbol"],"EURUSD")
        session.get.assert_called_once()
