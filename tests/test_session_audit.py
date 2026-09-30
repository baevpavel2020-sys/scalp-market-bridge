import unittest
from datetime import datetime, timezone
from scan_plus.core.session_levels import session_high_low
from scan_plus.core.sessions import active_sessions


def ts(iso):
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()*1000)


class SessionAuditTests(unittest.TestCase):
    def test_session_high_low_uses_latest_session_not_all_history(self):
        candles=[
            {"timestamp_ms":ts("2026-09-29T09:00:00"),"high":200,"low":190},
            {"timestamp_ms":ts("2026-09-30T09:00:00"),"high":150,"low":140},
        ]
        out=session_high_low(candles,market="forex",session="london")
        self.assertEqual(out["local_date"],"2026-09-30")
        self.assertEqual(out["high"],150)
        self.assertEqual(out["low"],140)

    def test_weekend_has_no_fx_session(self):
        saturday=ts("2026-10-03T12:00:00")
        self.assertEqual(active_sessions("forex",saturday),[])


if __name__=="__main__":
    unittest.main()
