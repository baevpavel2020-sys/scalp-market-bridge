import unittest
from datetime import datetime, timezone
from scan_plus.core.session_levels import session_high_low


def ms(iso):
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()*1000)


class SessionLevelAuditTests(unittest.TestCase):
    def test_current_incomplete_session_is_not_used_as_reference(self):
        # London in Europe/London: 08:00-17:00 local. At 12:00 the London
        # range is still forming, so the function must not return it.
        candles=[
            {"timestamp_ms":ms("2026-10-01T08:30:00+00:00"),"high":1.20,"low":1.10},
            {"timestamp_ms":ms("2026-10-01T11:30:00+00:00"),"high":1.30,"low":1.05},
        ]
        out=session_high_low(
            candles,market="forex",session="london",
            before_timestamp_ms=ms("2026-10-01T12:00:00+00:00")
        )
        self.assertFalse(out["ready"])

    def test_completed_session_can_be_used(self):
        candles=[
            {"timestamp_ms":ms("2026-10-01T08:30:00+00:00"),"high":1.20,"low":1.10},
            {"timestamp_ms":ms("2026-10-01T15:30:00+00:00"),"high":1.30,"low":1.05},
        ]
        out=session_high_low(
            candles,market="forex",session="london",
            before_timestamp_ms=ms("2026-10-01T18:00:00+00:00")
        )
        self.assertTrue(out["ready"])
        self.assertEqual(out["high"],1.30)
        self.assertEqual(out["low"],1.05)


if __name__=="__main__":
    unittest.main()
