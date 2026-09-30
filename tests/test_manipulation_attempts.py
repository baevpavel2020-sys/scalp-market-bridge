import unittest
from scan_plus.markets.crypto.attempt_guard import ManipulationAttemptGuard


class AttemptGuardTests(unittest.TestCase):
    def test_max_two_attempts_per_event(self):
        g=ManipulationAttemptGuard()
        self.assertTrue(g.register_attempt("pump-1",new_trigger=True).allowed)
        second=g.register_attempt("pump-1",new_trigger=True)
        self.assertTrue(second.allowed)
        self.assertEqual(second.state,"LAST_ATTEMPT")
        third=g.register_attempt("pump-1",new_trigger=True)
        self.assertFalse(third.allowed)
        self.assertEqual(third.state,"EVENT_LOCKED")

    def test_second_entry_requires_new_trigger(self):
        g=ManipulationAttemptGuard()
        g.register_attempt("pump-1",new_trigger=True)
        denied=g.register_attempt("pump-1",new_trigger=False)
        self.assertFalse(denied.allowed)
        self.assertEqual(denied.state,"WAIT_NEW_TRIGGER")


if __name__=="__main__":
    unittest.main()
