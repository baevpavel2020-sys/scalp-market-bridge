import unittest
from scan_plus.markets.crypto.event_memory import CausalEventMemory


class EventMemoryTests(unittest.TestCase):
    def test_same_symbol_builds_ordered_sequence(self):
        m=CausalEventMemory(max_age_seconds=100)
        for i,stage in enumerate(("abnormal_pump","exhaustion","failed_acceptance",
                                  "structure_break","bearish_displacement","failed_retest"),1):
            out=m.update("BTCUSDT",i*10,{stage:True})
        self.assertTrue(out["sequence_complete"])
        self.assertTrue(out["causal_order_confirmed"])

    def test_out_of_order_sequence_never_confirms(self):
        m=CausalEventMemory(max_age_seconds=100)
        m.update("BTCUSDT",30,{"structure_break":True})
        m.update("BTCUSDT",40,{"abnormal_pump":True})
        out=m.update("BTCUSDT",50,{"exhaustion":True})
        self.assertFalse(out["sequence_complete"])

    def test_symbols_are_isolated(self):
        m=CausalEventMemory(max_age_seconds=100)
        m.update("BTCUSDT",10,{"abnormal_pump":True})
        out=m.update("ETHUSDT",20,{"exhaustion":True})
        self.assertFalse(out["sequence_complete"])
        self.assertEqual(set(out["anchors"]),{"exhaustion"})

    def test_old_events_expire(self):
        m=CausalEventMemory(max_age_seconds=30)
        m.update("BTCUSDT",10,{"abnormal_pump":True})
        out=m.update("BTCUSDT",50,{"exhaustion":True})
        self.assertNotIn("abnormal_pump",out["anchors"])


if __name__=="__main__":
    unittest.main()
