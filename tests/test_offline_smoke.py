import unittest
from scripts.smoke_scan_plus import run


class OfflineSmokeTest(unittest.TestCase):
    def test_full_four_market_contract(self):
        run()


if __name__ == "__main__":
    unittest.main()
