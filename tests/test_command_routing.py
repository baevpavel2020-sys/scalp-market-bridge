import unittest

from scan_plus.multimarket import DEFAULT_MARKETS, parse_scan_command


class CommandRoutingTests(unittest.TestCase):
    def test_plain_scan_targets_all_markets(self):
        parsed = parse_scan_command("Скан")
        self.assertEqual(parsed["markets"], DEFAULT_MARKETS)
        self.assertEqual(parsed["symbols"], [])

    def test_market_commands(self):
        self.assertEqual(parse_scan_command("Скан крипта")["markets"], ("crypto",))
        self.assertEqual(parse_scan_command("Скан форекс")["markets"], ("forex",))
        self.assertEqual(parse_scan_command("Скан металлы")["markets"], ("commodities",))
        self.assertEqual(parse_scan_command("Скан нефть")["markets"], ("commodities",))
        self.assertEqual(parse_scan_command("Скан какао")["markets"], ("commodities",))

    def test_explicit_symbols_survive_routing(self):
        parsed = parse_scan_command("Скан крипта ENAUSDT HYPEUSDT")
        self.assertEqual(parsed["markets"], ("crypto",))
        self.assertEqual(parsed["symbols"], ["ENAUSDT", "HYPEUSDT"])


if __name__ == "__main__":
    unittest.main()
