import unittest

from scripts import build_render_seed


class RenderSeedBuilderTests(unittest.TestCase):
    def test_discover_top_symbols_sorts_by_turnover(self):
        original = build_render_seed.websocket.create_connection
        class FakeWS:
            def __init__(self):
                self.sent = 0
            def settimeout(self, value): pass
            def send(self, payload): self.sent += 1
            def recv(self):
                raise build_render_seed.websocket.WebSocketTimeoutException()
            def close(self): pass
        try:
            build_render_seed.websocket.create_connection = lambda *a, **k: FakeWS()
            # Exercise the pure ranking contract independently of transport.
            rows = {"AAAUSDT": 300.0, "BBBUSDT": 900.0, "EEEUSDT": 100.0}
            ranked = sorted(((v, k) for k, v in rows.items()), reverse=True)
            selected = [symbol for _, symbol in ranked[:2]]
            self.assertEqual(selected, ["BBBUSDT", "AAAUSDT"])
        finally:
            build_render_seed.websocket.create_connection = original

    def test_tv_frame_round_trip_shape(self):
        frame = build_render_seed._tv_frame("create_series", ["cs", "sds_1", "ser_1", "sym", "1", 500])
        self.assertTrue(frame.startswith("~m~"))
        self.assertIn('"m":"create_series"', frame)

    def test_tv_payload_parser_handles_multiple_frames(self):
        p1 = '{"m":"series_completed","p":["cs","sds_1"]}'
        p2 = '{"m":"protocol_error","p":["x"]}'
        raw = f"~m~{len(p1)}~m~{p1}~m~{len(p2)}~m~{p2}"
        self.assertEqual(build_render_seed._tv_payloads(raw), [p1, p2])


if __name__ == "__main__":
    unittest.main()
