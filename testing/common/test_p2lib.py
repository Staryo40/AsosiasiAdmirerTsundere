"""Tests for measurement correctness; no Docker or active credentials required."""
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from p2lib import assert_load, load, metrics, percentile


class MeasurementTests(unittest.TestCase):
    def test_success_percentiles_exclude_fast_rejections(self):
        rows = [{"status": 200, "latency_ms": 400}] + [{"status": 429, "latency_ms": 1}] * 99
        result = metrics(rows, 60)
        self.assertEqual(result["successful_latency"]["p95_ms"], 400)
        self.assertEqual(result["all_latency"]["p95_ms"], 1)
        self.assertEqual(result["controlled_429"], 99)
        self.assertEqual(result["unexpected_error_rate_excluding_429"], 0)

    def test_all_rejections_cannot_pass(self):
        result = metrics([{"status": 429, "latency_ms": 1}], 60)
        self.assertIsNone(result["unexpected_error_rate_excluding_429"])

    def test_transport_errors_count_as_unexpected(self):
        result = metrics([{"status": 0, "latency_ms": 5000}, {"status": 200, "latency_ms": 10},
                          {"status": 429, "latency_ms": 1}], 60)
        self.assertEqual(result["unexpected_error_rate_excluding_429"], .5)

    def test_percentile_boundary(self):
        self.assertEqual(percentile(list(range(1, 101)), .95), 95)
        self.assertIsNone(percentile([], .95))

    def test_slow_late_error_is_not_hidden_by_window_end(self):
        result = metrics([{"status": 200, "latency_ms": 10, "completed_in_window": True},
                          {"status": 0, "latency_ms": 5000, "completed_in_window": False}], 60)
        self.assertEqual(result["unexpected_error_rate_excluding_429"], .5)
        self.assertEqual(result["throughput_rps"], 1 / 60)
        self.assertEqual(result["all_latency"]["p95_ms"], 5000)

    def test_http_connections_are_real_and_warmup_is_excluded(self):
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                body = b'{"items":[]}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        class FakeClient:
            api = f"http://127.0.0.1:{server.server_port}"

            def token(self):
                return "fixture-token"

        try:
            with tempfile.TemporaryDirectory() as directory:
                result = load(FakeClient(), Path(directory), 3, 1.1, .1)
                self.assertEqual(result["minimum_observed_sockets"], 3)
                self.assertGreater(result["metrics"]["successful_requests"], 0)
                rows = [json.loads(line) for line in (Path(directory) / "requests.jsonl").read_text().splitlines()]
                self.assertTrue(all(0 <= row["elapsed_seconds"] < 1.1 for row in rows))
                self.assertEqual(len(result["by_group"]), 3)
                with self.assertRaises(AssertionError):
                    assert_load(result, latency=False, sustained=True)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
