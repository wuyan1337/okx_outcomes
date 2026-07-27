import unittest

from fastapi import HTTPException, Request

from backend import app as app_module


def make_request(peer: str, forwarded: str = "") -> Request:
    headers = []
    if forwarded:
        headers.append((b"x-forwarded-for", forwarded.encode("ascii")))
    return Request({
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/api/status",
        "raw_path": b"/api/status",
        "query_string": b"",
        "headers": headers,
        "client": (peer, 12345),
        "server": ("localhost", 443),
    })


class TrustedProxyRateLimitTests(unittest.TestCase):
    def setUp(self):
        with app_module._login_attempts_lock:
            app_module._login_attempts.clear()

    def tearDown(self):
        with app_module._login_attempts_lock:
            app_module._login_attempts.clear()

    def test_untrusted_peer_cannot_spoof_forwarded_ip(self):
        request = make_request("203.0.113.10", "198.51.100.99")
        self.assertEqual(app_module._request_client_ip(request), "203.0.113.10")

    def test_loopback_proxy_uses_rightmost_untrusted_forwarded_ip(self):
        request = make_request(
            "127.0.0.1",
            "192.0.2.250, 198.51.100.25",
        )
        self.assertEqual(app_module._request_client_ip(request), "198.51.100.25")

    def test_failed_attempts_are_limited_per_resolved_client(self):
        request = make_request("127.0.0.1", "198.51.100.25")
        for _ in range(app_module._MAX_ATTEMPTS):
            app_module._check_rate_limit(request)
            app_module._record_failed_attempt(request)

        with self.assertRaises(HTTPException) as raised:
            app_module._check_rate_limit(request)

        self.assertEqual(raised.exception.status_code, 429)
        other_client = make_request("127.0.0.1", "198.51.100.26")
        app_module._check_rate_limit(other_client)


if __name__ == "__main__":
    unittest.main()
