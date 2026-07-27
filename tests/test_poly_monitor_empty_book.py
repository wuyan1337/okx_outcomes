import asyncio
import unittest
from unittest import mock

from backend.services.poly_monitor import PolyMonitor


class _Response:
    def __init__(self, payload=None, status=200, error=None):
        self.payload = payload
        self.status = status
        self.error = error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        if self.error:
            raise self.error
        return self.payload


class _Session:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *args, **kwargs):
        if self.error:
            raise self.error
        return self.response


class PolyMonitorEmptyBookTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_empty_rest_book_returns_zero_bid(self):
        monitor = PolyMonitor(object(), "empty-rest")
        session = _Session(_Response({"bids": [], "asks": []}))
        with mock.patch(
            "backend.services.poly_monitor.aiohttp.ClientSession",
            return_value=session,
        ):
            bid, bids = await monitor._get_poly_best_bid_with_depth("token")

        self.assertEqual(bid, 0.0)
        self.assertEqual(bids, [])

    async def test_empty_book_cannot_be_used_as_initial_reference(self):
        monitor = PolyMonitor(object(), "empty-reference")
        monitor._get_poly_best_bid = mock.AsyncMock(return_value=0.0)

        result = await monitor.start("token", reference_price=0)

        self.assertFalse(result["ok"])
        self.assertFalse(monitor.running)

    async def test_failed_or_malformed_rest_book_is_ignored(self):
        monitor = PolyMonitor(object(), "failed-rest")
        cases = (
            _Session(error=OSError("network failed")),
            _Session(_Response({"asks": []})),
            _Session(_Response({"bids": "not-a-list"})),
            _Session(_Response(error=ValueError("invalid json"))),
        )
        for session in cases:
            with self.subTest(session=session), mock.patch(
                "backend.services.poly_monitor.aiohttp.ClientSession",
                return_value=session,
            ):
                bid, bids = await monitor._get_poly_best_bid_with_depth("token")
                self.assertIsNone(bid)
                self.assertEqual(bids, [])

    async def test_successful_empty_ws_book_reaches_risk_check_as_zero(self):
        monitor = PolyMonitor(object(), "empty-ws")
        monitor._check_price = mock.AsyncMock()

        with mock.patch(
            "backend.services.poly_monitor.market_event"
        ) as market_log:
            observed = await monitor._observe_book_payload(
                {"bids": [], "asks": [], "timestamp": "1700000000000"},
                "poly_ws",
                received_wall_ns=1_700_000_000_123_456_789,
                received_monotonic_ns=987_654_321,
            )

        self.assertTrue(observed)
        self.assertEqual(monitor.state.last_poly_bid, 0.0)
        monitor._check_price.assert_awaited_once_with(0.0, [], "poly_ws")
        trace = market_log.call_args
        self.assertEqual(trace.args, ("polymarket", "monitor.book"))
        self.assertEqual(trace.kwargs["source_timestamp"], "1700000000000")
        self.assertEqual(
            trace.kwargs["received_wall_ns"], 1_700_000_000_123_456_789
        )
        self.assertEqual(trace.kwargs["received_monotonic_ns"], 987_654_321)

    async def test_zero_bid_triggers_drop_against_positive_reference(self):
        monitor = PolyMonitor(object(), "empty-drop")
        monitor.state.reference_price = 0.55
        monitor.state.enabled = True
        monitor._running = True
        reasons = []

        async def record_trigger(reason, generation=None):
            reasons.append(reason)

        monitor._trigger = record_trigger
        await monitor._check_price(0.0, [], "empty_book")
        for _ in range(3):
            await asyncio.sleep(0)

        self.assertEqual(len(reasons), 1)
        self.assertIn("DROPPED", reasons[0])
        self.assertIn("empty_book", reasons[0])

    async def test_malformed_ws_book_does_not_reach_risk_check(self):
        monitor = PolyMonitor(object(), "malformed-ws")
        monitor._check_price = mock.AsyncMock()

        observed = await monitor._observe_book_payload({"asks": []}, "poly_ws")

        self.assertFalse(observed)
        monitor._check_price.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
