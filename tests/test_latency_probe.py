import asyncio
import json
import socket
import unittest
from unittest import mock

from backend.services import latency_probe


class FakeResponse:
    def __init__(self, status=200, payload=None, body=None):
        self.status = status
        if body is not None:
            self._body = body
        else:
            self._body = json.dumps(payload).encode("utf-8")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def read(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        if not self._responses:
            raise AssertionError("unexpected probe request")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def successful_node_result(node_id):
    return {
        "node_id": node_id,
        "label": node_id,
        "location": "Test",
        "route": "direct",
        "status": "ok",
        "okx_ms": 12,
        "okx": {
            "scope": "connectivity_only",
            "endpoint_path": "/api/v5/public/time",
            "cold_ms": 20.0,
            "warm": {
                "requested": 1,
                "attempted": 1,
                "successes": 1,
                "failures": 0,
                "success_rate": 1.0,
                "samples_ms": [12.0],
                "median_ms": 12.0,
                "min_ms": 12.0,
                "max_ms": 12.0,
                "jitter_ms": 0.0,
                "error_counts": {},
            },
            "status": "ok",
            "errors": [],
        },
    }


def contains_key(value, key):
    if isinstance(value, dict):
        return key in value or any(contains_key(item, key) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(contains_key(item, key) for item in value)
    return False


class LatencyProbeInputTests(unittest.TestCase):
    def test_region_normalisation_and_validation(self):
        self.assertEqual(latency_probe.normalise_region(" au "), "AU")
        self.assertEqual(latency_probe.normalise_region("cn"), "CN")
        self.assertEqual(latency_probe.normalise_region(""), "AU")

        with self.assertRaisesRegex(ValueError, "AU or CN"):
            latency_probe.normalise_region("US")

    def test_sample_normalisation_and_bounds(self):
        self.assertEqual(latency_probe.normalise_samples("5"), 5)
        self.assertEqual(latency_probe.normalise_samples(1), 1)
        self.assertEqual(
            latency_probe.normalise_samples(latency_probe.MAX_WARM_SAMPLES),
            latency_probe.MAX_WARM_SAMPLES,
        )

        for invalid in (0, latency_probe.MAX_WARM_SAMPLES + 1, "many", None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                latency_probe.normalise_samples(invalid)

    def test_okx_target_tracks_region_and_real_market_asset(self):
        au_target = latency_probe._okx_target("AU")
        self.assertEqual(au_target["url"], "https://us.okx.com/api/v5/public/time")
        self.assertIsNone(au_target["params"])
        self.assertEqual(au_target["scope"], "connectivity_only")
        self.assertEqual(au_target["validator"], "okx_time")

        cn_target = latency_probe._okx_target("CN", "yes-asset-1")
        self.assertEqual(cn_target["url"], "https://www.okx.com/api/v5/market/pm-books")
        self.assertEqual(cn_target["params"], {"instId": "yes-asset-1", "sz": "10"})
        self.assertEqual(
            cn_target["signed_path"],
            "/api/v5/market/pm-books?instId=yes-asset-1&sz=10",
        )
        self.assertEqual(cn_target["scope"], "market_read")
        self.assertEqual(cn_target["validator"], "okx_book")

    def test_poly_target_is_shared_direct_book_or_connectivity(self):
        time_target = latency_probe._poly_target()
        self.assertEqual(time_target["url"], "https://clob.polymarket.com/time")
        self.assertIsNone(time_target["params"])
        self.assertEqual(time_target["scope"], "connectivity_only")

        book_target = latency_probe._poly_target("poly-token-1")
        self.assertEqual(book_target["url"], "https://clob.polymarket.com/book")
        self.assertEqual(book_target["params"], {"token_id": "poly-token-1"})
        self.assertEqual(book_target["scope"], "market_read")
        self.assertEqual(book_target["validator"], "poly_book")


class LatencyPayloadValidationTests(unittest.TestCase):
    def test_valid_okx_and_poly_payloads(self):
        latency_probe._validate_payload(
            "okx_time",
            b'{"code":"0","data":[{"ts":"123"}]}',
        )
        latency_probe._validate_payload(
            "okx_book",
            b'{"code":"0","data":[{"bids":[],"asks":[]}]}',
        )
        latency_probe._validate_payload(
            "poly_book",
            b'{"bids":[],"asks":[]}',
        )
        latency_probe._validate_payload("poly_time", b"1234567890")

    def test_invalid_json_is_classified(self):
        with self.assertRaises(latency_probe.ProbeFailure) as caught:
            latency_probe._validate_payload("okx_time", b"not-json")

        self.assertEqual(caught.exception.error_type, "invalid_json")

    def test_okx_error_and_empty_data_are_rejected(self):
        with self.assertRaises(latency_probe.ProbeFailure) as caught:
            latency_probe._validate_payload(
                "okx_book",
                b'{"code":"51000","msg":"bad asset","data":[]}',
            )
        self.assertEqual(caught.exception.error_type, "upstream_api")
        self.assertEqual(caught.exception.upstream_code, "51000")

        with self.assertRaises(latency_probe.ProbeFailure) as caught:
            latency_probe._validate_payload(
                "okx_book",
                b'{"code":"0","data":[]}',
            )
        self.assertEqual(caught.exception.error_type, "empty_data")

    def test_invalid_poly_book_shape_is_rejected(self):
        with self.assertRaises(latency_probe.ProbeFailure) as caught:
            latency_probe._validate_payload("poly_book", b'{"bids":[]}')

        self.assertEqual(caught.exception.error_type, "invalid_response")

    def test_poly_time_rejects_non_timestamp_success_payloads(self):
        for body in (b'{"error":"upstream unavailable"}', b"true", b"-1"):
            with self.subTest(body=body), self.assertRaises(
                latency_probe.ProbeFailure
            ) as caught:
                latency_probe._validate_payload("poly_time", body)
            self.assertEqual(caught.exception.error_type, "invalid_response")


class LatencyRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_once_validates_body_and_uses_perf_counter(self):
        session = FakeSession([
            FakeResponse(payload={"code": "0", "data": [{"ts": "123"}]})
        ])
        target = latency_probe._okx_target("AU")

        with mock.patch.object(
            latency_probe.time,
            "perf_counter_ns",
            side_effect=[1_000_000, 3_500_000],
        ):
            elapsed = await latency_probe._request_once(session, target)

        self.assertEqual(elapsed, 2.5)
        self.assertEqual(len(session.requests), 1)
        url, options = session.requests[0]
        self.assertEqual(url, target["url"])
        self.assertIs(options["timeout"], latency_probe.PROBE_TIMEOUT)
        self.assertIsNone(options["params"])

    async def test_request_once_classifies_http_status(self):
        session = FakeSession([FakeResponse(status=503, payload={"error": "down"})])

        with self.assertRaises(latency_probe.ProbeFailure) as caught:
            await latency_probe._request_once(session, latency_probe._poly_target())

        self.assertEqual(caught.exception.error_type, "http_status")
        self.assertEqual(caught.exception.http_status, 503)

    async def test_market_request_builds_fresh_authenticated_headers(self):
        session = FakeSession([
            FakeResponse(payload={
                "code": "0",
                "data": [{"bids": [], "asks": []}],
            }),
        ])
        target = latency_probe._okx_target("AU", "yes-a")
        header_factory = mock.MagicMock(return_value={"OK-ACCESS-KEY": "test"})

        await latency_probe._request_once(session, target, header_factory)

        header_factory.assert_called_once_with(target)
        self.assertEqual(
            session.requests[0][1]["headers"],
            {"OK-ACCESS-KEY": "test"},
        )

    def test_dns_failure_is_not_collapsed_into_generic_connect(self):
        failure = latency_probe._classify_exception(
            socket.gaierror(socket.EAI_NONAME, "name not known")
        )

        self.assertEqual(failure.error_type, "dns")


class LatencySamplingTests(unittest.IsolatedAsyncioTestCase):
    def test_warm_statistics_include_failures_and_jitter(self):
        stats = latency_probe._warm_stats(5, [10.0, 30.0, 20.0, 40.0], 1)

        self.assertEqual(stats["attempted"], 5)
        self.assertEqual(stats["successes"], 4)
        self.assertEqual(stats["failures"], 1)
        self.assertEqual(stats["success_rate"], 0.8)
        self.assertEqual(stats["median_ms"], 25.0)
        self.assertEqual(stats["min_ms"], 10.0)
        self.assertEqual(stats["max_ms"], 40.0)
        self.assertEqual(stats["jitter_ms"], 30.0)

    async def test_cold_sample_is_excluded_from_warm_median(self):
        timeout = latency_probe.ProbeFailure("timeout", "request timed out")
        request_once = mock.AsyncMock(
            side_effect=[1000.0, 10.0, timeout, 30.0, 20.0, 40.0]
        )

        with mock.patch.object(latency_probe, "_request_once", request_once):
            result = await latency_probe._benchmark_session(
                object(), latency_probe._okx_target("AU"), 5
            )

        self.assertEqual(request_once.await_count, 6)
        self.assertEqual(result["cold_ms"], 1000.0)
        self.assertEqual(result["warm"]["samples_ms"], [10.0, 30.0, 20.0, 40.0])
        self.assertEqual(result["warm"]["median_ms"], 25.0)
        self.assertEqual(result["warm"]["error_counts"], {"timeout": 1})
        self.assertEqual(result["status"], "degraded")

    async def test_failed_cold_request_short_circuits_warm_samples(self):
        request_once = mock.AsyncMock(
            side_effect=latency_probe.ProbeFailure("proxy_connect", "proxy failed")
        )

        with mock.patch.object(latency_probe, "_request_once", request_once):
            result = await latency_probe._benchmark_session(
                object(), latency_probe._okx_target("AU"), 5
            )

        self.assertEqual(request_once.await_count, 1)
        self.assertIsNone(result["cold_ms"])
        self.assertEqual(result["warm"]["attempted"], 0)
        self.assertEqual(result["warm"]["skipped"], 5)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["errors"][0]["phase"], "cold")
        self.assertEqual(result["errors"][0]["type"], "proxy_connect")


class LatencyNodeBenchmarkTests(unittest.IsolatedAsyncioTestCase):
    async def test_node_benchmark_preserves_legacy_median_and_market_scope(self):
        session_context = mock.MagicMock()
        session = object()
        session_context.__aenter__ = mock.AsyncMock(return_value=session)
        session_context.__aexit__ = mock.AsyncMock(return_value=False)
        client_session = mock.MagicMock(return_value=session_context)
        benchmark = mock.AsyncMock(return_value={
            "scope": "market_read",
            "endpoint_path": "/api/v5/market/pm-books",
            "cold_ms": 100.0,
            "warm": {"median_ms": 12.6},
            "status": "ok",
            "errors": [],
        })
        header_factory = mock.MagicMock(return_value={"auth": "header"})

        with (
            mock.patch.object(latency_probe.aiohttp, "ClientSession", client_session),
            mock.patch.object(latency_probe, "_benchmark_session", benchmark),
        ):
            result = await latency_probe.benchmark_proxy_node(
                "DIRECT",
                {"label": "Direct", "location": "Tokyo", "host": None, "port": None},
                region="CN",
                samples=3,
                yes_asset_id="yes-asset-1",
                header_factory=header_factory,
            )

        self.assertEqual(result["okx_ms"], 13)
        self.assertEqual(result["route"], "direct")
        self.assertEqual(result["location"], "Tokyo")
        target = benchmark.await_args.args[1]
        self.assertEqual(target["url"], "https://www.okx.com/api/v5/market/pm-books")
        self.assertEqual(target["params"]["instId"], "yes-asset-1")
        self.assertEqual(benchmark.await_args.args[2], 3)
        self.assertIs(benchmark.await_args.args[3], header_factory)
        client_session.assert_called_once_with(
            connector=None,
            timeout=latency_probe.PROBE_TIMEOUT,
            trust_env=False,
        )

    async def test_proxy_setup_failure_returns_a_failed_node(self):
        node = {
            "label": "Broken Proxy",
            "location": "Test",
            "host": "127.0.0.1",
            "port": 9111,
        }

        with mock.patch.object(
            latency_probe.ProxyConnector,
            "from_url",
            side_effect=OSError("connection refused"),
        ):
            result = await latency_probe.benchmark_proxy_node(
                "BROKEN", node, samples=3
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["okx_ms"], -1)
        self.assertEqual(result["okx"]["warm"]["attempted"], 0)
        self.assertEqual(result["okx"]["errors"][0]["phase"], "setup")
        self.assertEqual(result["okx"]["errors"][0]["type"], "connect")

    async def test_legacy_scalar_does_not_fall_back_to_cold_when_warm_failed(self):
        session_context = mock.MagicMock()
        session_context.__aenter__ = mock.AsyncMock(return_value=object())
        session_context.__aexit__ = mock.AsyncMock(return_value=False)
        failed_warm = {
            "scope": "connectivity_only",
            "endpoint_path": "/api/v5/public/time",
            "cold_ms": 25.0,
            "warm": {"median_ms": None},
            "status": "failed",
            "errors": [{"type": "timeout", "phase": "warm"}],
        }

        with (
            mock.patch.object(
                latency_probe.aiohttp,
                "ClientSession",
                return_value=session_context,
            ),
            mock.patch.object(
                latency_probe,
                "_benchmark_session",
                new=mock.AsyncMock(return_value=failed_warm),
            ),
        ):
            result = await latency_probe.benchmark_proxy_node(
                "DIRECT",
                {"label": "Direct", "location": "Tokyo", "host": None},
                samples=1,
            )

        self.assertEqual(result["okx_ms"], -1)
        self.assertEqual(result["okx"]["cold_ms"], 25.0)

    async def test_failed_node_does_not_expose_a_partial_warm_median(self):
        session_context = mock.MagicMock()
        session_context.__aenter__ = mock.AsyncMock(return_value=object())
        session_context.__aexit__ = mock.AsyncMock(return_value=False)
        failed_partial_warm = {
            "scope": "market_read",
            "endpoint_path": "/api/v5/market/pm-books",
            "cold_ms": 25.0,
            "warm": {"median_ms": 4.0, "successes": 2, "requested": 5},
            "status": "failed",
            "errors": [{"type": "timeout", "phase": "warm"}],
        }

        with (
            mock.patch.object(
                latency_probe.aiohttp,
                "ClientSession",
                return_value=session_context,
            ),
            mock.patch.object(
                latency_probe,
                "_benchmark_session",
                new=mock.AsyncMock(return_value=failed_partial_warm),
            ),
        ):
            result = await latency_probe.benchmark_proxy_node(
                "DIRECT",
                {"label": "Direct", "location": "Tokyo", "host": None},
                samples=5,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["okx_ms"], -1)
        self.assertEqual(result["okx"]["warm"]["median_ms"], 4.0)


class LatencyBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_keeps_poly_top_level_once_and_has_no_total(self):
        nodes = {
            "DIRECT": {"label": "Direct", "host": None, "port": None},
            "PROXY": {"label": "Proxy", "host": "127.0.0.1", "port": 9111},
        }
        poly_result = {
            "shared": True,
            "route": "direct",
            "scope": "market_read",
            "endpoint_path": "/book",
            "cold_ms": 20.0,
            "warm": {"median_ms": 10.0},
            "status": "ok",
            "errors": [],
        }
        poly_probe = mock.AsyncMock(return_value=poly_result)
        header_factory = mock.MagicMock(return_value={"auth": "header"})

        async def node_probe(node_id, _node, **_kwargs):
            return successful_node_result(node_id)

        with (
            mock.patch.object(latency_probe, "benchmark_poly_direct", poly_probe),
            mock.patch.object(latency_probe, "benchmark_proxy_node", side_effect=node_probe) as node_mock,
        ):
            result = await latency_probe.benchmark_proxy_nodes(
                nodes,
                region="CN",
                samples=3,
                yes_asset_id="yes-1",
                poly_token_id="poly-1",
                concurrency=2,
                header_factory=header_factory,
            )

        poly_probe.assert_awaited_once_with(samples=3, poly_token_id="poly-1")
        self.assertEqual(node_mock.await_count, 2)
        for call in node_mock.await_args_list:
            self.assertEqual(call.kwargs["region"], "CN")
            self.assertEqual(call.kwargs["samples"], 3)
            self.assertEqual(call.kwargs["yes_asset_id"], "yes-1")
            self.assertIs(call.kwargs["header_factory"], header_factory)
        self.assertIs(result["poly_direct"], poly_result)
        self.assertEqual(result["region"], "CN")
        self.assertEqual(result["okx_base_url"], "https://www.okx.com")
        self.assertEqual(set(result["latencies"]), set(nodes))
        self.assertEqual(result["execution_path"]["status"], "not_measured")
        self.assertFalse(contains_key(result, "total_ms"))

    async def test_node_failures_are_isolated_and_concurrency_is_bounded(self):
        nodes = {
            name: {"label": name, "host": None, "port": None}
            for name in ("A", "BAD", "B", "C")
        }
        active = 0
        maximum_active = 0
        first_pair_started = asyncio.Event()

        async def node_probe(node_id, _node, **_kwargs):
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            if active >= 2:
                first_pair_started.set()
            try:
                await asyncio.wait_for(first_pair_started.wait(), timeout=0.5)
                await asyncio.sleep(0)
                if node_id == "BAD":
                    failed = successful_node_result(node_id)
                    failed["status"] = "failed"
                    failed["okx_ms"] = -1
                    failed["okx"]["status"] = "failed"
                    return failed
                return successful_node_result(node_id)
            finally:
                active -= 1

        with (
            mock.patch.object(
                latency_probe,
                "benchmark_poly_direct",
                new=mock.AsyncMock(return_value={"shared": True, "route": "direct"}),
            ),
            mock.patch.object(latency_probe, "benchmark_proxy_node", side_effect=node_probe),
        ):
            result = await latency_probe.benchmark_proxy_nodes(
                nodes,
                samples=1,
                concurrency=2,
            )

        self.assertEqual(maximum_active, 2)
        self.assertEqual(set(result["latencies"]), set(nodes))
        self.assertEqual(result["latencies"]["BAD"]["status"], "failed")
        self.assertTrue(all(
            result["latencies"][node_id]["status"] == "ok"
            for node_id in ("A", "B", "C")
        ))


if __name__ == "__main__":
    unittest.main()
