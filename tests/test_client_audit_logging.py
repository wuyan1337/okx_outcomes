import asyncio
import json
import logging
import unittest
from unittest import mock

import aiohttp

from backend.core import client as client_module


class FakeResponse:
    def __init__(self, payload=None, *, status=200, json_error=None):
        self.payload = payload
        self.status = status
        self.json_error = json_error
        self.content_type = "application/json"
        self.headers = {"Content-Length": "27"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakeSession:
    def __init__(self, *, get_results=None, post_results=None):
        self.get_results = list(get_results or [])
        self.post_results = list(post_results or [])
        self.requests = []
        self.closed = False

    @staticmethod
    def _take(results):
        if not results:
            raise AssertionError("unexpected HTTP request")
        result = results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def get(self, url, **kwargs):
        self.requests.append(("GET", url, kwargs))
        return self._take(self.get_results)

    def post(self, url, **kwargs):
        self.requests.append(("POST", url, kwargs))
        return self._take(self.post_results)


class ClientAuditLoggingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = client_module.OKXOutcomesClient(
            "api-key-secret",
            "hmac-secret",
            "passphrase-secret",
            "",
            base_url="https://www.okx.com",
            proxy_node=None,
        )
        self.client._account_id = "account-1"
        self.events = []
        self.audit_patch = mock.patch.object(
            client_module,
            "audit_event",
            side_effect=lambda event, **fields: self.events.append((event, fields)),
        )
        self.audit_patch.start()

    def tearDown(self):
        self.audit_patch.stop()

    def test_retired_proxy_node_is_rejected_before_network_use(self):
        with self.assertRaisesRegex(ValueError, "refusing automatic Direct fallback"):
            client_module.OKXOutcomesClient(
                "api-key-secret",
                "hmac-secret",
                "passphrase-secret",
                "",
                proxy_node="JP5",
            )

    def _events(self, name):
        return [fields for event, fields in self.events if event == name]

    async def test_get_success_records_only_query_keys_and_ack(self):
        session = FakeSession(
            get_results=[FakeResponse({"code": "0", "data": {"list": []}})]
        )
        self.client._get_session = mock.AsyncMock(return_value=session)

        result = await self.client._get(
            "/api/v5/predictions/orders",
            {"cursor": "do-not-log-this-value", "limit": "50"},
        )

        self.assertEqual(result["code"], "0")
        request = self._events("okx.http.request")[0]
        response = self._events("okx.http.response")[0]
        self.assertEqual(request["query"]["keys"], ["cursor", "limit"])
        self.assertNotIn("params", request)
        self.assertEqual(request["request_id"], response["request_id"])
        self.assertTrue(response["acknowledged"])
        self.assertTrue(response["response_received"])
        self.assertEqual(response["code"], "0")
        self.assertNotIn("do-not-log-this-value", json.dumps(self.events))
        # Authentication is sent to OKX, but it is never copied into audit.
        self.assertIn("OK-ACCESS-KEY", session.requests[0][2]["headers"])
        self.assertNotIn("api-key-secret", json.dumps(self.events))

    async def test_nonzero_response_is_acknowledged_warning(self):
        session = FakeSession(
            get_results=[FakeResponse({"code": "50001", "msg": "rejected"})]
        )
        self.client._get_session = mock.AsyncMock(return_value=session)

        result = await self.client._get("/api/v5/predictions/balance")

        self.assertEqual(result["code"], "50001")
        response = self._events("okx.http.response")[0]
        self.assertEqual(response["level"], logging.WARNING)
        self.assertTrue(response["acknowledged"])
        self.assertEqual(response["code"], "50001")

    async def test_timeout_is_unacknowledged_and_not_retried(self):
        session = FakeSession(get_results=[asyncio.TimeoutError()])
        self.client._get_session = mock.AsyncMock(return_value=session)

        result = await self.client._get("/api/v5/predictions/balance")

        self.assertEqual(result["code"], "TIMEOUT")
        timeout = self._events("okx.http.timeout")[0]
        self.assertFalse(timeout["acknowledged"])
        self.assertFalse(timeout["response_received"])
        self.assertFalse(timeout["retrying"])
        self.assertEqual(len(self._events("okx.http.request")), 1)

    async def test_connection_error_retries_with_same_request_id(self):
        first = FakeSession(
            get_results=[
                aiohttp.ClientConnectionError(
                    "reset at https://www.okx.com/api?cursor=retry-secret"
                )
            ]
        )
        second = FakeSession(get_results=[FakeResponse({"code": "0", "data": {}})])
        self.client._get_session = mock.AsyncMock(side_effect=[first, second])
        self.client.close = mock.AsyncMock()

        result = await self.client._get("/api/v5/predictions/balance")

        self.assertEqual(result["code"], "0")
        requests = self._events("okx.http.request")
        failure = self._events("okx.http.connection_error")[0]
        response = self._events("okx.http.response")[0]
        self.assertEqual([item["attempt"] for item in requests], [1, 2])
        self.assertEqual(requests[0]["request_id"], requests[1]["request_id"])
        self.assertEqual(requests[0]["request_id"], response["request_id"])
        self.assertTrue(failure["retrying"])
        self.assertFalse(failure["acknowledged"])
        self.assertNotIn("retry-secret", json.dumps(self.events))
        self.assertIn("?<query-redacted>", failure["error"]["message"])
        self.client.close.assert_awaited_once()

    async def test_invalid_json_is_a_terminal_unacknowledged_response(self):
        session = FakeSession(
            get_results=[FakeResponse(json_error=ValueError("invalid json"))]
        )
        self.client._get_session = mock.AsyncMock(return_value=session)

        result = await self.client._get("/api/v5/predictions/balance")

        self.assertEqual(result["code"], "INVALID_RESPONSE")
        invalid = self._events("okx.http.invalid_json")[0]
        self.assertFalse(invalid["acknowledged"])
        self.assertTrue(invalid["response_received"])
        self.assertEqual(len(self._events("okx.http.request")), 1)

    async def test_post_ack_is_distinct_from_order_status_and_body_is_safe(self):
        session = FakeSession(
            post_results=[FakeResponse({"code": "0", "data": {"txHash": "0xtx"}})]
        )
        self.client._get_session = mock.AsyncMock(return_value=session)
        body = {
            "action": {
                "type": "placeOrder",
                "orders": [
                    {
                        "assetId": "asset-1",
                        "side": "buy",
                        "price": "0.10",
                        "size": "10",
                        "clientOrderId": "client-order-1",
                        "reduceOnly": False,
                        "orderType": {"limit": {"tif": "gtc"}},
                    }
                ],
            },
            "nonce": 123,
            "signature": {"Ecdsa": {"r": "signature-secret"}},
        }

        result = await self.client._post("/api/v5/predictions/orders", body)

        self.assertEqual(result["code"], "0")
        request = self._events("okx.write.request")[0]
        response = self._events("okx.write.response")[0]
        self.assertEqual(request["request_id"], response["request_id"])
        self.assertTrue(response["acknowledged"])
        self.assertTrue(response["request_accepted"])
        self.assertFalse(response["order_status_confirmed"])
        serialised = json.dumps(self.events)
        self.assertNotIn("signature-secret", serialised)
        self.assertNotIn("api-key-secret", serialised)
        self.assertIn("client-order-1", serialised)

    async def test_post_timeout_and_invalid_json_remain_unacknowledged(self):
        timeout_session = FakeSession(post_results=[asyncio.TimeoutError()])
        invalid_session = FakeSession(
            post_results=[FakeResponse(json_error=ValueError("not json"))]
        )
        self.client._get_session = mock.AsyncMock(
            side_effect=[timeout_session, invalid_session]
        )
        body = {"action": {"type": "cancelAll", "assetIds": []}, "nonce": 1}

        timeout_result = await self.client._post("/timeout", body)
        invalid_result = await self.client._post("/invalid", body)

        self.assertEqual(timeout_result["code"], "TIMEOUT")
        self.assertEqual(invalid_result["code"], "INVALID_RESPONSE")
        timeout = self._events("okx.write.timeout")[0]
        invalid = self._events("okx.write.invalid_json")[0]
        self.assertFalse(timeout["acknowledged"])
        self.assertFalse(timeout["response_received"])
        self.assertFalse(invalid["acknowledged"])
        self.assertTrue(invalid["response_received"])
        self.assertFalse(invalid["order_status_confirmed"])


if __name__ == "__main__":
    unittest.main()
