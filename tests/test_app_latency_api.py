from types import SimpleNamespace
import unittest
from unittest import mock

from fastapi import HTTPException

from backend import app as app_module


class AccountHealthApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_zero_balance_is_a_healthy_authenticated_response(self):
        client = SimpleNamespace(
            base_url="https://us.okx.com",
            get_balance=mock.AsyncMock(return_value={
                "code": "0",
                "data": [{"balance": "0", "available": "0"}],
            }),
        )
        instance = SimpleNamespace(client=client, proxy_node="JP2")
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.dict(app_module.PROXY_NODES, {
                "JP2": {"label": "JP-TYO-02 | Tokyo | Direct"},
            }, clear=True),
        ):
            result = await app_module.account_health_v2("account-a", user="test")

        self.assertTrue(result["healthy"])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["region"], "AU")
        self.assertEqual(result["proxy_label"], "JP-TYO-02 | Tokyo | Direct")
        self.assertGreaterEqual(result["latency_ms"], 0)

    async def test_nonzero_okx_code_is_not_a_green_health_result(self):
        client = SimpleNamespace(
            base_url="https://www.okx.com",
            get_balance=mock.AsyncMock(return_value={
                "code": "50100",
                "msg": "authentication failed",
                "data": [],
            }),
        )
        instance = SimpleNamespace(client=client, proxy_node="JP1")
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_health_v2("account-a", user="test")

        self.assertFalse(result["healthy"])
        self.assertEqual(result["region"], "CN")
        self.assertIn("50100", result["errors"][0])


class ProxyLatencyApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_uses_region_assets_and_adds_assignment_metadata(self):
        nodes = {
            "JP2": {
                "name": "JP-TYO-02",
                "label": "JP-TYO-02 | Tokyo | Direct",
                "location": "Tokyo",
                "route": "direct",
                "host": None,
                "port": None,
            },
        }
        measured = {
            "schema_version": 2,
            "latencies": {"JP2": {"node_id": "JP2", "status": "ok"}},
        }
        benchmark = mock.AsyncMock(return_value=measured)
        auth = SimpleNamespace(headers=mock.MagicMock(return_value={"signed": "yes"}))
        instance = SimpleNamespace(
            client=SimpleNamespace(base_url="https://www.okx.com", auth=auth),
        )
        manager = SimpleNamespace(get_instance=lambda account_id: (
            instance if account_id == "account-a" else None
        ))

        with (
            mock.patch.object(app_module, "PROXY_NODES", nodes),
            mock.patch.object(app_module, "benchmark_proxy_nodes", benchmark),
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(app_module, "get_used_proxies", return_value={"JP2"}),
            mock.patch.object(app_module, "get_accounts", return_value=[{
                "proxy_node": "JP2",
                "name": "Account A",
            }]),
        ):
            result = await app_module.proxy_latency(
                region="CN",
                samples=3,
                yes_asset_id="yes-a",
                poly_token_id="poly-a",
                account_id="account-a",
                user="test",
            )

        benchmark.assert_awaited_once()
        call = benchmark.await_args
        self.assertIs(call.args[0], nodes)
        self.assertEqual(call.kwargs["region"], "CN")
        self.assertEqual(call.kwargs["samples"], 3)
        self.assertEqual(call.kwargs["yes_asset_id"], "yes-a")
        self.assertEqual(call.kwargs["poly_token_id"], "poly-a")
        signed = call.kwargs["header_factory"]({
            "signed_path": "/api/v5/market/pm-books?instId=yes-a&sz=10",
        })
        self.assertEqual(signed, {"signed": "yes"})
        auth.headers.assert_called_once_with(
            "GET",
            "/api/v5/market/pm-books?instId=yes-a&sz=10",
        )
        self.assertTrue(result["latencies"]["JP2"]["used"])
        self.assertEqual(result["latencies"]["JP2"]["account_name"], "Account A")

    async def test_invalid_region_and_unknown_single_node_are_rejected(self):
        with self.assertRaises(HTTPException) as invalid_region:
            await app_module.proxy_latency(region="US", samples=5, user="test")
        self.assertEqual(invalid_region.exception.status_code, 400)

        with (
            mock.patch.object(app_module, "PROXY_NODES", {}),
            self.assertRaises(HTTPException) as unknown_node,
        ):
            await app_module.proxy_latency_single("MISSING", user="test")
        self.assertEqual(unknown_node.exception.status_code, 404)

    async def test_market_probe_requires_a_matching_loaded_account(self):
        with self.assertRaises(HTTPException) as missing_account:
            await app_module.proxy_latency(
                region="AU",
                yes_asset_id="yes-a",
                user="test",
            )
        self.assertEqual(missing_account.exception.status_code, 400)

        instance = SimpleNamespace(client=SimpleNamespace(
            base_url="https://www.okx.com",
            auth=SimpleNamespace(headers=mock.MagicMock()),
        ))
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            self.assertRaises(HTTPException) as wrong_region,
        ):
            await app_module.proxy_latency(
                region="AU",
                yes_asset_id="yes-a",
                account_id="account-a",
                user="test",
            )
        self.assertEqual(wrong_region.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
