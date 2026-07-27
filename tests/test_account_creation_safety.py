import json
from types import SimpleNamespace
import unittest
from unittest import mock

from backend import app as app_module


class Request:
    async def json(self):
        return {
            "name": "Account A",
            "api_key": "api-key",
            "secret_key": "secret-key",
            "passphrase": "passphrase",
            "agent_key": "1" * 64,
            "proxy_node": "JP2",
            "region": "AU",
        }


class AccountCreationSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_node_is_rejected_before_account_is_created(self):
        manager = SimpleNamespace(add_account=mock.AsyncMock())
        failed = {
            "status": "failed",
            "okx": {"status": "failed", "errors": [{"type": "connect"}]},
        }
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "benchmark_proxy_node",
                new=mock.AsyncMock(return_value=failed),
            ),
        ):
            response = await app_module.create_new_account(Request(), user="test")

        self.assertEqual(response.status_code, 503)
        self.assertIn("failed", json.loads(response.body)["error"])
        manager.add_account.assert_not_awaited()

    async def test_healthy_node_allows_account_creation(self):
        manager = SimpleNamespace(
            add_account=mock.AsyncMock(return_value={"id": "account-a"})
        )
        healthy = {"status": "ok", "okx": {"status": "ok"}}
        signer = SimpleNamespace(address="0xabc")
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "benchmark_proxy_node",
                new=mock.AsyncMock(return_value=healthy),
            ),
            mock.patch("backend.core.auth.OKXSigner", return_value=signer),
        ):
            response = await app_module.create_new_account(Request(), user="test")

        self.assertTrue(response["ok"])
        self.assertEqual(response["node_health"], "ok")
        manager.add_account.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
