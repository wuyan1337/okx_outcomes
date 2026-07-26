import json
from types import SimpleNamespace
import unittest
from unittest import mock

from fastapi import HTTPException

from backend import app as app_module
from backend.core import database
from backend.services.auto_rebalance import AutoRebalance


class Request:
    def __init__(self, body):
        self.body = body

    async def json(self):
        return self.body


class FlatClient:
    async def get_orders(self, status="open"):
        return {"code": "0", "data": {"list": []}}

    async def get_positions(self, status="open"):
        return {"code": "0", "data": {"list": []}}

    async def close(self):
        return None


def stored_account():
    return {
        "id": "account-a",
        "name": "Account A",
        "api_key": "api-key",
        "secret_key": "secret-key",
        "passphrase": "passphrase",
        "agent_private_key": "1" * 64,
        "proxy_node": "JP2",
        "base_url": "https://us.okx.com",
    }


def idle_instance(client):
    idle = SimpleNamespace(running=False)
    monitor = SimpleNamespace(running=False, state=SimpleNamespace(enabled=False))
    sniper = SimpleNamespace(
        running=False,
        starting=False,
        state=SimpleNamespace(locked=False),
    )
    instance = SimpleNamespace(
        client=client,
        strategy_d=idle,
        strategy_s=idle,
        sniper=sniper,
        sniper_old=idle,
        poly_monitor=monitor,
        auto_rebalance=SimpleNamespace(_rebalancing=False),
    )
    instance.installed = []

    def install_client(replacement):
        previous = instance.client
        instance.client = replacement
        instance.installed.append(replacement)
        return previous

    instance.install_client = install_client
    return instance


class AccountConfigSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_rebalance_toggle_persists_enabled_state(self):
        instance = idle_instance(FlatClient())
        rebalance = AutoRebalance(object(), "toggle-success-no-file")
        rebalance._save_layers = mock.MagicMock(return_value=True)
        instance.auto_rebalance = rebalance
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_rebalance_toggle(
                "account-a", Request({"enabled": True}), user="test"
            )

        self.assertTrue(response["ok"])
        self.assertTrue(response["enabled"])
        self.assertTrue(rebalance.enabled)
        rebalance._save_layers.assert_called_once_with()

    async def test_rebalance_toggle_rolls_back_when_persistence_fails(self):
        instance = idle_instance(FlatClient())
        rebalance = AutoRebalance(object(), "toggle-failure-no-file")
        rebalance._save_layers = mock.MagicMock(return_value=False)
        instance.auto_rebalance = rebalance
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_rebalance_toggle(
                "account-a", Request({"enabled": True}), user="test"
            )

        self.assertEqual(response.status_code, 500)
        self.assertFalse(rebalance.enabled)
        self.assertFalse(json.loads(response.body)["enabled"])

    async def test_rebalance_toggle_rejects_non_boolean_values(self):
        instance = idle_instance(FlatClient())
        rebalance = AutoRebalance(object(), "toggle-invalid-no-file")
        rebalance._save_layers = mock.MagicMock(return_value=True)
        instance.auto_rebalance = rebalance
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            for value in ("true", 1, None):
                with self.subTest(value=value):
                    response = await app_module.account_rebalance_toggle(
                        "account-a", Request({"enabled": value}), user="test"
                    )
                    self.assertEqual(response.status_code, 400)

        self.assertFalse(rebalance.enabled)
        rebalance._save_layers.assert_not_called()

    async def test_clear_layers_is_rejected_while_strategy_is_running(self):
        instance = idle_instance(FlatClient())
        instance.strategy_d = SimpleNamespace(running=True)
        layers = {"market": {"asset": {"1": 2.0}}}
        save_layers = mock.MagicMock(return_value=True)
        instance.auto_rebalance = SimpleNamespace(
            _rebalancing=False,
            cost_layers=layers,
            _save_layers=save_layers,
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_rebalance_clear(
                "account-a", user="test"
            )

        self.assertEqual(response.status_code, 409)
        self.assertIs(instance.auto_rebalance.cost_layers, layers)
        save_layers.assert_not_called()

    async def test_clear_layers_rolls_back_when_persistence_fails(self):
        instance = idle_instance(FlatClient())
        layers = {"market": {"asset": {"1": 2.0}}}
        instance.auto_rebalance = SimpleNamespace(
            _rebalancing=False,
            cost_layers=layers,
            _save_layers=mock.MagicMock(return_value=False),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_rebalance_clear(
                "account-a", user="test"
            )

        self.assertEqual(response.status_code, 500)
        self.assertIs(instance.auto_rebalance.cost_layers, layers)
        self.assertIn("could not be persisted", json.loads(response.body)["error"])

    async def test_unknown_account_returns_404(self):
        manager = SimpleNamespace(get_instance=lambda _account_id: None)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(app_module, "get_account_decrypted", return_value=None),
        ):
            with self.assertRaises(HTTPException) as raised:
                await app_module.save_account_config_v2(
                    "missing", Request({"region": "CN"}), user="test"
                )

        self.assertEqual(raised.exception.status_code, 404)

    async def test_credential_change_is_rejected_when_exchange_has_open_orders(self):
        class Client(FlatClient):
            async def get_orders(self, status="open"):
                return {
                    "code": "0",
                    "data": {"list": [{"assetId": "123000", "oid": "open-1"}]},
                }

        instance = idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module, "get_account_decrypted", return_value=stored_account()
            ),
            mock.patch.object(database, "update_account") as update_account,
        ):
            response = await app_module.save_account_config_v2(
                "account-a", Request({"region": "CN"}), user="test"
            )

        self.assertEqual(response.status_code, 409)
        self.assertIn("Cancel all open orders", json.loads(response.body)["error"])
        update_account.assert_not_called()
        self.assertEqual(instance.installed, [])

    async def test_invalid_agent_key_is_rejected_before_database_update(self):
        instance = idle_instance(FlatClient())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module, "get_account_decrypted", return_value=stored_account()
            ),
            mock.patch.object(database, "update_account") as update_account,
        ):
            response = await app_module.save_account_config_v2(
                "account-a", Request({"agent_key": "not-a-private-key"}), user="test"
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid agent private key", json.loads(response.body)["error"])
        update_account.assert_not_called()
        self.assertEqual(instance.installed, [])

    async def test_valid_flat_credential_change_swaps_after_database_update(self):
        instance = idle_instance(FlatClient())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        calls = []

        def update_account(_account_id, **kwargs):
            calls.append(kwargs)
            return True

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module, "get_account_decrypted", return_value=stored_account()
            ),
            mock.patch.object(database, "update_account", side_effect=update_account),
        ):
            response = await app_module.save_account_config_v2(
                "account-a", Request({"region": "CN"}), user="test"
            )

        self.assertTrue(response["ok"])
        self.assertTrue(response["reloaded"])
        self.assertEqual(calls, [{"base_url": "https://www.okx.com"}])
        self.assertEqual(len(instance.installed), 1)
        self.assertEqual(instance.client.base_url, "https://www.okx.com")

    async def test_database_failure_keeps_the_old_client_installed(self):
        original = FlatClient()
        instance = idle_instance(original)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module, "get_account_decrypted", return_value=stored_account()
            ),
            mock.patch.object(
                database,
                "update_account",
                side_effect=RuntimeError("database unavailable"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                await app_module.save_account_config_v2(
                    "account-a", Request({"region": "CN"}), user="test"
                )

        self.assertIs(instance.client, original)
        self.assertEqual(instance.installed, [])


if __name__ == "__main__":
    unittest.main()
