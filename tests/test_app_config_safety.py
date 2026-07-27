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
    async def test_account_reconcile_repairs_rebalance_journal_first(self):
        class RecoverableRebalance:
            def __init__(self):
                self.recovery_required = True
                self._rebalancing = False
                self.called = 0

            async def recover(self):
                self.called += 1
                self.recovery_required = False
                return {"ok": True, "recovery_required": False}

        instance = idle_instance(FlatClient())
        instance.auto_rebalance = RecoverableRebalance()
        instance.recovery_locked = True
        instance.recovery_reason = "auto_rebalance_recovery_required"
        instance.recovery_details = {}
        instance.recovery_status = lambda: {
            "locked": instance.recovery_locked,
            "reason": instance.recovery_reason,
            "details": {},
        }

        async def reconcile():
            instance.recovery_locked = False
            instance.recovery_reason = ""
            return {"ok": True, "locked": False, "reason": "", "details": {}}

        instance.reconcile_account_safety = reconcile
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_recovery_reconcile(
                "account-a", user="test"
            )

        self.assertTrue(response["ok"])
        self.assertEqual(instance.auto_rebalance.called, 1)
        self.assertTrue(response["rebalance_recovery"]["ok"])

    async def test_recovery_lock_blocks_monitor_and_rebalance_enable(self):
        instance = idle_instance(FlatClient())
        instance.recovery_locked = True
        instance.recovery_reason = "unmanaged_exchange_state"
        instance.recovery_details = {}
        instance.poly_monitor.start = mock.AsyncMock()
        instance.auto_rebalance = SimpleNamespace(
            enabled=False,
            _rebalancing=False,
            set_enabled=mock.MagicMock(return_value=True),
            cancel=mock.AsyncMock(),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            monitor = await app_module.account_monitor_start(
                "account-a", Request({"poly_token_id": "poly"}), user="test"
            )
            rebalance = await app_module.account_rebalance_toggle(
                "account-a", Request({"enabled": True}), user="test"
            )

        self.assertEqual(monitor.status_code, 409)
        self.assertEqual(rebalance.status_code, 409)
        instance.poly_monitor.start.assert_not_awaited()
        instance.auto_rebalance.set_enabled.assert_not_called()

    async def test_recovery_lock_still_allows_rebalance_disable(self):
        instance = idle_instance(FlatClient())
        instance.recovery_locked = True
        instance.recovery_reason = "unmanaged_exchange_state"
        instance.recovery_details = {}
        instance.auto_rebalance = SimpleNamespace(
            enabled=True,
            _rebalancing=False,
            set_enabled=mock.MagicMock(return_value=True),
            cancel=mock.AsyncMock(),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_rebalance_toggle(
                "account-a", Request({"enabled": False}), user="test"
            )

        self.assertTrue(response["ok"])
        instance.auto_rebalance.set_enabled.assert_called_once_with(False)
        instance.auto_rebalance.cancel.assert_awaited_once_with()

    async def test_account_config_never_returns_decrypted_credentials(self):
        account = stored_account()
        account["passphrase"] = "private-passphrase-value"
        with (
            mock.patch.object(database, "get_account_decrypted", return_value=account),
            mock.patch.object(
                database,
                "get_global_config",
                side_effect=lambda key, default="": (
                    "telegram-secret" if key == "tg_token" else default
                ),
            ),
        ):
            response = await app_module.account_config_v2(
                "account-a", user="test"
            )

        self.assertEqual(response["api_key"], "")
        self.assertEqual(response["secret_key"], "")
        self.assertEqual(response["passphrase"], "")
        self.assertEqual(response["agent_key"], "")
        self.assertEqual(response["tg_token"], "")
        self.assertTrue(all(response["credentials_configured"].values()))
        self.assertTrue(response["tg_token_configured"])
        serialised = json.dumps(response)
        for secret in (
            "api-key",
            "secret-key",
            "private-passphrase-value",
            "1" * 64,
            "telegram-secret",
        ):
            self.assertNotIn(secret, serialised)

    async def test_global_config_hides_token_and_blank_update_preserves_it(self):
        stored = {"tg_token": "telegram-secret", "tg_chats": "123,456"}
        writes = []

        def get_value(key, default=""):
            return stored.get(key, default)

        def set_value(key, value):
            writes.append((key, value))
            stored[key] = value

        manager = SimpleNamespace(notifier=None)
        with (
            mock.patch.object(app_module, "get_global_config", side_effect=get_value),
            mock.patch.object(app_module, "set_global_config", side_effect=set_value),
            mock.patch.object(app_module, "manager", manager),
        ):
            response = await app_module.get_global_config_route(user="test")
            saved = await app_module.save_global_config(
                Request({"tg_token": "", "tg_chats": "123,456"}),
                user="test",
            )

        self.assertEqual(response["tg_token"], "")
        self.assertTrue(response["tg_token_configured"])
        self.assertNotIn("telegram-secret", json.dumps(response))
        self.assertTrue(saved["ok"])
        self.assertEqual(stored["tg_token"], "telegram-secret")
        self.assertNotIn(("tg_token", ""), writes)

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
