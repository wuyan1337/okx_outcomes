import importlib.util
from pathlib import Path
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

try:
    from backend.services import account_manager
except ModuleNotFoundError:
    client_module = types.ModuleType("backend.core.client")
    client_module.OKXOutcomesClient = type("OKXOutcomesClient", (), {})
    database_module = types.ModuleType("backend.core.database")
    for function_name in (
        "get_accounts",
        "get_account_decrypted",
        "create_account",
        "update_account",
        "delete_account",
        "get_available_proxies",
        "get_used_proxies",
        "get_global_config",
        "set_global_config",
        "get_sniper_state",
        "save_sniper_state",
    ):
        setattr(database_module, function_name, lambda *args, **kwargs: None)
    database_module.PROXY_NODES = {}
    dependency_modules = {
        "backend.core.client": client_module,
        "backend.core.database": database_module,
    }
    for module_name, class_name in (
        ("backend.services.manual_strategy_d", "ManualStrategyD"),
        ("backend.services.manual_strategy_s", "ManualStrategyS"),
        ("backend.services.poly_monitor", "PolyMonitor"),
        ("backend.services.auto_rebalance", "AutoRebalance"),
        ("backend.services.poly_sniper", "PolySniper"),
        ("backend.services.poly_sniper_old", "PolySniperOld"),
        ("backend.services.notifier", "TelegramNotifier"),
    ):
        dependency = types.ModuleType(module_name)
        setattr(dependency, class_name, type(class_name, (), {}))
        dependency_modules[module_name] = dependency
    isolated_name = "backend.services._account_manager_shutdown_test"
    source_path = (
        Path(__file__).resolve().parents[1]
        / "backend"
        / "services"
        / "account_manager.py"
    )
    spec = importlib.util.spec_from_file_location(isolated_name, source_path)
    account_manager = importlib.util.module_from_spec(spec)
    dependency_modules[isolated_name] = account_manager
    with patch.dict(sys.modules, dependency_modules):
        spec.loader.exec_module(account_manager)


class IdleComponent:
    running = False

    async def stop(self):
        raise AssertionError("an idle component must not be stopped")


class RunningSniper:
    def __init__(self):
        self._running = True
        self.stop_count = 0
        self.state = SimpleNamespace(locked=False)

    @property
    def running(self):
        return self._running

    async def stop(self):
        self.stop_count += 1
        self._running = False

    def get_state(self):
        return {"running": self._running, "status": "stopped", "locked": False}

    def get_config(self):
        return {"config": {"threshold_cents": "10"}, "targets": []}


class FakeClient:
    def __init__(self):
        self.cancel_all_count = 0
        self.close_count = 0

    async def cancel_all(self):
        self.cancel_all_count += 1

    async def close(self):
        self.close_count += 1


class AccountManagerSniperShutdownTests(unittest.IsolatedAsyncioTestCase):
    def test_snapshot_callback_reports_database_failure(self):
        instance = account_manager.AccountInstance.__new__(account_manager.AccountInstance)
        instance.account_id = "account-a"
        instance.sniper = RunningSniper()

        with patch.object(account_manager, "save_sniper_state") as save_state:
            self.assertTrue(instance._persist_sniper_snapshot())
            save_state.side_effect = RuntimeError("database locked")
            with self.assertLogs(account_manager.logger, level="ERROR"):
                self.assertFalse(instance._persist_sniper_snapshot())

    def test_corrupt_snapshot_guard_refuses_to_overwrite_database_row(self):
        instance = account_manager.AccountInstance.__new__(account_manager.AccountInstance)
        instance.account_id = "account-a"
        instance.sniper = RunningSniper()
        instance._sniper_snapshot_corrupt = True

        with (
            patch.object(account_manager, "save_sniper_state") as save_state,
            self.assertLogs(account_manager.logger, level="ERROR"),
        ):
            self.assertFalse(instance._persist_sniper_snapshot())

        save_state.assert_not_called()

    async def test_semantically_invalid_snapshot_is_locked_and_not_overwritten(self):
        sniper = SimpleNamespace(
            state=SimpleNamespace(status="stopped", locked=False),
            restore=AsyncMock(
                return_value={"ok": False, "error": "At least one target is required"}
            ),
        )
        instance = SimpleNamespace(
            sniper=sniper,
            _sniper_snapshot_corrupt=False,
            _sniper_snapshot_write_blocked=False,
            _persist_sniper_snapshot=MagicMock(),
        )
        account = {
            "id": "account-a",
            "name": "Account A",
            "api_key": "api",
            "secret_key": "secret",
            "passphrase": "pass",
            "agent_private_key": "1" * 64,
            "base_url": "https://us.okx.com",
            "proxy_node": "JP2",
        }
        snapshot = {
            "config": {},
            "runtime": {},
            "running": True,
            "corrupt": False,
        }
        manager = account_manager.AccountManager()

        with (
            patch.object(account_manager, "get_account_decrypted", return_value=account),
            patch.object(account_manager, "get_sniper_state", return_value=snapshot),
            patch.object(account_manager, "OKXOutcomesClient"),
            patch.object(account_manager, "AccountInstance", return_value=instance),
            self.assertLogs(account_manager.logger, level="ERROR"),
        ):
            loaded = await manager._load_account("account-a")

        self.assertIs(loaded, instance)
        self.assertTrue(instance._sniper_snapshot_write_blocked)
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper.state.locked)
        instance._persist_sniper_snapshot.assert_not_called()

    async def test_transient_restore_failure_keeps_snapshot_writes_available_for_retry(self):
        sniper = SimpleNamespace(
            state=SimpleNamespace(status="stopped", locked=False),
            restore=AsyncMock(
                return_value={
                    "ok": False,
                    "error": "position query failed",
                    "needs_reconciliation": True,
                }
            ),
        )
        instance = SimpleNamespace(
            sniper=sniper,
            _sniper_snapshot_corrupt=False,
            _sniper_snapshot_write_blocked=False,
            _persist_sniper_snapshot=MagicMock(),
        )
        account = {
            "id": "account-a",
            "name": "Account A",
            "api_key": "api",
            "secret_key": "secret",
            "passphrase": "pass",
            "agent_private_key": "1" * 64,
            "base_url": "https://us.okx.com",
            "proxy_node": "JP2",
        }
        snapshot = {
            "config": {"targets": [{"id": "target-a"}]},
            "runtime": {"locked": True},
            "running": True,
            "corrupt": False,
        }
        manager = account_manager.AccountManager()

        with (
            patch.object(account_manager, "get_account_decrypted", return_value=account),
            patch.object(account_manager, "get_sniper_state", return_value=snapshot),
            patch.object(account_manager, "OKXOutcomesClient"),
            patch.object(account_manager, "AccountInstance", return_value=instance),
            self.assertLogs(account_manager.logger, level="ERROR"),
        ):
            loaded = await manager._load_account("account-a")

        self.assertIs(loaded, instance)
        self.assertFalse(instance._sniper_snapshot_write_blocked)
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper.state.locked)

    async def test_service_shutdown_preserves_running_resume_marker(self):
        instance = account_manager.AccountInstance.__new__(account_manager.AccountInstance)
        instance.account_id = "account-a"
        instance.strategy_d = IdleComponent()
        instance.strategy_s = IdleComponent()
        instance.poly_monitor = IdleComponent()
        instance.sniper = RunningSniper()
        instance.sniper_old = IdleComponent()
        instance.client = FakeClient()

        with patch.object(account_manager, "save_sniper_state") as save_state:
            await instance.close(cancel_orders=False)

        self.assertEqual(instance.sniper.stop_count, 1)
        self.assertEqual(instance.client.cancel_all_count, 0)
        self.assertEqual(instance.client.close_count, 1)
        save_state.assert_called_once()
        args = save_state.call_args.args
        self.assertEqual(args[0], "account-a")
        self.assertTrue(args[2]["running"])
        self.assertTrue(args[3])


if __name__ == "__main__":
    unittest.main()
