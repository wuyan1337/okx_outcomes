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
        "cleanup_account_state_files",
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
    async def test_running_sniper_resumes_only_after_account_reconciliation(self):
        events = []

        class Sniper:
            state = SimpleNamespace(status="stopped", locked=False)

            async def restore(self, _state, resume=False):
                events.append("restore_resume" if resume else "restore_stopped")
                return {
                    "ok": True,
                    "resumed": bool(resume),
                    "needs_reconciliation": False,
                }

        async def reconcile(startup=False):
            events.append("account_reconcile")
            return {"ok": True, "locked": False}

        def start_heartbeat():
            events.append("heartbeat")

        instance = SimpleNamespace(
            sniper=Sniper(),
            recovery_locked=False,
            recovery_reason="",
            _sniper_snapshot_corrupt=False,
            _sniper_snapshot_write_blocked=False,
            _persist_sniper_snapshot=MagicMock(return_value=True),
            reconcile_account_safety=reconcile,
            start_account_heartbeat=start_heartbeat,
            _activate_recovery_lock=MagicMock(),
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
            "runtime": {"locked": False},
            "running": True,
            "corrupt": False,
        }
        manager = account_manager.AccountManager()

        with (
            patch.object(account_manager, "get_account_decrypted", return_value=account),
            patch.object(account_manager, "get_sniper_state", return_value=snapshot),
            patch.object(account_manager, "OKXOutcomesClient"),
            patch.object(account_manager, "AccountInstance", return_value=instance),
        ):
            loaded = await manager._load_account("account-a")

        self.assertIs(loaded, instance)
        self.assertEqual(
            events,
            ["restore_stopped", "account_reconcile", "restore_resume", "heartbeat"],
        )

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

    async def test_remove_account_does_not_cancel_all_after_verified_flat(self):
        manager = account_manager.AccountManager()
        instance = SimpleNamespace(close=AsyncMock())
        manager.instances["account-a"] = instance
        cleanup = {
            "account_id": "account-a",
            "complete": True,
            "files": [],
            "deleted": [],
            "missing": [],
            "errors": [],
        }

        with (
            patch.object(account_manager, "delete_account", return_value=True) as delete,
            patch.object(
                account_manager,
                "cleanup_account_state_files",
                return_value=cleanup,
            ) as clean_files,
        ):
            result = await manager.remove_account("account-a")

        self.assertEqual(
            result,
            {
                "deleted": True,
                "cleanup_complete": True,
                "state_cleanup": cleanup,
            },
        )
        instance.close.assert_awaited_once_with(cancel_orders=False)
        delete.assert_called_once_with("account-a")
        clean_files.assert_called_once_with("account-a")
        self.assertNotIn("account-a", manager.instances)

    async def test_remove_account_cleans_files_only_after_database_commit(self):
        manager = account_manager.AccountManager()
        events = []

        async def close(*, cancel_orders):
            self.assertFalse(cancel_orders)
            events.append("writers_closed")

        def delete(_account_id):
            events.append("database_committed")
            return True

        def cleanup(_account_id):
            events.append("files_cleaned")
            return {
                "account_id": "account-a",
                "complete": False,
                "files": [],
                "deleted": [],
                "missing": [],
                "errors": [{"name": "cost_layers_account-a.json", "status": "error"}],
            }

        manager.instances["account-a"] = SimpleNamespace(close=close)
        with (
            patch.object(account_manager, "delete_account", side_effect=delete),
            patch.object(
                account_manager,
                "cleanup_account_state_files",
                side_effect=cleanup,
            ),
        ):
            result = await manager.remove_account("account-a")

        self.assertEqual(
            events, ["writers_closed", "database_committed", "files_cleaned"]
        )
        self.assertTrue(result["deleted"])
        self.assertFalse(result["cleanup_complete"])

    async def test_remove_account_database_miss_drops_closed_instance_and_does_not_clean_files(self):
        manager = account_manager.AccountManager()
        closed_instance = SimpleNamespace(close=AsyncMock())
        manager.instances["account-a"] = closed_instance

        with (
            patch.object(account_manager, "delete_account", return_value=False),
            patch.object(account_manager, "cleanup_account_state_files") as cleanup,
            patch.object(
                manager,
                "_load_account",
                AsyncMock(return_value=None),
            ) as reload_account,
        ):
            with self.assertRaisesRegex(RuntimeError, "database row was not deleted"):
                await manager.remove_account("account-a")

        closed_instance.close.assert_awaited_once_with(cancel_orders=False)
        reload_account.assert_awaited_once_with("account-a")
        cleanup.assert_not_called()
        self.assertNotIn("account-a", manager.instances)

    async def test_remove_account_database_exception_reloads_fresh_instance(self):
        manager = account_manager.AccountManager()
        closed_instance = SimpleNamespace(close=AsyncMock())
        fresh_instance = SimpleNamespace(close=AsyncMock())
        manager.instances["account-a"] = closed_instance

        async def reload_account(account_id):
            self.assertEqual(account_id, "account-a")
            self.assertNotIn(account_id, manager.instances)
            manager.instances[account_id] = fresh_instance
            return fresh_instance

        with (
            patch.object(
                account_manager,
                "delete_account",
                side_effect=RuntimeError("database locked"),
            ),
            patch.object(account_manager, "cleanup_account_state_files") as cleanup,
            patch.object(
                manager,
                "_load_account",
                AsyncMock(side_effect=reload_account),
            ) as reload_mock,
        ):
            with self.assertRaisesRegex(RuntimeError, "database locked"):
                await manager.remove_account("account-a")

        closed_instance.close.assert_awaited_once_with(cancel_orders=False)
        fresh_instance.close.assert_not_awaited()
        reload_mock.assert_awaited_once_with("account-a")
        cleanup.assert_not_called()
        self.assertIs(manager.instances["account-a"], fresh_instance)
        self.assertIsNot(manager.instances["account-a"], closed_instance)

    async def test_remove_account_failed_reload_closes_partial_and_leaves_no_instance(self):
        manager = account_manager.AccountManager()
        closed_instance = SimpleNamespace(close=AsyncMock())
        partial_instance = SimpleNamespace(close=AsyncMock())
        manager.instances["account-a"] = closed_instance

        async def failed_reload(account_id):
            manager.instances[account_id] = partial_instance
            raise RuntimeError("reload failed")

        with (
            patch.object(
                account_manager,
                "delete_account",
                side_effect=RuntimeError("database locked"),
            ),
            patch.object(account_manager, "cleanup_account_state_files") as cleanup,
            patch.object(
                manager,
                "_load_account",
                AsyncMock(side_effect=failed_reload),
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "fresh live instance could not be restored",
            ):
                await manager.remove_account("account-a")

        closed_instance.close.assert_awaited_once_with(cancel_orders=False)
        partial_instance.close.assert_awaited_once_with(cancel_orders=False)
        cleanup.assert_not_called()
        self.assertNotIn("account-a", manager.instances)


if __name__ == "__main__":
    unittest.main()
