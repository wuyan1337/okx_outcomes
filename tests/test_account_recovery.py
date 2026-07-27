import asyncio
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from backend.services import account_manager


class FakeComponent:
    def __init__(self, running=False, *, enabled=False):
        self.running = running
        self.state = SimpleNamespace(enabled=enabled)
        self.stop_count = 0

    async def stop(self):
        self.stop_count += 1
        self.running = False
        self.state.enabled = False


class FakeAutoRebalance:
    def __init__(self):
        self.cancel_count = 0

    async def cancel(self):
        self.cancel_count += 1


class FakeSniper(FakeComponent):
    def __init__(
        self,
        running=False,
        *,
        locked=False,
        status="stopped",
        active_target_id="",
        asset_id="",
        buy_size="0",
        sold_size="0",
        position_before="0",
    ):
        super().__init__(running=running)
        self.starting = False
        self.state = SimpleNamespace(
            locked=locked,
            status=status,
            active_target_id=active_target_id,
            buy_size=Decimal(buy_size),
            sold_size=Decimal(sold_size),
            position_before=Decimal(position_before),
            entry_client_order_id="",
            exit_client_order_ids=[],
        )
        self.targets = (
            [SimpleNamespace(id=active_target_id, asset_id=asset_id)]
            if active_target_id and asset_id
            else []
        )


class FakeSniperOld(FakeComponent):
    def __init__(
        self,
        *,
        locked=False,
        status="stopped",
        side="",
        buy_size="0",
        sold_size="0",
    ):
        super().__init__(running=False)
        self.state = SimpleNamespace(
            locked=locked,
            status=status,
            holding_side=side,
            yes_asset_id="market-a000",
            no_asset_id="market-a100",
            buy_size=Decimal(buy_size),
            sold_size=Decimal(sold_size),
            buy_client_order_id="old-entry",
            sell_client_order_id="old-protect",
            exit_client_order_ids=[],
        )


class SafetyClient:
    def __init__(
        self,
        *,
        heartbeat=None,
        orders=None,
        positions=None,
    ):
        self.heartbeat_response = heartbeat or {"code": "0", "data": {}}
        self.orders_response = orders or {"code": "0", "data": {"list": []}}
        self.positions_response = positions or {"code": "0", "data": {"list": []}}
        self.heartbeat_called = asyncio.Event()
        self.dead_man_heartbeat_called = False

    async def heartbeat(self):
        self.dead_man_heartbeat_called = True
        return self.heartbeat_response

    async def get_orders(self, status="open"):
        self.heartbeat_called.set()
        if str(self.heartbeat_response.get("code", "")) != "0":
            return self.heartbeat_response
        return self.orders_response

    async def get_positions(self, status="open"):
        return self.positions_response


def make_instance(client, *, sniper=None):
    instance = account_manager.AccountInstance.__new__(account_manager.AccountInstance)
    instance.account_id = "account-a"
    instance.name = "Account A"
    instance.client = client
    instance.proxy_node = "Direct"
    instance.strategy_lifecycle_lock = asyncio.Lock()
    instance.strategy_d = FakeComponent()
    instance.strategy_s = FakeComponent()
    instance.poly_monitor = FakeComponent()
    instance.auto_rebalance = FakeAutoRebalance()
    instance.sniper = sniper or FakeSniper()
    instance.sniper_old = FakeComponent()
    instance.recovery_locked = False
    instance.recovery_reason = ""
    instance.recovery_details = {}
    instance.last_heartbeat_ok = None
    instance.last_reconciled_at = None
    instance._account_heartbeat_task = None
    instance._account_heartbeat_interval = 120.0
    instance._account_heartbeat_retry_delay = 0.001
    instance._recovery_lock = asyncio.Lock()
    return instance


class AccountRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def test_position_outcome_parser_accepts_only_matching_pairs(self):
        instance = make_instance(SafetyClient())
        self.assertEqual(
            instance._position_asset_id(
                {"marketId": "market-a", "tokenIndex": "1", "tokenName": "YES"}
            ),
            "market-a000",
        )
        self.assertEqual(
            instance._position_asset_id(
                {"marketId": "market-a", "tokenIndex": "2", "tokenName": "NO"}
            ),
            "market-a100",
        )

    async def test_invalid_position_outcome_locks_account_instead_of_defaulting_yes(self):
        invalid_positions = (
            {"size": "1"},
            {"marketId": "market-a", "size": "1"},
            {"marketId": "market-a", "tokenIndex": "1", "size": "1"},
            {"marketId": "market-a", "tokenName": "YES", "size": "1"},
            {
                "marketId": "market-a",
                "tokenIndex": "3",
                "tokenName": "YES",
                "size": "1",
            },
            {
                "marketId": "market-a",
                "tokenIndex": "1",
                "tokenName": "MAYBE",
                "size": "1",
            },
            {
                "marketId": "market-a",
                "tokenIndex": "1",
                "tokenName": "NO",
                "size": "1",
            },
        )
        for position in invalid_positions:
            with self.subTest(position=position):
                instance = make_instance(
                    SafetyClient(
                        positions={"code": "0", "data": {"list": [position]}}
                    )
                )
                result = await instance.reconcile_account_safety(startup=True)
                self.assertFalse(result["ok"])
                self.assertTrue(result["unsafe"])
                self.assertEqual(len(result["invalid_positions"]), 1)
                self.assertTrue(instance.recovery_locked)

    async def test_restored_sniper_old_ledger_is_managed_before_resume(self):
        client = SafetyClient(
            orders={
                "code": "0",
                "data": {
                    "list": [
                        {
                            "clientOrderId": "old-protect",
                            "oid": "protect-oid",
                        }
                    ]
                },
            },
            positions={
                "code": "0",
                "data": {
                    "list": [
                        {
                            "marketId": "market-a",
                            "tokenIndex": "1",
                            "tokenName": "YES",
                            "size": "2",
                        }
                    ]
                },
            },
        )
        instance = make_instance(client)
        instance.sniper_old = FakeSniperOld(
            locked=True,
            side="yes",
            buy_size="2",
        )

        result = await instance.reconcile_account_safety(startup=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["managed_sniper_old_position_size"], "2")
        self.assertEqual(result["unmanaged_order_count"], 0)
        self.assertEqual(result["unmanaged_position_count"], 0)

    async def test_account_heartbeat_survives_manual_strategy_stop(self):
        client = SafetyClient()
        instance = make_instance(client)
        instance.strategy_d.running = True
        instance.start_account_heartbeat()
        try:
            await asyncio.wait_for(client.heartbeat_called.wait(), timeout=1)
            await instance.strategy_d.stop()
            self.assertTrue(instance.account_heartbeat_running)
            self.assertFalse(client.dead_man_heartbeat_called)
        finally:
            await instance.stop_account_heartbeat()
        self.assertFalse(instance.account_heartbeat_running)

    async def test_heartbeat_failure_locks_account_and_stops_entry_activity(self):
        client = SafetyClient(
            heartbeat={"code": "ERROR", "msg": "heartbeat unavailable"}
        )
        instance = make_instance(client)
        instance.strategy_d.running = True
        instance.strategy_s.running = True
        instance.poly_monitor.running = True
        instance.poly_monitor.state.enabled = True
        instance.sniper = FakeSniper(running=True)
        instance.sniper_old.running = True
        instance.start_account_heartbeat()
        try:
            await asyncio.wait_for(client.heartbeat_called.wait(), timeout=1)
            for _ in range(100):
                if instance.strategy_d.stop_count:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(instance.recovery_locked)
            self.assertEqual(instance.recovery_reason, "heartbeat_failed")
            self.assertEqual(instance.strategy_d.stop_count, 1)
            self.assertEqual(instance.strategy_s.stop_count, 1)
            self.assertEqual(instance.sniper.stop_count, 1)
            self.assertEqual(instance.sniper_old.stop_count, 1)
            self.assertEqual(instance.poly_monitor.stop_count, 1)
            self.assertGreaterEqual(instance.auto_rebalance.cancel_count, 1)
            self.assertFalse(client.dead_man_heartbeat_called)
        finally:
            await instance.stop_account_heartbeat()

    async def test_startup_allows_only_restored_running_sniper_exposure(self):
        client = SafetyClient(
            positions={
                "code": "0",
                "data": {
                    "list": [
                        {
                            "marketId": "market-a",
                            "tokenIndex": "1",
                            "tokenName": "YES",
                            "size": "2",
                        }
                    ]
                },
            }
        )
        sniper = FakeSniper(
            running=True,
            locked=True,
            status="holding",
            active_target_id="target-a",
            asset_id="market-a000",
            buy_size="2",
        )
        instance = make_instance(client, sniper=sniper)

        result = await instance.reconcile_account_safety(startup=True)

        self.assertTrue(result["ok"])
        self.assertFalse(instance.recovery_locked)
        self.assertEqual(result["managed_sniper_position_size"], "2")
        self.assertEqual(result["unmanaged_position_count"], 0)
        self.assertEqual(sniper.stop_count, 0)

    async def test_stopped_restored_sniper_ledger_is_managed_before_resume(self):
        client = SafetyClient(
            positions={
                "code": "0",
                "data": {
                    "list": [
                        {
                            "marketId": "market-a",
                            "tokenIndex": "1",
                            "tokenName": "YES",
                            "size": "2",
                        }
                    ]
                },
            }
        )
        sniper = FakeSniper(
            running=False,
            locked=True,
            status="stopped",
            active_target_id="target-a",
            asset_id="market-a000",
            buy_size="2",
        )
        instance = make_instance(client, sniper=sniper)

        result = await instance.reconcile_account_safety(startup=True)

        self.assertTrue(result["ok"])
        self.assertFalse(instance.recovery_locked)
        self.assertEqual(result["managed_sniper_position_size"], "2")
        self.assertEqual(result["unmanaged_position_count"], 0)
        self.assertEqual(sniper.stop_count, 0)

    async def test_startup_locks_unmanaged_orders_and_positions(self):
        client = SafetyClient(
            orders={
                "code": "0",
                "data": {
                    "list": [
                        {
                            "oid": "order-1",
                            "clientOrderId": "manual-order",
                            "assetId": "market-a000",
                        }
                    ]
                },
            },
            positions={
                "code": "0",
                "data": {
                    "list": [
                        {
                            "marketId": "market-a",
                            "tokenIndex": "1",
                            "tokenName": "YES",
                            "size": "3",
                        }
                    ]
                },
            },
        )
        instance = make_instance(client)

        result = await instance.reconcile_account_safety(startup=True)

        self.assertFalse(result["ok"])
        self.assertTrue(instance.recovery_locked)
        self.assertEqual(instance.recovery_reason, "unmanaged_exchange_state")
        self.assertEqual(result["unmanaged_order_count"], 1)
        self.assertEqual(result["unmanaged_position_count"], 1)

    async def test_query_failure_locks_and_flat_reconcile_clears(self):
        client = SafetyClient(orders={"code": "ERROR", "msg": "route down"})
        instance = make_instance(client)

        failed = await instance.reconcile_account_safety(startup=True)
        self.assertFalse(failed["ok"])
        self.assertEqual(instance.recovery_reason, "startup_safety_query_failed")

        client.orders_response = {"code": "0", "data": {"list": []}}
        cleared = await instance.reconcile_account_safety()

        self.assertTrue(cleared["ok"])
        self.assertTrue(cleared["flat"])
        self.assertFalse(instance.recovery_locked)
        self.assertEqual(instance.recovery_reason, "")

    async def test_auto_rebalance_journal_keeps_flat_account_locked(self):
        class RebalanceRecovery(FakeAutoRebalance):
            recovery_required = True

            def recovery_status(self):
                return {
                    "required": True,
                    "stage": "monitor_cancel_all",
                    "rebalance_id": "rb-1",
                }

        instance = make_instance(SafetyClient())
        instance.auto_rebalance = RebalanceRecovery()

        result = await instance.reconcile_account_safety(startup=True)

        self.assertFalse(result["ok"])
        self.assertTrue(result["flat"])
        self.assertTrue(result["auto_rebalance_recovery_required"])
        self.assertEqual(
            instance.recovery_reason, "auto_rebalance_recovery_required"
        )

    async def test_sniper_old_error_ledger_keeps_flat_account_locked(self):
        instance = make_instance(SafetyClient())
        instance.sniper_old = FakeSniperOld(locked=True, status="error")

        result = await instance.reconcile_account_safety(startup=True)

        self.assertFalse(result["ok"])
        self.assertTrue(result["flat"])
        self.assertTrue(result["sniper_old_recovery_required"])
        self.assertTrue(instance.recovery_locked)
        self.assertEqual(instance.recovery_reason, "sniper_old_recovery_required")

    async def test_init_summary_counts_failed_accounts_explicitly(self):
        manager = account_manager.AccountManager()
        loaded = SimpleNamespace(recovery_locked=True)
        accounts = [
            {"id": "good", "name": "Good"},
            {"id": "bad", "name": "Bad"},
        ]
        manager._load_account = AsyncMock(
            side_effect=[loaded, RuntimeError("cannot decrypt account")]
        )

        with (
            patch.object(account_manager, "get_global_config", return_value=""),
            patch.object(account_manager, "get_accounts", return_value=accounts),
            patch.object(account_manager, "audit_event"),
            self.assertLogs(account_manager.logger, level="ERROR"),
        ):
            await manager.init()

        self.assertEqual(manager.last_init_summary["discovered_count"], 2)
        self.assertEqual(manager.last_init_summary["loaded_count"], 1)
        self.assertEqual(manager.last_init_summary["failed_count"], 1)
        self.assertEqual(manager.last_init_summary["failed_account_ids"], ["bad"])
        self.assertIn("bad", manager.load_failures)

    async def test_add_account_rolls_back_database_when_live_load_fails(self):
        manager = account_manager.AccountManager()
        with (
            patch.object(
                account_manager,
                "create_account",
                return_value={"id": "new-account", "name": "New"},
            ),
            patch("backend.core.database.update_account") as update_account,
            patch.object(
                manager,
                "_load_account",
                AsyncMock(side_effect=RuntimeError("load failed")),
            ),
            patch.object(account_manager, "delete_account") as delete_account,
        ):
            with self.assertRaisesRegex(RuntimeError, "load failed"):
                await manager.add_account(
                    "New",
                    "api",
                    "secret",
                    "passphrase",
                    "1" * 64,
                    "JP2",
                )

        update_account.assert_called_once_with(
            "new-account", base_url="https://us.okx.com"
        )
        delete_account.assert_called_once_with("new-account")
        self.assertNotIn("new-account", manager.instances)


if __name__ == "__main__":
    unittest.main()
