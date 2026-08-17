import asyncio
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest import mock


try:
    from backend import app as app_module
    OPTIONAL_DEPS_AVAILABLE = True
except ModuleNotFoundError:
    OPTIONAL_DEPS_AVAILABLE = False


def _idle_instance(client):
    idle = SimpleNamespace(running=False)
    monitor = SimpleNamespace(running=False, state=SimpleNamespace(enabled=False))
    return SimpleNamespace(
        client=client,
        strategy_s=idle,
        strategy_d=idle,
        sniper=idle,
        sniper_old=idle,
        poly_monitor=monitor,
    )


def _confirmed_ioc(status="FILLED", filled_size="1"):
    async def place(client, **kwargs):
        order_kwargs = dict(kwargs)
        order_kwargs.pop("market_id", None)
        order_kwargs.pop("attempts", None)
        order_kwargs.pop("delay", None)
        await client.place_order(**order_kwargs)
        return SimpleNamespace(
            response={"code": "0"},
            response_code="0",
            observation=SimpleNamespace(
                status=status,
                order_id="exit-order-1",
                filled_size=Decimal(str(filled_size)),
            ),
            rejected=False,
            unresolved=False,
        )

    return mock.AsyncMock(side_effect=place)


@unittest.skipUnless(OPTIONAL_DEPS_AVAILABLE, "runtime dependencies are not installed")
class CancelAllSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_flash_exit_plan_uses_the_deepest_level_needed_for_full_size(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "YES",
            "size": "5",
            "availableSize": "5",
        }

        class Client:
            def __init__(self):
                self.depths = []

            async def get_orderbook(self, yes_asset_id, depth=1):
                self.depths.append(depth)
                return {
                    "code": "0",
                    "data": [
                        {
                            "bids": [["0.50", "2"], ["0.49", "3"]],
                            "asks": [],
                        }
                    ],
                }

        client = Client()
        plan = await app_module._flash_exit_plan(
            client,
            position,
            account_id="account-a",
            trace_id="trace-a",
            phase="preflight",
        )

        self.assertEqual(client.depths, [50])
        self.assertEqual(plan["best_bid"], "0.49")
        self.assertEqual(plan["quote_capacity"], Decimal("5"))
        self.assertEqual(plan["levels_used"], 2)

    async def test_strategy_lifecycle_wrapper_serializes_same_account(self):
        instance = SimpleNamespace(strategy_lifecycle_lock=asyncio.Lock())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        active = 0
        maximum = 0

        @app_module._serialize_strategy_lifecycle
        async def operation(account_id):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0)
            active -= 1
            return account_id

        with mock.patch.object(app_module, "manager", manager):
            results = await asyncio.gather(
                operation("account-a"), operation("account-a")
            )

        self.assertEqual(results, ["account-a", "account-a"])
        self.assertEqual(maximum, 1)

    async def test_initial_query_failure_is_not_reported_as_no_orders(self):
        class Client:
            async def get_orders(self, status="open"):
                return {"code": "500", "message": "unavailable"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_cancel_all("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertIn("Cannot verify open orders", result["error"])

    async def test_missing_order_list_is_not_treated_as_verified_empty(self):
        class Client:
            async def get_orders(self, status="open"):
                return {"code": "0", "data": {}}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_cancel_all("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertIn("invalid response shape", result["error"])

    async def test_cancel_all_does_not_report_success_when_strategy_stop_is_unsafe(self):
        class UnsafeManual:
            running = True
            state = SimpleNamespace(enabled=False)

            async def stop(self):
                self.running = False
                return False

        class Client:
            async def get_orders(self, status="open", market_id=None):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                return {"code": "0", "data": {"list": []}}

        instance = _idle_instance(Client())
        instance.strategy_d = UnsafeManual()
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_cancel_all("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(
            instance.recovery_reason,
            "cancel_all_strategy_stop_incomplete",
        )
        self.assertFalse(result["stop"]["manual_d_safe"])

    async def test_missing_position_list_is_not_treated_as_verified_flat(self):
        class Client:
            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                return {"code": "0", "data": {}}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertIn("invalid response shape", result["error"])

    async def test_ask_sell_does_not_treat_an_order_query_failure_as_empty(self):
        class Client:
            async def get_orders(self, status="open"):
                return {"code": "500", "message": "unavailable"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_ask_sell("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertIn("Cannot verify open orders", result["error"])

    async def test_remaining_orders_are_not_reported_as_cancelled(self):
        order = {"assetId": "123000", "oid": "order-1"}

        class Client:
            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": [order]}}

            async def cancel_orders_batch(self, cancels):
                return {"code": "500", "message": "batch unavailable"}

            async def cancel_order(self, asset_id, order_id):
                return {"code": "0"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_cancel_all("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(result["result"]["failed"], 1)

    async def test_single_cancel_rejection_is_not_reported_as_success(self):
        class Client:
            async def cancel_order(self, asset_id, order_id):
                return {"code": "51000", "message": "order is already final"}

        class Request:
            async def json(self):
                return {"asset_id": "123000", "order_id": "order-1"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_cancel_order(
                "account-a", Request(), user="test"
            )

        payload = json.loads(result.body)
        self.assertFalse(payload["ok"])
        self.assertEqual(result.status_code, 409)
        self.assertIn("terminal order state", payload["error"])

    async def test_flash_stops_when_open_orders_cannot_be_verified(self):
        class Client:
            def __init__(self):
                self.position_queries = 0

            async def get_orders(self, status="open"):
                return {"code": "500", "message": "unavailable"}

            async def get_positions(self, status="open"):
                self.position_queries += 1
                return {"code": "0", "data": {"list": []}}

        client = Client()
        instance = _idle_instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertIn("Cannot verify open orders", result["error"])
        self.assertEqual(client.position_queries, 0)

    async def test_flash_preflight_preserves_protection_when_exit_book_is_empty(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2",
            "availableSize": "0",
        }
        protective_order = {
            "marketId": "market-1",
            "assetId": "market-1000",
            "oid": "protective-1",
            "side": "SELL",
        }

        class Client:
            def __init__(self):
                self.cancel_calls = []

            async def get_orders(self, status="open", market_id=None):
                return {"code": "0", "data": {"list": [protective_order]}}

            async def get_positions(self, status="open"):
                return {"code": "0", "data": {"list": [position]}}

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {"code": "0", "data": [{"bids": [], "asks": []}]}

            async def cancel_order(self, **kwargs):
                self.cancel_calls.append(kwargs)
                return {"code": "0"}

        client = Client()
        instance = _idle_instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertTrue(result["result"]["protection_preserved"])
        self.assertEqual(result["result"]["cancelled"], 0)
        self.assertEqual(client.cancel_calls, [])

    async def test_flash_refreshes_available_size_after_protection_is_cancelled(self):
        initial_position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2",
            "availableSize": "0",
        }
        released_position = {**initial_position, "availableSize": "2"}
        protective_order = {
            "marketId": "market-1",
            "assetId": "market-1000",
            "oid": "protective-1",
            "side": "SELL",
        }

        class Client:
            def __init__(self):
                self.order_queries = 0
                self.position_snapshots = [
                    [initial_position],
                    [released_position],
                    [],
                    [],
                ]
                self.place_calls = []

            async def get_orders(self, status="open", market_id=None):
                self.order_queries += 1
                rows = [protective_order] if self.order_queries == 1 else []
                return {"code": "0", "data": {"list": rows}}

            async def get_positions(self, status="open"):
                return {
                    "code": "0",
                    "data": {"list": self.position_snapshots.pop(0)},
                }

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {
                    "code": "0",
                    "data": [{"bids": [["0.45", "10"]], "asks": []}],
                }

            async def place_order(self, **kwargs):
                self.place_calls.append(kwargs)
                return {"code": "0"}

        client = Client()
        instance = _idle_instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        selected_cancel = mock.AsyncMock(
            return_value={
                "ok": True,
                "cancelled": 1,
                "failed": 0,
                "total": 1,
                "remaining_orders": [],
            }
        )
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_cancel_selected_orders_verified",
                new=selected_cancel,
            ),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=_confirmed_ioc("FILLED", "2"),
            ),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["cancelled"], 1)
        selected_cancel.assert_awaited_once()
        self.assertEqual(client.place_calls[0]["size"], "2")

    async def test_flash_uses_reduce_only_and_verifies_the_position_closed(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2.5",
            "availableSize": "2.5",
        }

        class Client:
            def __init__(self):
                self.position_snapshots = [[position], [], []]
                self.place_calls = []

            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                return {"code": "0", "data": {"list": self.position_snapshots.pop(0)}}

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {"code": "0", "data": [{"bids": [["0.45", "10"]], "asks": []}]}

            async def place_order(self, **kwargs):
                self.place_calls.append(kwargs)
                return {"code": "0"}

        client = Client()
        instance = _idle_instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=_confirmed_ioc("FILLED", "2.5"),
            ),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["submitted"], 1)
        self.assertEqual(result["result"]["closed"], 1)
        self.assertNotIn("sold", result["result"])
        self.assertEqual(len(client.place_calls), 1)
        self.assertTrue(client.place_calls[0]["reduce_only"])
        self.assertEqual(client.place_calls[0]["tif"], "ioc")

    async def test_flash_does_not_report_an_accepted_ioc_as_a_closed_position(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "2",
            "tokenName": "No",
            "size": "3",
            "availableSize": "3",
        }

        class Client:
            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                return {"code": "0", "data": {"list": [position]}}

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {"code": "0", "data": [{"bids": [], "asks": [["0.60", "10"]]}]}

            async def place_order(self, **kwargs):
                return {"code": "0"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=_confirmed_ioc("CANCELLED", "0"),
            ),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(result["result"]["submitted"], 5)
        self.assertEqual(result["result"]["closed"], 0)
        self.assertEqual(result["result"]["remaining"], 1)

    async def test_flash_restores_protection_after_repeated_terminal_ioc_failure(self):
        initial_position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "3",
            "availableSize": "0",
        }
        released_position = {**initial_position, "availableSize": "3"}
        old_sell = {
            "marketId": "market-1",
            "assetId": "market-1000",
            "side": "SELL",
            "oid": "old-sell",
            "clientOrderId": "old-cloid",
            "price": "0.50",
            "size": "3",
        }

        class Client:
            def __init__(self):
                self.order_queries = 0
                self.position_queries = 0
                self.restored = False
                self.place_calls = []

            async def get_orders(self, status="open", market_id=None):
                self.order_queries += 1
                if self.order_queries == 1:
                    rows = [old_sell]
                elif self.restored:
                    rows = [{
                        "marketId": "market-1",
                        "assetId": "market-1000",
                        "side": "SELL",
                        "oid": "restored-order",
                        "clientOrderId": "restored-cloid",
                        "price": "0.50",
                        "size": "3",
                    }]
                else:
                    rows = []
                return {"code": "0", "data": {"list": rows}}

            async def get_positions(self, status="open"):
                self.position_queries += 1
                rows = [initial_position] if self.position_queries == 1 else [released_position]
                return {"code": "0", "data": {"list": rows}}

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {
                    "code": "0",
                    "data": [{"bids": [["0.45", "10"]], "asks": []}],
                }

            async def place_order(self, **kwargs):
                self.place_calls.append(kwargs)
                if kwargs["tif"] == "gtc":
                    self.restored = True
                return {"code": "0"}

        client = Client()
        instance = _idle_instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        async def confirm(_client, **kwargs):
            await client.place_order(**{
                key: value
                for key, value in kwargs.items()
                if key not in {"market_id", "attempts", "delay"}
            })
            status = "ACTIVE" if kwargs["tif"] == "gtc" else "CANCELLED"
            return SimpleNamespace(
                response={"code": "0"},
                response_code="0",
                observation=SimpleNamespace(
                    status=status,
                    order_id=("restored-order" if status == "ACTIVE" else "exit-order"),
                    filled_size=Decimal("0"),
                ),
                rejected=False,
                unresolved=False,
            )

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_cancel_selected_orders_verified",
                new=mock.AsyncMock(
                    return_value={
                        "ok": True,
                        "cancelled": 1,
                        "failed": 0,
                        "total": 1,
                        "remaining_orders": [],
                    }
                ),
            ),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=mock.AsyncMock(side_effect=confirm),
            ),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(result["result"]["submitted"], 5)
        self.assertTrue(result["result"]["protection_complete"])
        self.assertTrue(result["result"]["protection_restored"]["market-1000"]["ok"])
        self.assertEqual(client.place_calls[-1]["tif"], "gtc")

    async def test_flash_final_query_failure_preserves_submitted_progress(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2",
            "availableSize": "2",
        }

        class Client:
            def __init__(self):
                self.position_queries = 0

            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                self.position_queries += 1
                if self.position_queries == 1:
                    return {"code": "0", "data": {"list": [position]}}
                return {"code": "TIMEOUT", "message": "unavailable"}

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {"code": "0", "data": [{"bids": [["0.45", "5"]], "asks": []}]}

            async def place_order(self, **kwargs):
                return {"code": "0"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=_confirmed_ioc("FILLED", "2"),
            ),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(result["result"]["submitted"], 1)
        self.assertIsNone(result["result"]["remaining"])
        self.assertIn("verification", result["result"]["failures"][0])

    async def test_flash_waits_for_pending_ioc_terminal_without_cancelling_it(self):
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2",
            "availableSize": "2",
        }

        class Client:
            def __init__(self):
                self.placed = False
                self.current_status = ""
                self.poll_index = 0
                self.place_calls = []
                self.cancel_calls = []
                self.client_order_id = ""

            async def get_orders(self, status="open", market_id=None):
                if not self.placed:
                    return {"code": "0", "data": {"list": []}}
                statuses = ["PENDING_PLACE", "PENDING_PLACE", "FILLED"]
                if status == "open":
                    self.current_status = statuses[min(self.poll_index, 2)]
                    self.poll_index += 1
                current = self.current_status
                terminal = {"FILLED", "PARTIALLY_FILLED", "FAILED", "CANCELLED", "EXPIRED"}
                include = (current in terminal) == (status == "closed")
                rows = []
                if include:
                    rows = [{
                        "oid": "exit-order-1",
                        "clientOrderId": self.client_order_id,
                        "marketId": "market-1",
                        "assetId": "market-1000",
                        "side": "SELL",
                        "status": current,
                        "price": "0.45",
                        "size": "2",
                        "filledSize": "2" if current == "FILLED" else "0",
                        "filledAmount": "0.9" if current == "FILLED" else "0",
                    }]
                return {"code": "0", "data": {"list": rows}}

            async def get_positions(self, status="open"):
                rows = [] if self.current_status == "FILLED" else [position]
                return {"code": "0", "data": {"list": rows}}

            async def get_orderbook(self, yes_asset_id, depth=1):
                return {"code": "0", "data": [{"bids": [["0.45", "5"]], "asks": []}]}

            async def place_order(self, **kwargs):
                self.place_calls.append(kwargs)
                self.client_order_id = kwargs["client_order_id"]
                self.placed = True
                return {"code": "0", "data": {"txHash": "exit-tx"}}

            async def cancel_order(self, **kwargs):
                self.cancel_calls.append(kwargs)
                return {"code": "0"}

        client = Client()
        instance = _idle_instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["closed"], 1)
        self.assertEqual(client.cancel_calls, [])
        self.assertEqual(client.place_calls[0]["tif"], "ioc")

    async def test_delete_removes_account_without_exchange_flattening(self):
        instance = _idle_instance(object())
        removal = {
            "deleted": True,
            "cleanup_complete": True,
            "state_cleanup": {"account_id": "account-a", "complete": True},
        }
        remove = mock.AsyncMock(return_value=removal)
        manager = SimpleNamespace(
            get_instance=lambda _account_id: instance,
            remove_account=remove,
        )
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_flash_close_account",
                new=mock.AsyncMock(),
            ) as mocked_flash,
        ):
            response = await app_module.remove_account("account-a", user="test")

        self.assertTrue(response["ok"])
        self.assertTrue(response["deleted"])
        self.assertTrue(response["exchange_state_not_modified"])
        remove.assert_awaited_once_with("account-a")
        mocked_flash.assert_not_awaited()

    async def test_delete_reports_local_cleanup_after_local_removal(self):
        instance = _idle_instance(object())
        removal = {
            "deleted": True,
            "cleanup_complete": True,
            "state_cleanup": {
                "account_id": "account-a",
                "complete": True,
                "files": [],
                "deleted": [],
                "missing": [],
                "errors": [],
            },
        }
        remove = mock.AsyncMock(return_value=removal)
        manager = SimpleNamespace(
            get_instance=lambda _account_id: instance,
            remove_account=remove,
        )
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_flash_close_account",
                new=mock.AsyncMock(),
            ) as mocked_flash,
        ):
            result = await app_module.remove_account("account-a", user="test")

        self.assertTrue(result["ok"])
        self.assertTrue(result["deleted"])
        self.assertTrue(result["cleanup_complete"])
        self.assertEqual(result["state_cleanup"], removal["state_cleanup"])
        self.assertTrue(result["exchange_state_not_modified"])
        remove.assert_awaited_once_with("account-a")
        mocked_flash.assert_not_awaited()

    async def test_delete_reports_partial_state_file_cleanup_after_database_delete(self):
        instance = _idle_instance(object())
        removal = {
            "deleted": True,
            "cleanup_complete": False,
            "state_cleanup": {
                "account_id": "account-a",
                "complete": False,
                "files": [
                    {
                        "name": "cost_layers_account-a.json",
                        "status": "error",
                        "error": "PermissionError: denied",
                    }
                ],
                "deleted": [],
                "missing": [],
                "errors": [
                    {
                        "name": "cost_layers_account-a.json",
                        "status": "error",
                        "error": "PermissionError: denied",
                    }
                ],
            },
        }
        manager = SimpleNamespace(
            get_instance=lambda _account_id: instance,
            remove_account=mock.AsyncMock(return_value=removal),
        )
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_flash_close_account",
                new=mock.AsyncMock(),
            ) as mocked_flash,
        ):
            payload = await app_module.remove_account("account-a", user="test")

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["deleted"])
        self.assertFalse(payload["cleanup_complete"])
        self.assertEqual(payload["state_cleanup"], removal["state_cleanup"])
        self.assertIn("credentials were deleted", payload["warning"])
        self.assertTrue(payload["exchange_state_not_modified"])
        mocked_flash.assert_not_awaited()

    async def test_flash_reconciles_a_stopped_sniper_lock_after_exchange_is_flat(self):
        class Client:
            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                return {"code": "0", "data": {"list": []}}

        class Sniper:
            def __init__(self):
                self.running = False
                self.state = SimpleNamespace(locked=True)
                self.stop_calls = 0

            async def stop(self):
                self.stop_calls += 1
                self.state.locked = False
                return {"ok": True, "needs_reconciliation": False}

        instance = _idle_instance(Client())
        instance.sniper = Sniper()
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertTrue(result["ok"])
        self.assertEqual(instance.sniper.stop_calls, 1)
        self.assertFalse(result["result"]["needs_reconciliation"])


if __name__ == "__main__":
    unittest.main()
