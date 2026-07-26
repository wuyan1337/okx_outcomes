import asyncio
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


@unittest.skipUnless(OPTIONAL_DEPS_AVAILABLE, "runtime dependencies are not installed")
class CancelAllSafetyTests(unittest.IsolatedAsyncioTestCase):
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

        self.assertFalse(result["ok"])
        self.assertIn("already final", result["error"])

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

    async def test_flash_uses_reduce_only_and_verifies_the_position_closed(self):
        order = {"assetId": "market-1000", "oid": "order-1"}
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "2.5",
            "availableSize": "2.5",
        }

        class Client:
            def __init__(self):
                self.order_snapshots = [[order], [], []]
                self.position_snapshots = [[position], []]
                self.place_calls = []

            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": self.order_snapshots.pop(0)}}

            async def cancel_orders_batch(self, cancels):
                return {"code": "0"}

            async def cancel_order(self, asset_id, order_id):
                return {"code": "0"}

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
                return {"code": "0", "data": [{"bids": [], "asks": [["0.60", "1"]]}]}

            async def place_order(self, **kwargs):
                return {"code": "0"}

        instance = _idle_instance(Client())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(result["result"]["submitted"], 1)
        self.assertEqual(result["result"]["closed"], 0)
        self.assertEqual(result["result"]["remaining"], 1)

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
            mock.patch.object(app_module.asyncio, "sleep", new=mock.AsyncMock()),
        ):
            result = await app_module.account_flash_close("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertEqual(result["result"]["submitted"], 1)
        self.assertIsNone(result["result"]["remaining"])
        self.assertIn("final verification", result["result"]["failures"][0])

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
