import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from backend.services.account_manager import AccountInstance
from backend.services.auto_rebalance import AutoRebalance
from backend.services.poly_monitor import PolyMonitor


def single_params(**overrides):
    params = {
        "strategy_type": "manual-s",
        "market_id": "market-1",
        "yes_asset_id": "yes-asset",
        "no_asset_id": "no-asset",
        "asset_id": "yes-asset",
        "side": "yes",
        "price_cents": Decimal("55"),
        "sell_price_cents": Decimal("56"),
        "asset_cost_cents": {"yes-asset": Decimal("55")},
        "mode": "classic",
        "stabilize_secs": 0,
        "start_time": 1,
    }
    params.update(overrides)
    return params


class PositionAndOrderClient:
    def __init__(self, positions=None, orders=None):
        self.positions = positions or []
        self.orders = orders or []
        self.place_calls = []

    async def get_positions(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": list(self.positions)}}

    async def get_orders(self, status="closed", market_id=None):
        return {"code": "0", "data": {"list": list(self.orders)}}

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return {"code": "0", "data": {"oid": f"sell-{len(self.place_calls)}"}}


class RebalanceLayerTests(unittest.IsolatedAsyncioTestCase):
    def test_same_price_layers_do_not_cross_market_or_asset(self):
        rebalance = AutoRebalance(object(), "scope-no-file")
        rebalance.cost_layers = {}
        rebalance._save_layers = lambda: None
        rebalance.add_to_layer("asset-a", 55, 2, market_id="market-a")
        rebalance.add_to_layer("asset-b", 55, 3, market_id="market-a")
        rebalance.add_to_layer("asset-a", 55, 4, market_id="market-b")

        rebalance.remove_from_highest_layer(
            "asset-a", 1, market_id="market-a"
        )

        self.assertEqual(
            rebalance._layers_for("asset-a", "market-a"),
            {Decimal("55"): 1.0},
        )
        self.assertEqual(
            rebalance._layers_for("asset-b", "market-a"),
            {Decimal("55"): 3.0},
        )
        self.assertEqual(
            rebalance._layers_for("asset-a", "market-b"),
            {Decimal("55"): 4.0},
        )

    def test_legacy_flat_file_is_quarantined(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "layers.json"
            path.write_text(json.dumps({"55": 12}), encoding="utf-8")
            rebalance = AutoRebalance(object(), "legacy-test")
            rebalance._layers_file = path
            rebalance.cost_layers = {}
            rebalance.legacy_cost_layers = {}
            with self.assertLogs(level="WARNING"):
                rebalance._load_layers()
            self.assertEqual(rebalance.cost_layers, {})
            self.assertEqual(rebalance.legacy_cost_layers, {Decimal("55"): 12.0})

    async def test_partial_fill_capture_is_idempotent_across_triggers(self):
        client = PositionAndOrderClient(
            positions=[{"assetId": "yes-asset", "size": "3"}],
            orders=[{
                "oid": "buy-partial-1",
                "assetId": "yes-asset",
                "side": "BUY",
                "status": "PARTIALLY_FILLED",
                "createdAt": "2",
                "filledSize": "3",
                "price": "0.551",
            }],
        )
        rebalance = AutoRebalance(client, "idempotent-no-file")
        rebalance.cost_layers = {}
        rebalance.captured_buy_fills = {}
        rebalance._save_layers = lambda: None
        params = single_params()
        assets = rebalance._active_assets(params)

        await rebalance._capture_partial_fills(params, assets)
        await rebalance._capture_partial_fills(params, assets)

        self.assertEqual(
            rebalance._layers_for("yes-asset", "market-1"),
            {Decimal("55.100"): 3.0},
        )
        self.assertEqual(
            rebalance.captured_buy_fills["market-1|yes-asset|buy-partial-1"],
            3.0,
        )

    async def test_manual_d_reconciles_yes_and_no_at_their_own_costs(self):
        client = PositionAndOrderClient(positions=[
            {
                "tokenId": "0x-not-an-okx-asset-id",
                "marketId": "42",
                "tokenIndex": "1",
                "size": "5",
            },
            {
                "tokenId": "0x-also-not-an-okx-asset-id",
                "marketId": "42",
                "tokenIndex": "2",
                "size": "6",
            },
        ])
        rebalance = AutoRebalance(client, "dual-no-file")
        rebalance.cost_layers = {}
        rebalance._save_layers = lambda: None
        params = {
            "strategy_type": "manual-d",
            "market_id": "42",
            "yes_asset_id": "42000",
            "no_asset_id": "42100",
            "asset_id": "42000",
            "asset_cost_cents": {
                "42000": Decimal("55"),
                "42100": Decimal("44"),
            },
        }
        assets = rebalance._active_assets(params)

        await rebalance._reconcile_with_positions(params=params, assets=assets)

        self.assertEqual(
            rebalance._layers_for("42000", "42"), {Decimal("55"): 5.0}
        )
        self.assertEqual(
            rebalance._layers_for("42100", "42"), {Decimal("44"): 6.0}
        )

    async def test_below_minimum_full_position_uses_reduce_only_sell_all(self):
        client = PositionAndOrderClient()
        rebalance = AutoRebalance(client, "sell-all-no-file")
        rebalance.cost_layers = {
            "market-1": {"yes-asset": {Decimal("0.1"): 100.0}}
        }
        params = single_params(
            price_cents=Decimal("0.1"),
            sell_price_cents=Decimal("0.2"),
            asset_cost_cents={"yes-asset": Decimal("0.1")},
        )

        count = await rebalance._post_layer_sells(
            params,
            rebalance._active_assets(params),
            {"yes-asset": Decimal("0.2")},
        )

        self.assertEqual(count, 1)
        self.assertEqual(client.place_calls[0]["size"], "100")
        self.assertTrue(client.place_calls[0]["reduce_only"])


class RebalanceFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_d_full_flow_reposts_both_assets(self):
        client = PositionAndOrderClient(positions=[
            {"assetId": "yes-asset", "size": "10"},
            {"assetId": "no-asset", "size": "20"},
        ])
        rebalance = AutoRebalance(client, "dual-full-flow-no-file")
        rebalance.cost_layers = {}
        rebalance._save_layers = lambda: None
        params = {
            "strategy_type": "manual-d",
            "market_id": "market-1",
            "yes_asset_id": "yes-asset",
            "no_asset_id": "no-asset",
            "asset_id": "yes-asset",
            "price_cents": Decimal("55"),
            "sell_price_cents": Decimal("56"),
            "asset_cost_cents": {
                "yes-asset": Decimal("55"),
                "no-asset": Decimal("44"),
            },
            "stabilize_secs": 0,
            "start_time": 1,
        }
        rebalance.get_strategy_params = lambda: params
        rebalance._wait_for_stable_price = AsyncMock(return_value=Decimal("0.56"))
        rebalance.restart_monitor = AsyncMock(return_value={"ok": True})
        rebalance.restart_strategy = AsyncMock(return_value={"ok": True})
        rebalance.stop_monitor = AsyncMock()

        result = await rebalance._do_rebalance(
            "test", "poly", Decimal("55"), "manual-d"
        )

        self.assertTrue(result)
        self.assertEqual(
            [call["asset_id"] for call in client.place_calls],
            ["yes-asset", "no-asset"],
        )
        self.assertTrue(all(call["reduce_only"] for call in client.place_calls))

    async def test_cancelled_rebalance_stops_partial_restart_components(self):
        import asyncio

        rebalance = AutoRebalance(object(), "cancel-lifecycle-no-file")
        events = []
        rebalance._rebalancing = True
        rebalance._task = asyncio.create_task(asyncio.sleep(60))

        async def stop_strategy():
            events.append("strategy")

        async def stop_monitor():
            events.append("monitor")

        rebalance.stop_strategy = stop_strategy
        rebalance.stop_monitor = stop_monitor

        await rebalance.cancel()

        self.assertFalse(rebalance.rebalancing)
        self.assertEqual(events, ["strategy", "monitor"])

    async def test_same_price_still_syncs_and_reposts_before_restart(self):
        rebalance = AutoRebalance(object(), "same-price-no-file")
        params = single_params()
        rebalance.get_strategy_params = lambda: params
        calls = []

        async def step(name, result=None):
            calls.append(name)
            return result

        rebalance._cancel_all_verified = lambda: step("cancel", {"ok": True})
        rebalance._capture_partial_fills = lambda *_: step("capture")
        rebalance._reconcile_with_positions = lambda *_args, **_kwargs: step(
            "reconcile"
        )
        rebalance._wait_for_stable_price = lambda *_args, **_kwargs: step(
            "stable", Decimal("0.55")
        )
        rebalance._sync_positions = lambda *_: step("sync")
        rebalance._post_layer_sells = lambda *_: step("sells", 1)
        rebalance._restart = lambda *_: step("restart")
        rebalance._notify = lambda *_: step("notify")

        result = await rebalance._do_rebalance(
            "test", "poly", Decimal("55"), "manual-s"
        )

        self.assertTrue(result)
        self.assertLess(calls.index("sync"), calls.index("sells"))
        self.assertLess(calls.index("sells"), calls.index("restart"))

    async def test_bid_sell_stable_poly_bid_floors_then_becomes_sell_level(self):
        rebalance = AutoRebalance(object(), "bid-sell-floor-no-file")
        params = single_params(
            mode="bid-sell",
            price_cents=Decimal("54"),
            sell_price_cents=Decimal("55"),
            asset_cost_cents={"yes-asset": Decimal("54")},
        )
        rebalance.get_strategy_params = lambda: params
        observed = {}

        async def no_op(*_args, **_kwargs):
            return None

        async def stable(*_args, **_kwargs):
            return Decimal("0.551")

        async def sells(_params, _assets, targets):
            observed["sell_target"] = targets["yes-asset"]
            return 0

        async def restart(price_cents, *_args):
            observed["restart_price"] = price_cents

        rebalance._cancel_all_verified = no_op
        rebalance._capture_partial_fills = no_op
        rebalance._reconcile_with_positions = no_op
        rebalance._sync_positions = no_op
        rebalance._wait_for_stable_price = stable
        rebalance._post_layer_sells = sells
        rebalance._restart = restart
        rebalance._notify = no_op

        result = await rebalance._do_rebalance(
            "test", "poly", Decimal("55"), "manual-s"
        )

        self.assertTrue(result)
        self.assertEqual(observed["restart_price"], Decimal("55.00"))
        self.assertEqual(observed["sell_target"], Decimal("55.00"))

    async def test_cancel_verification_error_is_a_hard_failure_without_cancel_all(self):
        class Client:
            def __init__(self):
                self.cancel_all_calls = 0

            async def cancel_all(self, _ids):
                self.cancel_all_calls += 1

            async def get_orders(self, status="open"):
                return {"code": "TIMEOUT", "msg": "timed out"}

        client = Client()
        rebalance = AutoRebalance(client, "cancel-query-no-file")
        with self.assertRaisesRegex(RuntimeError, "无法确认开放订单"):
            await rebalance._cancel_all_verified()
        self.assertEqual(client.cancel_all_calls, 0)

    async def test_restart_starts_monitor_first_and_stops_it_if_strategy_fails(self):
        rebalance = AutoRebalance(object(), "restart-order-no-file")
        calls = []

        async def monitor(*_args):
            calls.append("monitor")
            return {"ok": True}

        async def strategy(*_args):
            calls.append("strategy")
            raise RuntimeError("strategy refused")

        async def stop_monitor():
            calls.append("stop-monitor")

        rebalance.restart_monitor = monitor
        rebalance.restart_strategy = strategy
        rebalance.stop_monitor = stop_monitor

        with self.assertRaisesRegex(RuntimeError, "strategy refused"):
            await rebalance._restart(
                Decimal("55"), "poly", Decimal("0.55"), "manual-s"
            )
        self.assertEqual(calls, ["monitor", "strategy", "stop-monitor"])

    async def test_monitor_restart_failure_never_starts_strategy(self):
        rebalance = AutoRebalance(object(), "monitor-fail-no-file")
        strategy = AsyncMock(return_value={"ok": True})
        rebalance.restart_strategy = strategy
        rebalance.restart_monitor = AsyncMock(
            return_value={"ok": False, "error": "monitor unavailable"}
        )

        with self.assertRaisesRegex(RuntimeError, "monitor unavailable"):
            await rebalance._restart(
                Decimal("55"), "poly", Decimal("0.55"), "manual-s"
            )
        strategy.assert_not_awaited()

    async def test_protective_sell_failure_prevents_restart_and_success(self):
        rebalance = AutoRebalance(object(), "sell-fail-flow-no-file")
        params = single_params()
        rebalance.get_strategy_params = lambda: params
        notices = []
        restarted = False

        async def no_op(*_args, **_kwargs):
            return None

        async def stable(*_args, **_kwargs):
            return Decimal("0.56")

        async def failed_sell(*_args, **_kwargs):
            raise RuntimeError("protective sell rejected")

        async def restart(*_args, **_kwargs):
            nonlocal restarted
            restarted = True

        async def notify(message):
            notices.append(message)

        rebalance._cancel_all_verified = no_op
        rebalance._capture_partial_fills = no_op
        rebalance._reconcile_with_positions = no_op
        rebalance._sync_positions = no_op
        rebalance._wait_for_stable_price = stable
        rebalance._post_layer_sells = failed_sell
        rebalance._restart = restart
        rebalance._notify = notify

        result = await rebalance._do_rebalance(
            "test", "poly", Decimal("55"), "manual-s"
        )

        self.assertFalse(result)
        self.assertFalse(restarted)
        self.assertFalse(any("Auto-Rebalance 完成" in item for item in notices))


class MonitorCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_open_order_query_never_reports_cancel_success(self):
        class Client:
            async def cancel_all(self, _ids):
                return {"code": "0"}

            async def get_orders(self, status="open"):
                return {"code": "TIMEOUT", "msg": "timed out"}

        monitor = PolyMonitor(Client(), "monitor-cancel-no-file")
        with unittest.mock.patch(
            "backend.services.poly_monitor.asyncio.sleep", new=AsyncMock()
        ):
            result = await monitor._fast_cancel_all()
        self.assertFalse(result["ok"])
        self.assertIn("verification failed", result["error"])

    async def test_trigger_blocks_rebalance_when_cancel_is_unverified(self):
        monitor = PolyMonitor(object(), "monitor-trigger-no-file")
        monitor.state.poly_token_id = "poly"
        monitor._strategy_type = "manual-s"
        monitor._old_price_cents = Decimal("55")
        monitor._fast_cancel_all = AsyncMock(return_value={"ok": False})
        monitor.on_trigger_callback = AsyncMock()
        auto = SimpleNamespace(enabled=True, execute=AsyncMock())
        monitor.auto_rebalance = auto

        await monitor._trigger("risk")

        auto.execute.assert_not_awaited()


class AccountRestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_reload_cancels_rebalance_and_propagates_new_client(self):
        events = []

        class OldClient:
            async def close(self):
                events.append("old-client-close")

        class NewClient:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.signer = SimpleNamespace(address="0x1")

        class Auto:
            def __init__(self):
                self.client = None

            async def cancel(self):
                events.append("rebalance-cancel")

        instance = AccountInstance.__new__(AccountInstance)
        instance.account_id = "account-1"
        instance.client = OldClient()
        instance.auto_rebalance = Auto()
        instance.strategy_d = SimpleNamespace(client=None)
        instance.strategy_s = SimpleNamespace(client=None)
        instance.poly_monitor = SimpleNamespace(client=None)
        instance.sniper = SimpleNamespace(client=None)
        instance.sniper_old = SimpleNamespace(client=None)
        account = {
            "api_key": "api",
            "secret_key": "secret",
            "passphrase": "pass",
            "agent_private_key": "agent",
            "base_url": "https://us.okx.com",
            "proxy_node": "JP2",
        }

        with patch(
            "backend.services.account_manager.get_account_decrypted",
            return_value=account,
        ), patch("backend.services.account_manager.OKXOutcomesClient", NewClient):
            await instance.reload_client()

        self.assertEqual(events, ["rebalance-cancel", "old-client-close"])
        self.assertIs(instance.auto_rebalance.client, instance.client)
        self.assertIs(instance.poly_monitor.client, instance.client)
        self.assertIs(instance.strategy_d.client, instance.client)
        self.assertIs(instance.sniper.client, instance.client)

    async def test_close_cancels_rebalance_before_closing_client(self):
        events = []

        class Auto:
            async def cancel(self):
                events.append("rebalance-cancel")

        class Component:
            running = False

            async def stop(self):
                events.append("component-stop")

        class Client:
            async def close(self):
                events.append("client-close")

        instance = AccountInstance.__new__(AccountInstance)
        instance.auto_rebalance = Auto()
        instance.strategy_d = Component()
        instance.strategy_s = Component()
        instance.poly_monitor = Component()
        instance.sniper = Component()
        instance.sniper_old = Component()
        instance.client = Client()

        await instance.close(cancel_orders=False)

        self.assertEqual(events, ["rebalance-cancel", "client-close"])

    async def test_bid_sell_restart_keeps_point_one_cent_and_sell_monitor_ref(self):
        class StrategyS:
            def __init__(self):
                self.running = False
                self.state = SimpleNamespace(
                    running=False,
                    mode="bid-sell",
                    side="yes",
                    price_a=0.1,
                    sell_price=1.1,
                    market_id="market-1",
                    yes_asset_id="yes-asset",
                    no_asset_id="no-asset",
                    asset_id="yes-asset",
                    position_pct=100,
                    scan_interval=0.3,
                    stabilize_secs=10,
                    start_time=100,
                )
                self.start_kwargs = None

            async def start(self, **kwargs):
                self.start_kwargs = kwargs
                self.running = True
                self.state.running = True
                self.state.price_a = float(kwargs["price_a"])
                self.state.sell_price = float(kwargs["sell_price"])

        class Monitor:
            def __init__(self):
                self._strategy_type = "manual-s"
                self._running = False
                self.state = SimpleNamespace(
                    depth_threshold=4321.0, triggered=True, trigger_reason="risk", enabled=False
                )
                self.start_kwargs = None
                self.context = None

            async def start(self, **kwargs):
                self.start_kwargs = kwargs
                self._running = True
                return {"ok": True}

            def set_strategy_context(self, *args, **kwargs):
                self.context = (args, kwargs)

        instance = AccountInstance.__new__(AccountInstance)
        instance.strategy_s = StrategyS()
        instance.strategy_d = SimpleNamespace(
            running=False,
            state=SimpleNamespace(
                running=False, start_time=0, yes_asset_id="", price_a=0
            ),
        )
        instance.poly_monitor = Monitor()
        instance.auto_rebalance = AutoRebalance(object(), "account-restart-no-file")

        await instance._restart_monitor(Decimal("0.551"), "poly")
        await instance._restart_strategy(Decimal("1.1"), "manual-s")

        self.assertEqual(
            Decimal(instance.strategy_s.start_kwargs["price_a"]), Decimal("0.1")
        )
        self.assertEqual(
            Decimal(instance.strategy_s.start_kwargs["sell_price"]), Decimal("1.1")
        )
        self.assertEqual(instance.poly_monitor.start_kwargs["depth_threshold"], 4321.0)
        # Poly 55.10¢ is a bid: floor to the 55¢ OKX tick, then use that
        # resulting level as Bid-Sell's SELL monitor reference.
        self.assertEqual(instance.poly_monitor.context[0][1], Decimal("55.00"))


if __name__ == "__main__":
    unittest.main()
