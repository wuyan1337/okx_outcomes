import asyncio
from decimal import Decimal
import inspect
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from backend.core.price_utils import (
    align_cents_for_okx,
    align_okx_price,
    format_okx_price,
    format_poly_cents,
    is_okx_price_aligned,
    okx_tick,
)
from backend.services.manual_strategy_d import ManualStrategyD
from backend.services.manual_strategy_s import ManualStrategyS
from backend.services.auto_rebalance import AutoRebalance

try:
    from backend import app as app_module
    from backend.services.account_manager import AccountManager
    from backend.services.poly_monitor import PolyMonitor
    from backend.ws_book import (
        _apply_poly_price_changes,
        _invert_okx_book_arrays,
        _poly_book_from_maps,
        _poly_book_maps,
    )
    OPTIONAL_DEPS_AVAILABLE = True
except ModuleNotFoundError:
    OPTIONAL_DEPS_AVAILABLE = False


class PriceUtilsTests(unittest.TestCase):
    def test_okx_tick_boundaries(self):
        self.assertEqual(okx_tick("0.039"), Decimal("0.001"))
        self.assertEqual(okx_tick("0.04"), Decimal("0.01"))
        self.assertEqual(okx_tick("0.96"), Decimal("0.01"))
        self.assertEqual(okx_tick("0.961"), Decimal("0.001"))

    def test_directional_alignment_is_conservative(self):
        self.assertEqual(align_okx_price("0.0395", "buy"), Decimal("0.039"))
        self.assertEqual(align_okx_price("0.0395", "sell"), Decimal("0.04"))
        self.assertEqual(align_okx_price("0.0401", "buy"), Decimal("0.04"))
        self.assertEqual(align_okx_price("0.0401", "sell"), Decimal("0.05"))
        self.assertEqual(align_okx_price("0.9605", "buy"), Decimal("0.96"))
        self.assertEqual(align_okx_price("0.9605", "sell"), Decimal("0.961"))

    def test_tenth_cent_is_not_zero(self):
        self.assertEqual(align_cents_for_okx("0.1", "buy"), Decimal("0.100"))
        self.assertEqual(format_okx_price("0.001"), "0.001")

    def test_poly_display_has_hundredth_cent_precision(self):
        self.assertEqual(format_poly_cents("0.1234"), "12.34")
        self.assertEqual(format_poly_cents("0.0001"), "0.01")

    def test_aligned_price_validation(self):
        self.assertTrue(is_okx_price_aligned("0.039"))
        self.assertFalse(is_okx_price_aligned("0.0395"))
        self.assertTrue(is_okx_price_aligned("0.04"))
        self.assertFalse(is_okx_price_aligned("0.041"))


class ManualStrategyPriceTests(unittest.TestCase):
    def test_manual_s_accepts_tenth_cent_without_truncating_to_zero(self):
        buy, sell = ManualStrategyS._derive_prices("0.1")
        self.assertEqual(buy, Decimal("0.100"))
        self.assertEqual(sell, Decimal("1.100"))

    def test_manual_d_keeps_decimal_99_minus_a_semantics(self):
        buy_yes, buy_no, sell_yes, sell_no = ManualStrategyD._derive_prices("0.1")
        self.assertEqual(buy_yes, Decimal("0.100"))
        self.assertEqual(buy_no, Decimal("98.900"))
        self.assertEqual(sell_yes, Decimal("1.100"))
        self.assertEqual(sell_no, Decimal("99.900"))

    def test_manual_d_no_exit_uses_the_executable_aligned_buy(self):
        buy_yes, buy_no, sell_yes, sell_no = ManualStrategyD._derive_prices("3.9")
        self.assertEqual(buy_yes, Decimal("3.900"))
        self.assertEqual(buy_no, Decimal("95.00"))
        self.assertEqual(sell_yes, Decimal("5.00"))
        self.assertEqual(sell_no, Decimal("96.00"))

    def test_rebalance_uses_the_same_manual_d_boundary_prices(self):
        targets = AutoRebalance._target_prices(
            Decimal("3.9"),
            {
                "strategy_type": "manual-d",
                "yes_asset_id": "yes-asset",
                "no_asset_id": "no-asset",
            },
        )
        self.assertEqual(targets["asset_sells"]["yes-asset"], Decimal("5.00"))
        self.assertEqual(targets["asset_sells"]["no-asset"], Decimal("96.00"))
        self.assertIn("NO B95.00¢/S96.00¢", targets["display"])

    def test_manual_d_does_not_clear_rebalance_on_stop(self):
        strategy = ManualStrategyD(object(), account_id="unit-test-no-file")
        marker = object()
        strategy.auto_rebalance = marker
        strategy._save_pending_sells = lambda: None
        strategy.state.running = True
        strategy.running = True
        asyncio.run(strategy.stop())
        self.assertIs(strategy.auto_rebalance, marker)


@unittest.skipUnless(OPTIONAL_DEPS_AVAILABLE, "runtime dependencies are not installed")
class ManualOrderAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    class Request:
        def __init__(self, body):
            self.body = body

        async def json(self):
            return self.body

    class Client:
        def __init__(self, available_size="100.59", total_size="101.25"):
            self.available_size = available_size
            self.total_size = total_size
            self.placed = []
            self._cloid = 0

        def generate_client_order_id(self):
            self._cloid += 1
            return f"0x01{self._cloid:030x}"

        async def get_positions(self, status="open", market_id=None):
            return {
                "code": "0",
                "data": {
                    "list": [{
                        "marketId": "123",
                        "tokenIndex": "2",
                        "tokenName": "No",
                        "size": self.total_size,
                        "availableSize": self.available_size,
                        "avgPrice": "0.4",
                    }],
                },
            }

        async def place_order(self, **kwargs):
            self.placed.append(kwargs)
            return {
                "code": "0",
                "_clientOrderId": kwargs["client_order_id"],
            }

        async def get_orders(self, status="open", market_id=None):
            orders = []
            if status == "open":
                orders = [
                    {
                        "oid": f"order-{index}",
                        "clientOrderId": order["client_order_id"],
                        "marketId": "123",
                        "assetId": order["asset_id"],
                        "side": order["side"].upper(),
                        "status": "ACTIVE",
                        "price": order["price"],
                        "size": order["size"],
                        "filledSize": "0",
                    }
                    for index, order in enumerate(self.placed, start=1)
                ]
            return {"code": "0", "data": {"list": orders}}

    async def test_positions_expose_total_and_available_size(self):
        client = self.Client()
        instance = SimpleNamespace(client=client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        async def no_cache(_instance):
            return None

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(app_module, "_ensure_market_cache", no_cache),
        ):
            result = await app_module.account_positions("account-a", user="test")

        self.assertEqual(result["positions"][0]["size"], "101.25")
        self.assertEqual(result["positions"][0]["available_size"], "100.59")

    async def test_manual_sell_preserves_decimal_available_size(self):
        client = self.Client()
        instance = SimpleNamespace(client=client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "SELL",
            "price": "1",
            "size": "100.59",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertTrue(result["ok"])
        self.assertEqual(client.placed[0]["size"], "100.59")
        self.assertEqual(client.placed[0]["price"], "0.010")

    async def test_subminimum_manual_sell_all_requires_no_frozen_size(self):
        client = self.Client(available_size="100.59", total_size="100.59")
        instance = SimpleNamespace(client=client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "SELL",
            "price": "0.1",
            "size": "100.59",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            result = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertTrue(result["ok"])
        self.assertEqual(client.placed[0]["size"], "100.59")
        self.assertEqual(client.placed[0]["price"], "0.001")

    async def test_subminimum_manual_sell_rejects_frozen_position(self):
        client = self.Client(available_size="100.59", total_size="101.25")
        instance = SimpleNamespace(client=client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "SELL",
            "price": "0.1",
            "size": "100.59",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("no frozen size", json.loads(response.body)["error"])
        self.assertEqual(client.placed, [])

    async def test_manual_sell_rejects_frozen_or_unavailable_quantity(self):
        client = self.Client(available_size="100.59")
        instance = SimpleNamespace(client=client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "SELL",
            "price": "0.1",
            "size": "100.6",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("100.59", json.loads(response.body)["error"])
        self.assertEqual(client.placed, [])

    async def test_manual_orders_are_blocked_while_sniper_is_running(self):
        client = self.Client()
        sniper = SimpleNamespace(
            running=True,
            starting=False,
            state=SimpleNamespace(locked=False),
        )
        instance = SimpleNamespace(client=client, sniper=sniper)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "BUY",
            "price": "1",
            "size": "100",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertEqual(response.status_code, 409)
        self.assertIn("Sniper is running", json.loads(response.body)["error"])
        self.assertEqual(client.placed, [])

    async def test_manual_orders_are_blocked_while_sniper_is_starting(self):
        client = self.Client()
        sniper = SimpleNamespace(
            running=False,
            starting=True,
            state=SimpleNamespace(locked=False),
        )
        instance = SimpleNamespace(client=client, sniper=sniper)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "BUY",
            "price": "1",
            "size": "100",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(client.placed, [])

    async def test_manual_orders_are_blocked_after_critical_sniper_persistence_failure(self):
        client = self.Client()
        sniper = SimpleNamespace(
            running=False,
            starting=False,
            _persistence_failed=True,
            state=SimpleNamespace(locked=False),
        )
        instance = SimpleNamespace(client=client, sniper=sniper)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = self.Request({
            "asset_id": "123100",
            "side": "BUY",
            "price": "1",
            "size": "100",
            "tif": "gtc",
        })

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.account_manual_place(
                "account-a", request, user="test"
            )

        self.assertEqual(response.status_code, 409)
        self.assertIn("Sniper state is persisted", json.loads(response.body)["error"])
        self.assertEqual(client.placed, [])

    async def test_locked_sniper_blocks_manual_buy_but_allows_reduce_only_sell(self):
        client = self.Client(available_size="100.59", total_size="100.59")
        sniper = SimpleNamespace(
            running=False,
            starting=False,
            state=SimpleNamespace(locked=True),
        )
        instance = SimpleNamespace(client=client, sniper=sniper)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            buy_response = await app_module.account_manual_place(
                "account-a",
                self.Request({
                    "asset_id": "123100",
                    "side": "BUY",
                    "price": "1",
                    "size": "100",
                    "tif": "gtc",
                }),
                user="test",
            )
            sell_response = await app_module.account_manual_place(
                "account-a",
                self.Request({
                    "asset_id": "123100",
                    "side": "SELL",
                    "price": "1",
                    "size": "100.59",
                    "tif": "gtc",
                }),
                user="test",
            )

        self.assertEqual(buy_response.status_code, 409)
        self.assertTrue(sell_response["ok"])
        self.assertEqual(len(client.placed), 1)
        self.assertTrue(client.placed[0]["reduce_only"])

    async def test_manual_ioc_active_is_not_reported_success_before_terminal(self):
        client = self.Client()
        instance = SimpleNamespace(client=client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        active = SimpleNamespace(
            status="ACTIVE",
            order_id="order-1",
            filled_size=Decimal("0"),
        )
        cancelled = SimpleNamespace(
            status="CANCELLED",
            order_id="order-1",
            filled_size=Decimal("0"),
        )
        confirmation = SimpleNamespace(
            response={"code": "0"},
            response_code="0",
            observation=active,
            unresolved=False,
            rejected=False,
        )

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=mock.AsyncMock(return_value=confirmation),
            ),
            mock.patch.object(
                app_module,
                "wait_for_order",
                new=mock.AsyncMock(return_value=cancelled),
            ) as wait_for_order,
        ):
            response = await app_module.account_manual_place(
                "account-a",
                self.Request({
                    "asset_id": "123100",
                    "side": "BUY",
                    "price": "1",
                    "size": "100",
                    "tif": "ioc",
                }),
                user="test",
            )

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 409)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["order_status"], "CANCELLED")
        wait_for_order.assert_awaited_once()


@unittest.skipUnless(OPTIONAL_DEPS_AVAILABLE, "runtime dependencies are not installed")
class AccountStatsTests(unittest.IsolatedAsyncioTestCase):
    async def test_position_count_uses_documented_size_field(self):
        class Client:
            async def get_balance(self):
                return {"code": "0", "data": []}

            async def get_orders(self, status="open"):
                return {"code": "0", "data": {"list": []}}

            async def get_positions(self, status="open"):
                return {
                    "code": "0",
                    "data": {"list": [
                        {"size": "2.5"},
                        {"size": "0"},
                    ]},
                }

        idle = SimpleNamespace(running=False)
        manager = AccountManager()
        manager.instances["account-a"] = SimpleNamespace(
            client=Client(),
            strategy_d=idle,
            strategy_s=idle,
            sniper=SimpleNamespace(
                running=False,
                state=SimpleNamespace(locked=False),
            ),
            sniper_old=idle,
            poly_monitor=SimpleNamespace(
                running=False,
                state=SimpleNamespace(enabled=False),
            ),
            auto_rebalance=SimpleNamespace(_rebalancing=False),
        )

        stats = await manager.get_aggregated_stats()

        self.assertEqual(stats["total_positions"], 1)


@unittest.skipUnless(OPTIONAL_DEPS_AVAILABLE, "runtime dependencies are not installed")
class MonitorPriceTests(unittest.TestCase):
    def test_monitor_rounds_bid_half_up_to_executable_tick(self):
        self.assertEqual(PolyMonitor._risk_bid_price("0.5549"), Decimal("0.55"))
        self.assertEqual(PolyMonitor._risk_bid_price("0.5550"), Decimal("0.56"))
        self.assertEqual(PolyMonitor._risk_bid_price("0.5599"), Decimal("0.56"))
        self.assertEqual(PolyMonitor._risk_bid_price("0.5601"), Decimal("0.56"))
        self.assertEqual(PolyMonitor._risk_bid_price("0.0394"), Decimal("0.039"))
        self.assertEqual(PolyMonitor._risk_bid_price("0.0395"), Decimal("0.040"))

    def test_monitor_start_checks_use_the_same_half_up_mapping(self):
        source = inspect.getsource(app_module)
        self.assertNotIn("align_okx_price(poly_best_bid", source)
        self.assertEqual(source.count("_risk_bid_price(poly_best_bid)"), 3)


@unittest.skipUnless(OPTIONAL_DEPS_AVAILABLE, "runtime dependencies are not installed")
class BookPrecisionTests(unittest.TestCase):
    def test_okx_inversion_preserves_fine_tick(self):
        bids, asks = _invert_okx_book_arrays(
            bids=[["0.999", "2"]],
            asks=[["0.961", "3"]],
        )
        self.assertEqual(bids[0][0], "0.039")
        self.assertEqual(asks[0][0], "0.001")

    def test_poly_incremental_price_change_updates_book(self):
        bid_map, ask_map = _poly_book_maps(
            [{"price": "0.1234", "size": "10"}],
            [{"price": "0.1236", "size": "8"}],
        )
        changed = _apply_poly_price_changes(
            bid_map,
            ask_map,
            [
                {"asset_id": "token", "side": "BUY", "price": "0.1234", "size": "0"},
                {"asset_id": "token", "side": "BUY", "price": "0.1235", "size": "7"},
            ],
            "token",
        )
        bids, asks = _poly_book_from_maps(bid_map, ask_map)
        self.assertTrue(changed)
        self.assertEqual(bids, [{"price": "0.1235", "size": "7"}])
        self.assertEqual(asks, [{"price": "0.1236", "size": "8"}])


if __name__ == "__main__":
    unittest.main()
