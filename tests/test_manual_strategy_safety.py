import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock

from backend.services.manual_strategy_d import ManualStrategyD
from backend.services.manual_strategy_s import ManualStrategyS


def filled_buy(oid, asset_id, price="0.55", size="10", client_order_id=None):
    return {
        "oid": oid,
        "clientOrderId": client_order_id or oid,
        "assetId": asset_id,
        "side": "BUY",
        "status": "FILLED",
        "price": price,
        "filledSize": size,
        "createdAt": "2",
    }


def filled_sell(oid, asset_id, price="0.56", size="10", client_order_id=None):
    return {
        "oid": oid,
        "clientOrderId": client_order_id or oid,
        "assetId": asset_id,
        "side": "SELL",
        "status": "FILLED",
        "price": price,
        "filledSize": size,
    }


def own_filled_buy(strategy, order, exit_price=None):
    client_order_id = order["clientOrderId"]
    asset_id = order["assetId"]
    if exit_price is None:
        if isinstance(strategy, ManualStrategyD):
            exit_price = str(Decimal(order["price"]) + Decimal("0.01"))
        else:
            exit_price = strategy._cents_to_price_str(strategy.state.sell_price, "sell")
    outcome = "Yes" if asset_id == strategy.state.yes_asset_id else "No"
    strategy.state.owned_orders[client_order_id] = {
        "client_order_id": client_order_id,
        "order_id": order["oid"],
        "role": "entry_buy",
        "market_id": strategy.state.market_id,
        "asset_id": asset_id,
        "side": "BUY",
        "status": order["status"],
        "price": order["price"],
        "exit_price": exit_price,
        "size": order.get("size", order["filledSize"]),
        "filled_size": order["filledSize"],
        "filled_amount": "0",
        "accounted_fill_size": "0",
        "layer_recorded_size": "0",
        "outcome": outcome,
        "parent_buy_cloid": "",
    }
    return order


def protective_fill_orders(strategy):
    result = []
    for record in strategy.state.owned_orders.values():
        if record.get("role") != "protective_sell":
            continue
        result.append(filled_sell(
            record.get("order_id") or record["client_order_id"],
            record["asset_id"],
            price=record["price"],
            size=record["size"],
            client_order_id=record["client_order_id"],
        ))
    return result


class SellFailureClient:
    def __init__(self):
        self.place_calls = []

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return {"code": "1", "message": "Minimum order amount not met"}


class SuccessfulSellClient:
    def __init__(self):
        self.place_calls = []
        self.cancel_calls = []
        self.orders = {}
        self._sequence = 0

    def generate_client_order_id(self):
        self._sequence += 1
        return f"0x01{self._sequence:030x}"

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        order_id = f"sell-{len(self.place_calls)}"
        client_order_id = kwargs["client_order_id"]
        self.orders[client_order_id] = {
            "oid": order_id,
            "clientOrderId": client_order_id,
            "marketId": "market-1",
            "assetId": kwargs["asset_id"],
            "side": kwargs["side"].upper(),
            "status": "ACTIVE",
            "price": kwargs["price"],
            "size": kwargs["size"],
            "filledSize": "0",
            "filledAmount": "0",
        }
        return {"code": "0", "data": {"txHash": f"tx-{len(self.place_calls)}"}}

    async def get_orders(self, status="open", market_id=None):
        terminal = {"FILLED", "PARTIALLY_FILLED", "FAILED", "CANCELLED", "EXPIRED"}
        orders = [
            dict(order)
            for order in self.orders.values()
            if (order["status"] in terminal) == (status == "closed")
        ]
        return {"code": "0", "data": {"list": orders}}

    async def get_order(self, order_id):
        order = next(order for order in self.orders.values() if order["oid"] == order_id)
        return {"code": "0", "data": dict(order)}

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        order = next(
            order for order in self.orders.values()
            if order["oid"] == kwargs.get("order_id")
            or order["clientOrderId"] == kwargs.get("client_order_id")
        )
        order["status"] = "CANCELLED"
        return {"code": "0", "data": {"txHash": "cancel-tx"}}


class PositionAwareSellClient(SuccessfulSellClient):
    def __init__(self, position_response):
        super().__init__()
        self.position_response = position_response
        self.position_calls = []

    async def get_positions(self, **kwargs):
        self.position_calls.append(kwargs)
        return self.position_response


class CostLayerRecorder:
    def __init__(self):
        self.added = []
        self.removed = []

    def add_to_layer(self, asset_id, price_cents, quantity, market_id=""):
        self.added.append((asset_id, price_cents, quantity, market_id))

    def remove_from_highest_layer(self, asset_id, quantity, market_id=""):
        self.removed.append((asset_id, quantity, market_id))


class PairRollbackClient:
    def __init__(self):
        self.place_calls = []
        self.cancel_calls = []
        self.orders = {}
        self._sequence = 0

    def generate_client_order_id(self):
        self._sequence += 1
        return f"0x01{self._sequence:030x}"

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "20"}]}

    async def get_orders(self, status="open", market_id=None):
        terminal = {"FILLED", "PARTIALLY_FILLED", "FAILED", "CANCELLED", "EXPIRED"}
        return {"code": "0", "data": {"list": [
            dict(order)
            for order in self.orders.values()
            if (order["status"] in terminal) == (status == "closed")
        ]}}

    async def get_order(self, order_id):
        order = next(o for o in self.orders.values() if o["oid"] == order_id)
        return {"code": "0", "data": dict(order)}

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        if len(self.place_calls) == 1:
            client_order_id = kwargs["client_order_id"]
            self.orders[client_order_id] = {
                "oid": "yes-order-1",
                "clientOrderId": client_order_id,
                "marketId": "market-1",
                "assetId": kwargs["asset_id"],
                "side": "BUY",
                "status": "ACTIVE",
                "price": kwargs["price"],
                "size": kwargs["size"],
                "filledSize": "0",
                "filledAmount": "0",
            }
            return {"code": "0", "data": {"txHash": "yes-tx"}}
        return {"code": "1", "message": "Insufficient balance"}

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        order = next(
            order for order in self.orders.values()
            if order["oid"] == kwargs.get("order_id")
            or order["clientOrderId"] == kwargs.get("client_order_id")
        )
        order["status"] = "CANCELLED"
        return {"code": "0", "data": {"txHash": "cancel-tx"}}


class FilledPairRollbackClient(PairRollbackClient):
    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        call_number = len(self.place_calls)
        if kwargs["side"].lower() == "buy" and kwargs["asset_id"] == "no-asset":
            return {"code": "1", "message": "Insufficient balance"}

        client_order_id = kwargs["client_order_id"]
        is_entry = kwargs["side"].lower() == "buy"
        order = {
            "oid": "yes-filled" if is_entry else "yes-protection",
            "clientOrderId": client_order_id,
            "marketId": "market-1",
            "assetId": kwargs["asset_id"],
            "side": kwargs["side"].upper(),
            "status": "FILLED" if is_entry else "ACTIVE",
            "price": kwargs["price"],
            "size": kwargs["size"],
            "filledSize": kwargs["size"] if is_entry else "0",
            "filledAmount": "11" if is_entry else "0",
        }
        self.orders[client_order_id] = order
        return {"code": "0", "data": {"txHash": f"tx-{call_number}"}}


class UnknownRollbackClient:
    def __init__(self):
        self.cancel_calls = []

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return {"code": "0"}

    async def get_orders(self, status="open", market_id=None):
        if status == "open":
            return {
                "code": "0",
                "data": {"list": [{
                    "clientOrderId": "yes-client-unknown",
                    "assetId": "yes-asset",
                    "status": "PENDING_CANCEL",
                    "filledSize": "0",
                }]},
            }
        return {"code": "0", "data": {"list": []}}


class BarrierClient:
    async def get_orders(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}


class ManualStrategySafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_entry_barrier_defers_buy_until_release(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                strategy = strategy_class(
                    BarrierClient(),
                    account_id=f"{strategy_class.__name__}-barrier-no-file",
                )
                strategy._save_pending_sells = lambda: None
                strategy._seed_then_run = strategy._main_loop
                strategy._deploy_balance = AsyncMock()

                if strategy_class is ManualStrategyD:
                    await strategy.start(
                        market_id="market-1",
                        yes_asset_id="yes-asset",
                        no_asset_id="no-asset",
                        price_a="55",
                        defer_entry=True,
                    )
                else:
                    await strategy.start(
                        market_id="market-1",
                        yes_asset_id="yes-asset",
                        no_asset_id="no-asset",
                        side="yes",
                        price_a="55",
                        defer_entry=True,
                    )

                await asyncio.sleep(0)
                strategy._deploy_balance.assert_not_awaited()
                self.assertTrue(strategy.get_state()["entry_deferred"])
                self.assertTrue(strategy.release_entry_barrier())
                for _ in range(5):
                    await asyncio.sleep(0)
                strategy._deploy_balance.assert_awaited_once()
                self.assertFalse(strategy.get_state()["entry_deferred"])
                await strategy.stop()

    async def test_stop_cancels_deferred_entry_wait(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                strategy = strategy_class(
                    BarrierClient(),
                    account_id=f"{strategy_class.__name__}-barrier-stop-no-file",
                )
                strategy._save_pending_sells = lambda: None
                strategy._seed_then_run = strategy._main_loop
                strategy._deploy_balance = AsyncMock()
                if strategy_class is ManualStrategyD:
                    await strategy.start(
                        "market-1", "yes-asset", "no-asset", "55",
                        defer_entry=True,
                    )
                else:
                    await strategy.start(
                        "market-1", "yes-asset", "no-asset", "yes", "55",
                        defer_entry=True,
                    )
                await asyncio.sleep(0)

                safe = await asyncio.wait_for(strategy.stop(), timeout=0.2)

                self.assertTrue(safe)
                self.assertTrue(strategy._main_task.done())
                strategy._deploy_balance.assert_not_awaited()
                self.assertFalse(strategy.get_state()["entry_deferred"])

    async def test_realized_profit_uses_configured_entry_exit_spread(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                strategy = strategy_class(
                    object(),
                    account_id=f"{strategy_class.__name__}-profit-no-file",
                )
                strategy._save_pending_sells = lambda: None
                strategy.state.market_id = "market-1"
                strategy.state.owned_orders = {
                    "buy-1": {
                        "role": "entry_buy",
                        "client_order_id": "buy-1",
                        "market_id": "market-1",
                        "asset_id": "yes-asset",
                        "side": "BUY",
                        "status": "FILLED",
                        "price": "0.30",
                        "size": "10",
                        "filled_size": "10",
                        "layer_recorded_size": "10",
                    },
                    "sell-1": {
                        "role": "protective_sell",
                        "client_order_id": "sell-1",
                        "parent_buy_cloid": "buy-1",
                        "market_id": "market-1",
                        "asset_id": "yes-asset",
                        "side": "SELL",
                        "status": "FILLED",
                        "price": "0.35",
                        "size": "10",
                        "filled_size": "4",
                        "accounted_fill_size": "0",
                        "outcome": "Yes",
                    },
                }

                strategy._account_protective_fill(
                    strategy.state.owned_orders["sell-1"]
                )

                self.assertAlmostEqual(strategy.state.total_profit, 0.20)
                self.assertEqual(
                    strategy.state.owned_orders["sell-1"]["accounted_fill_size"],
                    "4",
                )

    async def test_manual_strategies_use_only_account_level_heartbeat(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = type("HeartbeatProbeClient", (), {})()
                client.heartbeat = AsyncMock()
                strategy = strategy_class(
                    client,
                    account_id=f"{strategy_class.__name__}-heartbeat-no-file",
                )
                strategy._save_pending_sells = lambda: None
                release = asyncio.Event()

                async def idle_main_loop():
                    await release.wait()

                strategy._seed_then_run = idle_main_loop
                if strategy_class is ManualStrategyD:
                    await strategy.start(
                        market_id="123",
                        yes_asset_id="123000",
                        no_asset_id="123100",
                        price_a="55",
                    )
                else:
                    await strategy.start(
                        market_id="123",
                        yes_asset_id="123000",
                        no_asset_id="123100",
                        side="yes",
                        price_a="55",
                    )

                await asyncio.sleep(0)
                client.heartbeat.assert_not_awaited()
                self.assertFalse(hasattr(strategy, "_heartbeat_task"))
                await strategy.stop()
                await asyncio.sleep(0)

    async def test_bid_sell_start_rejects_invalid_mode_and_distorted_spread(self):
        strategy = ManualStrategyS(object(), account_id="manual-s-start-guard-no-file")
        strategy._save_pending_sells = lambda: None

        with self.assertRaisesRegex(ValueError, "mode must be classic or bid-sell"):
            await strategy.start(
                market_id="123",
                yes_asset_id="123000",
                no_asset_id="123100",
                side="yes",
                price_a="10",
                sell_price="11",
                mode="unknown",
            )
        self.assertFalse(strategy.state.running)

        with self.assertRaisesRegex(ValueError, "exactly 1.0"):
            await strategy.start(
                market_id="123",
                yes_asset_id="123000",
                no_asset_id="123100",
                side="yes",
                price_a="3.9",
                sell_price="4.9",
                mode="bid-sell",
            )
        self.assertFalse(strategy.state.running)

    async def test_bid_sell_start_accepts_exact_spread_at_minimum_buy(self):
        strategy = ManualStrategyS(object(), account_id="manual-s-valid-start-no-file")
        strategy._save_pending_sells = lambda: None

        await strategy.start(
            market_id="123",
            yes_asset_id="123000",
            no_asset_id="123100",
            side="yes",
            price_a="0.1",
            sell_price="1.1",
            mode="bid-sell",
        )
        self.assertTrue(strategy.state.running)
        self.assertEqual(strategy.state.price_a, 0.1)
        self.assertEqual(strategy.state.sell_price, 1.1)
        await strategy.stop()

    async def test_minimum_sell_failure_stays_pending_and_backs_off(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = SellFailureClient()
                strategy = strategy_class(client, account_id="manual-safety-no-file")
                strategy._save_pending_sells = lambda: True
                strategy.state.start_time = 1
                strategy.state.market_id = "market-1"
                strategy.state.yes_asset_id = "yes-asset"
                strategy.state.no_asset_id = "no-asset"

                if strategy_class is ManualStrategyD:
                    strategy.state.price_a = 55
                    strategy.state.price_b = 44
                    strategy.state.sell_a = 56
                    strategy.state.sell_b = 45
                    order = filled_buy("buy-1", "yes-asset")
                else:
                    strategy.state.side = "yes"
                    strategy.state.asset_id = "yes-asset"
                    strategy.state.price_a = 55
                    strategy.state.sell_price = 56
                    order = filled_buy("buy-1", "yes-asset")
                own_filled_buy(strategy, order)

                await strategy._check_fills_post_sells([order])
                await strategy._check_fills_post_sells([order])

                self.assertNotIn("buy-1", strategy.state.pending_sells)
                self.assertEqual(len(client.place_calls), 1)
                self.assertEqual(strategy.state.sell_retry_counts["buy-1"], 1)

                strategy.state.sell_retry_after["buy-1"] = 0
                await strategy._check_fills_post_sells([order])
                self.assertEqual(len(client.place_calls), 2)
                self.assertEqual(strategy.state.sell_retry_counts["buy-1"], 2)
                self.assertTrue(any("裸仓风险" in line for line in strategy.state.logs))

    async def test_dual_strategy_rolls_back_yes_when_no_fails(self):
        client = PairRollbackClient()
        strategy = ManualStrategyD(client, account_id="manual-rollback-no-file")
        strategy._save_pending_sells = lambda: True
        strategy.state.market_id = "market-1"
        strategy.state.yes_asset_id = "yes-asset"
        strategy.state.no_asset_id = "no-asset"
        strategy.state.price_a = 55
        strategy.state.price_b = 44
        strategy.state.position_pct = 100

        await strategy._deploy_balance()

        self.assertEqual(len(client.place_calls), 2)
        self.assertEqual(client.cancel_calls, [{
            "asset_id": "yes-asset",
            "order_id": "yes-order-1",
        }])
        self.assertFalse(strategy.state.deployment_blocked)
        self.assertGreater(strategy.state.pair_retry_after, 0)

        await strategy._deploy_balance()
        self.assertEqual(len(client.place_calls), 2)

    async def test_dual_strategy_locks_after_unequal_rollback_fill(self):
        client = FilledPairRollbackClient()
        strategy = ManualStrategyD(
            client, account_id="manual-rollback-filled-no-file"
        )
        strategy._save_pending_sells = lambda: None
        strategy.state.market_id = "market-1"
        strategy.state.yes_asset_id = "yes-asset"
        strategy.state.no_asset_id = "no-asset"
        strategy.state.price_a = 55
        strategy.state.price_b = 44
        strategy.state.position_pct = 100

        await strategy._deploy_balance()

        self.assertTrue(strategy.state.deployment_blocked)
        self.assertIn("unequal fills", strategy.state.block_reason)
        self.assertEqual(client.cancel_calls, [])
        self.assertEqual(len(client.place_calls), 3)
        protective_call = next(
            call for call in client.place_calls if call["side"] == "sell"
        )
        self.assertTrue(protective_call["reduce_only"])

    async def test_unknown_yes_rollback_terminal_state_blocks_new_deployments(self):
        client = UnknownRollbackClient()
        strategy = ManualStrategyD(client, account_id="manual-unknown-rollback-no-file")
        strategy._save_pending_sells = lambda: True
        strategy.state.market_id = "market-1"
        strategy.state.yes_asset_id = "yes-asset"

        safe = await strategy._rollback_yes_order(
            {"code": "0", "_clientOrderId": "yes-client-unknown"},
            confirmation_attempts=1,
        )

        self.assertFalse(safe)
        self.assertTrue(strategy.state.deployment_blocked)
        self.assertEqual(client.cancel_calls, [])
        self.assertTrue(any("无法确认最终状态" in line and "新部署已锁定" in line for line in strategy.state.logs))

    async def test_dual_strategy_cost_layers_are_asset_scoped(self):
        client = SuccessfulSellClient()
        layers = CostLayerRecorder()
        strategy = ManualStrategyD(client, account_id="manual-d-layers-no-file")
        strategy._save_pending_sells = lambda: None
        strategy.auto_rebalance = layers
        strategy.state.start_time = 1
        strategy.state.market_id = "market-1"
        strategy.state.yes_asset_id = "yes-asset"
        strategy.state.no_asset_id = "no-asset"

        buy_yes = own_filled_buy(
            strategy, filled_buy("buy-yes", "yes-asset", "0.55")
        )
        buy_no = own_filled_buy(
            strategy, filled_buy("buy-no", "no-asset", "0.44")
        )
        await strategy._check_fills_post_sells([buy_yes, buy_no])
        await strategy._check_sell_fills(protective_fill_orders(strategy))

        self.assertEqual(layers.added, [
            ("yes-asset", Decimal("55"), 10.0, "market-1"),
            ("no-asset", Decimal("44"), 10.0, "market-1"),
        ])
        self.assertEqual(layers.removed, [
            ("yes-asset", 10.0, "market-1"),
            ("no-asset", 10.0, "market-1"),
        ])

    async def test_single_strategy_cost_layers_include_market_and_asset(self):
        client = SuccessfulSellClient()
        layers = CostLayerRecorder()
        strategy = ManualStrategyS(client, account_id="manual-s-layers-no-file")
        strategy._save_pending_sells = lambda: None
        strategy.auto_rebalance = layers
        strategy.state.start_time = 1
        strategy.state.market_id = "market-1"
        strategy.state.side = "yes"
        strategy.state.asset_id = "yes-asset"
        strategy.state.price_a = 55
        strategy.state.sell_price = 56

        buy_yes = own_filled_buy(
            strategy, filled_buy("buy-yes", "yes-asset", "0.55")
        )
        await strategy._check_fills_post_sells([buy_yes])
        await strategy._check_sell_fills(protective_fill_orders(strategy))

        self.assertEqual(layers.added, [
            ("yes-asset", Decimal("55"), 10.0, "market-1"),
        ])
        self.assertEqual(layers.removed, [
            ("yes-asset", 10.0, "market-1"),
        ])

    async def test_subminimum_sell_uses_sell_all_exception_for_both_strategies(self):
        position_response = {
            "code": "0",
            "data": {"list": [{
                "marketId": "123",
                "tokenIndex": "1",
                "tokenName": "Yes",
                "size": "10",
                "availableSize": "10",
            }]},
        }
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = PositionAwareSellClient(position_response)
                strategy = strategy_class(client, account_id="manual-sell-all-no-file")
                strategy._save_pending_sells = lambda: None
                strategy.state.start_time = 1
                strategy.state.market_id = "123"
                strategy.state.yes_asset_id = "123000"
                strategy.state.no_asset_id = "123100"
                if strategy_class is ManualStrategyD:
                    strategy.state.price_a = 0.1
                    strategy.state.price_b = 98.9
                    strategy.state.sell_a = 1.1
                    strategy.state.sell_b = 99.9
                else:
                    strategy.state.side = "yes"
                    strategy.state.asset_id = "123000"
                    strategy.state.price_a = 0.1
                    strategy.state.sell_price = 1.1

                small_buy = own_filled_buy(
                    strategy,
                    filled_buy("small-buy", "123000", "0.001", "10"),
                    exit_price="0.011",
                )
                await strategy._check_fills_post_sells([small_buy])

                self.assertEqual(len(client.place_calls), 1)
                self.assertEqual(client.place_calls[0]["side"], "sell")
                self.assertEqual(strategy.state.pending_sells["small-buy"], 10.0)
                self.assertTrue(any("sell-all exception" in line for line in strategy.state.logs))

    async def test_subminimum_sell_is_deferred_when_position_is_frozen_or_unknown(self):
        responses = (
            {
                "code": "0",
                "data": {"list": [{
                    "marketId": "123",
                    "tokenIndex": "1",
                    "size": "20",
                    "availableSize": "10",
                }]},
            },
            {"code": "1", "message": "position service unavailable"},
        )
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            for position_response in responses:
                with self.subTest(
                    strategy=strategy_class.__name__,
                    response_code=position_response["code"],
                ):
                    client = PositionAwareSellClient(position_response)
                    strategy = strategy_class(client, account_id="manual-not-sell-all-no-file")
                    strategy._save_pending_sells = lambda: True
                    strategy.state.start_time = 1
                    strategy.state.market_id = "123"
                    strategy.state.yes_asset_id = "123000"
                    strategy.state.no_asset_id = "123100"
                    if strategy_class is ManualStrategyD:
                        strategy.state.price_a = 0.1
                        strategy.state.price_b = 98.9
                        strategy.state.sell_a = 1.1
                        strategy.state.sell_b = 99.9
                    else:
                        strategy.state.side = "yes"
                        strategy.state.asset_id = "123000"
                        strategy.state.price_a = 0.1
                        strategy.state.sell_price = 1.1

                    small_buy = own_filled_buy(
                        strategy,
                        filled_buy("small-buy", "123000", "0.001", "10"),
                        exit_price="0.011",
                    )
                    await strategy._check_fills_post_sells([small_buy])

                    self.assertEqual(client.place_calls, [])
                    self.assertNotIn("small-buy", strategy.state.pending_sells)
                    self.assertEqual(strategy.state.sell_retry_counts["small-buy"], 1)

    async def test_same_asset_manual_buy_is_never_adopted(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = SuccessfulSellClient()
                strategy = strategy_class(client, account_id="manual-ownership-no-file")
                strategy._save_pending_sells = lambda: None
                strategy.state.start_time = 1
                strategy.state.market_id = "market-1"
                strategy.state.yes_asset_id = "yes-asset"
                strategy.state.no_asset_id = "no-asset"
                if strategy_class is ManualStrategyD:
                    strategy.state.price_a = 55
                    strategy.state.price_b = 44
                    strategy.state.sell_a = 56
                    strategy.state.sell_b = 45
                else:
                    strategy.state.side = "yes"
                    strategy.state.asset_id = "yes-asset"
                    strategy.state.price_a = 55
                    strategy.state.sell_price = 56

                manual_order = filled_buy(
                    "manual-buy",
                    "yes-asset",
                    client_order_id="another-tool-order",
                )
                await strategy._check_fills_post_sells([manual_order])

                self.assertEqual(client.place_calls, [])
                self.assertNotIn("another-tool-order", strategy.state.owned_orders)

    async def test_old_owned_buy_is_protected_after_restart_boundary(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = SuccessfulSellClient()
                strategy = strategy_class(client, account_id="manual-old-owned-no-file")
                strategy._save_pending_sells = lambda: None
                strategy.state.start_time = 999999
                strategy.state.market_id = "market-1"
                strategy.state.yes_asset_id = "yes-asset"
                strategy.state.no_asset_id = "no-asset"
                if strategy_class is ManualStrategyD:
                    strategy.state.price_a = 55
                    strategy.state.price_b = 44
                    strategy.state.sell_a = 56
                    strategy.state.sell_b = 45
                else:
                    strategy.state.side = "yes"
                    strategy.state.asset_id = "yes-asset"
                    strategy.state.price_a = 55
                    strategy.state.sell_price = 56
                old_buy = own_filled_buy(
                    strategy,
                    filled_buy("old-owned", "yes-asset"),
                )
                old_buy["createdAt"] = "1"

                await strategy._check_fills_post_sells([old_buy])

                self.assertEqual(len(client.place_calls), 1)
                self.assertEqual(strategy.state.pending_sells["old-owned"], Decimal("10"))

    async def test_terminal_protective_sell_releases_only_unfilled_remainder(self):
        cases = (("CANCELLED", "0", "10"), ("PARTIALLY_FILLED", "4", "6"))
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            for terminal_status, sell_filled, replacement_size in cases:
                with self.subTest(
                    strategy=strategy_class.__name__, status=terminal_status
                ):
                    client = SuccessfulSellClient()
                    strategy = strategy_class(
                        client, account_id="manual-release-protection-no-file"
                    )
                    strategy._save_pending_sells = lambda: None
                    strategy.state.start_time = 1
                    strategy.state.market_id = "market-1"
                    strategy.state.yes_asset_id = "yes-asset"
                    strategy.state.no_asset_id = "no-asset"
                    if strategy_class is ManualStrategyD:
                        strategy.state.price_a = 55
                        strategy.state.price_b = 44
                        strategy.state.sell_a = 56
                        strategy.state.sell_b = 45
                    else:
                        strategy.state.side = "yes"
                        strategy.state.asset_id = "yes-asset"
                        strategy.state.price_a = 55
                        strategy.state.sell_price = 56
                    buy = own_filled_buy(
                        strategy, filled_buy("buy-owned", "yes-asset")
                    )
                    await strategy._check_fills_post_sells([buy])
                    first_sell = next(
                        record
                        for record in strategy.state.owned_orders.values()
                        if record.get("role") == "protective_sell"
                    )
                    exchange_order = client.orders[first_sell["client_order_id"]]
                    exchange_order["status"] = terminal_status
                    exchange_order["filledSize"] = sell_filled

                    await strategy._check_sell_fills([dict(exchange_order)])
                    await strategy._check_fills_post_sells([buy])

                    self.assertEqual(len(client.place_calls), 2)
                    self.assertEqual(client.place_calls[-1]["size"], replacement_size)
                    self.assertEqual(
                        strategy.state.pending_sells["buy-owned"], Decimal("10")
                    )

    async def test_stop_cancels_only_owned_entry_and_retains_active_protection(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = SuccessfulSellClient()
                strategy = strategy_class(client, account_id="manual-stop-owned-no-file")
                strategy._save_pending_sells = lambda: None
                strategy.state.running = True
                strategy.running = True
                strategy.state.market_id = "market-1"
                strategy.state.yes_asset_id = "yes-asset"
                strategy.state.no_asset_id = "no-asset"
                if strategy_class is ManualStrategyS:
                    strategy.state.side = "yes"
                    strategy.state.asset_id = "yes-asset"
                    strategy.state.sell_price = 56
                entry_cloid = "owned-entry"
                sell_cloid = "owned-protection"
                entry_order = {
                    "oid": "entry-oid",
                    "clientOrderId": entry_cloid,
                    "marketId": "market-1",
                    "assetId": "yes-asset",
                    "side": "BUY",
                    "status": "ACTIVE",
                    "price": "0.55",
                    "size": "20",
                    "filledSize": "10",
                    "filledAmount": "5.5",
                }
                sell_order = {
                    "oid": "sell-oid",
                    "clientOrderId": sell_cloid,
                    "marketId": "market-1",
                    "assetId": "yes-asset",
                    "side": "SELL",
                    "status": "ACTIVE",
                    "price": "0.56",
                    "size": "10",
                    "filledSize": "0",
                    "filledAmount": "0",
                }
                client.orders = {
                    entry_cloid: entry_order,
                    sell_cloid: sell_order,
                }
                strategy.state.owned_orders = {
                    entry_cloid: {
                        "client_order_id": entry_cloid,
                        "order_id": "entry-oid",
                        "role": "entry_buy",
                        "market_id": "market-1",
                        "asset_id": "yes-asset",
                        "side": "BUY",
                        "status": "ACTIVE",
                        "price": "0.55",
                        "exit_price": "0.56",
                        "size": "20",
                        "filled_size": "10",
                        "filled_amount": "5.5",
                        "accounted_fill_size": "0",
                        "layer_recorded_size": "10",
                        "outcome": "Yes",
                        "parent_buy_cloid": "",
                    },
                    sell_cloid: {
                        "client_order_id": sell_cloid,
                        "order_id": "sell-oid",
                        "role": "protective_sell",
                        "market_id": "market-1",
                        "asset_id": "yes-asset",
                        "side": "SELL",
                        "status": "ACTIVE",
                        "price": "0.56",
                        "size": "10",
                        "filled_size": "0",
                        "filled_amount": "0",
                        "accounted_fill_size": "0",
                        "outcome": "Yes",
                        "parent_buy_cloid": entry_cloid,
                    },
                }

                safe = await strategy.stop()

                self.assertTrue(safe)
                self.assertEqual(client.cancel_calls, [{
                    "asset_id": "yes-asset",
                    "order_id": "entry-oid",
                }])
                self.assertEqual(
                    strategy.state.owned_orders[entry_cloid]["status"], "CANCELLED"
                )
                self.assertEqual(
                    strategy.state.owned_orders[sell_cloid]["status"], "ACTIVE"
                )

    async def test_critical_snapshot_failure_blocks_order_submission(self):
        for strategy_class in (ManualStrategyD, ManualStrategyS):
            with self.subTest(strategy=strategy_class.__name__):
                client = SuccessfulSellClient()
                strategy = strategy_class(client, account_id="manual-write-fail-no-file")
                strategy.state.market_id = "market-1"
                strategy._save_pending_sells = lambda: False

                record, confirmation = await strategy._place_owned_order(
                    role="entry_buy",
                    asset_id="yes-asset",
                    side="BUY",
                    price="0.55",
                    size="10",
                    outcome="Yes",
                    exit_price="0.56",
                )

                self.assertIsNotNone(record)
                self.assertIsNone(confirmation)
                self.assertTrue(strategy.state.deployment_blocked)
                self.assertEqual(client.place_calls, [])


if __name__ == "__main__":
    unittest.main()
