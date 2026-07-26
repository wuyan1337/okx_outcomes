import unittest
from decimal import Decimal

from backend.services.manual_strategy_d import ManualStrategyD
from backend.services.manual_strategy_s import ManualStrategyS


def filled_buy(oid, asset_id, price="0.55", size="10"):
    return {
        "oid": oid,
        "assetId": asset_id,
        "side": "BUY",
        "status": "FILLED",
        "price": price,
        "filledSize": size,
        "createdAt": "2",
    }


def filled_sell(oid, asset_id, price="0.56", size="10"):
    return {
        "oid": oid,
        "assetId": asset_id,
        "side": "SELL",
        "status": "FILLED",
        "price": price,
        "filledSize": size,
    }


class SellFailureClient:
    def __init__(self):
        self.place_calls = []

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return {"code": "1", "message": "Minimum order amount not met"}


class SuccessfulSellClient:
    def __init__(self):
        self.place_calls = []

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return {"code": "0", "data": {"oid": f"sell-{len(self.place_calls)}"}}


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

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "20"}]}

    async def get_orders(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}

    async def get_order(self, order_id):
        return {
            "code": "0",
            "data": {"oid": order_id, "status": "CANCELLED", "filledSize": "0"},
        }

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        if len(self.place_calls) == 1:
            return {
                "code": "0",
                "data": {"oid": "yes-order-1"},
                "_clientOrderId": "yes-client-1",
            }
        return {"code": "1", "message": "Insufficient balance"}

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return {"code": "0", "data": {"txHash": "cancel-tx"}}


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


class ManualStrategySafetyTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_unknown_yes_rollback_terminal_state_blocks_new_deployments(self):
        client = UnknownRollbackClient()
        strategy = ManualStrategyD(client, account_id="manual-unknown-rollback-no-file")
        strategy.state.market_id = "market-1"
        strategy.state.yes_asset_id = "yes-asset"

        safe = await strategy._rollback_yes_order(
            {"code": "0", "_clientOrderId": "yes-client-unknown"},
            confirmation_attempts=1,
        )

        self.assertFalse(safe)
        self.assertTrue(strategy.state.deployment_blocked)
        self.assertEqual(client.cancel_calls, [{
            "asset_id": "yes-asset",
            "client_order_id": "yes-client-unknown",
        }])
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

        await strategy._check_fills_post_sells([
            filled_buy("buy-yes", "yes-asset", "0.55"),
            filled_buy("buy-no", "no-asset", "0.44"),
        ])
        await strategy._check_sell_fills([
            filled_sell("sell-yes", "yes-asset", "0.56"),
            filled_sell("sell-no", "no-asset", "0.45"),
        ])

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

        await strategy._check_fills_post_sells([
            filled_buy("buy-yes", "yes-asset", "0.55"),
        ])
        await strategy._check_sell_fills([
            filled_sell("sell-yes", "yes-asset", "0.56"),
        ])

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

                await strategy._check_fills_post_sells([
                    filled_buy("small-buy", "123000", "0.001", "10"),
                ])

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

                    await strategy._check_fills_post_sells([
                        filled_buy("small-buy", "123000", "0.001", "10"),
                    ])

                    self.assertEqual(client.place_calls, [])
                    self.assertNotIn("small-buy", strategy.state.pending_sells)
                    self.assertEqual(strategy.state.sell_retry_counts["small-buy"], 1)


if __name__ == "__main__":
    unittest.main()
