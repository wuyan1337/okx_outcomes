import json
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest import mock

from backend import app as app_module
from backend.services.poly_monitor import PolyMonitor


class _Request:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


class _HttpResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload


class _HttpSession:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *_args, **_kwargs):
        return _HttpResponse(self._payload)


class _IdleStrategy:
    def __init__(self, *, stop_result=True):
        self.running = False
        self.stop_result = stop_result
        self.start_kwargs = None
        self.entry_released = False

    async def start(self, **kwargs):
        self.running = True
        self.start_kwargs = kwargs

    async def stop(self):
        self.running = False
        return self.stop_result

    def release_entry_barrier(self):
        if not self.running:
            return False
        self.entry_released = True
        return True


class _Monitor:
    def __init__(self, *, start_result=None):
        self.running = False
        self.state = SimpleNamespace(enabled=False)
        self.start_result = start_result or {"ok": True}
        self.start_kwargs = None
        self.context = None

    def _risk_bid_price(self, value):
        return PolyMonitor._risk_bid_price(value)

    async def start(self, **kwargs):
        self.start_kwargs = kwargs
        if self.start_result.get("ok"):
            self.running = True
            self.state.enabled = True
        return self.start_result

    async def stop(self):
        self.running = False
        self.state.enabled = False

    def set_strategy_context(self, *args, **kwargs):
        self.context = (args, kwargs)


def _strategy_instance(client, *, d_stop_result=True, monitor_result=None):
    return SimpleNamespace(
        account_id="account-a",
        client=client,
        strategy_d=_IdleStrategy(stop_result=d_stop_result),
        strategy_s=_IdleStrategy(),
        sniper=SimpleNamespace(
            running=False,
            starting=False,
            _persistence_failed=False,
            state=SimpleNamespace(locked=False),
        ),
        sniper_old=SimpleNamespace(running=False),
        poly_monitor=_Monitor(start_result=monitor_result),
        auto_rebalance=SimpleNamespace(_rebalancing=False),
    )


class StrategyBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_sniper_old_lock_counts_as_busy_without_becoming_live_automation(self):
        instance = _strategy_instance(SimpleNamespace())
        instance.sniper_old = SimpleNamespace(
            running=False,
            state=SimpleNamespace(locked=True, status="error"),
        )

        self.assertTrue(app_module._sniper_old_busy(instance))
        self.assertTrue(app_module._account_trading_busy(instance))
        self.assertFalse(app_module._automated_trading_active(instance))

    async def test_sniper_old_lock_blocks_every_new_strategy_entry(self):
        instance = _strategy_instance(SimpleNamespace())
        instance.sniper_old = SimpleNamespace(
            running=False,
            state=SimpleNamespace(locked=True, status="error"),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_entry_safety_response",
                new=mock.AsyncMock(return_value=None),
            ),
        ):
            sniper = await app_module.sniper_start(
                "account-a", _Request({}), user="test"
            )
            sniper_old = await app_module.sniper_old_start(
                "account-a", _Request({}), user="test"
            )
            manual_d = await app_module.account_manual_start_v2(
                "account-a", _Request({}), user="test"
            )
            manual_s = await app_module.account_manual_s_start(
                "account-a", _Request({}), user="test"
            )

        self.assertFalse(sniper["ok"])
        self.assertFalse(sniper_old["ok"])
        self.assertIn("another strategy", sniper["error"])
        self.assertIn("another strategy", sniper_old["error"])
        for response in (manual_d, manual_s):
            self.assertEqual(response.status_code, 400)
            payload = json.loads(response.body)
            self.assertFalse(payload["ok"])
            self.assertIn("running or locked", payload["error"])

    async def test_sniper_old_start_rejects_missing_or_invalid_settings(self):
        instance = _strategy_instance(SimpleNamespace())
        instance.sniper_old = SimpleNamespace(
            running=False,
            state=SimpleNamespace(locked=False),
            start=mock.AsyncMock(return_value={"ok": True}),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        required = {
            "yes_token_id": "poly-yes",
            "no_token_id": "poly-no",
            "yes_asset_id": "market-1000",
            "no_asset_id": "market-1100",
            "market_id": "market-1",
        }

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_entry_safety_response",
                new=mock.AsyncMock(return_value=None),
            ),
        ):
            missing = await app_module.sniper_old_start(
                "account-a", _Request({}), user="test"
            )
            malformed = await app_module.sniper_old_start(
                "account-a",
                _Request({**required, "threshold_cents": "not-a-number"}),
                user="test",
            )
            unsafe = await app_module.sniper_old_start(
                "account-a",
                _Request({**required, "position_pct": 101}),
                user="test",
            )

        for response in (missing, malformed, unsafe):
            self.assertEqual(response.status_code, 400)
            self.assertFalse(json.loads(response.body)["ok"])
        instance.sniper_old.start.assert_not_awaited()

    async def test_sniper_old_lock_blocks_manual_buy_but_allows_reduce_only_sell(self):
        class Client:
            async def get_positions(self, **_kwargs):
                return {
                    "code": "0",
                    "data": {
                        "list": [
                            {
                                "marketId": "market-1",
                                "tokenIndex": "1",
                                "tokenName": "YES",
                                "size": "2",
                                "availableSize": "2",
                            }
                        ]
                    },
                }

        instance = _strategy_instance(Client())
        instance.sniper_old = SimpleNamespace(
            running=False,
            state=SimpleNamespace(locked=True, status="error"),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        observation = SimpleNamespace(
            status="ACTIVE",
            order_id="sell-order",
            filled_size=Decimal("0"),
        )
        confirmation = SimpleNamespace(
            observation=observation,
            unresolved=False,
            rejected=False,
            response_code="0",
            response={"code": "0"},
        )

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_entry_safety_response",
                new=mock.AsyncMock(return_value=None),
            ),
            mock.patch.object(
                app_module,
                "generate_client_order_id",
                return_value="manual-cloid",
            ),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=mock.AsyncMock(return_value=confirmation),
            ) as place,
        ):
            buy = await app_module.account_manual_place(
                "account-a",
                _Request(
                    {
                        "asset_id": "market-1000",
                        "side": "BUY",
                        "price": "50",
                        "size": "2",
                    }
                ),
                user="test",
            )
            sell = await app_module.account_manual_place(
                "account-a",
                _Request(
                    {
                        "asset_id": "market-1000",
                        "side": "SELL",
                        "price": "50",
                        "size": "2",
                    }
                ),
                user="test",
            )

        self.assertEqual(buy.status_code, 409)
        self.assertIn("Sniper-Old", json.loads(buy.body)["error"])
        self.assertTrue(sell["ok"])
        self.assertTrue(place.await_args.kwargs["reduce_only"])

    async def test_sniper_old_stop_returns_409_when_exchange_state_is_unresolved(self):
        class SniperOld:
            running = True
            state = SimpleNamespace(
                locked=False, status="holding", reconciliation_error=""
            )

            async def stop(self):
                self.running = False
                self.state.locked = True
                self.state.status = "error"
                self.state.reconciliation_error = "position query failed"
                return False

        instance = SimpleNamespace(account_id="account-a", sniper_old=SniperOld())
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.sniper_old_stop("account-a", user="test")

        self.assertEqual(response.status_code, 409)
        payload = json.loads(response.body)
        self.assertFalse(payload["ok"])
        self.assertTrue(instance.recovery_locked)
        self.assertEqual(
            instance.recovery_reason, "sniper_old_stop_reconciliation_incomplete"
        )

    async def test_generic_stop_reports_sniper_reconciliation_failure(self):
        class AutoRebalance:
            async def cancel(self):
                return None

        class Sniper:
            running = True
            state = SimpleNamespace(locked=True)

            async def stop(self):
                self.running = False
                return {
                    "ok": True,
                    "locked": True,
                    "needs_reconciliation": True,
                }

        instance = SimpleNamespace(
            account_id="account-a",
            auto_rebalance=AutoRebalance(),
            strategy_d=_IdleStrategy(),
            strategy_s=_IdleStrategy(),
            sniper=Sniper(),
            sniper_old=SimpleNamespace(running=False),
            poly_monitor=SimpleNamespace(
                running=False, state=SimpleNamespace(enabled=False)
            ),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with mock.patch.object(app_module, "manager", manager):
            response = await app_module.stop_strategy("account-a", user="test")

        self.assertEqual(response.status_code, 409)
        payload = json.loads(response.body)
        self.assertFalse(payload["ok"])
        self.assertTrue(instance.recovery_locked)
        self.assertTrue(payload["results"]["sniper"]["needs_reconciliation"])

    async def test_manual_d_monitor_uses_the_executable_buy_tick(self):
        class Client:
            async def get_balance(self):
                return {"code": "0", "data": []}

            async def get_orderbook(self, _asset_id, depth=5):
                return {"code": "0", "data": [{"bids": [["0.55", "10"]]}]}

        instance = _strategy_instance(Client())
        request = _Request({
            "asset_id": "market-1000",
            "no_asset_id": "market-1100",
            "market_id": "market-1",
            "price": "55.5",
            "monitor": {
                "poly_token_id": "poly-yes",
                "poly_side": "yes",
                "depth_threshold": 3000,
            },
        })
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        poly_book = {"bids": [{"price": "0.5549", "size": "10"}]}

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module, "_entry_safety_response", new=mock.AsyncMock(return_value=None)
            ),
            mock.patch.object(app_module, "_record_monitor_mapping"),
            mock.patch("aiohttp.ClientSession", return_value=_HttpSession(poly_book)),
        ):
            result = await app_module.account_manual_start_v2(
                "account-a", request, user="test"
            )

        self.assertTrue(result["ok"])
        self.assertTrue(instance.strategy_d.start_kwargs["defer_entry"])
        self.assertTrue(instance.strategy_d.entry_released)
        self.assertEqual(instance.poly_monitor.start_kwargs["reference_price"], 0.55)
        self.assertEqual(instance.poly_monitor.context[0][1], 55.0)

    async def test_monitor_start_failure_locks_if_manual_rollback_is_unresolved(self):
        class Client:
            async def get_balance(self):
                return {"code": "0", "data": []}

            async def get_orderbook(self, _asset_id, depth=5):
                return {"code": "0", "data": [{"bids": [["0.55", "10"]]}]}

        instance = _strategy_instance(
            Client(), d_stop_result=False, monitor_result={"ok": False, "error": "no"}
        )
        request = _Request({
            "asset_id": "market-1000",
            "no_asset_id": "market-1100",
            "market_id": "market-1",
            "price": "55",
            "monitor": {
                "poly_token_id": "poly-yes",
                "poly_side": "yes",
                "depth_threshold": 3000,
            },
        })
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        poly_book = {"bids": [{"price": "0.55", "size": "10"}]}

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module, "_entry_safety_response", new=mock.AsyncMock(return_value=None)
            ),
            mock.patch("aiohttp.ClientSession", return_value=_HttpSession(poly_book)),
        ):
            response = await app_module.account_manual_start_v2(
                "account-a", request, user="test"
            )

        self.assertEqual(response.status_code, 409)
        payload = json.loads(response.body)
        self.assertFalse(payload["ok"])
        self.assertTrue(instance.recovery_locked)
        self.assertEqual(instance.recovery_reason, "monitor_start_rollback_incomplete")


class AskSellBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def _instance(self, client):
        idle = SimpleNamespace(running=False)
        return SimpleNamespace(
            account_id="account-a",
            client=client,
            strategy_d=idle,
            strategy_s=idle,
            sniper=SimpleNamespace(running=False, starting=False),
            sniper_old=idle,
            poly_monitor=SimpleNamespace(
                running=False, state=SimpleNamespace(enabled=False)
            ),
            auto_rebalance=SimpleNamespace(_rebalancing=False),
        )

    async def test_ask_sell_reloads_available_size_after_old_sell_is_cancelled(self):
        client = SimpleNamespace()
        instance = self._instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        position_before = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "10",
            "availableSize": "4",
        }
        position_after = {**position_before, "availableSize": "10"}
        old_sell = {
            "marketId": "market-1",
            "assetId": "market-1000",
            "side": "SELL",
            "oid": "old-sell",
            "clientOrderId": "old-cloid",
        }
        observation = SimpleNamespace(
            status="ACTIVE",
            order_id="new-sell",
            filled_size=Decimal("0"),
        )
        place_result = SimpleNamespace(
            observation=observation,
            unresolved=False,
            rejected=False,
            response_code="0",
            response={"code": "0"},
        )

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_load_poly_asset_map",
                return_value={
                    "market-1000": {"poly_token_id": "poly-yes", "side": "yes"}
                },
            ),
            mock.patch.object(
                app_module,
                "_query_open_orders_verified",
                new=mock.AsyncMock(side_effect=[[old_sell], []]),
            ),
            mock.patch.object(
                app_module,
                "_query_open_positions_verified",
                new=mock.AsyncMock(side_effect=[[position_before], [position_after]]),
            ),
            mock.patch.object(
                app_module,
                "cancel_and_confirm",
                new=mock.AsyncMock(
                    return_value=SimpleNamespace(
                        confirmed_terminal=True, response={"code": "0"}
                    )
                ),
            ),
            mock.patch.object(
                app_module, "place_and_confirm", new=mock.AsyncMock(return_value=place_result)
            ) as place,
            mock.patch.object(app_module, "generate_client_order_id", return_value="new-cloid"),
            mock.patch(
                "aiohttp.ClientSession",
                return_value=_HttpSession(
                    {"asks": [{"price": "0.56", "size": "20"}]}
                ),
            ),
        ):
            result = await app_module.account_ask_sell("account-a", user="test")

        self.assertTrue(result["ok"])
        self.assertEqual(place.await_args.kwargs["size"], "10")
        self.assertEqual(result["cancelled"], 1)

    async def test_ask_sell_terminal_failure_activates_recovery_lock(self):
        client = SimpleNamespace()
        instance = self._instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "10",
            "availableSize": "10",
        }
        place_result = SimpleNamespace(
            observation=None,
            unresolved=False,
            rejected=True,
            response_code="51000",
            response={"code": "51000", "message": "rejected"},
        )

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_load_poly_asset_map",
                return_value={
                    "market-1000": {"poly_token_id": "poly-yes", "side": "yes"}
                },
            ),
            mock.patch.object(
                app_module,
                "_query_open_orders_verified",
                new=mock.AsyncMock(side_effect=[[], []]),
            ),
            mock.patch.object(
                app_module,
                "_query_open_positions_verified",
                new=mock.AsyncMock(side_effect=[[position], [position]]),
            ),
            mock.patch.object(
                app_module, "place_and_confirm", new=mock.AsyncMock(return_value=place_result)
            ),
            mock.patch.object(app_module, "generate_client_order_id", return_value="new-cloid"),
            mock.patch(
                "aiohttp.ClientSession",
                return_value=_HttpSession(
                    {"asks": [{"price": "0.56", "size": "20"}]}
                ),
            ),
        ):
            result = await app_module.account_ask_sell("account-a", user="test")

        self.assertFalse(result["ok"])
        self.assertTrue(instance.recovery_locked)
        self.assertEqual(instance.recovery_reason, "ask_sell_protection_failed")

    async def test_ask_sell_restores_old_protection_when_replacement_is_rejected(self):
        client = SimpleNamespace()
        instance = self._instance(client)
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        position = {
            "marketId": "market-1",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "10",
            "availableSize": "10",
        }
        old_sell = {
            "marketId": "market-1",
            "assetId": "market-1000",
            "side": "SELL",
            "oid": "old-sell",
            "clientOrderId": "old-cloid",
            "price": "0.50",
            "size": "10",
        }
        rejected = SimpleNamespace(
            observation=None,
            unresolved=False,
            rejected=True,
            response_code="51000",
            response={"code": "51000", "message": "rejected"},
        )
        restore = mock.AsyncMock(
            return_value={
                "ok": True,
                "active": True,
                "status": "ACTIVE",
                "client_order_id": "restored-cloid",
            }
        )

        with (
            mock.patch.object(app_module, "manager", manager),
            mock.patch.object(
                app_module,
                "_load_poly_asset_map",
                return_value={
                    "market-1000": {"poly_token_id": "poly-yes", "side": "yes"}
                },
            ),
            mock.patch.object(
                app_module,
                "_query_open_orders_verified",
                new=mock.AsyncMock(side_effect=[[old_sell], []]),
            ),
            mock.patch.object(
                app_module,
                "_query_open_positions_verified",
                new=mock.AsyncMock(side_effect=[[position], [position], [position]]),
            ),
            mock.patch.object(
                app_module,
                "cancel_and_confirm",
                new=mock.AsyncMock(
                    return_value=SimpleNamespace(
                        confirmed_terminal=True,
                        response={"code": "0"},
                        observation=SimpleNamespace(status="CANCELLED"),
                    )
                ),
            ),
            mock.patch.object(
                app_module,
                "place_and_confirm",
                new=mock.AsyncMock(return_value=rejected),
            ),
            mock.patch.object(
                app_module,
                "_restore_flash_protection",
                new=restore,
            ),
            mock.patch.object(app_module, "generate_client_order_id", return_value="new-cloid"),
            mock.patch(
                "aiohttp.ClientSession",
                return_value=_HttpSession(
                    {"asks": [{"price": "0.56", "size": "20"}]}
                ),
            ),
        ):
            result = await app_module.account_ask_sell("account-a", user="test")

        self.assertFalse(result["ok"])
        restore.assert_awaited_once()
        self.assertTrue(result["results"][0]["rollback"]["ok"])


if __name__ == "__main__":
    unittest.main()
