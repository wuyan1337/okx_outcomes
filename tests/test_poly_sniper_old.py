import asyncio
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal
import time
from unittest.mock import AsyncMock

from backend.core import database as database_module
from backend.services.poly_sniper_old import PolySniperOld, SniperOldState


OPEN_STATUSES = {"PENDING_PLACE", "ACTIVE", "PENDING_CANCEL"}


class LifecycleClient:
    def __init__(self, plans=None, position_size="0"):
        self.plans = list(plans or [])
        self.position_size = Decimal(str(position_size))
        self.orders = {}
        self.place_calls = []
        self.cancel_calls = []
        self._next_id = 0

    def generate_client_order_id(self):
        self._next_id += 1
        return f"coid-{self._next_id}"

    def add_order(
        self,
        *,
        client_order_id,
        side,
        status,
        size,
        filled_size="0",
        filled_amount="0",
        asset_id="m000",
        order_id=None,
    ):
        order_id = order_id or f"oid-{client_order_id}"
        self.orders[client_order_id] = {
            "id": order_id,
            "oid": order_id,
            "clientOrderId": client_order_id,
            "marketId": "m",
            "assetId": asset_id,
            "side": side.upper(),
            "status": status,
            "size": str(size),
            "filledSize": str(filled_size),
            "filledAmount": str(filled_amount),
            "price": "0.40",
        }
        return self.orders[client_order_id]

    async def place_order(
        self,
        *,
        asset_id,
        side,
        price,
        size,
        tif,
        reduce_only=False,
        client_order_id=None,
    ):
        self.place_calls.append(
            {
                "asset_id": asset_id,
                "side": side,
                "price": price,
                "size": size,
                "tif": tif,
                "reduce_only": reduce_only,
                "client_order_id": client_order_id,
            }
        )
        plan = self.plans.pop(0)
        if plan.get("visible", True):
            filled = Decimal(str(plan.get("filled_size", "0")))
            amount = Decimal(str(plan.get("filled_amount", "0")))
            self.add_order(
                client_order_id=client_order_id,
                side=side,
                status=plan.get("status", "CANCELLED"),
                size=size,
                filled_size=filled,
                filled_amount=amount,
                asset_id=asset_id,
            )
            if side.lower() == "buy":
                self.position_size += filled
            else:
                self.position_size = max(Decimal("0"), self.position_size - filled)
        return {"code": plan.get("code", "0"), "message": plan.get("message", "OK")}

    async def cancel_order(
        self, *, asset_id, order_id=None, client_order_id=None
    ):
        self.cancel_calls.append(
            {
                "asset_id": asset_id,
                "order_id": order_id,
                "client_order_id": client_order_id,
            }
        )
        target = None
        if client_order_id:
            target = self.orders.get(client_order_id)
        elif order_id:
            target = next(
                (
                    order
                    for order in self.orders.values()
                    if order.get("oid") == order_id or order.get("id") == order_id
                ),
                None,
            )
        if target is not None and target["status"] in OPEN_STATUSES:
            target["status"] = "CANCELLED"
        return {"code": "0", "message": "OK"}

    async def get_orders(self, status="open", market_id=None):
        if status == "open":
            orders = [o for o in self.orders.values() if o["status"] in OPEN_STATUSES]
        else:
            orders = [o for o in self.orders.values() if o["status"] not in OPEN_STATUSES]
        return {"code": "0", "data": {"list": orders}}

    async def get_order(self, order_id):
        order = next(
            (
                item
                for item in self.orders.values()
                if item.get("oid") == order_id or item.get("id") == order_id
            ),
            None,
        )
        return {"code": "0", "data": order or {}}

    async def get_positions(self, status="open", market_id=None):
        positions = []
        if self.position_size > 0:
            positions.append(
                {
                    "marketId": "m",
                    "tokenIndex": "1",
                    "tokenName": "YES",
                    "size": format(self.position_size, "f"),
                    "availableSize": format(self.position_size, "f"),
                }
            )
        return {"code": "0", "data": {"list": positions}}

    async def get_orderbook(self, asset_id, depth=5):
        return {
            "code": "0",
            "data": [{"bids": [["0.40", "100"]], "asks": [["0.60", "100"]]}],
        }

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "100"}]}


class DelayedVisibilityClient(LifecycleClient):
    def __init__(self, plans=None, position_size="0", hidden_order_reads=0):
        super().__init__(plans=plans, position_size=position_size)
        self.hidden_order_reads = hidden_order_reads
        self.order_reads = 0

    async def get_orders(self, status="open", market_id=None):
        self.order_reads += 1
        if self.order_reads <= self.hidden_order_reads:
            return {"code": "0", "data": {"list": []}}
        return await super().get_orders(status=status, market_id=market_id)


def configured_sniper(client):
    sniper = PolySniperOld(client, account_id="acct")
    # Cancellation needs one observation/cancel pass and one follow-up
    # observation to prove the exact order reached a terminal state.
    sniper._order_confirmation_attempts = 2
    sniper._order_confirmation_delay = 0
    sniper.state = SniperOldState(
        status="monitoring",
        yes_token_id="poly-yes",
        no_token_id="poly-no",
        yes_asset_id="m000",
        no_asset_id="m100",
        market_id="m",
    )
    return sniper


class PolySniperOldLifecycleTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def recovery_snapshot(**runtime_overrides):
        runtime = {
            "status": "holding",
            "yes_token_id": "poly-yes",
            "no_token_id": "poly-no",
            "yes_asset_id": "m000",
            "no_asset_id": "m100",
            "market_id": "m",
            "holding_side": "yes",
            "buy_price": 0.4,
            "buy_size": 5,
            "sold_size": 0,
            "buy_client_order_id": "entry-1",
            "buy_order_id": "entry-oid",
            "sell_client_order_id": "protect-1",
            "sell_order_id": "protect-oid",
            "sell_filled_size": 0,
            "exit_client_order_ids": [],
            "resolved_exit_client_order_ids": [],
            "hold_start_epoch_ms": int(time.time() * 1000),
        }
        runtime.update(runtime_overrides)
        return {
            "config": {
                "threshold_cents": 40,
                "ask_levels": 99,
                "position_pct": 100,
                "timeout_secs": 120,
                "settle_ms": 0,
                "window_ms": 150,
            },
            "runtime": runtime,
        }

    async def test_persistence_failure_before_entry_prevents_exchange_write(self):
        client = LifecycleClient([])
        sniper = configured_sniper(client)
        sniper.on_state_change = lambda _snapshot: False

        bought = await sniper._execute_buy_immediate(
            "yes", "m000", [["0.40", "5"]], 0.10, 0.50
        )

        self.assertFalse(bought)
        self.assertEqual(client.place_calls, [])
        self.assertTrue(sniper.state.locked)
        self.assertIn("before-entry-submit", sniper.state.reconciliation_error)

    async def test_restore_phase_one_reconciles_without_submitting_orders(self):
        client = LifecycleClient(position_size="5")
        client.add_order(
            client_order_id="entry-1",
            order_id="entry-oid",
            side="buy",
            status="FILLED",
            size="5",
            filled_size="5",
            filled_amount="2",
        )
        client.add_order(
            client_order_id="protect-1",
            order_id="protect-oid",
            side="sell",
            status="ACTIVE",
            size="5",
        )
        sniper = PolySniperOld(client, account_id="acct")
        sniper._order_confirmation_attempts = 2
        sniper._order_confirmation_delay = 0
        persisted = []
        sniper.on_state_change = lambda snapshot: persisted.append(snapshot) or True
        snapshot = self.recovery_snapshot()
        snapshot["running"] = True

        result = await sniper.restore(snapshot, resume=False)

        self.assertTrue(result["ok"])
        self.assertFalse(result["resumed"])
        self.assertFalse(sniper.running)
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.status, "holding")
        self.assertEqual(client.place_calls, [])
        self.assertEqual(client.cancel_calls, [])
        self.assertEqual(len(persisted), 1)
        self.assertTrue(persisted[0]["running"])
        self.assertEqual(persisted[0]["runtime"]["status"], "holding")

    async def test_restore_phase_one_persistence_failure_blocks_resume(self):
        client = LifecycleClient(position_size="5")
        client.add_order(
            client_order_id="entry-1",
            order_id="entry-oid",
            side="buy",
            status="FILLED",
            size="5",
            filled_size="5",
            filled_amount="2",
        )
        client.add_order(
            client_order_id="protect-1",
            order_id="protect-oid",
            side="sell",
            status="ACTIVE",
            size="5",
        )
        sniper = PolySniperOld(client, account_id="acct")
        sniper.on_state_change = lambda _snapshot: False

        result = await sniper.restore(
            {**self.recovery_snapshot(), "running": True},
            resume=False,
        )

        self.assertFalse(result["ok"])
        self.assertTrue(result["needs_reconciliation"])
        self.assertFalse(sniper.running)
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.status, "error")
        self.assertIn("restore-phase-one", sniper.state.reconciliation_error)
        self.assertEqual(client.place_calls, [])

    async def test_running_false_stale_ledger_cleanup_is_synced_to_sqlite(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            original_path = database_module.DB_PATH
            database_module.DB_PATH = Path(temp_dir) / "accounts.db"
            try:
                database_module.init_db()
                connection = database_module.get_db()
                connection.execute(
                    """
                    INSERT INTO accounts
                        (id, name, api_key, secret_key, passphrase,
                         agent_private_key, proxy_node)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("acct", "old", "a", "b", "c", "d", "JP2"),
                )
                connection.commit()
                connection.close()

                snapshot = self.recovery_snapshot(
                    sell_client_order_id="",
                    sell_order_id="",
                )
                database_module.save_sniper_old_state(
                    "acct", snapshot["config"], snapshot["runtime"], False
                )
                client = LifecycleClient(position_size="0")
                client.add_order(
                    client_order_id="entry-1",
                    order_id="entry-oid",
                    side="buy",
                    status="FILLED",
                    size="5",
                    filled_size="5",
                    filled_amount="2",
                )
                sniper = PolySniperOld(client, account_id="acct")
                sniper.on_state_change = lambda data: (
                    database_module.save_sniper_old_state(
                        "acct",
                        data["config"],
                        data["runtime"],
                        data["running"],
                    )
                    or True
                )

                saved = database_module.get_sniper_old_state("acct")
                result = await sniper.restore(saved, resume=False)
                reloaded = database_module.get_sniper_old_state("acct")

                self.assertTrue(result["ok"])
                self.assertFalse(sniper.running)
                self.assertEqual(sniper.state.status, "stopped")
                self.assertFalse(sniper.state.locked)
                self.assertEqual(sniper.state.buy_size, 0)
                self.assertEqual(sniper.state.buy_client_order_id, "")
                self.assertFalse(reloaded["running"])
                self.assertEqual(reloaded["runtime"]["status"], "stopped")
                self.assertFalse(reloaded["runtime"]["locked"])
                self.assertEqual(reloaded["runtime"]["buy_size"], 0.0)
                self.assertEqual(
                    reloaded["runtime"]["buy_client_order_id"], ""
                )
            finally:
                database_module.DB_PATH = original_path

    async def test_restore_unknown_entry_fails_closed_without_duplicate_order(self):
        client = LifecycleClient(position_size="0")
        sniper = PolySniperOld(client, account_id="acct")
        sniper._order_confirmation_attempts = 1
        sniper._order_confirmation_delay = 0
        snapshot = self.recovery_snapshot(
            sell_client_order_id="",
            sell_order_id="",
        )

        result = await sniper.restore(snapshot, resume=False)

        self.assertFalse(result["ok"])
        self.assertTrue(result["needs_reconciliation"])
        self.assertTrue(sniper.state.locked)
        self.assertFalse(sniper.running)
        self.assertEqual(client.place_calls, [])

    async def test_restore_resume_keeps_owned_protective_sell_and_holding_loop(self):
        client = LifecycleClient(position_size="5")
        client.add_order(
            client_order_id="entry-1",
            order_id="entry-oid",
            side="buy",
            status="FILLED",
            size="5",
            filled_size="5",
            filled_amount="2",
        )
        client.add_order(
            client_order_id="protect-1",
            order_id="protect-oid",
            side="sell",
            status="ACTIVE",
            size="5",
        )
        sniper = PolySniperOld(client, account_id="acct")
        sniper._order_confirmation_attempts = 2
        sniper._order_confirmation_delay = 0

        result = await sniper.restore(self.recovery_snapshot(), resume=True)
        try:
            self.assertTrue(result["ok"])
            self.assertTrue(result["resumed"])
            self.assertTrue(sniper.running)
            self.assertFalse(sniper.state.locked)
            self.assertIsNotNone(sniper._hold_task)
            self.assertEqual(client.place_calls, [])
            self.assertEqual(client.cancel_calls, [])
        finally:
            sniper._running = False
            if sniper._hold_task:
                sniper._hold_task.cancel()
                await asyncio.gather(sniper._hold_task, return_exceptions=True)

    async def test_jump_threshold_uses_exact_raw_cents(self):
        below = configured_sniper(LifecycleClient())
        below.config.threshold_cents = 40
        below._ws_calibrated["yes"] = True
        below.state.last_yes_bid = 0.10
        below._baseline["yes"] = 0.10
        below._price_window["yes"] = [(time.monotonic(), 0.10)]
        below._snapshot_okx_asks = AsyncMock(return_value=[])

        await below._process_book_update({
            "asset_id": "poly-yes",
            "bids": [{"price": "0.4999"}],
        })

        below._snapshot_okx_asks.assert_not_awaited()

        exact = configured_sniper(LifecycleClient())
        exact.config.threshold_cents = 40
        exact._ws_calibrated["yes"] = True
        exact.state.last_yes_bid = 0.10
        exact._baseline["yes"] = 0.10
        exact._price_window["yes"] = [(time.monotonic(), 0.10)]
        exact._snapshot_okx_asks = AsyncMock(return_value=[])

        await exact._process_book_update({
            "asset_id": "poly-yes",
            "bids": [{"price": "0.5000"}],
        })

        exact._snapshot_okx_asks.assert_awaited_once_with("m000")

    async def test_entry_preserves_0039_fine_tick(self):
        client = LifecycleClient([
            {
                "status": "FILLED",
                "filled_size": "5",
                "filled_amount": "0.195",
            }
        ])
        sniper = configured_sniper(client)

        bought = await sniper._execute_buy_immediate(
            "yes", "m000", [["0.039", "5"]], 0.10, 0.50
        )

        self.assertTrue(bought)
        self.assertEqual(client.place_calls[0]["price"], "0.039")

    async def test_timeout_entry_is_reconciled_by_exact_cloid_without_cancel_all(self):
        client = LifecycleClient(
            [
                {
                    "code": "TIMEOUT",
                    "status": "FILLED",
                    "filled_size": "5",
                    "filled_amount": "2",
                }
            ]
        )
        sniper = configured_sniper(client)

        bought = await sniper._execute_buy_immediate(
            "yes", "m000", [["0.40", "5"]], 0.10, 0.50
        )

        self.assertTrue(bought)
        self.assertEqual(sniper.state.buy_client_order_id, "coid-1")
        self.assertEqual(sniper.state.buy_size, 5.0)
        self.assertFalse(sniper.state.locked)
        self.assertEqual(client.cancel_calls, [])
        self.assertFalse(hasattr(client, "cancel_all"))

    async def test_unresolved_entry_locks_and_preserves_cloid(self):
        client = LifecycleClient([{"code": "INVALID_RESPONSE", "visible": False}])
        sniper = configured_sniper(client)
        sniper._running = True

        bought = await sniper._execute_buy_immediate(
            "yes", "m000", [["0.40", "5"]], 0.10, 0.50
        )

        self.assertFalse(bought)
        self.assertFalse(sniper.running)
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.buy_client_order_id, "coid-1")
        self.assertEqual(client.cancel_calls[0]["client_order_id"], "coid-1")
        self.assertFalse(hasattr(client, "cancel_all"))

    async def test_partial_exit_keeps_selling_only_the_remaining_quantity(self):
        client = LifecycleClient(
            [
                {"status": "PARTIALLY_FILLED", "filled_size": "4", "filled_amount": "1.6"},
                {"status": "FILLED", "filled_size": "6", "filled_amount": "2.4"},
            ],
            position_size="10",
        )
        sniper = configured_sniper(client)
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.30
        sniper.state.buy_size = 10.0

        result = await sniper._handle_timeout("m000", "YES")

        self.assertTrue(result)
        self.assertEqual(
            [Decimal(call["size"]) for call in client.place_calls],
            [Decimal("10"), Decimal("6")],
        )
        self.assertEqual(client.position_size, Decimal("0"))
        self.assertEqual(sniper.state.status, "monitoring")
        self.assertFalse(sniper.state.locked)
        self.assertFalse(hasattr(client, "cancel_all"))

    async def test_stop_cancels_only_owned_sell_then_verifies_flat(self):
        client = LifecycleClient(
            [{"status": "FILLED", "filled_size": "5", "filled_amount": "2"}],
            position_size="5",
        )
        client.add_order(
            client_order_id="protect-1",
            order_id="sell-oid",
            side="sell",
            status="ACTIVE",
            size="5",
        )
        sniper = configured_sniper(client)
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.30
        sniper.state.buy_size = 5.0
        sniper.state.sell_client_order_id = "protect-1"
        sniper.state.sell_order_id = "sell-oid"

        result = await sniper.stop()

        self.assertTrue(result)
        self.assertEqual(len(client.cancel_calls), 1)
        self.assertEqual(client.cancel_calls[0]["order_id"], "sell-oid")
        self.assertEqual(sniper.state.status, "stopped")
        self.assertEqual(client.position_size, Decimal("0"))
        self.assertFalse(hasattr(client, "cancel_all"))

    async def test_stop_returns_false_and_keeps_state_when_entry_is_unresolved(self):
        client = LifecycleClient(position_size="5")
        sniper = configured_sniper(client)
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_client_order_id = "missing-entry"
        sniper.state.buy_size = 5.0

        result = await sniper.stop()

        self.assertFalse(result)
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.buy_client_order_id, "missing-entry")
        self.assertEqual(sniper.state.buy_size, 5.0)
        self.assertEqual(client.place_calls, [])

    async def test_unacknowledged_protective_sell_is_cancelled_before_market_exit(self):
        client = DelayedVisibilityClient(
            [
                {"status": "ACTIVE", "filled_size": "0"},
                {"status": "FILLED", "filled_size": "5", "filled_amount": "2"},
            ],
            position_size="5",
            hidden_order_reads=4,
        )
        sniper = configured_sniper(client)
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.30
        sniper.state.buy_size = 5.0

        result = await sniper._post_sell_at_price(0.40)

        self.assertTrue(result)
        self.assertEqual(client.cancel_calls[0]["client_order_id"], "coid-1")
        self.assertEqual(client.place_calls[0]["tif"], "gtc")
        self.assertEqual(client.place_calls[1]["tif"], "ioc")
        self.assertEqual(client.position_size, Decimal("0"))
        self.assertEqual(sniper.state.status, "monitoring")
        self.assertFalse(sniper.state.locked)
        self.assertFalse(hasattr(client, "cancel_all"))

    async def test_fine_tick_prices_are_not_rounded_to_zero(self):
        client = LifecycleClient(
            [{"status": "FILLED", "filled_size": "5", "filled_amount": "0.005"}],
            position_size="5",
        )
        sniper = configured_sniper(client)
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.002
        sniper.state.buy_size = 5.0

        async def fine_tick_book(asset_id, depth=5):
            return {
                "code": "0",
                "data": [{"bids": [["0.001", "100"]], "asks": [["0.999", "100"]]}],
            }

        client.get_orderbook = fine_tick_book
        result = await sniper._handle_timeout("m000", "YES")

        self.assertTrue(result)
        self.assertEqual(client.place_calls[0]["price"], "0.001")
        self.assertTrue(any("Exit @0.001" in line for line in sniper.state.logs))
        self.assertEqual(sniper._invert_bids_from_yes_book([["0.999", "10"]])[0][0], "0.001")
        self.assertEqual(sniper._invert_asks_from_yes_book([["0.001", "10"]])[0][0], "0.999")
        self.assertEqual(sniper._invert_bids_from_yes_book([["0.039", "10"]])[0][0], "0.961")
        self.assertEqual(sniper._invert_asks_from_yes_book([["0.961", "10"]])[0][0], "0.039")

    async def test_exit_preserves_0961_fine_tick(self):
        client = LifecycleClient(
            [{"status": "FILLED", "filled_size": "5", "filled_amount": "4.805"}],
            position_size="5",
        )
        sniper = configured_sniper(client)
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.90
        sniper.state.buy_size = 5.0

        async def fine_tick_book(asset_id, depth=5):
            return {
                "code": "0",
                "data": [{"bids": [["0.961", "100"]], "asks": [["0.039", "100"]]}],
            }

        client.get_orderbook = fine_tick_book
        result = await sniper._handle_timeout("m000", "YES")

        self.assertTrue(result)
        self.assertEqual(client.place_calls[0]["price"], "0.961")


if __name__ == "__main__":
    unittest.main()
