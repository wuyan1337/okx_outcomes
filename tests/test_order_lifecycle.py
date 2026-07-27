import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from backend.services import order_lifecycle


class LifecycleClient:
    def __init__(self, place_response, observations):
        self.place_response = place_response
        self.observations = list(observations)
        self.place_calls = []
        self.cancel_calls = []
        self.order_queries = 0
        self.poll_index = -1

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return dict(self.place_response)

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return {"code": "0", "data": {"txHash": "cancel-tx"}}

    async def get_orders(self, status="open", market_id=None):
        self.order_queries += 1
        if status == "open":
            self.poll_index += 1
        index = min(max(self.poll_index, 0), len(self.observations) - 1)
        current = self.observations[index] if self.observations else None
        terminal = {"FILLED", "PARTIALLY_FILLED", "FAILED", "CANCELLED", "EXPIRED"}
        rows = []
        if current and ((current["status"] in terminal) == (status == "closed")):
            rows = [dict(current)]
        return {"code": "0", "data": {"list": rows}}


def order(status, client_order_id="owned-1", filled="0"):
    return {
        "oid": "exchange-1",
        "clientOrderId": client_order_id,
        "marketId": "market-1",
        "assetId": "asset-1",
        "side": "BUY",
        "status": status,
        "price": "0.55",
        "size": "10",
        "filledSize": filled,
        "filledAmount": "0",
    }


class OrderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_code_zero_is_polled_until_active(self):
        client = LifecycleClient(
            {"code": "0", "data": {"txHash": "place-tx"}},
            [order("PENDING_PLACE"), order("ACTIVE")],
        )

        result = await order_lifecycle.place_and_confirm(
            client,
            client_order_id="owned-1",
            market_id="market-1",
            asset_id="asset-1",
            side="buy",
            price="0.55",
            size="10",
            attempts=2,
            delay=0,
        )

        self.assertTrue(result.confirmed)
        self.assertEqual(result.observation.status, "ACTIVE")
        self.assertGreaterEqual(client.order_queries, 2)

    async def test_timeout_is_reconciled_by_same_client_order_id(self):
        client = LifecycleClient(
            {"code": "TIMEOUT", "msg": "timed out"},
            [order("FILLED", filled="10")],
        )

        result = await order_lifecycle.place_and_confirm(
            client,
            client_order_id="owned-1",
            market_id="market-1",
            asset_id="asset-1",
            side="buy",
            price="0.55",
            size="10",
            attempts=1,
            delay=0,
        )

        self.assertTrue(result.confirmed)
        self.assertEqual(result.observation.status, "FILLED")
        self.assertEqual(result.observation.filled_size, 10)

    async def test_pending_place_never_counts_as_gtc_confirmation(self):
        client = LifecycleClient(
            {"code": "0", "data": {"txHash": "place-tx"}},
            [order("PENDING_PLACE")],
        )

        result = await order_lifecycle.place_and_confirm(
            client,
            client_order_id="owned-1",
            market_id="market-1",
            asset_id="asset-1",
            side="buy",
            price="0.55",
            size="10",
            attempts=1,
            delay=0,
        )

        self.assertFalse(result.confirmed)
        self.assertTrue(result.unresolved)

    async def test_explicit_business_rejection_is_not_misreported_as_unknown(self):
        client = LifecycleClient(
            {"code": "51000", "message": "Minimum order amount not met"},
            [],
        )

        result = await order_lifecycle.place_and_confirm(
            client,
            client_order_id="owned-1",
            market_id="market-1",
            asset_id="asset-1",
            side="buy",
            price="0.55",
            size="1",
            attempts=1,
            delay=0,
        )

        self.assertTrue(result.rejected)
        self.assertFalse(result.unresolved)
        self.assertEqual(client.order_queries, 0)

    async def test_cancel_waits_for_terminal_status(self):
        client = LifecycleClient(
            {"code": "0"},
            [order("PENDING_CANCEL"), order("CANCELLED")],
        )

        result = await order_lifecycle.cancel_and_confirm(
            client,
            client_order_id="owned-1",
            order_id="",
            market_id="market-1",
            asset_id="asset-1",
            attempts=2,
            delay=0,
        )

        self.assertTrue(result.confirmed_terminal)
        self.assertEqual(result.observation.status, "CANCELLED")
        self.assertEqual(client.cancel_calls, [])

    async def test_cancel_waits_for_pending_place_then_cancels_active_order(self):
        client = LifecycleClient(
            {"code": "0"},
            [order("PENDING_PLACE"), order("ACTIVE"), order("CANCELLED")],
        )

        result = await order_lifecycle.cancel_and_confirm(
            client,
            client_order_id="owned-1",
            order_id="",
            market_id="market-1",
            asset_id="asset-1",
            attempts=3,
            delay=0,
        )

        self.assertTrue(result.confirmed_terminal)
        self.assertEqual(result.observation.status, "CANCELLED")
        self.assertEqual(client.cancel_calls, [{
            "asset_id": "asset-1",
            "client_order_id": "owned-1",
        }])


class AtomicStateTests(unittest.TestCase):
    def test_failed_replace_leaves_previous_state_intact(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text('{"version":2}', encoding="utf-8")

            with mock.patch.object(
                order_lifecycle.os, "replace", side_effect=OSError("disk failure")
            ):
                with self.assertRaisesRegex(OSError, "disk failure"):
                    order_lifecycle.atomic_write_json(path, {"version": 3})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"version": 2})
            self.assertEqual(list(path.parent.glob(f".{path.name}.*.tmp")), [])

    def test_archive_keeps_unsettled_orders_and_prunes_only_fully_accounted_group(self):
        entry = {
            "role": "entry_buy",
            "client_order_id": "buy-1",
            "status": "FILLED",
            "filled_size": "10",
            "layer_recorded_size": "10",
        }
        protection = {
            "role": "protective_sell",
            "client_order_id": "sell-1",
            "parent_buy_cloid": "buy-1",
            "status": "ACTIVE",
            "size": "10",
            "filled_size": "0",
            "accounted_fill_size": "0",
        }
        owned = {"buy-1": entry, "sell-1": protection}
        pending = {"buy-1": "10"}
        retry_counts = {"buy-1": 2}
        retry_after = {"buy-1": 123}
        archive = []

        archived = order_lifecycle.archive_completed_order_groups(
            owned_orders=owned,
            pending_sells=pending,
            sell_retry_counts=retry_counts,
            sell_retry_after=retry_after,
            archive=archive,
        )
        self.assertEqual(archived, 0)
        self.assertEqual(set(owned), {"buy-1", "sell-1"})

        protection["status"] = "FILLED"
        protection["filled_size"] = "10"
        protection["accounted_fill_size"] = "10"
        archived = order_lifecycle.archive_completed_order_groups(
            owned_orders=owned,
            pending_sells=pending,
            sell_retry_counts=retry_counts,
            sell_retry_after=retry_after,
            archive=archive,
        )

        self.assertEqual(archived, 1)
        self.assertEqual(owned, {})
        self.assertEqual(pending, {})
        self.assertEqual(retry_counts, {})
        self.assertEqual(retry_after, {})
        self.assertEqual(archive[0]["entry"]["client_order_id"], "buy-1")


if __name__ == "__main__":
    unittest.main()
