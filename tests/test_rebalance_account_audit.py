from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from backend.services import account_manager as account_module
from backend.services import auto_rebalance as rebalance_module
from backend.services.auto_rebalance import AutoRebalance


def strategy_params():
    return {
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


class RebalanceAuditTests(unittest.IsolatedAsyncioTestCase):
    def test_constructor_can_audit_missing_state_before_any_rebalance(self):
        with patch.object(rebalance_module, "audit_event") as emit:
            rebalance = AutoRebalance(object(), "audit-constructor-no-file")

        self.assertIsNone(rebalance._active_rebalance_id)
        missing = [
            call
            for call in emit.call_args_list
            if call.args and call.args[0] == "rebalance.state_missing"
        ]
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0].kwargs["rebalance_id"], "")

    async def test_one_rebalance_id_covers_every_success_stage(self):
        rebalance = AutoRebalance(object(), "audit-flow-no-file")
        rebalance._save_layers = lambda: None
        params = strategy_params()
        rebalance.get_strategy_params = lambda: params
        rebalance._cancel_all_verified = AsyncMock(
            return_value={"ok": True, "cancelled": 2, "remaining": 0}
        )
        rebalance._capture_partial_fills = AsyncMock()
        rebalance._reconcile_with_positions = AsyncMock()
        rebalance._wait_for_stable_price = AsyncMock(return_value=Decimal("0.56"))
        rebalance._sync_positions = AsyncMock()
        rebalance._post_layer_sells = AsyncMock(return_value=1)
        rebalance._restart = AsyncMock()
        rebalance._notify = AsyncMock()

        with patch.object(rebalance_module, "audit_event") as emit:
            result = await rebalance._do_rebalance(
                "Poly moved",
                "poly-token",
                Decimal("55"),
                "manual-s",
                rebalance_id="rb-fixed",
            )

        self.assertTrue(result)
        structured = [
            (call.args[0], call.kwargs)
            for call in emit.call_args_list
            if call.args and call.args[0].startswith("rebalance.")
        ]
        stages = [
            fields["stage"]
            for event, fields in structured
            if event == "rebalance.stage_started"
        ]
        self.assertEqual(
            stages,
            [
                "cancel_open_orders",
                "capture_buy_fills",
                "reconcile_positions",
                "stabilize_poly_price",
                "sync_positions",
                "post_protective_sells",
                "restart_monitor_and_strategy",
            ],
        )
        self.assertTrue(
            all(fields["rebalance_id"] == "rb-fixed" for _, fields in structured)
        )
        completed = next(
            fields for event, fields in structured if event == "rebalance.completed"
        )
        self.assertEqual(completed["protective_sell_count"], 1)
        self.assertGreaterEqual(completed["elapsed_ms"], 0)

    async def test_failure_records_the_exact_stage(self):
        rebalance = AutoRebalance(object(), "audit-failure-no-file")
        rebalance._save_layers = lambda: None
        params = strategy_params()
        rebalance.get_strategy_params = lambda: params
        rebalance._cancel_all_verified = AsyncMock(return_value={"ok": True})
        rebalance._capture_partial_fills = AsyncMock()
        rebalance._reconcile_with_positions = AsyncMock()
        rebalance._wait_for_stable_price = AsyncMock(return_value=Decimal("0.56"))
        rebalance._sync_positions = AsyncMock()
        rebalance._post_layer_sells = AsyncMock(
            side_effect=RuntimeError("protective order rejected")
        )
        rebalance._notify = AsyncMock()

        with patch.object(rebalance_module, "audit_event") as emit:
            result = await rebalance._do_rebalance(
                "Poly moved",
                "poly-token",
                Decimal("55"),
                "manual-s",
                rebalance_id="rb-failed",
            )

        self.assertFalse(result)
        failure = next(
            call.kwargs
            for call in emit.call_args_list
            if call.args and call.args[0] == "rebalance.failed"
        )
        self.assertEqual(failure["rebalance_id"], "rb-failed")
        self.assertEqual(failure["failed_stage"], "post_protective_sells")

    async def test_each_poly_stability_observation_is_audited(self):
        class Response:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def json(self):
                return {
                    "timestamp": "1785100000123",
                    "bids": [
                        {"price": "0.551", "size": "10"},
                        *[
                            {"price": f"0.{index:03d}", "size": str(index)}
                            for index in range(100, 111)
                        ],
                    ],
                    "asks": [
                        {"price": f"0.{index:03d}", "size": str(index)}
                        for index in range(700, 688, -1)
                    ],
                }

        class Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            def get(self, *_args, **_kwargs):
                return Response()

        rebalance = AutoRebalance(object(), "audit-stability-no-file")
        rebalance._active_rebalance_id = "rb-sample"
        with (
            patch("aiohttp.ClientSession", return_value=Session()),
            patch.object(rebalance_module.asyncio, "sleep", new=AsyncMock()),
            patch.object(rebalance_module, "audit_event") as emit,
            patch.object(rebalance_module, "market_event") as market_emit,
        ):
            price = await rebalance._wait_for_stable_price(
                "poly-token", stabilize_secs=0
            )

        self.assertEqual(price, Decimal("0.551"))
        sample = next(
            call.kwargs
            for call in emit.call_args_list
            if call.args and call.args[0] == "rebalance.poly_stability_sample"
        )
        self.assertEqual(sample["rebalance_id"], "rb-sample")
        self.assertEqual(sample["current_bid"], Decimal("0.551"))
        self.assertEqual(sample["attempt"], 1)
        self.assertGreaterEqual(sample["elapsed_ms"], 0)
        market = market_emit.call_args
        self.assertEqual(market.args, ("poly_rest", "rebalance_stability_book"))
        self.assertEqual(market.kwargs["rebalance_id"], "rb-sample")
        self.assertEqual(market.kwargs["source_timestamp"], "1785100000123")
        self.assertEqual(len(market.kwargs["bids"]), 10)
        self.assertEqual(len(market.kwargs["asks"]), 10)
        self.assertEqual(market.kwargs["bids"][0]["price"], "0.551")
        self.assertEqual(market.kwargs["asks"][0]["price"], "0.689")
        self.assertGreater(market.kwargs["received_wall_ns"], 0)
        self.assertGreater(market.kwargs["received_monotonic_ns"], 0)


class AccountAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_load_audit_contains_route_but_no_credentials(self):
        credentials = {
            "id": "account-a",
            "name": "Account A",
            "api_key": "api-secret-value",
            "secret_key": "signing-secret-value",
            "passphrase": "passphrase-secret-value",
            "agent_private_key": "private-key-secret-value",
            "base_url": "https://us.okx.com",
            "proxy_node": "JP2",
        }

        class Client:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        instance = SimpleNamespace()
        manager = account_module.AccountManager()
        with (
            patch.object(
                account_module, "get_account_decrypted", return_value=credentials
            ),
            patch.object(account_module, "get_sniper_state", return_value=None),
            patch.object(account_module, "OKXOutcomesClient", Client),
            patch.object(account_module, "AccountInstance", return_value=instance),
            patch.object(account_module, "audit_event") as emit,
        ):
            loaded = await manager._load_account("account-a")

        self.assertIs(loaded, instance)
        rendered = repr(emit.call_args_list)
        for secret in (
            credentials["api_key"],
            credentials["secret_key"],
            credentials["passphrase"],
            credentials["agent_private_key"],
        ):
            self.assertNotIn(secret, rendered)
        self.assertIn("https://us.okx.com", rendered)
        self.assertIn("JP2", rendered)
        events = [call.args[0] for call in emit.call_args_list]
        self.assertIn("account.load_started", events)
        self.assertIn("account.load_completed", events)

    async def test_account_close_has_timed_start_and_completion_events(self):
        class Idle:
            running = False

        class Client:
            async def close(self):
                return None

        instance = account_module.AccountInstance.__new__(
            account_module.AccountInstance
        )
        instance.account_id = "account-close"
        instance.auto_rebalance = SimpleNamespace(cancel=AsyncMock())
        instance.strategy_d = Idle()
        instance.strategy_s = Idle()
        instance.poly_monitor = SimpleNamespace(
            running=False, state=SimpleNamespace(enabled=False)
        )
        instance.sniper = Idle()
        instance.sniper_old = Idle()
        instance.client = Client()

        with patch.object(account_module, "audit_event") as emit:
            await instance.close(cancel_orders=False)

        events = [call.args[0] for call in emit.call_args_list]
        self.assertEqual(events[0], "account.close_started")
        self.assertEqual(events[-1], "account.close_completed")
        self.assertGreaterEqual(emit.call_args.kwargs["elapsed_ms"], 0)

    async def test_sniper_restore_and_followup_persist_are_audited(self):
        credentials = {
            "id": "account-restore",
            "name": "Account Restore",
            "api_key": "api",
            "secret_key": "secret",
            "passphrase": "pass",
            "agent_private_key": "private",
            "base_url": "https://us.okx.com",
            "proxy_node": "JP2",
        }
        sniper = SimpleNamespace(
            restore=AsyncMock(return_value={"ok": True}),
            state=SimpleNamespace(status="stopped", locked=False),
        )
        instance = SimpleNamespace(
            sniper=sniper,
            _sniper_snapshot_corrupt=False,
            _sniper_snapshot_write_blocked=False,
            _persist_sniper_snapshot=MagicMock(return_value=True),
        )
        snapshot = {
            "config": {"targets": [{"id": "target-1"}]},
            "runtime": {"status": "running"},
            "running": True,
            "corrupt": False,
        }
        manager = account_module.AccountManager()

        with (
            patch.object(
                account_module, "get_account_decrypted", return_value=credentials
            ),
            patch.object(account_module, "get_sniper_state", return_value=snapshot),
            patch.object(account_module, "OKXOutcomesClient"),
            patch.object(account_module, "AccountInstance", return_value=instance),
            patch.object(account_module, "audit_event") as emit,
        ):
            loaded = await manager._load_account("account-restore")

        self.assertIs(loaded, instance)
        sniper.restore.assert_awaited_once()
        instance._persist_sniper_snapshot.assert_called_once_with()
        events = [call.args[0] for call in emit.call_args_list]
        self.assertIn("sniper.snapshot_loaded", events)
        self.assertIn("sniper.restore_started", events)
        self.assertIn("sniper.restore_completed", events)
        completed = next(
            call.kwargs
            for call in emit.call_args_list
            if call.args[0] == "sniper.restore_completed"
        )
        self.assertTrue(completed["ok"])
        self.assertGreaterEqual(completed["elapsed_ms"], 0)

    async def test_reload_audit_never_contains_loaded_credentials(self):
        credentials = {
            "api_key": "reload-api-secret",
            "secret_key": "reload-signing-secret",
            "passphrase": "reload-passphrase-secret",
            "agent_private_key": "reload-private-key-secret",
            "base_url": "https://www.okx.com",
            "proxy_node": "JP1",
        }

        class OldClient:
            base_url = "https://us.okx.com"
            proxy_node = "JP2"

            async def close(self):
                return None

        class NewClient:
            def __init__(self, **kwargs):
                self.base_url = kwargs["base_url"]
                self.proxy_node = kwargs["proxy_node"]
                self.signer = SimpleNamespace(address="0x1")

        instance = account_module.AccountInstance.__new__(
            account_module.AccountInstance
        )
        instance.account_id = "account-reload"
        instance.client = OldClient()
        instance.auto_rebalance = SimpleNamespace(cancel=AsyncMock(), client=None)
        instance.strategy_d = SimpleNamespace(client=None)
        instance.strategy_s = SimpleNamespace(client=None)
        instance.poly_monitor = SimpleNamespace(client=None)
        instance.sniper = SimpleNamespace(client=None)
        instance.sniper_old = SimpleNamespace(client=None)

        with (
            patch.object(
                account_module, "get_account_decrypted", return_value=credentials
            ),
            patch.object(account_module, "OKXOutcomesClient", NewClient),
            patch.object(account_module, "audit_event") as emit,
        ):
            await instance.reload_client()

        rendered = repr(emit.call_args_list)
        for field in (
            "api_key",
            "secret_key",
            "passphrase",
            "agent_private_key",
        ):
            self.assertNotIn(credentials[field], rendered)
        events = [call.args[0] for call in emit.call_args_list]
        self.assertIn("account.reload_started", events)
        self.assertIn("account.client_installed", events)
        self.assertIn("account.reload_completed", events)
        self.assertIn("https://www.okx.com", rendered)
        self.assertIn("JP1", rendered)


if __name__ == "__main__":
    unittest.main()
