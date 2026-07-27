import time
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from backend.services import poly_sniper_old as sniper_old_module
from backend.services.poly_sniper_old import PolySniperOld, SniperOldState


class BookClient:
    def __init__(self, book):
        self.book = book

    async def get_orderbook(self, _asset_id, depth=5):
        return self.book


def configured_sniper(client):
    sniper = PolySniperOld(client, account_id="telemetry-account")
    sniper._telemetry_run_id = "telemetry-run"
    sniper.state = SniperOldState(
        status="monitoring",
        yes_token_id="poly-yes",
        no_token_id="poly-no",
        yes_asset_id="okx-yes",
        no_asset_id="okx-no",
        market_id="market-1",
    )
    return sniper


class PolySniperOldTelemetryTests(unittest.IsolatedAsyncioTestCase):
    def test_records_raw_poly_book_and_price_changes(self):
        sniper = configured_sniper(BookClient({}))
        book = {
            "event_type": "book",
            "asset_id": "poly-yes",
            "timestamp": "111",
            "bids": [{"price": "0.1234", "size": "7"}],
            "asks": [{"price": "0.8766", "size": "8"}],
        }
        changes = {
            "event_type": "price_change",
            "timestamp": "112",
            "price_changes": [
                {
                    "asset_id": "poly-no",
                    "price": "0.4567",
                    "best_bid": "0.4501",
                }
            ],
        }

        with patch.object(sniper_old_module, "market_event") as emit:
            sniper._record_poly_ws_observation(
                book,
                received_wall_ns=10,
                received_monotonic_ns=20,
                frame_sequence=3,
                source_event_index=0,
            )
            sniper._record_poly_ws_observation(
                changes,
                received_wall_ns=11,
                received_monotonic_ns=21,
                frame_sequence=4,
                source_event_index=1,
            )

        book_call, change_call = emit.call_args_list
        self.assertEqual(book_call.args, ("polymarket", "sniper_old.ws_book"))
        self.assertIs(book_call.kwargs["raw_event"], book)
        self.assertEqual(book_call.kwargs["bids"], book["bids"])
        self.assertEqual(book_call.kwargs["asks"], book["asks"])
        self.assertEqual(book_call.kwargs["received_wall_ns"], 10)
        self.assertEqual(book_call.kwargs["received_monotonic_ns"], 20)
        self.assertEqual(book_call.kwargs["frame_sequence"], 3)

        self.assertEqual(
            change_call.args,
            ("polymarket", "sniper_old.ws_price_change"),
        )
        self.assertIs(change_call.kwargs["raw_event"], changes)
        self.assertEqual(
            change_call.kwargs["price_changes"], changes["price_changes"]
        )
        self.assertEqual(change_call.kwargs["source_event_index"], 1)

    async def test_bid_decision_contains_both_real_poly_sides_and_window(self):
        sniper = configured_sniper(BookClient({}))
        sniper.config.threshold_cents = 40
        sniper.config.window_ms = 150
        sniper._ws_calibrated["yes"] = True
        sniper.state.last_yes_bid = 0.10
        sniper.state.last_no_bid = 0.25
        sniper._baseline["yes"] = 0.10
        sniper._price_window["yes"] = [(time.monotonic(), 0.10)]

        with patch.object(sniper_old_module, "market_event") as emit:
            await sniper._process_book_update(
                {
                    "event_type": "book",
                    "asset_id": "poly-yes",
                    "bids": [{"price": "0.20", "size": "10"}],
                },
                received_wall_ns=101,
                received_monotonic_ns=202,
            )

        decision = next(
            call
            for call in emit.call_args_list
            if call.args == ("polymarket", "sniper_old.bid_decision")
        )
        self.assertEqual(decision.kwargs["decision"], "below_threshold")
        self.assertEqual(decision.kwargs["side"], "yes")
        self.assertEqual(decision.kwargs["previous_bid"], "0.10")
        self.assertEqual(decision.kwargs["current_bid"], "0.20")
        self.assertEqual(decision.kwargs["yes_bid"], "0.20")
        self.assertEqual(decision.kwargs["no_bid"], "0.25")
        self.assertEqual(decision.kwargs["window_low"], "0.10")
        self.assertEqual(decision.kwargs["threshold_cents"], "40")
        self.assertEqual(decision.kwargs["received_wall_ns"], 101)
        self.assertEqual(decision.kwargs["received_monotonic_ns"], 202)
        self.assertGreaterEqual(decision.kwargs["decision_duration_ms"], 0)

    async def test_trigger_decision_records_exact_window_before_okx_read(self):
        sniper = configured_sniper(BookClient({}))
        sniper.config.threshold_cents = 10
        sniper.config.window_ms = 150
        sniper._ws_calibrated["yes"] = True
        sniper.state.last_yes_bid = 0.10
        sniper.state.last_no_bid = 0.70
        sniper._baseline["yes"] = 0.10
        sniper._price_window["yes"] = [(time.monotonic(), 0.10)]
        sniper._snapshot_okx_asks = AsyncMock(return_value=[])

        with (
            patch.object(sniper_old_module, "market_event") as emit,
            patch.object(sniper_old_module, "audit_event"),
        ):
            await sniper._process_book_update(
                {
                    "event_type": "book",
                    "asset_id": "poly-yes",
                    "bids": [{"price": "0.20", "size": "10"}],
                },
                received_wall_ns=303,
                received_monotonic_ns=404,
            )

        trigger = next(
            call
            for call in emit.call_args_list
            if call.args == ("polymarket", "sniper_old.bid_decision")
            and call.kwargs["decision"] == "trigger"
        )
        self.assertEqual(trigger.kwargs["yes_bid"], "0.20")
        self.assertEqual(trigger.kwargs["no_bid"], "0.70")
        self.assertEqual(trigger.kwargs["window_low"], "0.10")
        self.assertEqual(trigger.kwargs["jump_cents"], "10.0")
        self.assertEqual(trigger.kwargs["threshold_cents"], "10")
        self.assertEqual(trigger.kwargs["window_ms"], 150)
        self.assertEqual(trigger.kwargs["window_points"], 2)
        sniper._snapshot_okx_asks.assert_awaited_once_with("okx-yes")

    async def test_settle_completion_records_timing_and_exit_decision(self):
        sniper = configured_sniper(BookClient({}))
        sniper._running = True
        sniper.config.settle_ms = 0
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.30
        sniper.state.settling = True
        sniper.state.settle_side = "yes"
        sniper.state.settle_initial = 0.10
        sniper.state.settle_price = 0.40
        sniper.state.settle_last_update = time.monotonic()
        sniper._market_exit = AsyncMock(return_value=True)

        with (
            patch.object(sniper_old_module, "market_event") as emit,
            patch.object(sniper_old_module, "audit_event"),
        ):
            await sniper._settle_then_sell()

        completed = next(
            call
            for call in emit.call_args_list
            if call.args == ("polymarket", "sniper_old.settle")
            and call.kwargs["phase"] == "completed"
        )
        self.assertEqual(completed.kwargs["initial_bid"], "0.10")
        self.assertEqual(completed.kwargs["final_bid"], "0.40")
        self.assertEqual(completed.kwargs["buy_price"], "0.30")
        self.assertEqual(completed.kwargs["configured_settle_ms"], 0)
        self.assertEqual(completed.kwargs["decision"], "exit_settled")
        self.assertGreaterEqual(completed.kwargs["stable_elapsed_ms"], 0)
        sniper._market_exit.assert_awaited_once_with(
            "okx-yes", "YES", reason="settled"
        )

    async def test_entry_book_logs_raw_yes_book_and_selected_no_top(self):
        raw = {
            "code": "0",
            "data": [
                {
                    "ts": "500",
                    "bids": [["0.60", "12"], ["0.59", "8"]],
                    "asks": [["0.61", "5"]],
                }
            ],
        }
        sniper = configured_sniper(BookClient(raw))

        with patch.object(sniper_old_module, "market_event") as emit:
            selected = await sniper._snapshot_okx_asks("okx-no")

        self.assertEqual(selected[0], ["0.40", "12"])
        event = emit.call_args
        self.assertEqual(event.args, ("okx", "sniper_old.rest_book"))
        self.assertEqual(event.kwargs["purpose"], "entry")
        self.assertEqual(event.kwargs["raw_book"], raw["data"][0])
        self.assertEqual(event.kwargs["yes_best_bid"], "0.60")
        self.assertEqual(event.kwargs["yes_best_ask"], "0.61")
        self.assertEqual(event.kwargs["selected_best_ask"], "0.40")
        self.assertTrue(event.kwargs["derived_from_yes_book"])
        self.assertGreaterEqual(event.kwargs["duration_ms"], 0)
        self.assertIsInstance(event.kwargs["received_wall_ns"], int)
        self.assertIsInstance(event.kwargs["received_monotonic_ns"], int)

    async def test_telemetry_failure_does_not_change_entry_book_result(self):
        raw = {
            "code": "0",
            "data": [{"bids": [["0.60", "12"]], "asks": [["0.61", "5"]]}],
        }
        sniper = configured_sniper(BookClient(raw))

        with (
            patch.object(
                sniper_old_module,
                "market_event",
                side_effect=RuntimeError("log queue unavailable"),
            ),
            patch.object(sniper_old_module.logger, "exception") as log_failure,
        ):
            selected = await sniper._snapshot_okx_asks("okx-no")

        self.assertEqual(selected, [["0.40", "12"]])
        log_failure.assert_called_once()

    async def test_exit_book_logs_raw_book_selected_top_and_preorder_decision(self):
        raw = {
            "code": "0",
            "data": [
                {
                    "ts": "600",
                    "bids": [["0.40", "100"]],
                    "asks": [["0.60", "100"]],
                }
            ],
        }
        sniper = configured_sniper(BookClient(raw))
        sniper._running = True
        sniper.state.status = "holding"
        sniper.state.holding_side = "yes"
        sniper.state.buy_price = 0.30
        sniper.state.buy_size = 5.0
        sniper._cancel_tracked_sell = AsyncMock(return_value=True)
        sniper._finish_verified_exit = AsyncMock(return_value=True)
        confirmation = SimpleNamespace(response={})
        observation = SimpleNamespace(filled_size=Decimal("5"))
        sniper._submit_ioc_and_confirm = AsyncMock(
            return_value=("terminal", observation, confirmation)
        )

        with patch.object(sniper_old_module, "market_event") as emit:
            result = await sniper._handle_timeout("okx-yes", "YES")

        self.assertTrue(result)
        book_event = next(
            call
            for call in emit.call_args_list
            if call.args == ("okx", "sniper_old.rest_book")
        )
        self.assertEqual(book_event.kwargs["purpose"], "exit")
        self.assertEqual(book_event.kwargs["raw_book"], raw["data"][0])
        self.assertEqual(book_event.kwargs["selected_best_bid"], "0.40")
        self.assertEqual(book_event.kwargs["remaining_size"], "5.0")
        self.assertGreaterEqual(book_event.kwargs["duration_ms"], 0)

        decision = next(
            call
            for call in emit.call_args_list
            if call.args == ("okx", "sniper_old.exit_decision")
        )
        self.assertEqual(decision.kwargs["decision"], "submit_ioc_sell")
        self.assertEqual(decision.kwargs["selected_best_bid"], "0.40")
        self.assertEqual(decision.kwargs["buy_price"], "0.30")
        self.assertIsInstance(decision.kwargs["decision_wall_ns"], int)
        self.assertIsInstance(decision.kwargs["decision_monotonic_ns"], int)


if __name__ == "__main__":
    unittest.main()
