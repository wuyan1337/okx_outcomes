import asyncio
import json
import time
import types
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from backend.services.poly_sniper import (
    PolySniper,
    SniperState,
    SniperTarget,
    TargetRuntime,
    _okx_price_text,
)


def target(target_id="a", token="poly-a", side="yes"):
    return SniperTarget(
        id=target_id,
        name=target_id,
        poly_token_id=token,
        market_id=f"market-{target_id}",
        yes_asset_id=f"yes-{target_id}",
        no_asset_id=f"no-{target_id}",
        side=side,
    )


class NoopClient:
    pass


class StringBookClient:
    async def get_orderbook(self, asset_id, depth=10):
        return {
            "code": "0",
            "data": [{
                "asks": [["0.21", "3.5"], ["0.20", "2"]],
                "bids": [["0.19", "4"]],
            }],
        }


class TriggerSniper(PolySniper):
    def __init__(self):
        super().__init__(NoopClient())
        self.entries = []

    async def _snapshot_okx_asks(self, selected):
        await asyncio.sleep(0)
        return [(Decimal("0.19"), Decimal("10"))]

    async def _execute_entry(self, selected, levels, previous_bid, current_bid):
        self.entries.append(selected.id)
        self.state.buy_size = Decimal("1")
        return True

    async def _holding_loop(self):
        return


class StartableSniper(PolySniper):
    async def _get_poly_bid(self, token_id):
        return Decimal("0.10")

    async def _monitor_loop(self):
        await asyncio.Event().wait()


class EntryClient:
    def __init__(self, filled_size="2.5", filled_amount="0.5"):
        self.cancelled = []
        self.place_calls = []
        self.filled_size = filled_size
        self.filled_amount = filled_amount

    def generate_client_order_id(self):
        return "entry-1"

    async def get_positions(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "10"}]}

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return {"code": "0", "_clientOrderId": "entry-1"}

    async def get_orders(self, status="open", market_id=None):
        if status == "open":
            return {"code": "0", "data": {"list": []}}
        return {
            "code": "0",
            "data": {"list": [{
                "clientOrderId": "entry-1",
                "assetId": "yes-a",
                "side": "BUY",
                "filledSize": self.filled_size,
                "filledAmount": self.filled_amount,
            }]},
        }

    async def cancel_order(self, **kwargs):
        self.cancelled.append(kwargs)
        return {"code": "0"}


class IocLifecycleSniper(PolySniper):
    async def _snapshot_okx_asks(self, selected):
        return [(Decimal("0.20"), Decimal("10"))]

    async def _holding_loop(self):
        return


class RestoreClient:
    async def get_orders(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}

    async def get_positions(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": [{
            "marketId": "market-a",
            "tokenIndex": "1",
            "tokenName": "Yes",
            "size": "5",
        }]}}


class PreallocatedExceptionClient:
    def __init__(self):
        self.place_calls = []

    def generate_client_order_id(self):
        return "preallocated-entry"

    async def get_positions(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "10"}]}

    async def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        raise asyncio.TimeoutError("request outcome is unknown")


class MinimumNotionalClient:
    def __init__(self):
        self.place_count = 0

    async def get_positions(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "10"}]}

    async def place_order(self, **kwargs):
        self.place_count += 1
        return {"code": "0", "_clientOrderId": "must-not-place"}


class TimeoutRecoveredEntryClient:
    def __init__(self):
        self.cancelled = []

    def generate_client_order_id(self):
        return "entry-timeout"

    async def get_positions(self, status="open", market_id=None):
        return {"code": "0", "data": {"list": []}}

    async def get_balance(self):
        return {"code": "0", "data": [{"oddsType": "points", "available": "10"}]}

    async def place_order(self, **kwargs):
        return {"code": "TIMEOUT", "_clientOrderId": kwargs["client_order_id"]}

    async def cancel_order(self, **kwargs):
        self.cancelled.append(kwargs)
        return {"code": "0"}

    async def get_orders(self, status="open", market_id=None):
        rows = []
        if status == "closed":
            rows.append({
                "clientOrderId": "entry-timeout",
                "assetId": "yes-a",
                "side": "BUY",
                "filledSize": "2",
                "filledAmount": "0.4",
            })
        return {"code": "0", "data": {"list": rows}}


class RestoreOrderClient:
    def __init__(self, open_client_order_ids=(), position_size="0"):
        self.open_client_order_ids = list(open_client_order_ids)
        self.position_size = position_size
        self.cancelled = []
        self.position_queries = 0

    async def get_orders(self, status="open", market_id=None):
        if status == "open":
            return {
                "code": "0",
                "data": {"list": [
                    {"clientOrderId": client_order_id}
                    for client_order_id in self.open_client_order_ids
                ]},
            }
        return {"code": "0", "data": {"list": []}}

    async def cancel_order(self, **kwargs):
        self.cancelled.append(kwargs)
        return {"code": "0"}

    async def get_positions(self, status="open", market_id=None):
        self.position_queries += 1
        rows = []
        if Decimal(self.position_size) > 0:
            rows.append({
                "marketId": "market-a",
                "tokenIndex": "1",
                "tokenName": "Yes",
                "size": self.position_size,
            })
        return {"code": "0", "data": {"list": rows}}


class FastFinalSniper(PolySniper):
    def __init__(self, client, final=False, filled_size="0", filled_amount="0"):
        super().__init__(client)
        self.final = final
        self.filled_size = Decimal(filled_size)
        self.filled_amount = Decimal(filled_amount)
        self.final_queries = []

    async def _wait_order_final(self, client_oid, asset_id, side, attempts=10):
        self.final_queries.append((client_oid, asset_id, side))
        return self.filled_size, self.filled_amount, self.final


class ExitTimeoutClient:
    def __init__(self):
        self.place_count = 0

    def generate_client_order_id(self):
        return "exit-timeout"

    async def place_order(self, **kwargs):
        self.place_count += 1
        raise asyncio.TimeoutError("unknown exit result")


class ClosedSellClient:
    async def get_orders(self, status="open", market_id=None):
        rows = []
        if status == "closed":
            rows.append({
                "clientOrderId": "exit-final",
                "assetId": "yes-a",
                "side": "SELL",
                "filledSize": "3.25",
                "filledAmount": "0.65",
            })
        return {"code": "0", "data": {"list": rows}}


class ExitTimeoutSniper(FastFinalSniper):
    async def _snapshot_okx_bids(self, selected):
        return [(Decimal("0.20"), Decimal("10"))]


class _TraceWebSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)

    async def send_str(self, payload):
        self.sent.append(json.loads(payload))


def _trace_book_message(
    action,
    bids,
    asks,
    sequence,
    previous_sequence,
    checksum=-702280706,
):
    return types.SimpleNamespace(
        type=1,  # aiohttp.WSMsgType.TEXT
        data=json.dumps({
            "arg": {"channel": "pm-books", "instId": "yes-a"},
            "action": action,
            "data": [{
                "bids": bids,
                "asks": asks,
                "seqId": sequence,
                "prevSeqId": previous_sequence,
                "checksum": checksum,
                "ts": str(1_700_000_000_000 + sequence),
            }],
        }),
    )


class PolySniperTests(unittest.IsolatedAsyncioTestCase):
    async def test_trigger_candidate_links_poly_frame_trace_and_receipt_clock(self):
        sniper = PolySniper(NoopClient())
        selected = target()
        sniper.targets = [selected]
        sniper._runtime = {selected.id: TargetRuntime()}
        sniper._token_targets = {selected.poly_token_id: [selected.id]}
        sniper.state = SniperState(status="monitoring")
        sniper._running = True
        sniper.config.threshold_cents = Decimal("1")
        sniper.config.window_ms = 1000
        sniper._try_trigger = AsyncMock()

        with patch("backend.services.poly_sniper.market_event") as market_log:
            await sniper._process_bid(
                selected.poly_token_id,
                Decimal("0.10"),
                signal_received_ns=111,
                frame_id="run:poly:1",
            )
            await sniper._process_bid(
                selected.poly_token_id,
                Decimal("0.20"),
                signal_received_ns=222,
                frame_id="run:poly:2",
                source_event_index=3,
                source_change_index=4,
            )
            if sniper._trigger_tasks:
                await asyncio.gather(*tuple(sniper._trigger_tasks))

        trigger_event = next(
            call
            for call in market_log.call_args_list
            if call.kwargs.get("decision") == "trigger_scheduled"
        )
        trace_id = trigger_event.kwargs["trace_id"]
        self.assertTrue(trace_id)
        self.assertEqual(trigger_event.kwargs["frame_id"], "run:poly:2")
        self.assertEqual(trigger_event.kwargs["signal_received_perf_ns"], 222)
        self.assertEqual(trigger_event.kwargs["source_event_index"], 3)
        self.assertEqual(trigger_event.kwargs["source_change_index"], 4)
        sniper._try_trigger.assert_awaited_once_with(
            selected.id,
            Decimal("0.10"),
            Decimal("0.20"),
            signal_received_ns=222,
            trace_id=trace_id,
            frame_id="run:poly:2",
        )

    async def test_trigger_latency_uses_frame_receipt_and_preserves_trace_id(self):
        sniper = PolySniper(NoopClient())
        selected = target()
        sniper.targets = [selected]
        sniper._runtime = {selected.id: TargetRuntime()}
        sniper._token_targets = {selected.poly_token_id: [selected.id]}
        sniper.state = SniperState(status="monitoring")
        sniper._running = True
        receipt_ns = time.perf_counter_ns()
        captured = {}

        async def capture_trigger(*args):
            captured.update(sniper._latency_active or {})
            sniper._finish_latency_trace("test")

        sniper._run_trigger = capture_trigger
        await sniper._try_trigger(
            selected.id,
            Decimal("0.10"),
            Decimal("0.20"),
            signal_received_ns=receipt_ns,
            trace_id="trace-from-frame",
            frame_id="run:poly:9",
        )

        self.assertEqual(captured["_started_ns"], receipt_ns)
        self.assertEqual(captured["trace_id"], "trace-from-frame")
        self.assertEqual(captured["poly_frame_id"], "run:poly:9")
        self.assertEqual(sniper._latency_history[-1]["trace_id"], "trace-from-frame")

    async def test_poly_price_change_trace_does_not_truncate_large_frame(self):
        sniper = PolySniper(NoopClient())
        changes = [
            {
                "asset_id": f"poly-{index}",
                "best_bid": "0.20",
                "price": "0.20",
                "size": str(index + 1),
            }
            for index in range(75)
        ]
        sniper._process_bid = AsyncMock()

        with patch("backend.services.poly_sniper.market_event") as market_log:
            await sniper._process_ws_event({
                "event_type": "price_change",
                "price_changes": changes,
            })

        trace = market_log.call_args
        self.assertEqual(trace.args, ("polymarket", "sniper.price_change"))
        self.assertEqual(len(trace.kwargs["changes"]), 75)
        self.assertEqual(sniper._process_bid.await_count, 75)

    async def test_okx_trace_observer_logs_snapshot_update_and_no_view(self):
        upstream = _TraceWebSocket([
            _trace_book_message(
                "snapshot",
                [["0.40", "10"]],
                [["0.60", "8"]],
                10,
                -1,
            ),
            _trace_book_message(
                "update",
                [],
                [["0.55", "3"]],
                11,
                10,
            ),
        ])

        class FakeSession:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def ws_connect(self, *args, **kwargs):
                return upstream

        client = NoopClient()
        client.base_url = "https://www.okx.com"
        client._proxy_node = ""
        sniper = PolySniper(client, account_id="account-1")
        sniper.targets = [target(side="no")]
        sniper._running = True
        sniper._run_id = "run-1"

        with (
            patch(
                "backend.services.poly_sniper.aiohttp.ClientSession",
                FakeSession,
            ),
            patch("backend.services.poly_sniper.market_event") as market_log,
        ):
            await sniper._okx_market_trace_session()

        self.assertEqual(
            upstream.sent,
            [{
                "op": "subscribe",
                "args": [{"channel": "pm-books", "instId": "yes-a"}],
            }],
        )
        self.assertEqual(len(market_log.call_args_list), 2)
        snapshot = market_log.call_args_list[0]
        self.assertEqual(snapshot.args, ("okx", "sniper.book_update"))
        self.assertEqual(snapshot.kwargs["action"], "snapshot")
        self.assertEqual(snapshot.kwargs["sequence"], 10)
        self.assertEqual(snapshot.kwargs["checksum_reported"], -702280706)
        self.assertFalse(snapshot.kwargs["checksum_verified"])
        self.assertIn(
            "unspecified", snapshot.kwargs["checksum_verification_reason"]
        )
        self.assertEqual(snapshot.kwargs["target_views"][0]["side"], "no")
        self.assertTrue(
            snapshot.kwargs["target_views"][0]["derived_from_yes_book"]
        )
        update = market_log.call_args_list[1]
        self.assertEqual(update.kwargs["action"], "update")
        self.assertEqual(update.kwargs["previous_sequence"], 10)
        self.assertEqual(update.kwargs["target_views"][0]["best_bid"], "0.45")
        self.assertIn("received_wall_ns", update.kwargs)
        self.assertIn("received_monotonic_ns", update.kwargs)

    async def test_okx_trace_delta_is_not_truncated(self):
        large_delta = [
            [f"0.{index + 100:03d}", str(index + 1)]
            for index in range(75)
        ]
        upstream = _TraceWebSocket([
            _trace_book_message("snapshot", [["0.40", "10"]], [], 10, -1),
            _trace_book_message("update", large_delta, [], 11, 10),
        ])

        class FakeSession:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def ws_connect(self, *args, **kwargs):
                return upstream

        client = NoopClient()
        client.base_url = "https://www.okx.com"
        client._proxy_node = ""
        sniper = PolySniper(client)
        sniper.targets = [target()]
        sniper._running = True

        with (
            patch(
                "backend.services.poly_sniper.aiohttp.ClientSession",
                FakeSession,
            ),
            patch("backend.services.poly_sniper.market_event") as market_log,
        ):
            await sniper._okx_market_trace_session()

        update = market_log.call_args_list[-1]
        self.assertEqual(update.kwargs["action"], "update")
        self.assertEqual(len(update.kwargs["delta_bids"]), 75)

    async def test_okx_trace_sequence_gap_is_logged_and_rejected(self):
        upstream = _TraceWebSocket([
            _trace_book_message("snapshot", [["0.40", "10"]], [], 10, -1),
            _trace_book_message("update", [["0.41", "2"]], [], 12, 8),
        ])

        class FakeSession:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def ws_connect(self, *args, **kwargs):
                return upstream

        client = NoopClient()
        client.base_url = "https://www.okx.com"
        client._proxy_node = ""
        sniper = PolySniper(client)
        sniper.targets = [target()]
        sniper._running = True

        with (
            patch(
                "backend.services.poly_sniper.aiohttp.ClientSession",
                FakeSession,
            ),
            patch("backend.services.poly_sniper.market_event") as market_log,
        ):
            with self.assertRaisesRegex(RuntimeError, "sequence gap"):
                await sniper._okx_market_trace_session()

        self.assertEqual(market_log.call_args_list[-1].args[1], "sniper.sequence_gap")
        self.assertEqual(
            market_log.call_args_list[-1].kwargs["expected_previous"], 10
        )
        self.assertEqual(
            market_log.call_args_list[-1].kwargs["received_previous"], 8
        )

    async def test_stop_cancels_okx_trace_observer(self):
        sniper = PolySniper(NoopClient())
        sniper._running = True
        sniper.state = SniperState(status="monitoring")
        observer = asyncio.create_task(asyncio.Event().wait())
        sniper._okx_trace_task = observer

        await sniper.stop()

        self.assertTrue(observer.done())
        self.assertTrue(observer.cancelled())
        self.assertIsNone(sniper._okx_trace_task)

    def test_legacy_mapping_arms_only_yes(self):
        sniper = PolySniper(NoopClient())
        targets = sniper._parse_targets(
            None,
            yes_token_id="poly-yes",
            no_token_id="poly-no",
            yes_asset_id="okx-yes",
            no_asset_id="okx-no",
            market_id="market",
            poly_url="url",
            selected_outcome="Argentina",
        )
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].poly_token_id, "poly-yes")
        self.assertEqual(targets[0].side, "yes")

    def test_no_book_mirror_preserves_outer_tick(self):
        asks = PolySniper._invert_asks_from_yes_book([["0.999", "7"]])
        self.assertEqual(asks, [(Decimal("0.001"), Decimal("7"))])
        self.assertEqual(_okx_price_text(asks[0][0]), "0.001")

    async def test_yes_entry_book_rows_are_decimal_and_sorted(self):
        sniper = PolySniper(StringBookClient())

        asks = await sniper._snapshot_okx_asks(target())

        self.assertEqual(asks, [
            (Decimal("0.20"), Decimal("2")),
            (Decimal("0.21"), Decimal("3.5")),
        ])

    def test_duplicate_poly_token_targets_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "poly_token_id is already used"):
            PolySniper._validate_targets([
                target("a", "poly-duplicate"),
                target("b", "poly-duplicate"),
            ])

    async def test_no_first_target_uses_no_legacy_summary_fields(self):
        sniper = PolySniper(NoopClient())
        selected = target("a", "poly-no", side="no")
        sniper.targets = [selected]
        sniper._runtime = {selected.id: TargetRuntime()}
        sniper._token_targets = {selected.poly_token_id: [selected.id]}
        sniper.state = SniperState(status="monitoring")
        sniper._set_legacy_fields(selected)

        await sniper._process_bid(selected.poly_token_id, Decimal("0.1234"))
        state = sniper.get_state()

        self.assertEqual(state["yes_token_id"], "")
        self.assertEqual(state["no_token_id"], "poly-no")
        self.assertEqual(state["last_yes_bid"], "0")
        self.assertEqual(state["last_no_bid"], "0.1234")

    async def test_price_changes_and_global_lock_allow_one_entry(self):
        sniper = TriggerSniper()
        sniper.targets = [target("a", "poly-a"), target("b", "poly-b")]
        sniper._runtime = {item.id: TargetRuntime() for item in sniper.targets}
        sniper._token_targets = {item.poly_token_id: [item.id] for item in sniper.targets}
        sniper.state = SniperState(status="monitoring")
        sniper._running = True
        sniper.config.threshold_cents = Decimal("1")
        sniper.config.window_ms = 1000

        await sniper._process_bid("poly-a", Decimal("0.10"))
        await sniper._process_bid("poly-b", Decimal("0.10"))
        await sniper._process_bid("poly-a", Decimal("0.10"))
        await sniper._process_bid("poly-b", Decimal("0.10"))
        await asyncio.gather(
            sniper._process_ws_event({
                "event_type": "price_change",
                "price_changes": [{"asset_id": "poly-a", "best_bid": "0.20"}],
            }),
            sniper._process_ws_event({
                "event_type": "price_change",
                "price_changes": [{"asset_id": "poly-b", "best_bid": "0.20"}],
            }),
        )
        await asyncio.sleep(0.01)

        self.assertEqual(len(sniper.entries), 1)
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.active_target_id, sniper.entries[0])

    async def test_holding_task_does_not_start_when_snapshot_persistence_fails(self):
        sniper = TriggerSniper()
        selected = target("a", "poly-a")
        sniper.targets = [selected]
        sniper._runtime = {selected.id: TargetRuntime()}
        sniper.state = SniperState(
            status="triggered",
            locked=True,
            active_target_id=selected.id,
        )
        sniper._running = True
        sniper.on_state_change = lambda snapshot: False

        await sniper._run_trigger(
            selected.id,
            Decimal("0.10"),
            Decimal("0.20"),
        )

        self.assertFalse(sniper.running)
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper._persistence_failed)
        self.assertIsNone(sniper._hold_task)

    async def test_empty_book_invalidates_jump_window_before_quotes_return(self):
        sniper = TriggerSniper()
        selected = target("a", "poly-a")
        sniper.targets = [selected]
        sniper._runtime = {selected.id: TargetRuntime()}
        sniper._token_targets = {selected.poly_token_id: [selected.id]}
        sniper.state = SniperState(status="monitoring")
        sniper._running = True
        sniper.config.threshold_cents = Decimal("1")
        sniper.config.window_ms = 1000

        await sniper._process_bid("poly-a", Decimal("0.10"))
        await sniper._process_bid("poly-a", Decimal("0.10"))
        await sniper._process_ws_event({
            "event_type": "book",
            "asset_id": "poly-a",
            "bids": [],
        })
        await sniper._process_bid("poly-a", Decimal("0.20"))

        runtime = sniper._runtime[selected.id]
        self.assertEqual(sniper.entries, [])
        self.assertTrue(runtime.calibrated)
        self.assertEqual(len(runtime.price_window), 1)
        self.assertEqual(runtime.price_window[0][1], Decimal("0.20"))

    async def test_unexpected_pre_entry_error_releases_global_lock(self):
        sniper = PolySniper(NoopClient())
        selected = target()
        sniper.targets = [selected]
        sniper._runtime = {selected.id: TargetRuntime()}
        sniper._token_targets = {selected.poly_token_id: [selected.id]}
        sniper.state = SniperState(status="monitoring")
        sniper._running = True

        async def fail_book(_target):
            raise TypeError("bad orderbook row")

        sniper._snapshot_okx_asks = fail_book
        await sniper._try_trigger(selected.id, Decimal("0.10"), Decimal("0.20"))

        self.assertFalse(sniper.state.locked)
        self.assertEqual(sniper.state.status, "monitoring")
        self.assertEqual(sniper.state.active_target_id, "")

    async def test_partial_ioc_entry_is_kept_without_resting_order_cancel(self):
        client = EntryClient()
        sniper = PolySniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(status="triggered", locked=True, active_target_id="a")

        with patch(
            "backend.services.poly_sniper.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep_mock:
            result = await sniper._execute_entry(
                selected,
                [(Decimal("0.20"), Decimal("10"))],
                Decimal("0.10"),
                Decimal("0.30"),
            )

        self.assertTrue(result)
        self.assertEqual(sniper.state.buy_size, Decimal("2.5"))
        self.assertEqual(sniper.state.buy_price, Decimal("0.2"))
        self.assertEqual(client.place_calls[0]["tif"], "ioc")
        self.assertEqual(client.place_calls[0]["price"], "0.20")
        self.assertEqual(client.cancelled, [])
        sleep_mock.assert_not_awaited()
        json.dumps(sniper.get_state())

    async def test_partial_ioc_fill_enters_holding(self):
        client = EntryClient()
        sniper = IocLifecycleSniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(
            status="triggered",
            locked=True,
            active_target_id=selected.id,
        )
        sniper._running = True

        await sniper._run_trigger(
            selected.id,
            Decimal("0.10"),
            Decimal("0.30"),
        )

        self.assertEqual(sniper.state.status, "holding")
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.buy_size, Decimal("2.5"))
        self.assertEqual(client.place_calls[0]["tif"], "ioc")
        self.assertEqual(client.cancelled, [])
        self.assertIsNotNone(sniper._hold_task)
        await sniper._hold_task

    async def test_zero_ioc_fill_releases_global_lock(self):
        client = EntryClient(filled_size="0", filled_amount="0")
        sniper = IocLifecycleSniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(
            status="triggered",
            locked=True,
            active_target_id=selected.id,
        )
        sniper._running = True

        await sniper._run_trigger(
            selected.id,
            Decimal("0.10"),
            Decimal("0.30"),
        )

        self.assertEqual(sniper.state.status, "monitoring")
        self.assertFalse(sniper.state.locked)
        self.assertEqual(sniper.state.active_target_id, "")
        self.assertFalse(sniper.state.entry_placement_attempted)
        self.assertEqual(sniper.state.entry_client_order_id, "")
        self.assertEqual(client.place_calls[0]["tif"], "ioc")
        self.assertEqual(client.cancelled, [])

    async def test_restore_attributes_only_position_above_pre_entry_baseline(self):
        sniper = PolySniper(RestoreClient())
        saved = {
            "config": {
                "threshold_cents": "10",
                "buy_limit_cents": "50",
                "position_pct": "100",
                "timeout_secs": "3",
                "settle_ms": 0,
                "window_ms": 150,
            },
            "targets": [target().to_dict()],
            "locked": True,
            "active_target_id": "a",
            "holding": {
                "buy_price": "0.2",
                "buy_size": "4",
                "sold_size": "0",
                "position_before": "2",
            },
        }

        result = await sniper.restore(saved, resume=False)

        self.assertTrue(result["ok"])
        self.assertTrue(result["needs_reconciliation"])
        self.assertEqual(result["remaining_size"], "3")
        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.buy_size, Decimal("3"))

    async def test_restore_keeps_stopped_configuration_for_page_refresh(self):
        sniper = PolySniper(NoopClient())
        saved = self._locked_saved_state()
        saved["locked"] = False
        saved["active_target_id"] = ""
        saved["target_prices"] = {"a": "0.1234"}

        result = await sniper.restore(saved, resume=False)
        state = sniper.get_state()

        self.assertTrue(result["ok"])
        self.assertFalse(state["running"])
        self.assertEqual(state["config"]["threshold_cents"], "10")
        self.assertEqual(state["targets"][0]["id"], "a")
        self.assertEqual(state["target_prices"], {"a": "0.1234"})
        json.dumps(state)

    async def test_entry_client_order_id_is_persisted_before_network_call(self):
        client = PreallocatedExceptionClient()
        sniper = PolySniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(status="triggered", locked=True, active_target_id="a")
        snapshots = []
        sniper.on_state_change = lambda snapshot: snapshots.append(snapshot)

        result = await sniper._execute_entry(
            selected,
            [(Decimal("0.20"), Decimal("10"))],
            Decimal("0.10"),
            Decimal("0.30"),
        )

        self.assertTrue(result)
        self.assertEqual(client.place_calls[0]["client_order_id"], "preallocated-entry")
        persisted_before_call = next(
            snapshot for snapshot in snapshots
            if snapshot.get("holding", {}).get("entry_client_order_id") == "preallocated-entry"
        )
        self.assertTrue(persisted_before_call["holding"]["entry_placement_attempted"])
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper.state.locked)

    async def test_entry_below_one_xp_is_not_submitted(self):
        client = MinimumNotionalClient()
        sniper = PolySniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(status="triggered", locked=True, active_target_id="a")

        result = await sniper._execute_entry(
            selected,
            [(Decimal("0.50"), Decimal("1"))],
            Decimal("0.10"),
            Decimal("0.60"),
        )

        self.assertFalse(result)
        self.assertEqual(client.place_count, 0)
        self.assertFalse(sniper.state.entry_placement_attempted)

    async def test_entry_is_not_submitted_when_snapshot_persistence_fails(self):
        client = PreallocatedExceptionClient()
        sniper = PolySniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(status="triggered", locked=True, active_target_id="a")
        sniper.on_state_change = lambda snapshot: False

        result = await sniper._execute_entry(
            selected,
            [(Decimal("0.20"), Decimal("10"))],
            Decimal("0.10"),
            Decimal("0.30"),
        )

        self.assertFalse(result)
        self.assertEqual(client.place_calls, [])
        self.assertFalse(sniper.state.entry_placement_attempted)
        self.assertEqual(sniper.state.entry_client_order_id, "")

    async def test_timeout_entry_recovers_fill_by_client_order_id(self):
        client = TimeoutRecoveredEntryClient()
        sniper = PolySniper(client)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(status="triggered", locked=True, active_target_id="a")

        result = await sniper._execute_entry(
            selected,
            [(Decimal("0.20"), Decimal("10"))],
            Decimal("0.10"),
            Decimal("0.30"),
        )

        self.assertTrue(result)
        self.assertEqual(sniper.state.buy_size, Decimal("2"))
        self.assertEqual(sniper.state.buy_price, Decimal("0.2"))
        self.assertEqual(client.cancelled, [{
            "asset_id": "yes-a",
            "client_order_id": "entry-timeout",
        }])

    async def test_locked_stopped_state_rejects_start(self):
        sniper = PolySniper(NoopClient())
        sniper.state = SniperState(status="stopped", locked=True, active_target_id="a")

        result = await sniper.start(targets=[target().to_dict()])

        self.assertFalse(result["ok"])
        self.assertIn("unresolved", result["error"])

    async def test_start_aborts_when_initial_snapshot_cannot_be_persisted(self):
        sniper = StartableSniper(NoopClient())
        sniper.on_state_change = lambda snapshot: False

        result = await sniper.start(targets=[target().to_dict()])

        self.assertFalse(result["ok"])
        self.assertIn("persisted", result["error"])
        self.assertFalse(sniper.running)
        self.assertEqual(sniper.state.status, "stopped")
        self.assertIsNone(sniper._task)

    async def test_stop_persistence_failure_is_reported_and_must_be_retried(self):
        sniper = PolySniper(NoopClient())
        sniper.targets = [target()]
        sniper.state = SniperState(status="monitoring")
        sniper._running = True
        sniper.on_state_change = lambda snapshot: False

        result = await sniper.stop()

        self.assertFalse(result["ok"])
        self.assertTrue(result["persistence_error"])
        self.assertTrue(result["needs_reconciliation"])
        self.assertFalse(sniper.running)
        self.assertEqual(sniper.state.status, "error")

        restart = await sniper.start(targets=[target().to_dict()])
        self.assertFalse(restart["ok"])
        self.assertIn("retry Stop", restart["error"])

        snapshots = []
        sniper.on_state_change = lambda snapshot: snapshots.append(snapshot) or True
        retry = await sniper.stop()

        self.assertTrue(retry["ok"])
        self.assertFalse(sniper._persistence_failed)
        self.assertEqual(sniper.state.status, "stopped")
        self.assertFalse(snapshots[-1]["running"])
        self.assertFalse(snapshots[-1]["persistence_error"])

    async def test_concurrent_starts_create_only_one_monitor_task(self):
        sniper = StartableSniper(NoopClient())
        persistence_entered = asyncio.Event()
        allow_persistence = asyncio.Event()

        async def persist(_snapshot):
            persistence_entered.set()
            await allow_persistence.wait()
            return True

        sniper.on_state_change = persist
        first = asyncio.create_task(sniper.start(targets=[target().to_dict()]))
        await persistence_entered.wait()
        self.assertTrue(sniper.starting)
        second = asyncio.create_task(sniper.start(targets=[target().to_dict()]))
        await asyncio.sleep(0)
        self.assertFalse(second.done())

        allow_persistence.set()
        first_result, second_result = await asyncio.gather(first, second)

        self.assertTrue(first_result["ok"])
        self.assertFalse(second_result["ok"])
        self.assertIn("Already running", second_result["error"])
        self.assertFalse(sniper.starting)
        await sniper.stop()

    async def test_stop_keeps_lock_when_entry_final_state_is_unknown(self):
        client = RestoreOrderClient()
        sniper = FastFinalSniper(client, final=False)
        sniper.targets = [target()]
        sniper.state = SniperState(
            status="entering",
            locked=True,
            active_target_id="a",
            entry_client_order_id="entry-unknown",
            entry_placement_attempted=True,
        )
        sniper._running = True

        await sniper.stop()

        self.assertTrue(sniper.state.locked)
        self.assertEqual(sniper.state.status, "stopped")
        self.assertEqual(
            sniper.final_queries,
            [("entry-unknown", "yes-a", "BUY")],
        )

    async def test_repeated_stop_clears_lock_after_okx_is_flat(self):
        client = RestoreOrderClient(position_size="0")
        sniper = FastFinalSniper(client, final=True, filled_size="2", filled_amount="0.4")
        sniper.targets = [target()]
        sniper.state = SniperState(
            status="stopped",
            locked=True,
            active_target_id="a",
            buy_price=Decimal("0.2"),
            buy_size=Decimal("2"),
            entry_client_order_id="entry-final",
            entry_placement_attempted=True,
        )

        result = await sniper.stop()

        self.assertTrue(result["ok"])
        self.assertFalse(sniper.state.locked)
        self.assertEqual(sniper.state.active_target_id, "")
        self.assertEqual(sniper.state.status, "stopped")

    async def test_restore_cancels_open_entry_and_blocks_until_final(self):
        client = RestoreOrderClient(open_client_order_ids=["entry-open"])
        sniper = FastFinalSniper(client, final=False)
        saved = self._locked_saved_state(
            entry_client_order_id="entry-open",
            entry_placement_attempted=True,
        )

        result = await sniper.restore(saved, resume=True)

        self.assertFalse(result["ok"])
        self.assertTrue(result["needs_reconciliation"])
        self.assertTrue(sniper.state.locked)
        self.assertFalse(sniper.running)
        self.assertEqual(client.position_queries, 0)
        self.assertEqual(client.cancelled, [{
            "asset_id": "yes-a",
            "client_order_id": "entry-open",
        }])

    async def test_restore_uses_final_entry_fill_before_position_reconciliation(self):
        client = RestoreOrderClient(open_client_order_ids=["entry-open"], position_size="2")
        sniper = FastFinalSniper(client, final=True, filled_size="2", filled_amount="0.4")
        saved = self._locked_saved_state(
            entry_client_order_id="entry-open",
            entry_placement_attempted=True,
        )

        result = await sniper.restore(saved, resume=False)

        self.assertTrue(result["ok"])
        self.assertEqual(result["remaining_size"], "2")
        self.assertEqual(sniper.state.buy_size, Decimal("2"))
        self.assertEqual(sniper.state.buy_price, Decimal("0.2"))
        self.assertTrue(sniper.state.locked)

    async def test_restore_blocks_on_unresolved_exit_order(self):
        client = RestoreOrderClient(open_client_order_ids=["exit-open"], position_size="4")
        sniper = FastFinalSniper(client, final=False)
        saved = self._locked_saved_state(
            buy_size="4",
            exit_client_order_ids=["exit-open"],
        )

        result = await sniper.restore(saved, resume=True)

        self.assertFalse(result["ok"])
        self.assertTrue(result["needs_reconciliation"])
        self.assertEqual(client.position_queries, 0)
        self.assertEqual(client.cancelled, [{
            "asset_id": "yes-a",
            "client_order_id": "exit-open",
        }])

    async def test_restore_does_not_clear_zero_ledger_with_unresolved_exit(self):
        client = RestoreOrderClient(open_client_order_ids=["exit-open"], position_size="0")
        sniper = FastFinalSniper(client, final=False)
        saved = self._locked_saved_state(exit_client_order_ids=["exit-open"])

        result = await sniper.restore(saved, resume=True)

        self.assertFalse(result["ok"])
        self.assertTrue(sniper.state.locked)
        self.assertEqual(
            sniper.final_queries,
            [("exit-open", "yes-a", "SELL")],
        )

    async def test_exit_timeout_exception_does_not_submit_duplicate_sells(self):
        client = ExitTimeoutClient()
        sniper = ExitTimeoutSniper(client, final=False)
        selected = target()
        sniper.targets = [selected]
        sniper.state = SniperState(
            status="holding",
            locked=True,
            active_target_id="a",
            buy_size=Decimal("5"),
        )
        sniper._running = True

        await sniper._exit_position("test-timeout")

        self.assertEqual(client.place_count, 1)
        self.assertEqual(sniper.state.exit_client_order_ids, ["exit-timeout"])
        self.assertEqual(sniper.state.exit_resolved_client_order_ids, [])
        self.assertEqual(sniper.state.status, "stopped")
        self.assertTrue(sniper.state.locked)
        self.assertFalse(sniper.running)

    async def test_exit_is_not_submitted_when_snapshot_persistence_fails(self):
        client = ExitTimeoutClient()
        sniper = ExitTimeoutSniper(client, final=False)
        sniper.targets = [target()]
        sniper.state = SniperState(
            status="holding",
            locked=True,
            active_target_id="a",
            buy_size=Decimal("5"),
        )
        sniper._running = True
        sniper.on_state_change = lambda snapshot: False

        await sniper._exit_position("test-persistence-failure")

        self.assertEqual(client.place_count, 0)
        self.assertEqual(sniper.state.exit_client_order_ids, [])
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper.state.locked)
        self.assertTrue(sniper._persistence_failed)

    async def test_wait_order_final_reads_closed_sell_fill(self):
        sniper = PolySniper(ClosedSellClient())

        size, amount, final = await sniper._wait_order_final(
            "exit-final",
            "yes-a",
            "SELL",
            attempts=1,
        )

        self.assertTrue(final)
        self.assertEqual(size, Decimal("3.25"))
        self.assertEqual(amount, Decimal("0.65"))

    @staticmethod
    def _locked_saved_state(**holding_overrides):
        holding = {
            "buy_price": "0.2",
            "buy_size": "0",
            "sold_size": "0",
            "position_before": "0",
            "entry_client_order_id": "",
            "entry_placement_attempted": False,
            "exit_client_order_ids": [],
            "exit_resolved_client_order_ids": [],
        }
        holding.update(holding_overrides)
        return {
            "config": {
                "threshold_cents": "10",
                "buy_limit_cents": "50",
                "position_pct": "100",
                "timeout_secs": "3",
                "settle_ms": 0,
                "window_ms": 150,
            },
            "targets": [target().to_dict()],
            "locked": True,
            "active_target_id": "a",
            "holding": holding,
        }


if __name__ == "__main__":
    unittest.main()
