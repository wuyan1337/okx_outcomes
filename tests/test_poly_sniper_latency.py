import asyncio
import json
import unittest
from decimal import Decimal
from unittest import mock

from backend.services import poly_sniper as poly_sniper_module
from backend.services.poly_sniper import (
    PolySniper,
    SniperState,
    SniperTarget,
    TargetRuntime,
)


def latency_target():
    return SniperTarget(
        id="a",
        name="Argentina",
        poly_token_id="poly-a",
        market_id="market-a",
        yes_asset_id="yes-a",
        no_asset_id="no-a",
        side="yes",
    )


def arm(sniper):
    selected = latency_target()
    sniper.targets = [selected]
    sniper._runtime = {selected.id: TargetRuntime()}
    sniper._token_targets = {selected.poly_token_id: [selected.id]}
    sniper.state = SniperState(status="monitoring")
    sniper._running = True
    return selected


def unlocked_saved_state(latency=None):
    result = {
        "config": {
            "threshold_cents": "10",
            "buy_limit_cents": "50",
            "position_pct": "100",
            "timeout_secs": "3",
            "settle_ms": 0,
            "window_ms": 150,
        },
        "targets": [latency_target().to_dict()],
        "locked": False,
        "active_target_id": "",
        "holding": {},
    }
    if latency is not None:
        result["latency"] = latency
    return result


class EntryClient:
    def __init__(self, response=None, *, position_ok=True, balance="10"):
        self.base_url = "https://us.okx.com"
        self._proxy_node = "JP2"
        self.response = response if response is not None else {
            "code": "0",
            "_clientOrderId": "entry-a",
        }
        self.position_ok = position_ok
        self.balance = balance
        self.cancelled = []

    def generate_client_order_id(self):
        return "entry-a"

    async def get_positions(self, status="open", market_id=None):
        if not self.position_ok:
            return {"code": "ERROR", "msg": "positions unavailable"}
        return {"code": "0", "data": {"list": []}}

    async def get_balance(self):
        return {
            "code": "0",
            "data": [{"oddsType": "points", "available": self.balance}],
        }

    async def place_order(self, **_kwargs):
        return dict(self.response)

    async def cancel_order(self, **kwargs):
        self.cancelled.append(kwargs)
        return {"code": "0"}


class RaisingEntryClient(EntryClient):
    async def place_order(self, **_kwargs):
        raise RuntimeError("order transport failed")


class NoOrderIdClient(EntryClient):
    def generate_client_order_id(self):
        return ""


class BlockingEntryClient(EntryClient):
    def __init__(self):
        super().__init__()
        self.place_started = asyncio.Event()
        self.release_place = asyncio.Event()

    async def place_order(self, **_kwargs):
        self.place_started.set()
        await self.release_place.wait()
        return dict(self.response)


class InstrumentedSniper(PolySniper):
    def __init__(self, client, asks=None, final_size="2", final=True):
        super().__init__(client)
        self.asks = asks if asks is not None else [
            (Decimal("0.20"), Decimal("10")),
        ]
        self.final_size = Decimal(final_size)
        self.final = final

    async def _snapshot_okx_asks(self, _target):
        return list(self.asks)

    async def _wait_order_final(self, _client_oid, _asset_id, _side, attempts=10):
        return self.final_size, self.final_size * Decimal("0.20"), self.final

    async def _holding_loop(self):
        return


class BlockingBookSniper(InstrumentedSniper):
    def __init__(self):
        super().__init__(EntryClient())
        self.book_started = asyncio.Event()
        self.release_book = asyncio.Event()

    async def _snapshot_okx_asks(self, _target):
        self.book_started.set()
        await self.release_book.wait()
        return list(self.asks)


class SettleSniper(InstrumentedSniper):
    def __init__(self, settled_bid, *, stop_during_settle=False):
        super().__init__(EntryClient())
        self.settled_bid = Decimal(settled_bid)
        self.stop_during_settle = stop_during_settle

    async def _wait_for_settle(self, _target):
        if self.stop_during_settle:
            self._running = False
        return self.settled_bid


class PolySniperLatencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_entry_records_stages_and_trigger_to_ack(self):
        sniper = InstrumentedSniper(EntryClient())
        arm(sniper)

        with mock.patch.object(
            poly_sniper_module.asyncio,
            "sleep",
            new=mock.AsyncMock(),
        ):
            await sniper._try_trigger(
                "a",
                Decimal("0.10"),
                Decimal("0.30"),
                signal_received_ns=poly_sniper_module.time.perf_counter_ns(),
            )

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "order_ack")
        self.assertEqual(trace["region"], "AU")
        self.assertEqual(trace["proxy_node"], "JP2")
        self.assertEqual(trace["proxy_name"], "JP-TYO-02")
        self.assertEqual(trace["response_code"], "0")
        self.assertTrue(trace["order_attempted"])
        self.assertGreaterEqual(trace["trigger_to_order_ack_ms"], 0)
        self.assertEqual(trace["trigger_to_order_ack_ms"], trace["trigger_to_finish_ms"])
        self.assertEqual(
            set(trace["stages_ms"]),
            {
                "signal_to_lock",
                "trigger_persist",
                "okx_book",
                "positions",
                "balance",
                "notify_queue",
                "pre_order_persist",
                "order_submit",
            },
        )
        self.assertIsNone(sniper._latency_active)
        json.dumps(sniper.get_state())

    async def test_empty_book_releases_trace_before_any_order(self):
        sniper = InstrumentedSniper(EntryClient(), asks=[])
        arm(sniper)

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "released_before_order")
        self.assertEqual(trace["reason"], "OKX has no usable ask depth")
        self.assertNotIn("trigger_to_order_ack_ms", trace)
        self.assertNotIn("order_attempted", trace)
        self.assertIn("okx_book", trace["stages_ms"])
        self.assertFalse(sniper.state.locked)
        self.assertEqual(sniper.state.active_target_id, "")
        self.assertIsNone(sniper._latency_active)

    async def test_settle_release_records_stage_and_no_ack(self):
        sniper = SettleSniper("0.10")
        arm(sniper)
        sniper.config.settle_ms = 1
        sniper.config.threshold_cents = Decimal("10")

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "released_before_order")
        self.assertIn("no longer meets threshold", trace["reason"])
        self.assertIn("settle", trace["stages_ms"])
        self.assertNotIn("okx_book", trace["stages_ms"])
        self.assertNotIn("trigger_to_order_ack_ms", trace)
        self.assertIsNone(sniper._latency_active)

    async def test_stopped_during_settle_finishes_trace(self):
        sniper = SettleSniper("0.30", stop_during_settle=True)
        arm(sniper)
        sniper.config.settle_ms = 1

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "stopped_during_settle")
        self.assertIn("settle", trace["stages_ms"])
        self.assertNotIn("trigger_to_order_ack_ms", trace)
        self.assertIsNone(sniper._latency_active)

    async def test_position_failure_is_a_released_pre_order_trace(self):
        sniper = InstrumentedSniper(EntryClient(position_ok=False))
        arm(sniper)

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "released_before_order")
        self.assertEqual(trace["reason"], "Entry did not fill")
        self.assertIn("positions", trace["stages_ms"])
        self.assertNotIn("balance", trace["stages_ms"])
        self.assertNotIn("order_attempted", trace)
        self.assertIsNone(sniper._latency_active)

    async def test_other_pre_order_early_returns_always_finish_the_trace(self):
        cases = [
            (
                "no eligible quote",
                InstrumentedSniper(
                    EntryClient(), asks=[(Decimal("0.40"), Decimal("10"))]
                ),
                None,
                "No OKX asks below both",
            ),
            (
                "depth below one share",
                InstrumentedSniper(
                    EntryClient(), asks=[(Decimal("0.20"), Decimal("0.5"))]
                ),
                None,
                "Entry did not fill",
            ),
            (
                "zero balance",
                InstrumentedSniper(EntryClient(balance="0")),
                None,
                "Entry did not fill",
            ),
            (
                "missing client order id",
                InstrumentedSniper(NoOrderIdClient()),
                None,
                "Entry did not fill",
            ),
            (
                "pre-order persistence failure",
                InstrumentedSniper(EntryClient()),
                lambda _snapshot: False,
                "Entry did not fill",
            ),
        ]
        for name, sniper, persistence, reason in cases:
            with self.subTest(name=name):
                arm(sniper)
                sniper.on_state_change = persistence

                await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

                trace = sniper.get_state()["latency"]["last"]
                self.assertEqual(trace["outcome"], "released_before_order")
                self.assertIn(reason, trace["reason"])
                self.assertNotIn("trigger_to_order_ack_ms", trace)
                self.assertNotIn("order_attempted", trace)
                self.assertFalse(sniper.state.locked)
                self.assertIsNone(sniper._latency_active)

    async def test_transport_exception_is_error_without_a_fake_ack(self):
        sniper = InstrumentedSniper(RaisingEntryClient())
        arm(sniper)

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        latency = sniper.get_state()["latency"]
        trace = latency["last"]
        self.assertEqual(trace["outcome"], "entry_exception")
        self.assertEqual(trace["reason"], "RuntimeError")
        self.assertTrue(trace["order_attempted"])
        self.assertIn("order_submit", trace["stages_ms"])
        self.assertNotIn("trigger_to_order_ack_ms", trace)
        self.assertEqual(latency["rolling"]["order_attempts"], 1)
        self.assertEqual(latency["rolling"]["order_ack_samples"], 0)
        self.assertEqual(latency["rolling"]["unacknowledged_order_attempts"], 1)
        self.assertEqual(latency["rolling"]["pre_order_exits"], 0)
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper.state.locked)
        self.assertIsNone(sniper._latency_active)

    async def test_timeout_result_is_not_reported_as_order_ack(self):
        client = EntryClient(response={"code": "TIMEOUT", "_clientOrderId": "entry-a"})
        sniper = InstrumentedSniper(client)
        arm(sniper)

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        latency = sniper.get_state()["latency"]
        trace = latency["last"]
        self.assertEqual(trace["outcome"], "order_unacknowledged")
        self.assertEqual(trace["response_code"], "TIMEOUT")
        self.assertTrue(trace["order_attempted"])
        self.assertNotIn("trigger_to_order_ack_ms", trace)
        self.assertEqual(latency["rolling"]["order_ack_samples"], 0)
        self.assertEqual(latency["rolling"]["unacknowledged_order_attempts"], 1)

    async def test_upstream_rejection_is_an_acknowledged_failed_response(self):
        client = EntryClient(response={"code": "51000", "msg": "rejected"})
        sniper = InstrumentedSniper(client)
        arm(sniper)

        await sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))

        latency = sniper.get_state()["latency"]
        trace = latency["last"]
        self.assertEqual(trace["outcome"], "order_response_error")
        self.assertEqual(trace["response_code"], "51000")
        self.assertIn("trigger_to_order_ack_ms", trace)
        self.assertEqual(latency["rolling"]["failed_order_responses"], 1)
        self.assertEqual(latency["rolling"]["unacknowledged_order_attempts"], 0)

    async def test_cancel_before_order_releases_trace_and_records_book_stage(self):
        sniper = BlockingBookSniper()
        arm(sniper)
        trigger = asyncio.create_task(
            sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))
        )
        await sniper.book_started.wait()

        trigger.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await trigger

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "released_before_order")
        self.assertIn("cancelled", trace["reason"].lower())
        self.assertIn("okx_book", trace["stages_ms"])
        self.assertNotIn("order_attempted", trace)
        self.assertFalse(sniper.state.locked)
        self.assertIsNone(sniper._latency_active)

    async def test_stop_cancels_an_active_pre_order_trace(self):
        sniper = BlockingBookSniper()
        arm(sniper)
        trigger = asyncio.create_task(
            sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))
        )
        sniper._trigger_tasks.add(trigger)
        trigger.add_done_callback(sniper._trigger_tasks.discard)
        await sniper.book_started.wait()

        result = await sniper.stop()

        self.assertTrue(result["ok"])
        self.assertTrue(result["stopped"])
        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "released_before_order")
        self.assertIn("cancelled", trace["reason"].lower())
        self.assertIn("okx_book", trace["stages_ms"])
        self.assertFalse(sniper.state.locked)
        self.assertEqual(sniper.state.status, "stopped")
        self.assertIsNone(sniper._latency_active)

    async def test_cancel_during_order_keeps_lock_and_finishes_trace(self):
        client = BlockingEntryClient()
        sniper = InstrumentedSniper(client)
        arm(sniper)
        trigger = asyncio.create_task(
            sniper._try_trigger("a", Decimal("0.10"), Decimal("0.30"))
        )
        await client.place_started.wait()

        trigger.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await trigger

        trace = sniper.get_state()["latency"]["last"]
        self.assertEqual(trace["outcome"], "trigger_error")
        self.assertTrue(trace["order_attempted"])
        self.assertIn("order_submit", trace["stages_ms"])
        self.assertNotIn("trigger_to_order_ack_ms", trace)
        self.assertEqual(sniper.state.status, "error")
        self.assertTrue(sniper.state.locked)
        self.assertIsNone(sniper._latency_active)

    def test_rolling_stats_separate_pre_order_unacknowledged_and_ack(self):
        sniper = PolySniper(object())
        sniper._latency_history = [
            {
                "outcome": "order_ack",
                "order_attempted": True,
                "trigger_to_order_ack_ms": 100,
                "response_code": "0",
                "stages_ms": {},
            },
            {
                "outcome": "order_response_error",
                "order_attempted": True,
                "trigger_to_order_ack_ms": 300,
                "response_code": "51000",
                "stages_ms": {},
            },
            {
                "outcome": "order_unacknowledged",
                "order_attempted": True,
                "response_code": "TIMEOUT",
                "stages_ms": {"order_submit": 500},
            },
            {
                "outcome": "released_before_order",
                "stages_ms": {"okx_book": 20},
            },
        ]

        rolling = sniper._latency_snapshot()["rolling"]

        self.assertEqual(rolling["window"], 4)
        self.assertEqual(rolling["order_attempts"], 3)
        self.assertEqual(rolling["order_ack_samples"], 2)
        self.assertEqual(rolling["successful_order_responses"], 1)
        self.assertEqual(rolling["failed_order_responses"], 1)
        self.assertEqual(rolling["unacknowledged_order_attempts"], 1)
        self.assertEqual(rolling["pre_order_exits"], 1)
        self.assertEqual(rolling["ack_rate"], 0.667)
        self.assertEqual(rolling["attempt_success_rate"], 0.333)
        self.assertEqual(rolling["success_rate"], 0.5)
        self.assertEqual(rolling["median_ms"], 200.0)
        self.assertEqual(rolling["min_ms"], 100.0)
        self.assertEqual(rolling["max_ms"], 300.0)
        self.assertEqual(rolling["jitter_ms"], 200.0)

    def test_rolling_stats_use_only_the_latest_twenty_traces(self):
        sniper = PolySniper(object())
        sniper._latency_history = [
            {
                "target_id": str(index),
                "outcome": "order_ack",
                "order_attempted": True,
                "response_code": "0",
                "trigger_to_order_ack_ms": index,
                "stages_ms": {},
            }
            for index in range(1, 26)
        ]

        latency = sniper._latency_snapshot()

        self.assertEqual(latency["rolling"]["window"], 20)
        self.assertEqual(latency["rolling"]["order_ack_samples"], 20)
        self.assertEqual(latency["rolling"]["min_ms"], 6.0)
        self.assertEqual(latency["rolling"]["max_ms"], 25.0)
        self.assertEqual(latency["rolling"]["median_ms"], 15.5)
        self.assertEqual(latency["recent"][0]["target_id"], "6")
        self.assertEqual(latency["last"]["target_id"], "25")

    def test_rolling_stats_do_not_mix_regions_or_proxy_routes(self):
        sniper = PolySniper(object())
        sniper._latency_history = [
            {
                "outcome": "order_ack",
                "order_attempted": True,
                "response_code": "0",
                "trigger_to_order_ack_ms": 40,
                "region": "AU",
                "proxy_node": "JP2",
                "stages_ms": {},
            },
            {
                "outcome": "order_ack",
                "order_attempted": True,
                "response_code": "0",
                "trigger_to_order_ack_ms": 90,
                "region": "CN",
                "proxy_node": "JP1",
                "stages_ms": {},
            },
        ]

        latency = sniper._latency_snapshot()

        self.assertEqual(len(latency["recent"]), 2)
        self.assertEqual(latency["rolling"]["window"], 1)
        self.assertEqual(latency["rolling"]["region"], "CN")
        self.assertEqual(latency["rolling"]["proxy_node"], "JP1")
        self.assertEqual(latency["rolling"]["median_ms"], 90.0)

    async def test_restore_keeps_final_traces_and_discards_process_local_state(self):
        saved = unlocked_saved_state({
            "recent": [
                {
                    "outcome": "order_response_error",
                    "response_code": "TIMEOUT",
                    "trigger_to_order_ack_ms": 999,
                    "stages_ms": {"order_submit": 500},
                },
                {
                    "_started_ns": 123,
                    "outcome": "order_ack",
                    "order_attempted": True,
                    "response_code": "0",
                    "trigger_to_finish_ms": -1,
                    "trigger_to_order_ack_ms": "50.04",
                    "stages_ms": {"okx_book": "12.34", "bad": "NaN"},
                },
                {"outcome": 123, "trigger_to_order_ack_ms": 1},
            ],
        })
        sniper = PolySniper(object())

        result = await sniper.restore(saved, resume=False)
        latency = sniper.get_state()["latency"]

        self.assertTrue(result["ok"])
        self.assertIsNone(sniper._latency_active)
        self.assertEqual(len(latency["recent"]), 2)
        repaired = latency["recent"][0]
        self.assertTrue(repaired["order_attempted"])
        self.assertNotIn("trigger_to_order_ack_ms", repaired)
        trace = latency["last"]
        self.assertNotIn("_started_ns", trace)
        self.assertNotIn("trigger_to_finish_ms", trace)
        self.assertEqual(trace["trigger_to_order_ack_ms"], 50.0)
        self.assertEqual(trace["stages_ms"], {"okx_book": 12.3})
        self.assertEqual(latency["rolling"]["order_ack_samples"], 1)
        self.assertEqual(latency["rolling"]["order_attempts"], 2)
        self.assertEqual(latency["rolling"]["unacknowledged_order_attempts"], 1)
        json.dumps(sniper.get_state())

    async def test_restore_ignores_non_list_latency_history(self):
        sniper = PolySniper(object())

        result = await sniper.restore(
            unlocked_saved_state({"recent": {"outcome": "bad-shape"}}),
            resume=False,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(sniper.get_state()["latency"]["status"], "no_samples")
        self.assertEqual(sniper._latency_history, [])


if __name__ == "__main__":
    unittest.main()
