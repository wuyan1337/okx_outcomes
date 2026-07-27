import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from backend import app as app_module
from backend.services.poly_monitor import PolyMonitor


def active_monitor(account_id="monitor-lifecycle"):
    monitor = PolyMonitor(object(), account_id)
    monitor._running = True
    monitor.state.enabled = True
    monitor.state.reference_price = 0.55
    return monitor


class PolyMonitorTriggerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_cancels_and_awaits_an_in_progress_trigger(self):
        monitor = active_monitor()
        cancel_started = asyncio.Event()
        never_finishes = asyncio.Event()

        async def blocked_cancel():
            cancel_started.set()
            await never_finishes.wait()
            return {"ok": True}

        monitor._fast_cancel_all = blocked_cancel
        monitor.on_trigger_callback = AsyncMock()
        auto = SimpleNamespace(
            enabled=True,
            execute=AsyncMock(),
            cancel=AsyncMock(),
        )
        monitor.auto_rebalance = auto

        await monitor._check_price(0.54, [], "test")
        trigger_task = monitor._trigger_task
        self.assertIsNotNone(trigger_task)
        await asyncio.wait_for(cancel_started.wait(), timeout=1)

        await monitor.stop()

        self.assertTrue(trigger_task.done())
        self.assertTrue(trigger_task.cancelled())
        self.assertIsNone(monitor._trigger_task)
        self.assertFalse(monitor.state.enabled)
        monitor.on_trigger_callback.assert_not_awaited()
        auto.execute.assert_not_awaited()
        auto.cancel.assert_awaited_once_with()

    async def test_stop_cancels_a_scheduled_trigger_before_it_starts(self):
        monitor = active_monitor("scheduled-stop")
        monitor._fast_cancel_all = AsyncMock(return_value={"ok": True})
        auto = SimpleNamespace(enabled=False, cancel=AsyncMock())
        monitor.auto_rebalance = auto

        trigger_task = monitor._schedule_trigger("risk")
        await monitor.stop()

        self.assertTrue(trigger_task.cancelled())
        monitor._fast_cancel_all.assert_not_awaited()
        auto.cancel.assert_awaited_once_with()

    async def test_genuine_trigger_runs_once_and_launches_rebalance(self):
        monitor = active_monitor("genuine-trigger")
        monitor.state.poly_token_id = "poly-token"
        monitor._strategy_type = "manual-s"
        monitor._old_price_cents = 55
        monitor._fast_cancel_all = AsyncMock(return_value={"ok": True})
        monitor.on_trigger_callback = AsyncMock()
        auto = SimpleNamespace(
            enabled=True,
            execute=AsyncMock(),
            cancel=AsyncMock(),
        )
        monitor.auto_rebalance = auto

        first = monitor._schedule_trigger("first risk")
        second = monitor._schedule_trigger("duplicate risk")
        self.assertIs(first, second)
        await first

        monitor._fast_cancel_all.assert_awaited_once_with()
        monitor.on_trigger_callback.assert_awaited_once_with()
        auto.execute.assert_awaited_once_with(
            trigger_reason="first risk",
            poly_token_id="poly-token",
            old_price_cents=55,
            strategy_type="manual-s",
        )
        auto.cancel.assert_not_awaited()

    async def test_internal_monitor_cleanup_does_not_cancel_rebalance(self):
        monitor = active_monitor("internal-cleanup")
        auto = SimpleNamespace(cancel=AsyncMock())
        monitor.auto_rebalance = auto

        await monitor.stop(cancel_rebalance=False)

        auto.cancel.assert_not_awaited()


class StopEndpointOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_stop_cancels_rebalance_before_strategy_state_check(self):
        events = []

        class Strategy:
            running = False

            async def stop(self):
                events.append("strategy-stop")
                self.running = False

        strategy = Strategy()

        class Rebalance:
            async def cancel(self):
                events.append("rebalance-cancel")
                # Simulate a rebalance that had reached its restart boundary.
                strategy.running = True

        class Monitor:
            running = True
            state = SimpleNamespace(enabled=True)

            async def stop(self):
                events.append("monitor-stop")

        instance = SimpleNamespace(
            strategy_lifecycle_lock=asyncio.Lock(),
            strategy_s=strategy,
            auto_rebalance=Rebalance(),
            poly_monitor=Monitor(),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)

        with patch.object(app_module, "manager", manager):
            result = await app_module.account_manual_s_stop(
                "account-a", user="test"
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            events,
            ["rebalance-cancel", "strategy-stop", "monitor-stop"],
        )

    async def test_disabling_rebalance_cancels_active_work_after_persisting(self):
        events = []

        class Rebalance:
            enabled = True

            def set_enabled(self, enabled):
                events.append(("persist", enabled))
                self.enabled = enabled
                return True

            async def cancel(self):
                events.append(("cancel", self.enabled))

        instance = SimpleNamespace(
            strategy_lifecycle_lock=asyncio.Lock(),
            auto_rebalance=Rebalance(),
        )
        manager = SimpleNamespace(get_instance=lambda _account_id: instance)
        request = SimpleNamespace(json=AsyncMock(return_value={"enabled": False}))

        with patch.object(app_module, "manager", manager):
            result = await app_module.account_rebalance_toggle(
                "account-a", request, user="test"
            )

        self.assertTrue(result["ok"])
        self.assertFalse(result["enabled"])
        self.assertEqual(events, [("persist", False), ("cancel", False)])


if __name__ == "__main__":
    unittest.main()
