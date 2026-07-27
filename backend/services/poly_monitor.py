"""Polymarket best-bid risk control for Manual-D and Manual-S."""
import asyncio
from decimal import Decimal, ROUND_HALF_UP
import logging
import secrets
import time
from typing import Optional
from dataclasses import dataclass

import aiohttp

from ..core.price_utils import as_decimal, okx_tick
from ..core.logging_config import audit_event, log_context, market_event

logger = logging.getLogger(__name__)

POLY_CLOB_BOOK = "https://clob.polymarket.com/book?token_id={token_id}"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class MonitorState:
    enabled: bool = False
    poly_token_id: str = ""
    reference_price: float = 0.0
    depth_threshold: float = 10000.0
    triggered: bool = False
    trigger_reason: str = ""
    last_poly_bid: float = 0.0
    last_poly_bid_size: float = 0.0
    last_poly_bid_value: float = 0.0
    last_check_ts: int = 0


class PolyMonitor:
    """Real-time Poly orderbook monitor for risk control."""

    def __init__(self, client, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.state = MonitorState()
        self._task: Optional[asyncio.Task] = None
        self._trigger_task: Optional[asyncio.Task] = None
        self._lifecycle_generation = 0
        self._run_id = ""
        self._trigger_trace_id = ""
        self.notifier = None
        self._running = False
        self.on_trigger_callback = None
        self.auto_rebalance = None
        self._strategy_type = ""
        self._strategy_mode = "classic"
        self._old_price_cents = Decimal("0")

    @property
    def running(self) -> bool:
        return self._running and not self.state.triggered

    async def start(self, poly_token_id: str, reference_price: float = 0.0, depth_threshold: float = 10000.0):
        """Start monitoring. If reference_price=0, fetch current Poly best bid as reference."""
        if self.auto_rebalance is not None and getattr(
            self.auto_rebalance, "recovery_required", False
        ):
            return {
                "ok": False,
                "error": "Account recovery is required before Monitor can start",
                "recovery": self.auto_rebalance.recovery_status(),
            }
        if self._running or (
            self._trigger_task is not None and not self._trigger_task.done()
        ):
            return {"ok": False, "error": "Monitor already running"}

        self._run_id = secrets.token_hex(8)
        self._trigger_trace_id = ""

        self.state.poly_token_id = poly_token_id
        self.state.depth_threshold = float(as_decimal(depth_threshold))
        self.state.triggered = False
        self.state.trigger_reason = ""

        # Get current Poly best bid as reference if not provided
        reference = as_decimal(reference_price)
        if reference <= 0:
            bid = await self._get_poly_best_bid(poly_token_id)
            if bid is None or bid <= 0:
                return {"ok": False, "error": "Cannot fetch Poly orderbook"}
            self.state.reference_price = bid
        else:
            self.state.reference_price = float(reference)

        self._lifecycle_generation += 1
        self._running = True
        self.state.enabled = True
        audit_event(
            "monitor.started",
            account_id=self.account_id,
            strategy="poly-monitor",
            run_id=self._run_id,
            poly_token_id=poly_token_id,
            reference_price=str(self.state.reference_price),
            depth_threshold=str(self.state.depth_threshold),
        )
        with log_context(
            account_id=self.account_id,
            strategy="poly-monitor",
            run_id=self._run_id,
        ):
            self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"[PolyMonitor] Started for {self.account_id}, ref_price={self.state.reference_price}")
        return {"ok": True, "reference_price": self.state.reference_price}

    def set_strategy_context(self, strategy_type: str, price_cents, asset_id: str = "", mode: str = "classic"):
        """Called by start flow to record what strategy is running."""
        self._strategy_type = strategy_type
        self._old_price_cents = as_decimal(price_cents)
        self._strategy_asset_id = asset_id
        self._strategy_mode = mode

    async def stop(self, *, cancel_rebalance: bool = True):
        """Stop and await every task owned by this monitor.

        Rebalance teardown calls this with ``cancel_rebalance=False`` to avoid
        recursively cancelling the rebalance task that is performing cleanup.
        User-facing stop paths keep the default and cancel rebalance first-class.
        """
        self._lifecycle_generation += 1
        self._running = False
        self.state.enabled = False

        current = asyncio.current_task()
        monitor_task = self._task
        trigger_task = self._trigger_task
        pending = []
        for task in (monitor_task, trigger_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
                pending.append(task)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        if self._task is monitor_task and monitor_task is not current:
            self._task = None
        if self._trigger_task is trigger_task and trigger_task is not current:
            self._trigger_task = None

        if cancel_rebalance and self.auto_rebalance is not None:
            await self.auto_rebalance.cancel()
        logger.info(f"[PolyMonitor] Stopped for {self.account_id}")
        audit_event(
            "monitor.stopped",
            account_id=self.account_id,
            strategy="poly-monitor",
            run_id=self._run_id,
            trigger_trace_id=self._trigger_trace_id,
            triggered=self.state.triggered,
            cancel_rebalance=cancel_rebalance,
        )

    def _schedule_trigger(self, reason: str) -> Optional[asyncio.Task]:
        """Schedule one owned trigger for the current monitor generation."""
        if not self._running or not self.state.enabled or self.state.triggered:
            return None
        if self._trigger_task is not None and not self._trigger_task.done():
            return self._trigger_task

        generation = self._lifecycle_generation
        self._trigger_trace_id = secrets.token_hex(8)
        audit_event(
            "monitor.trigger_scheduled",
            account_id=self.account_id,
            strategy="poly-monitor",
            run_id=self._run_id,
            trace_id=self._trigger_trace_id,
            reason=reason,
            lifecycle_generation=generation,
        )
        task = asyncio.create_task(self._trigger(reason, generation=generation))
        self._trigger_task = task

        def _trigger_done(done: asyncio.Task) -> None:
            if self._trigger_task is done:
                self._trigger_task = None
            if done.cancelled():
                return
            try:
                error = done.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                logger.error(
                    "[PolyMonitor] Trigger task failed for %s: %s",
                    self.account_id,
                    error,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(_trigger_done)
        return task

    def _trigger_generation_active(self, generation: Optional[int]) -> bool:
        return generation is None or (
            generation == self._lifecycle_generation and self.state.enabled
        )

    async def _stop_strategy_after_journal_failure(self, reason: str) -> None:
        """Freeze Manual activity even when journaling prevents account cancel-all."""
        if not self.on_trigger_callback:
            logger.critical(
                "[PolyMonitor] No strategy-stop callback is available after journal failure"
            )
            return
        try:
            stopped = await asyncio.wait_for(self.on_trigger_callback(), timeout=5.0)
            if stopped is not True:
                raise RuntimeError("strategy stop was not safely confirmed")
            audit_event(
                "monitor.strategy_stopped_after_persistence_failure",
                level=logging.WARNING,
                account_id=self.account_id,
                run_id=self._run_id,
                trace_id=self._trigger_trace_id,
                reason=reason,
            )
        except Exception as exc:
            logger.critical(
                "[PolyMonitor] Failed to stop strategy after journal failure: %s",
                exc,
            )

    async def _get_poly_best_bid(self, token_id: str) -> Optional[float]:
        """Fetch current Poly best bid price."""
        bid, _ = await self._get_poly_best_bid_with_depth(token_id)
        return bid

    @staticmethod
    def _best_bid_from_payload(data: dict) -> tuple[float, list]:
        """Parse a successful CLOB book, preserving an explicitly empty bid side."""
        if not isinstance(data, dict) or "bids" not in data:
            raise ValueError("Poly book response is missing bids")
        bids = data["bids"]
        if not isinstance(bids, list):
            raise ValueError("Poly book bids must be a list")
        if not bids:
            return 0.0, []
        bids = sorted(
            bids,
            key=lambda row: as_decimal(row.get("price", "0")),
            reverse=True,
        )
        return float(as_decimal(bids[0].get("price", "0"))), bids

    async def _observe_book_payload(
        self,
        data: dict,
        source: str,
        *,
        received_wall_ns: Optional[int] = None,
        received_monotonic_ns: Optional[int] = None,
    ) -> bool:
        """Apply one valid full-book observation to monitor state and risk checks."""
        try:
            best_bid, bids = self._best_bid_from_payload(data)
        except (AttributeError, TypeError, ValueError):
            return False
        self.state.last_poly_bid = best_bid
        self.state.last_check_ts = int(time.time() * 1000)
        market_event(
            "polymarket",
            "monitor.book",
            component="poly-monitor",
            account_id=self.account_id,
            run_id=self._run_id,
            poly_token_id=self.state.poly_token_id,
            feed_source=source,
            source_timestamp=data.get("timestamp", data.get("ts", "")),
            received_wall_ns=received_wall_ns or time.time_ns(),
            received_monotonic_ns=(
                received_monotonic_ns or time.monotonic_ns()
            ),
            best_bid=str(best_bid),
            bids=bids,
            asks=data.get("asks") or [],
        )
        await self._check_price(best_bid, bids, source)
        return True

    async def _get_poly_best_bid_with_depth(self, token_id: str):
        """Fetch current Poly best bid price and full bids list (sorted high->low)."""
        started_ns = time.perf_counter_ns()
        try:
            async with aiohttp.ClientSession() as sess:
                url = POLY_CLOB_BOOK.format(token_id=token_id)
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        received_wall_ns = time.time_ns()
                        received_monotonic_ns = time.monotonic_ns()
                        best_bid, bids = self._best_bid_from_payload(data)
                        market_event(
                            "polymarket",
                            "monitor.rest_book",
                            component="poly-monitor",
                            account_id=self.account_id,
                            run_id=self._run_id,
                            poly_token_id=token_id,
                            source_timestamp=data.get("timestamp", data.get("ts", "")),
                            received_wall_ns=received_wall_ns,
                            received_monotonic_ns=received_monotonic_ns,
                            http_status=resp.status,
                            elapsed_ms=round((time.perf_counter_ns() - started_ns) / 1_000_000, 3),
                            best_bid=str(best_bid),
                            bids=bids,
                            asks=data.get("asks") or [],
                        )
                        return best_bid, bids
                    market_event(
                        "polymarket",
                        "monitor.rest_error",
                        level=logging.WARNING,
                        component="poly-monitor",
                        account_id=self.account_id,
                        run_id=self._run_id,
                        poly_token_id=token_id,
                        http_status=resp.status,
                        elapsed_ms=round((time.perf_counter_ns() - started_ns) / 1_000_000, 3),
                    )
        except Exception as e:
            logger.warning(f"[PolyMonitor] Failed to get Poly bid: {e}")
            audit_event(
                "monitor.poly_rest_failed",
                level=logging.WARNING,
                account_id=self.account_id,
                run_id=self._run_id,
                poly_token_id=token_id,
                elapsed_ms=round((time.perf_counter_ns() - started_ns) / 1_000_000, 3),
                error=e,
            )
        return None, []

    async def _monitor_loop(self):
        """Main monitoring loop: REST poll every 1.5s + WS for instant updates."""
        token_id = self.state.poly_token_id

        # Start both: REST poller (reliable) + WS listener (fast)
        rest_task = asyncio.create_task(self._rest_poll_loop(token_id))
        ws_task = asyncio.create_task(self._ws_listen_loop(token_id))

        try:
            done, pending = await asyncio.wait(
                [rest_task, ws_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        except asyncio.CancelledError:
            rest_task.cancel()
            ws_task.cancel()
            await asyncio.gather(rest_task, ws_task, return_exceptions=True)

    async def _rest_poll_loop(self, token_id: str):
        """Poll Poly CLOB REST every 1.5s."""
        while self._running and not self.state.triggered:
            try:
                bid, bids = await self._get_poly_best_bid_with_depth(token_id)
                if bid is not None:
                    self.state.last_poly_bid = bid
                    self.state.last_check_ts = int(time.time() * 1000)
                    await self._check_price(bid, bids, "poly_rest")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"[PolyMonitor] REST poll error: {e}")
            await asyncio.sleep(1.5)

    async def _ws_listen_loop(self, token_id: str):
        """Listen to Poly WS for instant book updates."""
        import json
        retry_delay = 1
        while self._running and not self.state.triggered:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(POLY_WS_URL, heartbeat=15) as ws:
                        await ws.send_str(json.dumps({"type": "market", "assets_ids": [token_id]}))
                        logger.info(f"[PolyMonitor] WS subscribed: {token_id[:30]}...")
                        retry_delay = 1

                        async for msg in ws:
                            received_wall_ns = time.time_ns()
                            received_monotonic_ns = time.monotonic_ns()
                            if not self._running or self.state.triggered:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    raw = json.loads(msg.data)
                                    items = raw if isinstance(raw, list) else [raw]
                                    for data in items:
                                        if data.get("event_type") == "book":
                                            await self._observe_book_payload(
                                                data,
                                                "poly_ws",
                                                received_wall_ns=received_wall_ns,
                                                received_monotonic_ns=received_monotonic_ns,
                                            )
                                        elif data.get("event_type") == "price_change":
                                            changes = data.get("price_changes", [])
                                            market_event(
                                                "polymarket",
                                                "monitor.price_change",
                                                component="poly-monitor",
                                                account_id=self.account_id,
                                                run_id=self._run_id,
                                                poly_token_id=token_id,
                                                source_timestamp=data.get("timestamp", data.get("ts", "")),
                                                received_wall_ns=received_wall_ns,
                                                received_monotonic_ns=received_monotonic_ns,
                                                changes=changes,
                                            )
                                            if any(
                                                str(change.get("asset_id", change.get("assetId", ""))) in ("", str(token_id))
                                                for change in changes
                                            ):
                                                best_bid, bids = await self._get_poly_best_bid_with_depth(token_id)
                                                if best_bid is not None:
                                                    self.state.last_poly_bid = best_bid
                                                    self.state.last_check_ts = int(time.time() * 1000)
                                                    await self._check_price(best_bid, bids, "poly_ws_change")
                                except Exception:
                                    logger.debug("[PolyMonitor] Invalid WS payload", exc_info=True)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"[PolyMonitor] WS error: {e}")
            if self._running and not self.state.triggered:
                await asyncio.sleep(min(retry_delay, 5))
                retry_delay = min(retry_delay * 2, 5)

    @staticmethod
    def _risk_bid_price(price) -> Decimal:
        """Round a Poly bid to the nearest executable OKX tick, halves upward."""
        value = as_decimal(price)
        tick = okx_tick(value)
        return (value / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick

    async def _check_price(self, current_bid: float, bids: list, source: str):
        """Check if price deviated from reference.

        Two trigger conditions:
        A) Price DROPPED (current_bid < ref): reference price disappeared from bid side.
           Trigger immediately - no depth check needed.
        B) Price ROSE (current_bid > ref): someone is buying aggressively.
           Only trigger if depth at new best bid >= threshold (confirms real move).
           For bid-sell mode: ROSE also triggers (need to rebalance up to capture higher spread).
        """
        raw_current_bid = as_decimal(current_bid)
        current_bid = self._risk_bid_price(current_bid)
        ref = self._risk_bid_price(self.state.reference_price)
        # Calculate depth at best bid level (used for logging + condition B)
        best_bid_size = Decimal("0")
        if bids:
            best_price = as_decimal(bids[0].get("price", "0"))
            for b in bids:
                if as_decimal(b.get("price", "0")) == best_price:
                    best_bid_size += as_decimal(b.get("size", "0"))
                else:
                    break
        best_bid_value = current_bid * best_bid_size
        self.state.last_poly_bid_size = float(best_bid_size)
        self.state.last_poly_bid_value = float(best_bid_value)

        relation = "invalid_reference" if ref <= 0 else (
            "equal" if current_bid == ref else "dropped" if current_bid < ref else "rose"
        )
        market_event(
            "polymarket",
            "monitor.risk_observation",
            component="poly-monitor",
            account_id=self.account_id,
            run_id=self._run_id,
            trace_id=self._trigger_trace_id,
            poly_token_id=self.state.poly_token_id,
            feed_source=source,
            raw_bid=str(raw_current_bid),
            risk_bid=str(current_bid),
            raw_reference=str(self.state.reference_price),
            risk_reference=str(ref),
            best_bid_size=str(best_bid_size),
            best_bid_value=str(best_bid_value),
            depth_threshold=str(self.state.depth_threshold),
            relation=relation,
            bids=bids,
        )
        if ref <= 0 or current_bid == ref:
            return

        # CONDITION A: Price dropped (bid < reference)
        # Reference price disappeared from bids → immediate trigger
        if current_bid < ref:
            reason = (
                f"Poly bid DROPPED: {ref:.3f} -> {current_bid:.3f} "
                f"(reference gone from bid side) [{source}]"
            )
            self._schedule_trigger(reason)
            return

        # CONDITION B: Price rose (bid > reference)
        # Both classic and bid-sell trigger on ROSE (bid-sell needs to rebalance up)
        # Only trigger if depth at new best bid >= threshold
        if current_bid > ref:
            threshold = as_decimal(self.state.depth_threshold)
            if best_bid_value < threshold:
                logger.debug(
                    f"[PolyMonitor] Bid rose {ref:.2f}->{current_bid:.2f} "
                    f"but value {best_bid_value:.0f} xp < {threshold:.0f} xp, ignoring"
                )
                audit_event(
                    "monitor.signal_ignored_depth",
                    account_id=self.account_id,
                    run_id=self._run_id,
                    poly_token_id=self.state.poly_token_id,
                    risk_bid=str(current_bid),
                    bid_value=str(best_bid_value),
                    threshold=str(threshold),
                )
                return
            reason = (
                f"Poly bid ROSE: {ref:.3f} -> {current_bid:.3f} "
                f"({best_bid_value:,.0f} xp at new bid >= {threshold:,.0f} xp threshold) [{source}]"
            )
            self._schedule_trigger(reason)

    async def _trigger(self, reason: str, generation: Optional[int] = None):
        """Price deviated: freeze entries, verify stop, then cancel and notify."""
        if self.state.triggered or not self._trigger_generation_active(generation):
            return

        self.state.triggered = True
        self.state.trigger_reason = reason
        self._running = False
        logger.warning(f"[PolyMonitor] TRIGGERED: {reason}")
        audit_event(
            "monitor.triggered",
            level=logging.WARNING,
            account_id=self.account_id,
            strategy="poly-monitor",
            run_id=self._run_id,
            trace_id=self._trigger_trace_id,
            reason=reason,
            lifecycle_generation=generation,
        )

        recovery_id = self._trigger_trace_id or secrets.token_hex(8)
        self._trigger_trace_id = recovery_id
        recovery_context = {
            "source": "poly_monitor",
            "run_id": self._run_id,
            "trace_id": recovery_id,
            "reason": reason,
            "poly_token_id": self.state.poly_token_id,
            "reference_price": self.state.reference_price,
            "old_price_cents": self._old_price_cents,
            "strategy_type": self._strategy_type,
            "strategy_mode": self._strategy_mode,
            "strategy_asset_id": getattr(self, "_strategy_asset_id", ""),
            "lifecycle_generation": generation,
        }
        prepare_recovery = getattr(self.auto_rebalance, "prepare_recovery", None)
        get_params = getattr(self.auto_rebalance, "get_strategy_params", None)
        try:
            strategy_params = get_params() if callable(get_params) else {}
        except Exception as exc:
            logger.critical(
                "[PolyMonitor] Refusing cancel-all because strategy context is unavailable: %s",
                exc,
            )
            await self._stop_strategy_after_journal_failure(reason)
            return
        if not isinstance(strategy_params, dict) or not strategy_params:
            logger.critical(
                "[PolyMonitor] Refusing cancel-all because strategy parameters are unavailable"
            )
            await self._stop_strategy_after_journal_failure(reason)
            return
        if not callable(prepare_recovery) or not prepare_recovery(
            rebalance_id=recovery_id,
            stage="monitor_strategy_stop",
            phase="before",
            context=recovery_context,
            strategy_params=strategy_params,
        ):
            logger.critical(
                "[PolyMonitor] Refusing cancel-all because the strategy-stop journal could not be persisted"
            )
            audit_event(
                "monitor.strategy_stop_blocked_persistence",
                level=logging.CRITICAL,
                account_id=self.account_id,
                run_id=self._run_id,
                trace_id=recovery_id,
                reason=reason,
            )
            await self._stop_strategy_after_journal_failure(reason)
            return

        # Step 1: stop both Manual strategies and require an explicit safe result.
        strategies_stopped = False
        if self.on_trigger_callback:
            try:
                stop_result = await asyncio.wait_for(
                    self.on_trigger_callback(), timeout=5.0
                )
                strategies_stopped = stop_result is True
                if strategies_stopped:
                    logger.info("[PolyMonitor] Strategies stopped and verified")
                else:
                    logger.error(
                        "[PolyMonitor] Strategy stop returned an unsafe result; cancel-all and auto-rebalance blocked"
                    )
            except asyncio.TimeoutError:
                logger.error(
                    "[PolyMonitor] Strategy stop timed out (5s); cancel-all and auto-rebalance blocked"
                )
            except Exception as e:
                logger.error(
                    "[PolyMonitor] Strategy stop failed; cancel-all and auto-rebalance blocked: %s",
                    e,
                )
        else:
            logger.error(
                "[PolyMonitor] Strategy-stop callback unavailable; cancel-all and auto-rebalance blocked"
            )

        if not prepare_recovery(
            rebalance_id=recovery_id,
            stage="monitor_strategy_stop",
            phase="after" if strategies_stopped else "failed",
            context={
                **recovery_context,
                "strategies_stopped": strategies_stopped,
            },
            strategy_params=strategy_params,
        ):
            logger.critical(
                "[PolyMonitor] Strategy-stop result could not be persisted; cancel-all blocked"
            )
            return
        if not strategies_stopped:
            return

        if not self._trigger_generation_active(generation):
            logger.info("[PolyMonitor] Trigger stopped before cancel-all")
            return

        if not prepare_recovery(
            rebalance_id=recovery_id,
            stage="monitor_cancel_all",
            phase="before",
            context={**recovery_context, "strategies_stopped": True},
            strategy_params=strategy_params,
        ):
            logger.critical(
                "[PolyMonitor] Refusing cancel-all because its recovery journal could not be persisted"
            )
            audit_event(
                "monitor.cancel_all_blocked_persistence",
                level=logging.CRITICAL,
                account_id=self.account_id,
                run_id=self._run_id,
                trace_id=recovery_id,
                reason=reason,
            )
            return

        # Step 2: with entry loops stopped, cancel and verify the account order set.
        cancelled = await self._fast_cancel_all()
        logger.info(f"[PolyMonitor] Final cancel result: {cancelled}")
        cancel_confirmed = bool(cancelled.get("ok"))
        audit_event(
            "monitor.cancel_all_verified",
            level=logging.INFO if cancel_confirmed else logging.ERROR,
            account_id=self.account_id,
            run_id=self._run_id,
            trace_id=self._trigger_trace_id,
            result=cancelled,
        )
        if not prepare_recovery(
            rebalance_id=recovery_id,
            stage="monitor_cancel_all",
            phase="after" if cancel_confirmed else "failed",
            context={
                **recovery_context,
                "strategies_stopped": True,
                "cancel_result": cancelled,
            },
            strategy_params=strategy_params,
        ):
            logger.critical(
                "[PolyMonitor] Cancellation ran but its recovery stage could not be persisted"
            )
            return

        if not self._trigger_generation_active(generation):
            logger.info("[PolyMonitor] Trigger stopped before notification/rebalance")
            return

        # TG notification
        if self.notifier:
            try:
                wants_rebalance = bool(self.auto_rebalance and self.auto_rebalance.enabled)
                if cancel_confirmed and strategies_stopped:
                    rebal_status = " → 🔄 准备自动重部署" if wants_rebalance else ""
                    action = f"已确认开放订单为 0{rebal_status}"
                elif cancel_confirmed:
                    action = (
                        "已确认开放订单为 0，但策略停止未确认；"
                        "已禁止自动重部署，需手动检查"
                    )
                else:
                    action = (
                        "撤单失败或无法确认开放订单为 0；"
                        "已禁止自动重部署，需手动检查"
                    )
                msg = f"[{self.account_name or self.account_id}] ⚠️ <b>风控触发</b>\nReason: {reason}\nAction: {action}"
                logger.info(f"[PolyMonitor] Sending TG notification, enabled={self.notifier.enabled}")
                await self.notifier.send(msg)
                logger.info("[PolyMonitor] TG notification sent")
            except Exception as e:
                logger.error(f"[PolyMonitor] TG notify failed: {e}")
        else:
            logger.warning("[PolyMonitor] No notifier configured!")

        if not self._trigger_generation_active(generation):
            logger.info("[PolyMonitor] Trigger stopped before auto-rebalance")
            return

        # Auto-rebalance: on any price move (DROPPED or ROSE)
        if (
            self.auto_rebalance
            and self.auto_rebalance.enabled
            and cancel_confirmed
            and strategies_stopped
        ):
            try:
                await self.auto_rebalance.execute(
                    trigger_reason=reason,
                    poly_token_id=self.state.poly_token_id,
                    old_price_cents=self._old_price_cents,
                    strategy_type=self._strategy_type,
                    recovery_id=recovery_id,
                )
            except Exception as e:
                logger.error(f"[PolyMonitor] Auto-rebalance launch failed: {e}")
        elif self.auto_rebalance and self.auto_rebalance.enabled:
            logger.critical(
                "[PolyMonitor] Auto-rebalance blocked because cancellation or strategy stop was not verified"
            )

    async def _fast_cancel_all(self) -> dict:
        """Cancel orders and succeed only after a verified empty open-order list."""
        result = {"ok": False, "cancelled": 0, "failed": 0, "total": 0}

        async def _open_orders() -> list:
            response = await self.client.get_orders(status="open")
            if str(response.get("code", "")) != "0":
                message = response.get("message", response.get("msg", response.get("code")))
                raise RuntimeError(f"open-order verification failed: {message}")
            orders = response.get("data", {}).get("list", [])
            if not isinstance(orders, list):
                raise RuntimeError("open-order verification returned invalid data")
            return orders

        try:
            try:
                response = await self.client.cancel_all([])
                if str(response.get("code", "")) != "0":
                    logger.warning("[PolyMonitor] cancelAll response: %s", response)
            except Exception as exc:
                logger.warning("[PolyMonitor] cancelAll exception: %s", exc)

            await asyncio.sleep(0.5)
            for attempt in range(3):
                orders = await _open_orders()
                result["total"] = max(result["total"], len(orders))
                if not orders:
                    result.update(ok=True, failed=0, remaining=0)
                    logger.info("[PolyMonitor] Cancellation verified: 0 open orders")
                    return result

                cancels = []
                for order in orders:
                    order_id = order.get("oid", order.get("id", ""))
                    asset_id = order.get("assetId", order.get("tokenId", ""))
                    if order_id and asset_id:
                        cancels.append({"assetId": asset_id, "oid": order_id})
                if len(cancels) != len(orders):
                    result.update(failed=len(orders), remaining=len(orders))
                    result["error"] = "open orders are missing cancellation identifiers"
                    return result

                batch_ok = False
                try:
                    batch_response = await self.client.cancel_orders_batch(cancels)
                    batch_ok = str(batch_response.get("code", "")) == "0"
                    if not batch_ok:
                        logger.warning(
                            "[PolyMonitor] Batch cancel response: %s", batch_response
                        )
                except Exception as exc:
                    logger.warning("[PolyMonitor] Batch cancel exception: %s", exc)

                if not batch_ok:
                    async def _cancel_one(order):
                        order_id = order.get("oid", order.get("id", ""))
                        asset_id = order.get("assetId", order.get("tokenId", ""))
                        try:
                            item_response = await self.client.cancel_order(
                                asset_id=asset_id, order_id=order_id
                            )
                            return str(item_response.get("code", "")) == "0"
                        except Exception:
                            return False

                    outcomes = await asyncio.gather(
                        *[_cancel_one(order) for order in orders]
                    )
                    result["cancelled"] += sum(bool(value) for value in outcomes)
                else:
                    result["cancelled"] += len(cancels)
                if attempt < 2:
                    await asyncio.sleep(0.5)

            # Every successful path returns from an empty, successful query.
            remaining = await _open_orders()
            if not remaining:
                result.update(ok=True, failed=0, remaining=0)
                return result
            result.update(failed=len(remaining), remaining=len(remaining))
            result["error"] = f"{len(remaining)} orders remain open"
        except Exception as exc:
            result["failed"] = max(1, result.get("failed", 0))
            result["error"] = str(exc)

        logger.critical("[PolyMonitor] Cancellation not verified: %s", result)
        return result

    def status(self) -> dict:
        return {
            "enabled": self.state.enabled,
            "running": self._running,
            "triggered": self.state.triggered,
            "trigger_reason": self.state.trigger_reason,
            "reference_price": self.state.reference_price,
            "depth_threshold": self.state.depth_threshold,
            "last_poly_bid": self.state.last_poly_bid,
            "last_poly_bid_value": self.state.last_poly_bid_value,
            "poly_token_id": self.state.poly_token_id,
            "auto_rebalance": self.auto_rebalance.status() if self.auto_rebalance else None,
        }

    def recovery_status(self) -> dict:
        if self.auto_rebalance is None:
            return {"required": False, "available": False}
        return self.auto_rebalance.recovery_status()

    async def recover(self) -> dict:
        if self.auto_rebalance is None:
            return {"ok": False, "error": "Auto-Rebalance recovery is unavailable"}
        return await self.auto_rebalance.recover()
