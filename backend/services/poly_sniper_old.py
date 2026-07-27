"""Poly Sniper — cross-market latency arbitrage.

Monitors Polymarket YES and NO tokens simultaneously via WS.
When either token's best bid jumps by >= threshold in a single tick,
instantly buys on OKX (IOC), waits for the configured settle window, then
exits at the current OKX best bid with reduce-only IOC orders.

States: IDLE → MONITORING → TRIGGERED → HOLDING → back to MONITORING or STOP
"""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional, Dict, Callable
from dataclasses import dataclass, field
from ..core.logging_config import audit_event, market_event
from ..core.price_utils import (
    as_decimal,
    format_okx_price,
    invert_binary_price,
)
from .size_utils import normalize_size_str, normalize_buy_size_str, size_to_float
from .order_lifecycle import (
    TERMINAL_ORDER_STATUSES,
    cancel_and_confirm,
    generate_client_order_id,
    normalise_order_status,
    observe_order,
    place_and_confirm,
)

import aiohttp

logger = logging.getLogger(__name__)


def _display_price(value) -> str:
    """Preserve exact fine ticks and non-tick average fill prices in logs."""
    value_decimal = as_decimal(value)
    try:
        return format_okx_price(value_decimal)
    except ValueError:
        return format(value_decimal, "f")

POLY_CLOB_BOOK = "https://clob.polymarket.com/book?token_id={token_id}"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class SniperOldConfig:
    threshold_cents: int = 40       # Jump threshold in cents
    ask_levels: int = 99            # How many ask levels to eat (99=all)
    position_pct: int = 100         # Balance percentage to use
    timeout_secs: int = 120         # Seconds before forced exit
    settle_ms: int = 0           # Ms to wait for price to stabilize after jump
    window_ms: int = 150            # Rolling window for cumulative jump detection


@dataclass
class SniperOldState:
    status: str = "idle"            # idle, monitoring, holding, stopped
    # Token info
    yes_token_id: str = ""          # Poly YES token
    no_token_id: str = ""           # Poly NO token
    yes_asset_id: str = ""          # OKX YES asset
    no_asset_id: str = ""           # OKX NO asset
    market_id: str = ""
    poly_url: str = ""
    selected_outcome: str = ""
    # Monitoring
    last_yes_bid: float = 0.0
    last_no_bid: float = 0.0
    # Settling state (waiting for price stabilization)
    settling: bool = False
    settle_side: str = ""           # which side triggered
    settle_price: float = 0.0       # latest price during settle
    settle_initial: float = 0.0     # price before the jump
    settle_last_update: float = 0.0 # time.monotonic() of last price change
    # OKX snapshot at trigger time (before market catches up)
    okx_snapshot_price: float = 0.0 # OKX best ask at moment of trigger
    okx_snapshot_asks: list = field(default_factory=list)  # Full ask levels snapshot
    # Holding state
    holding_side: str = ""          # "yes" or "no" - which token we bought
    buy_price: float = 0.0          # Price we bought at
    buy_size: float = 0.0           # How many we bought
    sold_size: float = 0.0          # Cumulative confirmed exits
    buy_client_order_id: str = ""   # Exact entry ownership / reconciliation key
    buy_order_id: str = ""
    sell_target: float = 0.0        # Target sell price (Poly jump price)
    sell_order_id: str = ""         # OKX sell order ID
    sell_client_order_id: str = ""
    sell_filled_size: float = 0.0
    exit_client_order_ids: list = field(default_factory=list)
    resolved_exit_client_order_ids: list = field(default_factory=list)
    hold_start: float = 0.0        # When we entered holding
    hold_start_epoch_ms: int = 0
    locked: bool = False            # Unresolved exchange state blocks restart
    reconciliation_error: str = ""
    # Logs
    logs: list = field(default_factory=list)


class PolySniperOld:
    """Cross-market latency arbitrage between Poly and OKX."""

    def __init__(self, client, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.config = SniperOldConfig()
        self.state = SniperOldState()
        self.notifier = None
        self._task: Optional[asyncio.Task] = None
        self._hold_task: Optional[asyncio.Task] = None
        self._settle_task: Optional[asyncio.Task] = None
        self._running = False
        self._stop_requested = False
        self._stop_lock = asyncio.Lock()
        self._exit_lock = asyncio.Lock()
        self._order_confirmation_attempts = 10
        self._order_confirmation_delay = 0.3
        self._ws_calibrated = {"yes": False, "no": False}
        self._price_window: Dict[str, list] = {"yes": [], "no": []}  # [(timestamp, price), ...]
        self._baseline: Dict[str, float] = {"yes": 0.0, "no": 0.0}  # Tracks high watermark per side
        self._last_no_trigger_log: Dict[str, float] = {"yes": 0.0, "no": 0.0}  # Rate-limit no-trigger logs
        self._telemetry_run_id = ""
        self._poly_frame_sequence = 0
        self.on_state_change: Optional[Callable[[dict], bool]] = None
        self._restoring = False

    @property
    def running(self) -> bool:
        return self._running

    def log(self, msg: str):
        from datetime import datetime
        entry = f"[{datetime.now().strftime('%I:%M:%S %p')}] {msg}"
        self.state.logs.append(entry)
        if len(self.state.logs) > 100:
            self.state.logs = self.state.logs[-100:]
        logger.info(f"[SniperOld:{self.account_id}] {msg}")

    def get_config(self) -> dict:
        return {
            "threshold_cents": self.config.threshold_cents,
            "ask_levels": self.config.ask_levels,
            "position_pct": self.config.position_pct,
            "timeout_secs": self.config.timeout_secs,
            "settle_ms": self.config.settle_ms,
            "window_ms": self.config.window_ms,
        }

    def get_runtime(self) -> dict:
        return {
            "status": self.state.status,
            "yes_token_id": self.state.yes_token_id,
            "no_token_id": self.state.no_token_id,
            "yes_asset_id": self.state.yes_asset_id,
            "no_asset_id": self.state.no_asset_id,
            "market_id": self.state.market_id,
            "poly_url": self.state.poly_url,
            "selected_outcome": self.state.selected_outcome,
            "last_yes_bid": self.state.last_yes_bid,
            "last_no_bid": self.state.last_no_bid,
            "holding_side": self.state.holding_side,
            "buy_price": self.state.buy_price,
            "buy_size": self.state.buy_size,
            "sold_size": self.state.sold_size,
            "buy_client_order_id": self.state.buy_client_order_id,
            "buy_order_id": self.state.buy_order_id,
            "sell_target": self.state.sell_target,
            "sell_order_id": self.state.sell_order_id,
            "sell_client_order_id": self.state.sell_client_order_id,
            "sell_filled_size": self.state.sell_filled_size,
            "exit_client_order_ids": list(self.state.exit_client_order_ids),
            "resolved_exit_client_order_ids": list(
                self.state.resolved_exit_client_order_ids
            ),
            "hold_start_epoch_ms": self.state.hold_start_epoch_ms,
            "locked": self.state.locked,
            "reconciliation_error": self.state.reconciliation_error,
        }

    def _persistence_snapshot(self) -> dict:
        return {
            "config": self.get_config(),
            "runtime": self.get_runtime(),
            "running": self._running,
        }

    def _persist(
        self,
        context: str,
        *,
        fail_closed: bool = True,
        running_override: Optional[bool] = None,
    ) -> bool:
        if self._restoring or self.on_state_change is None:
            return True
        try:
            snapshot = self._persistence_snapshot()
            if running_override is not None:
                snapshot["running"] = bool(running_override)
            saved = self.on_state_change(snapshot)
        except Exception as exc:
            saved = False
            logger.exception(
                "[SniperOld:%s] persistence callback failed at %s",
                self.account_id,
                context,
            )
        if saved is not False:
            return True
        if fail_closed:
            self._running = False
            self.state.status = "error"
            self.state.locked = True
            self.state.reconciliation_error = (
                f"Sniper Old persistence failed at {context}"
            )
            self.log(self.state.reconciliation_error)
        return False

    @staticmethod
    def _elapsed_ms(started_ns: int) -> float:
        return round(max(0, time.perf_counter_ns() - started_ns) / 1_000_000, 3)

    @staticmethod
    def _first_level_price(rows):
        first = rows[0] if rows else None
        if isinstance(first, (list, tuple)):
            return first[0] if first else None
        if isinstance(first, dict):
            return first.get("price")
        return None

    def _telemetry_fields(self) -> dict:
        return {
            "component": "poly-sniper-old",
            "strategy": "poly-sniper-old",
            "account_id": self.account_id,
            "run_id": self._telemetry_run_id,
            "market_id": self.state.market_id,
        }

    def _emit_market_event(self, source: str, event: str, **fields) -> None:
        """Telemetry must never alter whether a trade is evaluated or submitted."""
        try:
            market_event(source, event, **fields)
        except Exception:
            logger.exception(
                "[SniperOld:%s] market telemetry failed for %s",
                self.account_id,
                event,
            )

    def _emit_audit_event(self, event: str, **fields) -> None:
        try:
            audit_event(event, **fields)
        except Exception:
            logger.exception(
                "[SniperOld:%s] audit telemetry failed for %s",
                self.account_id,
                event,
            )

    def _record_poly_ws_observation(
        self,
        data: dict,
        *,
        received_wall_ns: int,
        received_monotonic_ns: int,
        frame_sequence: int,
        source_event_index: int,
    ) -> None:
        """Persist the unmodified Poly market payload before any decision logic."""
        event_type = str(data.get("event_type", ""))
        if event_type not in {"book", "price_change"}:
            return
        self._emit_market_event(
            "polymarket",
            f"sniper_old.ws_{event_type}",
            **self._telemetry_fields(),
            frame_sequence=frame_sequence,
            source_event_index=source_event_index,
            event_type=event_type,
            poly_token_id=str(data.get("asset_id", "")),
            source_timestamp=data.get("timestamp", data.get("ts", "")),
            received_wall_ns=received_wall_ns,
            received_monotonic_ns=received_monotonic_ns,
            bids=data.get("bids") if event_type == "book" else None,
            asks=data.get("asks") if event_type == "book" else None,
            price_changes=(
                data.get("price_changes") if event_type == "price_change" else None
            ),
            raw_event=data,
        )

    def _record_bid_decision(
        self,
        *,
        asset_id: str,
        side: str,
        previous_bid,
        current_bid,
        decision: str,
        started_ns: int,
        received_wall_ns: int,
        received_monotonic_ns: int,
        **fields,
    ) -> None:
        self._emit_market_event(
            "polymarket",
            "sniper_old.bid_decision",
            **self._telemetry_fields(),
            poly_token_id=asset_id,
            side=side,
            previous_bid=_display_price(previous_bid) if previous_bid else "0",
            current_bid=_display_price(current_bid) if current_bid else "0",
            yes_bid=_display_price(self.state.last_yes_bid) if self.state.last_yes_bid else "0",
            no_bid=_display_price(self.state.last_no_bid) if self.state.last_no_bid else "0",
            status=self.state.status,
            settling=self.state.settling,
            decision=decision,
            received_wall_ns=received_wall_ns,
            received_monotonic_ns=received_monotonic_ns,
            decision_duration_ms=self._elapsed_ms(started_ns),
            **fields,
        )

    async def start(self, yes_token_id: str, no_token_id: str,
                    yes_asset_id: str, no_asset_id: str, market_id: str,
                    threshold_cents: int = 40, ask_levels: int = 99,
                    position_pct: int = 100, timeout_secs: int = 120,
                    settle_ms: int = 0, window_ms: int = 150,
                    poly_url: str = "", selected_outcome: str = ""):
        """Start sniper monitoring."""
        if self._running:
            return {"ok": False, "error": "Already running"}
        if self.state.locked:
            return {
                "ok": False,
                "error": self.state.reconciliation_error
                or "Sniper Old has unresolved exchange state; reconcile it before restarting",
                "locked": True,
            }

        self.config.threshold_cents = threshold_cents
        self.config.ask_levels = ask_levels
        self.config.position_pct = position_pct
        self.config.timeout_secs = timeout_secs
        self.config.settle_ms = settle_ms
        self.config.window_ms = window_ms

        self._price_window = {"yes": [], "no": []}
        self._baseline = {"yes": 0.0, "no": 0.0}
        self._last_no_trigger_log = {"yes": 0.0, "no": 0.0}
        self._telemetry_run_id = f"{self.account_id}-{time.time_ns()}"
        self._poly_frame_sequence = 0
        self.state = SniperOldState(
            status="monitoring",
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            yes_asset_id=yes_asset_id,
            no_asset_id=no_asset_id,
            market_id=market_id,
        )
        self.state.poly_url = poly_url
        self.state.selected_outcome = selected_outcome
        self._stop_requested = False

        # Get initial Poly prices
        yes_bid = await self._get_poly_bid(yes_token_id)
        no_bid = await self._get_poly_bid(no_token_id)
        if yes_bid is not None:
            self.state.last_yes_bid = yes_bid
        if no_bid is not None:
            self.state.last_no_bid = no_bid

        # Cross-validate: compare Poly bid with OKX ask to ensure correct token pairing
        try:
            okx_book = await self.client.get_orderbook(yes_asset_id, depth=1)
            okx_ask = None
            if str(okx_book.get("code", "")) == "0":
                asks = okx_book.get("data", [{}])[0].get("asks", [])
                if asks:
                    okx_ask = float(asks[0][0])
            if okx_ask and yes_bid:
                diff = abs(yes_bid - okx_ask)
                if diff > 0.15:
                    self.log(f"⚠️ WARNING: Poly YES bid ({yes_bid:.3f}) and OKX ask ({okx_ask:.3f}) differ by {diff:.3f} — possible token mismatch!")
                else:
                    self.log(f"✓ Poly YES={yes_bid:.3f}, OKX ask={okx_ask:.3f} (diff={diff:.3f})")
        except Exception:
            pass

        self._running = True
        if not self._persist("start"):
            return {
                "ok": False,
                "error": self.state.reconciliation_error,
                "locked": True,
            }
        self._task = asyncio.create_task(self._monitor_loop())
        self._emit_audit_event(
            "sniper_old.started",
            **self._telemetry_fields(),
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            yes_asset_id=yes_asset_id,
            no_asset_id=no_asset_id,
            threshold_cents=threshold_cents,
            ask_levels=ask_levels,
            position_pct=position_pct,
            timeout_secs=timeout_secs,
            settle_ms=settle_ms,
            window_ms=window_ms,
        )
        self.log(f"Started: threshold={threshold_cents}¢, levels={ask_levels}, pct={position_pct}%, settle={settle_ms}ms, window={window_ms}ms")
        return {"ok": True}

    async def stop(self) -> bool:
        """Stop new signals, reconcile owned orders, and prove the position is flat."""
        async with self._stop_lock:
            self._stop_requested = True
            self._running = False
            self.state.settling = False

            monitor_task = self._task
            if monitor_task is not None and monitor_task is not asyncio.current_task():
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)
            if self._task is monitor_task:
                self._task = None

            # Do not cancel an in-flight settle/exit coroutine: cancelling an
            # HTTP await could leave an accepted order without reconciliation.
            for task in (self._settle_task, self._hold_task):
                if task is not None and task is not asyncio.current_task() and not task.done():
                    await asyncio.gather(task, return_exceptions=True)

            asset_id = self._holding_asset_id()
            if not asset_id and not self.state.buy_client_order_id:
                self._clear_holding(status="stopped")
                self._persist("stop-flat", fail_closed=False)
                self.log("Stopped")
                return True

            if not await self._reconcile_entry_order():
                return False
            if not await self._cancel_tracked_sell():
                return False

            live_position = await self._matching_position_size()
            if live_position is None:
                self._mark_reconciliation_failure(
                    "Stop could not verify the current OKX position"
                )
                return False
            if live_position > Decimal("0"):
                tracked_remaining = self._remaining_size()
                if live_position > tracked_remaining:
                    # Entry reconciliation proved the cloid, so a larger live
                    # position means the local fill ledger was stale. Preserve
                    # the real quantity instead of forgetting it.
                    self.state.buy_size = float(
                        Decimal(str(self.state.sold_size)) + live_position
                    )
                if not asset_id:
                    asset_id = self._holding_asset_id()
                label = "YES" if self.state.holding_side == "yes" else "NO"
                if not asset_id or not await self._handle_timeout(asset_id, label):
                    return False
            else:
                self._clear_holding(status="stopped")

            self._stop_requested = False
            self._persist("stop-complete", fail_closed=False)
            self.log("Stopped (owned orders terminal, position flat)")
            return True

    # ─── Monitoring ───

    async def _monitor_loop(self):
        """Main loop: WS primary, REST fallback."""
        try:
            while self._running:
                try:
                    await self._ws_monitor()
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.debug(f"[Sniper] WS error: {e}")
                if self._running:
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    async def _ws_monitor(self):
        """Connect to Poly WS and watch for jumps."""
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(POLY_WS_URL, heartbeat=15) as ws:
                # Subscribe to both tokens in one message
                import json
                await ws.send_str(json.dumps({
                    "type": "market",
                    "assets_ids": [self.state.yes_token_id, self.state.no_token_id]
                }))
                self._ws_calibrated = {"yes": False, "no": False}
                self.log("WS connected, monitoring YES + NO")

                async for msg in ws:
                    received_wall_ns = time.time_ns()
                    received_monotonic_ns = time.monotonic_ns()
                    self._poly_frame_sequence += 1
                    frame_sequence = self._poly_frame_sequence
                    if not self._running:
                        break
                    if self.state.status != "monitoring" and not self.state.settling:
                        # In holding mode, just keep connection alive
                        continue
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            raw = json.loads(msg.data)
                            # Response can be array (snapshot) or single object (update)
                            items = raw if isinstance(raw, list) else [raw]
                            for source_event_index, data in enumerate(items):
                                evt = data.get("event_type", "")
                                self._record_poly_ws_observation(
                                    data,
                                    received_wall_ns=received_wall_ns,
                                    received_monotonic_ns=received_monotonic_ns,
                                    frame_sequence=frame_sequence,
                                    source_event_index=source_event_index,
                                )
                                if evt == "book":
                                    await self._process_book_update(
                                        data,
                                        received_wall_ns=received_wall_ns,
                                        received_monotonic_ns=received_monotonic_ns,
                                    )
                                elif not hasattr(self, '_dbg_logged'):
                                    # Log first non-book message for debugging
                                    self._dbg_logged = True
                                    self.log(f"WS msg event_type={evt}, keys={list(data.keys())[:8]}")
                        except Exception:
                            pass
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

    async def _process_book_update(
        self,
        data: dict,
        *,
        received_wall_ns: Optional[int] = None,
        received_monotonic_ns: Optional[int] = None,
    ):
        """Check if a jump occurred. Uses settle window to capture full move."""
        decision_started_ns = time.perf_counter_ns()
        received_wall_ns = received_wall_ns or time.time_ns()
        received_monotonic_ns = received_monotonic_ns or time.monotonic_ns()
        asset_id = data.get("asset_id", "")
        bids = data.get("bids", [])
        if not bids:
            side = (
                "yes" if asset_id == self.state.yes_token_id
                else "no" if asset_id == self.state.no_token_id
                else "unknown"
            )
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=0,
                current_bid=0,
                decision="empty_book",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                bid_levels=0,
            )
            return

        # Get best bid
        bids_sorted = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
        current_bid = float(bids_sorted[0].get("price", 0))

        # Determine which token
        if asset_id == self.state.yes_token_id:
            prev_bid = self.state.last_yes_bid
            self.state.last_yes_bid = current_bid
            side = "yes"
        elif asset_id == self.state.no_token_id:
            prev_bid = self.state.last_no_bid
            self.state.last_no_bid = current_bid
            side = "no"
        else:
            self._record_bid_decision(
                asset_id=asset_id,
                side="unknown",
                previous_bid=0,
                current_bid=current_bid,
                decision="unknown_token",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                bid_levels=len(bids),
            )
            return

        # If currently settling, update settle price to latest value
        if self.state.settling and side == self.state.settle_side:
            if current_bid != self.state.settle_price:
                self.state.settle_price = current_bid
                self.state.settle_last_update = time.monotonic()
                self.log(f"Settle update: {side.upper()} now {current_bid:.3f}")
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=prev_bid,
                current_bid=current_bid,
                decision="settle_observation",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                settle_side=self.state.settle_side,
                settle_price=_display_price(self.state.settle_price),
                settle_elapsed_ms=round(
                    max(0, time.monotonic() - self.state.settle_last_update) * 1000,
                    3,
                ),
            )
            return

        # Ignore updates during settling for the other side
        if self.state.settling:
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=prev_bid,
                current_bid=current_bid,
                decision="ignored_other_side_during_settle",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                settle_side=self.state.settle_side,
            )
            return

        if prev_bid <= 0:
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=prev_bid,
                current_bid=current_bid,
                decision="initial_bid",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
            )
            return

        # Skip first WS update per side (calibration — avoids false trigger from stale REST data)
        if not self._ws_calibrated.get(side, False):
            self._ws_calibrated[side] = True
            self._baseline[side] = current_bid  # Set initial baseline
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=prev_bid,
                current_bid=current_bid,
                decision="calibrated",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                baseline=_display_price(current_bid),
            )
            return

        # Update baseline (high watermark — only goes up)
        if current_bid > self._baseline[side]:
            self._baseline[side] = current_bid

        # Rolling window jump detection: check if price moved >= threshold within window_ms
        now = time.monotonic()
        window = self._price_window[side]
        window.append((now, current_bid))
        # Trim entries older than window_ms
        cutoff = now - (self.config.window_ms / 1000.0)
        while window and window[0][0] < cutoff:
            window.pop(0)
        # Calculate cumulative jump: current price vs lowest price in window
        window_low = min(p for _, p in window)
        jump_cents_raw = (
            as_decimal(current_bid) - as_decimal(window_low)
        ) * Decimal("100")
        threshold_cents = Decimal(str(self.config.threshold_cents))
        if jump_cents_raw < threshold_cents:
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=prev_bid,
                current_bid=current_bid,
                decision="below_threshold",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                window_low=_display_price(window_low),
                window_high=_display_price(max(p for _, p in window)),
                jump_cents=format(jump_cents_raw, "f"),
                threshold_cents=format(threshold_cents, "f"),
                window_ms=self.config.window_ms,
                window_points=len(window),
                baseline=_display_price(self._baseline[side]),
            )
            # Explain non-actions without flooding logs: log near misses or once every ~2s per side.
            last_log = self._last_no_trigger_log.get(side, 0.0)
            near_miss = jump_cents_raw >= Decimal(
                str(max(1, self.config.threshold_cents - 3))
            )
            if near_miss or now - last_log >= 2.0:
                self._last_no_trigger_log[side] = now
                needed = threshold_cents - jump_cents_raw
                self.log(
                    f"NO TRIGGER: {side.upper()} {window_low:.3f}→{current_bid:.3f} "
                    f"(+{float(jump_cents_raw):.2f}¢) < threshold {self.config.threshold_cents}¢ "
                    f"(need +{float(needed):.2f}¢, window={self.config.window_ms}ms, points={len(window)})"
                )
        if jump_cents_raw >= threshold_cents:
            self._price_window[side] = []  # Reset window after trigger
            # Bounce filter: if current price is far below baseline, this is a dead cat bounce during crash
            baseline = self._baseline[side]
            if baseline > 0 and current_bid < baseline * 0.7:
                self._record_bid_decision(
                    asset_id=asset_id,
                    side=side,
                    previous_bid=prev_bid,
                    current_bid=current_bid,
                    decision="bounce_filter",
                    started_ns=decision_started_ns,
                    received_wall_ns=received_wall_ns,
                    received_monotonic_ns=received_monotonic_ns,
                    window_low=_display_price(window_low),
                    jump_cents=format(jump_cents_raw, "f"),
                    threshold_cents=format(threshold_cents, "f"),
                    baseline=_display_price(baseline),
                    bounce_floor=_display_price(baseline * 0.7),
                )
                self.log(f"BOUNCE SKIP: {side.upper()} {window_low:.3f}→{current_bid:.3f} (+{float(jump_cents_raw):.2f}¢) but current {current_bid:.3f} < baseline {baseline:.3f}*0.7={baseline*0.7:.3f}")
                return
            self._record_bid_decision(
                asset_id=asset_id,
                side=side,
                previous_bid=prev_bid,
                current_bid=current_bid,
                decision="trigger",
                started_ns=decision_started_ns,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                window_low=_display_price(window_low),
                jump_cents=format(jump_cents_raw, "f"),
                threshold_cents=format(threshold_cents, "f"),
                window_ms=self.config.window_ms,
                window_points=len(window),
                baseline=_display_price(baseline),
            )
            self._emit_audit_event(
                "sniper_old.trigger_detected",
                **self._telemetry_fields(),
                side=side,
                poly_token_id=asset_id,
                previous_bid=_display_price(prev_bid),
                current_bid=_display_price(current_bid),
                window_low=_display_price(window_low),
                jump_cents=format(jump_cents_raw, "f"),
                threshold_cents=format(threshold_cents, "f"),
                window_ms=self.config.window_ms,
            )
            self.log(f"JUMP detected: {side.upper()} {window_low:.3f} → {current_bid:.3f} (+{float(jump_cents_raw):.2f}¢) within {self.config.window_ms}ms, buying immediately...")
            # IMMEDIATELY snapshot OKX orderbook and BUY before it catches up
            okx_asset = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
            snapshot_asks = await self._snapshot_okx_asks(okx_asset)
            if not snapshot_asks:
                self._emit_market_event(
                    "okx",
                    "sniper_old.entry_decision",
                    **self._telemetry_fields(),
                    side=side,
                    selected_asset_id=okx_asset,
                    decision="skip_no_asks",
                    poly_bid=_display_price(current_bid),
                )
                self.log("OKX snapshot failed — no asks, skipping")
                return
            snapshot_best_ask = float(snapshot_asks[0][0])
            self.log(f"OKX snapshot: best ask={_display_price(snapshot_best_ask)} (poly={current_bid:.3f})")

            # BUY FIRST — execute immediately at snapshot prices
            bought = await self._execute_buy_immediate(side, okx_asset, snapshot_asks, window_low, current_bid)
            if not bought:
                return

            # THEN settle — watch Poly before exiting at the current OKX bid.
            self.state.settling = True
            self.state.settle_side = side
            self.state.settle_price = current_bid
            self.state.settle_initial = prev_bid
            self.state.settle_last_update = time.monotonic()
            self._emit_market_event(
                "polymarket",
                "sniper_old.settle",
                **self._telemetry_fields(),
                phase="started",
                side=side,
                poly_token_id=asset_id,
                initial_bid=_display_price(prev_bid),
                current_bid=_display_price(current_bid),
                settle_ms=self.config.settle_ms,
                settle_started_monotonic_ns=time.monotonic_ns(),
            )
            # Start settle timer task; it will execute the best-bid IOC exit.
            self._settle_task = asyncio.create_task(self._settle_then_sell())

    async def _settle_then_sell(self):
        """After buying, wait for Poly to stabilize, then exit at OKX best bid."""
        try:
            settle_sec = self.config.settle_ms / 1000.0
            while self._running and self.state.settling:
                elapsed = time.monotonic() - self.state.settle_last_update
                if elapsed >= settle_sec:
                    # Price stable
                    final_price = self.state.settle_price
                    initial_price = self.state.settle_initial
                    side = self.state.settle_side
                    self.state.settling = False
                    settle_decision = (
                        "exit_reverted_to_or_below_cost"
                        if final_price <= self.state.buy_price
                        else "exit_settled"
                    )
                    self._emit_market_event(
                        "polymarket",
                        "sniper_old.settle",
                        **self._telemetry_fields(),
                        phase="completed",
                        side=side,
                        initial_bid=_display_price(initial_price),
                        final_bid=_display_price(final_price),
                        buy_price=_display_price(self.state.buy_price),
                        configured_settle_ms=self.config.settle_ms,
                        stable_elapsed_ms=round(elapsed * 1000, 3),
                        decision=settle_decision,
                        decision_wall_ns=time.time_ns(),
                        decision_monotonic_ns=time.monotonic_ns(),
                    )
                    self._emit_audit_event(
                        "sniper_old.settle_completed",
                        **self._telemetry_fields(),
                        side=side,
                        initial_bid=_display_price(initial_price),
                        final_bid=_display_price(final_price),
                        buy_price=_display_price(self.state.buy_price),
                        configured_settle_ms=self.config.settle_ms,
                        stable_elapsed_ms=round(elapsed * 1000, 3),
                        decision=settle_decision,
                    )
                    self.log(f"Settled: {side.upper()} final={final_price:.3f} (from {initial_price:.3f})")
                    # If price reverted below buy cost, stop immediately
                    if final_price <= self.state.buy_price:
                        self.log(f"Settled price {final_price:.3f} <= buy cost {_display_price(self.state.buy_price)}, selling at best bid")
                        asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
                        label = "YES" if side == "yes" else "NO"
                        await self._notify(f"⚠️ settle价 {final_price:.3f} ≤ 成本 {_display_price(self.state.buy_price)}, 市价出")
                        await self._handle_timeout(asset_id, label)
                        return
                    # Old mode exits immediately at the current marketable bid.
                    asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
                    label = "YES" if side == "yes" else "NO"
                    await self._market_exit(asset_id, label, reason="settled")
                    return
                await asyncio.sleep(0.05)
            # Stop owns reconciliation.  Do not race it with another exit order.
            if self.state.settling:
                self._emit_market_event(
                    "polymarket",
                    "sniper_old.settle",
                    **self._telemetry_fields(),
                    phase="stopped",
                    side=self.state.settle_side,
                    initial_bid=_display_price(self.state.settle_initial),
                    final_bid=_display_price(self.state.settle_price),
                    decision="stop_owns_reconciliation",
                    decision_wall_ns=time.time_ns(),
                    decision_monotonic_ns=time.monotonic_ns(),
                )
            self.state.settling = False
        finally:
            if self._settle_task is asyncio.current_task():
                self._settle_task = None


    def _okx_query_asset(self, side_or_asset: str) -> str:
        """OKX pm-books exists only for YES asset; NO book must be inferred from YES book."""
        return self.state.yes_asset_id if side_or_asset == "no" or side_or_asset == self.state.no_asset_id else side_or_asset

    def _invert_asks_from_yes_book(self, yes_bids: list) -> list:
        """Synthetic NO asks = 1 - YES bids."""
        out = []
        for b in yes_bids or []:
            if not b:
                continue
            row = list(b)
            row[0] = format_okx_price(invert_binary_price(b[0]))
            out.append(row)
        out.sort(key=lambda x: float(x[0]))
        return out

    def _invert_bids_from_yes_book(self, yes_asks: list) -> list:
        """Synthetic NO bids = 1 - YES asks."""
        out = []
        for a in yes_asks or []:
            if not a:
                continue
            row = list(a)
            row[0] = format_okx_price(invert_binary_price(a[0]))
            out.append(row)
        out.sort(key=lambda x: float(x[0]), reverse=True)
        return out

    async def _snapshot_okx_asks(self, asset_id: str) -> list:
        """Instantly grab OKX asks at trigger moment."""
        started_ns = time.perf_counter_ns()
        query_asset = self._okx_query_asset(asset_id)
        try:
            book = await self.client.get_orderbook(query_asset, depth=10)
            received_wall_ns = time.time_ns()
            received_monotonic_ns = time.monotonic_ns()
            if str(book.get("code", "")) != "0":
                self._emit_audit_event(
                    "sniper_old.entry_book_failed",
                    level=logging.WARNING,
                    **self._telemetry_fields(),
                    query_asset_id=query_asset,
                    selected_asset_id=asset_id,
                    response_code=str(book.get("code", "")),
                    response_message=book.get("msg", book.get("message", "")),
                    duration_ms=self._elapsed_ms(started_ns),
                )
                self.log(f"OKX snapshot book error for {query_asset}: {book.get('msg','')}")
                return []
            data = book.get("data", [{}])[0]
            if asset_id == self.state.no_asset_id:
                selected_asks = self._invert_asks_from_yes_book(data.get("bids", []))
            else:
                selected_asks = data.get("asks", [])
            self._emit_market_event(
                "okx",
                "sniper_old.rest_book",
                **self._telemetry_fields(),
                purpose="entry",
                query_asset_id=query_asset,
                selected_asset_id=asset_id,
                selected_side=("no" if asset_id == self.state.no_asset_id else "yes"),
                source_timestamp=data.get("ts", ""),
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                duration_ms=self._elapsed_ms(started_ns),
                raw_book=data,
                yes_bids=data.get("bids", []),
                yes_asks=data.get("asks", []),
                yes_best_bid=self._first_level_price(data.get("bids", [])),
                yes_best_ask=self._first_level_price(data.get("asks", [])),
                selected_asks=selected_asks,
                selected_best_ask=self._first_level_price(selected_asks),
                derived_from_yes_book=asset_id == self.state.no_asset_id,
            )
            return selected_asks  # [[price, size], ...]
        except Exception as e:
            self._emit_audit_event(
                "sniper_old.entry_book_exception",
                level=logging.WARNING,
                **self._telemetry_fields(),
                query_asset_id=query_asset,
                selected_asset_id=asset_id,
                error_type=type(e).__name__,
                error=str(e),
                duration_ms=self._elapsed_ms(started_ns),
            )
            self.log(f"OKX snapshot error: {e}")
            return []

    def _holding_asset_id(self) -> str:
        if self.state.holding_side == "yes":
            return self.state.yes_asset_id
        if self.state.holding_side == "no":
            return self.state.no_asset_id
        return ""

    def _remaining_size(self) -> Decimal:
        return max(
            Decimal("0"),
            Decimal(str(self.state.buy_size)) - Decimal(str(self.state.sold_size)),
        )

    def _mark_reconciliation_failure(self, message: str) -> None:
        self._running = False
        self.state.status = "error"
        self.state.locked = True
        self.state.reconciliation_error = str(message)
        self._persist("reconciliation-failure", fail_closed=False)
        self.log(f"RECONCILIATION REQUIRED: {message}")

    def _apply_entry_observation(self, observation) -> None:
        if observation is None:
            return
        if observation.order_id:
            self.state.buy_order_id = observation.order_id
        filled_size = Decimal(observation.filled_size)
        if filled_size <= 0:
            return
        filled_amount = Decimal(observation.filled_amount)
        self.state.buy_size = float(filled_size)
        if filled_amount > 0:
            self.state.buy_price = float(filled_amount / filled_size)
        self._persist("entry-observed")

    def _apply_exit_observation(self, client_order_id: str, observation) -> Decimal:
        if observation is None:
            return Decimal("0")
        if client_order_id in self.state.resolved_exit_client_order_ids:
            return Decimal("0")
        filled = max(Decimal("0"), Decimal(observation.filled_size))
        self.state.sold_size = float(Decimal(str(self.state.sold_size)) + filled)
        self.state.resolved_exit_client_order_ids.append(client_order_id)
        self._persist("exit-observed")
        return filled

    async def _submit_ioc_and_confirm(
        self,
        *,
        asset_id: str,
        side: str,
        price: str,
        size: str,
        reduce_only: bool,
        client_order_id: str,
    ):
        """Submit one IOC and return only after its exact cloid is terminal."""
        confirmation = await place_and_confirm(
            self.client,
            client_order_id=client_order_id,
            market_id=self.state.market_id,
            asset_id=asset_id,
            side=side,
            price=price,
            size=size,
            tif="ioc",
            reduce_only=reduce_only,
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        if confirmation.rejected:
            return "rejected", None, confirmation

        observation = confirmation.observation
        if observation is not None and normalise_order_status(
            observation.status
        ) in TERMINAL_ORDER_STATUSES:
            return "terminal", observation, confirmation

        # IOC may transiently appear ACTIVE/PENDING, and TIMEOUT/invalid HTTP
        # responses are ambiguous.  Cancel only this cloid, then require a
        # terminal observation before another order can be submitted.
        cancel_confirmation = await cancel_and_confirm(
            self.client,
            client_order_id=client_order_id,
            order_id=observation.order_id if observation is not None else "",
            market_id=self.state.market_id,
            asset_id=asset_id,
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        if (
            cancel_confirmation.confirmed_terminal
            and cancel_confirmation.observation is not None
        ):
            return "terminal", cancel_confirmation.observation, confirmation
        return "unresolved", None, confirmation

    async def _reconcile_entry_order(self) -> bool:
        client_order_id = self.state.buy_client_order_id
        if not client_order_id:
            return True
        asset_id = self._holding_asset_id()
        if not asset_id:
            # Before a fill is known, settle_side still identifies the asset.
            side = self.state.settle_side
            asset_id = (
                self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
            )
        observation = await observe_order(
            self.client,
            client_order_id=client_order_id,
            order_id=self.state.buy_order_id,
            market_id=self.state.market_id,
        )
        if observation is not None and normalise_order_status(
            observation.status
        ) in TERMINAL_ORDER_STATUSES:
            self._apply_entry_observation(observation)
            return True
        confirmation = await cancel_and_confirm(
            self.client,
            client_order_id=client_order_id,
            order_id=(observation.order_id if observation else self.state.buy_order_id),
            market_id=self.state.market_id,
            asset_id=asset_id,
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        if confirmation.confirmed_terminal and confirmation.observation is not None:
            self._apply_entry_observation(confirmation.observation)
            return True
        self._mark_reconciliation_failure(
            f"Entry order {client_order_id} terminal state could not be confirmed"
        )
        return False

    async def _cancel_tracked_sell(self) -> bool:
        client_order_id = self.state.sell_client_order_id
        order_id = self.state.sell_order_id
        if not client_order_id and not order_id:
            return True
        asset_id = self._holding_asset_id()
        confirmation = await cancel_and_confirm(
            self.client,
            client_order_id=client_order_id,
            order_id=order_id,
            market_id=self.state.market_id,
            asset_id=asset_id,
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        if not confirmation.confirmed_terminal or confirmation.observation is None:
            self._mark_reconciliation_failure(
                f"Protective sell {client_order_id or order_id} terminal state could not be confirmed"
            )
            return False
        observation = confirmation.observation
        cumulative = max(Decimal("0"), Decimal(observation.filled_size))
        previous = Decimal(str(self.state.sell_filled_size))
        if cumulative > previous:
            self.state.sold_size = float(
                Decimal(str(self.state.sold_size)) + cumulative - previous
            )
            self.state.sell_filled_size = float(cumulative)
        self.state.sell_client_order_id = ""
        self.state.sell_order_id = ""
        self._persist("protective-sell-terminal")
        return True

    async def _matching_position_size(self) -> Optional[Decimal]:
        try:
            response = await self.client.get_positions(
                status="open", market_id=self.state.market_id or None
            )
        except TypeError:
            response = await self.client.get_positions(status="open")
        except Exception:
            return None
        if not isinstance(response, dict) or str(response.get("code", "")) != "0":
            return None
        data = response.get("data")
        positions = data.get("list") if isinstance(data, dict) else None
        if not isinstance(positions, list):
            return None
        expected_index = "1" if self.state.holding_side == "yes" else "2"
        expected_name = "YES" if self.state.holding_side == "yes" else "NO"
        total = Decimal("0")
        for position in positions:
            if self.state.market_id and str(position.get("marketId") or "") != self.state.market_id:
                continue
            token_index = str(position.get("tokenIndex") or "")
            token_name = str(position.get("tokenName") or "").upper()
            if token_index != expected_index and token_name != expected_name:
                continue
            try:
                total += Decimal(
                    str(position.get("size", position.get("availableSize", "0")) or "0")
                )
            except Exception:
                return None
        return max(Decimal("0"), total)

    def _clear_holding(self, *, status: str) -> None:
        self.state.status = status
        self.state.holding_side = ""
        self.state.buy_price = 0.0
        self.state.buy_size = 0.0
        self.state.sold_size = 0.0
        self.state.buy_client_order_id = ""
        self.state.buy_order_id = ""
        self.state.sell_target = 0.0
        self.state.sell_order_id = ""
        self.state.sell_client_order_id = ""
        self.state.sell_filled_size = 0.0
        self.state.exit_client_order_ids = []
        self.state.resolved_exit_client_order_ids = []
        self.state.hold_start = 0.0
        self.state.hold_start_epoch_ms = 0
        self.state.locked = False
        self.state.reconciliation_error = ""
        if hasattr(self.state, "_market_exit_reason"):
            delattr(self.state, "_market_exit_reason")

    # ─── Execution ───

    async def _execute_buy_immediate(self, side: str, asset_id: str, snapshot_asks: list,
                                      prev_bid: float, current_bid: float) -> bool:
        """Buy on OKX immediately using snapshot asks. Returns True if filled."""
        self.state.status = "holding"
        label = "YES" if side == "yes" else "NO"
        jump_c = round((current_bid - prev_bid) * 100)
        await self._notify(
            f"\u26a1 {label} \u8df3\u4ef7: {prev_bid:.3f}\u2192{current_bid:.3f} (+{jump_c}\u00a2)"
        )

        # Take first N levels from SNAPSHOT. Do not filter by Poly price:
        # Old mode uses snapshot liquidity to get in, then exits via IOC sell.
        levels = snapshot_asks[:self.config.ask_levels]
        if not levels:
            self._emit_market_event(
                "okx",
                "sniper_old.entry_decision",
                **self._telemetry_fields(),
                side=side,
                selected_asset_id=asset_id,
                previous_poly_bid=_display_price(prev_bid),
                current_poly_bid=_display_price(current_bid),
                decision="skip_no_selected_levels",
                configured_ask_levels=self.config.ask_levels,
                snapshot_asks=snapshot_asks,
            )
            self.log("No snapshot ask levels, skipping")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: \u65e0\u6df1\u5ea6")
            self.state.status = "monitoring"
            return False
        max_price = float(levels[-1][0])
        total_ask_size = sum(float(lvl[1]) for lvl in levels)

        self.log(f"Executing immediate buy: ask levels={len(levels)}, max_price={_display_price(max_price)}, poly={current_bid:.3f}")

        # Calculate buy size
        try:
            bal_resp = await self.client.get_balance()
            available = 0.0
            if str(bal_resp.get("code", "")) == "0":
                for entry in bal_resp.get("data", []):
                    if entry.get("oddsType") == "points":
                        available = float(entry.get("available", "0"))
                        break

            budget = available * (self.config.position_pct / 100.0)
            max_units = size_to_float(budget / max_price)
            buy_size = min(max_units, size_to_float(total_ask_size))

            if buy_size < 1:
                self._emit_market_event(
                    "okx",
                    "sniper_old.entry_decision",
                    **self._telemetry_fields(),
                    side=side,
                    selected_asset_id=asset_id,
                    previous_poly_bid=_display_price(prev_bid),
                    current_poly_bid=_display_price(current_bid),
                    decision="skip_size_below_minimum",
                    selected_asks=levels,
                    selected_best_ask=self._first_level_price(levels),
                    selected_max_price=_display_price(max_price),
                    total_selected_ask_size=total_ask_size,
                    available_balance=available,
                    position_pct=self.config.position_pct,
                    budget=budget,
                    calculated_buy_size=buy_size,
                )
                self.log(f"Insufficient balance or size too small: avail={available}, budget={budget}")
                await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: \u4f59\u989d\u4e0d\u8db3")
                self.state.status = "monitoring"
                return False

        except Exception as e:
            self._emit_audit_event(
                "sniper_old.entry_sizing_failed",
                level=logging.WARNING,
                **self._telemetry_fields(),
                side=side,
                selected_asset_id=asset_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            self.log(f"Balance error: {e}")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {e}")
            self.state.status = "monitoring"
            return False

        # Pre-allocate and retain the cloid before the write.  TIMEOUT and
        # invalid HTTP responses are reconciled against this exact identifier.
        price_str = format_okx_price(max_price)
        self._emit_market_event(
            "okx",
            "sniper_old.entry_decision",
            **self._telemetry_fields(),
            side=side,
            selected_asset_id=asset_id,
            previous_poly_bid=_display_price(prev_bid),
            current_poly_bid=_display_price(current_bid),
            jump_cents=format(
                (as_decimal(current_bid) - as_decimal(prev_bid)) * Decimal("100"),
                "f",
            ),
            decision="submit_ioc_buy",
            selected_asks=levels,
            selected_best_ask=self._first_level_price(levels),
            selected_max_price=price_str,
            total_selected_ask_size=total_ask_size,
            available_balance=available,
            position_pct=self.config.position_pct,
            budget=budget,
            calculated_buy_size=normalize_buy_size_str(buy_size),
            decision_wall_ns=time.time_ns(),
            decision_monotonic_ns=time.monotonic_ns(),
        )
        client_order_id = generate_client_order_id(self.client)
        self.state.holding_side = side
        self.state.buy_client_order_id = client_order_id
        if not self._persist("before-entry-submit"):
            return False
        outcome, observation, confirmation = await self._submit_ioc_and_confirm(
            asset_id=asset_id,
            side="buy",
            price=price_str,
            size=normalize_buy_size_str(buy_size),
            reduce_only=False,
            client_order_id=client_order_id,
        )
        if outcome == "rejected":
            msg = confirmation.response.get(
                "message", confirmation.response.get("msg", "Order rejected")
            )
            self.log(f"Buy order failed: {msg}")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {msg}")
            self.state.buy_client_order_id = ""
            self.state.holding_side = ""
            self.state.status = "monitoring"
            self._persist("entry-rejected")
            return False
        if outcome != "terminal" or observation is None:
            self._mark_reconciliation_failure(
                f"Entry order {client_order_id} state is unresolved"
            )
            await self._notify(
                "\ud83d\udea8 \u4e70\u5165\u8ba2\u5355\u72b6\u6001\u65e0\u6cd5\u786e\u8ba4, \u5df2\u9501\u5b9a\u7b49\u5f85\u5bf9\u8d26"
            )
            return False

        self._apply_entry_observation(observation)
        filled_size = self.state.buy_size
        fill_price = self.state.buy_price or max_price
        if filled_size < 1:
            self.log("Buy order got 0 fill — no liquidity below limit")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: \u65e0\u6df1\u5ea6")
            self.state.buy_client_order_id = ""
            self.state.buy_order_id = ""
            self.state.holding_side = ""
            self.state.status = "monitoring"
            self._persist("entry-zero-fill")
            return False

        # Buy success — store state for sell later
        self.state.buy_price = fill_price
        self.state.buy_size = filled_size
        self.state.sold_size = 0.0
        self.state.hold_start = time.monotonic()
        self.state.hold_start_epoch_ms = int(time.time() * 1000)
        if not self._persist("entry-filled"):
            return False

        self.log(f"Bought {label} @{_display_price(fill_price)} x{normalize_size_str(filled_size)}")
        await self._notify(f"\u2705 \u5df2\u4e70\u5165 {label} @{_display_price(fill_price)} x{normalize_size_str(filled_size)}")
        return True

    async def _post_sell_at_price(self, poly_new_price: float):
        """Post sell order at the settled Poly price."""
        side = self.state.holding_side
        label = "YES" if side == "yes" else "NO"
        asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
        fill_price = self.state.buy_price
        filled_size = self.state.buy_size

        sell_price_str = format_okx_price(poly_new_price, "sell")
        self.state.sell_target = float(sell_price_str)
        sell_size_str = normalize_size_str(filled_size)
        client_order_id = generate_client_order_id(self.client)
        self.state.sell_client_order_id = client_order_id
        if not self._persist("before-protective-sell-submit"):
            return False
        confirmation = await place_and_confirm(
            self.client,
            client_order_id=client_order_id,
            market_id=self.state.market_id,
            asset_id=asset_id,
            side="sell",
            price=sell_price_str,
            size=sell_size_str,
            tif="gtc",
            reduce_only=True,
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        observation = confirmation.observation
        if confirmation.rejected:
            self.state.sell_client_order_id = ""
            self._persist("protective-sell-rejected")
            message = confirmation.response.get(
                "message", confirmation.response.get("msg", "Order rejected")
            )
            self.log(f"Protective sell rejected: {message}; exiting at market")
            return await self._handle_timeout(asset_id, label)
        if confirmation.unresolved or observation is None:
            cancel_confirmation = await cancel_and_confirm(
                self.client,
                client_order_id=client_order_id,
                order_id="",
                market_id=self.state.market_id,
                asset_id=asset_id,
                attempts=self._order_confirmation_attempts,
                delay=self._order_confirmation_delay,
            )
            if (
                not cancel_confirmation.confirmed_terminal
                or cancel_confirmation.observation is None
            ):
                self._mark_reconciliation_failure(
                    f"Protective sell {client_order_id} state is unresolved"
                )
                await self._notify(
                    "\ud83d\udea8 \u4fdd\u62a4 Sell \u72b6\u6001\u65e0\u6cd5\u786e\u8ba4, \u5df2\u9501\u5b9a\u7b49\u5f85\u5bf9\u8d26"
                )
                return False
            observation = cancel_confirmation.observation

        status = normalise_order_status(observation.status)
        self.state.sell_order_id = observation.order_id
        cumulative = max(Decimal("0"), Decimal(observation.filled_size))
        self.state.sell_filled_size = float(cumulative)
        self.state.sold_size = float(cumulative)
        if not self._persist("protective-sell-observed"):
            return False
        if status == "ACTIVE":
            self.log(f"Posted sell @{sell_price_str} x{sell_size_str}")
            await self._notify(f"\ud83d\udce4 \u5df2\u6302 Sell {label} @{sell_price_str} x{normalize_size_str(filled_size)}")
            self._hold_task = asyncio.create_task(self._holding_loop())
            return True

        if self._remaining_size() <= 0:
            return await self._finish_verified_exit(label)
        self.log(f"Protective sell reached {status}; exiting remaining position")
        return await self._handle_timeout(asset_id, label)

    async def _execute_buy(self, side: str, poly_new_price: float, initial_price: float = 0.0):
        """Buy on OKX immediately. LEGACY — kept for compatibility but new flow uses _execute_buy_immediate + _post_sell_at_price."""
        asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
        snapshot_asks = self.state.okx_snapshot_asks
        if not snapshot_asks:
            self.log("No snapshot available, trying live orderbook")
            snapshot_asks = await self._snapshot_okx_asks(asset_id)
        prev = initial_price if initial_price > 0 else (
            self.state.last_yes_bid if side == "yes" else self.state.last_no_bid
        )
        bought = await self._execute_buy_immediate(
            side, asset_id, snapshot_asks, prev, poly_new_price
        )
        if bought:
            label = "YES" if side == "yes" else "NO"
            await self._market_exit(asset_id, label, reason="post-buy")

    # ─── Holding ───

    async def _holding_loop(self):
        """Monitor sell order until filled or timeout."""
        try:
            asset_id = self.state.yes_asset_id if self.state.holding_side == "yes" else self.state.no_asset_id
            label = "YES" if self.state.holding_side == "yes" else "NO"

            while self._running and self.state.status == "holding":
                await asyncio.sleep(5)
                observation = await observe_order(
                    self.client,
                    client_order_id=self.state.sell_client_order_id,
                    order_id=self.state.sell_order_id,
                    market_id=self.state.market_id,
                )
                if observation is not None:
                    if observation.order_id:
                        self.state.sell_order_id = observation.order_id
                    cumulative = max(Decimal("0"), Decimal(observation.filled_size))
                    previous = Decimal(str(self.state.sell_filled_size))
                    if cumulative > previous:
                        self.state.sold_size = float(
                            Decimal(str(self.state.sold_size)) + cumulative - previous
                        )
                        self.state.sell_filled_size = float(cumulative)
                        if not self._persist("protective-sell-fill-update"):
                            return
                    status = normalise_order_status(observation.status)
                    if status in TERMINAL_ORDER_STATUSES:
                        self.state.sell_client_order_id = ""
                        self.state.sell_order_id = ""
                        if not self._persist("protective-sell-terminal"):
                            return
                        if self._remaining_size() <= 0:
                            await self._finish_verified_exit(label)
                        else:
                            await self._handle_timeout(asset_id, label)
                        return

                # Check timeout
                elapsed = time.monotonic() - self.state.hold_start
                if elapsed >= self.config.timeout_secs:
                    await self._handle_timeout(asset_id, label)
                    return

        except asyncio.CancelledError:
            raise
        finally:
            if self._hold_task is asyncio.current_task():
                self._hold_task = None

    async def _market_exit(self, asset_id: str, label: str, reason: str = "market"):
        """Exit immediately at current OKX best bid using IOC sell."""
        self.state._market_exit_reason = reason
        self.log(f"Market exit ({reason}) — selling at best bid")
        return await self._handle_timeout(asset_id, label)

    async def _handle_timeout(self, asset_id: str, label: str):
        """Exit with exact-cloid IOC orders until the verified position is flat."""
        async with self._exit_lock:
            self.log(f"Market/timeout exit ({self.config.timeout_secs}s config) — attempting IOC sell")
            if not await self._cancel_tracked_sell():
                return False

            for attempt in range(5):
                remaining = self._remaining_size()
                if remaining <= 0:
                    if await self._finish_verified_exit(label):
                        return True
                    if self.state.locked:
                        return False
                    remaining = self._remaining_size()

                if attempt > 0:
                    await asyncio.sleep(1)
                book_started_ns = time.perf_counter_ns()
                query_asset = self._okx_query_asset(asset_id)
                try:
                    book = await self.client.get_orderbook(
                        query_asset, depth=5
                    )
                    received_wall_ns = time.time_ns()
                    received_monotonic_ns = time.monotonic_ns()
                    if not isinstance(book, dict) or str(book.get("code", "")) != "0":
                        raise RuntimeError("orderbook query failed")
                    book_data = book.get("data", [{}])
                    if not isinstance(book_data, list) or not book_data:
                        raise RuntimeError("invalid orderbook response")
                    bids = (
                        self._invert_bids_from_yes_book(book_data[0].get("asks", []))
                        if asset_id == self.state.no_asset_id
                        else book_data[0].get("bids", [])
                    )
                    self._emit_market_event(
                        "okx",
                        "sniper_old.rest_book",
                        **self._telemetry_fields(),
                        purpose="exit",
                        exit_reason=getattr(
                            self.state, "_market_exit_reason", "timeout"
                        ),
                        attempt=attempt + 1,
                        query_asset_id=query_asset,
                        selected_asset_id=asset_id,
                        selected_side=self.state.holding_side,
                        source_timestamp=book_data[0].get("ts", ""),
                        received_wall_ns=received_wall_ns,
                        received_monotonic_ns=received_monotonic_ns,
                        duration_ms=self._elapsed_ms(book_started_ns),
                        raw_book=book_data[0],
                        yes_bids=book_data[0].get("bids", []),
                        yes_asks=book_data[0].get("asks", []),
                        yes_best_bid=self._first_level_price(
                            book_data[0].get("bids", [])
                        ),
                        yes_best_ask=self._first_level_price(
                            book_data[0].get("asks", [])
                        ),
                        selected_bids=bids,
                        selected_best_bid=self._first_level_price(bids),
                        derived_from_yes_book=asset_id == self.state.no_asset_id,
                        remaining_size=format(remaining, "f"),
                    )
                    if not bids:
                        raise RuntimeError("OKX has no executable bid")
                    best_bid = float(bids[0][0])
                except Exception as exc:
                    self._emit_audit_event(
                        "sniper_old.exit_book_failed",
                        level=logging.WARNING,
                        **self._telemetry_fields(),
                        exit_reason=getattr(
                            self.state, "_market_exit_reason", "timeout"
                        ),
                        attempt=attempt + 1,
                        query_asset_id=query_asset,
                        selected_asset_id=asset_id,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        duration_ms=self._elapsed_ms(book_started_ns),
                    )
                    self.log(f"Exit book attempt {attempt + 1}/5 failed: {exc}")
                    continue

                self._emit_market_event(
                    "okx",
                    "sniper_old.exit_decision",
                    **self._telemetry_fields(),
                    exit_reason=getattr(
                        self.state, "_market_exit_reason", "timeout"
                    ),
                    attempt=attempt + 1,
                    selected_asset_id=asset_id,
                    selected_side=self.state.holding_side,
                    decision="submit_ioc_sell",
                    selected_best_bid=_display_price(best_bid),
                    buy_price=_display_price(self.state.buy_price),
                    remaining_size=format(remaining, "f"),
                    decision_wall_ns=time.time_ns(),
                    decision_monotonic_ns=time.monotonic_ns(),
                )
                client_order_id = generate_client_order_id(self.client)
                self.state.exit_client_order_ids.append(client_order_id)
                if not self._persist("before-exit-submit"):
                    return False
                exit_price_str = format_okx_price(best_bid)
                outcome, observation, confirmation = await self._submit_ioc_and_confirm(
                    asset_id=asset_id,
                    side="sell",
                    price=exit_price_str,
                    size=normalize_size_str(remaining),
                    reduce_only=True,
                    client_order_id=client_order_id,
                )
                if outcome == "unresolved":
                    self._mark_reconciliation_failure(
                        f"Exit order {client_order_id} state is unresolved"
                    )
                    await self._notify(
                        "🚨 退出订单状态无法确认, 已停止并锁定等待对账"
                    )
                    return False
                if outcome == "rejected":
                    message = confirmation.response.get(
                        "message", confirmation.response.get("msg", "Order rejected")
                    )
                    self.log(
                        f"Exit order rejected (attempt {attempt + 1}/5): {message}"
                    )
                    continue

                exit_filled = self._apply_exit_observation(
                    client_order_id, observation
                )
                if exit_filled > 0:
                    profit = float(exit_filled) * (best_bid - self.state.buy_price)
                    self.log(
                        f"Exit @{exit_price_str} x{normalize_size_str(exit_filled)} "
                        f"| {profit:+.2f} xp | remaining={normalize_size_str(self._remaining_size())}"
                    )
                    exit_reason = getattr(self.state, "_market_exit_reason", "timeout")
                    prefix = "⏰ 超时出" if exit_reason == "timeout" else "⚡ 市价出"
                    await self._notify(
                        f"{prefix} @{exit_price_str} x{normalize_size_str(exit_filled)} "
                        f"| {profit:+.2f} xp"
                    )
                else:
                    self.log(f"Attempt {attempt + 1}/5: terminal 0 fill")

                if self._remaining_size() <= 0 and await self._finish_verified_exit(label):
                    return True

            # A delayed external fill can make the position flat even when no
            # individual attempt observed a fill. Verify once before locking.
            if await self._finish_verified_exit(label):
                return True
            if not self.state.locked:
                self._mark_reconciliation_failure(
                    f"Exit incomplete after 5 terminal attempts; {self._remaining_size()} remains"
                )
            await self._notify(
                "🚨 卖出5次后仍有仓位, 已停止并锁定等待人工介入"
            )
            return False

    async def _finish_verified_exit(self, label: str) -> bool:
        live_position = None
        for attempt in range(5):
            live_position = await self._matching_position_size()
            if live_position is None:
                self._mark_reconciliation_failure(
                    "Exit fills were observed but the final OKX position could not be verified"
                )
                return False
            if live_position <= 0:
                break
            if attempt < 4:
                await asyncio.sleep(0.2)
        if live_position is None:
            return False
        if live_position > 0:
            self.state.buy_size = float(
                Decimal(str(self.state.sold_size)) + live_position
            )
            return False

        total_sold = self.state.sold_size
        self.log(
            f"{label} position verified flat after selling "
            f"{normalize_size_str(total_sold)}"
        )
        next_status = (
            "monitoring" if self._running and not self._stop_requested else "stopped"
        )
        self._clear_holding(status=next_status)
        self._persist("position-verified-flat", fail_closed=False)
        if next_status == "monitoring":
            self.log("Resumed monitoring")
        return True

    def _reset_to_monitoring(self):
        """Reset state back to monitoring."""
        self._clear_holding(status="monitoring")
        self.log("Resumed monitoring")

    # ─── Helpers ───

    async def _get_poly_bid(self, token_id: str) -> Optional[float]:
        """Fetch current Poly best bid."""
        try:
            async with aiohttp.ClientSession() as sess:
                url = POLY_CLOB_BOOK.format(token_id=token_id)
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        bids = data.get("bids", [])
                        if bids:
                            bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
                            return float(bids[0].get("price", 0))
        except Exception as e:
            logger.debug(f"[Sniper] Poly bid fetch error: {e}")
        return None

    async def _notify(self, msg: str):
        """Send TG notification."""
        full = f"[{self.account_name or self.account_id}] {msg}"
        if self.notifier:
            try:
                await self.notifier.send(full)
            except Exception as e:
                logger.warning(f"[Sniper] Notify failed: {e}")

    @staticmethod
    def _restore_number(data: dict, key: str, *, integer: bool = False):
        value = data.get(key, 0)
        if isinstance(value, bool):
            raise ValueError(f"{key} must be numeric")
        return int(value) if integer else float(value)

    async def _reconcile_restored_ledger(self) -> bool:
        """Resolve every durable cloid and verify the matching live position."""
        if self.state.buy_client_order_id:
            if not self.state.holding_side:
                self._mark_reconciliation_failure(
                    "Persisted entry cloid has no holding_side"
                )
                return False
            if not await self._reconcile_entry_order():
                return False

        for client_order_id in list(self.state.exit_client_order_ids):
            if client_order_id in self.state.resolved_exit_client_order_ids:
                continue
            observation = await observe_order(
                self.client,
                client_order_id=client_order_id,
                market_id=self.state.market_id,
            )
            if observation is None or normalise_order_status(
                observation.status
            ) not in TERMINAL_ORDER_STATUSES:
                confirmation = await cancel_and_confirm(
                    self.client,
                    client_order_id=client_order_id,
                    order_id=observation.order_id if observation else "",
                    market_id=self.state.market_id,
                    asset_id=self._holding_asset_id(),
                    attempts=self._order_confirmation_attempts,
                    delay=self._order_confirmation_delay,
                )
                if (
                    not confirmation.confirmed_terminal
                    or confirmation.observation is None
                ):
                    self._mark_reconciliation_failure(
                        f"Exit order {client_order_id} could not be reconciled"
                    )
                    return False
                observation = confirmation.observation
            self._apply_exit_observation(client_order_id, observation)

        if self.state.sell_client_order_id or self.state.sell_order_id:
            observation = await observe_order(
                self.client,
                client_order_id=self.state.sell_client_order_id,
                order_id=self.state.sell_order_id,
                market_id=self.state.market_id,
            )
            status = normalise_order_status(observation.status) if observation else ""
            if status in TERMINAL_ORDER_STATUSES:
                cumulative = max(Decimal("0"), Decimal(observation.filled_size))
                previous = Decimal(str(self.state.sell_filled_size))
                if cumulative > previous:
                    self.state.sold_size = float(
                        Decimal(str(self.state.sold_size)) + cumulative - previous
                    )
                self.state.sell_filled_size = float(cumulative)
                self.state.sell_client_order_id = ""
                self.state.sell_order_id = ""
            elif status != "ACTIVE":
                if not await self._cancel_tracked_sell():
                    return False
            elif observation.order_id:
                self.state.sell_order_id = observation.order_id

        live_position = await self._matching_position_size()
        if live_position is None:
            self._mark_reconciliation_failure(
                "Restored Sniper Old position could not be verified"
            )
            return False
        if live_position > 0:
            if not self.state.holding_side:
                self._mark_reconciliation_failure(
                    "Live position exists but persisted holding_side is missing"
                )
                return False
            self.state.buy_size = float(
                Decimal(str(self.state.sold_size)) + live_position
            )
            self.state.status = "holding"
            self.state.locked = True
            self.state.reconciliation_error = "Recovery reconciled; resume is pending"
            return True

        if self.state.sell_client_order_id or self.state.sell_order_id:
            if not await self._cancel_tracked_sell():
                return False
        self._clear_holding(status="stopped")
        return True

    async def restore(self, snapshot: dict, *, resume: bool = False) -> dict:
        """Restore a durable ledger; never submit a new entry during phase one."""
        if not isinstance(snapshot, dict):
            return {"ok": False, "error": "Invalid Sniper Old snapshot"}
        config_data = snapshot.get("config", snapshot)
        runtime = snapshot.get("runtime", snapshot)
        if not isinstance(config_data, dict) or not isinstance(runtime, dict):
            return {"ok": False, "error": "Invalid Sniper Old snapshot objects"}

        self._running = False
        self._stop_requested = False
        self._restoring = True
        try:
            self.config = SniperOldConfig(
                threshold_cents=self._restore_number(
                    config_data, "threshold_cents", integer=True
                ),
                ask_levels=self._restore_number(config_data, "ask_levels", integer=True),
                position_pct=self._restore_number(
                    config_data, "position_pct", integer=True
                ),
                timeout_secs=self._restore_number(
                    config_data, "timeout_secs", integer=True
                ),
                settle_ms=self._restore_number(config_data, "settle_ms", integer=True),
                window_ms=self._restore_number(config_data, "window_ms", integer=True),
            )
            state = SniperOldState(
                status=str(runtime.get("status") or "stopped"),
                yes_token_id=str(runtime.get("yes_token_id") or ""),
                no_token_id=str(runtime.get("no_token_id") or ""),
                yes_asset_id=str(runtime.get("yes_asset_id") or ""),
                no_asset_id=str(runtime.get("no_asset_id") or ""),
                market_id=str(runtime.get("market_id") or ""),
                poly_url=str(runtime.get("poly_url") or ""),
                selected_outcome=str(runtime.get("selected_outcome") or ""),
                last_yes_bid=self._restore_number(runtime, "last_yes_bid"),
                last_no_bid=self._restore_number(runtime, "last_no_bid"),
                holding_side=str(runtime.get("holding_side") or "").lower(),
                buy_price=self._restore_number(runtime, "buy_price"),
                buy_size=self._restore_number(runtime, "buy_size"),
                sold_size=self._restore_number(runtime, "sold_size"),
                buy_client_order_id=str(runtime.get("buy_client_order_id") or ""),
                buy_order_id=str(runtime.get("buy_order_id") or ""),
                sell_target=self._restore_number(runtime, "sell_target"),
                sell_order_id=str(runtime.get("sell_order_id") or ""),
                sell_client_order_id=str(
                    runtime.get("sell_client_order_id") or ""
                ),
                sell_filled_size=self._restore_number(runtime, "sell_filled_size"),
                exit_client_order_ids=[
                    str(value)
                    for value in runtime.get("exit_client_order_ids", [])
                    if value
                ],
                resolved_exit_client_order_ids=[
                    str(value)
                    for value in runtime.get("resolved_exit_client_order_ids", [])
                    if value
                ],
                hold_start_epoch_ms=self._restore_number(
                    runtime, "hold_start_epoch_ms", integer=True
                ),
                locked=True,
                reconciliation_error="Startup reconciliation in progress",
            )
            if state.holding_side not in {"", "yes", "no"}:
                raise ValueError("holding_side must be yes or no")
            if any(
                value < 0
                for value in (state.buy_price, state.buy_size, state.sold_size)
            ):
                raise ValueError("trade ledger values cannot be negative")
            has_trade_ledger = bool(
                state.holding_side
                or state.buy_client_order_id
                or state.sell_client_order_id
                or state.sell_order_id
                or state.exit_client_order_ids
                or state.buy_size > 0
            )
            has_config = all(
                (
                    state.yes_token_id,
                    state.no_token_id,
                    state.yes_asset_id,
                    state.no_asset_id,
                    state.market_id,
                )
            )
            if (has_trade_ledger or resume) and not has_config:
                raise ValueError("Persisted Sniper Old market/token mapping is incomplete")
            self.state = state
            if state.hold_start_epoch_ms:
                elapsed = max(
                    0.0,
                    time.time() - state.hold_start_epoch_ms / 1000.0,
                )
                state.hold_start = max(0.0, time.monotonic() - elapsed)

            if has_trade_ledger and not await self._reconcile_restored_ledger():
                return {
                    "ok": False,
                    "error": self.state.reconciliation_error,
                    "needs_reconciliation": True,
                    "locked": True,
                }
            if not has_trade_ledger:
                self._clear_holding(status="stopped")

            if not resume:
                # Phase one can correct fills, order ids, remaining size, or
                # clear a stale ledger.  Make that correction durable before
                # account-wide safety checks are allowed to continue.  Keep
                # the saved run marker so a crash between phase one and phase
                # two does not silently disable an intended resume.
                self._restoring = False
                if not self._persist(
                    "restore-phase-one",
                    running_override=bool(snapshot.get("running", False)),
                ):
                    return {
                        "ok": False,
                        "error": self.state.reconciliation_error,
                        "needs_reconciliation": True,
                        "locked": True,
                    }
                return {
                    "ok": True,
                    "resumed": False,
                    "holding": self.state.status == "holding",
                }

            self.state.locked = False
            self.state.reconciliation_error = ""
            self._running = True
            self._telemetry_run_id = f"{self.account_id}-{time.time_ns()}"
            if self.state.status == "holding":
                if self.state.sell_client_order_id or self.state.sell_order_id:
                    self._hold_task = asyncio.create_task(self._holding_loop())
                else:
                    asset_id = self._holding_asset_id()
                    label = "YES" if self.state.holding_side == "yes" else "NO"
                    self._hold_task = asyncio.create_task(
                        self._handle_timeout(asset_id, label)
                    )
            else:
                self.state.status = "monitoring"
                self._task = asyncio.create_task(self._monitor_loop())
        except (TypeError, ValueError) as exc:
            self._running = False
            self.state.status = "error"
            self.state.locked = True
            self.state.reconciliation_error = str(exc)
            return {"ok": False, "error": str(exc), "locked": True}
        except Exception as exc:
            self._mark_reconciliation_failure(
                f"Sniper Old restore reconciliation failed: {type(exc).__name__}: {exc}"
            )
            return {
                "ok": False,
                "error": self.state.reconciliation_error,
                "needs_reconciliation": True,
                "locked": True,
            }
        finally:
            self._restoring = False

        if not self._persist("restore-resume"):
            return {
                "ok": False,
                "error": self.state.reconciliation_error,
                "locked": True,
            }
        return {"ok": True, "resumed": True, "holding": self.state.status == "holding"}

    def get_state(self) -> dict:
        # Report settling as a sub-status
        effective_status = self.state.status
        if self.state.settling:
            effective_status = "settling"
        return {
            "running": self._running,
            "status": effective_status,
            "locked": self.state.locked,
            "reconciliation_error": self.state.reconciliation_error,
            "settling": {
                "side": self.state.settle_side,
                "initial": self.state.settle_initial,
                "current": self.state.settle_price,
                "elapsed_ms": int((time.monotonic() - self.state.settle_last_update) * 1000) if self.state.settle_last_update else 0,
            } if self.state.settling else None,
            "config": {
                "threshold_cents": self.config.threshold_cents,
                "ask_levels": self.config.ask_levels,
                "position_pct": self.config.position_pct,
                "timeout_secs": self.config.timeout_secs,
                "settle_ms": self.config.settle_ms,
                "window_ms": self.config.window_ms,
            },
            "holding": {
                "side": self.state.holding_side,
                "buy_price": self.state.buy_price,
                "buy_size": self.state.buy_size,
                "sold_size": self.state.sold_size,
                "remaining_size": float(self._remaining_size()),
                "buy_client_order_id": self.state.buy_client_order_id,
                "sell_client_order_id": self.state.sell_client_order_id,
                "exit_client_order_ids": list(self.state.exit_client_order_ids),
                "sell_target": self.state.sell_target,
                "elapsed": int(time.monotonic() - self.state.hold_start) if self.state.hold_start else 0,
            } if (self.state.holding_side or self.state.buy_client_order_id) else None,
            "last_yes_bid": self.state.last_yes_bid,
            "last_no_bid": self.state.last_no_bid,
            "yes_token_id": self.state.yes_token_id,
            "no_token_id": self.state.no_token_id,
            "yes_asset_id": self.state.yes_asset_id,
            "no_asset_id": self.state.no_asset_id,
            "poly_url": getattr(self.state, "poly_url", ""),
            "selected_outcome": getattr(self.state, "selected_outcome", ""),
            "logs": self.state.logs[-20:],
        }
