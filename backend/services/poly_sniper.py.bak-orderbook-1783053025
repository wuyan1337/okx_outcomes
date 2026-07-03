"""Poly Sniper — cross-market latency arbitrage.

Monitors Polymarket YES and NO tokens simultaneously via WS.
When either token's best bid jumps by >= threshold in a single tick,
instantly buys on OKX (ioc) and sells at the new Poly price (gtc).

States: IDLE → MONITORING → TRIGGERED → HOLDING → back to MONITORING or STOP
"""
import asyncio
import logging
import time
from typing import Optional, Dict
from dataclasses import dataclass, field
from .size_utils import normalize_size_str, normalize_buy_size_str, size_to_float

import aiohttp

logger = logging.getLogger(__name__)

POLY_CLOB_BOOK = "https://clob.polymarket.com/book?token_id={token_id}"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class SniperConfig:
    threshold_cents: int = 40       # Jump threshold in cents
    buy_limit_cents: int = 50    # Max buy price in cents
    position_pct: int = 100         # Balance percentage to use
    timeout_secs: int = 120         # Seconds before forced exit
    settle_ms: int = 0           # Ms to wait for price to stabilize after jump
    window_ms: int = 150            # Rolling window for cumulative jump detection


@dataclass
class SniperState:
    status: str = "idle"            # idle, monitoring, holding, stopped
    # Token info
    yes_token_id: str = ""          # Poly YES token
    no_token_id: str = ""           # Poly NO token
    yes_asset_id: str = ""          # OKX YES asset
    no_asset_id: str = ""           # OKX NO asset
    market_id: str = ""
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
    sell_target: float = 0.0        # Target sell price (Poly jump price)
    sell_order_id: str = ""         # OKX sell order ID
    hold_start: float = 0.0        # When we entered holding
    # Logs
    logs: list = field(default_factory=list)


class PolySniper:
    """Cross-market latency arbitrage between Poly and OKX."""

    def __init__(self, client, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.config = SniperConfig()
        self.state = SniperState()
        self.notifier = None
        self._task: Optional[asyncio.Task] = None
        self._hold_task: Optional[asyncio.Task] = None
        self._running = False
        self._ws_calibrated = {"yes": False, "no": False}
        self._price_window: Dict[str, list] = {"yes": [], "no": []}  # [(timestamp, price), ...]
        self._baseline: Dict[str, float] = {"yes": 0.0, "no": 0.0}  # Tracks high watermark per side
        self._last_no_trigger_log: Dict[str, float] = {"yes": 0.0, "no": 0.0}  # Rate-limit no-trigger logs

    @property
    def running(self) -> bool:
        return self._running

    def log(self, msg: str):
        from datetime import datetime
        entry = f"[{datetime.now().strftime('%I:%M:%S %p')}] {msg}"
        self.state.logs.append(entry)
        if len(self.state.logs) > 100:
            self.state.logs = self.state.logs[-100:]
        logger.info(f"[Sniper:{self.account_id}] {msg}")

    async def start(self, yes_token_id: str, no_token_id: str,
                    yes_asset_id: str, no_asset_id: str, market_id: str,
                    threshold_cents: int = 40, buy_limit_cents: int = 50,
                    position_pct: int = 100, timeout_secs: int = 120,
                    settle_ms: int = 0, window_ms: int = 150,
                    poly_url: str = "", selected_outcome: str = ""):
        """Start sniper monitoring."""
        if self._running:
            return {"ok": False, "error": "Already running"}

        self.config.threshold_cents = threshold_cents
        self.config.buy_limit_cents = buy_limit_cents
        self.config.position_pct = position_pct
        self.config.timeout_secs = timeout_secs
        self.config.settle_ms = settle_ms
        self.config.window_ms = window_ms

        self._price_window = {"yes": [], "no": []}
        self._baseline = {"yes": 0.0, "no": 0.0}
        self._last_no_trigger_log = {"yes": 0.0, "no": 0.0}
        self.state = SniperState(
            status="monitoring",
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            yes_asset_id=yes_asset_id,
            no_asset_id=no_asset_id,
            market_id=market_id,
        )
        self.state.poly_url = poly_url
        self.state.selected_outcome = selected_outcome

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
        self._task = asyncio.create_task(self._monitor_loop())
        self.log(f"Started: threshold={threshold_cents}¢, buy_limit={buy_limit_cents}¢, pct={position_pct}%, settle={settle_ms}ms, window={window_ms}ms")
        return {"ok": True}

    async def stop(self):
        """Stop sniper."""
        self._running = False
        self.state.status = "stopped"
        self.state.settling = False
        # Cancel open sell order if holding
        if self.state.sell_order_id and self.state.holding_side:
            asset_id = self.state.yes_asset_id if self.state.holding_side == "yes" else self.state.no_asset_id
            try:
                await self.client.cancel_order(asset_id=asset_id, order_id=self.state.sell_order_id)
                self.log(f"Cancelled sell order {self.state.sell_order_id}")
            except Exception as e:
                self.log(f"Cancel sell on stop failed: {e}")
        if self._hold_task:
            self._hold_task.cancel()
            try:
                await self._hold_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self.log("Stopped")

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
                            for data in items:
                                evt = data.get("event_type", "")
                                if evt == "book":
                                    await self._process_book_update(data)
                                elif not hasattr(self, '_dbg_logged'):
                                    # Log first non-book message for debugging
                                    self._dbg_logged = True
                                    self.log(f"WS msg event_type={evt}, keys={list(data.keys())[:8]}")
                        except Exception:
                            pass
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

    async def _process_book_update(self, data: dict):
        """Check if a jump occurred. Uses settle window to capture full move."""
        asset_id = data.get("asset_id", "")
        bids = data.get("bids", [])
        if not bids:
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
            return

        # If currently settling, update settle price to latest value
        if self.state.settling and side == self.state.settle_side:
            if current_bid != self.state.settle_price:
                self.state.settle_price = current_bid
                self.state.settle_last_update = time.monotonic()
                self.log(f"Settle update: {side.upper()} now {current_bid:.3f}")
            return

        # Ignore updates during settling for the other side
        if self.state.settling:
            return

        if prev_bid <= 0:
            return

        # Skip first WS update per side (calibration — avoids false trigger from stale REST data)
        if not self._ws_calibrated.get(side, False):
            self._ws_calibrated[side] = True
            self._baseline[side] = current_bid  # Set initial baseline
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
        jump_cents_raw = (current_bid - window_low) * 100
        jump_cents = round(jump_cents_raw)
        if jump_cents < self.config.threshold_cents:
            # Explain non-actions without flooding logs: log near misses or once every ~2s per side.
            last_log = self._last_no_trigger_log.get(side, 0.0)
            near_miss = jump_cents >= max(1, self.config.threshold_cents - 3)
            if near_miss or now - last_log >= 2.0:
                self._last_no_trigger_log[side] = now
                needed = self.config.threshold_cents - jump_cents
                self.log(
                    f"NO TRIGGER: {side.upper()} {window_low:.3f}→{current_bid:.3f} "
                    f"(+{jump_cents_raw:.1f}¢/{jump_cents}¢) < threshold {self.config.threshold_cents}¢ "
                    f"(need +{needed}¢, window={self.config.window_ms}ms, points={len(window)})"
                )
        if jump_cents >= self.config.threshold_cents:
            self._price_window[side] = []  # Reset window after trigger
            # Bounce filter: if current price is far below baseline, this is a dead cat bounce during crash
            baseline = self._baseline[side]
            if baseline > 0 and current_bid < baseline * 0.7:
                self.log(f"BOUNCE SKIP: {side.upper()} {window_low:.3f}→{current_bid:.3f} (+{jump_cents}¢) but current {current_bid:.3f} < baseline {baseline:.3f}*0.7={baseline*0.7:.3f}")
                return
            self.log(f"JUMP detected: {side.upper()} {window_low:.3f} → {current_bid:.3f} (+{jump_cents}¢) within {self.config.window_ms}ms, buying immediately...")
            # Snapshot OKX orderbook to check if arb still exists
            okx_asset = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
            snapshot_asks = await self._snapshot_okx_asks(okx_asset)
            if not snapshot_asks:
                self.log("OKX snapshot failed — no asks, skipping")
                return
            snapshot_best_ask = float(snapshot_asks[0][0])
            if snapshot_best_ask >= current_bid:
                self.log(f"OKX already caught up: ask={snapshot_best_ask:.2f} >= poly={current_bid:.3f}, skipping")
                return
            self.log(f"OKX snapshot: best ask={snapshot_best_ask:.2f} (poly={current_bid:.3f}, spread={round((current_bid - snapshot_best_ask)*100)}¢)")
            # BUY at buy_limit price
            bought = await self._execute_buy_immediate(side, okx_asset, window_low, current_bid)
            if not bought:
                return

            # THEN settle — watch Poly for final stable price to determine sell target
            self.state.settling = True
            self.state.settle_side = side
            self.state.settle_price = current_bid
            self.state.settle_initial = prev_bid
            self.state.settle_last_update = time.monotonic()
            # Start settle timer task (will post sell when done)
            asyncio.create_task(self._settle_then_sell())

    async def _settle_then_sell(self):
        """After buying, wait for Poly price to stabilize, then post sell at final price."""
        settle_sec = self.config.settle_ms / 1000.0
        while self._running and self.state.settling:
            elapsed = time.monotonic() - self.state.settle_last_update
            if elapsed >= settle_sec:
                # Price stable
                final_price = self.state.settle_price
                initial_price = self.state.settle_initial
                side = self.state.settle_side
                self.state.settling = False
                self.log(f"Settled: {side.upper()} final={final_price:.3f} (from {initial_price:.3f})")
                # If price reverted below buy cost, stop immediately
                if final_price <= self.state.buy_price:
                    self.log(f"Settled price {final_price:.3f} <= buy cost {self.state.buy_price:.2f}, selling at best bid")
                    asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
                    label = "YES" if side == "yes" else "NO"
                    await self._notify(f"⚠️ settle价 {final_price:.3f} ≤ 成本 {self.state.buy_price:.2f}, 市价出")
                    await self._handle_timeout(asset_id, label)
                    return
                # Post sell at settled price
                await self._post_sell_at_price(final_price)
                return
            # Check every 50ms
            await asyncio.sleep(0.05)
        # If stopped during settling — still need to post sell at whatever we have
        if self.state.buy_size > 0 and self.state.status == "holding":
            self.state.settling = False
            # Use current settle_price as best available
            await self._post_sell_at_price(self.state.settle_price)

    async def _snapshot_okx_asks(self, asset_id: str) -> list:
        """Instantly grab OKX asks at trigger moment."""
        try:
            book = await self.client.get_orderbook(asset_id, depth=10)
            if str(book.get("code", "")) != "0":
                return []
            asks = book.get("data", [{}])[0].get("asks", [])
            return asks  # [[price, size], ...]
        except Exception as e:
            self.log(f"OKX snapshot error: {e}")
            return []

    # ─── Execution ───

    async def _execute_buy_immediate(self, side: str, asset_id: str,
                                      prev_bid: float, current_bid: float) -> bool:
        """Buy on OKX at buy_limit price. GTC sweeps all asks <= limit."""
        self.state.status = "holding"
        label = "YES" if side == "yes" else "NO"
        jump_c = round((current_bid - prev_bid) * 100)
        await self._notify(
            f"\u26a1 {label} \u8df3\u4ef7: {prev_bid:.3f}\u2192{current_bid:.3f} (+{jump_c}\u00a2)"
        )

        buy_limit = self.config.buy_limit_cents / 100.0

        self.log(f"Executing buy: limit={buy_limit:.2f}, poly={current_bid:.3f}")

        # Calculate buy size from balance
        try:
            bal_resp = await self.client.get_balance()
            available = 0.0
            if str(bal_resp.get("code", "")) == "0":
                for entry in bal_resp.get("data", []):
                    if entry.get("oddsType") == "points":
                        available = float(entry.get("available", "0"))
                        break

            budget = available * (self.config.position_pct / 100.0)
            buy_size = size_to_float(budget / buy_limit)

            if buy_size < 1:
                self.log(f"Insufficient balance: avail={available}, budget={budget}, limit={buy_limit}")
                await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: \u4f59\u989d\u4e0d\u8db3")
                self.state.status = "monitoring"
                return False

        except Exception as e:
            self.log(f"Balance error: {e}")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {e}")
            self.state.status = "monitoring"
            return False

        # Place GTC limit buy — sweeps all asks <= buy_limit
        price_str = f"{buy_limit:.2f}"
        try:
            resp = await self.client.place_order(
                asset_id=asset_id,
                side="buy",
                price=price_str,
                size=normalize_buy_size_str(buy_size),
                tif="gtc",
            )
            _buy_client_oid = resp.get("_clientOrderId", "") if isinstance(resp, dict) else ""
            if str(resp.get("code", "")) != "0":
                msg = resp.get("message", resp.get("msg", ""))
                self.log(f"Buy order failed: {msg}")
                await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {msg}")
                self.state.status = "monitoring"
                return False

        except Exception as e:
            self.log(f"Buy order exception: {e}")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {e}")
            self.state.status = "monitoring"
            return False

        # Check fill - poll open+closed orders (GTC may partially fill)
        filled_size = 0.0
        fill_price = buy_limit
        for _attempt in range(4):
            await asyncio.sleep(1)
            try:
                for _status in ("closed", "open"):
                    orders = await self.client.get_orders(status=_status)
                    if str(orders.get("code", "")) == "0":
                        for o in orders.get("data", {}).get("list", []):
                            if _buy_client_oid and o.get("clientOrderId") != _buy_client_oid:
                                continue
                            if (o.get("assetId") == asset_id and
                                o.get("side", "").upper() == "BUY" and
                                float(o.get("filledSize", "0") or "0") > 0):
                                filled_size = float(o.get("filledSize", "0") or "0")
                                filled_amt = float(o.get("filledAmount", "0") or "0")
                                fill_price = filled_amt / filled_size if filled_amt > 0 else buy_limit
                                break
                    if filled_size > 0:
                        break
                if filled_size > 0:
                    break
            except Exception as e:
                self.log(f"Fill check error: {e}")

        # Cancel any remaining open portion of the GTC buy
        try:
            await self.client.cancel_all([])
        except Exception:
            pass

        # Post-cancel final poll: catch last-second fills during cancel flight
        if filled_size < 1:
            await asyncio.sleep(1)
            try:
                for _status in ("closed", "open"):
                    orders = await self.client.get_orders(status=_status)
                    if str(orders.get("code", "")) == "0":
                        for o in orders.get("data", {}).get("list", []):
                            if _buy_client_oid and o.get("clientOrderId") != _buy_client_oid:
                                continue
                            if (o.get("assetId") == asset_id and
                                o.get("side", "").upper() == "BUY" and
                                float(o.get("filledSize", "0") or "0") > 0):
                                filled_size = float(o.get("filledSize", "0") or "0")
                                filled_amt = float(o.get("filledAmount", "0") or "0")
                                fill_price = filled_amt / filled_size if filled_amt > 0 else buy_limit
                                break
                        if filled_size > 0:
                            break
            except Exception as e:
                self.log(f"Post-cancel poll error: {e}")

        if filled_size < 1:
            self.log("Buy order got 0 fill — no liquidity below limit")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: \u65e0\u6df1\u5ea6")
            self.state.status = "monitoring"
            return False

        # Buy success — store state for sell later
        self.state.holding_side = side
        self.state.buy_price = fill_price
        self.state.buy_size = filled_size
        self.state.hold_start = time.monotonic()
        self.state.hold_start_epoch_ms = int(time.time() * 1000)

        self.log(f"Bought {label} @{fill_price:.2f} x{normalize_size_str(filled_size)}")
        await self._notify(f"\u2705 \u5df2\u4e70\u5165 {label} @{fill_price:.2f} x{normalize_size_str(filled_size)}")
        return True

    async def _post_sell_at_price(self, poly_new_price: float):
        """Post sell order at the settled Poly price."""
        side = self.state.holding_side
        label = "YES" if side == "yes" else "NO"
        asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id
        fill_price = self.state.buy_price
        filled_size = self.state.buy_size

        # Price tick: 0.01 for 0.04-0.96, 0.001 outside that range
        tick = 0.001 if (poly_new_price < 0.04 or poly_new_price > 0.96) else 0.01
        decimals = 3 if tick == 0.001 else 2
        sell_price_raw = round(poly_new_price, decimals)
        self.state.sell_target = sell_price_raw

        sell_price_str = f"{sell_price_raw:.{decimals}f}"
        sell_size_str = normalize_size_str(filled_size)
        try:
            sell_resp = await self.client.place_order(
                asset_id=asset_id,
                side="sell",
                price=sell_price_str,
                size=sell_size_str,
                tif="gtc",
            )
            if str(sell_resp.get("code", "")) == "0":
                self.state.sell_order_id = sell_resp.get("data", {}).get("oid", "") or sell_resp.get("data", {}).get("orderId", "")
                self.log(f"Posted sell @{sell_price_str} x{sell_size_str}")
                await self._notify(f"\ud83d\udce4 \u5df2\u6302 Sell {label} @{sell_price_str} x{normalize_size_str(filled_size)}")
            else:
                msg = sell_resp.get("message", sell_resp.get("msg", ""))
                self.log(f"Sell post failed: {msg}")
                await self._notify(f"\u26a0\ufe0f \u6302\u5356\u5931\u8d25: {msg}")
        except Exception as e:
            self.log(f"Sell order exception: {e}")
            await self._notify(f"\u26a0\ufe0f \u6302\u5356\u5f02\u5e38: {e}")

        # Start holding loop (timeout monitoring)
        self._hold_task = asyncio.create_task(self._holding_loop())

    async def _execute_buy(self, side: str, poly_new_price: float, initial_price: float = 0.0):
        """Buy on OKX immediately. LEGACY — kept for compatibility but new flow uses _execute_buy_immediate + _post_sell_at_price."""
        self.state.status = "holding"
        asset_id = self.state.yes_asset_id if side == "yes" else self.state.no_asset_id

        # Notify trigger
        label = "YES" if side == "yes" else "NO"
        prev = initial_price if initial_price > 0 else (self.state.last_yes_bid if side == "yes" else self.state.last_no_bid)
        jump_c = round((poly_new_price - prev) * 100)
        await self._notify(
            f"⚡ {label} 跳价: {prev:.3f}→{poly_new_price:.3f} (+{jump_c}¢)"
        )

        # Use OKX snapshot from trigger moment (NOT live orderbook)
        snapshot_asks = self.state.okx_snapshot_asks
        if not snapshot_asks:
            # Fallback: try live (shouldn't happen normally)
            self.log("No snapshot available, trying live orderbook")
            try:
                book = await self.client.get_orderbook(asset_id, depth=10)
                if str(book.get("code", "")) != "0":
                    self.log(f"Orderbook error: {book.get('msg','')}")
                    await self._notify(f"❌ 买入失败: OKX orderbook error")
                    self.state.status = "monitoring"
                    return
                snapshot_asks = book.get("data", [{}])[0].get("asks", [])
            except Exception as e:
                self.log(f"Orderbook fetch error: {e}")
                await self._notify(f"❌ 买入失败: {e}")
                self.state.status = "monitoring"
                return

        if not snapshot_asks:
            self.log("No asks available")
            await self._notify(f"❌ 买入失败: 无深度")
            self.state.status = "monitoring"
            return

        # Take levels from SNAPSHOT, only those cheaper than poly price
        levels = [l for l in snapshot_asks if float(l[0]) < poly_new_price]
        if not levels:
            self.log(f"No asks below poly price {poly_new_price:.3f}, skipping")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: \u65e0\u6df1\u5ea6")
            self.state.status = "monitoring"
            return
        # Max price = highest ask level from filtered snapshot
        max_price = float(levels[-1][0])
        # Total available size across snapshot levels
        total_ask_size = sum(float(lvl[1]) for lvl in levels)

        # Final check: if OKX has already moved past our snapshot price,
        # the IOC will simply get 0 fill (which we handle below).
        # Log the spread for visibility.
        spread_cents = round((poly_new_price - max_price) * 100)
        self.log(f"Executing: buy @<={max_price:.2f} (snapshot), sell target={poly_new_price:.3f}, spread={spread_cents}¢")

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
                self.log(f"Insufficient balance or size too small: avail={available}, budget={budget}")
                await self._notify(f"❌ 买入失败: 余额不足")
                self.state.status = "monitoring"
                return

        except Exception as e:
            self.log(f"Balance error: {e}")
            await self._notify(f"❌ 买入失败: {e}")
            self.state.status = "monitoring"
            return

        # Place IOC buy order (place_order returns txHash, track via orders list)
        price_str = f"{max_price:.2f}"
        try:
            resp = await self.client.place_order(
                asset_id=asset_id,
                side="buy",
                price=price_str,
                size=normalize_buy_size_str(buy_size),
                tif="gtc",
            )
            _buy_client_oid = resp.get("_clientOrderId", "") if isinstance(resp, dict) else ""
            if str(resp.get("code", "")) != "0":
                msg = resp.get("message", resp.get("msg", ""))
                self.log(f"Buy order failed: {msg}")
                await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {msg}")
                self.state.status = "monitoring"
                return

        except Exception as e:
            self.log(f"Buy order exception: {e}")
            await self._notify(f"\u274c \u4e70\u5165\u5931\u8d25: {e}")
            self.state.status = "monitoring"
            return

        # Check fill - poll closed orders (IOC settles on-chain, may take 1-3s)
        filled_size = 0.0
        fill_price = max_price
        for _attempt in range(4):
            await asyncio.sleep(1)
            try:
                orders = await self.client.get_orders(status="closed")
                if str(orders.get("code", "")) == "0":
                    for o in orders.get("data", {}).get("list", []):
                        # Match by clientOrderId for precision (avoid picking up old orders)
                        if _buy_client_oid and o.get("clientOrderId") != _buy_client_oid:
                            continue
                        if (o.get("assetId") == asset_id and
                            o.get("side", "").upper() == "BUY" and
                            float(o.get("filledSize", "0") or "0") > 0):
                            filled_size = float(o.get("filledSize", "0") or "0")
                            filled_amt = float(o.get("filledAmount", "0") or "0")
                            fill_price = filled_amt / filled_size if filled_amt > 0 else max_price
                            break
                if filled_size > 0:
                    break
            except Exception as e:
                self.log(f"Fill check error: {e}")

        if filled_size < 1:
            self.log("IOC order got 0 fill — OKX already moved or no liquidity")
            await self._notify(f"❌ 买入失败: 无深度")
            self.state.status = "monitoring"
            return

        # Buy success
        self.state.holding_side = side
        self.state.buy_price = fill_price
        self.state.buy_size = filled_size
        # Post sell at Poly new price (ensure above cost)
        # Price tick: 0.01 for 0.04-0.96, 0.001 outside that range
        tick = 0.001 if (poly_new_price < 0.04 or poly_new_price > 0.96) else 0.01
        decimals = 3 if tick == 0.001 else 2
        sell_price_raw = round(poly_new_price, decimals)
        if sell_price_raw <= fill_price:
            sell_price_raw = round(fill_price + tick, decimals)

        self.state.sell_target = sell_price_raw
        self.state.hold_start = time.monotonic()
        self.state.hold_start_epoch_ms = int(time.time() * 1000)

        self.log(f"Bought {label} @{fill_price:.2f} x{normalize_size_str(filled_size)}")
        await self._notify(f"✅ 已买入 {label} @{fill_price:.2f} x{normalize_size_str(filled_size)}")

        sell_price_str = f"{sell_price_raw:.{decimals}f}"
        sell_size_str = normalize_size_str(filled_size)
        try:
            sell_resp = await self.client.place_order(
                asset_id=asset_id,
                side="sell",
                price=sell_price_str,
                size=sell_size_str,
                tif="gtc",
            )
            if str(sell_resp.get("code", "")) == "0":
                self.state.sell_order_id = sell_resp.get("data", {}).get("oid", "") or sell_resp.get("data", {}).get("orderId", "")
                self.log(f"Posted sell @{sell_price_str} x{sell_size_str}")
                await self._notify(f"📤 已挂 Sell {label} @{sell_price_str} x{normalize_size_str(filled_size)}")
            else:
                msg = sell_resp.get("message", sell_resp.get("msg", ""))
                self.log(f"Sell post failed: {msg}")
                await self._notify(f"⚠️ 挂卖失败: {msg}")
        except Exception as e:
            self.log(f"Sell order exception: {e}")
            await self._notify(f"⚠️ 挂卖异常: {e}")

        # Start holding monitor
        self._hold_task = asyncio.create_task(self._holding_loop())

    # ─── Holding ───

    async def _holding_loop(self):
        """Monitor sell order until filled or timeout."""
        try:
            asset_id = self.state.yes_asset_id if self.state.holding_side == "yes" else self.state.no_asset_id
            label = "YES" if self.state.holding_side == "yes" else "NO"

            while self._running and self.state.status == "holding":
                await asyncio.sleep(5)

                # Check sell order fill status
                # API doesn't return oid from place_order, so poll orders if no oid
                try:
                    sell_filled = 0.0
                    if self.state.sell_order_id:
                        resp = await self.client.get_order(self.state.sell_order_id)
                        if str(resp.get("code", "")) == "0":
                            od = resp.get("data", {})
                            sell_filled = float(od.get("filledSize", "0") or "0")
                            status = od.get("status", "").upper()
                            if status == "CANCELLED" and sell_filled < 1:
                                self.log("Sell order cancelled externally")
                                self.state.sell_order_id = ""
                                await self._handle_timeout(asset_id, label)
                                return
                    else:
                        # No oid — scan SELL orders for this asset since hold_start
                        hold_start_ms = getattr(self.state, 'hold_start_epoch_ms', int(time.time() * 1000))
                        for status_filter in ("open", "closed"):
                            orders_resp = await self.client.get_orders(status=status_filter)
                            if str(orders_resp.get("code", "")) == "0":
                                for o in orders_resp.get("data", {}).get("list", []):
                                    if (o.get("assetId") == asset_id and
                                        o.get("side", "").upper() == "SELL" and
                                        int(o.get("createdAt", "0") or "0") >= hold_start_ms):
                                        sell_filled += float(o.get("filledSize", "0") or "0")
                                        found_oid = o.get("id", o.get("oid", ""))
                                        if found_oid:
                                            self.state.sell_order_id = found_oid

                    if sell_filled >= self.state.buy_size * 0.95:
                        profit = sell_filled * (self.state.sell_target - self.state.buy_price)
                        self.log(f"Sell filled @{self.state.sell_target:.2f} x{normalize_size_str(sell_filled)} | +{profit:.2f} xp")
                        await self._notify(
                            f"💰 Sell {label} 成交 @{self.state.sell_target:.2f} "
                            f"x{normalize_size_str(sell_filled)} | +{profit:.2f} xp"
                        )
                        self._reset_to_monitoring()
                        return
                except Exception as e:
                    self.log(f"Sell check error: {e}")

                # Check timeout
                elapsed = time.monotonic() - self.state.hold_start
                if elapsed >= self.config.timeout_secs:
                    await self._handle_timeout(asset_id, label)
                    return

        except asyncio.CancelledError:
            pass

    async def _handle_timeout(self, asset_id: str, label: str):
        """Timeout: try to exit at best available price."""
        self.log(f"Timeout ({self.config.timeout_secs}s) — attempting exit")

        # Cancel sell order
        if self.state.sell_order_id:
            try:
                await self.client.cancel_order(asset_id=asset_id, order_id=self.state.sell_order_id)
            except Exception:
                pass

        # Check OKX best bid
        try:
            book = await self.client.get_orderbook(asset_id, depth=5)
            if str(book.get("code", "")) != "0":
                await self._notify(f"🚨 超时+无法获取orderbook, 已停止等待人工介入")
                self.state.status = "stopped"
                self._running = False
                return

            bids = book.get("data", [{}])[0].get("bids", [])
            if not bids:
                await self._notify(f"🚨 超时+OKX无bid, 已停止等待人工介入")
                self.state.status = "stopped"
                self._running = False
                return

            best_bid = float(bids[0][0])

            # Check how much of sell order already filled (partial fills)
            if True:
                remaining = self.state.buy_size
                if self.state.sell_order_id:
                    try:
                        so = await self.client.get_order(self.state.sell_order_id)
                        if str(so.get("code", "")) == "0":
                            already_sold = float(so.get("data", {}).get("filledSize", "0") or "0")
                            if already_sold > 0:
                                remaining = self.state.buy_size - already_sold
                                self.log(f"Sell partially filled: {already_sold:.0f} sold, {remaining:.0f} remaining")
                    except Exception:
                        pass
                if remaining < 1:
                    # Already fully sold via partial fills before cancel took effect
                    profit = self.state.buy_size * (self.state.sell_target - self.state.buy_price)
                    self.log(f"Already sold via partial fills | +{profit:.2f} xp")
                    await self._notify(f"💰 Sell {label} 成交 (partial) | +{profit:.2f} xp")
                    self._reset_to_monitoring()
                    return
                # Try up to 5 times to exit remaining at best bid
                size_str = normalize_size_str(remaining)
                for attempt in range(5):
                    # Re-scan orderbook for fresh best bid (except first attempt)
                    if attempt > 0:
                        await asyncio.sleep(1)
                        try:
                            book2 = await self.client.get_orderbook(asset_id, depth=5)
                            if str(book2.get("code", "")) == "0":
                                bids2 = book2.get("data", [{}])[0].get("bids", [])
                                if bids2:
                                    best_bid = float(bids2[0][0])
                                    if best_bid < self.state.buy_price:
                                        self.log(f"Retry {attempt+1}/5: bid={best_bid:.2f} < cost, selling at loss")
                        except Exception:
                            pass
                        self.log(f"Retry {attempt+1}/5: trying @{best_bid:.2f}")

                    exit_resp = await self.client.place_order(
                        asset_id=asset_id,
                        side="sell",
                        price=f"{best_bid:.2f}",
                        size=size_str,
                        tif="ioc",
                    )
                    if str(exit_resp.get("code", "")) != "0":
                        msg = exit_resp.get("message", exit_resp.get("msg", ""))
                        self.log(f"Timeout exit order failed (attempt {attempt+1}): {msg}")
                        continue

                    # Verify fill — place_order doesn't return oid, use clientOrderId
                    _exit_coid = exit_resp.get("_clientOrderId", "") if isinstance(exit_resp, dict) else ""
                    await asyncio.sleep(1.5)
                    exit_filled = 0.0
                    try:
                        for _sf in ("closed", "open"):
                            ords = await self.client.get_orders(status=_sf)
                            if str(ords.get("code", "")) == "0":
                                for _o in ords.get("data", {}).get("list", []):
                                    if _exit_coid and _o.get("clientOrderId") == _exit_coid:
                                        exit_filled = float(_o.get("filledSize", "0") or "0")
                                        break
                            if exit_filled > 0:
                                break
                    except Exception:
                        pass

                    if exit_filled >= 1:
                        profit = exit_filled * (best_bid - self.state.buy_price)
                        self.log(f"Timeout exit @{best_bid:.2f} x{normalize_size_str(exit_filled)} | +{profit:.2f} xp")
                        await self._notify(
                            f"⏰ 超时出 @{best_bid:.2f} x{normalize_size_str(exit_filled)} | +{profit:.2f} xp"
                        )
                        self._reset_to_monitoring()
                        return
                    self.log(f"Attempt {attempt+1}/5: 0 fill")

                # All 5 attempts failed — stop for manual intervention
                self.log("Timeout: 5 attempts all 0 fill, stopping")
                await self._notify("🚨 卖出5次均无成交, 已停止等待人工介入")
                self.state.status = "stopped"
                self._running = False


        except Exception as e:
            self.log(f"Timeout handling error: {e}")
            await self._notify(f"🚨 超时处理异常: {e}, 已停止等待人工介入")
            self.state.status = "stopped"
            self._running = False

    def _reset_to_monitoring(self):
        """Reset state back to monitoring."""
        self.state.status = "monitoring"
        self.state.holding_side = ""
        self.state.buy_price = 0.0
        self.state.buy_size = 0.0
        self.state.sell_target = 0.0
        self.state.sell_order_id = ""
        self.state.hold_start = 0.0
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

    def get_state(self) -> dict:
        # Report settling as a sub-status
        effective_status = self.state.status
        if self.state.settling:
            effective_status = "settling"
        return {
            "running": self._running,
            "status": effective_status,
            "settling": {
                "side": self.state.settle_side,
                "initial": self.state.settle_initial,
                "current": self.state.settle_price,
                "elapsed_ms": int((time.monotonic() - self.state.settle_last_update) * 1000) if self.state.settle_last_update else 0,
            } if self.state.settling else None,
            "config": {
                "threshold_cents": self.config.threshold_cents,
                "buy_limit_cents": self.config.buy_limit_cents,
                "position_pct": self.config.position_pct,
                "timeout_secs": self.config.timeout_secs,
                "settle_ms": self.config.settle_ms,
                "window_ms": self.config.window_ms,
            },
            "holding": {
                "side": self.state.holding_side,
                "buy_price": self.state.buy_price,
                "buy_size": self.state.buy_size,
                "sell_target": self.state.sell_target,
                "elapsed": int(time.monotonic() - self.state.hold_start) if self.state.hold_start else 0,
            } if self.state.status == "holding" else None,
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
