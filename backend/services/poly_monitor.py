"""POLY Monitor - Risk control module for Manual-D and Manual-S strategies.

Monitors Polymarket orderbook in real-time. If Poly best bid/ask moves away from
the locked price (set at start), immediately cancels all OKX orders and sends TG alert.

Logic:
- User sets poly_url + token (outcome) before starting strategy
- On strategy start with monitor enabled: lock current Poly best bid as reference_price
- Monitor checks: if Poly best_bid != reference_price -> CANCEL ALL + TG alert
- Also checks: if OKX best_bid != reference_price -> CANCEL ALL + TG alert (already late)
- After trigger: monitor stops, strategy keeps its state (user manually restarts)
"""
import asyncio
import logging
import time
from typing import Optional
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)

POLY_CLOB_BOOK = "https://clob.polymarket.com/book?token_id={token_id}"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class MonitorState:
    enabled: bool = False
    poly_token_id: str = ""
    reference_price: float = 0.0  # locked price at start
    depth_threshold: float = 10000.0  # min $ at new best bid to trigger (default $10k)
    triggered: bool = False
    trigger_reason: str = ""
    last_poly_bid: float = 0.0
    last_poly_bid_size: float = 0.0  # size at best bid
    last_poly_bid_value: float = 0.0  # $ value at best bid (price * size)
    last_check_ts: int = 0


class PolyMonitor:
    """Real-time Poly orderbook monitor for risk control."""

    def __init__(self, client, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.state = MonitorState()
        self._task: Optional[asyncio.Task] = None
        self.notifier = None
        self._running = False
        self.on_trigger_callback = None  # Called when triggered to stop strategies
        self.auto_rebalance = None       # AutoRebalance instance (set by AccountInstance)
        self._strategy_type = ""         # "manual-s" or "manual-d" (set on start)
        self._strategy_mode = "classic"   # "classic" or "bid-sell"
        self._strategy_mode = "classic"   # "classic" or "bid-sell"
        self._old_price_cents = 0        # Strategy price when monitor started

    @property
    def running(self) -> bool:
        return self._running and not self.state.triggered

    async def start(self, poly_token_id: str, reference_price: float = 0.0, depth_threshold: float = 10000.0):
        """Start monitoring. If reference_price=0, fetch current Poly best bid as reference."""
        if self._running:
            return {"ok": False, "error": "Monitor already running"}

        self.state.poly_token_id = poly_token_id
        self.state.depth_threshold = depth_threshold
        self.state.triggered = False
        self.state.trigger_reason = ""

        # Get current Poly best bid as reference if not provided
        if reference_price <= 0:
            bid = await self._get_poly_best_bid(poly_token_id)
            if bid is None:
                return {"ok": False, "error": "Cannot fetch Poly orderbook"}
            self.state.reference_price = bid
        else:
            self.state.reference_price = reference_price

        self._running = True
        self.state.enabled = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"[PolyMonitor] Started for {self.account_id}, ref_price={self.state.reference_price}")
        return {"ok": True, "reference_price": self.state.reference_price}

    def set_strategy_context(self, strategy_type: str, price_cents: int, asset_id: str = "", mode: str = "classic"):
        """Called by start flow to record what strategy is running."""
        self._strategy_type = strategy_type
        self._old_price_cents = price_cents
        self._strategy_asset_id = asset_id
        self._strategy_mode = mode
        self._strategy_mode = mode

    async def stop(self):
        """Stop monitoring."""
        self._running = False
        self.state.enabled = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        logger.info(f"[PolyMonitor] Stopped for {self.account_id}")

    async def _get_poly_best_bid(self, token_id: str) -> Optional[float]:
        """Fetch current Poly best bid price."""
        bid, _ = await self._get_poly_best_bid_with_depth(token_id)
        return bid

    async def _get_poly_best_bid_with_depth(self, token_id: str):
        """Fetch current Poly best bid price and full bids list (sorted high->low)."""
        try:
            async with aiohttp.ClientSession() as sess:
                url = POLY_CLOB_BOOK.format(token_id=token_id)
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        bids = data.get("bids", [])
                        if bids:
                            # Sort bids by price descending (CLOB returns ascending)
                            bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
                            return float(bids[0].get("price", 0)), bids
        except Exception as e:
            logger.warning(f"[PolyMonitor] Failed to get Poly bid: {e}")
        return None, []

    async def _monitor_loop(self):
        """Main monitoring loop: REST poll every 1.5s + WS for instant updates."""
        import json
        token_id = self.state.poly_token_id

        # Start both: REST poller (reliable) + WS listener (fast)
        rest_task = asyncio.create_task(self._rest_poll_loop(token_id))
        ws_task = asyncio.create_task(self._ws_listen_loop(token_id))

        try:
            # Wait until either task finishes (triggered or cancelled)
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
                            if not self._running or self.state.triggered:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    raw = json.loads(msg.data)
                                    items = raw if isinstance(raw, list) else [raw]
                                    for data in items:
                                        if data.get("event_type") == "book":
                                            bids = data.get("bids", [])
                                            if bids:
                                                bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
                                                best_bid = float(bids[0].get("price", 0))
                                                self.state.last_poly_bid = best_bid
                                                self.state.last_check_ts = int(time.time() * 1000)
                                                await self._check_price(best_bid, bids, "poly_ws")
                                except Exception:
                                    pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"[PolyMonitor] WS error: {e}")
            if self._running and not self.state.triggered:
                await asyncio.sleep(min(retry_delay, 5))
                retry_delay = min(retry_delay * 2, 5)

    async def _check_price(self, current_bid: float, bids: list, source: str):
        """Check if price deviated from reference.

        Two trigger conditions:
        A) Price DROPPED (current_bid < ref): reference price disappeared from bid side.
           Trigger immediately - no depth check needed.
        B) Price ROSE (current_bid > ref): someone is buying aggressively.
           Only trigger if depth at new best bid >= threshold (confirms real move).
        """
        ref = self.state.reference_price
        if ref <= 0:
            return

        # Price hasn't changed - all good
        if abs(current_bid - ref) <= 0.005:
            return

        # Calculate depth at best bid level (used for logging + condition B)
        best_bid_size = 0.0
        if bids:
            best_price = float(bids[0].get("price", 0))
            for b in bids:
                if abs(float(b.get("price", 0)) - best_price) < 0.0005:
                    best_bid_size += float(b.get("size", 0))
                else:
                    break
        best_bid_value = current_bid * best_bid_size
        self.state.last_poly_bid_size = best_bid_size
        self.state.last_poly_bid_value = best_bid_value

        # CONDITION A: Price dropped (bid < reference)
        # Reference price disappeared from bids → immediate trigger
        if current_bid < ref - 0.005:
            reason = (
                f"Poly bid DROPPED: {ref:.3f} -> {current_bid:.3f} "
                f"(reference gone from bid side) [{source}]"
            )
            asyncio.ensure_future(asyncio.shield(self._trigger(reason)))
            return

        # CONDITION B: Price rose (bid > reference)
        # In bid-sell mode, ROSE means our sell is MORE likely to fill - skip trigger
        if self._strategy_mode == "bid-sell" and current_bid > ref + 0.005:
            logger.info(
                f"[PolyMonitor] Bid rose {ref:.3f}->{current_bid:.3f} "
                f"but bid-sell mode: sell safer at higher bid, ignoring"
            )
            return

        # Only trigger if depth at new best bid >= threshold
        if current_bid > ref + 0.005:
            threshold = self.state.depth_threshold
            if best_bid_value < threshold:
                logger.debug(
                    f"[PolyMonitor] Bid rose {ref:.2f}->{current_bid:.2f} "
                    f"but value ${best_bid_value:.0f} < ${threshold:.0f}, ignoring"
                )
                return
            reason = (
                f"Poly bid ROSE: {ref:.3f} -> {current_bid:.3f} "
                f"(${best_bid_value:,.0f} at new bid >= ${threshold:,.0f} threshold) [{source}]"
            )
            asyncio.ensure_future(asyncio.shield(self._trigger(reason)))

    async def _trigger(self, reason: str):
        """Price deviated - CANCEL FIRST, then stop strategy, then notify."""
        if self.state.triggered:
            return

        self.state.triggered = True
        self.state.trigger_reason = reason
        self._running = False
        logger.warning(f"[PolyMonitor] TRIGGERED: {reason}")

        # Step 1: CANCEL ALL ORDERS IMMEDIATELY (highest priority)
        cancelled = await self._fast_cancel_all()
        logger.info(f"[PolyMonitor] Final cancel result: {cancelled}")

        # Step 2: Stop strategies (with timeout so it can never block)
        if self.on_trigger_callback:
            try:
                await asyncio.wait_for(self.on_trigger_callback(), timeout=5.0)
                logger.info("[PolyMonitor] Strategies stopped via callback")
            except asyncio.TimeoutError:
                logger.error("[PolyMonitor] Strategy stop timed out (5s) - forcing")
            except Exception as e:
                logger.error(f"[PolyMonitor] Strategy stop failed: {e}")

        # TG notification
        if self.notifier:
            try:
                rebal_status = " → 🔄 准备自动重部署" if (self.auto_rebalance and self.auto_rebalance.enabled) else ""
                msg = f"[{self.account_name or self.account_id}] ⚠️ <b>风控触发</b>\nReason: {reason}\nAction: 已全部撤单{rebal_status}"
                logger.info(f"[PolyMonitor] Sending TG notification, enabled={self.notifier.enabled}")
                await self.notifier.send(msg)
                logger.info("[PolyMonitor] TG notification sent")
            except Exception as e:
                logger.error(f"[PolyMonitor] TG notify failed: {e}")
        else:
            logger.warning("[PolyMonitor] No notifier configured!")

        # Auto-rebalance: on any price move (DROPPED or ROSE)
        if self.auto_rebalance and self.auto_rebalance.enabled:
            try:
                await self.auto_rebalance.execute(
                    trigger_reason=reason,
                    poly_token_id=self.state.poly_token_id,
                    old_price_cents=self._old_price_cents,
                    strategy_type=self._strategy_type,
                )
            except Exception as e:
                logger.error(f"[PolyMonitor] Auto-rebalance launch failed: {e}")

    async def _fast_cancel_all(self) -> dict:
        """Cancel all open orders. Try cancelAll first (fastest), then batch, then individual."""
        import asyncio
        result = {"cancelled": 0, "failed": 0, "total": 0}

        # Attempt 0: cancelAll (empty assetIds = cancel ALL orders for this account)
        try:
            cancel_resp = await self.client.cancel_all([])
            if str(cancel_resp.get("code", "")) == "0":
                # Verify: check if orders actually gone
                await asyncio.sleep(0.5)
                verify = await self.client.get_orders(status="open")
                remaining = []
                if str(verify.get("code", "")) == "0":
                    remaining = verify.get("data", {}).get("list", [])
                if not remaining:
                    logger.info("[PolyMonitor] cancelAll succeeded + verified (0 remaining)")
                    result["cancelled"] = -1
                    return result
                else:
                    logger.warning(f"[PolyMonitor] cancelAll returned OK but {len(remaining)} orders still open, falling back")
            else:
                logger.warning(f"[PolyMonitor] cancelAll response: {cancel_resp}")
        except Exception as e:
            logger.warning(f"[PolyMonitor] cancelAll failed: {e}, falling back")

        for attempt in range(3):
            try:
                orders_resp = await self.client.get_orders(status="open")
                orders = []
                if str(orders_resp.get("code", "")) == "0":
                    orders = orders_resp.get("data", {}).get("list", [])
                if not orders:
                    break  # All cancelled
                result["total"] = len(orders)

                # Try batch cancel first (single request)
                cancels = []
                for o in orders:
                    oid = o.get("oid", o.get("id", ""))
                    asset_id = o.get("assetId", "")
                    if oid and asset_id:
                        cancels.append({"assetId": asset_id, "oid": oid})
                if cancels:
                    try:
                        batch_resp = await self.client.cancel_orders_batch(cancels)
                        if str(batch_resp.get("code", "")) == "0":
                            result["cancelled"] = len(cancels)
                            # Verify
                            await asyncio.sleep(0.5)
                            verify = await self.client.get_orders(status="open")
                            remaining = verify.get("data", {}).get("list", []) if str(verify.get("code","")) == "0" else orders
                            if not remaining:
                                break  # Success
                            logger.warning(f"[PolyMonitor] Batch cancel: {len(remaining)} still open, retrying")
                            continue
                    except Exception as be:
                        logger.warning(f"[PolyMonitor] Batch cancel failed: {be}, falling back to parallel")

                # Fallback: parallel individual cancels
                async def _cancel_one(o):
                    oid = o.get("oid", o.get("id", ""))
                    asset_id = o.get("assetId", "")
                    if not oid or not asset_id:
                        return False
                    try:
                        r = await self.client.cancel_order(asset_id=asset_id, order_id=oid)
                        ok = str(r.get("code", "")) == "0"
                        if not ok:
                            logger.warning(f"[PolyMonitor] cancel_order {oid} resp: {r}")
                        return ok
                    except Exception as e:
                        logger.warning(f"[PolyMonitor] cancel_order {oid} exception: {e}")
                        return False

                results = await asyncio.gather(*[_cancel_one(o) for o in orders])
                result["cancelled"] = sum(1 for r in results if r)
                result["failed"] = sum(1 for r in results if not r)

                if result["failed"] == 0:
                    break
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"[PolyMonitor] _fast_cancel_all attempt {attempt+1} error: {e}")
                await asyncio.sleep(0.5)

        if result.get("failed", 0) > 0:
            logger.critical(f"[PolyMonitor] {result['failed']} orders NOT cancelled after 3 attempts!")
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
