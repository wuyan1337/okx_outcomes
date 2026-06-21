"""Auto-Rebalance module for PolyMonitor.

When monitor triggers (price DROP only), instead of just stopping:
1. Cancel all orders + stop strategy
2. Observe Poly for 60s (1 check x 5s), wait for stable price
3. Determine new price (Poly best bid rounded to cent)
4. Post sells for existing positions at max(cost_price, new_price+1)
5. Restart strategy at new price with monitor

Cost layers: tracks positions by buy price to ensure never selling below cost.
Persisted to JSON file per account.
"""
import asyncio
import json
import logging
import os
import time
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

COST_LAYERS_FILE = "data/cost_layers_{account_id}.json"
STABILIZE_INTERVAL = 5       # seconds between checks
STABILIZE_COUNT = 1         # consecutive same-price checks needed (1=5s)
STABILIZE_TOLERANCE = 0.005  # price must be within this to count as "same"


class AutoRebalance:
    """Handles post-trigger rebalance: observe, reposition, restart."""

    def __init__(self, client, account_id: str, account_name: str = ""):
        self.client = client
        self.account_id = account_id
        self.account_name = account_name
        self.notifier = None
        self.enabled = False  # toggled from UI

        # Cost layers: {price_cents: quantity}
        self.cost_layers: Dict[int, float] = {}
        self._layers_file = COST_LAYERS_FILE.format(account_id=account_id)
        self._load_layers()

        # State
        self._rebalancing = False
        self._task: Optional[asyncio.Task] = None

        # Callbacks (set by AccountInstance)
        self.get_strategy_params = None   # () -> dict with market_id, asset_ids, side, poly_token_id
        self.restart_strategy = None      # (new_price_cents) -> None
        self.restart_monitor = None       # (reference_price, poly_token_id) -> None

    @property
    def rebalancing(self) -> bool:
        return self._rebalancing

    # ─── Cost Layer Management ───

    def _load_layers(self):
        try:
            if os.path.exists(self._layers_file):
                with open(self._layers_file, "r") as f:
                    data = json.load(f)
                # Keys are strings in JSON, convert to int
                self.cost_layers = {int(k): float(v) for k, v in data.items() if float(v) > 0}
                logger.info(f"[Rebalance:{self.account_id}] Loaded {len(self.cost_layers)} cost layers")
        except Exception as e:
            logger.warning(f"[Rebalance:{self.account_id}] Failed to load layers: {e}")

    def _save_layers(self):
        try:
            os.makedirs(os.path.dirname(self._layers_file), exist_ok=True)
            # Clean zero/negative layers before saving
            clean = {k: v for k, v in self.cost_layers.items() if v > 0.5}
            with open(self._layers_file, "w") as f:
                json.dump(clean, f)
        except Exception as e:
            logger.warning(f"[Rebalance:{self.account_id}] Failed to save layers: {e}")

    def add_to_layer(self, price_cents: int, quantity: float):
        """Record a buy fill into cost layers."""
        if quantity <= 0:
            return
        current = self.cost_layers.get(price_cents, 0.0)
        self.cost_layers[price_cents] = current + quantity
        self._save_layers()

    def remove_from_layer(self, price_cents: int, quantity: float):
        """Record a sell fill - remove from the specified layer."""
        if price_cents not in self.cost_layers:
            return
        self.cost_layers[price_cents] = max(0, self.cost_layers[price_cents] - quantity)
        if self.cost_layers[price_cents] < 0.5:
            del self.cost_layers[price_cents]
        self._save_layers()

    def remove_from_highest_layer(self, quantity: float):
        """Remove quantity starting from highest-cost layer (LIFO for sell fills)."""
        remaining = quantity
        for price in sorted(self.cost_layers.keys(), reverse=True):
            if remaining <= 0:
                break
            avail = self.cost_layers[price]
            take = min(avail, remaining)
            self.cost_layers[price] -= take
            remaining -= take
            if self.cost_layers[price] < 0.5:
                del self.cost_layers[price]
        self._save_layers()

    def sync_with_positions(self, total_position_size: float):
        """Sync cost_layers total with actual OKX position size.
        If layers sum > actual position, proportionally reduce (sells happened outside our tracking).
        If layers sum < actual position, ignore (buys happened outside, we can't know cost).
        """
        layers_total = sum(self.cost_layers.values())
        if layers_total <= 0:
            return
        if total_position_size <= 0:
            self.cost_layers.clear()
            self._save_layers()
            return
        if layers_total > total_position_size + 1:
            # Proportionally shrink
            ratio = total_position_size / layers_total
            for k in list(self.cost_layers.keys()):
                self.cost_layers[k] *= ratio
                if self.cost_layers[k] < 0.5:
                    del self.cost_layers[k]
            self._save_layers()

    # ─── Main Rebalance Flow ───

    async def execute(self, trigger_reason: str, poly_token_id: str,
                      old_price_cents: int, strategy_type: str):
        """Called by PolyMonitor after trigger + cancel. Runs the full rebalance."""
        if self._rebalancing:
            logger.warning(f"[Rebalance:{self.account_id}] Already rebalancing, skip")
            return
        self._rebalancing = True
        self._task = asyncio.create_task(
            self._do_rebalance(trigger_reason, poly_token_id, old_price_cents, strategy_type)
        )

    async def _do_rebalance(self, trigger_reason: str, poly_token_id: str,
                            old_price_cents: int, strategy_type: str):
        """Full rebalance sequence."""
        try:
            # Cancel all remaining orders (ensure clean slate including old sells)
            try:
                await self.client.cancel_all([])
                await asyncio.sleep(0.5)
            except Exception:
                pass

            # Step 0: Record any partially filled buys from the cancelled orders
            await self._capture_partial_fills(old_price_cents)

            # Step 0b: Reconcile with actual OKX position (handles orphaned positions
            # where sells were cancelled but the underlying position still exists)
            await self._reconcile_with_position(old_price_cents)

            # Step 1: Notify start
            await self._notify(f"🔄 Auto-Rebalance 开始观察 (5s)\nTrigger: {trigger_reason}")

            # Step 2: Wait for stable price
            new_price_decimal = await self._wait_for_stable_price(poly_token_id)
            if new_price_decimal is None:
                await self._notify("❌ Auto-Rebalance 失败: 观察期内价格未稳定，需手动处理")
                self._rebalancing = False
                return

            # Convert to cents (round to nearest cent)
            new_price_cents = round(new_price_decimal * 100)

            # Validate range
            if new_price_cents < 4 or new_price_cents > 95:
                await self._notify(f"❌ Auto-Rebalance 失败: 新价 {new_price_cents}¢ 超出范围(4-95)")
                self._rebalancing = False
                return

            # Step 3: Same price as before? Just restart
            if new_price_cents == old_price_cents:
                await self._notify(f"✅ 价格回到 {old_price_cents}¢，原价重启")
                await self._restart(old_price_cents, poly_token_id, new_price_decimal, strategy_type)
                self._rebalancing = False
                return

            # Step 4: Sync cost layers with actual positions
            # Guard: if we just captured partial fills, don't let sync wipe them
            # (OKX position API may lag behind cancel/fill events)
            layers_snapshot = dict(self.cost_layers)  # preserve actual costs
            layers_before_sync = sum(self.cost_layers.values())
            await self._sync_positions()
            layers_after_sync = sum(self.cost_layers.values())
            if layers_before_sync > 0 and layers_after_sync <= 0:
                logger.warning(f"[Rebalance:{self.account_id}] sync wiped {layers_before_sync:.0f} units - API lag, restoring from snapshot")
                # Restore from snapshot (preserves actual cost prices, not old_price_cents)
                for price_c, qty in layers_snapshot.items():
                    self.add_to_layer(price_c, qty)

            # Step 5: Post sells for existing layers
            new_sell_price_cents = new_price_cents + 1
            sells_posted = await self._post_layer_sells(new_sell_price_cents, strategy_type)

            # Step 6: Restart strategy at new price
            await self._restart(new_price_cents, poly_token_id, new_price_decimal, strategy_type)

            # Step 7: Notify success
            layers_info = ", ".join(f"{v:.0f}@{k}¢" for k, v in sorted(self.cost_layers.items()))
            await self._notify(
                f"✅ Auto-Rebalance 完成\n"
                f"新价: {new_price_cents}¢ (buy) / {new_sell_price_cents}¢ (sell)\n"
                f"存量 sells: {sells_posted} 笔\n"
                f"持仓层: {layers_info or '无'}"
            )

        except Exception as e:
            logger.error(f"[Rebalance:{self.account_id}] Error: {e}", exc_info=True)
            await self._notify(f"❌ Auto-Rebalance 异常: {e}\n需手动处理")
        finally:
            self._rebalancing = False

    async def _capture_partial_fills(self, price_cents: int):
        """After cancel, check if any buy orders were partially filled.
        Those filled portions are now positions that need tracking."""
        try:
            # Get recently closed/cancelled orders - partial fills show up
            resp = await self.client.get_orders(status="closed")
            if str(resp.get("code", "")) != "0":
                return
            orders = resp.get("data", {}).get("list", [])

            params = self.get_strategy_params() if self.get_strategy_params else {}
            asset_id = params.get("asset_id", "")

            for o in orders:
                if o.get("side", "").upper() != "BUY":
                    continue
                if asset_id and o.get("assetId", "") != asset_id:
                    continue
                filled = float(o.get("filledSize", "0") or "0")
                status = o.get("status", "").upper()
                # Only care about orders that were cancelled with partial fill
                if status in ("CANCELLED",) and filled > 0.5:
                    # This was a partially filled buy that got cancelled by monitor
                    order_price_str = o.get("price", "0")
                    order_price_cents = round(float(order_price_str) * 100)
                    self.add_to_layer(order_price_cents, filled)
                    logger.info(f"[Rebalance] Captured partial fill: {filled} @ {order_price_cents}¢")

        except Exception as e:
            logger.warning(f"[Rebalance] capture_partial_fills error: {e}")

    async def _reconcile_with_position(self, old_price_cents: int):
        """After capture, check actual OKX position. If position > layers sum,
        the difference is orphaned (sells were cancelled but position remains).
        Add the gap at old_price_cents (best available cost estimate)."""
        try:
            params = self.get_strategy_params() if self.get_strategy_params else {}
            asset_id = params.get('asset_id', '')
            if not asset_id:
                return

            # Retry up to 3 times with delay to handle API lag after cancel
            actual_pos = 0.0
            for attempt in range(3):
                if attempt > 0:
                    await asyncio.sleep(1.0)
                actual_pos = 0.0
                resp = await self.client.get_positions(status='open')
                if str(resp.get('code', '')) != '0':
                    continue
                raw = resp.get('data', {})
                pos_list = raw.get('list', []) if isinstance(raw, dict) else raw if isinstance(raw, list) else []
                for p in pos_list:
                    if p.get('assetId', '') == asset_id or p.get('yesAssetId', '') == asset_id or p.get('noAssetId', '') == asset_id:
                        actual_pos += float(p.get('size', p.get('pos', '0')) or '0')
                if actual_pos > 0:
                    break

            layers_total = sum(self.cost_layers.values())
            gap = actual_pos - layers_total
            if gap > 0.5:
                # Orphaned position not tracked in layers - add at old strategy price
                self.add_to_layer(old_price_cents, gap)
                logger.info(f"[Rebalance] Reconciled orphan position: +{gap:.0f} @ {old_price_cents}cent (actual={actual_pos:.0f}, layers={layers_total:.0f})")

        except Exception as e:
            logger.warning(f"[Rebalance] reconcile_with_position error: {e}")

        async def _wait_for_stable_price(self, poly_token_id: str) -> Optional[float]:
        """Poll Poly every 5s. Need 12 consecutive readings at same price (within tolerance).
        Returns stable price as decimal, or None if not stabilized within max attempts."""
        import aiohttp

        consecutive = 0
        stable_price = None
        max_total_checks = 36  # 3 minutes max (36 * 5s)

        for _ in range(max_total_checks):
            await asyncio.sleep(STABILIZE_INTERVAL)

            try:
                async with aiohttp.ClientSession() as sess:
                    url = f"https://clob.polymarket.com/book?token_id={poly_token_id}"
                    async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status != 200:
                            consecutive = 0
                            continue
                        data = await resp.json()
                        bids = data.get("bids", [])
                        if not bids:
                            consecutive = 0
                            continue
                        bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
                        current_bid = float(bids[0].get("price", 0))
            except Exception:
                consecutive = 0
                continue

            if stable_price is None:
                stable_price = current_bid
                consecutive = 1
            elif abs(current_bid - stable_price) <= STABILIZE_TOLERANCE:
                consecutive += 1
            else:
                # Price changed, reset
                stable_price = current_bid
                consecutive = 1

            if consecutive >= STABILIZE_COUNT:
                return stable_price

        return None  # Did not stabilize

    async def _sync_positions(self):
        """Sync cost_layers with actual OKX position for the active asset."""
        try:
            params = self.get_strategy_params() if self.get_strategy_params else {}
            asset_id = params.get("asset_id", "")
            if not asset_id:
                return

            resp = await self.client.get_positions(status="open")
            if str(resp.get("code", "")) != "0":
                return

            raw = resp.get("data", {})
            pos_list = raw.get("list", []) if isinstance(raw, dict) else raw if isinstance(raw, list) else []

            total_size = 0.0
            for p in pos_list:
                # Match by assetId or by checking tokenName
                if p.get("assetId", "") == asset_id or p.get("yesAssetId", "") == asset_id or p.get("noAssetId", "") == asset_id:
                    total_size += float(p.get("size", p.get("pos", "0")) or "0")

            self.sync_with_positions(total_size)

        except Exception as e:
            logger.warning(f"[Rebalance] sync_positions error: {e}")

    async def _post_layer_sells(self, new_sell_cents: int, strategy_type: str) -> int:
        """Post sell orders for each cost layer at max(cost, new_sell_price)."""
        params = self.get_strategy_params() if self.get_strategy_params else {}
        asset_id = params.get("asset_id", "")
        if not asset_id:
            return 0

        sells_posted = 0
        for cost_cents, quantity in list(self.cost_layers.items()):
            if quantity < 1:
                continue

            # Sell at max(cost_price, new_sell_price) — never below cost
            sell_cents = max(cost_cents, new_sell_cents)

            # Validate sell price range
            if sell_cents > 96:
                await self._notify(f"⚠️ 层 {cost_cents}¢ x{quantity:.0f} sell价 {sell_cents}¢ 超限，跳过")
                continue
            if sell_cents < 5:
                continue

            price_str = f"{sell_cents / 100.0:.2f}" if 4 <= sell_cents <= 96 else f"{sell_cents / 100.0:.3f}"
            size_str = f"{quantity:.10f}".rstrip("0").rstrip(".")

            try:
                resp = await self.client.place_order(
                    asset_id=asset_id,
                    side="sell",
                    price=price_str,
                    size=size_str,
                    tif="gtc",
                )
                if str(resp.get("code", "")) == "0":
                    sells_posted += 1
                    logger.info(f"[Rebalance] Posted sell: {quantity:.0f} @ {sell_cents}¢ (cost {cost_cents}¢)")
                else:
                    msg = resp.get("message", resp.get("msg", ""))
                    logger.warning(f"[Rebalance] Sell failed for layer {cost_cents}¢: {msg}")
                    # If insufficient balance, some might already be sold
                    if "Insufficient" in msg:
                        await self._notify(f"⚠️ 层 {cost_cents}¢ 挂sell失败(余额不足)，可能已部分卖出")
            except Exception as e:
                logger.warning(f"[Rebalance] Sell order error for layer {cost_cents}¢: {e}")

        return sells_posted

    async def _restart(self, new_price_cents: int, poly_token_id: str,
                       reference_price_decimal: float, strategy_type: str):
        """Restart strategy + monitor at new price."""
        try:
            if self.restart_strategy:
                await self.restart_strategy(new_price_cents, strategy_type)
            if self.restart_monitor:
                await self.restart_monitor(reference_price_decimal, poly_token_id)
        except Exception as e:
            logger.error(f"[Rebalance] Restart failed: {e}")
            await self._notify(f"⚠️ 重启失败: {e}")

    async def _notify(self, msg: str):
        """Send TG notification."""
        full_msg = f"[{self.account_name or self.account_id}] {msg}"
        if self.notifier:
            try:
                await self.notifier.send(full_msg)
            except Exception as e:
                logger.warning(f"[Rebalance] Notify failed: {e}")
        logger.info(f"[Rebalance] {msg}")

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "rebalancing": self._rebalancing,
            "cost_layers": {str(k): v for k, v in self.cost_layers.items()},
            "total_held": sum(self.cost_layers.values()),
        }

    async def cancel(self):
        """Cancel ongoing rebalance."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._rebalancing = False
