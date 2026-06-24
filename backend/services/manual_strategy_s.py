"""Strategy: Manual-S (Single-side) — Buy one side@a, Sell@(a+1), repeat.

Logic:
- User picks either Yes OR No side and sets price a.
- Buy {side}@a¢
- When Buy fills → post Sell {side}@(a+1)¢
- When Sell fills → balance freed → re-deploy
- Same 1¢ spread profit per round-trip, but only one side.

Persistence: pending_sells saved to JSON file to survive restarts.
"""
import asyncio
import json
import logging
import os
from typing import Optional, Set, Dict
from dataclasses import dataclass, field
from datetime import datetime

from ..core.client import OKXOutcomesClient

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_FAILURES = 5
SELLS_FILE_S = "data/pending_sells_s_{account_id}.json"


@dataclass
class ManualStateS:
    running: bool = False
    side: str = ""            # "yes" or "no"
    price_a: int = 0          # Buy price in cents
    sell_price: int = 0       # Sell price = a + 1
    market_id: str = ""
    yes_asset_id: str = ""
    no_asset_id: str = ""
    asset_id: str = ""        # The one we're trading (yes or no asset)
    pending_sells: Dict[str, float] = field(default_factory=dict)
    posted_sells: Set[str] = field(default_factory=set)  # sell order IDs we posted
    start_time: int = 0
    total_sells_posted: int = 0
    total_profit: float = 0.0
    consecutive_failures: int = 0
    notified_fills: Set[str] = field(default_factory=set)  # order IDs already notified
    position_pct: int = 100  # % of available balance to deploy
    logs: list = field(default_factory=list)


class ManualStrategyS:
    """Single-side spread: Buy@a, Sell@(a+1) on one token only."""

    def __init__(self, client: OKXOutcomesClient, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.state = ManualStateS()
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self.notifier = None
        self.running = False
        self.cycles = 0
        self.auto_rebalance = None  # Set by AccountInstance for cost layer tracking
        self._sells_file = SELLS_FILE_S.format(account_id=account_id)
        self._load_pending_sells()

    def _load_pending_sells(self):
        """Load pending_sells from disk."""
        try:
            if os.path.exists(self._sells_file):
                with open(self._sells_file, "r") as f:
                    data = json.load(f)
                raw = data.get("oids", [])
                if isinstance(raw, dict):
                    self.state.pending_sells = {k: float(v) for k, v in raw.items()}
                else:
                    self.state.pending_sells = {oid: 999999.0 for oid in raw}
                self.state.start_time = data.get("start_time", 0)
                logger.info(f"[S] Loaded {len(self.state.pending_sells)} pending sells from disk")
        except Exception as e:
            logger.warning(f"[S] Failed to load pending sells: {e}")

    def _save_pending_sells(self):
        """Persist pending_sells to disk."""
        try:
            os.makedirs(os.path.dirname(self._sells_file), exist_ok=True)
            with open(self._sells_file, "w") as f:
                json.dump({
                    "oids": dict(self.state.pending_sells),
                    "start_time": self.state.start_time,
                }, f)
        except Exception as e:
            logger.warning(f"[S] Failed to save pending sells: {e}")

    def log(self, msg: str, level: str = "INFO"):
        entry = f"[{datetime.now().strftime('%I:%M:%S %p')}] {level} {msg}"
        self.state.logs.append(entry)
        if len(self.state.logs) > 200:
            self.state.logs = self.state.logs[-200:]
        logger.info(msg)

    async def start(self, market_id: str, yes_asset_id: str, no_asset_id: str,
                    side: str, price_a: float, size: int = 0, position_pct: int = 100):
        """Start single-side strategy."""
        if self.state.running:
            return

        a = int(price_a)
        if a < 4 or a > 95:
            self.log(f"价格 {a} 超出范围 (4-95)", "ERROR")
            return

        side = side.lower()
        if side not in ("yes", "no"):
            self.log(f"Invalid side: {side}", "ERROR")
            return

        self.state.running = True
        self.running = True
        self.state.market_id = market_id
        self.state.yes_asset_id = yes_asset_id
        self.state.no_asset_id = no_asset_id
        self.state.side = side
        self.state.asset_id = yes_asset_id if side == "yes" else no_asset_id
        self.state.price_a = a
        self.state.sell_price = a + 1
        self.state.position_pct = max(1, min(100, position_pct))
        self.state.consecutive_failures = 0

        import time as _time
        self.state.start_time = int(_time.time() * 1000)
        self._save_pending_sells()

        label = "Yes" if side == "yes" else "No"
        self.log(f"Manual-S started: Buy {label}@{a}¢ → Sell {label}@{a+1}¢")

        # Pre-seed notified_fills with existing closed orders so we don't spam old fills on start
        self._main_task = asyncio.create_task(self._seed_then_run())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _seed_then_run(self):
        """Seed notified_fills + _notified_amounts with existing orders so we don't spam old fills."""
        try:
            if not hasattr(self.state, '_notified_amounts'):
                self.state._notified_amounts = {}
            # Seed from closed orders
            resp = await self.client.get_orders(status="closed")
            if str(resp.get("code", "")) == "0":
                for o in resp.get("data", {}).get("list", []):
                    oid = o.get("id", o.get("oid", ""))
                    if oid:
                        self.state.notified_fills.add(oid)
                        filled = float(o.get("filledSize", o.get("size", "0")) or "0")
                        if filled > 0:
                            self.state._notified_amounts[oid] = filled
            # Also seed from open orders (partial fills)
            resp2 = await self.client.get_orders(status="open")
            if str(resp2.get("code", "")) == "0":
                for o in resp2.get("data", {}).get("list", []):
                    oid = o.get("id", o.get("oid", ""))
                    filled = float(o.get("filledSize", "0") or "0")
                    if oid and filled > 0:
                        self.state.notified_fills.add(oid)
                        self.state._notified_amounts[oid] = filled
            # Pre-seed sell fill tracker to avoid re-notifying old sell fills
            if not hasattr(self.state, '_sell_fills_tracked'):
                self.state._sell_fills_tracked = {}
            all_orders = []
            if str(resp.get("code", "")) == "0":
                all_orders += resp.get("data", {}).get("list", [])
            if str(resp2.get("code", "")) == "0":
                all_orders += resp2.get("data", {}).get("list", [])
            for o in all_orders:
                oid = o.get("id", o.get("oid", ""))
                side = o.get("side", "").upper()
                if side == "SELL" and o.get("assetId", "") == self.state.asset_id:
                    sfilled = float(o.get("filledSize", o.get("size", "0")) or "0")
                    if sfilled > 0:
                        self.state._sell_fills_tracked[oid] = sfilled
            self.log(f"Pre-seeded {len(self.state.notified_fills)} existing fills, {len(self.state._sell_fills_tracked)} sell fills")
        except Exception as e:
            self.log(f"Seed fills error: {e}", "WARN")
        await self._main_loop()

    async def stop(self):
        if not self.state.running:
            return
        self.state.running = False
        self.running = False
        self._save_pending_sells()
        self.log("已停止 (挂单保留)")
        for task in [self._main_task, self._heartbeat_task]:
            if task:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    # ─── Main Loop ───

    async def _main_loop(self):
        try:
            await self._deploy_balance()
            while self.state.running:
                await asyncio.sleep(10)
                if not self.state.running:
                    break
                # Fetch both closed AND open orders to catch partial fills
                _closed_orders = []
                _open_orders = []
                try:
                    _resp = await self.client.get_orders(status="closed")
                    if str(_resp.get("code", "")) == "0":
                        _closed_orders = _resp.get("data", {}).get("list", [])
                except Exception as e:
                    self.log(f"Fetch closed orders error: {e}", "WARN")
                try:
                    _resp2 = await self.client.get_orders(status="open")
                    if str(_resp2.get("code", "")) == "0":
                        _open_orders = _resp2.get("data", {}).get("list", [])
                except Exception as e:
                    self.log(f"Fetch open orders error: {e}", "WARN")

                # Combine: closed + open orders that have partial fills
                _all_filled = _closed_orders + [
                    o for o in _open_orders
                    if float(o.get("filledSize", "0") or "0") > 0
                ]

                await self._notify_all_fills(_all_filled)
                await self._check_fills_post_sells(_all_filled)
                await self._check_sell_fills(_all_filled)
                await self._deploy_balance()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log(f"致命错误: {e}", "ERROR")
            self.state.running = False
            self.running = False

    async def _check_fills_post_sells(self, order_list=None):
        try:
            if order_list is None:
                resp = await self.client.get_orders(status="closed")
                if str(resp.get("code", "")) != "0":
                    return
                order_list = resp.get("data", {}).get("list", [])
            changed = False

            for o in order_list:
                oid = o.get("id", o.get("oid", ""))
                if not oid:
                    continue
                prev_processed = self.state.pending_sells.get(oid, 0.0)

                created = int(o.get("createdAt", "0") or "0")
                if created < self.state.start_time:
                    continue

                side = o.get("side", "").upper()
                filled_size = o.get("filledSize", o.get("size", "0"))
                status = o.get("status", "").upper()

                # Handle both FILLED and PARTIALLY_FILLED
                if status not in ("FILLED", "PARTIALLY_FILLED", "ACTIVE"):
                    continue
                if side != "BUY":
                    continue
                if o.get("assetId", "") != self.state.asset_id:
                    continue

                filled = float(filled_size)
                if filled < 0.01:
                    continue
                new_fill = filled - prev_processed
                if new_fill < 0.01:
                    continue

                # Derive sell price from actual buy order price (like Manual-D)
                buy_price_str = o.get("price", "")
                if buy_price_str:
                    buy_cents = round(float(buy_price_str) * 100)
                    sell_cents = buy_cents + 1
                else:
                    sell_cents = self.state.sell_price
                sell_price = self._cents_to_price_str(sell_cents)
                label = "Yes" if self.state.side == "yes" else "No"

                size_str = f"{new_fill:.10f}".rstrip("0").rstrip(".")
                # Minimum order check: price * size must >= 1 xp
                sell_notional = (sell_cents / 100.0) * new_fill
                if sell_notional < 1.0:
                    # Accumulate - don't post undersized sell, wait for more fills
                    self.log(f"Sell too small: {new_fill:.0f} x {sell_cents}\u00a2 = {sell_notional:.2f} xp < 1 xp, deferring")
                    continue
                sell_resp = await self.client.place_order(
                    asset_id=self.state.asset_id,
                    side="sell",
                    price=sell_price,
                    size=size_str,
                    tif="gtc",
                )

                if str(sell_resp.get("code", "")) == "0":
                    self.state.pending_sells[oid] = filled
                    sell_oid = sell_resp.get("data", {}).get("oid", "") or sell_resp.get("data", {}).get("orderId", "")
                    if sell_oid:
                        self.state.posted_sells.add(sell_oid)
                    self.state.total_sells_posted += 1
                    self.state.consecutive_failures = 0
                    changed = True
                    # Track in cost layers for auto-rebalance
                    if self.auto_rebalance:
                        self.auto_rebalance.add_to_layer(self.state.price_a, new_fill)
                    self.log(f"挂 Sell {label} @{sell_price} x{size_str}")
                    if self.notifier:
                        asyncio.create_task(self.notifier.send(f"[{self.account_name}] ✅ Buy {label} 成交 x{size_str} → 挂 Sell @{sell_price}"))
                else:
                    msg = sell_resp.get("message", "")
                    if "Insufficient" in msg or "Minimum" in msg:
                        self.state.pending_sells[oid] = filled
                        changed = True
                    else:
                        self._record_failure(f"Sell {label} 失败: {msg}")
                        if self.notifier:
                            asyncio.create_task(self.notifier.send(f"[{self.account_name}] ⚠️ Sell {label} 失败: {msg}\n裸仓风险!"))

            if changed:
                self._save_pending_sells()

        except Exception as e:
            self._record_failure(f"检查成交异常: {e}")


    async def _check_sell_fills(self, order_list=None):
        """Check if our sell orders have been filled -> profit realized.
        Scans ALL sell orders for current asset (no posted_sells dependency, survives restart).
        """
        try:
            if order_list is None:
                _closed = []
                _open = []
                try:
                    r1 = await self.client.get_orders(status="closed")
                    if str(r1.get("code", "")) == "0":
                        _closed = r1.get("data", {}).get("list", [])
                except:
                    pass
                try:
                    r2 = await self.client.get_orders(status="open")
                    if str(r2.get("code", "")) == "0":
                        _open = r2.get("data", {}).get("list", [])
                except:
                    pass
                order_list = _closed + _open

            if not hasattr(self.state, '_sell_fills_tracked'):
                self.state._sell_fills_tracked = {}

            for o in order_list:
                oid = o.get("id", o.get("oid", ""))
                if not oid:
                    continue
                side = o.get("side", "").upper()
                if side != "SELL":
                    continue
                # Only track sells for current asset
                if o.get("assetId", "") != self.state.asset_id:
                    continue
                status = o.get("status", "").upper()
                if status not in ("FILLED", "PARTIALLY_FILLED", "ACTIVE"):
                    continue
                filled = float(o.get("filledSize", o.get("size", "0")) or "0")
                price = o.get("price", "?")
                if filled < 0.01:
                    continue
                # Incremental: only notify new fill amount
                prev_sell_filled = self.state._sell_fills_tracked.get(oid, 0.0)
                new_sell_fill = filled - prev_sell_filled
                if new_sell_fill < 0.5:
                    continue
                self.state._sell_fills_tracked[oid] = filled
                self.state.posted_sells.add(oid)
                profit = new_sell_fill * 0.01
                self.state.total_profit += profit
                # Remove from cost layers (sell filled = position closed)
                if self.auto_rebalance:
                    self.auto_rebalance.remove_from_highest_layer(new_sell_fill)
                if status == "FILLED":
                    self.state.posted_sells.discard(oid)
                    pass  # keep in tracked to prevent re-notify
                label = "Yes" if self.state.side == "yes" else "No"
                self.log(f"\U0001f4b0 Sell {label} \u6210\u4ea4 x{new_sell_fill:.0f} @{price} | +{profit:.2f} xp")
                if self.notifier:
                    import asyncio
                    asyncio.create_task(self.notifier.send(
                        f"[{self.account_name}] \U0001f4b0 Sell {label} \u6210\u4ea4 x{int(new_sell_fill)} @{price} | +{profit:.2f} xp"
                    ))
        except Exception as e:
            self.log(f"\u68c0\u67e5sell\u6210\u4ea4\u5f02\u5e38: {e}", "WARN")

    async def _notify_all_fills(self, order_list=None):
        """Notify on ANY filled order incrementally (tracks already-notified amounts)."""
        if not self.notifier:
            return
        try:
            if order_list is None:
                resp = await self.client.get_orders(status="closed")
                if str(resp.get("code", "")) != "0":
                    return
                order_list = resp.get("data", {}).get("list", [])
            # Use a dict to track notified fill amounts (not just OID set)
            if not hasattr(self.state, '_notified_amounts'):
                self.state._notified_amounts = {}
            for o in order_list:
                oid = o.get("id", o.get("oid", ""))
                if not oid:
                    continue
                # Skip current asset's fills - _check_fills_post_sells handles those
                if o.get("assetId", "") == self.state.asset_id:
                    # Still track amounts to avoid spam if asset changes later
                    filled_tmp = float(o.get("filledSize", o.get("size", "0")))
                    if filled_tmp > 0:
                        self.state._notified_amounts[oid] = filled_tmp
                        self.state.notified_fills.add(oid)
                    continue
                status = o.get("status", "").upper()
                if status not in ("FILLED", "PARTIALLY_FILLED", "ACTIVE"):
                    continue
                filled = float(o.get("filledSize", o.get("size", "0")))
                if filled < 0.01:
                    continue
                prev_notified = self.state._notified_amounts.get(oid, 0.0)
                new_fill = filled - prev_notified
                if new_fill < 0.5:
                    continue
                self.state._notified_amounts[oid] = filled
                self.state.notified_fills.add(oid)
                side = o.get("side", "").upper()
                price = o.get("price", "?")
                emoji = "\U0001f7e2" if side == "BUY" else "\U0001f534"
                label = "Yes" if self.state.side == "yes" else "No"
                asyncio.create_task(self.notifier.send(
                    f"[{self.account_name}] {emoji} <b>{side} {label}</b> @{price} x{int(new_fill)} \u6210\u4ea4"
                ))
        except Exception as e:
            self.log(f"notify_all_fills \u5f02\u5e38: {e}", "WARN")

    async def _deploy_balance(self):
        try:

            balance = await self.client.get_balance()
            available = self._parse_available(balance)

            # Calculate how much is already locked in open BUY orders
            locked_in_buys = 0.0
            try:
                resp_open = await self.client.get_orders(status="open")
                if str(resp_open.get("code", "")) == "0":
                    for o in resp_open.get("data", {}).get("list", []):
                        if o.get("side", "").upper() == "BUY" and o.get("assetId", "") == self.state.asset_id:
                            sz = float(o.get("size", "0") or "0") - float(o.get("filledSize", "0") or "0")
                            px = float(o.get("price", "0") or "0")
                            locked_in_buys += sz * px
            except Exception:
                pass

            # total_capital = available + locked; max to deploy = total * pct%
            total_capital = available + locked_in_buys
            max_deploy = total_capital * self.state.position_pct / 100.0
            # How much more we can deploy = max - already deployed
            remaining_budget = max_deploy - locked_in_buys
            if remaining_budget <= 0:
                return

            cost_per_unit = self.state.price_a / 100.0
            units = int(remaining_budget / cost_per_unit)

            if units < 1:
                return

            # Min order = 1 xp notional
            min_units = max(1, int(1.0 / cost_per_unit) + 1)
            if units < min_units:
                return

            price_str = self._cents_to_price_str(self.state.price_a)
            label = "Yes" if self.state.side == "yes" else "No"

            resp = await self.client.place_order(
                asset_id=self.state.asset_id,
                side="buy",
                price=price_str,
                size=str(units),
                tif="gtc",
            )

            if str(resp.get("code", "")) == "0":
                self.state.consecutive_failures = 0
                self.log(f"挂 Buy {label} @{self.state.price_a}¢ x{units}")
            else:
                msg = resp.get("message", "")
                if "Minimum" in msg or "Insufficient" in msg:
                    return
                self._record_failure(f"Buy {label} 失败: {msg}")
                if self.notifier:
                    asyncio.create_task(self.notifier.send(f"[{self.account_name}] ⚠️ Buy {label} 失败: {msg}"))

            self.cycles += 1

        except Exception as e:
            self._record_failure(f"Deploy 异常: {e}")

    # ─── Failure tracking ───

    def _record_failure(self, msg: str):
        self.state.consecutive_failures += 1
        self.log(msg, "ERROR")
        if self.state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.log(f"🚨 连续失败 {self.state.consecutive_failures} 次，自动停止", "ERROR")
            asyncio.create_task(self._emergency_stop())

    async def _emergency_stop(self):
        if self.notifier:
            try:
                await self.notifier.send(
                    f"[{self.account_name}] 🚨 Manual-S 连续失败 {self.state.consecutive_failures} 次，已自动停止"
                )
            except Exception:
                pass
        await self.stop()

    # ─── Helpers ───

    async def _heartbeat_loop(self):
        try:
            while self.state.running:
                try:
                    await self.client.heartbeat()
                except Exception as e:
                    self.log(f"心跳异常: {e}", "WARN")
                await asyncio.sleep(120)
        except asyncio.CancelledError:
            pass

    def _parse_available(self, balance_resp: dict) -> float:
        if str(balance_resp.get("code", "")) != "0":
            return 0.0
        data = balance_resp.get("data", [])
        for entry in data:
            if entry.get("oddsType") == "points":
                return float(entry.get("available", "0"))
        return 0.0

    def _cents_to_price_str(self, cents: int) -> str:
        price = cents / 100.0
        return f"{price:.2f}" if 4 <= cents <= 96 else f"{price:.3f}"

    def get_state(self) -> dict:
        return {
            "running": self.state.running,
            "side": self.state.side,
            "price_a": self.state.price_a,
            "sell_price": self.state.sell_price,
            "total_sells": self.state.total_sells_posted,
            "total_profit": self.state.total_profit,
            "consecutive_failures": self.state.consecutive_failures,
            "logs": self.state.logs[-20:],
        }
