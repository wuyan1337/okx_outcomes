"""Strategy 1: Manual spread market making — Buy@a, Sell@(a+1), repeat.

Logic:
- Set price a (e.g. 55). Buy Yes@a¢, Buy No@(99-a)¢, split balance evenly.
- When a Buy Yes fills → immediately post Sell Yes@(a+1)¢
- When a Buy No fills → immediately post Sell No@(100-a)¢
- When Sells fill → balance increases → re-deploy into new Buy pairs
- Never cancel orders. Just keep adding.
- Profit = 1¢ per round-trip (buy@a, sell@a+1).

Persistence: pending_sells saved to JSON file to survive restarts.
Fix: sell price derived from buy order's actual fill price, not current state.
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
SELLS_FILE_D = "data/pending_sells_d_{account_id}.json"


@dataclass
class ManualStateD:
    running: bool = False
    price_a: int = 0          # Yes buy price in cents (e.g. 55)
    price_b: int = 0          # No buy price = 99 - a (e.g. 44)
    sell_a: int = 0           # Yes sell price = a + 1 (e.g. 56)
    sell_b: int = 0           # No sell price = 100 - a (e.g. 45)
    market_id: str = ""
    yes_asset_id: str = ""
    no_asset_id: str = ""
    # Track processed buy fills: oid -> total filledSize already handled
    pending_sells: Dict[str, float] = field(default_factory=dict)
    posted_sells: Set[str] = field(default_factory=set)  # sell order IDs we posted
    start_time: int = 0  # millis timestamp when strategy started
    # Stats
    total_sells_posted: int = 0
    total_profit: float = 0.0
    consecutive_failures: int = 0
    position_pct: int = 100
    logs: list = field(default_factory=list)
    notified_fills: Set[str] = field(default_factory=set)  # order IDs already notified


class ManualStrategyD:
    """Spread market making: Buy@a, Sell@(a+1). No merge, no cancel."""

    def __init__(self, client: OKXOutcomesClient, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.state = ManualStateD()
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self.notifier = None
        self.running = False
        self.auto_rebalance = None  # Set by AccountInstance for cost layer tracking
        self.cycles = 0
        self._sells_file = SELLS_FILE_D.format(account_id=account_id)
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
                    # Legacy: convert list to dict with 0 size (already processed, won't re-trigger)
                    self.state.pending_sells = {oid: 999999.0 for oid in raw}
                self.state.start_time = data.get("start_time", 0)
                logger.info(f"[D] Loaded {len(self.state.pending_sells)} pending sells from disk")
        except Exception as e:
            logger.warning(f"[D] Failed to load pending sells: {e}")

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
            logger.warning(f"[D] Failed to save pending sells: {e}")

    def log(self, msg: str, level: str = "INFO"):
        entry = f"[{datetime.now().strftime('%I:%M:%S %p')}] {level} {msg}"
        self.state.logs.append(entry)
        if len(self.state.logs) > 200:
            self.state.logs = self.state.logs[-200:]
        logger.info(msg)

    async def start(self, market_id: str, yes_asset_id: str, no_asset_id: str,
                    price_a: float, size: int = 0, position_pct: int = 100):
        """Start the spread strategy."""
        if self.state.running:
            return

        a = int(price_a)
        if a < 4 or a > 95:
            self.log(f"价格 {a} 超出范围 (4-95)", "ERROR")
            return

        self.state.running = True
        self.running = True
        self.state.market_id = market_id
        self.state.yes_asset_id = yes_asset_id
        self.state.no_asset_id = no_asset_id
        self.state.price_a = a
        self.state.position_pct = max(1, min(100, position_pct))
        self.state.price_b = 99 - a
        self.state.sell_a = a + 1
        self.state.sell_b = 100 - a
        self.state.consecutive_failures = 0

        import time as _time
        self.state.start_time = int(_time.time() * 1000)
        self._save_pending_sells()
        self.log(f"Manual started: Buy Yes@{a}¢ / No@{99-a}¢ → Sell Yes@{a+1}¢ / No@{100-a}¢")

        self._main_task = asyncio.create_task(self._seed_then_run())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Stop strategy. Does NOT cancel orders."""
        if not self.state.running:
            return

        self.state.running = False
        self.running = False
        self.auto_rebalance = None  # Set by AccountInstance for cost layer tracking
        self._save_pending_sells()
        self.log("已停止 (挂单保留)")

        for task in [self._main_task, self._heartbeat_task]:
            if task:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    async def update_price(self, price_a: float):
        """Update price while running — only affects NEW buy orders.
        Existing fills still get sell@(original_buy_price+1)."""
        a = int(price_a)
        if a < 4 or a > 95:
            return
        self.state.price_a = a
        self.state.price_b = 99 - a
        self.state.sell_a = a + 1
        self.state.sell_b = 100 - a
        self.log(f"更新: Buy Yes@{a}¢ No@{99-a}¢ → Sell Yes@{a+1}¢ No@{100-a}¢")


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
            if not hasattr(self.state, '_notified_amounts'):
                self.state._notified_amounts = {}
            for o in order_list:
                oid = o.get("id", o.get("oid", ""))
                if not oid:
                    continue
                asset_id = o.get("assetId", "")
                # Skip current strategy's assets - _check_fills_post_sells handles those
                if asset_id in (self.state.yes_asset_id, self.state.no_asset_id):
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
                label = "Yes" if asset_id == self.state.yes_asset_id else "No"
                asyncio.create_task(self.notifier.send(
                    f"[{self.account_name}] {emoji} <b>{side} {label}</b> @{price} x{int(new_fill)} \u6210\u4ea4"
                ))
        except Exception as e:
            self.log(f"notify_all_fills error: {e}", "WARN")


    async def _seed_then_run(self):
        """Seed notified_fills + _notified_amounts with existing orders."""
        try:
            if not hasattr(self.state, '_notified_amounts'):
                self.state._notified_amounts = {}
            resp = await self.client.get_orders(status="closed")
            if str(resp.get("code", "")) == "0":
                for o in resp.get("data", {}).get("list", []):
                    oid = o.get("id", o.get("oid", ""))
                    if oid:
                        self.state.notified_fills.add(oid)
                        filled = float(o.get("filledSize", o.get("size", "0")) or "0")
                        if filled > 0:
                            self.state._notified_amounts[oid] = filled
            resp2 = await self.client.get_orders(status="open")
            if str(resp2.get("code", "")) == "0":
                for o in resp2.get("data", {}).get("list", []):
                    oid = o.get("id", o.get("oid", ""))
                    filled = float(o.get("filledSize", "0") or "0")
                    if oid and filled > 0:
                        self.state.notified_fills.add(oid)
                        self.state._notified_amounts[oid] = filled
            self.log(f"Pre-seeded {len(self.state.notified_fills)} existing fills")
            # Pre-seed sell fill tracker
            if not hasattr(self.state, '_sell_fills_tracked'):
                self.state._sell_fills_tracked = {}
            for o in (resp.get('data', {}).get('list', []) if str(resp.get('code', '')) == '0' else []) + (resp2.get('data', {}).get('list', []) if str(resp2.get('code', '')) == '0' else []):
                oid = o.get('id', o.get('oid', ''))
                side = o.get('side', '').upper()
                asset_id = o.get('assetId', '')
                if side == 'SELL' and asset_id in (self.state.yes_asset_id, self.state.no_asset_id):
                    sfilled = float(o.get('filledSize', o.get('size', '0')) or '0')
                    if sfilled > 0:
                        self.state._sell_fills_tracked[oid] = sfilled
        except Exception as e:
            self.log(f"Seed fills error: {e}", "WARN")
        await self._main_loop()

    # ─── Main Loop ───

    async def _main_loop(self):
        """Every 60s: check fills → post sells, then deploy available balance."""
        try:
            # Initial deploy
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

                # Combine: closed + active orders with partial fills
                _all_filled = _closed_orders + [
                    o for o in _open_orders
                    if float(o.get("filledSize", "0") or "0") > 0
                ]

                # Step 0: Notify all fills
                await self._notify_all_fills(_all_filled)

                # Step 1: Check for filled buys → post sells
                await self._check_fills_post_sells(_all_filled)

                # Step 1.5: Check sell fills
                await self._check_sell_fills(_all_filled)

                # Step 2: Deploy any available balance into new buy pairs
                await self._deploy_balance()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log(f"致命错误: {e}", "ERROR")
            self.state.running = False
            self.running = False
        self.auto_rebalance = None  # Set by AccountInstance for cost layer tracking

    async def _check_fills_post_sells(self, order_list=None):
        """Check filled buy orders and post corresponding sell orders.
        
        IMPORTANT: sell price is derived from the buy order's actual fill price,
        NOT from self.state.sell_a/sell_b. This prevents the update_price() race
        condition where changing price mid-loop would cause sells at wrong prices.
        """
        try:
            # Use passed order_list if available, otherwise fetch
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
                # Check if we've already processed the full filled amount
                prev_processed = self.state.pending_sells.get(oid, 0.0)

                # Only orders created after strategy started
                created = int(o.get("createdAt", "0") or "0")
                if created < self.state.start_time:
                    continue

                asset_id = o.get("assetId", "")
                side = o.get("side", "").upper()
                filled_size = o.get("filledSize", o.get("size", "0"))

                # Only care about our BUY orders that actually filled
                status = o.get("status", "").upper()
                # Handle both FILLED and PARTIALLY_FILLED
                if status not in ("FILLED", "PARTIALLY_FILLED", "ACTIVE"):
                    continue
                if side != "BUY":
                    continue
                if asset_id not in (self.state.yes_asset_id, self.state.no_asset_id):
                    continue

                filled = float(filled_size)
                if filled < 0.01:
                    continue
                # Only process the NEW portion (incremental fill)
                new_fill = filled - prev_processed
                if new_fill < 0.01:
                    continue

                # Derive sell price from the ORDER's actual buy price (not current state)
                # This prevents the update_price() race condition
                buy_price_str = o.get("price", "")
                if buy_price_str:
                    buy_cents = round(float(buy_price_str) * 100)
                    sell_cents = buy_cents + 1
                else:
                    # Fallback to state if order doesn't have price field
                    if asset_id == self.state.yes_asset_id:
                        sell_cents = self.state.sell_a
                    else:
                        sell_cents = self.state.sell_b

                if asset_id == self.state.yes_asset_id:
                    label = "Yes"
                else:
                    label = "No"

                sell_price = self._cents_to_price_str(sell_cents)

                # Post sell order
                size_str = f"{new_fill:.10f}".rstrip("0").rstrip(".")
                sell_resp = await self.client.place_order(
                    asset_id=asset_id,
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
                    self.log(f"挂 Sell {label} @{sell_price} x{size_str}")
                    if self.notifier:
                        asyncio.create_task(self.notifier.send(f"[{self.account_name}] ✅ Buy {label} 成交 x{size_str} → 挂 Sell @{sell_price}"))
                else:
                    msg = sell_resp.get("message", "")
                    # Skip marking oid so we retry, but dont count as failure if token issue
                    if "Insufficient" in msg or "Minimum" in msg:
                        self.state.pending_sells[oid] = filled  # Don't retry this one
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
        Scans ALL sell orders for current assets (no posted_sells dependency, survives restart).
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

            # Track sells for both yes and no assets
            my_assets = set()
            if self.state.yes_asset_id:
                my_assets.add(self.state.yes_asset_id)
            if self.state.no_asset_id:
                my_assets.add(self.state.no_asset_id)

            for o in order_list:
                oid = o.get("id", o.get("oid", ""))
                if not oid:
                    continue
                side = o.get("side", "").upper()
                if side != "SELL":
                    continue
                asset_id = o.get("assetId", "")
                if asset_id not in my_assets:
                    continue
                status = o.get("status", "").upper()
                if status not in ("FILLED", "PARTIALLY_FILLED", "ACTIVE"):
                    continue
                filled = float(o.get("filledSize", o.get("size", "0")) or "0")
                price = o.get("price", "?")
                if filled < 0.01:
                    continue
                prev_sell_filled = self.state._sell_fills_tracked.get(oid, 0.0)
                new_sell_fill = filled - prev_sell_filled
                if new_sell_fill < 0.5:
                    continue
                self.state._sell_fills_tracked[oid] = filled
                self.state.posted_sells.add(oid)
                profit = new_sell_fill * 0.01
                self.state.total_profit += profit
                if status == "FILLED":
                    self.state.posted_sells.discard(oid)
                    pass  # keep in tracked to prevent re-notify
                label = "Yes" if asset_id == self.state.yes_asset_id else "No"
                self.log(f"\U0001f4b0 Sell {label} \u6210\u4ea4 x{new_sell_fill:.0f} @{price} | +{profit:.2f} xp")
                if self.notifier:
                    import asyncio
                    asyncio.create_task(self.notifier.send(
                        f"[{self.account_name}] \U0001f4b0 Sell {label} \u6210\u4ea4 x{int(new_sell_fill)} @{price} | +{profit:.2f} xp"
                    ))
        except Exception as e:
            self.log(f"\u68c0\u67e5sell\u6210\u4ea4\u5f02\u5e38: {e}", "WARN")

    async def _deploy_balance(self):
        """Deploy available balance into Buy Yes + Buy No pairs."""
        try:

            balance = await self.client.get_balance()
            available = self._parse_available(balance)
            available = available * self.state.position_pct / 100.0

            pair_cost = (self.state.price_a + self.state.price_b) / 100.0
            pairs = int(available / pair_cost)

            if pairs < 1:
                return

            # OKX minimum order notional = 1 xp per side
            # Ensure: pairs * price_a/100 >= 1 AND pairs * price_b/100 >= 1
            min_yes = max(1, int(1.0 / (self.state.price_a / 100.0)) + 1)
            min_no = max(1, int(1.0 / (self.state.price_b / 100.0)) + 1)
            min_pairs = max(min_yes, min_no)
            if pairs < min_pairs:
                return  # Not enough balance for minimum order

            yes_price = self._cents_to_price_str(self.state.price_a)
            no_price = self._cents_to_price_str(self.state.price_b)

            # Place Buy Yes
            resp_yes = await self.client.place_order(
                asset_id=self.state.yes_asset_id,
                side="buy",
                price=yes_price,
                size=str(pairs),
                tif="gtc",
            )
            if str(resp_yes.get("code", "")) == "0":
                self.state.consecutive_failures = 0
                self.log(f"挂 Buy Yes @{self.state.price_a}¢ x{pairs}")
            else:
                msg = resp_yes.get("message", "")
                if "Minimum" in msg or "Insufficient" in msg:
                    return  # Not a real failure, just skip
                self._record_failure(f"Buy Yes 失败: {msg}")
                if self.notifier:
                    asyncio.create_task(self.notifier.send(f"[{self.account_name}] ⚠️ Buy Yes 失败: {msg}"))
                return

            # Place Buy No
            resp_no = await self.client.place_order(
                asset_id=self.state.no_asset_id,
                side="buy",
                price=no_price,
                size=str(pairs),
                tif="gtc",
            )
            if str(resp_no.get("code", "")) == "0":
                self.state.consecutive_failures = 0
                self.log(f"挂 Buy No @{self.state.price_b}¢ x{pairs}")
            else:
                msg = resp_no.get("message", "")
                if "Minimum" in msg or "Insufficient" in msg:
                    return
                self._record_failure(f"Buy No 失败: {msg}")
                if self.notifier:
                    asyncio.create_task(self.notifier.send(f"[{self.account_name}] ⚠️ Buy No 失败: {msg}"))

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
                    f"🚨 Manual 策略连续失败 {self.state.consecutive_failures} 次\n已自动停止"
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
        if 4 <= cents <= 96:
            return f"{price:.2f}"
        else:
            return f"{price:.3f}"

    def get_state(self) -> dict:
        return {
            "running": self.state.running,
            "phase": "running" if self.state.running else "idle",
            "price_a": self.state.price_a,
            "price_b": self.state.price_b,
            "size": 0,
            "total_merges": self.state.total_sells_posted,
            "total_profit": self.state.total_profit,
            "consecutive_failures": self.state.consecutive_failures,
            "logs": self.state.logs[-20:],
        }
