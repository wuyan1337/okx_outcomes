"""Manual dual-side spread strategy.

Logic:
- Set price a (e.g. 55). Buy Yes@a¢, Buy No@(99-a)¢, split balance evenly.
- When a Buy Yes fill is detected → post Sell Yes@(a+1)¢
- When a Buy No fill is detected → post Sell No@(100-a)¢
- When Sells fill → balance increases → re-deploy into new Buy pairs
- Sell targets are one cent above the fill and round up when that target is off tick.

Persistence: pending_sells saved to JSON file to survive restarts.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Set, Dict
from dataclasses import dataclass, field
from datetime import datetime

from ..core.price_utils import (
    align_cents_for_okx,
    as_decimal,
    format_okx_cents,
    price_to_cents,
)
from .size_utils import normalize_size_str

if TYPE_CHECKING:
    from ..core.client import OKXOutcomesClient

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_FAILURES = 5
SELLS_FILE_D = Path(__file__).resolve().parents[2] / "data" / "pending_sells_d_{account_id}.json"


@dataclass
class ManualStateD:
    running: bool = False
    price_a: float = 0.0
    price_b: float = 0.0
    sell_a: float = 0.0
    sell_b: float = 0.0
    market_id: str = ""
    yes_asset_id: str = ""
    no_asset_id: str = ""
    # Buy order ID -> filledSize already covered by accepted sell orders.
    pending_sells: Dict[str, float] = field(default_factory=dict)
    posted_sells: Set[str] = field(default_factory=set)  # sell order IDs we posted
    sell_retry_counts: Dict[str, int] = field(default_factory=dict)
    sell_retry_after: Dict[str, float] = field(default_factory=dict)
    deployment_blocked: bool = False
    pair_retry_count: int = 0
    pair_retry_after: float = 0.0
    start_time: int = 0  # millis timestamp when strategy started
    # Stats
    total_sells_posted: int = 0
    total_profit: float = 0.0
    consecutive_failures: int = 0
    position_pct: int = 100
    scan_interval: float = 0.3  # seconds between main loop scans
    stabilize_secs: float = 0.3  # seconds for rebalance stabilization
    logs: list = field(default_factory=list)
    notified_fills: Set[str] = field(default_factory=set)  # order IDs already notified


class ManualStrategyD:
    """Spread market making on explicit YES and NO outcome assets."""

    def __init__(self, client: OKXOutcomesClient, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.state = ManualStateD()
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self.notifier = None
        self.running = False
        self.auto_rebalance = None
        self.cycles = 0
        self._sells_file = Path(str(SELLS_FILE_D).format(account_id=account_id))
        self._load_pending_sells()

    def _load_pending_sells(self):
        """Load pending_sells from disk."""
        try:
            if self._sells_file.exists():
                with open(self._sells_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                raw = data.get("oids", [])
                if isinstance(raw, dict):
                    self.state.pending_sells = {k: float(v) for k, v in raw.items()}
                else:
                    # Legacy lists contain order IDs treated as fully handled.
                    self.state.pending_sells = {oid: 999999.0 for oid in raw}
                self.state.start_time = data.get("start_time", 0)
                logger.info(f"[D] Loaded {len(self.state.pending_sells)} pending sells from disk")
        except Exception as e:
            logger.warning(f"[D] Failed to load pending sells: {e}")

    def _save_pending_sells(self):
        """Persist pending_sells to disk."""
        try:
            self._sells_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._sells_file, "w", encoding="utf-8") as f:
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
                    price_a: float, size: int = 0, position_pct: int = 100, scan_interval: float = 0.3, stabilize_secs: float = 0.3):
        """Start the spread strategy."""
        if self.state.running:
            return

        a, b, sell_a, sell_b = self._derive_prices(price_a)

        self.state.running = True
        self.running = True
        self.state.market_id = market_id
        self.state.yes_asset_id = yes_asset_id
        self.state.no_asset_id = no_asset_id
        self.state.price_a = float(a)
        self.state.position_pct = max(1, min(100, position_pct))
        self.state.scan_interval = max(0.3, min(120.0, float(scan_interval)))
        self.state.stabilize_secs = max(0.3, min(120.0, float(stabilize_secs)))
        self.state.price_b = float(b)
        self.state.sell_a = float(sell_a)
        self.state.sell_b = float(sell_b)
        self.state.consecutive_failures = 0
        self.state.deployment_blocked = False
        self.state.pair_retry_count = 0
        self.state.pair_retry_after = 0.0

        import time as _time
        self.state.start_time = int(_time.time() * 1000)
        self._save_pending_sells()
        self.log(f"Manual started: Buy Yes@{a}¢ / No@{b}¢ → Sell Yes@{sell_a}¢ / No@{sell_b}¢")

        self._main_task = asyncio.create_task(self._seed_then_run())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Stop strategy. Does NOT cancel orders."""
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

    async def update_price(self, price_a: float):
        """Update price while running — only affects NEW buy orders.
        Existing fills still get sell@(original_buy_price+1)."""
        a, b, sell_a, sell_b = self._derive_prices(price_a)
        self.state.price_a = float(a)
        self.state.price_b = float(b)
        self.state.sell_a = float(sell_a)
        self.state.sell_b = float(sell_b)
        self.log(f"更新: Buy Yes@{a}¢ No@{b}¢ → Sell Yes@{sell_a}¢ No@{sell_b}¢")


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
                    f"[{self.account_name}] {emoji} <b>{side} {label}</b> @{price} x{normalize_size_str(new_fill)} \u6210\u4ea4"
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
        """Check fills, post matching sells, and redeploy at scan_interval."""
        try:
            # Initial deploy
            await self._deploy_balance()

            while self.state.running:
                await asyncio.sleep(self.state.scan_interval)
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
    async def _check_fills_post_sells(self, order_list=None):
        """Check filled buy orders and post corresponding sell orders.
        
        The sell price is derived from the buy order so a later configuration
        update cannot change the exit price for an existing fill.
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
                if self.state.sell_retry_after.get(oid, 0.0) > asyncio.get_running_loop().time():
                    continue

                buy_price_str = o.get("price", "")
                if buy_price_str:
                    buy_cents = price_to_cents(buy_price_str)
                    requested_sell_cents = buy_cents + Decimal("1")
                    sell_cents = align_cents_for_okx(requested_sell_cents, "sell")
                else:
                    if asset_id == self.state.yes_asset_id:
                        buy_cents = as_decimal(self.state.price_a)
                        sell_cents = as_decimal(self.state.sell_a)
                    else:
                        buy_cents = as_decimal(self.state.price_b)
                        sell_cents = as_decimal(self.state.sell_b)

                if asset_id == self.state.yes_asset_id:
                    label = "Yes"
                else:
                    label = "No"

                sell_price = self._cents_to_price_str(sell_cents, "sell")

                size_str = f"{new_fill:.10f}".rstrip("0").rstrip(".")
                sell_notional = sell_cents / Decimal("100") * as_decimal(new_fill)
                if sell_notional < Decimal("1"):
                    if await self._is_full_available_position(asset_id, new_fill):
                        self.log(
                            f"Sell {label} @{sell_price} x{size_str} uses OKX sell-all exception"
                        )
                    else:
                        self._defer_failed_sell(
                            oid,
                            label,
                            f"{size_str} x {sell_cents}\u00a2 = {sell_notional:.2f} xp below 1 xp minimum",
                        )
                        continue
                sell_resp = await self.client.place_order(
                    asset_id=asset_id,
                    side="sell",
                    price=sell_price,
                    size=size_str,
                    tif="gtc",
                )

                if str(sell_resp.get("code", "")) == "0":
                    self.state.pending_sells[oid] = filled
                    self.state.sell_retry_counts.pop(oid, None)
                    self.state.sell_retry_after.pop(oid, None)
                    sell_oid = sell_resp.get("data", {}).get("oid", "") or sell_resp.get("data", {}).get("orderId", "")
                    if sell_oid:
                        self.state.posted_sells.add(sell_oid)
                    self.state.total_sells_posted += 1
                    self.state.consecutive_failures = 0
                    changed = True
                    if self.auto_rebalance:
                        self.auto_rebalance.add_to_layer(
                            asset_id,
                            buy_cents,
                            new_fill,
                            market_id=self.state.market_id,
                        )
                    self.log(f"挂 Sell {label} @{sell_price} x{size_str}")
                    if self.notifier:
                        asyncio.create_task(self.notifier.send(f"[{self.account_name}] ✅ Buy {label} 成交 x{size_str} → 挂 Sell @{sell_price}"))
                else:
                    msg = str(sell_resp.get("message") or sell_resp.get("msg") or "unknown error")
                    if "Insufficient" in msg or "Minimum" in msg:
                        self._defer_failed_sell(oid, label, msg)
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
                if self.auto_rebalance:
                    self.auto_rebalance.remove_from_highest_layer(
                        asset_id,
                        new_sell_fill,
                        market_id=self.state.market_id,
                    )
                if status == "FILLED":
                    self.state.posted_sells.discard(oid)
                label = "Yes" if asset_id == self.state.yes_asset_id else "No"
                self.log(f"\U0001f4b0 Sell {label} \u6210\u4ea4 x{normalize_size_str(new_sell_fill)} @{price} | +{profit:.2f} xp")
                if self.notifier:
                    import asyncio
                    asyncio.create_task(self.notifier.send(
                        f"[{self.account_name}] \U0001f4b0 Sell {label} \u6210\u4ea4 x{normalize_size_str(new_sell_fill)} @{price} | +{profit:.2f} xp"
                    ))
        except Exception as e:
            self.log(f"\u68c0\u67e5sell\u6210\u4ea4\u5f02\u5e38: {e}", "WARN")

    async def _deploy_balance(self):
        """Deploy available balance into Buy Yes + Buy No pairs."""
        try:
            if self.state.deployment_blocked:
                return
            if self.state.pair_retry_after > asyncio.get_running_loop().time():
                return

            balance = await self.client.get_balance()
            available = self._parse_available(balance)

            locked_in_buys = Decimal("0")
            try:
                resp_open = await self.client.get_orders(status="open")
                if str(resp_open.get("code", "")) == "0":
                    for o in resp_open.get("data", {}).get("list", []):
                        if o.get("side", "").upper() == "BUY":
                            aid = o.get("assetId", "")
                            if aid in (self.state.yes_asset_id, self.state.no_asset_id):
                                sz = as_decimal(o.get("size", "0") or "0") - as_decimal(o.get("filledSize", "0") or "0")
                                px = as_decimal(o.get("price", "0") or "0")
                                locked_in_buys += sz * px
            except Exception:
                pass

            total_capital = available + locked_in_buys
            max_deploy = total_capital * self.state.position_pct / Decimal("100")
            remaining_budget = max_deploy - locked_in_buys
            if remaining_budget <= 0:
                return

            pair_cost = (as_decimal(self.state.price_a) + as_decimal(self.state.price_b)) / Decimal("100")
            pairs = int((remaining_budget / pair_cost).to_integral_value(rounding=ROUND_FLOOR))

            if pairs < 1:
                return

            yes_cost = as_decimal(self.state.price_a) / Decimal("100")
            no_cost = as_decimal(self.state.price_b) / Decimal("100")
            min_yes = max(1, int((Decimal("1") / yes_cost).to_integral_value(rounding=ROUND_CEILING)))
            min_no = max(1, int((Decimal("1") / no_cost).to_integral_value(rounding=ROUND_CEILING)))
            min_pairs = max(min_yes, min_no)
            if pairs < min_pairs:
                return  # Not enough balance for minimum order

            yes_price = self._cents_to_price_str(self.state.price_a, "buy")
            no_price = self._cents_to_price_str(self.state.price_b, "buy")

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
                msg = str(resp_yes.get("message") or resp_yes.get("msg") or "unknown error")
                self._defer_pair_retry(f"Buy Yes 失败: {msg}")
                if "Minimum" in msg or "Insufficient" in msg:
                    return
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
                self.state.pair_retry_count = 0
                self.state.pair_retry_after = 0.0
                self.log(f"挂 Buy No @{self.state.price_b}¢ x{pairs}")
            else:
                msg = str(resp_no.get("message") or resp_no.get("msg") or "unknown error")
                self.log(f"Buy No 失败: {msg}; 正在撤回本轮 Buy Yes", "ERROR")
                rollback_safe = await self._rollback_yes_order(resp_yes)
                if rollback_safe:
                    self._defer_pair_retry(f"Pair deployment rolled back after Buy No failure: {msg}")
                    if "Minimum" not in msg and "Insufficient" not in msg:
                        self._record_failure(f"Buy No 失败: {msg}")
                if self.notifier:
                    asyncio.create_task(self.notifier.send(
                        f"[{self.account_name}] ⚠️ Buy No 失败: {msg}\n已执行本轮 Buy Yes 回滚检查，请查看策略状态"
                    ))
                return

            self.cycles += 1

        except Exception as e:
            self._record_failure(f"Deploy 异常: {e}")

    def _defer_failed_sell(self, oid: str, label: str, message: str):
        attempt = self.state.sell_retry_counts.get(oid, 0) + 1
        delay = min(60, 2 ** min(attempt, 6))
        self.state.sell_retry_counts[oid] = attempt
        self.state.sell_retry_after[oid] = asyncio.get_running_loop().time() + delay
        warning = (
            f"Sell {label} 失败 ({attempt}): {message}; 裸仓风险，"
            f"{delay}s 后重试"
        )
        self.log(warning, "ERROR")
        if self.notifier:
            asyncio.create_task(self.notifier.send(f"[{self.account_name}] ⚠️ {warning}"))

    async def _is_full_available_position(self, asset_id: str, quantity) -> bool:
        """Confirm the order would sell the asset's entire unfrozen position."""
        try:
            response = await self.client.get_positions(
                status="open",
                market_id=self.state.market_id or None,
            )
            if str(response.get("code", "")) != "0":
                raise RuntimeError(
                    response.get("message") or response.get("msg") or "position query failed"
                )
            raw = response.get("data") or {}
            positions = raw.get("list", []) if isinstance(raw, dict) else raw
            if not isinstance(positions, list):
                raise ValueError("invalid position response")

            total = Decimal("0")
            available = Decimal("0")
            matched = False
            for position in positions:
                position_asset_id = str(position.get("assetId") or "")
                if not position_asset_id:
                    market_id = str(position.get("marketId") or "")
                    token_index = str(position.get("tokenIndex") or "")
                    token_name = str(position.get("tokenName") or "").upper()
                    if market_id:
                        is_no = token_index == "2" or token_name == "NO"
                        position_asset_id = f"{market_id}{'100' if is_no else '000'}"
                if position_asset_id != str(asset_id):
                    continue
                if "size" not in position or "availableSize" not in position:
                    return False
                matched = True
                total += as_decimal(position["size"])
                available += as_decimal(position["availableSize"])

            requested = as_decimal(quantity)
            return matched and requested > 0 and requested == available == total
        except Exception as exc:
            self.log(f"Sell-all position check failed: {exc}", "WARN")
            return False

    def _defer_pair_retry(self, message: str):
        self.state.pair_retry_count += 1
        delay = min(60, 2 ** min(self.state.pair_retry_count, 6))
        self.state.pair_retry_after = asyncio.get_running_loop().time() + delay
        self.log(f"{message}; {delay}s 后重试", "WARN")

    def _block_new_deployments(self, message: str):
        self.state.deployment_blocked = True
        warning = f"新部署已锁定: {message}；现有成交仍继续管理"
        self.log(warning, "ERROR")
        if self.notifier:
            asyncio.create_task(self.notifier.send(f"[{self.account_name}] 🚨 {warning}"))

    async def _rollback_yes_order(self, response: dict, confirmation_attempts: int = 10) -> bool:
        """Cancel the first leg and confirm that it created no one-sided fill."""
        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        order_id = str(
            response.get("_oid")
            or response.get("oid")
            or data.get("oid")
            or data.get("orderId")
            or data.get("id")
            or ""
        )
        client_order_id = str(
            response.get("_clientOrderId")
            or response.get("clientOrderId")
            or data.get("clientOrderId")
            or ""
        )
        if not order_id and not client_order_id:
            self._block_new_deployments("Buy Yes 已受理，但响应没有可撤单的订单标识，终态未知")
            return False

        try:
            cancel_kwargs = {"order_id": order_id} if order_id else {"client_order_id": client_order_id}
            cancel_resp = await self.client.cancel_order(
                asset_id=self.state.yes_asset_id,
                **cancel_kwargs,
            )
        except Exception as exc:
            self._block_new_deployments(f"Buy Yes 撤单异常: {exc}，终态未知")
            return False

        if str(cancel_resp.get("code", "")) != "0":
            error = cancel_resp.get("message") or cancel_resp.get("msg") or "unknown error"
            self._block_new_deployments(f"Buy Yes 撤单被拒绝: {error}，终态未知")
            return False

        status, filled = await self._wait_order_terminal(
            order_id,
            client_order_id,
            attempts=confirmation_attempts,
        )
        if not status:
            self._block_new_deployments("Buy Yes 撤单已受理，但无法确认最终状态")
            return False
        if filled > Decimal("0") or status in ("FILLED", "PARTIALLY_FILLED"):
            self._block_new_deployments(
                f"Buy Yes 撤单终态 {status}，已有 {normalize_size_str(filled)} 单边成交"
            )
            return False

        self.log(f"本轮 Buy Yes 撤单已确认 ({status})")
        return True

    async def _wait_order_terminal(
        self,
        order_id: str,
        client_order_id: str,
        attempts: int = 10,
    ) -> tuple[str, Decimal]:
        terminal = {"CANCELLED", "FILLED", "PARTIALLY_FILLED", "FAILED", "EXPIRED"}
        for attempt in range(attempts):
            candidates = []
            queried_by_order_id = False
            if order_id and hasattr(self.client, "get_order"):
                try:
                    resp = await self.client.get_order(order_id)
                    if str(resp.get("code", "")) == "0" and isinstance(resp.get("data"), dict):
                        candidates.append(resp["data"])
                        queried_by_order_id = True
                except Exception:
                    pass
            if not queried_by_order_id:
                try:
                    for query_status in ("closed", "open"):
                        resp = await self.client.get_orders(
                            status=query_status,
                            market_id=self.state.market_id or None,
                        )
                        if str(resp.get("code", "")) == "0":
                            candidates.extend((resp.get("data") or {}).get("list", []))
                except Exception:
                    pass

            for order in candidates:
                candidate_oid = str(order.get("oid") or order.get("id") or order.get("orderId") or "")
                candidate_cloid = str(order.get("clientOrderId") or "")
                if not ((order_id and candidate_oid == order_id) or
                        (client_order_id and candidate_cloid == client_order_id)):
                    continue
                status = str(order.get("status") or "").upper()
                if status in terminal:
                    return status, as_decimal(order.get("filledSize", "0") or "0")
            if attempt + 1 < attempts:
                await asyncio.sleep(0.3)
        return "", Decimal("0")

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

    def _parse_available(self, balance_resp: dict) -> Decimal:
        if str(balance_resp.get("code", "")) != "0":
            return Decimal("0")
        data = balance_resp.get("data", [])
        for entry in data:
            if entry.get("oddsType") == "points":
                return as_decimal(entry.get("available", "0"))
        return Decimal("0")

    @staticmethod
    def _derive_prices(price_a):
        a = align_cents_for_okx(price_a, "buy")
        b = align_cents_for_okx(Decimal("99") - a, "buy")
        sell_a = align_cents_for_okx(a + Decimal("1"), "sell")
        sell_b = align_cents_for_okx(Decimal("100") - a, "sell")
        return a, b, sell_a, sell_b

    def _cents_to_price_str(self, cents, side="nearest") -> str:
        return format_okx_cents(cents, side)

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
            "deployment_blocked": self.state.deployment_blocked,
            "logs": self.state.logs[-20:],
        }
