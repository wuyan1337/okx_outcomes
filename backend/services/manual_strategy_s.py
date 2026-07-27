"""Strategy: Manual-S (Single-side) — Buy one side@a, Sell@(a+1), repeat.

Logic:
- User picks either Yes OR No side and sets price a.
- Buy {side}@a¢
- When Buy fills → post the configured Sell for the same outcome asset
- When Sell fills → balance freed → re-deploy
- The default Sell target is one cent above the normalized Buy price.

Persistence: pending_sells saved to JSON file to survive restarts.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import json
import logging
import secrets
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
from ..core.logging_config import audit_event, log_context
from .size_utils import normalize_size_str

if TYPE_CHECKING:
    from ..core.client import OKXOutcomesClient

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_FAILURES = 5
SELLS_FILE_S = Path(__file__).resolve().parents[2] / "data" / "pending_sells_s_{account_id}.json"


@dataclass
class ManualStateS:
    running: bool = False
    mode: str = "classic"     # "classic" (MS) or "bid-sell" (BS)
    side: str = ""            # "yes" or "no"
    price_a: float = 0.0
    sell_price: float = 0.0
    market_id: str = ""
    yes_asset_id: str = ""
    no_asset_id: str = ""
    asset_id: str = ""        # The one we're trading (yes or no asset)
    pending_sells: Dict[str, float] = field(default_factory=dict)
    posted_sells: Set[str] = field(default_factory=set)  # sell order IDs we posted
    sell_retry_counts: Dict[str, int] = field(default_factory=dict)
    sell_retry_after: Dict[str, float] = field(default_factory=dict)
    start_time: int = 0
    total_sells_posted: int = 0
    total_profit: float = 0.0
    consecutive_failures: int = 0
    notified_fills: Set[str] = field(default_factory=set)  # order IDs already notified
    position_pct: int = 100  # % of available balance to deploy
    scan_interval: float = 0.3  # seconds between main loop scans
    stabilize_secs: float = 0.3  # seconds for rebalance stabilization
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
        self.auto_rebalance = None
        self._run_id = ""
        self._sells_file = Path(str(SELLS_FILE_S).format(account_id=account_id))
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
                    self.state.pending_sells = {oid: 999999.0 for oid in raw}
                self.state.start_time = data.get("start_time", 0)
                logger.info(f"[S] Loaded {len(self.state.pending_sells)} pending sells from disk")
        except Exception as e:
            logger.warning(f"[S] Failed to load pending sells: {e}")

    def _save_pending_sells(self):
        """Persist pending_sells to disk."""
        try:
            self._sells_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._sells_file, "w", encoding="utf-8") as f:
                json.dump({
                    "oids": dict(self.state.pending_sells),
                    "start_time": self.state.start_time,
                }, f)
            audit_event(
                "manual.pending_sells_saved",
                account_id=self.account_id,
                strategy="manual-s",
                run_id=self._run_id,
                path=self._sells_file,
                pending_count=len(self.state.pending_sells),
            )
        except Exception as e:
            logger.warning(f"[S] Failed to save pending sells: {e}")
            audit_event(
                "manual.pending_sells_save_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                strategy="manual-s",
                run_id=self._run_id,
                error=e,
            )

    def log(self, msg: str, level: str = "INFO"):
        now = datetime.now()
        entry = f"[{now.strftime('%I:%M:%S')}.{now.microsecond // 1000:03d} {now.strftime('%p')}] {level} {msg}"
        self.state.logs.append(entry)
        if len(self.state.logs) > 200:
            self.state.logs = self.state.logs[-200:]
        level_name = str(level).upper()
        log_level = {
            "DEBUG": logging.DEBUG,
            "WARN": logging.WARNING,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }.get(level_name, logging.INFO)
        logger.log(log_level, "[Manual-S:%s run=%s] %s", self.account_id, self._run_id, msg)
        audit_event(
            "strategy.message",
            level=log_level,
            account_id=self.account_id,
            strategy="manual-s",
            run_id=self._run_id,
            mode=self.state.mode,
            message=msg,
        )

    async def start(self, market_id: str, yes_asset_id: str, no_asset_id: str,
                    side: str, price_a: float, sell_price: float = 0, mode: str = "classic", size: int = 0, position_pct: int = 100, scan_interval: float = 0.3, stabilize_secs: float = 0.3):
        """Start single-side strategy."""
        if self.state.running:
            return

        self._run_id = secrets.token_hex(8)

        mode = str(mode).strip().lower()
        if mode not in ("classic", "bid-sell"):
            raise ValueError("mode must be classic or bid-sell")

        a, actual_sell = self._derive_prices(price_a, sell_price)
        if mode == "bid-sell":
            if a < Decimal("0.1"):
                raise ValueError("bid-sell buy price must be at least 0.1¢")
            if actual_sell - a != Decimal("1"):
                raise ValueError(
                    "bid-sell requires an executable sell-buy spread of exactly 1.0¢"
                )

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
        self.state.price_a = float(a)
        self.state.sell_price = float(actual_sell)
        self.state.mode = mode
        self.state.position_pct = max(1, min(100, position_pct))
        self.state.scan_interval = max(0.3, min(120.0, float(scan_interval)))
        self.state.stabilize_secs = max(0.3, min(120.0, float(stabilize_secs)))
        self.state.consecutive_failures = 0

        import time as _time
        self.state.start_time = int(_time.time() * 1000)
        self._save_pending_sells()

        label = "Yes" if side == "yes" else "No"
        self.log(f"Manual-S started: Buy {label}@{a}¢ → Sell {label}@{actual_sell}¢")

        # Pre-seed notified_fills with existing closed orders so we don't spam old fills on start
        audit_event(
            "strategy.started",
            account_id=self.account_id,
            strategy="manual-s",
            run_id=self._run_id,
            mode=self.state.mode,
            market_id=market_id,
            asset_id=self.state.asset_id,
            side=self.state.side,
            buy_price_cents=str(self.state.price_a),
            sell_price_cents=str(self.state.sell_price),
            scan_interval=self.state.scan_interval,
        )
        with log_context(
            account_id=self.account_id,
            strategy="manual-s",
            run_id=self._run_id,
            mode=self.state.mode,
        ):
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
        audit_event(
            "strategy.stopped",
            account_id=self.account_id,
            strategy="manual-s",
            run_id=self._run_id,
            mode=self.state.mode,
            pending_count=len(self.state.pending_sells),
        )
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
                if self.state.sell_retry_after.get(oid, 0.0) > asyncio.get_running_loop().time():
                    continue

                sell_cents = self.state.sell_price
                sell_price = self._cents_to_price_str(sell_cents, "sell")
                label = "Yes" if self.state.side == "yes" else "No"

                size_str = f"{new_fill:.10f}".rstrip("0").rstrip(".")
                sell_notional = as_decimal(sell_cents) / Decimal("100") * as_decimal(new_fill)
                if sell_notional < Decimal("1"):
                    if await self._is_full_available_position(self.state.asset_id, new_fill):
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
                    asset_id=self.state.asset_id,
                    side="sell",
                    price=sell_price,
                    size=size_str,
                    tif="gtc",
                )

                if str(sell_resp.get("code", "")) != "0" and "Invalid request parameters" in str(sell_resp.get("message", "")):
                    safe_units = int(new_fill)
                    safe_notional = as_decimal(sell_cents) / Decimal("100") * safe_units
                    if safe_units >= 1 and safe_notional >= Decimal("1") and str(safe_units) != size_str:
                        self.log(f"Sell {label} 参数失败，降级整数重试: {size_str} -> {safe_units}")
                        sell_resp = await self.client.place_order(
                            asset_id=self.state.asset_id,
                            side="sell",
                            price=sell_price,
                            size=str(safe_units),
                            tif="gtc",
                        )
                        if str(sell_resp.get("code", "")) == "0":
                            filled = prev_processed + safe_units
                            new_fill = float(safe_units)
                            size_str = str(safe_units)

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
                        buy_cents = (
                            price_to_cents(o["price"])
                            if o.get("price")
                            else as_decimal(self.state.price_a)
                        )
                        self.auto_rebalance.add_to_layer(
                            self.state.asset_id,
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
                    self.auto_rebalance.remove_from_highest_layer(
                        self.state.asset_id,
                        new_sell_fill,
                        market_id=self.state.market_id,
                    )
                if status == "FILLED":
                    self.state.posted_sells.discard(oid)
                label = "Yes" if self.state.side == "yes" else "No"
                self.log(f"\U0001f4b0 Sell {label} \u6210\u4ea4 x{normalize_size_str(new_sell_fill)} @{price} | +{profit:.2f} xp")
                if self.notifier:
                    import asyncio
                    asyncio.create_task(self.notifier.send(
                        f"[{self.account_name}] \U0001f4b0 Sell {label} \u6210\u4ea4 x{normalize_size_str(new_sell_fill)} @{price} | +{profit:.2f} xp"
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
                    f"[{self.account_name}] {emoji} <b>{side} {label}</b> @{price} x{normalize_size_str(new_fill)} \u6210\u4ea4"
                ))
        except Exception as e:
            self.log(f"notify_all_fills \u5f02\u5e38: {e}", "WARN")

    async def _deploy_balance(self):
        try:

            balance = await self.client.get_balance()
            available = self._parse_available(balance)

            locked_in_buys = Decimal("0")
            try:
                resp_open = await self.client.get_orders(status="open")
                if str(resp_open.get("code", "")) == "0":
                    for o in resp_open.get("data", {}).get("list", []):
                        if o.get("side", "").upper() == "BUY" and o.get("assetId", "") == self.state.asset_id:
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

            cost_per_unit = as_decimal(self.state.price_a) / Decimal("100")
            units = int((remaining_budget / cost_per_unit).to_integral_value(rounding=ROUND_FLOOR))

            if units < 1:
                return

            min_units = max(1, int((Decimal("1") / cost_per_unit).to_integral_value(rounding=ROUND_CEILING)))
            if units < min_units:
                return

            price_str = self._cents_to_price_str(self.state.price_a, "buy")
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
                    result = await self.client.heartbeat()
                    if str(result.get("code", "")) != "0":
                        self.log(
                            "Heartbeat failed: "
                            + str(result.get("message") or result.get("msg") or result.get("code")),
                            "WARN",
                        )
                    else:
                        audit_event(
                            "strategy.heartbeat_ok",
                            account_id=self.account_id,
                            strategy="manual-s",
                            run_id=self._run_id,
                            mode=self.state.mode,
                        )
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
    def _derive_prices(price_a, sell_price=0):
        a = align_cents_for_okx(price_a, "buy")
        requested_sell = as_decimal(sell_price)
        if requested_sell <= 0:
            requested_sell = a + Decimal("1")
        return a, align_cents_for_okx(requested_sell, "sell")

    def _cents_to_price_str(self, cents, side="nearest") -> str:
        return format_okx_cents(cents, side)

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
