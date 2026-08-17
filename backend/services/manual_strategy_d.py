"""Manual dual-side spread strategy.

Logic:
- Set price a (e.g. 55). Buy Yes@a¢, Buy No@(99-a)¢, split balance evenly.
- When a Buy Yes fill is detected → post Sell Yes@(a+1)¢
- When a Buy No fill is detected → post Sell No one cent above the executable,
  BUY-aligned No entry (then align the Sell upward to the OKX tick)
- When Sells fill → balance increases → re-deploy into new Buy pairs
- Sell targets are one cent above the fill and round up when that target is off tick.

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
from .order_lifecycle import (
    PLACE_CONFIRMATION_STATUSES,
    RESERVING_ORDER_STATUSES,
    TERMINAL_ORDER_STATUSES,
    archive_completed_order_groups,
    atomic_write_json,
    cancel_and_confirm,
    decimal_text,
    generate_client_order_id,
    normalise_order_status,
    observation_from_order,
    owned_order_remaining,
    place_and_confirm,
    update_owned_record,
    wait_for_order,
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
    # Owned BUY cloid -> quantity covered by active/filled protective sells.
    pending_sells: Dict[str, Decimal] = field(default_factory=dict)
    # Exact strategy ownership ledger, keyed by clientOrderId.
    owned_orders: Dict[str, dict] = field(default_factory=dict)
    # Bounded history of fully settled entry/protection groups.
    order_archive: list = field(default_factory=list)
    posted_sells: Set[str] = field(default_factory=set)  # sell order IDs we posted
    sell_retry_counts: Dict[str, int] = field(default_factory=dict)
    sell_retry_after: Dict[str, float] = field(default_factory=dict)
    deployment_blocked: bool = False
    block_reason: str = ""
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
        self.notifier = None
        self.running = False
        self.auto_rebalance = None
        self.cycles = 0
        self._run_id = ""
        self._order_confirmation_attempts = 10
        self._order_confirmation_delay = 0.3
        self._entry_release_event = asyncio.Event()
        self._entry_release_event.set()
        self._sells_file = Path(str(SELLS_FILE_D).format(account_id=account_id))
        self._load_pending_sells()

    def _load_pending_sells(self):
        """Load pending_sells from disk."""
        try:
            if self._sells_file.exists():
                with open(self._sells_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError("manual state root must be an object")
                raw = data.get("oids", [])
                if isinstance(raw, dict):
                    self.state.pending_sells = {
                        str(k): as_decimal(v) for k, v in raw.items()
                    }
                else:
                    # Legacy lists contain order IDs treated as fully handled.
                    self.state.pending_sells = {
                        str(oid): Decimal("999999") for oid in raw
                    }
                owned = data.get("owned_orders", {})
                if not isinstance(owned, dict):
                    raise ValueError("owned_orders must be an object")
                self.state.owned_orders = {}
                for client_order_id, value in owned.items():
                    if not isinstance(value, dict):
                        raise ValueError("owned order records must be objects")
                    client_order_id = str(client_order_id or "")
                    if not client_order_id:
                        raise ValueError("owned order is missing clientOrderId")
                    record = dict(value)
                    record["client_order_id"] = client_order_id
                    record["status"] = normalise_order_status(record.get("status")) or "UNKNOWN"
                    for key in (
                        "size",
                        "filled_size",
                        "filled_amount",
                        "accounted_fill_size",
                        "layer_recorded_size",
                    ):
                        record[key] = decimal_text(record.get(key, "0"))
                    self.state.owned_orders[client_order_id] = record
                raw_archive = data.get("order_archive", [])
                if isinstance(raw_archive, list):
                    self.state.order_archive = [
                        dict(item) for item in raw_archive if isinstance(item, dict)
                    ][-200:]
                self.state.start_time = int(data.get("start_time", 0) or 0)
                self.state.deployment_blocked = bool(data.get("deployment_blocked", False))
                self.state.block_reason = str(data.get("block_reason") or "")
                logger.info(f"[D] Loaded {len(self.state.pending_sells)} pending sells from disk")
        except Exception as e:
            logger.warning(f"[D] Failed to load pending sells: {e}")
            self.state.deployment_blocked = True
            self.state.block_reason = f"state load failed: {e}"

    def _state_payload(self) -> dict:
        return {
            "version": 4,
            "oids": {
                str(key): decimal_text(value)
                for key, value in self.state.pending_sells.items()
            },
            "start_time": self.state.start_time,
            "owned_orders": {
                str(client_order_id): dict(record)
                for client_order_id, record in self.state.owned_orders.items()
            },
            "order_archive": [dict(item) for item in self.state.order_archive[-200:]],
            "deployment_blocked": bool(self.state.deployment_blocked),
            "block_reason": self.state.block_reason,
        }

    def _save_pending_sells(self) -> bool:
        """Persist pending_sells to disk."""
        try:
            atomic_write_json(self._sells_file, self._state_payload())
            audit_event(
                "manual.pending_sells_saved",
                account_id=self.account_id,
                strategy="manual-d",
                run_id=self._run_id,
                path=self._sells_file,
                pending_count=len(self.state.pending_sells),
                owned_order_count=len(self.state.owned_orders),
            )
            return True
        except Exception as e:
            self.state.deployment_blocked = True
            self.state.block_reason = f"state write failed: {e}"
            logger.warning(f"[D] Failed to save pending sells: {e}")
            audit_event(
                "manual.pending_sells_save_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                strategy="manual-d",
                run_id=self._run_id,
                error=e,
            )
            return False

    def _persist_state(self) -> bool:
        """Treat legacy test overrides returning None as a successful write."""
        archived = archive_completed_order_groups(
            owned_orders=self.state.owned_orders,
            pending_sells=self.state.pending_sells,
            sell_retry_counts=self.state.sell_retry_counts,
            sell_retry_after=self.state.sell_retry_after,
            archive=self.state.order_archive,
        )
        if archived:
            audit_event(
                "manual.orders_archived",
                account_id=self.account_id,
                strategy="manual-d",
                run_id=self._run_id,
                archived_groups=archived,
                active_order_count=len(self.state.owned_orders),
                archive_count=len(self.state.order_archive),
            )
        return self._save_pending_sells() is not False

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
        logger.log(log_level, "[Manual-D:%s run=%s] %s", self.account_id, self._run_id, msg)
        audit_event(
            "strategy.message",
            level=log_level,
            account_id=self.account_id,
            strategy="manual-d",
            run_id=self._run_id,
            message=msg,
        )

    async def start(self, market_id: str, yes_asset_id: str, no_asset_id: str,
                    price_a: float, size: int = 0, position_pct: int = 100, scan_interval: float = 0.3, stabilize_secs: float = 0.3,
                    defer_entry: bool = False):
        """Start the spread strategy."""
        if self.state.running:
            return

        self._run_id = secrets.token_hex(8)

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
        self.state.pair_retry_count = 0
        self.state.pair_retry_after = 0.0
        self.state.deployment_blocked = False
        self.state.block_reason = ""
        if defer_entry:
            self._entry_release_event.clear()
        else:
            self._entry_release_event.set()

        import time as _time
        self.state.start_time = int(_time.time() * 1000)
        if not self._persist_state():
            self.state.running = False
            self.running = False
            self._entry_release_event.set()
            raise RuntimeError("Manual-D state could not be persisted")
        self.log(f"Manual started: Buy Yes@{a}¢ / No@{b}¢ → Sell Yes@{sell_a}¢ / No@{sell_b}¢")

        audit_event(
            "strategy.started",
            account_id=self.account_id,
            strategy="manual-d",
            run_id=self._run_id,
            market_id=market_id,
            yes_asset_id=yes_asset_id,
            no_asset_id=no_asset_id,
            price_yes_cents=str(self.state.price_a),
            price_no_cents=str(self.state.price_b),
            scan_interval=self.state.scan_interval,
        )
        with log_context(
            account_id=self.account_id,
            strategy="manual-d",
            run_id=self._run_id,
        ):
            self._main_task = asyncio.create_task(self._main_loop())

    def release_entry_barrier(self) -> bool:
        """Allow new BUY deployment after the caller finishes atomic startup."""
        if not self.state.running:
            return False
        was_deferred = not self._entry_release_event.is_set()
        self._entry_release_event.set()
        if was_deferred:
            self.log("Entry barrier released; new BUY deployment is enabled")
        return True

    async def stop(self):
        """Stop entries, reconcile owned BUYs, and retain confirmed protection."""
        needs_reconcile = self.state.deployment_blocked or any(
            normalise_order_status(record.get("status")) in RESERVING_ORDER_STATUSES
            for record in self.state.owned_orders.values()
        )
        if not self.state.running and not needs_reconcile:
            self._entry_release_event.set()
            return True
        self.state.running = False
        self.running = False
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except (asyncio.CancelledError, Exception):
                pass
        self._entry_release_event.set()
        safe = True
        for entry in list(self.state.owned_orders.values()):
            if entry.get("role") != "entry_buy":
                continue
            if normalise_order_status(entry.get("status")) in RESERVING_ORDER_STATUSES:
                safe = (await self._cancel_owned_entry(entry)) and safe

        safe = (await self._reconcile_protective_orders()) and safe
        for entry in list(self.state.owned_orders.values()):
            if entry.get("role") == "entry_buy" and as_decimal(
                entry.get("filled_size", "0") or "0"
            ) > 0:
                safe = (await self._ensure_protection_for_entry(entry)) and safe

        if safe:
            self.state.deployment_blocked = False
            self.state.block_reason = ""
        if not self._persist_state():
            safe = False
        if safe:
            self.log("已停止 (owned Buy 已终态确认，ACTIVE protective Sell 保留)")
        else:
            self.log("停止未完成安全对账；新部署保持锁定", "ERROR")
            self.state.deployment_blocked = True
            if not self.state.block_reason:
                self.state.block_reason = "stop reconciliation incomplete"
            self._persist_state()
        audit_event(
            "strategy.stopped",
            account_id=self.account_id,
            strategy="manual-d",
            run_id=self._run_id,
            pending_count=len(self.state.pending_sells),
            safe=safe,
        )
        return safe

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
            if str(resp.get("code", "")) != "0" or str(resp2.get("code", "")) != "0":
                self._block_new_deployments(
                    "initial owned-order reconciliation snapshot failed"
                )
            else:
                all_orders = (
                    resp.get("data", {}).get("list", [])
                    + resp2.get("data", {}).get("list", [])
                )
                await self._check_sell_fills(all_orders)
                await self._check_fills_post_sells(all_orders)
        except Exception as e:
            self.log(f"Seed fills error: {e}", "WARN")
            self._block_new_deployments(
                f"initial owned-order reconciliation failed: {e}"
            )
        await self._main_loop()

    # ─── Main Loop ───

    async def _main_loop(self):
        """Check fills, post matching sells, and redeploy at scan_interval."""
        try:
            # Preserve the original immediate start unless the API has placed
            # this run behind the atomic-start entry barrier.
            if self._entry_release_event.is_set():
                await self._deploy_balance()

            while self.state.running:
                if self._entry_release_event.is_set():
                    await asyncio.sleep(self.state.scan_interval)
                else:
                    try:
                        await asyncio.wait_for(
                            self._entry_release_event.wait(),
                            timeout=self.state.scan_interval,
                        )
                    except asyncio.TimeoutError:
                        pass
                if not self.state.running:
                    break

                # Fetch a complete lifecycle snapshot.  A partial/failed
                # snapshot is never followed by new deployment.
                _closed_orders = []
                _open_orders = []
                snapshot_ok = True
                try:
                    _resp = await self.client.get_orders(status="closed")
                    if str(_resp.get("code", "")) == "0":
                        _closed_orders = _resp.get("data", {}).get("list", [])
                    else:
                        snapshot_ok = False
                except Exception as e:
                    snapshot_ok = False
                    self.log(f"Fetch closed orders error: {e}", "WARN")
                try:
                    _resp2 = await self.client.get_orders(status="open")
                    if str(_resp2.get("code", "")) == "0":
                        _open_orders = _resp2.get("data", {}).get("list", [])
                    else:
                        snapshot_ok = False
                except Exception as e:
                    snapshot_ok = False
                    self.log(f"Fetch open orders error: {e}", "WARN")

                if not snapshot_ok:
                    continue
                _all_orders = _closed_orders + _open_orders

                # Release cancelled/failed protection before calculating gaps.
                await self._check_sell_fills(_all_orders)

                # Check for owned filled buys → post exact protective sells.
                await self._check_fills_post_sells(_all_orders)

                # Step 2: Deploy any available balance into new buy pairs
                if self._entry_release_event.is_set():
                    await self._deploy_balance()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log(f"致命错误: {e}", "ERROR")
            self.state.running = False
            self.running = False

    def _owned_record_for_order(self, order: dict, role: str = ""):
        observation = observation_from_order(order)
        record = self.state.owned_orders.get(observation.client_order_id)
        if record is None and observation.order_id:
            for candidate in self.state.owned_orders.values():
                if str(candidate.get("order_id") or "") == observation.order_id:
                    record = candidate
                    break
        if record is None or (role and record.get("role") != role):
            return None, observation
        if (
            str(record.get("asset_id") or "") != observation.asset_id
            or str(record.get("side") or "").upper() != observation.side
        ):
            self._block_new_deployments(
                f"owned order identity mismatch for {record.get('client_order_id', '')}"
            )
            return None, observation
        return record, observation

    async def _place_owned_order(
        self,
        *,
        role: str,
        asset_id: str,
        side: str,
        price: str,
        size,
        outcome: str,
        parent_buy_cloid: str = "",
        exit_price: str = "",
        market_id: str = "",
        reduce_only: bool = False,
    ):
        try:
            client_order_id = generate_client_order_id(self.client)
        except Exception as exc:
            self._block_new_deployments(f"clientOrderId generation failed: {exc}")
            return None, None

        size_text = decimal_text(size)
        record = {
            "client_order_id": client_order_id,
            "order_id": "",
            "role": role,
            "market_id": str(market_id or self.state.market_id),
            "asset_id": str(asset_id),
            "side": str(side).upper(),
            "status": "PREPARED",
            "price": str(price),
            "size": size_text,
            "filled_size": "0",
            "filled_amount": "0",
            "accounted_fill_size": "0",
            "outcome": outcome,
            "parent_buy_cloid": parent_buy_cloid,
            "exit_price": str(exit_price or (price if role == "protective_sell" else "")),
            "run_id": self._run_id,
        }
        self.state.owned_orders[client_order_id] = record
        if not self._persist_state():
            self._block_new_deployments(
                f"{role} clientOrderId snapshot could not be persisted",
                persist=False,
            )
            return record, None

        confirmation = await place_and_confirm(
            self.client,
            client_order_id=client_order_id,
            market_id=str(market_id or self.state.market_id),
            asset_id=str(asset_id),
            side=str(side).lower(),
            price=str(price),
            size=size_text,
            tif="gtc",
            reduce_only=reduce_only,
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        record["response_code"] = confirmation.response_code
        if confirmation.observation is not None:
            update_owned_record(record, confirmation.observation)
        elif confirmation.rejected:
            record["status"] = "FAILED"
            record["error"] = str(
                confirmation.response.get("message")
                or confirmation.response.get("msg")
                or confirmation.response_code
            )
        elif confirmation.response_code == "0":
            # OKX accepted the placement.  Its order-list projection can lag
            # behind the successful placement response, so keep the quantity
            # reserved just as the original Manual strategy did instead of
            # re-submitting another protective sell for the same fill.
            record["status"] = "PENDING_PLACE"
            record["error"] = ""
        else:
            record["status"] = "UNKNOWN"
            record["error"] = str(
                confirmation.response.get("message")
                or confirmation.response.get("msg")
                or confirmation.response_code
            )

        if not self._persist_state():
            self._block_new_deployments(
                f"{role} result could not be persisted",
                persist=False,
            )
        if confirmation.unresolved and confirmation.response_code != "0":
            self._block_new_deployments(
                f"{role} {client_order_id} final placement state is unknown"
            )
        return record, confirmation

    def _entry_coverage(self, buy_cloid: str) -> tuple[Decimal, Decimal]:
        exited = Decimal("0")
        reserved = Decimal("0")
        for record in self.state.owned_orders.values():
            if (
                record.get("role") != "protective_sell"
                or str(record.get("parent_buy_cloid") or "") != buy_cloid
            ):
                continue
            filled = as_decimal(record.get("filled_size", "0") or "0")
            exited += filled
            if normalise_order_status(record.get("status")) in RESERVING_ORDER_STATUSES:
                reserved += owned_order_remaining(record)
        return exited, reserved

    def _sync_pending_marker(self, entry: dict) -> None:
        buy_cloid = str(entry.get("client_order_id") or "")
        filled = as_decimal(entry.get("filled_size", "0") or "0")
        exited, reserved = self._entry_coverage(buy_cloid)
        covered = min(filled, exited + reserved)
        if covered > 0:
            self.state.pending_sells[buy_cloid] = covered
        else:
            self.state.pending_sells.pop(buy_cloid, None)

    def _record_entry_cost_layer(self, entry: dict) -> None:
        filled = as_decimal(entry.get("filled_size", "0") or "0")
        recorded = as_decimal(entry.get("layer_recorded_size", "0") or "0")
        delta = filled - recorded
        if delta <= 0:
            return
        if self.auto_rebalance:
            self.auto_rebalance.add_to_layer(
                str(entry.get("asset_id") or ""),
                price_to_cents(entry.get("price") or "0"),
                delta,
                market_id=str(entry.get("market_id") or self.state.market_id),
            )
        entry["layer_recorded_size"] = decimal_text(filled)

    def _account_protective_fill(self, record: dict) -> None:
        filled = as_decimal(record.get("filled_size", "0") or "0")
        accounted = as_decimal(record.get("accounted_fill_size", "0") or "0")
        delta = filled - accounted
        if delta <= 0:
            return
        parent = self.state.owned_orders.get(
            str(record.get("parent_buy_cloid") or "")
        )
        if parent is None:
            self._block_new_deployments(
                f"protective SELL {record.get('client_order_id', '')} is missing its parent BUY"
            )
            return
        if self.auto_rebalance:
            self.auto_rebalance.remove_from_highest_layer(
                str(record.get("asset_id") or ""),
                delta,
                market_id=str(record.get("market_id") or self.state.market_id),
            )
        buy_price = as_decimal(parent.get("price", "0") or "0")
        sell_price = as_decimal(record.get("price", "0") or "0")
        profit_delta = delta * (sell_price - buy_price)
        record["accounted_fill_size"] = decimal_text(filled)
        self.state.total_profit += float(profit_delta)
        label = str(record.get("outcome") or "")
        self.log(
            f"💰 Sell {label} 成交 x{normalize_size_str(delta)} "
            f"@{record.get('price', '?')} | {float(profit_delta):+.2f} xp"
        )
        if self.notifier:
            asyncio.create_task(self.notifier.send(
                f"[{self.account_name}] 💰 Sell {label} 成交 "
                f"x{normalize_size_str(delta)} @{record.get('price', '?')}"
            ))

    async def _ensure_protection_for_entry(self, entry: dict) -> bool:
        buy_cloid = str(entry.get("client_order_id") or "")
        filled = as_decimal(entry.get("filled_size", "0") or "0")
        if not buy_cloid or filled < Decimal("0.01"):
            return True
        self._record_entry_cost_layer(entry)
        exited, reserved = self._entry_coverage(buy_cloid)
        unprotected = filled - exited - reserved
        self._sync_pending_marker(entry)
        if unprotected < Decimal("0.01"):
            return True
        if self.state.sell_retry_after.get(buy_cloid, 0.0) > asyncio.get_running_loop().time():
            return False

        asset_id = str(entry.get("asset_id") or "")
        label = str(entry.get("outcome") or ("Yes" if asset_id == self.state.yes_asset_id else "No"))
        sell_price = str(entry.get("exit_price") or "")
        if not sell_price:
            buy_cents = price_to_cents(entry.get("price") or "0")
            sell_price = self._cents_to_price_str(
                align_cents_for_okx(buy_cents + Decimal("1"), "sell"),
                "sell",
            )
            entry["exit_price"] = sell_price
        sell_cents = price_to_cents(sell_price)
        size_text = decimal_text(unprotected)
        sell_notional = sell_cents / Decimal("100") * unprotected
        if sell_notional < Decimal("1"):
            if await self._is_full_available_position(
                asset_id,
                unprotected,
                market_id=str(entry.get("market_id") or self.state.market_id),
            ):
                self.log(
                    f"Sell {label} @{sell_price} x{size_text} uses OKX sell-all exception"
                )
            else:
                self._defer_failed_sell(
                    buy_cloid,
                    label,
                    f"{size_text} x {sell_cents}¢ = {sell_notional:.2f} xp below 1 xp minimum",
                )
                return False

        sell_record, confirmation = await self._place_owned_order(
            role="protective_sell",
            asset_id=asset_id,
            side="SELL",
            price=sell_price,
            size=unprotected,
            outcome=label,
            parent_buy_cloid=buy_cloid,
            market_id=str(entry.get("market_id") or self.state.market_id),
            reduce_only=True,
        )
        if sell_record is None or confirmation is None:
            return False

        status = normalise_order_status(sell_record.get("status"))
        self._account_protective_fill(sell_record)
        self._sync_pending_marker(entry)
        if status in {"ACTIVE", "FILLED"}:
            self.state.sell_retry_counts.pop(buy_cloid, None)
            self.state.sell_retry_after.pop(buy_cloid, None)
            self.state.total_sells_posted += 1
            self.state.consecutive_failures = 0
            self.log(f"挂 Sell {label} @{sell_price} x{size_text} ({status})")
            if self.notifier:
                asyncio.create_task(self.notifier.send(
                    f"[{self.account_name}] ✅ Buy {label} 成交 x{size_text} "
                    f"→ Sell @{sell_price} {status}"
                ))
            self._persist_state()
            return True
        if status == "PARTIALLY_FILLED":
            self.state.sell_retry_after.pop(buy_cloid, None)
            self.log(
                f"Sell {label} 部分成交后终止；剩余未保护数量将在下一轮重挂",
                "WARN",
            )
            self._persist_state()
            return False
        if status in {"FAILED", "CANCELLED", "EXPIRED"}:
            message = str(sell_record.get("error") or status)
            self._defer_failed_sell(buy_cloid, label, message)
            self._persist_state()
            return False
        return False

    async def _cancel_owned_entry(self, entry: dict) -> bool:
        status = normalise_order_status(entry.get("status"))
        if status in TERMINAL_ORDER_STATUSES:
            return True
        confirmation = await cancel_and_confirm(
            self.client,
            client_order_id=str(entry.get("client_order_id") or ""),
            order_id=str(entry.get("order_id") or ""),
            market_id=str(entry.get("market_id") or self.state.market_id),
            asset_id=str(entry.get("asset_id") or ""),
            attempts=self._order_confirmation_attempts,
            delay=self._order_confirmation_delay,
        )
        if confirmation.observation is None:
            self._block_new_deployments(
                f"entry BUY {entry.get('client_order_id', '')} cancellation final state is unknown"
            )
            return False
        update_owned_record(entry, confirmation.observation)
        if not self._persist_state():
            self._block_new_deployments(
                "entry BUY cancellation result could not be persisted",
                persist=False,
            )
            return False
        return confirmation.confirmed_terminal

    async def _reconcile_protective_orders(self) -> bool:
        safe = True
        for record in list(self.state.owned_orders.values()):
            if record.get("role") != "protective_sell":
                continue
            if normalise_order_status(record.get("status")) not in RESERVING_ORDER_STATUSES:
                self._account_protective_fill(record)
                continue
            observation = await wait_for_order(
                self.client,
                client_order_id=str(record.get("client_order_id") or ""),
                order_id=str(record.get("order_id") or ""),
                market_id=str(record.get("market_id") or self.state.market_id),
                accepted_statuses=PLACE_CONFIRMATION_STATUSES,
                attempts=self._order_confirmation_attempts,
                delay=self._order_confirmation_delay,
            )
            if observation is None:
                if str(record.get("response_code") or "").upper() == "0":
                    # A successful place response is sufficient to reserve
                    # this fill.  Keep polling on later passes, but do not
                    # turn normal exchange-indexing lag into repeated locks.
                    continue
                safe = False
                self._block_new_deployments(
                    f"protective SELL {record.get('client_order_id', '')} state is unknown"
                )
                continue
            update_owned_record(record, observation)
            self._account_protective_fill(record)
        for entry in self.state.owned_orders.values():
            if entry.get("role") == "entry_buy":
                self._sync_pending_marker(entry)
        if not self._persist_state():
            safe = False
        return safe

    async def _check_fills_post_sells(self, order_list=None):
        """Protect fills from exact strategy-owned BUY clientOrderIds only."""
        try:
            if order_list is None:
                order_list = []
                for query_status in ("closed", "open"):
                    response = await self.client.get_orders(status=query_status)
                    if str(response.get("code", "")) != "0":
                        return
                    order_list.extend((response.get("data") or {}).get("list", []))
            changed = False
            for o in order_list:
                record, observation = self._owned_record_for_order(o, "entry_buy")
                if record is None:
                    continue
                update_owned_record(record, observation)
                if not record.get("exit_price") and observation.price:
                    record["exit_price"] = self._cents_to_price_str(
                        align_cents_for_okx(
                            price_to_cents(observation.price) + Decimal("1"),
                            "sell",
                        ),
                        "sell",
                    )
                self._record_entry_cost_layer(record)
                changed = True

            for entry in list(self.state.owned_orders.values()):
                if entry.get("role") != "entry_buy":
                    continue
                await self._ensure_protection_for_entry(entry)
            if changed:
                self._persist_state()
        except Exception as e:
            self._record_failure(f"检查成交异常: {e}")


    async def _check_sell_fills(self, order_list=None):
        """Reconcile exact owned protective SELL orders and release remainder."""
        try:
            if order_list is None:
                order_list = []
                for query_status in ("closed", "open"):
                    response = await self.client.get_orders(status=query_status)
                    if str(response.get("code", "")) != "0":
                        return
                    order_list.extend((response.get("data") or {}).get("list", []))
            changed = False
            for o in order_list:
                record, observation = self._owned_record_for_order(
                    o, "protective_sell"
                )
                if record is None:
                    continue
                previous_status = normalise_order_status(record.get("status"))
                update_owned_record(record, observation)
                status = normalise_order_status(record.get("status"))
                self._account_protective_fill(record)
                order_key = str(record.get("order_id") or record.get("client_order_id") or "")
                if status == "ACTIVE":
                    self.state.posted_sells.add(order_key)
                elif status in TERMINAL_ORDER_STATUSES:
                    self.state.posted_sells.discard(order_key)
                    if status in {"PARTIALLY_FILLED", "FAILED", "CANCELLED", "EXPIRED"}:
                        parent = str(record.get("parent_buy_cloid") or "")
                        self.state.sell_retry_after.pop(parent, None)
                        if previous_status != status:
                            self.log(
                                f"Protective Sell {record.get('outcome', '')} reached {status}; "
                                "unprotected remainder returned to retry queue",
                                "WARN",
                            )
                changed = True
            for entry in self.state.owned_orders.values():
                if entry.get("role") == "entry_buy":
                    self._sync_pending_marker(entry)
            if changed:
                self._persist_state()
        except Exception as e:
            self.log(f"\u68c0\u67e5sell\u6210\u4ea4\u5f02\u5e38: {e}", "WARN")

    async def _deploy_balance(self):
        """Deploy available balance into Buy Yes + Buy No pairs."""
        try:
            if not self._entry_release_event.is_set():
                return
            if self.state.deployment_blocked:
                return
            if self.state.pair_retry_after > asyncio.get_running_loop().time():
                return

            balance = await self.client.get_balance()
            available = self._parse_available(balance)

            locked_in_buys = Decimal("0")
            try:
                resp_open = await self.client.get_orders(status="open")
                if str(resp_open.get("code", "")) != "0":
                    self.log("Open-order budget snapshot failed; deployment skipped", "WARN")
                    return
                for o in resp_open.get("data", {}).get("list", []):
                    if o.get("side", "").upper() == "BUY":
                        aid = o.get("assetId", "")
                        if aid in (self.state.yes_asset_id, self.state.no_asset_id):
                            sz = as_decimal(o.get("size", "0") or "0") - as_decimal(o.get("filledSize", "0") or "0")
                            px = as_decimal(o.get("price", "0") or "0")
                            locked_in_buys += sz * px
            except Exception as exc:
                self.log(f"Open-order budget snapshot failed: {exc}", "WARN")
                return

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

            yes_record, yes_confirmation = await self._place_owned_order(
                role="entry_buy",
                asset_id=self.state.yes_asset_id,
                side="BUY",
                price=yes_price,
                size=pairs,
                outcome="Yes",
                exit_price=self._cents_to_price_str(
                    align_cents_for_okx(
                        price_to_cents(yes_price) + Decimal("1"), "sell"
                    ),
                    "sell",
                ),
            )
            if yes_record is None or yes_confirmation is None:
                return
            await self._ensure_protection_for_entry(yes_record)
            yes_status = normalise_order_status(yes_record.get("status"))
            if yes_status not in {"ACTIVE", "FILLED"}:
                msg = str(yes_record.get("error") or yes_status or "unknown error")
                self._defer_pair_retry(f"Buy Yes 未形成有效 GTC: {msg}")
                if yes_status == "PARTIALLY_FILLED" or as_decimal(
                    yes_record.get("filled_size", "0") or "0"
                ) > 0:
                    self._block_new_deployments(
                        "Buy Yes ended with a one-sided fill; protection remains active"
                    )
                return
            self.state.consecutive_failures = 0
            self.log(f"挂 Buy Yes @{self.state.price_a}¢ x{pairs} ({yes_status})")

            no_record, no_confirmation = await self._place_owned_order(
                role="entry_buy",
                asset_id=self.state.no_asset_id,
                side="BUY",
                price=no_price,
                size=pairs,
                outcome="No",
                exit_price=self._cents_to_price_str(
                    align_cents_for_okx(
                        price_to_cents(no_price) + Decimal("1"), "sell"
                    ),
                    "sell",
                ),
            )
            if no_record is not None:
                await self._ensure_protection_for_entry(no_record)
            no_status = normalise_order_status(
                no_record.get("status") if no_record else "UNKNOWN"
            )
            if no_confirmation is not None and no_status in {"ACTIVE", "FILLED"}:
                self.state.consecutive_failures = 0
                self.state.pair_retry_count = 0
                self.state.pair_retry_after = 0.0
                self.log(f"挂 Buy No @{self.state.price_b}¢ x{pairs} ({no_status})")
            else:
                msg = str(
                    (no_record or {}).get("error")
                    or no_status
                    or "unknown error"
                )
                self.log(f"Buy No 失败: {msg}; 正在撤回本轮 Buy Yes", "ERROR")
                rollback_safe = await self._cancel_owned_entry(yes_record)
                if no_record is not None and no_status in RESERVING_ORDER_STATUSES:
                    rollback_safe = (
                        await self._cancel_owned_entry(no_record)
                    ) and rollback_safe
                await self._ensure_protection_for_entry(yes_record)
                if no_record is not None:
                    await self._ensure_protection_for_entry(no_record)
                yes_filled = as_decimal(yes_record.get("filled_size", "0") or "0")
                no_filled = as_decimal(
                    (no_record or {}).get("filled_size", "0") or "0"
                )
                if yes_filled != no_filled:
                    self._block_new_deployments(
                        "Pair rollback left unequal fills "
                        f"(Yes {decimal_text(yes_filled)} / "
                        f"No {decimal_text(no_filled)}); protection remains active"
                    )
                    rollback_safe = False
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
            self._persist_state()

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

    async def _is_full_available_position(
        self, asset_id: str, quantity, market_id: str = ""
    ) -> bool:
        """Confirm the order would sell the asset's entire unfrozen position."""
        try:
            response = await self.client.get_positions(
                status="open",
                market_id=market_id or self.state.market_id or None,
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

    def _block_new_deployments(self, message: str, *, persist: bool = True):
        self.state.deployment_blocked = True
        self.state.block_reason = str(message)
        warning = f"新部署已锁定: {message}；现有成交仍继续管理"
        self.log(warning, "ERROR")
        if persist:
            self._persist_state()
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
        record = self.state.owned_orders.get(client_order_id)
        if record is None and order_id:
            record = next(
                (
                    candidate
                    for candidate in self.state.owned_orders.values()
                    if str(candidate.get("order_id") or "") == order_id
                ),
                None,
            )
        if record is None:
            client_order_id = client_order_id or generate_client_order_id(self.client)
            record = {
                "client_order_id": client_order_id,
                "order_id": order_id,
                "role": "entry_buy",
                "market_id": self.state.market_id,
                "asset_id": self.state.yes_asset_id,
                "side": "BUY",
                "status": "ACTIVE",
                "price": self._cents_to_price_str(self.state.price_a or 55, "buy"),
                "exit_price": self._cents_to_price_str(self.state.sell_a or 56, "sell"),
                "size": "0",
                "filled_size": "0",
                "filled_amount": "0",
                "accounted_fill_size": "0",
                "outcome": "Yes",
                "parent_buy_cloid": "",
                "run_id": self._run_id,
            }
            self.state.owned_orders[client_order_id] = record
            if not self._persist_state():
                return False
        previous_attempts = self._order_confirmation_attempts
        self._order_confirmation_attempts = confirmation_attempts
        try:
            safe = await self._cancel_owned_entry(record)
        finally:
            self._order_confirmation_attempts = previous_attempts
        if not safe:
            self._block_new_deployments("Buy Yes 撤单已受理，但无法确认最终状态")
            return False
        status = normalise_order_status(record.get("status"))
        filled = as_decimal(record.get("filled_size", "0") or "0")
        if filled > Decimal("0") or status in ("FILLED", "PARTIALLY_FILLED"):
            await self._ensure_protection_for_entry(record)
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
        observation = await wait_for_order(
            self.client,
            client_order_id=client_order_id,
            order_id=order_id,
            market_id=self.state.market_id,
            accepted_statuses=TERMINAL_ORDER_STATUSES,
            attempts=attempts,
            delay=self._order_confirmation_delay,
        )
        if observation is None:
            return "", Decimal("0")
        return observation.status, observation.filled_size

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
        # Build each exit from the executable, BUY-aligned entry.  Around the
        # 4c/96c tick boundaries, deriving NO sell from the unaligned
        # complement can disagree with the price actually submitted later.
        sell_b = align_cents_for_okx(b + Decimal("1"), "sell")
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
            "block_reason": self.state.block_reason,
            "owned_order_count": len(self.state.owned_orders),
            "archived_order_group_count": len(self.state.order_archive),
            "entry_deferred": not self._entry_release_event.is_set(),
            "logs": self.state.logs[-20:],
        }
