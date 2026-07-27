"""Polymarket-signal / OKX-execution sniper engine.

Each target is an explicit mapping from one real Polymarket outcome token to
one OKX YES or NO asset.  All targets are observed together, but an atomic
engine-wide lock allows only the first valid signal to enter a trade.  The
lock is released only after the strategy's tracked position is fully sold.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import secrets
import time
from datetime import datetime, timezone
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, Iterable, Optional

import aiohttp
from aiohttp_socks import ProxyConnector

from ..core.logging_config import audit_event, log_context, market_event
from ..core.proxy_nodes import PROXY_NODES
from .size_utils import normalize_buy_size_str, normalize_size_str

logger = logging.getLogger(__name__)

POLY_CLOB_BOOK = "https://clob.polymarket.com/book?token_id={token_id}"
POLY_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OKX_WS_DEFAULT = "wss://ws.okx.com:8443/ws/v5/business"
OKX_WS_US = "wss://wsus.okx.com/ws/v5/business"

ZERO = Decimal("0")
ONE = Decimal("1")
CENT = Decimal("0.01")
OUTER_TICK = Decimal("0.001")
UNACKNOWLEDGED_RESPONSE_CODES = frozenset({
    "", "TIMEOUT", "ERROR", "INVALID", "INVALID_RESPONSE",
})


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else default
    except (InvalidOperation, TypeError, ValueError):
        return default


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _okx_tick(price: Decimal) -> Decimal:
    return OUTER_TICK if price < Decimal("0.04") or price > Decimal("0.96") else CENT


def _floor_to_tick(price: Decimal) -> Decimal:
    tick = _okx_tick(price)
    return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick


def _okx_price_text(price: Decimal) -> str:
    aligned = _floor_to_tick(price)
    decimals = 3 if _okx_tick(aligned) == OUTER_TICK else 2
    return f"{aligned:.{decimals}f}"


@dataclass
class SniperConfig:
    threshold_cents: Decimal = Decimal("40")
    buy_limit_cents: Decimal = Decimal("50")
    position_pct: Decimal = Decimal("100")
    timeout_secs: Decimal = Decimal("3")
    settle_ms: int = 0
    window_ms: int = 150

    def to_dict(self) -> dict:
        return {
            "threshold_cents": _decimal_text(self.threshold_cents),
            "buy_limit_cents": _decimal_text(self.buy_limit_cents),
            "position_pct": _decimal_text(self.position_pct),
            "timeout_secs": _decimal_text(self.timeout_secs),
            "settle_ms": self.settle_ms,
            "window_ms": self.window_ms,
        }


@dataclass
class SniperTarget:
    id: str
    name: str
    poly_token_id: str
    market_id: str
    yes_asset_id: str
    no_asset_id: str
    side: str = "yes"
    budget: Optional[Decimal] = None
    poly_url: str = ""
    poly_market_id: str = ""

    @property
    def asset_id(self) -> str:
        return self.yes_asset_id if self.side == "yes" else self.no_asset_id

    def to_dict(self) -> dict:
        data = asdict(self)
        data["budget"] = _decimal_text(self.budget) if self.budget is not None else None
        data["asset_id"] = self.asset_id
        data["poly_outcome"] = self.name
        data["okx_market_id"] = self.market_id
        data["okx_side"] = self.side.upper()
        data["okx_asset_id"] = self.asset_id
        return data

    @classmethod
    def from_dict(cls, raw: dict, index: int) -> "SniperTarget":
        side = str(raw.get("side") or raw.get("okx_side") or "yes").strip().lower()
        budget_raw = raw.get("budget")
        return cls(
            id=str(raw.get("id") or f"target-{index + 1}"),
            name=str(raw.get("name") or raw.get("poly_outcome") or raw.get("selected_outcome") or f"Target {index + 1}"),
            poly_token_id=str(raw.get("poly_token_id") or raw.get("token_id") or ""),
            market_id=str(raw.get("market_id") or raw.get("okx_market_id") or ""),
            yes_asset_id=str(raw.get("yes_asset_id") or ""),
            no_asset_id=str(raw.get("no_asset_id") or ""),
            side=side,
            budget=_decimal(budget_raw) if budget_raw not in (None, "") else None,
            poly_url=str(raw.get("poly_url") or ""),
            poly_market_id=str(raw.get("poly_market_id") or ""),
        )


@dataclass
class TargetRuntime:
    last_bid: Decimal = ZERO
    calibrated: bool = False
    baseline: Decimal = ZERO
    price_window: list = field(default_factory=list)
    last_no_trigger_log: float = 0.0


@dataclass
class SniperState:
    status: str = "idle"
    active_target_id: str = ""
    locked: bool = False
    buy_price: Decimal = ZERO
    buy_size: Decimal = ZERO
    sold_size: Decimal = ZERO
    position_before: Decimal = ZERO
    entry_client_order_id: str = ""
    entry_placement_attempted: bool = False
    exit_client_order_ids: list = field(default_factory=list)
    exit_resolved_client_order_ids: list = field(default_factory=list)
    sell_order_id: str = ""
    hold_start: float = 0.0
    hold_start_epoch_ms: int = 0
    logs: list = field(default_factory=list)

    # Compatibility summary fields consumed by account status and dashboard polling.
    yes_token_id: str = ""
    no_token_id: str = ""
    yes_asset_id: str = ""
    no_asset_id: str = ""
    market_id: str = ""
    poly_url: str = ""
    selected_outcome: str = ""
    last_yes_bid: Decimal = ZERO
    last_no_bid: Decimal = ZERO


class PolySniper:
    """Monitor explicit Poly-to-OKX mappings with one global trade lock."""

    def __init__(self, client, account_id: str = "default"):
        self.client = client
        self.account_id = account_id
        self.account_name = ""
        self.notifier = None
        self.on_state_change = None
        self.config = SniperConfig()
        self.targets: list[SniperTarget] = []
        self.state = SniperState()
        self._runtime: Dict[str, TargetRuntime] = {}
        self._token_targets: Dict[str, list[str]] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._okx_trace_task: Optional[asyncio.Task] = None
        self._hold_task: Optional[asyncio.Task] = None
        self._trigger_tasks: set[asyncio.Task] = set()
        self._trigger_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._starting = False
        self._persistence_failed = False
        self._latency_active: Optional[dict] = None
        self._latency_history: list[dict] = []
        self._run_id = ""
        self._poly_frame_sequence = 0

    @property
    def running(self) -> bool:
        return self._running

    @property
    def starting(self) -> bool:
        return self._starting

    def log(self, message: str, level: str | int = logging.INFO):
        if isinstance(level, str):
            log_level = {
                "DEBUG": logging.DEBUG,
                "INFO": logging.INFO,
                "WARN": logging.WARNING,
                "WARNING": logging.WARNING,
                "ERROR": logging.ERROR,
                "CRITICAL": logging.CRITICAL,
            }.get(level.upper(), logging.INFO)
        else:
            log_level = int(level)
        now = datetime.now()
        entry = f"[{now.strftime('%I:%M:%S')}.{now.microsecond // 1000:03d} {now.strftime('%p')}] {message}"
        self.state.logs.append(entry)
        self.state.logs = self.state.logs[-100:]
        logger.log(
            log_level,
            "[Sniper:%s run=%s] %s",
            self.account_id,
            self._run_id,
            message,
        )
        audit_event(
            "strategy.message",
            level=log_level,
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            active_target_id=self.state.active_target_id,
            status=self.state.status,
            message=message,
        )

    def _spawn_task(self, coroutine, label: str) -> asyncio.Task:
        """Create a named task carrying stable strategy correlation context."""
        with log_context(
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
        ):
            return asyncio.create_task(
                coroutine,
                name=f"sniper:{self.account_id}:{label}",
            )

    def _spawn_okx_trace_task(self) -> Optional[asyncio.Task]:
        if not str(getattr(self.client, "base_url", "") or ""):
            audit_event(
                "telemetry.unavailable",
                level=logging.WARNING,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                feed="okx",
                reason="client base_url is unavailable",
            )
            return None
        return self._spawn_task(self._okx_market_trace_loop(), "okx-trace")

    @staticmethod
    def _elapsed_ms(started_ns: int) -> float:
        return round((time.perf_counter_ns() - started_ns) / 1_000_000, 1)

    def _begin_latency_trace(
        self,
        target_id: str,
        signal_ns: Optional[int] = None,
        trace_id: str = "",
        frame_id: str = "",
    ) -> str:
        target = self._target(target_id)
        started_ns = signal_ns or time.perf_counter_ns()
        trace_id = trace_id or secrets.token_hex(8)
        base_url = str(getattr(self.client, "base_url", ""))
        proxy_node = str(getattr(self.client, "_proxy_node", "") or "")
        proxy_name = str(PROXY_NODES.get(proxy_node, {}).get("name", proxy_node))
        self._latency_active = {
            "_started_ns": started_ns,
            "trace_id": trace_id,
            "poly_frame_id": frame_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "target_id": target.id,
            "target_name": target.name,
            "side": target.side,
            "region": "CN" if "www.okx.com" in base_url else "AU",
            "proxy_node": proxy_node,
            "proxy_name": proxy_name,
            "stages_ms": {},
        }
        return trace_id

    def _next_poly_frame_id(self) -> str:
        self._poly_frame_sequence += 1
        return f"{self._run_id or 'unscoped'}:poly:{self._poly_frame_sequence}"

    def _record_latency_stage(self, name: str, started_ns: int):
        if self._latency_active is not None:
            self._latency_active["stages_ms"][name] = self._elapsed_ms(started_ns)

    def _mark_latency_order_attempted(self):
        if self._latency_active is not None:
            self._latency_active["order_attempted"] = True

    def _finish_latency_trace(
        self,
        outcome: str,
        *,
        reason: str = "",
        response_code: str = "",
        order_ack: bool = False,
    ):
        trace = self._latency_active
        if trace is None:
            return
        elapsed = self._elapsed_ms(int(trace.pop("_started_ns")))
        trace["outcome"] = str(outcome)
        trace["trigger_to_finish_ms"] = elapsed
        if order_ack:
            trace["trigger_to_order_ack_ms"] = elapsed
        if reason:
            trace["reason"] = str(reason)[:200]
        if response_code:
            trace["response_code"] = str(response_code)[:40]
        self._latency_history.append(trace)
        self._latency_history = self._latency_history[-50:]
        audit_event(
            "sniper.trigger_latency",
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            trace=trace,
        )
        self._latency_active = None

    def _latency_snapshot(self) -> dict:
        recent = []
        for saved in self._latency_history[-20:]:
            if not isinstance(saved, dict):
                continue
            item = dict(saved)
            stages = item.get("stages_ms")
            item["stages_ms"] = dict(stages) if isinstance(stages, dict) else {}
            recent.append(item)

        rolling_recent = recent
        rolling_region = None
        rolling_proxy_node = None
        if recent and (
            "region" in recent[-1] or "proxy_node" in recent[-1]
        ):
            rolling_region = str(recent[-1].get("region") or "")
            rolling_proxy_node = str(recent[-1].get("proxy_node") or "")
            rolling_recent = [
                item for item in recent
                if str(item.get("region") or "") == rolling_region
                and str(item.get("proxy_node") or "") == rolling_proxy_node
            ]

        ack_samples = []
        acknowledged_ids = set()
        for index, item in enumerate(rolling_recent):
            value = self._valid_latency_ms(item.get("trigger_to_order_ack_ms"))
            if value is not None:
                ack_samples.append(value)
                acknowledged_ids.add(index)
        order_attempts = sum(
            1
            for index, item in enumerate(rolling_recent)
            if bool(item.get("order_attempted")) or index in acknowledged_ids
        )
        successful = sum(
            1 for item in rolling_recent
            if self._valid_latency_ms(item.get("trigger_to_order_ack_ms")) is not None
            and str(item.get("response_code", "")) == "0"
        )
        rolling = {
            "window": len(rolling_recent),
            "region": rolling_region,
            "proxy_node": rolling_proxy_node,
            "order_attempts": order_attempts,
            "order_ack_samples": len(ack_samples),
            "successful_order_responses": successful,
            "failed_order_responses": len(ack_samples) - successful,
            "unacknowledged_order_attempts": max(0, order_attempts - len(ack_samples)),
            "pre_order_exits": max(0, len(rolling_recent) - order_attempts),
            "ack_rate": round(len(ack_samples) / order_attempts, 3) if order_attempts else None,
            "attempt_success_rate": round(successful / order_attempts, 3) if order_attempts else None,
            "success_rate": round(successful / len(ack_samples), 3) if ack_samples else None,
            "median_ms": None,
            "min_ms": None,
            "max_ms": None,
            "jitter_ms": None,
        }
        if ack_samples:
            ordered = sorted(ack_samples)
            middle = len(ordered) // 2
            median = (
                ordered[middle]
                if len(ordered) % 2
                else (ordered[middle - 1] + ordered[middle]) / 2
            )
            rolling.update({
                "median_ms": round(median, 1),
                "min_ms": round(min(ordered), 1),
                "max_ms": round(max(ordered), 1),
                "jitter_ms": round(max(ordered) - min(ordered), 1),
            })
        return {
            "status": "measured" if recent else "no_samples",
            "last": recent[-1] if recent else None,
            "rolling": rolling,
            "recent": recent,
        }

    @staticmethod
    def _valid_latency_ms(value: Any) -> Optional[float]:
        if isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(number) or number < 0:
            return None
        return number

    @classmethod
    def _restore_latency_history(cls, saved_latency: Any) -> list[dict]:
        if not isinstance(saved_latency, dict):
            return []
        saved_recent = saved_latency.get("recent")
        if not isinstance(saved_recent, list):
            return []

        restored = []
        for raw in saved_recent[-50:]:
            if not isinstance(raw, dict) or not isinstance(raw.get("outcome"), str):
                continue
            trace = dict(raw)
            # An in-progress monotonic timestamp is process-local and must
            # never be resumed as a completed latency sample.
            trace.pop("_started_ns", None)
            clean_stages = {}
            stages = trace.get("stages_ms")
            if isinstance(stages, dict):
                for name, value in stages.items():
                    elapsed = cls._valid_latency_ms(value)
                    if elapsed is not None:
                        clean_stages[str(name)[:60]] = round(elapsed, 1)
            trace["stages_ms"] = clean_stages
            for key in ("trigger_to_finish_ms", "trigger_to_order_ack_ms"):
                elapsed = cls._valid_latency_ms(trace.get(key))
                if elapsed is None:
                    trace.pop(key, None)
                else:
                    trace[key] = round(elapsed, 1)
            response_code = str(trace.get("response_code", ""))[:40]
            if response_code:
                trace["response_code"] = response_code
            if response_code.upper() in UNACKNOWLEDGED_RESPONSE_CODES:
                # Repair traces written by the brief legacy instrumentation
                # that labelled local TIMEOUT/ERROR results as exchange ACKs.
                had_false_ack = "trigger_to_order_ack_ms" in trace
                trace.pop("trigger_to_order_ack_ms", None)
                if response_code or had_false_ack or trace.get("order_attempted") is True:
                    trace["order_attempted"] = True
                elif "order_attempted" in trace:
                    trace["order_attempted"] = False
            elif "trigger_to_order_ack_ms" in trace:
                trace["order_attempted"] = True
            elif "order_attempted" in trace:
                trace["order_attempted"] = trace["order_attempted"] is True
            restored.append(trace)
        return restored[-50:]

    async def start(
        self,
        yes_token_id: str = "",
        no_token_id: str = "",
        yes_asset_id: str = "",
        no_asset_id: str = "",
        market_id: str = "",
        threshold_cents: Any = 40,
        buy_limit_cents: Any = 50,
        position_pct: Any = 100,
        timeout_secs: Any = 3,
        settle_ms: int = 0,
        window_ms: int = 150,
        poly_url: str = "",
        selected_outcome: str = "",
        targets: Optional[list[dict]] = None,
        restoring: bool = False,
    ) -> dict:
        async with self._lifecycle_lock:
            self._starting = True
            try:
                return await self._start_locked(
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                    yes_asset_id=yes_asset_id,
                    no_asset_id=no_asset_id,
                    market_id=market_id,
                    threshold_cents=threshold_cents,
                    buy_limit_cents=buy_limit_cents,
                    position_pct=position_pct,
                    timeout_secs=timeout_secs,
                    settle_ms=settle_ms,
                    window_ms=window_ms,
                    poly_url=poly_url,
                    selected_outcome=selected_outcome,
                    targets=targets,
                    restoring=restoring,
                )
            finally:
                self._starting = False

    async def _start_locked(
        self,
        yes_token_id: str = "",
        no_token_id: str = "",
        yes_asset_id: str = "",
        no_asset_id: str = "",
        market_id: str = "",
        threshold_cents: Any = 40,
        buy_limit_cents: Any = 50,
        position_pct: Any = 100,
        timeout_secs: Any = 3,
        settle_ms: int = 0,
        window_ms: int = 150,
        poly_url: str = "",
        selected_outcome: str = "",
        targets: Optional[list[dict]] = None,
        restoring: bool = False,
    ) -> dict:
        """Start the engine.

        New callers pass ``targets``.  The legacy keyword arguments create one
        explicit YES target so an old UI cannot silently arm both outcomes.
        """
        if self._running:
            return {"ok": False, "error": "Already running"}
        if self._persistence_failed:
            return {
                "ok": False,
                "error": "A critical Sniper state was not persisted; retry Stop before restarting",
            }
        if self.state.locked:
            return {
                "ok": False,
                "error": "Sniper is locked by an unresolved order or position; reconcile it before restarting",
            }

        try:
            parsed_targets = self._parse_targets(
                targets,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                yes_asset_id=yes_asset_id,
                no_asset_id=no_asset_id,
                market_id=market_id,
                poly_url=poly_url,
                selected_outcome=selected_outcome,
            )
            if poly_url:
                for target in parsed_targets:
                    if not target.poly_url:
                        target.poly_url = poly_url
            self._validate_targets(parsed_targets)
            self.config = SniperConfig(
                threshold_cents=_decimal(threshold_cents),
                buy_limit_cents=_decimal(buy_limit_cents),
                position_pct=_decimal(position_pct),
                timeout_secs=_decimal(timeout_secs),
                settle_ms=max(0, int(settle_ms)),
                window_ms=max(1, int(window_ms)),
            )
            self._validate_config()
        except (TypeError, ValueError) as exc:
            return {"ok": False, "error": str(exc)}

        previous_logs = self.state.logs[-100:] if restoring else []
        self._run_id = secrets.token_hex(8)
        self._poly_frame_sequence = 0
        self.targets = parsed_targets
        self.state = SniperState(status="monitoring", logs=previous_logs)
        self._set_legacy_fields(parsed_targets[0])
        self._runtime = {target.id: TargetRuntime() for target in parsed_targets}
        self._token_targets = {}
        self._latency_active = None
        for target in parsed_targets:
            self._token_targets.setdefault(target.poly_token_id, []).append(target.id)

        self._running = True
        self._task = self._spawn_task(self._monitor_loop(), "poly-feed")
        self._okx_trace_task = self._spawn_okx_trace_task()
        audit_event(
            "strategy.started",
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            restoring=restoring,
            target_count=len(parsed_targets),
            targets=[target.to_dict() for target in parsed_targets],
            config=self.config.to_dict(),
        )
        self.log(
            f"Started {len(parsed_targets)} target(s): threshold={self.config.threshold_cents}¢, "
            f"buy_limit={self.config.buy_limit_cents}¢, window={self.config.window_ms}ms"
        )
        if not await self._emit_state_change():
            self._running = False
            self.state.status = "stopped"
            monitor_task = self._task
            okx_trace_task = self._okx_trace_task
            self._task = None
            self._okx_trace_task = None
            startup_tasks = [
                task for task in (monitor_task, okx_trace_task)
                if task and not task.done()
            ]
            for task in startup_tasks:
                task.cancel()
            if startup_tasks:
                await asyncio.gather(*startup_tasks, return_exceptions=True)
            self.log(
                "Start aborted: initial configuration could not be persisted",
                logging.ERROR,
            )
            return {
                "ok": False,
                "error": "Sniper configuration could not be persisted",
            }
        return {"ok": True, "targets": [target.to_dict() for target in parsed_targets]}

    async def stop(self):
        async with self._lifecycle_lock:
            return await self._stop_locked()

    async def _stop_locked(self):
        """Stop receiving signals. Existing tracked exposure is left visible."""
        was_running = self._running
        prior_status = self.state.status
        self._running = False
        self.state.status = "stopped"
        for task in (
            *self._trigger_tasks,
            self._hold_task,
            self._task,
            self._okx_trace_task,
        ):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._hold_task = None
        self._task = None
        self._okx_trace_task = None
        self._trigger_tasks.clear()
        if not was_running and self.state.locked:
            self.log("Rechecking stopped target against OKX")
            result = await self._restore_active_trade(resume=False)
            self.log("Stopped")
            return await self._persist_stop_result(result)

        active = self._active_target()
        if active and self.state.entry_client_order_id:
            await self._cancel_strategy_order(active.asset_id, self.state.entry_client_order_id)
            final_size, final_amount, entry_final = await self._wait_order_final(
                self.state.entry_client_order_id,
                active.asset_id,
                "BUY",
            )
            if final_size > self.state.buy_size:
                self.state.buy_size = final_size
                if final_amount > ZERO:
                    self.state.buy_price = final_amount / final_size
            entry_open = await self._is_strategy_order_open(
                self.state.entry_client_order_id, active.market_id
            )
            self.state.locked = (
                not entry_final
                or entry_open is not False
                or self.state.buy_size > self.state.sold_size
            )
            if not entry_final or entry_open is not False:
                self.log(
                    "Stop could not confirm the entry order is final; target remains locked",
                    logging.ERROR,
                )
        elif prior_status == "entering" and self.state.entry_placement_attempted:
            self.state.locked = True
            self.log(
                "Stop interrupted an untracked entry; target remains locked for reconciliation",
                logging.ERROR,
            )
        elif self.state.buy_size <= self.state.sold_size:
            self.state.locked = False
            self.state.active_target_id = ""
        self.log("Stopped")
        audit_event(
            "strategy.stopped",
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            prior_status=prior_status,
            locked=self.state.locked,
            buy_size=_decimal_text(self.state.buy_size),
            sold_size=_decimal_text(self.state.sold_size),
        )
        return await self._persist_stop_result({
            "ok": True,
            "stopped": True,
            "locked": self.state.locked,
            "status": self.state.status,
            "needs_reconciliation": self.state.locked,
        })

    async def _persist_stop_result(self, result: dict) -> dict:
        """Persist the stopped marker before reporting Stop as successful."""
        self._persistence_failed = False
        if await self._emit_state_change():
            return result

        self._persistence_failed = True
        self.state.status = "error"
        message = (
            "Sniper stopped in memory, but its stopped state could not be persisted; "
            "retry Stop before restarting the service"
        )
        self.log(message, logging.ERROR)
        prior_error = str(result.get("error") or "").strip()
        return {
            **result,
            "ok": False,
            "stopped": True,
            "locked": self.state.locked,
            "status": self.state.status,
            "needs_reconciliation": True,
            "persistence_error": True,
            "error": f"{prior_error}; {message}" if prior_error else message,
        }

    async def _persist_critical_state(self, failure_message: str) -> bool:
        """Fail closed when a safety-critical runtime snapshot cannot be saved."""
        if await self._emit_state_change():
            return True
        self._persistence_failed = True
        self._running = False
        self.state.status = "error"
        self.state.locked = True
        self.log(failure_message, logging.ERROR)
        return False

    def _parse_targets(self, targets: Optional[list[dict]], **legacy: str) -> list[SniperTarget]:
        if targets is not None:
            if not isinstance(targets, list):
                raise ValueError("targets must be a list")
            return [SniperTarget.from_dict(item, index) for index, item in enumerate(targets)]

        # Old routes supplied both Poly token IDs. Only the explicitly mapped
        # YES token is armed; NO must be added as its own target by the new UI.
        return [
            SniperTarget(
                id="legacy-yes",
                name=legacy["selected_outcome"] or "YES",
                poly_token_id=legacy["yes_token_id"],
                market_id=legacy["market_id"],
                yes_asset_id=legacy["yes_asset_id"],
                no_asset_id=legacy["no_asset_id"],
                side="yes",
                poly_url=legacy["poly_url"],
            )
        ]

    @staticmethod
    def _validate_targets(targets: list[SniperTarget]):
        if not targets:
            raise ValueError("At least one target is required")
        ids = set()
        token_ids = set()
        for target in targets:
            if not target.id or target.id in ids:
                raise ValueError("Target IDs must be non-empty and unique")
            ids.add(target.id)
            if target.side not in {"yes", "no"}:
                raise ValueError(f"Target {target.id}: side must be yes or no")
            if not target.poly_token_id:
                raise ValueError(f"Target {target.id}: poly_token_id is required")
            if target.poly_token_id in token_ids:
                raise ValueError(
                    f"Target {target.id}: poly_token_id is already used by another target"
                )
            token_ids.add(target.poly_token_id)
            if not target.market_id:
                raise ValueError(f"Target {target.id}: market_id is required")
            if not target.yes_asset_id:
                raise ValueError(f"Target {target.id}: yes_asset_id is required for OKX books")
            if not target.asset_id:
                raise ValueError(f"Target {target.id}: {target.side}_asset_id is required")
            if target.budget is not None and target.budget <= ZERO:
                raise ValueError(f"Target {target.id}: budget must be positive")

    def _validate_config(self):
        if self.config.threshold_cents <= ZERO:
            raise ValueError("threshold_cents must be positive")
        if not ZERO < self.config.buy_limit_cents < Decimal("100"):
            raise ValueError("buy_limit_cents must be between 0 and 100")
        if not ZERO < self.config.position_pct <= Decimal("100"):
            raise ValueError("position_pct must be between 0 and 100")
        if self.config.timeout_secs <= ZERO:
            raise ValueError("timeout_secs must be positive")

    def _set_legacy_fields(self, target: SniperTarget):
        self.state.yes_token_id = target.poly_token_id if target.side == "yes" else ""
        self.state.no_token_id = target.poly_token_id if target.side == "no" else ""
        self.state.yes_asset_id = target.yes_asset_id
        self.state.no_asset_id = target.no_asset_id
        self.state.market_id = target.market_id
        self.state.poly_url = target.poly_url
        self.state.selected_outcome = target.name

    async def _monitor_loop(self):
        try:
            while self._running:
                try:
                    await self._ws_monitor()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    logger.debug("[Sniper] Poly WS error: %s", exc, exc_info=True)
                if self._running:
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            return

    @staticmethod
    def _apply_okx_trace_levels(levels: dict[Decimal, list], rows: Iterable) -> None:
        for row in rows or []:
            if len(row) < 2:
                continue
            price = _decimal(row[0])
            size = _decimal(row[1])
            if size <= ZERO:
                levels.pop(price, None)
            else:
                levels[price] = list(row)

    def _okx_trace_views(self, yes_asset_id: str, bids: list, asks: list) -> list[dict]:
        views = []
        yes_best_bid = _decimal(bids[0][0]) if bids else ZERO
        yes_best_ask = _decimal(asks[0][0]) if asks else ZERO
        for target in self.targets:
            if target.yes_asset_id != yes_asset_id:
                continue
            if target.side == "no":
                best_bid = ONE - yes_best_ask if yes_best_ask > ZERO else ZERO
                best_ask = ONE - yes_best_bid if yes_best_bid > ZERO else ZERO
                derived = True
            else:
                best_bid = yes_best_bid
                best_ask = yes_best_ask
                derived = False
            views.append({
                "target_id": target.id,
                "target_name": target.name,
                "side": target.side,
                "asset_id": target.asset_id,
                "best_bid": _decimal_text(best_bid),
                "best_ask": _decimal_text(best_ask),
                "derived_from_yes_book": derived,
            })
        return views

    async def _okx_market_trace_loop(self):
        """Observe OKX books for diagnostics only; never drive a trade decision."""
        retry_delay = 1.0
        while self._running:
            try:
                await self._okx_market_trace_session()
                retry_delay = 1.0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(
                    "[Sniper:%s] OKX trace observer error: %s",
                    self.account_id,
                    exc,
                    exc_info=True,
                )
                audit_event(
                    "sniper.okx_trace_error",
                    level=logging.WARNING,
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    error=exc,
                    retry_delay=retry_delay,
                )
            if self._running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 10.0)

    async def _okx_market_trace_session(self):
        yes_assets = sorted({target.yes_asset_id for target in self.targets if target.yes_asset_id})
        if not yes_assets:
            audit_event(
                "telemetry.unavailable",
                level=logging.WARNING,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                feed="okx",
                reason="no yesAssetId mappings",
            )
            return

        connector = None
        proxy_node = str(getattr(self.client, "_proxy_node", "") or "")
        node = PROXY_NODES.get(proxy_node, {})
        if node.get("host") is not None:
            connector = ProxyConnector.from_url(
                f"socks5://{node['host']}:{node['port']}"
            )
        base_url = str(getattr(self.client, "base_url", "") or "")
        endpoint = OKX_WS_US if "us.okx.com" in base_url else OKX_WS_DEFAULT
        book_maps: dict[str, tuple[dict[Decimal, list], dict[Decimal, list]]] = {}
        last_sequences: dict[str, int] = {}
        snapshots: set[str] = set()

        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.ws_connect(endpoint, heartbeat=20) as ws:
                await ws.send_str(json.dumps({
                    "op": "subscribe",
                    "args": [
                        {"channel": "pm-books", "instId": asset_id}
                        for asset_id in yes_assets
                    ],
                }))
                audit_event(
                    "sniper.okx_trace_connected",
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    endpoint=endpoint,
                    proxy_node=proxy_node or "Direct",
                    yes_asset_ids=yes_assets,
                )
                async for msg in ws:
                    received_wall_ns = time.time_ns()
                    received_monotonic_ns = time.monotonic_ns()
                    if not self._running:
                        return
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise RuntimeError(f"OKX trace websocket ended: {msg.type}")
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    data = json.loads(msg.data)
                    if data.get("event") == "error" or data.get("code") not in (None, "0", 0):
                        raise RuntimeError(
                            data.get("msg") or data.get("message") or "OKX trace subscription failed"
                        )
                    if data.get("arg", {}).get("channel") != "pm-books":
                        continue
                    asset_id = str(data.get("arg", {}).get("instId") or "")
                    if asset_id not in yes_assets:
                        continue
                    action = str(data.get("action") or "").lower()
                    for book in data.get("data") or []:
                        bid_map, ask_map = book_maps.setdefault(asset_id, ({}, {}))
                        delta_bids = book.get("bids") or []
                        delta_asks = book.get("asks") or []
                        sequence = book.get("seqId", book.get("seq_id"))
                        previous_sequence = book.get("prevSeqId", book.get("prev_seq_id"))
                        checksum = book.get("checksum")
                        if action == "snapshot":
                            bid_map.clear()
                            ask_map.clear()
                            self._apply_okx_trace_levels(bid_map, delta_bids)
                            self._apply_okx_trace_levels(ask_map, delta_asks)
                            snapshots.add(asset_id)
                        elif action == "update":
                            if asset_id not in snapshots:
                                raise RuntimeError("OKX trace update arrived before snapshot")
                            last_sequence = last_sequences.get(asset_id)
                            if last_sequence is not None and previous_sequence is not None:
                                try:
                                    contiguous = int(previous_sequence) == int(last_sequence)
                                except (TypeError, ValueError) as exc:
                                    raise RuntimeError("OKX trace sequence is invalid") from exc
                                if not contiguous:
                                    market_event(
                                        "okx",
                                        "sniper.sequence_gap",
                                        level=logging.WARNING,
                                        component="poly-sniper",
                                        account_id=self.account_id,
                                        run_id=self._run_id,
                                        yes_asset_id=asset_id,
                                        expected_previous=last_sequence,
                                        received_previous=previous_sequence,
                                        sequence=sequence,
                                    )
                                    raise RuntimeError("OKX trace sequence gap")
                            self._apply_okx_trace_levels(bid_map, delta_bids)
                            self._apply_okx_trace_levels(ask_map, delta_asks)
                        else:
                            continue
                        if sequence is not None:
                            last_sequences[asset_id] = int(sequence)
                        bids = [bid_map[key] for key in sorted(bid_map, reverse=True)[:10]]
                        asks = [ask_map[key] for key in sorted(ask_map)[:10]]
                        market_event(
                            "okx",
                            "sniper.book_update",
                            component="poly-sniper",
                            account_id=self.account_id,
                            run_id=self._run_id,
                            endpoint=endpoint,
                            proxy_node=proxy_node or "Direct",
                            yes_asset_id=asset_id,
                            action=action,
                            source_timestamp=book.get("ts", ""),
                            received_wall_ns=received_wall_ns,
                            received_monotonic_ns=received_monotonic_ns,
                            sequence=sequence,
                            previous_sequence=previous_sequence,
                            checksum_reported=checksum,
                            checksum_verified=(False if checksum is not None else None),
                            checksum_verification_reason=(
                                "official pm-books canonicalization is unspecified"
                                if checksum is not None
                                else "checksum not supplied"
                            ),
                            delta_bids=(delta_bids if action == "update" else None),
                            delta_asks=(delta_asks if action == "update" else None),
                            raw_snapshot=(
                                {"bids": delta_bids, "asks": delta_asks}
                                if action == "snapshot"
                                else None
                            ),
                            bid_level_count=len(bid_map),
                            ask_level_count=len(ask_map),
                            bids=bids,
                            asks=asks,
                            target_views=self._okx_trace_views(asset_id, bids, asks),
                            telemetry_only=True,
                        )

    async def _ws_monitor(self):
        token_ids = list(self._token_targets)
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(POLY_WS_URL, heartbeat=15) as ws:
                for runtime in self._runtime.values():
                    runtime.last_bid = ZERO
                    runtime.baseline = ZERO
                    runtime.calibrated = False
                    runtime.price_window = []
                self.state.last_yes_bid = ZERO
                self.state.last_no_bid = ZERO
                await ws.send_str(json.dumps({"type": "market", "assets_ids": token_ids}))
                self.log(f"WS connected, monitoring {len(self.targets)} explicit target(s)")
                async for msg in ws:
                    received_wall_ns = time.time_ns()
                    received_monotonic_ns = time.monotonic_ns()
                    signal_received_ns = time.perf_counter_ns()
                    frame_id = self._next_poly_frame_id()
                    if not self._running:
                        return
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            raw = json.loads(msg.data)
                            events = raw if isinstance(raw, list) else [raw]
                            for event_index, event in enumerate(events):
                                await self._process_ws_event(
                                    event,
                                    received_wall_ns=received_wall_ns,
                                    received_monotonic_ns=received_monotonic_ns,
                                    signal_received_ns=signal_received_ns,
                                    frame_id=frame_id,
                                    source_event_index=event_index,
                                )
                        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                            logger.debug(
                                "[Sniper] Invalid Poly WS event: %s",
                                exc,
                                exc_info=True,
                            )
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        return

    async def _process_ws_event(
        self,
        event: dict,
        *,
        received_wall_ns: Optional[int] = None,
        received_monotonic_ns: Optional[int] = None,
        signal_received_ns: Optional[int] = None,
        frame_id: str = "",
        source_event_index: int = 0,
    ):
        event_type = event.get("event_type", "")
        if event_type == "book":
            bids = event.get("bids") or []
            token_id = str(event.get("asset_id", ""))
            market_event(
                "polymarket",
                "sniper.book",
                component="poly-sniper",
                account_id=self.account_id,
                run_id=self._run_id,
                frame_id=frame_id,
                source_event_index=source_event_index,
                poly_token_id=token_id,
                target_ids=self._token_targets.get(token_id, []),
                source_timestamp=event.get("timestamp", event.get("ts", "")),
                received_wall_ns=received_wall_ns or time.time_ns(),
                received_monotonic_ns=received_monotonic_ns or time.monotonic_ns(),
                bids=bids,
                asks=event.get("asks") or [],
            )
            best = max((_decimal(row.get("price")) for row in bids), default=ZERO)
            await self._process_bid(
                token_id,
                best,
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                signal_received_ns=signal_received_ns,
                frame_id=frame_id,
                source_event_index=source_event_index,
            )
            return
        if event_type == "price_change":
            changes = event.get("price_changes") or []
            market_event(
                "polymarket",
                "sniper.price_change",
                component="poly-sniper",
                account_id=self.account_id,
                run_id=self._run_id,
                frame_id=frame_id,
                source_event_index=source_event_index,
                source_timestamp=event.get("timestamp", event.get("ts", "")),
                received_wall_ns=received_wall_ns or time.time_ns(),
                received_monotonic_ns=received_monotonic_ns or time.monotonic_ns(),
                changes=changes,
            )
            for change_index, change in enumerate(changes):
                # best_bid is the post-change book value. A raw trade/order
                # price is not a safe substitute when a level was removed.
                best_bid = change.get("best_bid")
                if best_bid not in (None, ""):
                    token_id = change.get("asset_id", change.get("assetId", ""))
                    await self._process_bid(
                        str(token_id),
                        _decimal(best_bid),
                        received_wall_ns=received_wall_ns,
                        received_monotonic_ns=received_monotonic_ns,
                        signal_received_ns=signal_received_ns,
                        frame_id=frame_id,
                        source_event_index=source_event_index,
                        source_change_index=change_index,
                    )

    async def _process_book_update(self, data: dict):
        """Compatibility hook used by older tests/callers."""
        await self._process_ws_event({**data, "event_type": "book"})

    async def _process_bid(
        self,
        token_id: str,
        current_bid: Decimal,
        *,
        received_wall_ns: Optional[int] = None,
        received_monotonic_ns: Optional[int] = None,
        signal_received_ns: Optional[int] = None,
        frame_id: str = "",
        source_event_index: int = 0,
        source_change_index: Optional[int] = None,
    ):
        signal_received_ns = signal_received_ns or time.perf_counter_ns()
        decision_wall_ns = received_wall_ns or time.time_ns()
        decision_monotonic_ns = received_monotonic_ns or time.monotonic_ns()
        target_ids = self._token_targets.get(token_id, [])
        decision_context = {
            "frame_id": frame_id,
            "source_event_index": source_event_index,
            "source_change_index": source_change_index,
            "signal_received_perf_ns": signal_received_ns,
        }
        if current_bid <= ZERO:
            for target_id in target_ids:
                runtime = self._runtime[target_id]
                runtime.last_bid = ZERO
                runtime.baseline = ZERO
                runtime.calibrated = False
                runtime.price_window = []
                if target_id == self.targets[0].id:
                    if self.targets[0].side == "yes":
                        self.state.last_yes_bid = ZERO
                    else:
                        self.state.last_no_bid = ZERO
                market_event(
                    "polymarket",
                    "sniper.bid_decision",
                    component="poly-sniper",
                    account_id=self.account_id,
                    run_id=self._run_id,
                    target_id=target_id,
                    poly_token_id=token_id,
                    received_wall_ns=decision_wall_ns,
                    received_monotonic_ns=decision_monotonic_ns,
                    current_bid="0",
                    decision="empty_bid_reset",
                    status=self.state.status,
                    locked=self.state.locked,
                    **decision_context,
                )
            return
        for target_id in target_ids:
            runtime = self._runtime[target_id]
            previous = runtime.last_bid
            runtime.last_bid = current_bid
            if target_id == self.targets[0].id:
                if self.targets[0].side == "yes":
                    self.state.last_yes_bid = current_bid
                else:
                    self.state.last_no_bid = current_bid

            if self.state.locked or self.state.status != "monitoring":
                market_event(
                    "polymarket",
                    "sniper.bid_decision",
                    component="poly-sniper",
                    account_id=self.account_id,
                    run_id=self._run_id,
                    target_id=target_id,
                    poly_token_id=token_id,
                    received_wall_ns=decision_wall_ns,
                    received_monotonic_ns=decision_monotonic_ns,
                    previous_bid=_decimal_text(previous),
                    current_bid=_decimal_text(current_bid),
                    decision="observed_while_locked",
                    status=self.state.status,
                    locked=self.state.locked,
                    **decision_context,
                )
                continue
            if not runtime.calibrated or previous <= ZERO:
                runtime.calibrated = True
                runtime.baseline = current_bid
                runtime.price_window = [(time.monotonic(), current_bid)]
                market_event(
                    "polymarket",
                    "sniper.bid_decision",
                    component="poly-sniper",
                    account_id=self.account_id,
                    run_id=self._run_id,
                    target_id=target_id,
                    poly_token_id=token_id,
                    received_wall_ns=decision_wall_ns,
                    received_monotonic_ns=decision_monotonic_ns,
                    previous_bid=_decimal_text(previous),
                    current_bid=_decimal_text(current_bid),
                    window_low=_decimal_text(current_bid),
                    window_high=_decimal_text(current_bid),
                    threshold_cents=_decimal_text(self.config.threshold_cents),
                    decision="calibrated",
                    **decision_context,
                )
                continue

            now = time.monotonic()
            runtime.price_window.append((now, current_bid))
            cutoff = now - self.config.window_ms / 1000
            runtime.price_window = [(stamp, price) for stamp, price in runtime.price_window if stamp >= cutoff]
            runtime.baseline = max((price for _, price in runtime.price_window), default=current_bid)
            window_low = min((price for _, price in runtime.price_window), default=current_bid)
            jump_cents = (current_bid - window_low) * Decimal("100")

            if jump_cents < self.config.threshold_cents:
                market_event(
                    "polymarket",
                    "sniper.bid_decision",
                    component="poly-sniper",
                    account_id=self.account_id,
                    run_id=self._run_id,
                    target_id=target_id,
                    poly_token_id=token_id,
                    received_wall_ns=decision_wall_ns,
                    received_monotonic_ns=decision_monotonic_ns,
                    previous_bid=_decimal_text(previous),
                    current_bid=_decimal_text(current_bid),
                    window_low=_decimal_text(window_low),
                    window_high=_decimal_text(runtime.baseline),
                    jump_cents=_decimal_text(jump_cents),
                    threshold_cents=_decimal_text(self.config.threshold_cents),
                    window_ms=self.config.window_ms,
                    decision="below_threshold",
                    **decision_context,
                )
                if now - runtime.last_no_trigger_log >= 2:
                    runtime.last_no_trigger_log = now
                    self.log(
                        f"NO TRIGGER: {target_id} {_decimal_text(window_low)}→{_decimal_text(current_bid)} "
                        f"(+{_decimal_text(jump_cents)}¢) < {self.config.threshold_cents}¢"
                    )
                continue

            runtime.price_window = []
            if runtime.baseline > ZERO and current_bid < runtime.baseline * Decimal("0.7"):
                market_event(
                    "polymarket",
                    "sniper.bid_decision",
                    component="poly-sniper",
                    account_id=self.account_id,
                    run_id=self._run_id,
                    target_id=target_id,
                    poly_token_id=token_id,
                    received_wall_ns=decision_wall_ns,
                    received_monotonic_ns=decision_monotonic_ns,
                    previous_bid=_decimal_text(previous),
                    current_bid=_decimal_text(current_bid),
                    window_low=_decimal_text(window_low),
                    window_high=_decimal_text(runtime.baseline),
                    jump_cents=_decimal_text(jump_cents),
                    decision="bounce_filter",
                    **decision_context,
                )
                self.log(f"BOUNCE SKIP: {target_id} remains below 70% of its high watermark")
                continue
            candidate_trace_id = secrets.token_hex(8)
            market_event(
                "polymarket",
                "sniper.bid_decision",
                component="poly-sniper",
                account_id=self.account_id,
                run_id=self._run_id,
                target_id=target_id,
                poly_token_id=token_id,
                received_wall_ns=decision_wall_ns,
                received_monotonic_ns=decision_monotonic_ns,
                previous_bid=_decimal_text(previous),
                current_bid=_decimal_text(current_bid),
                window_low=_decimal_text(window_low),
                window_high=_decimal_text(runtime.baseline),
                jump_cents=_decimal_text(jump_cents),
                threshold_cents=_decimal_text(self.config.threshold_cents),
                decision="trigger_scheduled",
                trace_id=candidate_trace_id,
                **decision_context,
            )
            task = self._spawn_task(
                self._try_trigger(
                    target_id,
                    window_low,
                    current_bid,
                    signal_received_ns=signal_received_ns,
                    trace_id=candidate_trace_id,
                    frame_id=frame_id,
                ),
                f"trigger:{target_id}",
            )
            self._trigger_tasks.add(task)
            task.add_done_callback(self._trigger_tasks.discard)

    async def _try_trigger(
        self,
        target_id: str,
        previous_bid: Decimal,
        current_bid: Decimal,
        signal_received_ns: Optional[int] = None,
        trace_id: str = "",
        frame_id: str = "",
    ):
        trace_id = trace_id or secrets.token_hex(8)
        async with self._trigger_lock:
            if not self._running or self.state.locked or self.state.status != "monitoring":
                audit_event(
                    "sniper.trigger_candidate_rejected",
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    trace_id=trace_id,
                    poly_frame_id=frame_id,
                    target_id=target_id,
                    running=self._running,
                    locked=self.state.locked,
                    status=self.state.status,
                    reason="global_lock_or_state",
                )
                return
            self.state.locked = True
            self.state.active_target_id = target_id
            self.state.status = "triggered"
        trace_id = self._begin_latency_trace(
            target_id,
            signal_received_ns,
            trace_id=trace_id,
            frame_id=frame_id,
        )
        with log_context(trace_id=trace_id, target_id=target_id):
            audit_event(
                "sniper.trigger_acquired",
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                trace_id=trace_id,
                poly_frame_id=frame_id,
                target_id=target_id,
                previous_poly_bid=_decimal_text(previous_bid),
                current_poly_bid=_decimal_text(current_bid),
            )
            self._record_latency_stage(
                "signal_to_lock",
                signal_received_ns or time.perf_counter_ns(),
            )
            persist_started = time.perf_counter_ns()
            try:
                await self._emit_state_change()
            finally:
                self._record_latency_stage("trigger_persist", persist_started)

            try:
                await self._run_trigger(target_id, previous_bid, current_bid)
            except asyncio.CancelledError:
                await self._handle_trigger_failure(target_id, "Trigger task was cancelled")
                raise
            except Exception as exc:
                logger.exception("[Sniper:%s] Trigger failed", self.account_id)
                await self._handle_trigger_failure(target_id, f"Trigger error: {exc}")

    async def _handle_trigger_failure(self, target_id: str, reason: str):
        if self.state.active_target_id != target_id:
            return
        has_uncertain_order = bool(
            self.state.entry_placement_attempted
            or self.state.entry_client_order_id
            or self.state.buy_size > self.state.sold_size
        )
        if has_uncertain_order:
            self._finish_latency_trace("trigger_error", reason=reason)
            self.log(reason, logging.ERROR)
            self.state.status = "error"
            self.state.locked = True
            await self._persist_critical_state(
                "Uncertain trigger state could not be persisted; retry Stop before restarting"
            )
            return
        await self._release_trigger(reason)

    async def _run_trigger(
        self, target_id: str, previous_bid: Decimal, current_bid: Decimal
    ):
        target = self._target(target_id)
        self.log(
            f"JUMP: {target.name} ({target.side.upper()}) {_decimal_text(previous_bid)}→"
            f"{_decimal_text(current_bid)}; global lock acquired"
        )
        if self.config.settle_ms > 0:
            self.state.status = "settling"
            settle_started = time.perf_counter_ns()
            try:
                current_bid = await self._wait_for_settle(target)
            finally:
                self._record_latency_stage("settle", settle_started)
            if not self._running:
                self._finish_latency_trace("stopped_during_settle")
                return
            if (current_bid - previous_bid) * Decimal("100") < self.config.threshold_cents:
                await self._release_trigger("Poly move no longer meets threshold after settle window")
                return
            self.state.status = "triggered"
        book_started = time.perf_counter_ns()
        try:
            asks = await self._snapshot_okx_asks(target)
        finally:
            self._record_latency_stage("okx_book", book_started)
        if not asks:
            await self._release_trigger("OKX has no usable ask depth")
            return

        price_cap = min(self.config.buy_limit_cents / Decimal("100"), current_bid)
        eligible = [(price, size) for price, size in asks if price <= price_cap and price < current_bid]
        if not eligible:
            await self._release_trigger("No OKX asks below both the Poly signal and buy limit")
            return

        bought = await self._execute_entry(target, eligible, previous_bid, current_bid)
        if not bought:
            await self._release_trigger("Entry did not fill")
            return
        if not self._running or self.state.status == "error":
            return
        self.state.status = "holding"
        self.state.hold_start = time.monotonic()
        self.state.hold_start_epoch_ms = int(time.time() * 1000)
        if not await self._persist_critical_state(
            "Holding state could not be persisted; retry Stop before restarting"
        ):
            return
        self._hold_task = self._spawn_task(self._holding_loop(), "holding")

    async def _wait_for_settle(self, target: SniperTarget) -> Decimal:
        """Wait until the active Poly token has been unchanged for settle_ms."""
        runtime = self._runtime[target.id]
        observed = runtime.last_bid
        last_change = time.monotonic()
        settle_seconds = self.config.settle_ms / 1000
        while self._running:
            await asyncio.sleep(min(0.05, settle_seconds))
            if runtime.last_bid != observed:
                observed = runtime.last_bid
                last_change = time.monotonic()
            if time.monotonic() - last_change >= settle_seconds:
                self.log(f"Poly settled at {_decimal_text(observed)} for {self.config.settle_ms}ms")
                return observed
        return observed

    async def _release_trigger(self, reason: str):
        self._finish_latency_trace("released_before_order", reason=reason)
        self.log(f"Trigger released: {reason}")
        self.state.status = "monitoring" if self._running else "stopped"
        self.state.locked = False
        self.state.active_target_id = ""
        self.state.entry_client_order_id = ""
        self.state.entry_placement_attempted = False
        await self._emit_state_change()

    async def _snapshot_okx_asks(self, target_or_asset) -> list[tuple[Decimal, Decimal]]:
        target = target_or_asset if isinstance(target_or_asset, SniperTarget) else self._active_target()
        if target is None:
            return []
        started_ns = time.perf_counter_ns()
        try:
            response = await self.client.get_orderbook(target.yes_asset_id, depth=10)
            received_wall_ns = time.time_ns()
            received_monotonic_ns = time.monotonic_ns()
            response_code = str(response.get("code", ""))
            if response_code != "0":
                audit_event(
                    "sniper.entry_book_failed",
                    level=logging.WARNING,
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    target_id=target.id,
                    yes_asset_id=target.yes_asset_id,
                    side=target.side,
                    response_code=response_code,
                    response_message=response.get("msg", response.get("message", "")),
                    duration_ms=self._elapsed_ms(started_ns),
                )
                self.log(
                    f"OKX book error: {response.get('msg', '')}",
                    logging.WARNING,
                )
                return []
            book = (response.get("data") or [{}])[0]
            if target.side == "no":
                rows = self._invert_asks_from_yes_book(book.get("bids", []))
            else:
                rows = []
                for row in book.get("asks", []) or []:
                    if len(row) >= 2:
                        price, size = _decimal(row[0]), _decimal(row[1])
                        if ZERO < price < ONE and size > ZERO:
                            rows.append((price, size))
                rows = sorted(rows, key=lambda row: row[0])
            market_event(
                "okx",
                "sniper.rest_book",
                component="poly-sniper",
                account_id=self.account_id,
                run_id=self._run_id,
                purpose="entry",
                target_id=target.id,
                target_side=target.side,
                market_id=target.market_id,
                yes_asset_id=target.yes_asset_id,
                selected_asset_id=target.asset_id,
                source_timestamp=book.get("ts", ""),
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                duration_ms=self._elapsed_ms(started_ns),
                yes_bids=(book.get("bids") or [])[:10],
                yes_asks=(book.get("asks") or [])[:10],
                selected_asks=[
                    [_decimal_text(price), _decimal_text(size)] for price, size in rows[:10]
                ],
                derived_from_yes_book=target.side == "no",
            )
            return rows
        except Exception as exc:
            logger.exception(
                "[Sniper:%s] entry order-book snapshot failed for %s",
                self.account_id,
                target.id,
            )
            audit_event(
                "sniper.entry_book_exception",
                level=logging.ERROR,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                yes_asset_id=target.yes_asset_id,
                duration_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            self.log(f"OKX book exception: {exc}", logging.ERROR)
            return []

    @staticmethod
    def _invert_asks_from_yes_book(yes_bids: Iterable) -> list[tuple[Decimal, Decimal]]:
        result = []
        for row in yes_bids or []:
            if len(row) >= 2:
                price, size = ONE - _decimal(row[0]), _decimal(row[1])
                if ZERO < price < ONE and size > ZERO:
                    result.append((price, size))
        return sorted(result, key=lambda row: row[0])

    @staticmethod
    def _invert_bids_from_yes_book(yes_asks: Iterable) -> list[tuple[Decimal, Decimal]]:
        result = []
        for row in yes_asks or []:
            if len(row) >= 2:
                price, size = ONE - _decimal(row[0]), _decimal(row[1])
                if ZERO < price < ONE and size > ZERO:
                    result.append((price, size))
        return sorted(result, key=lambda row: row[0], reverse=True)

    async def _execute_entry(
        self,
        target: SniperTarget,
        levels: list[tuple[Decimal, Decimal]],
        previous_bid: Decimal,
        current_bid: Decimal,
    ) -> bool:
        max_price = _floor_to_tick(max(price for price, _ in levels))
        depth_size = sum((size for price, size in levels if price <= max_price), ZERO)
        audit_event(
            "sniper.entry_depth_evaluated",
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            target_id=target.id,
            side=target.side,
            asset_id=target.asset_id,
            previous_poly_bid=_decimal_text(previous_bid),
            current_poly_bid=_decimal_text(current_bid),
            max_price=_decimal_text(max_price),
            depth_size=_decimal_text(depth_size),
            levels=[
                [_decimal_text(price), _decimal_text(size)] for price, size in levels[:10]
            ],
            decision="continue" if max_price > ZERO and depth_size >= ONE else "reject_depth",
        )
        if max_price <= ZERO or depth_size < ONE:
            return False

        placement_attempted = False
        try:
            position_started = time.perf_counter_ns()
            try:
                position_before = await self._query_position_size(target)
            finally:
                self._record_latency_stage("positions", position_started)
            if position_before is None:
                self.log(
                    "Entry blocked: OKX position baseline is unavailable",
                    logging.WARNING,
                )
                return False
            self.state.position_before = position_before
            balance_started = time.perf_counter_ns()
            try:
                balance = await self._available_balance()
            finally:
                self._record_latency_stage("balance", balance_started)
            budget = target.budget if target.budget is not None else balance * self.config.position_pct / Decimal("100")
            budget = min(budget, balance)
            requested_size = min((budget / max_price).to_integral_value(rounding=ROUND_DOWN), depth_size)
            size_text = normalize_buy_size_str(requested_size)
            executable_size = _decimal(size_text)
            audit_event(
                "sniper.entry_size_evaluated",
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                asset_id=target.asset_id,
                available_balance=_decimal_text(balance),
                configured_target_budget=(
                    _decimal_text(target.budget) if target.budget is not None else None
                ),
                position_pct=_decimal_text(self.config.position_pct),
                effective_budget=_decimal_text(budget),
                max_price=_decimal_text(max_price),
                depth_size=_decimal_text(depth_size),
                requested_size=_decimal_text(requested_size),
                normalized_size=size_text,
                notional=_decimal_text(executable_size * max_price),
                decision=(
                    "continue"
                    if executable_size >= ONE and executable_size * max_price >= ONE
                    else "reject_minimum"
                ),
            )
            if executable_size < ONE or executable_size * max_price < ONE:
                self.log(f"Insufficient balance/depth for {target.id}")
                return False

            notify_started = time.perf_counter_ns()
            try:
                await self._notify(
                    f"⚡ {target.name} {target.side.upper()} {_decimal_text(previous_bid)}→{_decimal_text(current_bid)}"
                )
            finally:
                self._record_latency_stage("notify_queue", notify_started)
            client_oid = ""
            oid_factory = getattr(self.client, "generate_client_order_id", None)
            if callable(oid_factory):
                generated_oid = oid_factory()
                client_oid = str(generated_oid) if generated_oid else ""
            if not client_oid:
                self.log(
                    "Entry blocked: clientOrderId could not be generated",
                    logging.ERROR,
                )
                return False
            self.state.status = "entering"
            self.state.entry_placement_attempted = True
            self.state.entry_client_order_id = client_oid
            persist_started = time.perf_counter_ns()
            try:
                persisted = await self._emit_state_change()
            finally:
                self._record_latency_stage("pre_order_persist", persist_started)
            if not persisted:
                self.state.status = "triggered"
                self.state.entry_placement_attempted = False
                self.state.entry_client_order_id = ""
                self.log(
                    "Entry blocked: clientOrderId snapshot could not be persisted",
                    logging.ERROR,
                )
                return False
            placement_attempted = True
            order_kwargs = dict(
                asset_id=target.asset_id,
                side="buy",
                price=_okx_price_text(max_price),
                size=size_text,
                # This is an immediate, price-protected sweep.  IOC preserves
                # partial fills and makes OKX cancel any unfilled remainder,
                # so a process failure cannot leave a Sniper entry resting.
                tif="ioc",
            )
            if client_oid:
                order_kwargs["client_order_id"] = client_oid
            audit_event(
                "sniper.entry_order_submitting",
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                asset_id=target.asset_id,
                side="buy",
                price=order_kwargs["price"],
                size=size_text,
                tif="ioc",
                client_order_id=client_oid,
            )
            order_started = time.perf_counter_ns()
            self._mark_latency_order_attempted()
            try:
                response = await self.client.place_order(**order_kwargs)
            finally:
                self._record_latency_stage("order_submit", order_started)
            response_code = str(response.get("code", "")) if isinstance(response, dict) else "INVALID"
            order_acknowledged = (
                response_code.upper() not in UNACKNOWLEDGED_RESPONSE_CODES
            )
            self._finish_latency_trace(
                (
                    "order_ack"
                    if response_code == "0"
                    else "order_response_error"
                    if order_acknowledged
                    else "order_unacknowledged"
                ),
                response_code=response_code,
                order_ack=order_acknowledged,
            )
            response_oid = str(response.get("_clientOrderId") or client_oid)
            audit_event(
                "sniper.entry_order_response",
                level=logging.INFO if response_code == "0" else logging.WARNING,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                asset_id=target.asset_id,
                client_order_id=response_oid,
                response_code=response_code,
                response_message=response.get("message", response.get("msg", "")),
                acknowledged=order_acknowledged,
                duration_ms=self._elapsed_ms(order_started),
            )
            if response_oid != client_oid:
                client_oid = response_oid
                self.state.entry_client_order_id = client_oid
                await self._emit_state_change()
            if str(response.get("code", "")) != "0":
                self.log(
                    f"Entry rejected: {response.get('message', response.get('msg', ''))}",
                    logging.WARNING,
                )
                if str(response.get("code", "")).upper() in {"TIMEOUT", "ERROR"}:
                    await self._cancel_strategy_order(target.asset_id, client_oid)
                    filled_size, filled_amount, entry_final = await self._wait_order_final(
                        client_oid, target.asset_id, "BUY"
                    )
                    if entry_final:
                        self.state.buy_size = filled_size
                        self.state.buy_price = (
                            filled_amount / filled_size
                            if filled_size > ZERO and filled_amount > ZERO
                            else max_price
                        )
                        self.state.sold_size = ZERO
                        await self._emit_state_change()
                        if filled_size <= ZERO:
                            self.state.status = "triggered"
                            self.state.entry_placement_attempted = False
                            self.state.entry_client_order_id = ""
                            await self._emit_state_change()
                        return filled_size > ZERO
                    self.state.status = "error"
                    self.state.locked = True
                    self._running = False
                    self.log(
                        "Entry result is ambiguous; stopped with global lock",
                        logging.ERROR,
                    )
                    await self._persist_critical_state(
                        "Ambiguous entry state could not be persisted; retry Stop before restarting"
                    )
                    return True
                self.state.status = "triggered"
                self.state.entry_placement_attempted = False
                self.state.entry_client_order_id = ""
                await self._emit_state_change()
                return False
            client_oid = response_oid
            self.state.entry_client_order_id = client_oid
            await self._emit_state_change()

            # IOC has no resting remainder to cancel.  Keep the persisted
            # clientOrderId and confirm its final fill through closed orders.
            filled_size, filled_amount, entry_final = await self._wait_order_final(
                client_oid,
                target.asset_id,
                "BUY",
            )
            self.state.buy_size = filled_size
            self.state.buy_price = filled_amount / filled_size if filled_amount > ZERO else max_price
            self.state.sold_size = ZERO
            if not entry_final:
                self.state.status = "error"
                self.state.locked = True
                self._running = False
                self.log(
                    "Entry order final state could not be confirmed; stopped with global lock",
                    logging.ERROR,
                )
                await self._persist_critical_state(
                    "Unconfirmed entry state could not be persisted; retry Stop before restarting"
                )
                return True
            if filled_size <= ZERO:
                self.state.status = "triggered"
                self.state.entry_placement_attempted = False
                self.state.entry_client_order_id = ""
                await self._emit_state_change()
                return False
            self.log(
                f"Bought {target.name} {target.side.upper()} @{_decimal_text(self.state.buy_price)} "
                f"x{normalize_size_str(filled_size)}"
            )
            await self._emit_state_change()
            return True
        except Exception as exc:
            self._finish_latency_trace(
                "entry_exception",
                reason=type(exc).__name__,
            )
            self.log(f"Entry exception: {exc}", logging.ERROR)
            if placement_attempted:
                self.state.status = "error"
                self.state.locked = True
                self._running = False
                self.log(
                    "Entry result is ambiguous; stopped with global lock",
                    logging.ERROR,
                )
                await self._persist_critical_state(
                    "Ambiguous entry state could not be persisted; retry Stop before restarting"
                )
                return True
            return False

    async def _available_balance(self) -> Decimal:
        started_ns = time.perf_counter_ns()
        try:
            response = await self.client.get_balance()
            response_code = str(response.get("code", ""))
            available = ZERO
            for entry in response.get("data") or []:
                if entry.get("oddsType") == "points":
                    available = _decimal(entry.get("available"))
                    break
            audit_event(
                "sniper.balance_snapshot",
                level=logging.INFO if response_code == "0" else logging.WARNING,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                response_code=response_code,
                available=_decimal_text(available),
                duration_ms=self._elapsed_ms(started_ns),
            )
            if response_code != "0":
                raise RuntimeError(response.get("msg") or "balance query failed")
            return available
        except Exception as exc:
            logger.exception("[Sniper:%s] balance snapshot failed", self.account_id)
            audit_event(
                "sniper.balance_exception",
                level=logging.ERROR,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                duration_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            raise

    async def _cancel_strategy_order(self, asset_id: str, client_oid: str) -> bool:
        if not client_oid:
            self.log(
                "Cannot safely cancel order: clientOrderId missing",
                logging.ERROR,
            )
            return False
        try:
            response = await self.client.cancel_order(asset_id=asset_id, client_order_id=client_oid)
            if str(response.get("code", "")) != "0":
                self.log(
                    f"Target order cancel rejected: {response.get('msg', '')}",
                    logging.WARNING,
                )
                return False
            return True
        except Exception as exc:
            self.log(f"Target order cancel failed: {exc}", logging.ERROR)
            return False

    async def _is_strategy_order_open(self, client_oid: str, market_id: str) -> Optional[bool]:
        if not client_oid:
            return None
        try:
            response = await self.client.get_orders(status="open", market_id=market_id)
            if str(response.get("code", "")) != "0":
                return None
            return any(
                str(order.get("clientOrderId") or "") == client_oid
                for order in (response.get("data") or {}).get("list", [])
            )
        except Exception as exc:
            logger.debug("[Sniper] open-order confirmation failed: %s", exc)
            return None

    async def _wait_order_final(
        self,
        client_oid: str,
        asset_id: str,
        side: str,
        attempts: int = 10,
    ) -> tuple[Decimal, Decimal, bool]:
        """Wait until a submitted order is visible in OKX's closed-order view."""
        best_size = ZERO
        best_amount = ZERO
        if not client_oid:
            return best_size, best_amount, False
        for attempt in range(attempts):
            started_ns = time.perf_counter_ns()
            try:
                response = await self.client.get_orders(status="closed")
                response_code = str(response.get("code", ""))
                orders = (response.get("data") or {}).get("list", [])
                matched_order = None
                if response_code == "0":
                    for order in orders:
                        if str(order.get("clientOrderId") or "") != client_oid:
                            continue
                        if str(order.get("assetId") or "") != asset_id:
                            continue
                        if str(order.get("side") or "").upper() != side.upper():
                            continue
                        size = _decimal(order.get("filledSize"))
                        amount = _decimal(order.get("filledAmount"))
                        if size >= best_size:
                            best_size, best_amount = size, amount
                        matched_order = order
                        audit_event(
                            "sniper.order_final_poll",
                            account_id=self.account_id,
                            strategy="poly-sniper",
                            run_id=self._run_id,
                            client_order_id=client_oid,
                            asset_id=asset_id,
                            side=side.upper(),
                            attempt=attempt + 1,
                            attempts=attempts,
                            response_code=response_code,
                            order_count=len(orders),
                            matched=True,
                            order_status=order.get("status", order.get("state", "closed")),
                            filled_size=_decimal_text(best_size),
                            filled_amount=_decimal_text(best_amount),
                            duration_ms=self._elapsed_ms(started_ns),
                        )
                        return best_size, best_amount, True
                audit_event(
                    "sniper.order_final_poll",
                    level=(
                        logging.INFO if response_code == "0" else logging.WARNING
                    ),
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    client_order_id=client_oid,
                    asset_id=asset_id,
                    side=side.upper(),
                    attempt=attempt + 1,
                    attempts=attempts,
                    response_code=response_code,
                    response_message=response.get("msg", response.get("message", "")),
                    order_count=len(orders),
                    matched=matched_order is not None,
                    best_filled_size=_decimal_text(best_size),
                    best_filled_amount=_decimal_text(best_amount),
                    duration_ms=self._elapsed_ms(started_ns),
                )
            except Exception as exc:
                logger.warning(
                    "[Sniper:%s] final order poll %s/%s failed for %s: %s",
                    self.account_id,
                    attempt + 1,
                    attempts,
                    client_oid,
                    exc,
                )
                audit_event(
                    "sniper.order_final_poll_exception",
                    level=logging.WARNING,
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    client_order_id=client_oid,
                    asset_id=asset_id,
                    side=side.upper(),
                    attempt=attempt + 1,
                    attempts=attempts,
                    duration_ms=self._elapsed_ms(started_ns),
                    error=exc,
                )
            if attempt + 1 < attempts:
                await asyncio.sleep(0.3)
        audit_event(
            "sniper.order_final_unconfirmed",
            level=logging.ERROR,
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            client_order_id=client_oid,
            asset_id=asset_id,
            side=side.upper(),
            attempts=attempts,
            best_filled_size=_decimal_text(best_size),
            best_filled_amount=_decimal_text(best_amount),
        )
        return best_size, best_amount, False

    async def _holding_loop(self):
        try:
            stoploss_checked = False
            while self._running and self.state.status == "holding":
                elapsed = Decimal(str(time.monotonic() - self.state.hold_start))
                if elapsed >= ONE and not stoploss_checked:
                    stoploss_checked = True
                    target = self._active_target()
                    poly_bid = await self._get_poly_bid(target.poly_token_id) if target else None
                    audit_event(
                        "sniper.stoploss_evaluated",
                        account_id=self.account_id,
                        strategy="poly-sniper",
                        run_id=self._run_id,
                        target_id=target.id if target else "",
                        buy_price=_decimal_text(self.state.buy_price),
                        poly_bid=(
                            _decimal_text(poly_bid) if poly_bid is not None else None
                        ),
                        elapsed_ms=round(float(elapsed * Decimal("1000")), 1),
                        decision=(
                            "exit"
                            if poly_bid is not None and self.state.buy_price > poly_bid
                            else "hold"
                            if poly_bid is not None
                            else "unavailable"
                        ),
                    )
                    if poly_bid is not None and self.state.buy_price > poly_bid:
                        self.log(
                            f"Stoploss: buy={_decimal_text(self.state.buy_price)} > "
                            f"Poly bid={_decimal_text(poly_bid)}"
                        )
                        await self._exit_position("stoploss-1s")
                        return
                if elapsed >= self.config.timeout_secs:
                    await self._exit_position("timeout")
                    return
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            return

    async def _exit_position(self, reason: str):
        target = self._active_target()
        if target is None:
            await self._stop_with_exposure("Active target is missing")
            return
        self.state.status = "exiting"
        self.log(f"Exit started ({reason})")
        audit_event(
            "sniper.exit_started",
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            target_id=target.id,
            asset_id=target.asset_id,
            reason=reason,
            buy_size=_decimal_text(self.state.buy_size),
            sold_size=_decimal_text(self.state.sold_size),
            position_before=_decimal_text(self.state.position_before),
        )
        await self._emit_state_change()

        for attempt in range(5):
            remaining = self.state.buy_size - self.state.sold_size
            if remaining <= ZERO:
                if await self._confirm_exit(target):
                    await self._reset_to_monitoring()
                    return
                if not self._running:
                    return
                remaining = self.state.buy_size - self.state.sold_size
            bids = await self._snapshot_okx_bids(target)
            if not bids:
                await self._stop_with_exposure("OKX has no usable bid")
                return
            best_bid = _floor_to_tick(bids[0][0])
            try:
                client_oid = ""
                oid_factory = getattr(self.client, "generate_client_order_id", None)
                if callable(oid_factory):
                    generated_oid = oid_factory()
                    client_oid = str(generated_oid) if generated_oid else ""
                if not client_oid:
                    await self._stop_with_exposure(
                        "Exit blocked: clientOrderId could not be generated"
                    )
                    return
                self.state.exit_client_order_ids.append(client_oid)
                if not await self._emit_state_change():
                    self.state.exit_client_order_ids.remove(client_oid)
                    await self._stop_with_exposure(
                        "Exit blocked: clientOrderId snapshot could not be persisted"
                    )
                    return
                order_kwargs = dict(
                    asset_id=target.asset_id,
                    side="sell",
                    price=_okx_price_text(best_bid),
                    size=normalize_size_str(remaining),
                    tif="ioc",
                    reduce_only=True,
                )
                if client_oid:
                    order_kwargs["client_order_id"] = client_oid
                audit_event(
                    "sniper.exit_order_submitting",
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    target_id=target.id,
                    asset_id=target.asset_id,
                    reason=reason,
                    attempt=attempt + 1,
                    client_order_id=client_oid,
                    side="sell",
                    price=order_kwargs["price"],
                    size=order_kwargs["size"],
                    tif="ioc",
                    reduce_only=True,
                    remaining_before=_decimal_text(remaining),
                )
                order_started_ns = time.perf_counter_ns()
                response = await self.client.place_order(**order_kwargs)
                response_oid = str(response.get("_clientOrderId") or client_oid)
                audit_event(
                    "sniper.exit_order_response",
                    level=(
                        logging.INFO
                        if str(response.get("code", "")) == "0"
                        else logging.WARNING
                    ),
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    target_id=target.id,
                    asset_id=target.asset_id,
                    attempt=attempt + 1,
                    client_order_id=response_oid,
                    response_code=str(response.get("code", "")),
                    response_message=response.get("msg", response.get("message", "")),
                    duration_ms=self._elapsed_ms(order_started_ns),
                )
                if response_oid and response_oid not in self.state.exit_client_order_ids:
                    self.state.exit_client_order_ids.append(response_oid)
                    await self._emit_state_change()
                client_oid = response_oid
                if str(response.get("code", "")) != "0":
                    self.log(
                        f"Exit attempt {attempt + 1} rejected: {response.get('msg', '')}",
                        logging.WARNING,
                    )
                    if str(response.get("code", "")).upper() in {"TIMEOUT", "ERROR"}:
                        filled, _, final = await self._wait_order_final(
                            client_oid, target.asset_id, "SELL"
                        )
                        if not final:
                            await self._stop_with_exposure(
                                "Exit result is ambiguous; stopped for reconciliation"
                            )
                            return
                        if client_oid not in self.state.exit_resolved_client_order_ids:
                            self.state.exit_resolved_client_order_ids.append(client_oid)
                        self.state.sold_size = min(
                            self.state.buy_size,
                            self.state.sold_size + filled,
                        )
                        await self._emit_state_change()
                        continue
                    if client_oid and client_oid not in self.state.exit_resolved_client_order_ids:
                        self.state.exit_resolved_client_order_ids.append(client_oid)
                        await self._emit_state_change()
                    await asyncio.sleep(1)
                    continue
                filled, _, final = await self._wait_order_final(
                    client_oid, target.asset_id, "SELL"
                )
                if not final:
                    await self._stop_with_exposure(
                        "Exit order final state could not be confirmed"
                    )
                    return
                if client_oid not in self.state.exit_resolved_client_order_ids:
                    self.state.exit_resolved_client_order_ids.append(client_oid)
                # Every exit order has a unique clientOrderId, so adding each
                # order's final cumulative fill does not double count retries.
                self.state.sold_size = min(self.state.buy_size, self.state.sold_size + filled)
                self.log(
                    f"Exit attempt {attempt + 1}: filled={_decimal_text(filled)}, "
                    f"remaining={_decimal_text(self.state.buy_size - self.state.sold_size)}"
                )
                await self._emit_state_change()
                if self.state.sold_size >= self.state.buy_size:
                    if await self._confirm_exit(target):
                        await self._reset_to_monitoring()
                        return
                    if not self._running:
                        return
            except Exception as exc:
                self.log(
                    f"Exit attempt {attempt + 1} exception: {exc}",
                    logging.ERROR,
                )
                if not client_oid:
                    await self._stop_with_exposure(
                        "Exit placement failed without a trackable clientOrderId"
                    )
                    return
                filled, _, final = await self._wait_order_final(
                    client_oid, target.asset_id, "SELL"
                )
                if not final:
                    await self._stop_with_exposure(
                        "Exit placement result is ambiguous; stopped for reconciliation"
                    )
                    return
                if client_oid not in self.state.exit_resolved_client_order_ids:
                    self.state.exit_resolved_client_order_ids.append(client_oid)
                self.state.sold_size = min(
                    self.state.buy_size,
                    self.state.sold_size + filled,
                )
                await self._emit_state_change()
                if self.state.sold_size >= self.state.buy_size:
                    if await self._confirm_exit(target):
                        await self._reset_to_monitoring()
                        return
                    if not self._running:
                        return
            await asyncio.sleep(1)

        await self._stop_with_exposure("Exit retries exhausted; manual reconciliation required")

    async def _query_position_size(self, target: SniperTarget) -> Optional[Decimal]:
        started_ns = time.perf_counter_ns()
        try:
            response = await self.client.get_positions(status="open", market_id=target.market_id)
            response_code = str(response.get("code", ""))
            if response_code != "0":
                audit_event(
                    "sniper.position_snapshot",
                    level=logging.WARNING,
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    target_id=target.id,
                    market_id=target.market_id,
                    asset_id=target.asset_id,
                    response_code=response_code,
                    matched=False,
                    duration_ms=self._elapsed_ms(started_ns),
                )
                return None
            positions = (response.get("data") or {}).get("list", [])
            size = self._matching_position_size(positions, target)
            audit_event(
                "sniper.position_snapshot",
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                market_id=target.market_id,
                asset_id=target.asset_id,
                response_code=response_code,
                matching_size=_decimal_text(size),
                position_count=len(positions),
                duration_ms=self._elapsed_ms(started_ns),
            )
            return size
        except Exception as exc:
            logger.exception(
                "[Sniper:%s] position snapshot failed for %s",
                self.account_id,
                target.id,
            )
            audit_event(
                "sniper.position_exception",
                level=logging.ERROR,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                market_id=target.market_id,
                asset_id=target.asset_id,
                duration_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            return None

    async def _confirm_exit(self, target: SniperTarget) -> bool:
        """Confirm the position returned to its pre-entry size."""
        strategy_position = max(ZERO, self.state.buy_size - self.state.sold_size)
        for attempt in range(3):
            actual = await self._query_position_size(target)
            if actual is None:
                await self._stop_with_exposure("Exit fill reported but position reconciliation failed")
                return False
            strategy_position = max(ZERO, actual - self.state.position_before)
            audit_event(
                "sniper.exit_reconciliation",
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                asset_id=target.asset_id,
                attempt=attempt + 1,
                position_before=_decimal_text(self.state.position_before),
                actual_position=_decimal_text(actual),
                strategy_position=_decimal_text(strategy_position),
                buy_size=_decimal_text(self.state.buy_size),
                sold_size=_decimal_text(self.state.sold_size),
                decision="flat" if strategy_position <= ZERO else "retry",
            )
            if strategy_position <= ZERO:
                return True
            self.state.sold_size = max(
                ZERO, self.state.buy_size - min(self.state.buy_size, strategy_position)
            )
            await asyncio.sleep(0.5)
        self.log(
            f"Position reconciliation still shows {_decimal_text(strategy_position)} "
            "Sniper shares"
        )
        return False

    async def _snapshot_okx_bids(self, target: SniperTarget) -> list[tuple[Decimal, Decimal]]:
        started_ns = time.perf_counter_ns()
        try:
            response = await self.client.get_orderbook(target.yes_asset_id, depth=10)
            received_wall_ns = time.time_ns()
            received_monotonic_ns = time.monotonic_ns()
            response_code = str(response.get("code", ""))
            if response_code != "0":
                audit_event(
                    "sniper.exit_book_failed",
                    level=logging.WARNING,
                    account_id=self.account_id,
                    strategy="poly-sniper",
                    run_id=self._run_id,
                    target_id=target.id,
                    yes_asset_id=target.yes_asset_id,
                    side=target.side,
                    response_code=response_code,
                    response_message=response.get("msg", response.get("message", "")),
                    duration_ms=self._elapsed_ms(started_ns),
                )
                return []
            book = (response.get("data") or [{}])[0]
            if target.side == "no":
                rows = self._invert_bids_from_yes_book(book.get("asks", []))
            else:
                rows = []
                for row in book.get("bids", []) or []:
                    if len(row) >= 2:
                        price, size = _decimal(row[0]), _decimal(row[1])
                        if price > ZERO and size > ZERO:
                            rows.append((price, size))
                rows = sorted(rows, key=lambda row: row[0], reverse=True)
            market_event(
                "okx",
                "sniper.rest_book",
                component="poly-sniper",
                account_id=self.account_id,
                run_id=self._run_id,
                purpose="exit",
                target_id=target.id,
                target_side=target.side,
                market_id=target.market_id,
                yes_asset_id=target.yes_asset_id,
                selected_asset_id=target.asset_id,
                source_timestamp=book.get("ts", ""),
                received_wall_ns=received_wall_ns,
                received_monotonic_ns=received_monotonic_ns,
                duration_ms=self._elapsed_ms(started_ns),
                yes_bids=(book.get("bids") or [])[:10],
                yes_asks=(book.get("asks") or [])[:10],
                selected_bids=[
                    [_decimal_text(price), _decimal_text(size)] for price, size in rows[:10]
                ],
                derived_from_yes_book=target.side == "no",
            )
            return rows
        except Exception as exc:
            logger.exception(
                "[Sniper:%s] exit order-book snapshot failed for %s",
                self.account_id,
                target.id,
            )
            audit_event(
                "sniper.exit_book_exception",
                level=logging.ERROR,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                target_id=target.id,
                yes_asset_id=target.yes_asset_id,
                duration_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            self.log(f"OKX bid book exception: {exc}", logging.ERROR)
            return []

    async def _stop_with_exposure(self, reason: str):
        self.log(reason, logging.ERROR)
        self.state.status = "stopped"
        self._running = False
        await self._persist_critical_state(
            "Stopped exposure state could not be persisted; retry Stop before restarting"
        )

    async def _reset_to_monitoring(self):
        active = self.state.active_target_id
        self.state.status = "monitoring" if self._running else "stopped"
        self.state.active_target_id = ""
        self.state.locked = False
        self.state.buy_price = ZERO
        self.state.buy_size = ZERO
        self.state.sold_size = ZERO
        self.state.position_before = ZERO
        self.state.entry_client_order_id = ""
        self.state.entry_placement_attempted = False
        self.state.exit_client_order_ids = []
        self.state.exit_resolved_client_order_ids = []
        self.state.sell_order_id = ""
        self.state.hold_start = 0.0
        self.state.hold_start_epoch_ms = 0
        for runtime in self._runtime.values():
            runtime.price_window = []
            runtime.baseline = runtime.last_bid
            runtime.calibrated = runtime.last_bid > ZERO
        self.log(
            f"{active} fully exited; "
            + ("monitoring resumed" if self._running else "global lock cleared")
        )
        await self._emit_state_change()

    def _target(self, target_id: str) -> SniperTarget:
        for target in self.targets:
            if target.id == target_id:
                return target
        raise KeyError(target_id)

    def _active_target(self) -> Optional[SniperTarget]:
        try:
            return self._target(self.state.active_target_id) if self.state.active_target_id else None
        except KeyError:
            return None

    async def _get_poly_bid(self, token_id: str) -> Optional[Decimal]:
        started_ns = time.perf_counter_ns()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    POLY_CLOB_BOOK.format(token_id=token_id),
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as response:
                    received_wall_ns = time.time_ns()
                    received_monotonic_ns = time.monotonic_ns()
                    if response.status != 200:
                        audit_event(
                            "sniper.poly_stoploss_book_failed",
                            level=logging.WARNING,
                            account_id=self.account_id,
                            strategy="poly-sniper",
                            run_id=self._run_id,
                            poly_token_id=token_id,
                            http_status=response.status,
                            duration_ms=self._elapsed_ms(started_ns),
                        )
                        return None
                    book = await response.json()
                    received_wall_ns = time.time_ns()
                    received_monotonic_ns = time.monotonic_ns()
                    bids = book.get("bids", [])
                    best_bid = max(
                        (_decimal(row.get("price")) for row in bids),
                        default=ZERO,
                    ) or None
                    market_event(
                        "polymarket",
                        "sniper.stoploss_rest_book",
                        component="poly-sniper",
                        account_id=self.account_id,
                        run_id=self._run_id,
                        poly_token_id=token_id,
                        endpoint="/book",
                        source_timestamp=book.get("timestamp", book.get("ts", "")),
                        received_wall_ns=received_wall_ns,
                        received_monotonic_ns=received_monotonic_ns,
                        duration_ms=self._elapsed_ms(started_ns),
                        http_status=response.status,
                        bids=bids,
                        asks=book.get("asks") or [],
                        best_bid=(
                            _decimal_text(best_bid) if best_bid is not None else None
                        ),
                    )
                    return best_bid
        except Exception as exc:
            logger.warning(
                "[Sniper:%s] Poly stoploss book failed for %s: %s",
                self.account_id,
                token_id,
                exc,
            )
            audit_event(
                "sniper.poly_stoploss_book_exception",
                level=logging.WARNING,
                account_id=self.account_id,
                strategy="poly-sniper",
                run_id=self._run_id,
                poly_token_id=token_id,
                duration_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            return None

    async def _notify(self, message: str):
        if self.notifier:
            try:
                await self.notifier.send(f"[{self.account_name or self.account_id}] {message}")
            except Exception as exc:
                logger.warning("[Sniper] Notify failed: %s", exc)

    async def _emit_state_change(self) -> bool:
        """Publish a material state transition to the application's store."""
        callback = self.on_state_change
        if callback is None:
            return True
        try:
            result = callback(self.get_state())
            if inspect.isawaitable(result):
                result = await result
            return result is not False
        except Exception as exc:
            logger.error("[Sniper:%s] state persistence hook failed: %s", self.account_id, exc)
            return False

    def get_config(self) -> dict:
        """Return the JSON-safe configuration persisted by the application."""
        config = self.config.to_dict()
        config["poly_url"] = self.targets[0].poly_url if self.targets else ""
        return {
            "config": config,
            "targets": [target.to_dict() for target in self.targets],
        }

    def get_state(self) -> dict:
        """Return configuration plus JSON-safe runtime state for page refreshes."""
        active = self._active_target()
        remaining = max(ZERO, self.state.buy_size - self.state.sold_size)
        return {
            "running": self._running,
            "status": self.state.status,
            "locked": self.state.locked,
            "persistence_error": self._persistence_failed,
            "needs_reconciliation": bool(
                self.state.locked or self._persistence_failed
            ),
            "active_target_id": self.state.active_target_id,
            "active_target": active.to_dict() if active else None,
            **self.get_config(),
            "holding": {
                "target_id": self.state.active_target_id,
                "buy_price": _decimal_text(self.state.buy_price),
                "buy_size": _decimal_text(self.state.buy_size),
                "sold_size": _decimal_text(self.state.sold_size),
                "remaining_size": _decimal_text(remaining),
                "position_before": _decimal_text(self.state.position_before),
                "entry_client_order_id": self.state.entry_client_order_id,
                "entry_placement_attempted": self.state.entry_placement_attempted,
                "exit_client_order_ids": list(self.state.exit_client_order_ids),
                "exit_resolved_client_order_ids": list(
                    self.state.exit_resolved_client_order_ids
                ),
                "hold_start_epoch_ms": self.state.hold_start_epoch_ms,
                "elapsed": int(time.monotonic() - self.state.hold_start) if self.state.hold_start else 0,
            } if self.state.locked else None,
            "target_prices": {
                target.id: _decimal_text(self._runtime.get(target.id, TargetRuntime()).last_bid)
                for target in self.targets
            },
            "last_yes_bid": _decimal_text(self.state.last_yes_bid),
            "last_no_bid": _decimal_text(self.state.last_no_bid),
            "yes_token_id": self.state.yes_token_id,
            "no_token_id": self.state.no_token_id,
            "yes_asset_id": self.state.yes_asset_id,
            "no_asset_id": self.state.no_asset_id,
            "market_id": self.state.market_id,
            "poly_url": self.state.poly_url,
            "selected_outcome": self.state.selected_outcome,
            "latency": self._latency_snapshot(),
            "logs": self.state.logs[-20:],
        }

    async def restore(self, saved_state: dict, resume: bool = False) -> dict:
        """Restore state and reconcile tracked orders/positions with OKX.

        ``resume`` controls whether monitoring or holding tasks restart after
        reconciliation. Any order whose final state cannot be confirmed keeps
        the engine stopped and globally locked.
        """
        config = saved_state.get("config") or {}
        self._run_id = secrets.token_hex(8)
        self._poly_frame_sequence = 0
        raw_targets = saved_state.get("targets") or []
        try:
            parsed = [SniperTarget.from_dict(item, index) for index, item in enumerate(raw_targets)]
            self._validate_targets(parsed)
        except (TypeError, ValueError) as exc:
            return {"ok": False, "error": str(exc)}

        self.targets = parsed
        try:
            self.config = SniperConfig(
                threshold_cents=_decimal(config.get("threshold_cents", 40)),
                buy_limit_cents=_decimal(config.get("buy_limit_cents", 50)),
                position_pct=_decimal(config.get("position_pct", 100)),
                timeout_secs=_decimal(config.get("timeout_secs", 3)),
                settle_ms=max(0, int(config.get("settle_ms", 0))),
                window_ms=max(1, int(config.get("window_ms", 150))),
            )
            self._validate_config()
        except (TypeError, ValueError) as exc:
            return {"ok": False, "error": str(exc)}
        restored_poly_url = str(config.get("poly_url") or "")
        if restored_poly_url:
            for target in parsed:
                if not target.poly_url:
                    target.poly_url = restored_poly_url
        self._runtime = {target.id: TargetRuntime() for target in parsed}
        self._token_targets = {}
        for target in parsed:
            self._token_targets.setdefault(target.poly_token_id, []).append(target.id)

        holding = saved_state.get("holding") or {}
        locked = bool(saved_state.get("locked"))
        self.state = SniperState(
            status="stopped",
            active_target_id=str(saved_state.get("active_target_id") or ""),
            locked=locked,
            buy_price=_decimal(holding.get("buy_price")),
            buy_size=_decimal(holding.get("buy_size")),
            sold_size=_decimal(holding.get("sold_size")),
            position_before=_decimal(holding.get("position_before")),
            entry_client_order_id=str(holding.get("entry_client_order_id") or ""),
            entry_placement_attempted=bool(holding.get("entry_placement_attempted")),
            exit_client_order_ids=list(holding.get("exit_client_order_ids") or []),
            exit_resolved_client_order_ids=list(
                holding.get("exit_resolved_client_order_ids") or []
            ),
            hold_start_epoch_ms=int(holding.get("hold_start_epoch_ms") or 0),
            logs=list(saved_state.get("logs") or [])[-100:],
        )
        self._latency_history = self._restore_latency_history(
            saved_state.get("latency")
        )
        self._latency_active = None
        audit_event(
            "strategy.restore_started",
            account_id=self.account_id,
            strategy="poly-sniper",
            run_id=self._run_id,
            resume=resume,
            saved_status=saved_state.get("status", ""),
            saved_locked=bool(saved_state.get("locked")),
        )
        self._set_legacy_fields(parsed[0])
        prices = saved_state.get("target_prices") or {}
        for target in parsed:
            self._runtime[target.id].last_bid = _decimal(prices.get(target.id))

        if (locked and not self.state.entry_placement_attempted
                and not self.state.entry_client_order_id
                and not (
                    set(self.state.exit_client_order_ids)
                    - set(self.state.exit_resolved_client_order_ids)
                )
                and self.state.buy_size <= self.state.sold_size):
            self.state.locked = False
            self.state.active_target_id = ""
            self.log("Restored pre-entry lock with no submitted order; lock cleared")
            if resume:
                return await self.start(
                    targets=[target.to_dict() for target in parsed],
                    restoring=True,
                    **self.config.to_dict(),
                )
            await self._emit_state_change()
            return {"ok": True, "resumed": False, "needs_reconciliation": False}
        if locked or self.state.buy_size > self.state.sold_size:
            self.state.locked = True
            self.log("Restored tracked exposure; reconciling with OKX")
            return await self._restore_active_trade(resume=resume)
        if resume:
            return await self.start(targets=[target.to_dict() for target in parsed], restoring=True, **self.config.to_dict())
        await self._emit_state_change()
        return {"ok": True, "resumed": False, "needs_reconciliation": False}

    async def _restore_active_trade(self, resume: bool) -> dict:
        """Reconcile one persisted active target before allowing new entries."""
        target = self._active_target()
        if target is None:
            self.state.status = "error"
            self.state.locked = True
            self.log("Restore blocked: active target is missing", logging.ERROR)
            await self._emit_state_change()
            return {"ok": False, "resumed": False, "needs_reconciliation": True,
                    "error": "Active target is missing"}

        try:
            open_response = await self.client.get_orders(status="open", market_id=target.market_id)
            if str(open_response.get("code", "")) != "0":
                raise RuntimeError(open_response.get("msg") or "open-order query failed")
            open_orders = (open_response.get("data") or {}).get("list", [])
            open_client_order_ids = {
                str(order.get("clientOrderId") or "")
                for order in open_orders
                if order.get("clientOrderId")
            }

            if self.state.entry_placement_attempted or self.state.entry_client_order_id:
                if not self.state.entry_client_order_id:
                    raise RuntimeError(
                        "entry placement was attempted without a persisted clientOrderId"
                    )
                if self.state.entry_client_order_id in open_client_order_ids:
                    await self._cancel_strategy_order(
                        target.asset_id,
                        self.state.entry_client_order_id,
                    )
                final_size, final_amount, entry_final = await self._wait_order_final(
                    self.state.entry_client_order_id,
                    target.asset_id,
                    "BUY",
                )
                if not entry_final:
                    raise RuntimeError(
                        "entry order final state could not be confirmed during restore"
                    )
                if final_size > self.state.buy_size:
                    self.state.buy_size = final_size
                    if final_amount > ZERO:
                        self.state.buy_price = final_amount / final_size

            resolved_exit_ids = set(self.state.exit_resolved_client_order_ids)
            for exit_client_oid in dict.fromkeys(self.state.exit_client_order_ids):
                if not exit_client_oid or exit_client_oid in resolved_exit_ids:
                    continue
                if exit_client_oid in open_client_order_ids:
                    await self._cancel_strategy_order(target.asset_id, exit_client_oid)
                _, _, exit_final = await self._wait_order_final(
                    exit_client_oid,
                    target.asset_id,
                    "SELL",
                )
                if not exit_final:
                    raise RuntimeError(
                        f"exit order {exit_client_oid} final state could not be confirmed during restore"
                    )
                self.state.exit_resolved_client_order_ids.append(exit_client_oid)
                resolved_exit_ids.add(exit_client_oid)

            position_response = await self.client.get_positions(status="open", market_id=target.market_id)
            if str(position_response.get("code", "")) != "0":
                raise RuntimeError(position_response.get("msg") or "position query failed")
            actual_position = self._matching_position_size(
                (position_response.get("data") or {}).get("list", []), target
            )
        except Exception as exc:
            self.state.status = "error"
            self.state.locked = True
            self.log(f"Restore reconciliation failed: {exc}", logging.ERROR)
            await self._emit_state_change()
            return {"ok": False, "resumed": False, "needs_reconciliation": True,
                    "error": str(exc)}

        tracked_remaining = max(ZERO, self.state.buy_size - self.state.sold_size)
        strategy_position = max(ZERO, actual_position - self.state.position_before)
        if strategy_position <= ZERO:
            self.state.buy_size = self.state.sold_size
            self._running = resume
            await self._reset_to_monitoring()
            if resume:
                self._task = self._spawn_task(self._monitor_loop(), "poly-feed")
                self._okx_trace_task = self._spawn_okx_trace_task()
            self.log("Restore reconciliation found no remaining target position")
            return {"ok": True, "resumed": resume, "needs_reconciliation": False}

        if tracked_remaining <= ZERO:
            self.state.status = "error"
            self.state.locked = True
            self.log(
                "Restore blocked: OKX position exists but no strategy quantity is attributable",
                logging.ERROR,
            )
            await self._emit_state_change()
            return {"ok": False, "resumed": False, "needs_reconciliation": True,
                    "error": "Unattributed OKX position exists for active target"}

        # Do not claim or close any pre-existing quantity above this strategy's
        # persisted ledger. Conversely, a smaller live position proves that at
        # least part of the persisted quantity has already been sold.
        strategy_remaining = min(tracked_remaining, strategy_position)
        self.state.buy_size = self.state.sold_size + strategy_remaining
        self.state.status = "holding" if resume else "stopped"
        self.state.locked = True
        if resume:
            self._running = True
            elapsed = max(0.0, time.time() - self.state.hold_start_epoch_ms / 1000.0)
            self.state.hold_start = time.monotonic() - elapsed
            self._task = self._spawn_task(self._monitor_loop(), "poly-feed")
            self._okx_trace_task = self._spawn_okx_trace_task()
            self._hold_task = self._spawn_task(self._holding_loop(), "holding")
            self.log(f"Restored locked trade with {_decimal_text(strategy_remaining)} shares remaining")
        else:
            self.log(f"Restored stopped trade with {_decimal_text(strategy_remaining)} shares remaining")
        await self._emit_state_change()
        return {
            "ok": True,
            "resumed": resume,
            "needs_reconciliation": not resume,
            "remaining_size": _decimal_text(strategy_remaining),
        }

    @staticmethod
    def _matching_position_size(positions: Iterable[dict], target: SniperTarget) -> Decimal:
        total = ZERO
        expected_index = "1" if target.side == "yes" else "2"
        expected_name = target.side.upper()
        for position in positions or []:
            if str(position.get("marketId") or "") != target.market_id:
                continue
            token_index = str(position.get("tokenIndex") or "")
            token_name = str(position.get("tokenName") or "").upper()
            if token_index == expected_index or token_name == expected_name:
                total += max(ZERO, _decimal(position.get("size")))
        return total
