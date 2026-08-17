"""Account manager: manages OKX client instances and strategies per account."""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Optional

from ..core.client import OKXOutcomesClient
from ..core.logging_config import audit_event as _emit_audit_event, log_context
from ..core.price_utils import align_okx_price, as_decimal, price_to_cents
from ..core.database import (
    get_accounts, get_account_decrypted, create_account, update_account,
    delete_account, cleanup_account_state_files, get_available_proxies,
    get_used_proxies, PROXY_NODES,
    get_global_config, set_global_config, get_sniper_state, save_sniper_state,
    get_sniper_old_state, save_sniper_old_state
)
from .manual_strategy_d import ManualStrategyD
from .manual_strategy_s import ManualStrategyS
from .poly_monitor import PolyMonitor
from .auto_rebalance import AutoRebalance
from .poly_sniper import PolySniper
from .poly_sniper_old import PolySniperOld
from .notifier import TelegramNotifier

logger = logging.getLogger(__name__)

ACCOUNT_HEARTBEAT_INTERVAL_SECONDS = 120.0
_POSITION_EPSILON = Decimal("0.000000001")


def audit_event(event: str, *, level: int = logging.INFO, **fields) -> None:
    """Emit account audit data without letting logging affect lifecycle work."""
    try:
        _emit_audit_event(event, level=level, **fields)
    except Exception:
        logger.exception("Account audit logging failed for event %s", event)


def _elapsed_ms(started_ns: int) -> float:
    return round((time.perf_counter_ns() - started_ns) / 1_000_000, 3)


class AccountInstance:
    """A live account instance with its own client and strategies."""

    def __init__(self, account_id: str, name: str, client: OKXOutcomesClient,
                 proxy_node: str, notifier: Optional[TelegramNotifier] = None):
        self.account_id = account_id
        self.name = name
        self.client = client
        self.client._account_id = account_id
        self.proxy_node = proxy_node
        self.strategy_lifecycle_lock = asyncio.Lock()
        self.recovery_locked = False
        self.recovery_reason = ""
        self.recovery_details: dict = {}
        self.last_heartbeat_ok: Optional[float] = None
        self.last_reconciled_at: Optional[float] = None
        self._account_heartbeat_task: Optional[asyncio.Task] = None
        self._account_heartbeat_interval = ACCOUNT_HEARTBEAT_INTERVAL_SECONDS
        self._account_heartbeat_retry_delay = 2.0
        self._recovery_lock = asyncio.Lock()
        self.strategy_d = ManualStrategyD(client, account_id=account_id)
        self.strategy_s = ManualStrategyS(client, account_id=account_id)
        self.poly_monitor = PolyMonitor(client, account_id=account_id)
        self.poly_monitor.account_name = name
        self.strategy_d.account_name = name
        self.strategy_s.account_name = name
        self.poly_monitor.on_trigger_callback = self._stop_all_strategies
        # Auto-rebalance
        self.auto_rebalance = AutoRebalance(client, account_id=account_id, account_name=name)
        self.poly_monitor.auto_rebalance = self.auto_rebalance
        self.auto_rebalance.get_strategy_params = self._get_strategy_params
        self.auto_rebalance.restart_strategy = self._restart_strategy
        self.auto_rebalance.restart_monitor = self._restart_monitor
        self.auto_rebalance.stop_strategy = self._stop_all_strategies
        self.auto_rebalance.stop_monitor = self._stop_monitor_for_rebalance
        self.strategy_s.auto_rebalance = self.auto_rebalance
        # Sniper
        self.sniper = PolySniper(client, account_id=account_id)
        self.sniper.account_name = name
        self.sniper.on_state_change = self._persist_sniper_snapshot
        self._sniper_snapshot_corrupt = False
        self._sniper_snapshot_write_blocked = False
        self.sniper_old = PolySniperOld(client, account_id=account_id)
        self.sniper_old.account_name = name
        self.sniper_old.on_state_change = self._persist_sniper_old_snapshot
        self._sniper_old_snapshot_corrupt = False
        self._sniper_old_snapshot_write_blocked = False
        self.strategy_d.auto_rebalance = self.auto_rebalance
        if notifier:
            self.strategy_d.notifier = notifier
            self.strategy_s.notifier = notifier
            self.poly_monitor.notifier = notifier
            self.auto_rebalance.notifier = notifier
            self.sniper.notifier = notifier
            self.sniper_old.notifier = notifier
        audit_event(
            "account.instance_created",
            account_id=self.account_id,
            proxy_node=self.proxy_node,
            base_url=getattr(self.client, "base_url", ""),
            notifier_enabled=bool(notifier),
        )

    @property
    def account_heartbeat_running(self) -> bool:
        task = getattr(self, "_account_heartbeat_task", None)
        return bool(task is not None and not task.done())

    def recovery_status(self) -> dict:
        """Return the account-wide entry gate and reconciliation health."""
        rebalance = getattr(self, "auto_rebalance", None)
        rebalance_provider = getattr(rebalance, "recovery_status", None)
        try:
            rebalance_recovery = (
                rebalance_provider() if callable(rebalance_provider) else {}
            )
        except Exception as exc:
            rebalance_recovery = {
                "required": True,
                "stage": "status_read_failed",
                "error": str(exc),
            }
        return {
            "locked": bool(getattr(self, "recovery_locked", False)),
            "reason": str(getattr(self, "recovery_reason", "") or ""),
            "details": dict(getattr(self, "recovery_details", {}) or {}),
            "heartbeat_running": self.account_heartbeat_running,
            "last_heartbeat_ok": getattr(self, "last_heartbeat_ok", None),
            "last_reconciled_at": getattr(self, "last_reconciled_at", None),
            "auto_rebalance": rebalance_recovery,
        }

    def start_account_heartbeat(self) -> Optional[asyncio.Task]:
        """Start one account-level read-only safety heartbeat task."""
        current = getattr(self, "_account_heartbeat_task", None)
        if current is not None and not current.done():
            return current
        with log_context(account_id=self.account_id, strategy="account-safety"):
            task = asyncio.create_task(
                self._account_heartbeat_loop(),
                name=f"account-heartbeat:{self.account_id}",
            )
        self._account_heartbeat_task = task
        audit_event(
            "account.heartbeat_started",
            account_id=self.account_id,
            interval_seconds=self._account_heartbeat_interval,
            mode="read_only_account_probe",
        )
        return task

    async def stop_account_heartbeat(self) -> None:
        """Stop and await the account-level read-only safety poll."""
        task = getattr(self, "_account_heartbeat_task", None)
        if task is None:
            return
        current = asyncio.current_task()
        if task is not current and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if getattr(self, "_account_heartbeat_task", None) is task and task is not current:
            self._account_heartbeat_task = None
        audit_event("account.heartbeat_stopped", account_id=self.account_id)

    async def _account_read_safety_probe(self) -> dict:
        """Probe authenticated account state without arming OKX cancel-all.

        The documented prediction heartbeat stores a cancelAll signature and
        removes every order after expiry, including protective SELL orders.
        That behavior is incompatible with strategies that intentionally keep
        protection on an open position, so the service uses read-only account
        queries as its liveness/reconciliation heartbeat instead.
        """
        last_error = None
        for attempt in range(3):
            try:
                orders = self._verified_response_list(
                    await self.client.get_orders(status="open"), "open orders"
                )
                positions = self._verified_response_list(
                    await self.client.get_positions(status="open"), "open positions"
                )
                return {
                    "code": "0",
                    "open_order_count": len(orders),
                    "position_count": len(positions),
                    "mode": "read_only_account_probe",
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(
                        max(
                            0.0,
                            float(
                                getattr(self, "_account_heartbeat_retry_delay", 2.0)
                            ),
                        )
                    )
        raise RuntimeError(str(last_error or "account safety probe failed"))

    async def _account_heartbeat_loop(self) -> None:
        try:
            while True:
                started_ns = time.perf_counter_ns()
                failure = ""
                response_code = ""
                try:
                    response = await self._account_read_safety_probe()
                    response_code = (
                        str(response.get("code", ""))
                        if isinstance(response, dict)
                        else "INVALID_RESPONSE"
                    )
                    if response_code != "0":
                        if isinstance(response, dict):
                            failure = str(
                                response.get("message")
                                or response.get("msg")
                                or response_code
                            )
                        else:
                            failure = "invalid heartbeat response"
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    response_code = "EXCEPTION"
                    failure = f"{type(exc).__name__}: {exc}"

                if failure:
                    self._activate_recovery_lock(
                        "heartbeat_failed",
                        {
                            "response_code": response_code,
                            "message": failure,
                            "elapsed_ms": self._elapsed_ms(started_ns),
                        },
                    )
                    audit_event(
                        "account.heartbeat_failed",
                        level=logging.ERROR,
                        account_id=self.account_id,
                        response_code=response_code,
                        message=failure,
                        elapsed_ms=self._elapsed_ms(started_ns),
                    )
                    await self._stop_entry_activity_for_recovery(
                        "account heartbeat could not be renewed"
                    )
                else:
                    self.last_heartbeat_ok = time.time()
                    audit_event(
                        "account.heartbeat_ok",
                        account_id=self.account_id,
                        mode="read_only_account_probe",
                        open_order_count=response.get("open_order_count", 0),
                        position_count=response.get("position_count", 0),
                        elapsed_ms=self._elapsed_ms(started_ns),
                    )
                    if self.recovery_locked:
                        await self.reconcile_account_safety()

                await asyncio.sleep(self._account_heartbeat_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._activate_recovery_lock(
                "heartbeat_task_failed",
                {"message": f"{type(exc).__name__}: {exc}"},
            )
            audit_event(
                "account.heartbeat_task_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                error=exc,
            )
            await self._stop_entry_activity_for_recovery(
                "account heartbeat task terminated unexpectedly"
            )
        finally:
            if getattr(self, "_account_heartbeat_task", None) is asyncio.current_task():
                self._account_heartbeat_task = None

    def _activate_recovery_lock(self, reason: str, details: Optional[dict] = None) -> None:
        previous_reason = str(getattr(self, "recovery_reason", "") or "")
        was_locked = bool(getattr(self, "recovery_locked", False))
        self.recovery_locked = True
        self.recovery_reason = str(reason or "account_safety_unresolved")
        self.recovery_details = dict(details or {})
        audit_event(
            "account.recovery_locked",
            level=logging.ERROR,
            account_id=self.account_id,
            reason=self.recovery_reason,
            previous_reason=previous_reason,
            already_locked=was_locked,
            details=self.recovery_details,
        )

    def _clear_recovery_lock(self) -> None:
        if not getattr(self, "recovery_locked", False):
            return
        previous_reason = self.recovery_reason
        self.recovery_locked = False
        self.recovery_reason = ""
        self.recovery_details = {}
        audit_event(
            "account.recovery_cleared",
            account_id=self.account_id,
            previous_reason=previous_reason,
        )

    async def _stop_entry_activity_for_recovery(self, reason: str) -> None:
        """Stop every local component that could create new exchange exposure."""
        audit_event(
            "account.recovery_stop_started",
            level=logging.WARNING,
            account_id=self.account_id,
            reason=reason,
        )
        failures = []

        async def stop_component(name: str, operation) -> None:
            try:
                await operation()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures.append({"component": name, "error": str(exc)})
                audit_event(
                    "account.recovery_stop_component_failed",
                    level=logging.ERROR,
                    account_id=self.account_id,
                    component=name,
                    error=exc,
                )

        auto_rebalance = getattr(self, "auto_rebalance", None)
        if auto_rebalance is not None:
            await stop_component("auto-rebalance", auto_rebalance.cancel)
        for name, strategy in (
            ("manual-d", getattr(self, "strategy_d", None)),
            ("manual-s", getattr(self, "strategy_s", None)),
        ):
            if strategy is not None and bool(getattr(strategy, "running", False)):
                await stop_component(name, strategy.stop)
        sniper = getattr(self, "sniper", None)
        if sniper is not None and bool(
            getattr(sniper, "running", False) or getattr(sniper, "starting", False)
        ):
            await stop_component("poly-sniper", sniper.stop)
        sniper_old = getattr(self, "sniper_old", None)
        if sniper_old is not None and bool(getattr(sniper_old, "running", False)):
            await stop_component("poly-sniper-old", sniper_old.stop)
        monitor = getattr(self, "poly_monitor", None)
        monitor_enabled = bool(
            getattr(getattr(monitor, "state", None), "enabled", False)
        )
        if monitor is not None and bool(getattr(monitor, "running", False) or monitor_enabled):
            await stop_component("poly-monitor", monitor.stop)

        audit_event(
            "account.recovery_stop_completed",
            level=logging.ERROR if failures else logging.WARNING,
            account_id=self.account_id,
            reason=reason,
            failures=failures,
        )

    @staticmethod
    def _verified_response_list(response: dict, label: str) -> list[dict]:
        if not isinstance(response, dict) or str(response.get("code", "")) != "0":
            message = (
                response.get("message", response.get("msg", response.get("code", "unknown")))
                if isinstance(response, dict)
                else "invalid response"
            )
            raise RuntimeError(f"{label} query failed: {message}")
        data = response.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("list"), list):
            raise RuntimeError(f"{label} query returned invalid data")
        return data["list"]

    @staticmethod
    def _position_outcome(position: dict) -> str:
        token_index = str(position.get("tokenIndex", "") or "").strip()
        token_name = str(position.get("tokenName", "") or "").strip().upper()
        if not token_index or not token_name:
            raise ValueError("position outcome requires tokenIndex and tokenName")
        if token_index not in {"1", "2"}:
            raise ValueError(f"unknown position tokenIndex: {token_index}")
        if token_name not in {"YES", "NO"}:
            raise ValueError(f"unknown position tokenName: {token_name}")
        expected_name = "YES" if token_index == "1" else "NO"
        if token_name != expected_name:
            raise ValueError(
                f"conflicting position outcome: tokenIndex={token_index}, tokenName={token_name}"
            )
        return expected_name

    @classmethod
    def _position_asset_id(cls, position: dict) -> str:
        asset_id = str(position.get("assetId") or "")
        if asset_id:
            if position.get("tokenIndex") not in (None, "") or position.get(
                "tokenName"
            ) not in (None, ""):
                cls._position_outcome(position)
            return asset_id
        market_id = str(position.get("marketId") or "")
        if not market_id:
            raise ValueError("position assetId or marketId is required")
        outcome = cls._position_outcome(position)
        return f"{market_id}{'100' if outcome == 'NO' else '000'}"

    def _managed_sniper_snapshot(self) -> dict:
        sniper = getattr(self, "sniper", None)
        state = getattr(sniper, "state", None)
        if sniper is None or state is None:
            return {"asset_id": "", "remaining": Decimal("0"), "client_order_ids": set()}
        # A restored Sniper is deliberately reconciled with resume=False before
        # account-wide safety checks.  Its exact ledger remains managed while
        # stopped/locked; requiring a running task here would misclassify the
        # very exposure that must be audited before tasks are allowed to resume.
        if not bool(getattr(state, "locked", False)):
            return {"asset_id": "", "remaining": Decimal("0"), "client_order_ids": set()}
        active_target_id = str(getattr(state, "active_target_id", "") or "")
        target = next(
            (
                item
                for item in getattr(sniper, "targets", []) or []
                if str(getattr(item, "id", "") or "") == active_target_id
            ),
            None,
        )
        if target is None:
            return {"asset_id": "", "remaining": Decimal("0"), "client_order_ids": set()}
        position_before = as_decimal(getattr(state, "position_before", 0))
        remaining = max(
            Decimal("0"),
            as_decimal(getattr(state, "buy_size", 0))
            - as_decimal(getattr(state, "sold_size", 0)),
        )
        if position_before > _POSITION_EPSILON:
            # Sniper explicitly excludes this baseline quantity from its exit ledger.
            remaining = Decimal("0")
        client_order_ids = {
            str(value)
            for value in (
                [getattr(state, "entry_client_order_id", "")]
                + list(getattr(state, "exit_client_order_ids", []) or [])
            )
            if value
        }
        return {
            "asset_id": str(getattr(target, "asset_id", "") or ""),
            "remaining": remaining,
            "client_order_ids": client_order_ids,
        }

    def _managed_sniper_old_snapshot(self) -> dict:
        sniper = getattr(self, "sniper_old", None)
        state = getattr(sniper, "state", None)
        if sniper is None or state is None or not bool(getattr(state, "locked", False)):
            return {"asset_id": "", "remaining": Decimal("0"), "client_order_ids": set()}
        side = str(getattr(state, "holding_side", "") or "").lower()
        asset_id = (
            str(getattr(state, "yes_asset_id", "") or "")
            if side == "yes"
            else str(getattr(state, "no_asset_id", "") or "") if side == "no" else ""
        )
        remaining = max(
            Decimal("0"),
            as_decimal(getattr(state, "buy_size", 0))
            - as_decimal(getattr(state, "sold_size", 0)),
        )
        client_order_ids = {
            str(value)
            for value in (
                [
                    getattr(state, "buy_client_order_id", ""),
                    getattr(state, "sell_client_order_id", ""),
                ]
                + list(getattr(state, "exit_client_order_ids", []) or [])
            )
            if value
        }
        return {
            "asset_id": asset_id,
            "remaining": remaining,
            "client_order_ids": client_order_ids,
        }

    def _classify_exchange_state(
        self, open_orders: list[dict], positions: list[dict]
    ) -> dict:
        managed = self._managed_sniper_snapshot()
        managed_old = self._managed_sniper_old_snapshot()
        managed_ledger_conflict = bool(
            as_decimal(managed["remaining"]) > _POSITION_EPSILON
            and as_decimal(managed_old["remaining"]) > _POSITION_EPSILON
        )
        managed_client_ids = (
            managed["client_order_ids"] | managed_old["client_order_ids"]
        )
        unmanaged_orders = []
        for order in open_orders:
            client_order_id = str(order.get("clientOrderId") or "")
            if not client_order_id or client_order_id not in managed_client_ids:
                unmanaged_orders.append(order)

        nonzero_positions = []
        invalid_positions = []
        for position in positions:
            try:
                asset_id = self._position_asset_id(position)
            except (AttributeError, TypeError, ValueError) as exc:
                invalid_positions.append(
                    {
                        "asset_id": str(position.get("assetId") or ""),
                        "error": str(exc),
                    }
                )
                continue
            try:
                size = as_decimal(
                    position.get("size", position.get("availableSize", "0"))
                )
            except (TypeError, ValueError) as exc:
                invalid_positions.append(
                    {"asset_id": asset_id, "error": str(exc)}
                )
                continue
            if abs(size) > _POSITION_EPSILON:
                nonzero_positions.append((asset_id, abs(size)))

        active_managed = (
            managed_old
            if as_decimal(managed_old["remaining"]) > _POSITION_EPSILON
            else managed
        )
        allowances = (
            {
                str(active_managed["asset_id"]): as_decimal(
                    active_managed["remaining"]
                )
            }
            if not managed_ledger_conflict
            else {}
        )
        unmanaged_positions = []
        managed_position_size = Decimal("0")
        managed_old_position_size = Decimal("0")
        for asset_id, size in nonzero_positions:
            allowance = allowances.get(asset_id, Decimal("0"))
            if asset_id and allowance > _POSITION_EPSILON:
                covered = min(size, allowance)
                allowances[asset_id] = allowance - covered
                if active_managed is managed_old:
                    managed_old_position_size += covered
                else:
                    managed_position_size += covered
                extra = size - covered
                if extra > _POSITION_EPSILON:
                    unmanaged_positions.append(
                        {"asset_id": asset_id, "size": format(extra, "f")}
                    )
            else:
                unmanaged_positions.append(
                    {"asset_id": asset_id, "size": format(size, "f")}
                )

        return {
            "open_order_count": len(open_orders),
            "position_count": len(nonzero_positions),
            "unmanaged_order_count": len(unmanaged_orders),
            "unmanaged_position_count": len(unmanaged_positions),
            "unmanaged_order_ids": [
                str(order.get("oid", order.get("id", "")) or "")
                for order in unmanaged_orders[:20]
            ],
            "unmanaged_positions": unmanaged_positions[:20],
            "invalid_positions": invalid_positions[:20],
            "managed_sniper_position_size": format(managed_position_size, "f"),
            "managed_sniper_old_position_size": format(
                managed_old_position_size, "f"
            ),
            "managed_ledger_conflict": managed_ledger_conflict,
            "flat": not open_orders and not nonzero_positions and not invalid_positions,
            "unsafe": bool(
                unmanaged_orders
                or unmanaged_positions
                or invalid_positions
                or managed_ledger_conflict
            ),
        }

    async def reconcile_account_safety(self, *, startup: bool = False) -> dict:
        """Reconcile account-wide exchange exposure and maintain the entry lock."""
        lock = getattr(self, "_recovery_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._recovery_lock = lock
        async with lock:
            started_ns = time.perf_counter_ns()
            audit_event(
                "account.recovery_reconcile_started",
                account_id=self.account_id,
                startup=startup,
                locked=bool(self.recovery_locked),
            )
            try:
                order_response, position_response = await asyncio.gather(
                    self.client.get_orders(status="open"),
                    self.client.get_positions(status="open"),
                )
                open_orders = self._verified_response_list(order_response, "open orders")
                positions = self._verified_response_list(position_response, "open positions")
                classification = self._classify_exchange_state(open_orders, positions)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_reconciled_at = time.time()
                reason = "startup_safety_query_failed" if startup else "safety_query_failed"
                self._activate_recovery_lock(
                    reason,
                    {
                        "message": f"{type(exc).__name__}: {exc}",
                        "startup": startup,
                    },
                )
                await self._stop_entry_activity_for_recovery(
                    "account exposure could not be verified"
                )
                result = {
                    **self.recovery_status(),
                    "ok": False,
                    "flat": False,
                    "elapsed_ms": self._elapsed_ms(started_ns),
                }
                audit_event(
                    "account.recovery_reconcile_failed",
                    level=logging.ERROR,
                    account_id=self.account_id,
                    startup=startup,
                    error=exc,
                    elapsed_ms=result["elapsed_ms"],
                )
                return result

            self.last_reconciled_at = time.time()
            rebalance_recovery = self.recovery_status().get("auto_rebalance", {})
            rebalance_recovery_required = bool(
                isinstance(rebalance_recovery, dict)
                and rebalance_recovery.get("required")
            )
            sniper_old_state = getattr(
                getattr(self, "sniper_old", None), "state", None
            )
            sniper_old_recovery_required = bool(
                getattr(self, "_sniper_old_snapshot_corrupt", False)
                or getattr(self, "_sniper_old_snapshot_write_blocked", False)
                or (
                    getattr(sniper_old_state, "locked", False)
                    and str(getattr(sniper_old_state, "status", "")) == "error"
                )
            )
            classification["auto_rebalance_recovery_required"] = (
                rebalance_recovery_required
            )
            classification["sniper_old_recovery_required"] = (
                sniper_old_recovery_required
            )
            if sniper_old_recovery_required:
                self._activate_recovery_lock(
                    "sniper_old_recovery_required",
                    {**classification, "startup": startup},
                )
                await self._stop_entry_activity_for_recovery(
                    "Sniper Old durable ledger requires operator reconciliation"
                )
            elif rebalance_recovery_required:
                self._activate_recovery_lock(
                    "auto_rebalance_recovery_required",
                    {
                        **classification,
                        "startup": startup,
                        "auto_rebalance": rebalance_recovery,
                    },
                )
                await self._stop_entry_activity_for_recovery(
                    "Auto-Rebalance recovery journal requires operator reconciliation"
                )
            elif classification["unsafe"]:
                self._activate_recovery_lock(
                    "unmanaged_exchange_state",
                    {**classification, "startup": startup},
                )
                await self._stop_entry_activity_for_recovery(
                    "unmanaged orders or positions require reconciliation"
                )
            elif classification["flat"]:
                self._clear_recovery_lock()

            result = {
                **self.recovery_status(),
                "ok": not self.recovery_locked,
                **classification,
                "elapsed_ms": self._elapsed_ms(started_ns),
            }
            audit_event(
                "account.recovery_reconcile_completed",
                level=logging.INFO if not self.recovery_locked else logging.WARNING,
                account_id=self.account_id,
                startup=startup,
                locked=self.recovery_locked,
                reason=self.recovery_reason,
                classification=classification,
                elapsed_ms=result["elapsed_ms"],
            )
            return result

    @staticmethod
    def _elapsed_ms(started_ns: int) -> float:
        return _elapsed_ms(started_ns)

    def _persist_sniper_snapshot(self, snapshot: Optional[dict] = None) -> bool:
        started_ns = time.perf_counter_ns()
        audit_event(
            "sniper.persistence_started",
            account_id=self.account_id,
            supplied_snapshot=snapshot is not None,
        )
        if (
            getattr(self, "_sniper_snapshot_corrupt", False)
            or getattr(self, "_sniper_snapshot_write_blocked", False)
        ):
            logger.error(
                "Refusing to overwrite unsafe sniper snapshot for %s",
                self.account_id,
            )
            audit_event(
                "sniper.persistence_blocked",
                level=logging.ERROR,
                account_id=self.account_id,
                elapsed_ms=self._elapsed_ms(started_ns),
            )
            return False

        try:
            state = snapshot or self.sniper.get_state()
            config = self.sniper.get_config()
            save_sniper_state(
                self.account_id,
                config,
                state,
                bool(state.get("running")),
            )
            audit_event(
                "sniper.state_persisted",
                account_id=self.account_id,
                running=bool(state.get("running")),
                status=state.get("status", ""),
                locked=bool(state.get("locked")),
                active_target_id=state.get("active_target_id", ""),
                target_count=len((config or {}).get("targets", [])),
                elapsed_ms=self._elapsed_ms(started_ns),
            )
            return True
        except Exception as exc:
            logger.error("Failed to persist sniper state for %s: %s", self.account_id, exc)
            audit_event(
                "sniper.persistence_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                error=exc,
                elapsed_ms=self._elapsed_ms(started_ns),
            )
            return False

    def _persist_sniper_old_snapshot(self, snapshot: Optional[dict] = None) -> bool:
        if (
            getattr(self, "_sniper_old_snapshot_corrupt", False)
            or getattr(self, "_sniper_old_snapshot_write_blocked", False)
        ):
            return False
        try:
            data = snapshot or self.sniper_old._persistence_snapshot()
            save_sniper_old_state(
                self.account_id,
                data.get("config", {}),
                data.get("runtime", {}),
                bool(data.get("running")),
            )
            audit_event(
                "sniper_old.state_persisted",
                account_id=self.account_id,
                running=bool(data.get("running")),
                status=(data.get("runtime") or {}).get("status", ""),
                locked=bool((data.get("runtime") or {}).get("locked")),
            )
            return True
        except Exception as exc:
            logger.error(
                "Failed to persist Sniper Old state for %s: %s",
                self.account_id,
                exc,
            )
            audit_event(
                "sniper_old.persistence_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                error=exc,
            )
            return False

    def _get_strategy_params(self) -> dict:
        """Return current strategy params for auto-rebalance."""
        monitor_strategy = getattr(self.poly_monitor, "_strategy_type", "")
        use_s = self.strategy_s.state.running or (
            not self.strategy_d.state.running and monitor_strategy == "manual-s"
        )
        use_d = self.strategy_d.state.running or (
            not self.strategy_s.state.running and monitor_strategy == "manual-d"
        )
        if not use_s and not use_d:
            s_started = getattr(self.strategy_s.state, "start_time", 0) or 0
            d_started = getattr(self.strategy_d.state, "start_time", 0) or 0
            use_s = bool(self.strategy_s.state.asset_id and s_started >= d_started)
            use_d = bool(self.strategy_d.state.yes_asset_id and not use_s)

        if use_s:
            asset_id = str(self.strategy_s.state.asset_id or "")
            buy_cents = as_decimal(self.strategy_s.state.price_a)
            sell_cents = as_decimal(self.strategy_s.state.sell_price)
            mode = self.strategy_s.state.mode or "classic"
            return {
                "strategy_type": "manual-s",
                "market_id": self.strategy_s.state.market_id,
                "yes_asset_id": self.strategy_s.state.yes_asset_id,
                "no_asset_id": self.strategy_s.state.no_asset_id,
                "asset_id": asset_id,
                "side": self.strategy_s.state.side,
                "price_cents": buy_cents,
                "sell_price_cents": sell_cents,
                "monitor_price_cents": sell_cents if mode == "bid-sell" else buy_cents,
                "asset_cost_cents": {asset_id: buy_cents},
                "mode": mode,
                "stabilize_secs": self.strategy_s.state.stabilize_secs,
                "scan_interval": self.strategy_s.state.scan_interval,
                "start_time": self.strategy_s.state.start_time,
            }
        if use_d:
            yes_asset_id = str(self.strategy_d.state.yes_asset_id or "")
            no_asset_id = str(self.strategy_d.state.no_asset_id or "")
            yes_cost = as_decimal(self.strategy_d.state.price_a)
            no_cost = as_decimal(self.strategy_d.state.price_b)
            return {
                "strategy_type": "manual-d",
                "market_id": self.strategy_d.state.market_id,
                "yes_asset_id": yes_asset_id,
                "no_asset_id": no_asset_id,
                "asset_id": yes_asset_id,
                "side": "yes",
                "price_cents": yes_cost,
                "sell_price_cents": as_decimal(self.strategy_d.state.sell_a),
                "monitor_price_cents": yes_cost,
                "asset_cost_cents": {
                    yes_asset_id: yes_cost,
                    no_asset_id: no_cost,
                },
                "stabilize_secs": self.strategy_d.state.stabilize_secs,
                "scan_interval": self.strategy_d.state.scan_interval,
                "start_time": self.strategy_d.state.start_time,
            }
        return {}

    async def _restart_strategy(self, new_price_cents: Decimal, strategy_type: str):
        """Restart strategy at new price (called by auto-rebalance)."""
        params = self._get_strategy_params()
        if not params:
            raise RuntimeError("Cannot restart: strategy parameters are unavailable")
        if params.get("strategy_type") != strategy_type:
            raise RuntimeError("Cannot restart: strategy context changed")
        new_reference = as_decimal(new_price_cents)
        target_prices = self.auto_rebalance._target_prices(new_reference, params)

        if strategy_type == "manual-s":
            if self.strategy_s.state.running or self.strategy_s.running:
                raise RuntimeError("Cannot restart: Manual-S is already running")
            if getattr(self.strategy_s.state, "deployment_blocked", False):
                reason = str(
                    getattr(self.strategy_s.state, "block_reason", "")
                    or "safety reconciliation is incomplete"
                )
                raise RuntimeError(f"Cannot restart Manual-S: {reason}")
            pct = self.strategy_s.state.position_pct if self.strategy_s.state.position_pct else 100
            scan_int = self.strategy_s.state.scan_interval if hasattr(self.strategy_s.state, 'scan_interval') else 10
            stab_secs = self.strategy_s.state.stabilize_secs if hasattr(self.strategy_s.state, 'stabilize_secs') else 10
            mode = self.strategy_s.state.mode or "classic"
            new_buy = target_prices["buy"]
            new_sell = target_prices["sell"]

            await self.strategy_s.start(
                market_id=params["market_id"],
                yes_asset_id=params["yes_asset_id"],
                no_asset_id=params["no_asset_id"],
                side=params["side"],
                price_a=format(new_buy, "f"),
                sell_price=format(new_sell, "f"),
                mode=mode,
                position_pct=pct,
                scan_interval=scan_int,
                stabilize_secs=stab_secs,
                defer_entry=True,
            )
            if not self.strategy_s.state.running or not self.strategy_s.running:
                raise RuntimeError("Manual-S restart did not enter running state")
            if getattr(self.strategy_s.state, "deployment_blocked", False):
                reason = str(
                    getattr(self.strategy_s.state, "block_reason", "")
                    or "deployment was blocked during restart"
                )
                raise RuntimeError(f"Manual-S restart is blocked: {reason}")
            if self.strategy_s.release_entry_barrier() is not True:
                raise RuntimeError("Manual-S restart entry barrier could not be released")
        elif strategy_type == "manual-d":
            if self.strategy_d.state.running or self.strategy_d.running:
                raise RuntimeError("Cannot restart: Manual-D is already running")
            if getattr(self.strategy_d.state, "deployment_blocked", False):
                reason = str(
                    getattr(self.strategy_d.state, "block_reason", "")
                    or "safety reconciliation is incomplete"
                )
                raise RuntimeError(f"Cannot restart Manual-D: {reason}")
            pct = self.strategy_d.state.position_pct if self.strategy_d.state.position_pct else 100
            scan_int = self.strategy_d.state.scan_interval if hasattr(self.strategy_d.state, 'scan_interval') else 10
            stab_secs = self.strategy_d.state.stabilize_secs if hasattr(self.strategy_d.state, 'stabilize_secs') else 10
            await self.strategy_d.start(
                market_id=params["market_id"],
                yes_asset_id=params["yes_asset_id"],
                no_asset_id=params["no_asset_id"],
                price_a=format(target_prices["buy"], "f"),
                position_pct=pct,
                scan_interval=scan_int,
                stabilize_secs=stab_secs,
                defer_entry=True,
            )
            if not self.strategy_d.state.running or not self.strategy_d.running:
                raise RuntimeError("Manual-D restart did not enter running state")
            if getattr(self.strategy_d.state, "deployment_blocked", False):
                reason = str(
                    getattr(self.strategy_d.state, "block_reason", "")
                    or "deployment was blocked during restart"
                )
                raise RuntimeError(f"Manual-D restart is blocked: {reason}")
            if self.strategy_d.release_entry_barrier() is not True:
                raise RuntimeError("Manual-D restart entry barrier could not be released")
        else:
            raise RuntimeError(f"Unsupported strategy type: {strategy_type}")
        return {"ok": True}

    async def _restart_monitor(self, reference_price: Decimal, poly_token_id: str):
        """Restart monitor at new reference price."""
        if getattr(self.poly_monitor, "running", False):
            raise RuntimeError("Cannot restart monitor: another monitor is already running")
        depth_threshold = self.poly_monitor.state.depth_threshold
        if getattr(self.poly_monitor, "_task", None) is not None:
            await self.poly_monitor.stop(cancel_rebalance=False)
        # Reset triggered state
        self.poly_monitor.state.triggered = False
        self.poly_monitor.state.trigger_reason = ""
        self.poly_monitor._running = False
        self.poly_monitor.state.enabled = False
        # Get strategy context for the monitor
        params = self._get_strategy_params()
        if not params:
            raise RuntimeError("Cannot restart monitor: strategy parameters are unavailable")
        strategy_type = params["strategy_type"]
        price_cents = price_to_cents(
            align_okx_price(as_decimal(reference_price), "buy")
        )

        self.poly_monitor.set_strategy_context(
            strategy_type,
            price_cents,
            params.get("asset_id", ""),
            mode=params.get("mode", "classic"),
        )
        result = await self.poly_monitor.start(
            poly_token_id=poly_token_id,
            reference_price=format(as_decimal(reference_price), "f"),
            depth_threshold=depth_threshold,
        )
        if not isinstance(result, dict) or not result.get("ok"):
            raise RuntimeError(
                (result or {}).get("error", "Poly monitor failed to restart")
                if isinstance(result, dict)
                else "Poly monitor failed to restart"
            )
        return result

    async def _stop_monitor_for_rebalance(self):
        """Stop monitor state without recursively cancelling Auto-Rebalance."""
        await self.poly_monitor.stop(cancel_rebalance=False)

    async def _stop_all_strategies(self):
        """Stop both Manual strategies and return True only when both confirm safety."""
        safe = True
        for strategy_name, strategy in (
            ("manual-d", self.strategy_d),
            ("manual-s", self.strategy_s),
        ):
            try:
                stopped = await strategy.stop()
                if stopped is not True:
                    safe = False
                    logger.error(
                        "[%s] %s stop did not confirm a safe terminal state",
                        self.account_id,
                        strategy_name,
                    )
            except Exception as exc:
                safe = False
                logger.error(
                    "[%s] %s stop failed: %s",
                    self.account_id,
                    strategy_name,
                    exc,
                    exc_info=True,
                )
        audit_event(
            "account.manual_strategies_stop_completed",
            level=logging.INFO if safe else logging.ERROR,
            account_id=self.account_id,
            safe=safe,
        )
        return safe

    async def close(self, cancel_orders: bool = False):
        started_ns = time.perf_counter_ns()
        stage = "start"
        account_id = getattr(self, "account_id", "")
        sniper_was_running = self.sniper.running
        sniper_old_was_running = self.sniper_old.running
        audit_event(
            "account.close_started",
            account_id=account_id,
            cancel_orders=cancel_orders,
            strategy_d_running=bool(self.strategy_d.running),
            strategy_s_running=bool(self.strategy_s.running),
            monitor_running=bool(self.poly_monitor.running),
            sniper_running=bool(sniper_was_running),
            sniper_old_running=bool(self.sniper_old.running),
            rebalance_running=bool(
                getattr(getattr(self, "auto_rebalance", None), "_rebalancing", False)
            ),
            recovery_locked=bool(getattr(self, "recovery_locked", False)),
            recovery_reason=str(getattr(self, "recovery_reason", "") or ""),
        )
        try:
            stage = "stop_account_heartbeat"
            await self.stop_account_heartbeat()
            # A background rebalance can otherwise outlive the client and restart
            # strategies or submit orders after account shutdown/removal.
            stage = "cancel_rebalance"
            if hasattr(self, "auto_rebalance") and self.auto_rebalance:
                await self.auto_rebalance.cancel()
            stage = "stop_manual_d"
            if self.strategy_d.running:
                await self.strategy_d.stop()
            stage = "stop_manual_s"
            if self.strategy_s.running:
                await self.strategy_s.stop()
            monitor_enabled = bool(
                getattr(getattr(self.poly_monitor, "state", None), "enabled", False)
            )
            stage = "stop_poly_monitor"
            if self.poly_monitor.running or monitor_enabled:
                await self.poly_monitor.stop()
            stage = "stop_sniper"
            if self.sniper.running:
                await self.sniper.stop()
            stage = "stop_sniper_old"
            if self.sniper_old.running:
                await self.sniper_old.stop()
            # Session shutdown is non-destructive by default. User-facing cancel/flash
            # flows verify exact order lifecycles before this method closes the client.
            if cancel_orders:
                stage = "cancel_exchange_orders"
                cancel_started_ns = time.perf_counter_ns()
                try:
                    cancel_result = await self.client.cancel_all()
                    audit_event(
                        "account.close_cancel_all_response",
                        account_id=account_id,
                        code=(cancel_result or {}).get("code", "")
                        if isinstance(cancel_result, dict)
                        else "",
                        elapsed_ms=self._elapsed_ms(cancel_started_ns),
                    )
                except Exception as exc:
                    audit_event(
                        "account.close_cancel_all_failed",
                        level=logging.ERROR,
                        account_id=account_id,
                        error=exc,
                        elapsed_ms=self._elapsed_ms(cancel_started_ns),
                    )
            elif sniper_was_running:
                stage = "persist_resume_marker"
                snapshot = self.sniper.get_state()
                snapshot["running"] = True
                self._persist_sniper_snapshot(snapshot)
            if not cancel_orders and sniper_old_was_running:
                stage = "persist_sniper_old_resume_marker"
                old_snapshot = self.sniper_old._persistence_snapshot()
                old_snapshot["running"] = True
                if not self._persist_sniper_old_snapshot(old_snapshot):
                    raise RuntimeError(
                        "Could not preserve Sniper Old restart marker"
                    )
            stage = "close_client"
            await self.client.close()
        except asyncio.CancelledError:
            audit_event(
                "account.close_cancelled",
                level=logging.WARNING,
                account_id=account_id,
                stage=stage,
                elapsed_ms=self._elapsed_ms(started_ns),
            )
            raise
        except Exception as exc:
            audit_event(
                "account.close_failed",
                level=logging.ERROR,
                account_id=account_id,
                stage=stage,
                elapsed_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            raise
        audit_event(
            "account.close_completed",
            account_id=account_id,
            cancel_orders=cancel_orders,
            resume_marker_preserved=bool(
                not cancel_orders and (sniper_was_running or sniper_old_was_running)
            ),
            elapsed_ms=self._elapsed_ms(started_ns),
        )

    def install_client(self, client: OKXOutcomesClient) -> OKXOutcomesClient:
        """Atomically redirect every strategy reference to a prepared client."""
        client._account_id = self.account_id
        previous = self.client
        self.client = client
        self.strategy_d.client = client
        self.strategy_s.client = client
        self.poly_monitor.client = client
        self.auto_rebalance.client = client
        self.sniper.client = client
        self.sniper_old.client = client
        audit_event(
            "account.client_installed",
            account_id=self.account_id,
            previous_base_url=getattr(previous, "base_url", ""),
            new_base_url=getattr(client, "base_url", ""),
            previous_proxy_node=getattr(previous, "_proxy_node", ""),
            new_proxy_node=getattr(client, "_proxy_node", ""),
        )
        return previous

    async def reload_client(self):
        """Build and install a fresh client before closing the previous one."""
        started_ns = time.perf_counter_ns()
        stage = "load_account_config"
        heartbeat_was_running = self.account_heartbeat_running
        audit_event("account.reload_started", account_id=self.account_id)
        try:
            acc = get_account_decrypted(self.account_id)
            if not acc:
                raise RuntimeError("Account not found")
            audit_event(
                "account.reload_config_loaded",
                account_id=self.account_id,
                base_url=acc.get("base_url", "https://us.okx.com"),
                proxy_node=acc.get("proxy_node", "JP2"),
            )
            if heartbeat_was_running:
                stage = "stop_account_heartbeat"
                await self.stop_account_heartbeat()
            stage = "cancel_rebalance"
            if hasattr(self, "auto_rebalance") and self.auto_rebalance:
                await self.auto_rebalance.cancel()
            stage = "build_client"
            replacement = OKXOutcomesClient(
                api_key=acc["api_key"],
                secret_key=acc["secret_key"],
                passphrase=acc["passphrase"],
                agent_private_key=acc["agent_private_key"],
                base_url=acc.get("base_url", "https://us.okx.com"),
                proxy_node=acc.get("proxy_node", "JP2"),
            )
            if not replacement.signer.address:
                raise ValueError("Invalid agent private key")
            stage = "install_client"
            previous = self.install_client(replacement)
            stage = "close_previous_client"
            if previous and hasattr(previous, "close"):
                previous_close_ns = time.perf_counter_ns()
                try:
                    await previous.close()
                    audit_event(
                        "account.previous_client_closed",
                        account_id=self.account_id,
                        elapsed_ms=self._elapsed_ms(previous_close_ns),
                    )
                except Exception as exc:
                    logger.warning("Failed to close previous client for %s: %s", self.account_id, exc)
                    audit_event(
                        "account.previous_client_close_failed",
                        level=logging.WARNING,
                        account_id=self.account_id,
                        elapsed_ms=self._elapsed_ms(previous_close_ns),
                        error=exc,
                    )
        except asyncio.CancelledError:
            audit_event(
                "account.reload_cancelled",
                level=logging.WARNING,
                account_id=self.account_id,
                stage=stage,
                elapsed_ms=self._elapsed_ms(started_ns),
            )
            raise
        except Exception as exc:
            if heartbeat_was_running and not self.account_heartbeat_running:
                self.start_account_heartbeat()
            audit_event(
                "account.reload_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                stage=stage,
                elapsed_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            raise
        if heartbeat_was_running:
            self.start_account_heartbeat()
        audit_event(
            "account.reload_completed",
            account_id=self.account_id,
            base_url=getattr(self.client, "base_url", ""),
            proxy_node=getattr(self.client, "_proxy_node", ""),
            elapsed_ms=self._elapsed_ms(started_ns),
        )

class AccountManager:
    """Manages all account instances."""

    def __init__(self):
        self.instances: Dict[str, AccountInstance] = {}
        self.notifier: Optional[TelegramNotifier] = None
        self.load_failures: Dict[str, dict] = {}
        self.last_init_summary: dict = {
            "discovered_count": 0,
            "loaded_count": 0,
            "failed_count": 0,
            "recovery_locked_count": 0,
        }


    async def init(self):
        """Load all accounts from DB and create client instances."""
        started_ns = time.perf_counter_ns()
        audit_event("accounts.init_started")
        self.load_failures = {}
        # Init notifier from global config
        tg_token = get_global_config("tg_token", "")
        tg_chats_str = get_global_config("tg_chats", "")
        tg_chats = [c.strip() for c in tg_chats_str.split(",") if c.strip()] if tg_chats_str else []
        self.notifier = TelegramNotifier(token=tg_token, chat_ids=tg_chats)

        # Load accounts
        accounts = get_accounts()
        audit_event("accounts.init_discovered", account_count=len(accounts))
        loaded_count = 0
        for acc in accounts:
            try:
                instance = await self._load_account(acc["id"])
                if instance is not None:
                    loaded_count += 1
            except Exception as e:
                logger.error(f"Failed to load account {acc['name']}: {e}")
                self.load_failures[str(acc.get("id", ""))] = {
                    "name": str(acc.get("name", "") or ""),
                    "error_type": type(e).__name__,
                    "message": str(e),
                }
                partial = self.instances.pop(acc.get("id", ""), None)
                if partial is not None:
                    partial._sniper_snapshot_write_blocked = True
                    partial._sniper_old_snapshot_write_blocked = True
                    try:
                        await partial.close(cancel_orders=False)
                    except Exception as cleanup_exc:
                        audit_event(
                            "account.load_cleanup_failed",
                            level=logging.ERROR,
                            account_id=acc.get("id", ""),
                            error=cleanup_exc,
                        )
                audit_event(
                    "account.load_failed",
                    level=logging.ERROR,
                    account_id=acc.get("id", ""),
                    stage="init_loop",
                    error=e,
                )
        recovery_locked_count = sum(
            bool(getattr(instance, "recovery_locked", False))
            for instance in self.instances.values()
        )
        self.last_init_summary = {
            "discovered_count": len(accounts),
            "loaded_count": loaded_count,
            "failed_count": len(self.load_failures),
            "failed_account_ids": list(self.load_failures),
            "recovery_locked_count": recovery_locked_count,
        }
        audit_event(
            "accounts.init_completed",
            **self.last_init_summary,
            elapsed_ms=_elapsed_ms(started_ns),
        )

    async def _finalize_account_load(
        self,
        instance: AccountInstance,
        account_id: str,
        *,
        restore_status: str,
        started_ns: int,
        level: int = logging.INFO,
        resume_sniper: bool = False,
        sniper_restore_state: Optional[dict] = None,
        resume_sniper_old: bool = False,
        sniper_old_restore_state: Optional[dict] = None,
    ) -> AccountInstance:
        reconcile = getattr(instance, "reconcile_account_safety", None)
        safety_result = None
        if callable(reconcile):
            safety_result = await reconcile(startup=True)
        resume_result = None
        safety_ok = bool(
            isinstance(safety_result, dict)
            and safety_result.get("ok")
            and not safety_result.get("locked")
            and not getattr(instance, "recovery_locked", False)
        )
        if resume_sniper and sniper_restore_state is not None:
            if safety_ok:
                resume_started_ns = time.perf_counter_ns()
                audit_event(
                    "sniper.resume_after_account_reconcile_started",
                    account_id=account_id,
                )
                try:
                    resume_result = await instance.sniper.restore(
                        sniper_restore_state,
                        resume=True,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    resume_result = {"ok": False, "error": str(exc)}
                if not isinstance(resume_result, dict) or not resume_result.get("ok"):
                    error = (
                        resume_result.get("error", "Sniper resume failed")
                        if isinstance(resume_result, dict)
                        else "Sniper resume returned an invalid result"
                    )
                    instance._activate_recovery_lock(
                        "sniper_resume_failed_after_reconcile",
                        {"error": error, "resume_result": resume_result},
                    )
                    level = logging.WARNING
                audit_event(
                    "sniper.resume_after_account_reconcile_completed",
                    level=(
                        logging.INFO
                        if isinstance(resume_result, dict) and resume_result.get("ok")
                        else logging.ERROR
                    ),
                    account_id=account_id,
                    ok=bool(
                        isinstance(resume_result, dict) and resume_result.get("ok")
                    ),
                    needs_reconciliation=bool(
                        isinstance(resume_result, dict)
                        and resume_result.get("needs_reconciliation")
                    ),
                    error=(
                        resume_result.get("error", "")
                        if isinstance(resume_result, dict)
                        else "invalid result"
                    ),
                    elapsed_ms=_elapsed_ms(resume_started_ns),
                )
            else:
                level = logging.WARNING
                audit_event(
                    "sniper.resume_blocked_by_account_reconcile",
                    level=logging.WARNING,
                    account_id=account_id,
                    safety_result=safety_result,
                    recovery_reason=str(
                        getattr(instance, "recovery_reason", "") or ""
                    ),
                )
        old_resume_result = None
        if resume_sniper_old and sniper_old_restore_state is not None:
            if safety_ok and not resume_sniper:
                try:
                    old_resume_result = await instance.sniper_old.restore(
                        sniper_old_restore_state,
                        resume=True,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    old_resume_result = {"ok": False, "error": str(exc)}
                if (
                    not isinstance(old_resume_result, dict)
                    or not old_resume_result.get("ok")
                ):
                    error = (
                        old_resume_result.get("error", "Sniper Old resume failed")
                        if isinstance(old_resume_result, dict)
                        else "Sniper Old resume returned an invalid result"
                    )
                    instance._activate_recovery_lock(
                        "sniper_old_resume_failed_after_reconcile",
                        {"error": error, "resume_result": old_resume_result},
                    )
                    level = logging.WARNING
            else:
                instance._activate_recovery_lock(
                    "sniper_old_resume_blocked_by_account_reconcile",
                    {
                        "safety_result": safety_result,
                        "new_sniper_resume_requested": resume_sniper,
                    },
                )
                level = logging.WARNING
        start_heartbeat = getattr(instance, "start_account_heartbeat", None)
        if callable(start_heartbeat):
            start_heartbeat()
        audit_event(
            "account.load_completed",
            level=level,
            account_id=account_id,
            restore_status=restore_status,
            recovery_locked=bool(getattr(instance, "recovery_locked", False)),
            recovery_reason=str(getattr(instance, "recovery_reason", "") or ""),
            safety_ok=(
                bool(safety_result.get("ok"))
                if isinstance(safety_result, dict)
                else None
            ),
            sniper_resume_requested=resume_sniper,
            sniper_resumed=bool(
                isinstance(resume_result, dict) and resume_result.get("resumed")
            ),
            sniper_resume_ok=(
                bool(resume_result.get("ok"))
                if isinstance(resume_result, dict)
                else None
            ),
            sniper_old_resume_requested=resume_sniper_old,
            sniper_old_resumed=bool(
                isinstance(old_resume_result, dict)
                and old_resume_result.get("resumed")
            ),
            sniper_old_resume_ok=(
                bool(old_resume_result.get("ok"))
                if isinstance(old_resume_result, dict)
                else None
            ),
            elapsed_ms=_elapsed_ms(started_ns),
        )
        return instance

    async def _load_account(self, account_id: str) -> Optional[AccountInstance]:
        """Load a single account into memory."""
        started_ns = time.perf_counter_ns()
        stage = "load_account_config"
        audit_event("account.load_started", account_id=account_id)
        try:
            acc = get_account_decrypted(account_id)
            if not acc:
                audit_event(
                    "account.load_not_found",
                    level=logging.WARNING,
                    account_id=account_id,
                    elapsed_ms=_elapsed_ms(started_ns),
                )
                return None
            audit_event(
                "account.config_loaded",
                account_id=account_id,
                base_url=acc.get("base_url", "https://us.okx.com"),
                proxy_node=acc.get("proxy_node", ""),
            )

            stage = "build_client"
            client = OKXOutcomesClient(
                api_key=acc["api_key"],
                secret_key=acc["secret_key"],
                passphrase=acc["passphrase"],
                agent_private_key=acc["agent_private_key"],
                base_url=acc.get("base_url", "https://us.okx.com"),
                proxy_node=acc["proxy_node"],
            )

            stage = "create_instance"
            instance = AccountInstance(
                account_id=acc["id"],
                name=acc["name"],
                client=client,
                proxy_node=acc["proxy_node"],
                notifier=self.notifier,
            )
            self.instances[acc["id"]] = instance
            audit_event(
                "account.instance_registered",
                account_id=account_id,
                loaded_account_count=len(self.instances),
            )

            stage = "load_sniper_snapshot"
            saved_sniper_running = False
            restore_state = None
            saved_sniper = get_sniper_state(acc["id"])
            if saved_sniper:
                saved_sniper_running = bool(saved_sniper.get("running"))
                saved_config = (
                    saved_sniper.get("config")
                    if isinstance(saved_sniper.get("config"), dict)
                    else {}
                )
                saved_runtime = (
                    saved_sniper.get("runtime")
                    if isinstance(saved_sniper.get("runtime"), dict)
                    else {}
                )
                saved_targets = saved_config.get("targets", [])
                audit_event(
                    "sniper.snapshot_loaded",
                    account_id=account_id,
                    running=bool(saved_sniper.get("running")),
                    corrupt=bool(saved_sniper.get("corrupt")),
                    corrupt_fields=saved_sniper.get("corrupt_fields") or [],
                    config_target_count=(
                        len(saved_targets) if isinstance(saved_targets, list) else 0
                    ),
                    runtime_status=saved_runtime.get("status", ""),
                )
                if saved_sniper.get("corrupt"):
                    instance._sniper_snapshot_corrupt = True
                    instance._sniper_snapshot_write_blocked = True
                    instance.sniper.state.status = "error"
                    instance.sniper.state.locked = True
                    logger.error(
                        "Sniper snapshot for %s is corrupt (%s); preserving the database row and blocking writes",
                        acc["id"],
                        ", ".join(saved_sniper.get("corrupt_fields") or ["unknown field"]),
                    )
                    audit_event(
                        "sniper.restore_blocked_corrupt_snapshot",
                        level=logging.ERROR,
                        account_id=account_id,
                        corrupt_fields=saved_sniper.get("corrupt_fields") or [],
                    )
                    stage = "reconcile_account_safety"
                    return await self._finalize_account_load(
                        instance,
                        account_id,
                        restore_status="blocked_corrupt_snapshot",
                        started_ns=started_ns,
                        level=logging.WARNING,
                    )
                restore_state = dict(saved_runtime)
                restore_state.update(saved_config)
                stage = "restore_sniper"
                restore_started_ns = time.perf_counter_ns()
                audit_event(
                    "sniper.restore_started",
                    account_id=account_id,
                    resume=False,
                    resume_requested=saved_sniper_running,
                )
                try:
                    result = await instance.sniper.restore(
                        restore_state,
                        resume=False,
                    )
                except asyncio.CancelledError:
                    audit_event(
                        "sniper.restore_cancelled",
                        level=logging.WARNING,
                        account_id=account_id,
                        elapsed_ms=_elapsed_ms(restore_started_ns),
                    )
                    raise
                except Exception as restore_exc:
                    audit_event(
                        "sniper.restore_failed",
                        level=logging.ERROR,
                        account_id=account_id,
                        elapsed_ms=_elapsed_ms(restore_started_ns),
                        error=restore_exc,
                    )
                    raise
                audit_event(
                    "sniper.restore_completed",
                    level=(logging.INFO if result.get("ok") else logging.ERROR),
                    account_id=account_id,
                    ok=bool(result.get("ok")),
                    needs_reconciliation=bool(
                        result.get("needs_reconciliation", False)
                    ),
                    error=result.get("error", ""),
                    elapsed_ms=_elapsed_ms(restore_started_ns),
                )
                if not result.get("ok"):
                    instance.sniper.state.status = "error"
                    instance.sniper.state.locked = True
                    semantic_failure = not result.get("needs_reconciliation", False)
                    if semantic_failure:
                        # Parsing/config validation failed before a trustworthy
                        # runtime could be restored. Preserve the original row.
                        instance._sniper_snapshot_write_blocked = True
                    logger.error(
                        "Sniper restore for %s requires attention: %s",
                        acc["id"],
                        result.get("error", "unknown error"),
                    )
                    stage = "reconcile_account_safety"
                    return await self._finalize_account_load(
                        instance,
                        account_id,
                        restore_status="needs_attention",
                        started_ns=started_ns,
                        level=logging.WARNING,
                    )
                stage = "persist_restored_sniper"
                instance._persist_sniper_snapshot()

            stage = "load_sniper_old_snapshot"
            saved_sniper_old = get_sniper_old_state(acc["id"])
            saved_sniper_old_running = False
            sniper_old_restore_state = None
            if saved_sniper_old:
                saved_sniper_old_running = bool(saved_sniper_old.get("running"))
                old_config = saved_sniper_old.get("config")
                old_runtime = saved_sniper_old.get("runtime")
                if saved_sniper_old.get("corrupt"):
                    instance._sniper_old_snapshot_corrupt = True
                    instance._sniper_old_snapshot_write_blocked = True
                    instance.sniper_old.state.status = "error"
                    instance.sniper_old.state.locked = True
                    instance.sniper_old.state.reconciliation_error = (
                        "Sniper Old persistence snapshot is corrupt"
                    )
                    instance._activate_recovery_lock(
                        "sniper_old_corrupt_snapshot",
                        {
                            "corrupt_fields": saved_sniper_old.get("corrupt_fields")
                            or []
                        },
                    )
                elif isinstance(old_config, dict) and isinstance(old_runtime, dict):
                    sniper_old_restore_state = {
                        "config": old_config,
                        "runtime": old_runtime,
                        "running": saved_sniper_old_running,
                    }
                    old_result = await instance.sniper_old.restore(
                        sniper_old_restore_state,
                        resume=False,
                    )
                    if not old_result.get("ok"):
                        instance._activate_recovery_lock(
                            "sniper_old_restore_requires_attention",
                            {"result": old_result},
                        )
                    else:
                        # Phase one may have corrected the durable ledger.
                        # Resume from that reconciled state, never from the
                        # stale snapshot read before exchange reconciliation.
                        sniper_old_restore_state = (
                            instance.sniper_old._persistence_snapshot()
                        )
                        sniper_old_restore_state["running"] = (
                            saved_sniper_old_running
                        )
                else:
                    instance._sniper_old_snapshot_write_blocked = True
                    instance._activate_recovery_lock(
                        "sniper_old_invalid_snapshot",
                        {},
                    )
            stage = "reconcile_account_safety"
            return await self._finalize_account_load(
                instance,
                account_id,
                restore_status=("restored" if saved_sniper else "no_snapshot"),
                started_ns=started_ns,
                resume_sniper=(saved_sniper_running if saved_sniper else False),
                sniper_restore_state=(restore_state if saved_sniper else None),
                resume_sniper_old=(
                    saved_sniper_old_running
                    if saved_sniper_old and not getattr(
                        instance, "_sniper_old_snapshot_write_blocked", False
                    )
                    else False
                ),
                sniper_old_restore_state=sniper_old_restore_state,
            )
        except asyncio.CancelledError:
            audit_event(
                "account.load_cancelled",
                level=logging.WARNING,
                account_id=account_id,
                stage=stage,
                elapsed_ms=_elapsed_ms(started_ns),
            )
            raise
        except Exception as exc:
            audit_event(
                "account.load_failed",
                level=logging.ERROR,
                account_id=account_id,
                stage=stage,
                elapsed_ms=_elapsed_ms(started_ns),
                error=exc,
            )
            raise

    async def add_account(self, name: str, api_key: str, secret_key: str,
                          passphrase: str, agent_private_key: str,
                          proxy_node: str, region: str = "AU") -> dict:
        """Create and load an account, rolling back an incomplete creation."""
        result = create_account(name, api_key, secret_key, passphrase,
                                agent_private_key, proxy_node)
        account_id = result["id"]
        try:
            # Set base_url based on region before constructing the live client.
            base_url = "https://www.okx.com" if region == "CN" else "https://us.okx.com"
            from ..core.database import update_account as _upd
            _upd(account_id, base_url=base_url)
            await self._load_account(account_id)
            return result
        except BaseException:
            partial = self.instances.pop(account_id, None)
            if partial is not None:
                try:
                    await partial.close(cancel_orders=False)
                except Exception:
                    logger.exception(
                        "Failed to close partially created account %s", account_id
                    )
            try:
                delete_account(account_id)
            except Exception:
                logger.exception(
                    "Failed to roll back database row for account %s", account_id
                )
            audit_event(
                "account.create_rolled_back",
                level=logging.ERROR,
                account_id=account_id,
            )
            raise

    async def remove_account(self, account_id: str) -> dict:
        """Stop all writers, commit the DB deletion, then remove exact state files."""
        started_ns = time.perf_counter_ns()
        closed_instance = False
        audit_event(
            "account.remove_started",
            account_id=account_id,
            loaded=account_id in self.instances,
        )
        if account_id in self.instances:
            # Local removal stops writers and closes this client's session. It
            # deliberately does not place, cancel, or otherwise modify orders
            # and positions that may still exist on the exchange.
            await self.instances[account_id].close(cancel_orders=False)
            # A closed client/strategy graph must never remain discoverable as
            # a live account. If the database write fails below, recovery can
            # only register a newly constructed instance.
            self.instances.pop(account_id, None)
            closed_instance = True
        try:
            deleted = delete_account(account_id)
        except Exception as exc:
            await self._recover_failed_account_removal(
                account_id,
                closed_instance=closed_instance,
                failure="database_exception",
                cause=exc,
                started_ns=started_ns,
            )
            raise
        if not deleted:
            error = RuntimeError("Account database row was not deleted")
            await self._recover_failed_account_removal(
                account_id,
                closed_instance=closed_instance,
                failure="database_miss",
                cause=error,
                started_ns=started_ns,
            )
            raise error
        state_cleanup = cleanup_account_state_files(account_id)
        result = {
            "deleted": bool(deleted),
            "cleanup_complete": bool(state_cleanup["complete"]),
            "state_cleanup": state_cleanup,
        }
        audit_event(
            "account.remove_completed",
            level=(
                logging.INFO
                if result["deleted"] and result["cleanup_complete"]
                else logging.ERROR
            ),
            account_id=account_id,
            deleted=result["deleted"],
            cleanup_complete=result["cleanup_complete"],
            cleanup_errors=state_cleanup["errors"],
            elapsed_ms=_elapsed_ms(started_ns),
        )
        return result

    async def _recover_failed_account_removal(
        self,
        account_id: str,
        *,
        closed_instance: bool,
        failure: str,
        cause: BaseException,
        started_ns: int,
    ) -> Optional[AccountInstance]:
        """Reload a fresh live graph after a failed post-close DB deletion."""
        if not closed_instance:
            audit_event(
                "account.remove_failed",
                level=logging.ERROR,
                account_id=account_id,
                failure=failure,
                instance_recovery="not_required",
                error=cause,
                elapsed_ms=_elapsed_ms(started_ns),
            )
            return self.instances.get(account_id)

        # Be explicit even if a future refactor calls this after registering a
        # partial object: never expose anything derived from the closed graph.
        self.instances.pop(account_id, None)
        try:
            replacement = await self._load_account(account_id)
        except asyncio.CancelledError:
            audit_event(
                "account.remove_failed_recovery_cancelled",
                level=logging.ERROR,
                account_id=account_id,
                failure=failure,
                error=cause,
                elapsed_ms=_elapsed_ms(started_ns),
            )
            raise
        except Exception as recovery_exc:
            partial = self.instances.pop(account_id, None)
            if partial is not None:
                try:
                    await partial.close(cancel_orders=False)
                except Exception:
                    logger.exception(
                        "Failed to close partial removal recovery for %s",
                        account_id,
                    )
            audit_event(
                "account.remove_failed_recovery_failed",
                level=logging.CRITICAL,
                account_id=account_id,
                failure=failure,
                error=cause,
                recovery_error=recovery_exc,
                elapsed_ms=_elapsed_ms(started_ns),
            )
            raise RuntimeError(
                "Account deletion failed and a fresh live instance could not be restored"
            ) from recovery_exc

        if replacement is None:
            # A database miss can mean another writer already removed the row.
            # The old object remains closed and absent; no state files are
            # touched because this process did not prove its own DB deletion.
            self.instances.pop(account_id, None)
            audit_event(
                "account.remove_failed_recovery_not_found",
                level=logging.ERROR,
                account_id=account_id,
                failure=failure,
                error=cause,
                elapsed_ms=_elapsed_ms(started_ns),
            )
            return None

        audit_event(
            "account.remove_failed_recovered",
            level=logging.ERROR,
            account_id=account_id,
            failure=failure,
            instance_recovery="fresh_instance_loaded",
            error=cause,
            elapsed_ms=_elapsed_ms(started_ns),
        )
        return replacement

    async def update_account_credentials(self, account_id: str, **kwargs):
        """Update credentials and reload client."""
        started_ns = time.perf_counter_ns()
        audit_event(
            "account.credentials_update_started",
            account_id=account_id,
            changed_field_count=len(kwargs),
        )
        update_account(account_id, **kwargs)
        # Reload the instance
        if account_id in self.instances:
            await self.instances[account_id].close()
            del self.instances[account_id]
        await self._load_account(account_id)
        audit_event(
            "account.credentials_update_completed",
            account_id=account_id,
            elapsed_ms=_elapsed_ms(started_ns),
        )

    def get_instance(self, account_id: str) -> Optional[AccountInstance]:
        return self.instances.get(account_id)

    async def get_aggregated_stats(self) -> dict:
        """Get aggregated stats across all accounts."""
        total_balance = 0.0
        total_available = 0.0
        total_orders = 0
        total_positions = 0
        running_count = 0

        for inst in self.instances.values():
            try:
                bal = await inst.client.get_balance()
                if isinstance(bal, dict) and str(bal.get("code", "")) == "0":
                    data = bal.get("data", [])
                    for entry in data:
                        total_balance += float(entry.get("balance", "0"))
                        total_available += float(entry.get("available", "0"))
            except Exception:
                pass

            try:
                orders = await inst.client.get_orders(status="open")
                if isinstance(orders, dict) and str(orders.get("code", "")) == "0":
                    raw_o = orders.get("data", {})
                    total_orders += len(raw_o.get("list", []) if isinstance(raw_o, dict) else raw_o if isinstance(raw_o, list) else [])
            except Exception:
                pass

            try:
                positions = await inst.client.get_positions(status="open")
                if isinstance(positions, dict) and str(positions.get("code", "")) == "0":
                    raw_p = positions.get("data", {})
                    pos_l = raw_p.get("list", []) if isinstance(raw_p, dict) else raw_p if isinstance(raw_p, list) else []
                    for position in pos_l:
                        try:
                            if as_decimal(position.get("size", "0")) != 0:
                                total_positions += 1
                        except ValueError:
                            continue
            except Exception:
                pass

            sniper = getattr(inst, "sniper", None)
            poly_monitor = getattr(inst, "poly_monitor", None)
            if (
                getattr(getattr(inst, "strategy_d", None), "running", False)
                or getattr(getattr(inst, "strategy_s", None), "running", False)
                or getattr(sniper, "running", False)
                or getattr(getattr(sniper, "state", None), "locked", False)
                or getattr(sniper, "_persistence_failed", False)
                or getattr(getattr(inst, "sniper_old", None), "running", False)
                or getattr(poly_monitor, "running", False)
                or getattr(getattr(poly_monitor, "state", None), "enabled", False)
                or getattr(getattr(inst, "auto_rebalance", None), "_rebalancing", False)
                or getattr(inst, "recovery_locked", False)
            ):
                running_count += 1

        return {
            "total_balance": f"{total_balance:.2f}",
            "total_available": f"{total_available:.2f}",
            "total_orders": total_orders,
            "total_positions": total_positions,
            "running_accounts": running_count,
            "total_accounts": len(self.instances),
        }

    async def close_all(self, cancel_orders: bool = False):
        started_ns = time.perf_counter_ns()
        audit_event(
            "accounts.close_all_started",
            account_count=len(self.instances),
            cancel_orders=cancel_orders,
        )
        for inst in self.instances.values():
            await inst.close(cancel_orders=cancel_orders)
        if self.notifier:
            await self.notifier.close()
        audit_event(
            "accounts.close_all_completed",
            account_count=len(self.instances),
            cancel_orders=cancel_orders,
            elapsed_ms=_elapsed_ms(started_ns),
        )
