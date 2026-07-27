"""Account manager: manages OKX client instances and strategies per account."""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Optional

from ..core.client import OKXOutcomesClient
from ..core.logging_config import audit_event as _emit_audit_event
from ..core.price_utils import align_okx_price, as_decimal, price_to_cents
from ..core.database import (
    get_accounts, get_account_decrypted, create_account, update_account,
    delete_account, get_available_proxies, get_used_proxies, PROXY_NODES,
    get_global_config, set_global_config, get_sniper_state, save_sniper_state
)
from .manual_strategy_d import ManualStrategyD
from .manual_strategy_s import ManualStrategyS
from .poly_monitor import PolyMonitor
from .auto_rebalance import AutoRebalance
from .poly_sniper import PolySniper
from .poly_sniper_old import PolySniperOld
from .notifier import TelegramNotifier

logger = logging.getLogger(__name__)


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
            )
            if not self.strategy_s.state.running or not self.strategy_s.running:
                raise RuntimeError("Manual-S restart did not enter running state")
        elif strategy_type == "manual-d":
            if self.strategy_d.state.running or self.strategy_d.running:
                raise RuntimeError("Cannot restart: Manual-D is already running")
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
            )
            if not self.strategy_d.state.running or not self.strategy_d.running:
                raise RuntimeError("Manual-D restart did not enter running state")
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
        self.poly_monitor.set_strategy_context(
            strategy_type,
            price_cents,
            params.get("asset_id", ""),
            mode=params.get("mode", "classic"),
        )
        return result

    async def _stop_monitor_for_rebalance(self):
        """Stop monitor state without recursively cancelling Auto-Rebalance."""
        await self.poly_monitor.stop(cancel_rebalance=False)

    async def _stop_all_strategies(self):
        """Stop all running strategies (called by monitor on trigger)."""
        if self.strategy_d.running:
            await self.strategy_d.stop()
        if self.strategy_s.running:
            await self.strategy_s.stop()

    async def close(self, cancel_orders: bool = True):
        started_ns = time.perf_counter_ns()
        stage = "start"
        account_id = getattr(self, "account_id", "")
        sniper_was_running = self.sniper.running
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
        )
        try:
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
            # Manual account removal can still cancel, but app/service shutdown must not
            # wipe live exchange orders. Lifespan close_all passes cancel_orders=False.
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
            resume_marker_preserved=bool(not cancel_orders and sniper_was_running),
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
            audit_event(
                "account.reload_failed",
                level=logging.ERROR,
                account_id=self.account_id,
                stage=stage,
                elapsed_ms=self._elapsed_ms(started_ns),
                error=exc,
            )
            raise
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


    async def init(self):
        """Load all accounts from DB and create client instances."""
        started_ns = time.perf_counter_ns()
        audit_event("accounts.init_started")
        # Init notifier from global config
        tg_token = get_global_config("tg_token", "")
        tg_chats_str = get_global_config("tg_chats", "")
        tg_chats = [c.strip() for c in tg_chats_str.split(",") if c.strip()] if tg_chats_str else []
        self.notifier = TelegramNotifier(token=tg_token, chat_ids=tg_chats)

        # Load accounts
        accounts = get_accounts()
        audit_event("accounts.init_discovered", account_count=len(accounts))
        for acc in accounts:
            try:
                await self._load_account(acc["id"])
            except Exception as e:
                logger.error(f"Failed to load account {acc['name']}: {e}")
                audit_event(
                    "account.load_failed",
                    level=logging.ERROR,
                    account_id=acc.get("id", ""),
                    stage="init_loop",
                    error=e,
                )
        audit_event(
            "accounts.init_completed",
            discovered_count=len(accounts),
            loaded_count=len(self.instances),
            failed_count=max(0, len(accounts) - len(self.instances)),
            elapsed_ms=_elapsed_ms(started_ns),
        )

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
            saved_sniper = get_sniper_state(acc["id"])
            if saved_sniper:
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
                    audit_event(
                        "account.load_completed",
                        level=logging.WARNING,
                        account_id=account_id,
                        restore_status="blocked_corrupt_snapshot",
                        elapsed_ms=_elapsed_ms(started_ns),
                    )
                    return instance
                restore_state = dict(saved_runtime)
                restore_state.update(saved_config)
                stage = "restore_sniper"
                restore_started_ns = time.perf_counter_ns()
                audit_event(
                    "sniper.restore_started",
                    account_id=account_id,
                    resume=bool(saved_sniper.get("running")),
                )
                try:
                    result = await instance.sniper.restore(
                        restore_state,
                        resume=bool(saved_sniper.get("running")),
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
                    audit_event(
                        "account.load_completed",
                        level=logging.WARNING,
                        account_id=account_id,
                        restore_status="needs_attention",
                        write_blocked=bool(
                            instance._sniper_snapshot_write_blocked
                        ),
                        elapsed_ms=_elapsed_ms(started_ns),
                    )
                    return instance
                stage = "persist_restored_sniper"
                instance._persist_sniper_snapshot()
            audit_event(
                "account.load_completed",
                account_id=account_id,
                restore_status=("restored" if saved_sniper else "no_snapshot"),
                elapsed_ms=_elapsed_ms(started_ns),
            )
            return instance
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
        """Create account in DB and load into memory."""
        result = create_account(name, api_key, secret_key, passphrase,
                                agent_private_key, proxy_node)
        # Set base_url based on region
        base_url = "https://www.okx.com" if region == "CN" else "https://us.okx.com"
        from ..core.database import update_account as _upd
        _upd(result["id"], base_url=base_url)
        await self._load_account(result["id"])
        return result

    async def remove_account(self, account_id: str) -> bool:
        """Stop strategies, close client, remove from DB."""
        started_ns = time.perf_counter_ns()
        audit_event(
            "account.remove_started",
            account_id=account_id,
            loaded=account_id in self.instances,
        )
        if account_id in self.instances:
            await self.instances[account_id].close()
            del self.instances[account_id]
        delete_account(account_id)
        audit_event(
            "account.remove_completed",
            account_id=account_id,
            elapsed_ms=_elapsed_ms(started_ns),
        )
        return True

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
