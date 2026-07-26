"""Account manager: manages OKX client instances and strategies per account."""
import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional

from ..core.client import OKXOutcomesClient
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


class AccountInstance:
    """A live account instance with its own client and strategies."""

    def __init__(self, account_id: str, name: str, client: OKXOutcomesClient,
                 proxy_node: str, notifier: Optional[TelegramNotifier] = None):
        self.account_id = account_id
        self.name = name
        self.client = client
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
        self.auto_rebalance.stop_monitor = self.poly_monitor.stop
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

    def _persist_sniper_snapshot(self, snapshot: Optional[dict] = None) -> bool:
        if (
            getattr(self, "_sniper_snapshot_corrupt", False)
            or getattr(self, "_sniper_snapshot_write_blocked", False)
        ):
            logger.error(
                "Refusing to overwrite unsafe sniper snapshot for %s",
                self.account_id,
            )
            return False
        try:
            state = snapshot or self.sniper.get_state()
            save_sniper_state(
                self.account_id,
                self.sniper.get_config(),
                state,
                bool(state.get("running")),
            )
            return True
        except Exception as exc:
            logger.error("Failed to persist sniper state for %s: %s", self.account_id, exc)
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
            await self.poly_monitor.stop()
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

    async def _stop_all_strategies(self):
        """Stop all running strategies (called by monitor on trigger)."""
        if self.strategy_d.running:
            await self.strategy_d.stop()
        if self.strategy_s.running:
            await self.strategy_s.stop()

    async def close(self, cancel_orders: bool = True):
        sniper_was_running = self.sniper.running
        # A background rebalance can otherwise outlive the client and restart
        # strategies or submit orders after account shutdown/removal.
        if hasattr(self, "auto_rebalance") and self.auto_rebalance:
            await self.auto_rebalance.cancel()
        if self.strategy_d.running:
            await self.strategy_d.stop()
        if self.strategy_s.running:
            await self.strategy_s.stop()
        monitor_enabled = bool(
            getattr(getattr(self.poly_monitor, "state", None), "enabled", False)
        )
        if self.poly_monitor.running or monitor_enabled:
            await self.poly_monitor.stop()
        if self.sniper.running:
            await self.sniper.stop()
        if self.sniper_old.running:
            await self.sniper_old.stop()
        # Manual account removal can still cancel, but app/service shutdown must not
        # wipe live exchange orders. Lifespan close_all passes cancel_orders=False.
        if cancel_orders:
            try:
                await self.client.cancel_all()
            except Exception:
                pass
        elif sniper_was_running:
            snapshot = self.sniper.get_state()
            snapshot["running"] = True
            self._persist_sniper_snapshot(snapshot)
        await self.client.close()

    def install_client(self, client: OKXOutcomesClient) -> OKXOutcomesClient:
        """Atomically redirect every strategy reference to a prepared client."""
        previous = self.client
        self.client = client
        self.strategy_d.client = client
        self.strategy_s.client = client
        self.poly_monitor.client = client
        self.auto_rebalance.client = client
        self.sniper.client = client
        self.sniper_old.client = client
        return previous

    async def reload_client(self):
        """Build and install a fresh client before closing the previous one."""
        acc = get_account_decrypted(self.account_id)
        if not acc:
            raise RuntimeError("Account not found")
        if hasattr(self, "auto_rebalance") and self.auto_rebalance:
            await self.auto_rebalance.cancel()
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
        previous = self.install_client(replacement)
        if previous and hasattr(previous, "close"):
            try:
                await previous.close()
            except Exception as exc:
                logger.warning("Failed to close previous client for %s: %s", self.account_id, exc)

class AccountManager:
    """Manages all account instances."""

    def __init__(self):
        self.instances: Dict[str, AccountInstance] = {}
        self.notifier: Optional[TelegramNotifier] = None


    async def init(self):
        """Load all accounts from DB and create client instances."""
        # Init notifier from global config
        tg_token = get_global_config("tg_token", "")
        tg_chats_str = get_global_config("tg_chats", "")
        tg_chats = [c.strip() for c in tg_chats_str.split(",") if c.strip()] if tg_chats_str else []
        self.notifier = TelegramNotifier(token=tg_token, chat_ids=tg_chats)

        # Load accounts
        accounts = get_accounts()
        for acc in accounts:
            try:
                await self._load_account(acc["id"])
            except Exception as e:
                logger.error(f"Failed to load account {acc['name']}: {e}")

    async def _load_account(self, account_id: str) -> Optional[AccountInstance]:
        """Load a single account into memory."""
        acc = get_account_decrypted(account_id)
        if not acc:
            return None

        client = OKXOutcomesClient(
            api_key=acc["api_key"],
            secret_key=acc["secret_key"],
            passphrase=acc["passphrase"],
            agent_private_key=acc["agent_private_key"],
            base_url=acc.get("base_url", "https://us.okx.com"),
            proxy_node=acc["proxy_node"],
        )

        instance = AccountInstance(
            account_id=acc["id"],
            name=acc["name"],
            client=client,
            proxy_node=acc["proxy_node"],
            notifier=self.notifier,
        )
        self.instances[acc["id"]] = instance
        saved_sniper = get_sniper_state(acc["id"])
        if saved_sniper:
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
                return instance
            restore_state = dict(saved_sniper.get("runtime") or {})
            restore_state.update(saved_sniper.get("config") or {})
            result = await instance.sniper.restore(
                restore_state,
                resume=bool(saved_sniper.get("running")),
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
                return instance
            instance._persist_sniper_snapshot()
        return instance

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
        if account_id in self.instances:
            await self.instances[account_id].close()
            del self.instances[account_id]
        delete_account(account_id)
        return True

    async def update_account_credentials(self, account_id: str, **kwargs):
        """Update credentials and reload client."""
        update_account(account_id, **kwargs)
        # Reload the instance
        if account_id in self.instances:
            await self.instances[account_id].close()
            del self.instances[account_id]
        await self._load_account(account_id)

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
        for inst in self.instances.values():
            await inst.close(cancel_orders=cancel_orders)
        if self.notifier:
            await self.notifier.close()
