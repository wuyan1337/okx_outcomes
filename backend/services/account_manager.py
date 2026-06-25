"""Account manager: manages OKX client instances and strategies per account."""
import asyncio
import logging
from typing import Dict, Optional

from ..core.client import OKXOutcomesClient
from ..core.database import (
    get_accounts, get_account_decrypted, create_account, update_account,
    delete_account, get_available_proxies, get_used_proxies, PROXY_NODES,
    get_global_config, set_global_config
)
from .manual_strategy_d import ManualStrategyD
from .manual_strategy_s import ManualStrategyS
from .poly_monitor import PolyMonitor
from .auto_rebalance import AutoRebalance
from .poly_sniper import PolySniper
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
        self.strategy_s.auto_rebalance = self.auto_rebalance
        # Sniper
        self.sniper = PolySniper(client, account_id=account_id)
        self.sniper.account_name = name
        self.strategy_d.auto_rebalance = self.auto_rebalance
        if notifier:
            self.strategy_d.notifier = notifier
            self.strategy_s.notifier = notifier
            self.poly_monitor.notifier = notifier
            self.auto_rebalance.notifier = notifier
            self.sniper.notifier = notifier

    def _get_strategy_params(self) -> dict:
        """Return current strategy params for auto-rebalance."""
        if self.strategy_s.state.running or self.strategy_s.state.asset_id:
            return {
                "strategy_type": "manual-s",
                "market_id": self.strategy_s.state.market_id,
                "yes_asset_id": self.strategy_s.state.yes_asset_id,
                "no_asset_id": self.strategy_s.state.no_asset_id,
                "asset_id": self.strategy_s.state.asset_id,
                "side": self.strategy_s.state.side,
                "price_cents": self.strategy_s.state.price_a,
            }
        elif self.strategy_d.state.running or self.strategy_d.state.yes_asset_id:
            return {
                "strategy_type": "manual-d",
                "market_id": self.strategy_d.state.market_id,
                "yes_asset_id": self.strategy_d.state.yes_asset_id,
                "no_asset_id": self.strategy_d.state.no_asset_id,
                "asset_id": self.strategy_d.state.yes_asset_id,
                "side": "yes",
                "price_cents": self.strategy_d.state.price_a,
            }
        return {}

    async def _restart_strategy(self, new_price_cents: int, strategy_type: str):
        """Restart strategy at new price (called by auto-rebalance)."""
        params = self._get_strategy_params()
        if not params:
            return

        if strategy_type == "manual-s":
            # Preserve position_pct and spread from running strategy
            # new_price_cents = stable poly bid = new SELL level
            pct = self.strategy_s.state.position_pct if self.strategy_s.state.position_pct else 100
            old_spread = self.strategy_s.state.sell_price - self.strategy_s.state.price_a if self.strategy_s.state.sell_price > 0 else 1
            new_sell = new_price_cents  # poly stable bid = sell target
            new_buy = new_price_cents - old_spread
            if new_buy < 4:
                new_buy = 4
                new_sell = new_buy + old_spread
            await self.strategy_s.start(
                market_id=params["market_id"],
                yes_asset_id=params["yes_asset_id"],
                no_asset_id=params["no_asset_id"],
                side=params["side"],
                price_a=float(new_buy),
                sell_price=float(new_sell),
                position_pct=pct,
            )
        elif strategy_type == "manual-d":
            pct = self.strategy_d.state.position_pct if self.strategy_d.state.position_pct else 100
            await self.strategy_d.start(
                market_id=params["market_id"],
                yes_asset_id=params["yes_asset_id"],
                no_asset_id=params["no_asset_id"],
                price_a=float(new_price_cents),
                position_pct=pct,
            )

    async def _restart_monitor(self, reference_price: float, poly_token_id: str):
        """Restart monitor at new reference price."""
        # Reset triggered state
        self.poly_monitor.state.triggered = False
        self.poly_monitor.state.trigger_reason = ""
        self.poly_monitor._running = False
        self.poly_monitor.state.enabled = False
        # Get strategy context for the monitor
        params = self._get_strategy_params()
        strategy_type = params.get("strategy_type", "manual-s") if params else "manual-s"
        price_cents = params.get("price_cents", round(reference_price * 100)) if params else round(reference_price * 100)

        result = await self.poly_monitor.start(
            poly_token_id=poly_token_id,
            reference_price=reference_price,
        )
        if result.get("ok"):
            self.poly_monitor.set_strategy_context(strategy_type, price_cents, params.get("asset_id", "") if params else "")

    async def _stop_all_strategies(self):
        """Stop all running strategies (called by monitor on trigger)."""
        if self.strategy_d.running:
            await self.strategy_d.stop()
        if self.strategy_s.running:
            await self.strategy_s.stop()
        # Clear DB poly state so restart doesn't resurrect stopped strategies
        try:
            from ..core.database import set_global_config
            import json
            set_global_config(f"poly_state_{self.account_id}", json.dumps({"running": False}))
        except Exception:
            pass

    async def close(self):
        if self.strategy_d.running:
            await self.strategy_d.stop()
        if self.strategy_s.running:
            await self.strategy_s.stop()
        if self.poly_monitor.running:
            await self.poly_monitor.stop()
        if self.sniper.running:
            await self.sniper.stop()
        # Cancel ALL open orders on shutdown to prevent orphaned positions
        try:
            await self.client.cancel_all()
        except Exception:
            pass
        await self.client.close()



    async def reload_client(self):
        """Reload OKX client with fresh credentials from DB."""
        from ..core.database import get_account_decrypted, PROXY_NODES
        acc = get_account_decrypted(self.account_id)
        if not acc:
            return
        proxy_node = acc.get("proxy_node", "JP2")
        from ..core.client import OKXOutcomesClient
        if self.client and hasattr(self.client, 'close'):
            try:
                await self.client.close()
            except Exception:
                pass
        self.client = OKXOutcomesClient(
            api_key=acc["api_key"],
            secret_key=acc["secret_key"],
            passphrase=acc["passphrase"],
            agent_private_key=acc["agent_private_key"],
            base_url=acc.get("base_url", "https://us.okx.com"),
            proxy_node=proxy_node,
        )
        # Propagate to strategies
        if hasattr(self, 'strategy_d'):
            self.strategy_d.client = self.client
        if hasattr(self, 'strategy_s'):
            self.strategy_s.client = self.client
        if hasattr(self, 'poly_monitor'):
            self.poly_monitor.client = self.client
        if hasattr(self, 'sniper') and self.sniper:
            self.sniper.client = self.client

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
                    total_positions += len([p for p in pos_l if float(p.get("pos", "0")) != 0])
            except Exception:
                pass

            if inst.strategy_d.running or inst.strategy_s.running:
                running_count += 1

        return {
            "total_balance": f"{total_balance:.2f}",
            "total_available": f"{total_available:.2f}",
            "total_orders": total_orders,
            "total_positions": total_positions,
            "running_accounts": running_count,
            "total_accounts": len(self.instances),
        }

    async def close_all(self):
        for inst in self.instances.values():
            await inst.close()
        if self.notifier:
            await self.notifier.close()
