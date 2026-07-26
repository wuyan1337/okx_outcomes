"""Rebuild a monitored strategy after a Polymarket risk trigger.

The rebalance is deliberately fail closed.  It only restarts entry logic after
all open orders are confirmed gone, positions for every traded outcome have
been reconciled, and every tracked position has an executable protective sell.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional

from ..core.price_utils import (
    align_cents_for_okx,
    align_okx_price,
    as_decimal,
    format_okx_cents,
    price_to_cents,
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
COST_LAYERS_FILE = DATA_DIR / "cost_layers_{account_id}.json"
STABILIZE_INTERVAL = 5
STABILIZE_TOLERANCE = Decimal("0.005")
MIN_SELL_NOTIONAL = Decimal("1")
LAYER_EPSILON = 1e-9


class AutoRebalance:
    """Observe a new Poly price, protect inventory, then restart the strategy."""

    def __init__(self, client, account_id: str, account_name: str = ""):
        self.client = client
        self.account_id = account_id
        self.account_name = account_name
        self.notifier = None
        self.enabled = False

        # market_id -> asset_id -> cost_cents -> quantity
        self.cost_layers: Dict[str, Dict[str, Dict[Decimal, float]]] = {}
        # A pre-v2 file had no market/asset identity.  Keep it visible and on
        # disk, but never attach it to a live asset automatically.
        self.legacy_cost_layers: Dict[Decimal, float] = {}
        # market|asset|order -> closed BUY filledSize already considered by the
        # rebalance catcher.  This prevents an old closed partial fill from
        # being re-added on every later trigger.
        self.captured_buy_fills: Dict[str, float] = {}
        self._layers_file = Path(str(COST_LAYERS_FILE).format(account_id=account_id))
        self._load_layers()

        self._rebalancing = False
        self._task: Optional[asyncio.Task] = None

        self.get_strategy_params = None
        self.restart_strategy = None
        self.restart_monitor = None
        self.stop_strategy = None
        self.stop_monitor = None

    @property
    def rebalancing(self) -> bool:
        return self._rebalancing

    # ---- Cost layers -------------------------------------------------

    @staticmethod
    def _decode_price_layers(raw: object) -> Dict[Decimal, float]:
        if not isinstance(raw, dict):
            return {}
        decoded: Dict[Decimal, float] = {}
        for price, quantity in raw.items():
            try:
                quantity_value = float(quantity)
                if quantity_value > LAYER_EPSILON:
                    decoded[as_decimal(price)] = quantity_value
            except (TypeError, ValueError):
                continue
        return decoded

    def _load_layers(self):
        # Loading is fail-closed: only an explicit JSON boolean can enable it.
        self.enabled = False
        try:
            if not self._layers_file.exists():
                return
            with open(self._layers_file, "r", encoding="utf-8") as handle:
                data = json.load(handle)

            if isinstance(data, dict) and data.get("version") == 2:
                saved_enabled = data.get("enabled", False)
                if isinstance(saved_enabled, bool):
                    self.enabled = saved_enabled
                else:
                    self.enabled = False
                    logger.warning(
                        "[Rebalance:%s] Ignoring non-boolean enabled value",
                        self.account_id,
                    )
                loaded: Dict[str, Dict[str, Dict[Decimal, float]]] = {}
                raw_markets = data.get("layers", {})
                if isinstance(raw_markets, dict):
                    for market_id, raw_assets in raw_markets.items():
                        if not isinstance(raw_assets, dict):
                            continue
                        for asset_id, raw_layers in raw_assets.items():
                            layers = self._decode_price_layers(raw_layers)
                            if layers:
                                loaded.setdefault(str(market_id), {})[str(asset_id)] = layers
                self.cost_layers = loaded
                self.legacy_cost_layers = self._decode_price_layers(
                    data.get("legacy_unassigned", {})
                )
                raw_captured = data.get("captured_buy_fills", {})
                if isinstance(raw_captured, dict):
                    for key, value in raw_captured.items():
                        try:
                            captured = float(value)
                        except (TypeError, ValueError):
                            continue
                        if captured > LAYER_EPSILON:
                            self.captured_buy_fills[str(key)] = captured
            elif isinstance(data, dict):
                # v1 was a flat {price: quantity} map.  There is no safe way to
                # infer which market/outcome owns it.
                self.cost_layers = {}
                self.legacy_cost_layers = self._decode_price_layers(data)
                if self.legacy_cost_layers:
                    logger.warning(
                        "[Rebalance:%s] Legacy flat cost layers were isolated; "
                        "they will not be applied to a market or asset",
                        self.account_id,
                    )

            scoped_count = sum(
                len(layers)
                for assets in self.cost_layers.values()
                for layers in assets.values()
            )
            logger.info(
                "[Rebalance:%s] Loaded %s scoped cost layers",
                self.account_id,
                scoped_count,
            )
        except Exception as exc:
            logger.warning(
                "[Rebalance:%s] Failed to load rebalance state: %s",
                self.account_id,
                exc,
            )

    def _save_layers(self) -> bool:
        temp_file = None
        try:
            clean_layers = {}
            for market_id, assets in self.cost_layers.items():
                clean_assets = {}
                for asset_id, layers in assets.items():
                    clean = {
                        format(price_cents, "f"): quantity
                        for price_cents, quantity in layers.items()
                        if quantity > LAYER_EPSILON
                    }
                    if clean:
                        clean_assets[asset_id] = clean
                if clean_assets:
                    clean_layers[market_id] = clean_assets

            payload = {
                "version": 2,
                "enabled": self.enabled,
                "layers": clean_layers,
                "legacy_unassigned": {
                    format(price_cents, "f"): quantity
                    for price_cents, quantity in self.legacy_cost_layers.items()
                    if quantity > LAYER_EPSILON
                },
                "captured_buy_fills": {
                    key: quantity
                    for key, quantity in self.captured_buy_fills.items()
                    if quantity > LAYER_EPSILON
                },
            }
            self._layers_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self._layers_file.with_name(self._layers_file.name + ".tmp")
            with open(temp_file, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
                handle.flush()
            temp_file.replace(self._layers_file)
            return True
        except Exception as exc:
            logger.warning(
                "[Rebalance:%s] Failed to save rebalance state: %s",
                self.account_id,
                exc,
            )
            if temp_file is not None:
                try:
                    temp_file.unlink(missing_ok=True)
                except OSError:
                    pass
            return False

    def set_enabled(self, enabled: bool) -> bool:
        """Persist an account's Auto-Rebalance switch atomically."""
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        previous = self.enabled
        self.enabled = enabled
        if self._save_layers():
            return True
        self.enabled = previous
        return False

    def _layers_for(
        self, asset_id: str, market_id: str = "", *, create: bool = False
    ) -> Dict[Decimal, float]:
        asset_key = str(asset_id or "")
        market_key = str(market_id or "")
        if not asset_key:
            return {}
        if create:
            return self.cost_layers.setdefault(market_key, {}).setdefault(asset_key, {})
        return self.cost_layers.get(market_key, {}).get(asset_key, {})

    def _prune_scope(self, asset_id: str, market_id: str = ""):
        market_key = str(market_id or "")
        asset_key = str(asset_id or "")
        assets = self.cost_layers.get(market_key)
        if not assets:
            return
        if not assets.get(asset_key):
            assets.pop(asset_key, None)
        if not assets:
            self.cost_layers.pop(market_key, None)

    def add_to_layer(
        self,
        asset_id,
        price_cents=None,
        quantity: float | None = None,
        market_id: str = "",
    ):
        """Record a buy fill under an explicit market and outcome asset.

        The old ``add_to_layer(price, quantity)`` form remains accepted only so
        an in-flight old caller cannot crash.  Such data is quarantined as
        unassigned and is never used to restore a live position.
        """
        if quantity is None:
            legacy_price, legacy_quantity = asset_id, price_cents
            if legacy_quantity is None or float(legacy_quantity) <= 0:
                return
            key = as_decimal(legacy_price)
            self.legacy_cost_layers[key] = (
                self.legacy_cost_layers.get(key, 0.0) + float(legacy_quantity)
            )
            logger.warning(
                "[Rebalance:%s] Legacy unscoped add_to_layer call was quarantined",
                self.account_id,
            )
            self._save_layers()
            return

        quantity_value = float(quantity)
        if quantity_value <= 0 or not str(asset_id or ""):
            return
        layers = self._layers_for(str(asset_id), market_id, create=True)
        key = as_decimal(price_cents)
        layers[key] = layers.get(key, 0.0) + quantity_value
        self._save_layers()

    def remove_from_layer(
        self,
        asset_id,
        price_cents=None,
        quantity: float | None = None,
        market_id: str = "",
    ):
        """Remove a sell fill from one exact cost layer."""
        if quantity is None:
            legacy_price, legacy_quantity = asset_id, price_cents
            if legacy_quantity is None:
                return
            layers = self.legacy_cost_layers
            key = as_decimal(legacy_price)
            amount = float(legacy_quantity)
        else:
            layers = self._layers_for(str(asset_id), market_id)
            key = as_decimal(price_cents)
            amount = float(quantity)
        if key not in layers or amount <= 0:
            return
        layers[key] = max(0.0, layers[key] - amount)
        if layers[key] <= LAYER_EPSILON:
            layers.pop(key, None)
        if quantity is not None:
            self._prune_scope(str(asset_id), market_id)
        self._save_layers()

    def remove_from_highest_layer(
        self, asset_id, quantity: float | None = None, market_id: str = ""
    ):
        """Remove a sell fill from the highest cost of one outcome only."""
        if quantity is None:
            remaining = float(asset_id)
            layers = self.legacy_cost_layers
            logger.warning(
                "[Rebalance:%s] Legacy unscoped remove call only touched quarantined data",
                self.account_id,
            )
            scoped_asset = ""
        else:
            remaining = float(quantity)
            scoped_asset = str(asset_id or "")
            layers = self._layers_for(scoped_asset, market_id)
        if remaining <= 0:
            return
        for price in sorted(list(layers), reverse=True):
            if remaining <= LAYER_EPSILON:
                break
            take = min(layers[price], remaining)
            layers[price] -= take
            remaining -= take
            if layers[price] <= LAYER_EPSILON:
                layers.pop(price, None)
        if scoped_asset:
            self._prune_scope(scoped_asset, market_id)
        self._save_layers()

    def sync_with_positions(
        self, asset_id: str, total_position_size: float, market_id: str = ""
    ):
        """Reduce one asset's layers to its actual position size."""
        layers = self._layers_for(asset_id, market_id)
        layers_total = sum(layers.values())
        if layers_total <= LAYER_EPSILON:
            return
        actual = max(0.0, float(total_position_size))
        if actual <= LAYER_EPSILON:
            layers.clear()
        elif layers_total > actual + LAYER_EPSILON:
            # The source of an untracked position reduction is unknown.  Drop
            # the cheapest inventory first and retain the highest recorded
            # costs, which is the conservative choice for future sell floors.
            reduction = layers_total - actual
            for price in sorted(list(layers)):
                if reduction <= LAYER_EPSILON:
                    break
                take = min(layers[price], reduction)
                layers[price] -= take
                reduction -= take
                if layers[price] <= LAYER_EPSILON:
                    layers.pop(price, None)
        else:
            return
        self._prune_scope(asset_id, market_id)
        self._save_layers()

    @staticmethod
    def _position_asset_id(position: dict) -> str:
        # Position ``tokenId`` is the on-chain token address, not OKX's
        # prediction assetId.  Only use an explicit assetId or derive the
        # documented asset ID from marketId + outcome index.
        value = position.get("assetId")
        if value:
            return str(value)
        market_id = str(position.get("marketId", "") or "")
        if not market_id:
            return ""
        token_index = str(position.get("tokenIndex", "") or "")
        token_name = str(position.get("tokenName", "") or "").upper()
        is_no = token_index == "2" or token_name == "NO"
        return f"{market_id}{'100' if is_no else '000'}"

    @classmethod
    def _position_matches_asset(cls, position: dict, asset_id: str) -> bool:
        return bool(asset_id) and cls._position_asset_id(position) == str(asset_id)

    @staticmethod
    def _order_asset_id(order: dict) -> str:
        return str(order.get("assetId", order.get("tokenId", "")) or "")

    # ---- Strategy metadata ------------------------------------------

    def _params(self) -> dict:
        params = self.get_strategy_params() if self.get_strategy_params else {}
        if not isinstance(params, dict) or not params:
            raise RuntimeError("strategy parameters are unavailable")
        if not params.get("market_id"):
            raise RuntimeError("strategy market_id is unavailable")
        return params

    @staticmethod
    def _active_assets(params: dict) -> list[dict]:
        market_id = str(params.get("market_id", "") or "")
        costs = params.get("asset_cost_cents", {}) or {}
        if params.get("strategy_type") == "manual-d":
            raw_assets = [
                ("YES", str(params.get("yes_asset_id", "") or "")),
                ("NO", str(params.get("no_asset_id", "") or "")),
            ]
        else:
            raw_assets = [
                (str(params.get("side", "") or "").upper() or "OUTCOME",
                 str(params.get("asset_id", "") or ""))
            ]
        assets = []
        seen = set()
        for label, asset_id in raw_assets:
            if not asset_id or asset_id in seen:
                continue
            seen.add(asset_id)
            if asset_id not in costs:
                raise RuntimeError(f"missing recorded buy cost for {label} asset")
            assets.append(
                {
                    "label": label,
                    "market_id": market_id,
                    "asset_id": asset_id,
                    "cost_cents": as_decimal(costs[asset_id]),
                }
            )
        if not assets:
            raise RuntimeError("strategy outcome assets are unavailable")
        if params.get("strategy_type") == "manual-d" and len(assets) != 2:
            raise RuntimeError("Manual-D requires distinct YES and NO assets")
        return assets

    @staticmethod
    def _target_prices(new_reference_cents: Decimal, params: dict) -> dict:
        """Return buy/sell cents and per-asset protective sell targets."""
        reference = as_decimal(new_reference_cents)
        strategy_type = params.get("strategy_type")
        if strategy_type == "manual-d":
            yes_buy = align_cents_for_okx(reference, "buy")
            no_buy = align_cents_for_okx(Decimal("99") - yes_buy, "buy")
            yes_sell = align_cents_for_okx(yes_buy + Decimal("1"), "sell")
            no_sell = align_cents_for_okx(Decimal("100") - yes_buy, "sell")
            return {
                "buy": yes_buy,
                "sell": yes_sell,
                "asset_sells": {
                    str(params.get("yes_asset_id", "")): yes_sell,
                    str(params.get("no_asset_id", "")): no_sell,
                },
                "display": f"YES B{yes_buy}¢/S{yes_sell}¢, NO B{no_buy}¢/S{no_sell}¢",
            }

        old_buy = as_decimal(params.get("price_cents", 0))
        old_sell = as_decimal(params.get("sell_price_cents", old_buy + Decimal("1")))
        spread = old_sell - old_buy
        if spread <= 0:
            raise RuntimeError("strategy sell spread must be positive")
        if params.get("mode", "classic") == "bid-sell":
            sell = align_cents_for_okx(reference, "sell")
            raw_buy = sell - spread
            if raw_buy < Decimal("0.1"):
                raise RuntimeError(
                    f"Bid-Sell buy price {raw_buy}¢ is below OKX minimum 0.1¢"
                )
            buy = align_cents_for_okx(raw_buy, "buy")
        else:
            buy = align_cents_for_okx(reference, "buy")
            sell = align_cents_for_okx(buy + spread, "sell")
        asset_id = str(params.get("asset_id", "") or "")
        return {
            "buy": buy,
            "sell": sell,
            "asset_sells": {asset_id: sell},
            "display": f"B{buy}¢ / S{sell}¢",
        }

    # ---- Rebalance flow ---------------------------------------------

    async def execute(
        self,
        trigger_reason: str,
        poly_token_id: str,
        old_price_cents,
        strategy_type: str,
    ):
        if self._rebalancing:
            logger.warning("[Rebalance:%s] Already rebalancing", self.account_id)
            return self._task
        self._rebalancing = True
        self._task = asyncio.create_task(
            self._do_rebalance(
                trigger_reason,
                poly_token_id,
                as_decimal(old_price_cents),
                strategy_type,
            )
        )
        return self._task

    async def _do_rebalance(
        self,
        trigger_reason: str,
        poly_token_id: str,
        old_price_cents: Decimal,
        strategy_type: str,
    ) -> bool:
        try:
            params = self._params()
            if params.get("strategy_type") != strategy_type:
                raise RuntimeError("strategy context changed during rebalance")
            assets = self._active_assets(params)

            await self._cancel_all_verified()
            await self._capture_partial_fills(params, assets)
            await self._reconcile_with_positions(params=params, assets=assets)

            stabilize_secs = float(params.get("stabilize_secs", 10))
            await self._notify(
                f"🔄 Auto-Rebalance 开始观察 ({stabilize_secs:g}s)\n"
                f"Trigger: {trigger_reason}"
            )
            stable_price = await self._wait_for_stable_price(
                poly_token_id, stabilize_secs=stabilize_secs
            )
            if stable_price is None:
                raise RuntimeError("观察期内价格未稳定")

            new_reference_cents = price_to_cents(
                # The observed value is a Poly bid, so map it down to the
                # executable OKX tick.  Bid-Sell uses that bid level as its
                # SELL reference; it must not ceil the observed market bid.
                align_okx_price(stable_price, "buy")
            )
            if not Decimal("0.1") <= new_reference_cents <= Decimal("99.9"):
                raise RuntimeError(f"新价 {new_reference_cents}¢ 不可执行")

            # This is intentionally performed even when the price is unchanged:
            # the trigger already cancelled the old protective sells.
            await self._sync_positions(params, assets)
            targets = self._target_prices(new_reference_cents, params)
            sells_posted = await self._post_layer_sells(
                params, assets, targets["asset_sells"]
            )
            await self._restart(
                new_reference_cents, poly_token_id, stable_price, strategy_type
            )

            layers_info = self._format_active_layers(params, assets)
            same_price = "（原监控价）" if new_reference_cents == old_price_cents else ""
            await self._notify(
                "✅ Auto-Rebalance 完成\n"
                f"新价{same_price}: {targets['display']}\n"
                f"存量 sells: {sells_posted} 笔\n"
                f"持仓层: {layers_info or '无'}"
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "[Rebalance:%s] Failed: %s", self.account_id, exc, exc_info=True
            )
            await self._notify(f"❌ Auto-Rebalance 失败: {exc}\n未重启，需手动处理")
            return False
        finally:
            self._rebalancing = False

    async def _cancel_all_verified(self) -> dict:
        """Batch-cancel leftovers and require a query proving zero open orders.

        The monitor already used the rate-limited cancelAll endpoint at trigger
        time.  Rebalance must not issue a second cancelAll call.
        """
        total_seen = 0
        for attempt in range(3):
            response = await self.client.get_orders(status="open")
            if str(response.get("code", "")) != "0":
                raise RuntimeError(
                    "无法确认开放订单: "
                    + str(response.get("message", response.get("msg", response.get("code", "unknown"))))
                )
            orders = response.get("data", {}).get("list", [])
            if not isinstance(orders, list):
                raise RuntimeError("开放订单响应格式无效")
            total_seen = max(total_seen, len(orders))
            if not orders:
                return {"ok": True, "cancelled": total_seen, "remaining": 0}

            cancels = []
            for order in orders:
                order_id = order.get("oid", order.get("id", ""))
                asset_id = self._order_asset_id(order)
                if order_id and asset_id:
                    cancels.append({"assetId": asset_id, "oid": order_id})
            if not cancels:
                raise RuntimeError(f"仍有 {len(orders)} 笔订单但缺少撤单标识")
            batch = await self.client.cancel_orders_batch(cancels)
            if str(batch.get("code", "")) != "0":
                logger.warning("[Rebalance] batch cancellation response: %s", batch)
                async def _cancel_one(cancel):
                    try:
                        result = await self.client.cancel_order(
                            asset_id=cancel["assetId"], order_id=cancel["oid"]
                        )
                        return str(result.get("code", "")) == "0"
                    except Exception:
                        return False

                outcomes = await asyncio.gather(
                    *[_cancel_one(cancel) for cancel in cancels]
                )
                if not all(outcomes):
                    logger.warning(
                        "[Rebalance] %s individual cancellations were not accepted",
                        sum(1 for outcome in outcomes if not outcome),
                    )
            if attempt < 2:
                await asyncio.sleep(0.5)

        # Verify once more after the final batch request.  Never treat the
        # request acknowledgement itself as proof of cancellation.
        await asyncio.sleep(0.5)
        response = await self.client.get_orders(status="open")
        if str(response.get("code", "")) != "0":
            raise RuntimeError("最终开放订单核验失败")
        orders = response.get("data", {}).get("list", [])
        if not isinstance(orders, list):
            raise RuntimeError("最终开放订单响应格式无效")
        if not orders:
            return {"ok": True, "cancelled": total_seen, "remaining": 0}
        raise RuntimeError(f"撤单后仍有 {len(orders)} 笔开放订单")

    async def _capture_partial_fills(self, params=None, assets=None):
        params = params or self._params()
        assets = assets or self._active_assets(params)
        market_id = str(params.get("market_id", ""))
        response = await self.client.get_orders(status="closed", market_id=market_id)
        if str(response.get("code", "")) != "0":
            raise RuntimeError("无法查询已关闭订单以核对部分成交")
        orders = response.get("data", {}).get("list", [])
        if not isinstance(orders, list):
            raise RuntimeError("已关闭订单响应格式无效")

        assets_by_id = {item["asset_id"]: item for item in assets}
        # Only the position amount not already represented by layers may be
        # captured.  Normal Manual-S/D fill handlers record the same fills, so
        # blindly adding every closed partial order would duplicate them.
        position_sizes = await self._fetch_position_sizes(params, assets)
        untracked = {
            item["asset_id"]: max(
                0.0,
                position_sizes[item["asset_id"]]
                - sum(
                    self._layers_for(item["asset_id"], item["market_id"]).values()
                ),
            )
            for item in assets
        }
        strategy_start = int(params.get("start_time", 0) or 0)
        for order in orders:
            if str(order.get("side", "")).upper() != "BUY":
                continue
            asset_id = self._order_asset_id(order)
            if asset_id not in assets_by_id:
                continue
            created_at = int(order.get("createdAt", "0") or "0")
            if strategy_start and created_at < strategy_start:
                continue
            status = str(order.get("status", "")).upper()
            filled = float(order.get("filledSize", "0") or "0")
            if status not in {"FILLED", "PARTIALLY_FILLED", "CANCELLED"} or filled <= LAYER_EPSILON:
                continue
            order_id = str(
                order.get("oid", order.get("id", order.get("clientOrderId", "")))
                or ""
            )
            if not order_id:
                raise RuntimeError("部分成交订单缺少订单标识")
            capture_key = f"{market_id}|{asset_id}|{order_id}"
            previously_seen = self.captured_buy_fills.get(capture_key, 0.0)
            incremental = max(0.0, filled - previously_seen)
            amount_to_add = min(incremental, untracked[asset_id])
            order_price = order.get("price")
            if order_price in (None, ""):
                raise RuntimeError("部分成交订单缺少价格")
            cost_cents = price_to_cents(as_decimal(order_price))
            # Set this before add_to_layer, whose atomic save will then persist
            # the layer and its idempotency watermark together.
            self.captured_buy_fills[capture_key] = max(previously_seen, filled)
            if amount_to_add > LAYER_EPSILON:
                self.add_to_layer(
                    asset_id, cost_cents, amount_to_add, market_id=market_id
                )
                untracked[asset_id] -= amount_to_add
                logger.info(
                    "[Rebalance] Captured %s partial fill: %s @ %s¢",
                    assets_by_id[asset_id]["label"],
                    amount_to_add,
                    cost_cents,
                )
        self._save_layers()

    async def _fetch_position_sizes(self, params: dict, assets: list[dict]) -> dict:
        market_id = str(params.get("market_id", ""))
        response = await self.client.get_positions(status="open", market_id=market_id)
        if str(response.get("code", "")) != "0":
            raise RuntimeError(
                "无法查询持仓: "
                + str(response.get("message", response.get("msg", response.get("code", "unknown"))))
            )
        raw = response.get("data", {})
        positions = raw.get("list", []) if isinstance(raw, dict) else []
        if not isinstance(positions, list):
            raise RuntimeError("持仓响应格式无效")
        sizes = {item["asset_id"]: 0.0 for item in assets}
        assets_by_label = {item["label"]: item["asset_id"] for item in assets}
        for position in positions:
            asset_id = self._position_asset_id(position)
            if asset_id not in sizes and not position.get("assetId"):
                token_index = str(position.get("tokenIndex", "") or "")
                token_name = str(position.get("tokenName", "") or "").upper()
                label = "NO" if token_index == "2" or token_name == "NO" else "YES"
                asset_id = assets_by_label.get(label, asset_id)
            if asset_id in sizes:
                sizes[asset_id] += float(
                    position.get("size", position.get("pos", "0")) or "0"
                )
        return sizes

    async def _reconcile_with_positions(self, old_price_cents=None, params=None, assets=None):
        """Attach orphaned YES/NO inventory to each asset's real buy cost."""
        params = params or self._params()
        assets = assets or self._active_assets(params)
        sizes = await self._fetch_position_sizes(params, assets)
        for item in assets:
            layers = self._layers_for(item["asset_id"], item["market_id"])
            gap = sizes[item["asset_id"]] - sum(layers.values())
            if gap > LAYER_EPSILON:
                self.add_to_layer(
                    item["asset_id"],
                    item["cost_cents"],
                    gap,
                    market_id=item["market_id"],
                )
                logger.info(
                    "[Rebalance] Reconciled %s orphan: +%s @ %s¢",
                    item["label"],
                    gap,
                    item["cost_cents"],
                )

    async def _sync_positions(self, params=None, assets=None):
        params = params or self._params()
        assets = assets or self._active_assets(params)
        sizes = await self._fetch_position_sizes(params, assets)
        for item in assets:
            self.sync_with_positions(
                item["asset_id"],
                sizes[item["asset_id"]],
                market_id=item["market_id"],
            )

    async def _wait_for_stable_price(
        self, poly_token_id: str, stabilize_secs: float = 10
    ) -> Optional[Decimal]:
        import aiohttp

        consecutive = 0
        stable_price: Optional[Decimal] = None
        stabilize_count = max(1, int(float(stabilize_secs) // STABILIZE_INTERVAL))
        for _ in range(36):
            await asyncio.sleep(STABILIZE_INTERVAL)
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"https://clob.polymarket.com/book?token_id={poly_token_id}"
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=5)
                    ) as response:
                        if response.status != 200:
                            consecutive = 0
                            continue
                        data = await response.json()
                bids = data.get("bids", [])
                if not bids:
                    consecutive = 0
                    continue
                current_bid = max(as_decimal(bid.get("price", "0")) for bid in bids)
            except Exception:
                consecutive = 0
                continue

            if stable_price is None:
                stable_price = current_bid
                consecutive = 1
            elif abs(current_bid - stable_price) <= STABILIZE_TOLERANCE:
                consecutive += 1
            else:
                stable_price = current_bid
                consecutive = 1
            if consecutive >= stabilize_count:
                return stable_price
        return None

    @staticmethod
    def _plan_sell_orders(
        layers: Dict[Decimal, float], requested_sell_cents: Decimal
    ) -> list[tuple[Decimal, float]]:
        """Combine sub-minimum layers without ever selling below their cost."""
        candidates = []
        for cost_cents, quantity in layers.items():
            if quantity <= LAYER_EPSILON:
                continue
            safe_sell = align_cents_for_okx(
                max(as_decimal(cost_cents), requested_sell_cents), "sell"
            )
            candidates.append((safe_sell, float(quantity)))
        candidates.sort(key=lambda item: item[0])

        plans: list[tuple[Decimal, float]] = []
        pending_price: Optional[Decimal] = None
        pending_quantity = 0.0
        for sell_cents, quantity in candidates:
            pending_price = sell_cents if pending_price is None else max(pending_price, sell_cents)
            pending_quantity += quantity
            notional = pending_price / Decimal("100") * as_decimal(pending_quantity)
            if notional >= MIN_SELL_NOTIONAL:
                plans.append((pending_price, pending_quantity))
                pending_price = None
                pending_quantity = 0.0

        if pending_quantity > LAYER_EPSILON:
            if not plans:
                assert pending_price is not None
                # OKX permits a below-minimum reduce-only order when it closes
                # the entire remaining position (sell-all exception).
                plans.append((pending_price, pending_quantity))
                return plans
            last_price, last_quantity = plans.pop()
            merged_price = max(last_price, pending_price or last_price)
            plans.append((merged_price, last_quantity + pending_quantity))
        return plans

    async def _post_layer_sells(self, params, assets=None, target_sells=None) -> int:
        # Compatibility for direct callers of the old private helper:
        # _post_layer_sells(new_sell_cents, strategy_type).
        if not isinstance(params, dict):
            requested = as_decimal(params)
            params = self._params()
            assets = self._active_assets(params)
            target_sells = {item["asset_id"]: requested for item in assets}
        else:
            assets = assets or self._active_assets(params)
            target_sells = target_sells or {}

        posted = 0
        for item in assets:
            asset_id = item["asset_id"]
            if asset_id not in target_sells:
                raise RuntimeError(f"missing sell target for {item['label']} asset")
            layers = self._layers_for(asset_id, item["market_id"])
            plans = self._plan_sell_orders(
                layers, as_decimal(target_sells[asset_id])
            )
            for sell_cents, quantity in plans:
                price_str = format_okx_cents(sell_cents, "sell")
                size_str = format(as_decimal(quantity).normalize(), "f")
                response = await self.client.place_order(
                    asset_id=asset_id,
                    side="sell",
                    price=price_str,
                    size=size_str,
                    tif="gtc",
                    reduce_only=True,
                )
                if str(response.get("code", "")) != "0":
                    message = response.get("message", response.get("msg", response))
                    raise RuntimeError(
                        f"{item['label']} protective sell failed: {message}"
                    )
                posted += 1
                logger.info(
                    "[Rebalance] Posted %s protective sell: %s @ %s¢",
                    item["label"],
                    size_str,
                    sell_cents,
                )
        return posted

    async def _restart(
        self,
        new_price_cents: Decimal,
        poly_token_id: str,
        reference_price_decimal: Decimal,
        strategy_type: str,
    ):
        """Restart both components; any failure aborts completion reporting."""
        if not self.restart_strategy:
            raise RuntimeError("strategy restart callback is unavailable")
        if not self.restart_monitor:
            raise RuntimeError("monitor restart callback is unavailable")
        # Start risk control before entry logic.  If strategy start fails, tear
        # the newly started monitor back down so the account is left stopped.
        try:
            monitor_result = await self.restart_monitor(
                as_decimal(reference_price_decimal), poly_token_id
            )
            if isinstance(monitor_result, dict) and not monitor_result.get("ok", False):
                raise RuntimeError(monitor_result.get("error", "monitor restart failed"))
        except Exception:
            if self.stop_strategy:
                try:
                    await self.stop_strategy()
                except Exception as stop_exc:
                    logger.error(
                        "[Rebalance] Failed to stop strategy after restart error: %s",
                        stop_exc,
                    )
            if self.stop_monitor:
                try:
                    await self.stop_monitor()
                except Exception as stop_exc:
                    logger.error(
                        "[Rebalance] Failed to stop monitor after monitor restart error: %s",
                        stop_exc,
                    )
            raise
        try:
            strategy_result = await self.restart_strategy(
                as_decimal(new_price_cents), strategy_type
            )
            if isinstance(strategy_result, dict) and not strategy_result.get("ok", False):
                raise RuntimeError(strategy_result.get("error", "strategy restart failed"))
        except Exception:
            if self.stop_strategy:
                try:
                    await self.stop_strategy()
                except Exception as stop_exc:
                    logger.error(
                        "[Rebalance] Failed to stop strategy after restart error: %s",
                        stop_exc,
                    )
            if self.stop_monitor:
                try:
                    await self.stop_monitor()
                except Exception as stop_exc:
                    logger.error("[Rebalance] Failed to stop monitor after restart error: %s", stop_exc)
            raise

    def _format_active_layers(self, params: dict, assets: list[dict]) -> str:
        parts = []
        for item in assets:
            layers = self._layers_for(item["asset_id"], params.get("market_id", ""))
            for price_cents, quantity in sorted(layers.items()):
                parts.append(
                    f"{item['label']} {quantity:g}@{format(price_cents, 'f')}¢"
                )
        return ", ".join(parts)

    async def _notify(self, message: str):
        full_message = f"[{self.account_name or self.account_id}] {message}"
        if self.notifier:
            try:
                await self.notifier.send(full_message)
            except Exception as exc:
                logger.warning("[Rebalance] Notify failed: %s", exc)
        logger.info("[Rebalance] %s", message)

    def status(self) -> dict:
        serialized = {
            market_id: {
                asset_id: {
                    format(price_cents, "f"): quantity
                    for price_cents, quantity in layers.items()
                }
                for asset_id, layers in assets.items()
            }
            for market_id, assets in self.cost_layers.items()
        }
        return {
            "enabled": self.enabled,
            "rebalancing": self._rebalancing,
            "cost_layers": serialized,
            "legacy_unassigned": {
                format(price_cents, "f"): quantity
                for price_cents, quantity in self.legacy_cost_layers.items()
            },
            "total_held": sum(
                sum(layers.values())
                for assets in self.cost_layers.values()
                for layers in assets.values()
            ),
        }

    async def cancel(self):
        was_rebalancing = self._rebalancing
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._rebalancing = False
        if was_rebalancing:
            if self.stop_strategy:
                try:
                    await self.stop_strategy()
                except Exception as exc:
                    logger.error("[Rebalance] Failed to stop strategy during cancel: %s", exc)
            if self.stop_monitor:
                try:
                    await self.stop_monitor()
                except Exception as exc:
                    logger.error("[Rebalance] Failed to stop monitor during cancel: %s", exc)
