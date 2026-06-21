"""OKX Outcomes REST API client with proxy routing support."""
import json
import asyncio
import logging
from typing import Optional

import aiohttp
from aiohttp_socks import ProxyConnector

from .auth import OKXAuth, OKXSigner
from .database import PROXY_NODES

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=5)


class OKXOutcomesClient:
    """REST client for OKX Outcomes prediction market with optional SOCKS5 proxy."""

    def __init__(self, api_key: str, secret_key: str, passphrase: str,
                 agent_private_key: str, base_url: str = "https://us.okx.com",
                 proxy_node: str = None):
        self.base_url = base_url
        self.auth = OKXAuth(api_key, secret_key, passphrase)
        self.signer = OKXSigner(agent_private_key)
        self._session: Optional[aiohttp.ClientSession] = None
        self._proxy_node = proxy_node
        self.last_heartbeat: Optional[str] = None
        # Region for clientOrderId: 0=CN(www.okx.com), 1=AU(us.okx.com)
        self._region = 1 if 'us.okx.com' in base_url else 0

    def _get_connector(self):
        """Create a SOCKS5 connector if proxy_node is configured."""
        if not self._proxy_node or self._proxy_node not in PROXY_NODES:
            return None
        node = PROXY_NODES[self._proxy_node]
        if node["host"] is None:
            return None  # JP2 = direct
        proxy_url = f"socks5://{node['host']}:{node['port']}"
        return ProxyConnector.from_url(proxy_url)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = self._get_connector()
            self._session = aiohttp.ClientSession(
                timeout=REQUEST_TIMEOUT,
                connector=connector
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ─── Read APIs ───

    async def get_events(self, status: str = "active", page_size: int = 50,
                         tag: str = None, cursor: str = None) -> dict:
        params = {"status": status, "pageSize": str(page_size)}
        if tag:
            params["tag"] = tag
        if cursor:
            params["cursor"] = cursor
        return await self._get("/api/v5/predictions/events", params)

    async def get_event(self, event_id: str) -> dict:
        return await self._get(f"/api/v5/predictions/events/{event_id}")

    async def get_event_markets(self, event_id: str) -> dict:
        return await self._get(f"/api/v5/predictions/events/{event_id}/markets")

    async def get_market(self, market_id: str) -> dict:
        return await self._get(f"/api/v5/predictions/markets/{market_id}")

    async def search_events(self, keyword: str, page_size: int = 20) -> dict:
        return await self._get("/api/v5/predictions/events/search",
                               {"keyword": keyword, "pageSize": str(page_size)})

    async def get_orderbook(self, yes_asset_id: str, depth: int = 5) -> dict:
        return await self._get("/api/v5/market/pm-books",
                               {"instId": yes_asset_id, "sz": str(depth)})

    async def get_ticker(self, yes_asset_id: str) -> dict:
        return await self._get("/api/v5/market/ticker", {"instId": yes_asset_id})

    async def get_balance(self) -> dict:
        return await self._get("/api/v5/predictions/balance")

    async def get_positions(self, status: str = "open", market_id: str = None) -> dict:
        params = {"status": status}
        if market_id:
            params["marketId"] = market_id
        return await self._get("/api/v5/predictions/positions", params)

    async def get_orders(self, status: str = "open", market_id: str = None, page_size: int = 20) -> dict:
        """Fetch orders with automatic pagination (OKX max 20/page)."""
        all_orders = []
        cursor = None
        for _ in range(50):  # Safety: max 1000 orders
            params = {"status": status, "pageSize": str(page_size)}
            if market_id:
                params["marketId"] = market_id
            if cursor:
                params["cursor"] = cursor
            resp = await self._get("/api/v5/predictions/orders", params)
            if str(resp.get("code", "")) != "0":
                # Return error as-is on first page, or partial results on later pages
                if not all_orders:
                    return resp
                break
            data = resp.get("data", {})
            page = data.get("list", [])
            all_orders.extend(page)
            if not data.get("hasNext"):
                break
            cursor = data.get("nextCursor", "")
            if not cursor:
                break
        return {"code": "0", "data": {"list": all_orders}}

    async def get_order(self, order_id: str) -> dict:
        return await self._get(f"/api/v5/predictions/orders/{order_id}")

    # ─── Write APIs ───

    async def place_order(self, asset_id: str, side: str, price: str, size: str,
                          tif: str = "gtc", reduce_only: bool = False) -> dict:
        nonce = self.signer.now_millis()
        client_order_id = self.signer.generate_client_order_id(region=self._region)

        order = {
            "assetId": asset_id,
            "side": side,
            "marketType": "prediction",
            "clientOrderId": client_order_id,
            "price": price,
            "reduceOnly": reduce_only,
            "size": size,
            "orderType": {"limit": {"tif": tif}},
        }

        signature = self.signer.sign_place_order([order], nonce)

        body = {
            "action": {
                "type": "placeOrder",
                "grouping": "na",
                "orders": [order],
            },
            "nonce": nonce,
            "signature": {"Ecdsa": signature},
        }
        return await self._post("/api/v5/predictions/orders", body)

    async def cancel_order(self, asset_id: str, order_id: str = None,
                           client_order_id: str = None) -> dict:
        nonce = self.signer.now_millis()

        cancel = {
            "assetId": asset_id,
            "marketType": "prediction",
        }
        if order_id:
            cancel["oid"] = order_id
        elif client_order_id:
            cancel["clientOrderId"] = client_order_id
        else:
            raise ValueError("Must provide order_id or client_order_id")

        signature = self.signer.sign_cancel([cancel], nonce)

        body = {
            "action": {
                "type": "cancel",
                "cancels": [cancel],
            },
            "nonce": nonce,
            "signature": {"Ecdsa": signature},
        }
        return await self._post("/api/v5/predictions/orders/cancel", body)

    async def cancel_orders_batch(self, cancels: list) -> dict:
        nonce = self.signer.now_millis()

        cancel_items = []
        for c in cancels:
            item = {"assetId": c["assetId"], "marketType": "prediction"}
            if "oid" in c:
                item["oid"] = c["oid"]
            elif "clientOrderId" in c:
                item["clientOrderId"] = c["clientOrderId"]
            cancel_items.append(item)

        signature = self.signer.sign_cancel(cancel_items, nonce)

        body = {
            "action": {
                "type": "cancel",
                "cancels": cancel_items,
            },
            "nonce": nonce,
            "signature": {"Ecdsa": signature},
        }
        return await self._post("/api/v5/predictions/orders/cancel", body)

    async def cancel_all(self, asset_ids: list = None) -> dict:
        nonce = self.signer.now_millis()
        expires_after = nonce + 60000

        ids = asset_ids or []
        signature = self.signer.sign_cancel_all(ids, nonce, expires_after)

        body = {
            "action": {
                "type": "cancelAll",
                "assetIds": ids,
                "marketType": "prediction",
            },
            "nonce": nonce,
            "expiresAfter": expires_after,
            "signature": {"Ecdsa": signature},
        }
        return await self._post("/api/v5/predictions/orders/cancel-all", body)

    async def merge(self, market_id: str, size: str) -> dict:
        nonce = self.signer.now_millis()
        signature = self.signer.sign_merge(market_id, size, nonce)

        body = {
            "action": {
                "type": "predictionMerge",
                "marketId": market_id,
                "size": size,
            },
            "nonce": nonce,
            "signature": {"Ecdsa": signature},
        }
        return await self._post("/api/v5/predictions/positions/merge", body)

    async def heartbeat(self) -> dict:
        """Send heartbeat with retry (3 attempts, 2s apart)."""
        import asyncio as _asyncio
        for attempt in range(3):
            nonce = self.signer.now_millis()
            expires_after = nonce + 300000

            signature = self.signer.sign_cancel_all([], nonce, expires_after)

            body = {
                "action": {
                    "type": "cancelAll",
                    "assetIds": [],
                    "marketType": "prediction",
                },
                "nonce": nonce,
                "expiresAfter": expires_after,
                "signature": {"Ecdsa": signature},
            }
            try:
                result = await self._post("/api/v5/predictions/heartbeat", body)
                if isinstance(result, dict) and str(result.get("code", "")) == "0":
                    from datetime import datetime, timezone
                    self.last_heartbeat = datetime.now(timezone.utc).isoformat()
                    return result
                # Non-zero code, retry
                if attempt < 2:
                    await _asyncio.sleep(2)
            except Exception:
                if attempt < 2:
                    await _asyncio.sleep(2)
                else:
                    raise
        return result

    # ─── Internal ───

    async def _get(self, path: str, params: dict = None) -> dict:
        query = ""
        if params:
            from urllib.parse import urlencode as _urlencode; query = "?" + _urlencode(params)
        full_path = path + query
        headers = self.auth.headers("GET", full_path)

        for attempt in range(2):
            session = await self._get_session()
            try:
                async with session.get(self.base_url + full_path, headers=headers) as resp:
                    data = await resp.json()
                    return data
            except asyncio.TimeoutError:
                logger.error(f"GET {path} timed out")
                return {"code": "TIMEOUT", "msg": "Request timed out"}
            except (aiohttp.ClientError, OSError) as e:
                # Connection error — close stale session and retry once
                if attempt == 0:
                    logger.warning(f"GET {path} connection error, rebuilding session: {e}")
                    await self.close()
                    continue
                logger.error(f"GET {path} failed after retry: {e}")
                return {"code": "ERROR", "msg": str(e)}
            except Exception as e:
                logger.error(f"GET {path} failed: {e}")
                return {"code": "ERROR", "msg": str(e)}
        return {"code": "ERROR", "msg": "Unexpected"}

    async def _post(self, path: str, body: dict) -> dict:
        body_str = json.dumps(body, separators=(",", ":"))
        headers = self.auth.headers("POST", path, body_str)

        for attempt in range(2):
            session = await self._get_session()
            try:
                async with session.post(
                    self.base_url + path, headers=headers, data=body_str
                ) as resp:
                    data = await resp.json()
                    return data
            except asyncio.TimeoutError:
                logger.error(f"POST {path} timed out")
                return {"code": "TIMEOUT", "msg": "Request timed out"}
            except (aiohttp.ClientError, OSError) as e:
                if attempt == 0:
                    logger.warning(f"POST {path} connection error, rebuilding session: {e}")
                    await self.close()
                    continue
                logger.error(f"POST {path} failed after retry: {e}")
                return {"code": "ERROR", "msg": str(e)}
            except Exception as e:
                logger.error(f"POST {path} failed: {e}")
                return {"code": "ERROR", "msg": str(e)}
        return {"code": "ERROR", "msg": "Unexpected"}
