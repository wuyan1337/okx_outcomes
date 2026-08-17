"""OKX Outcomes REST API client with proxy routing support."""
import json
import asyncio
import logging
import re
import secrets
import time
import traceback
from typing import Optional

import aiohttp
from aiohttp_socks import ProxyConnector

from .auth import OKXAuth, OKXSigner
from .logging_config import audit_event
from .proxy_nodes import require_proxy_node

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=5)
_LOG_MAX_TEXT = 512
_LOG_MAX_PARAM_KEYS = 32
_LOG_MAX_ACTION_ITEMS = 50
_QUERY_IN_TEXT_RE = re.compile(r"\?[^\s'\"<>]+")


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
        require_proxy_node(self._proxy_node)
        self.last_heartbeat: Optional[str] = None
        # Region for clientOrderId: 0=CN(www.okx.com), 1=AU(us.okx.com)
        self._region = 1 if 'us.okx.com' in base_url else 0

    def _get_connector(self):
        """Create a SOCKS5 connector if proxy_node is configured."""
        node = require_proxy_node(self._proxy_node)
        if node is None:
            return None
        if node["host"] is None:
            return None
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

    def _log_context(self) -> dict:
        return {
            "account_id": str(getattr(self, "_account_id", "") or ""),
            "base_url": self._bounded_text(self.base_url),
            "region": "AU" if self._region == 1 else "CN",
            "proxy_node": str(self._proxy_node or "Direct"),
        }

    @staticmethod
    def _elapsed_ms(started_ns: int) -> float:
        return round((time.perf_counter_ns() - started_ns) / 1_000_000, 3)

    @staticmethod
    def _bounded_text(value, limit: int = _LOG_MAX_TEXT) -> str:
        text = str(value or "")
        return text if len(text) <= limit else text[:limit] + "…"

    @classmethod
    def _safe_message(cls, value, limit: int = _LOG_MAX_TEXT) -> str:
        """Bound diagnostic text and remove any embedded query values."""
        without_queries = _QUERY_IN_TEXT_RE.sub("?<query-redacted>", str(value or ""))
        return cls._bounded_text(without_queries, limit)

    @classmethod
    def _safe_error_summary(cls, error: BaseException) -> dict:
        frames = traceback.extract_tb(error.__traceback__)[-12:]
        return {
            "type": type(error).__name__,
            "message": cls._safe_message(error),
            "stack": [
                {
                    "file": cls._bounded_text(frame.filename, 256),
                    "line": frame.lineno,
                    "function": cls._bounded_text(frame.name, 128),
                }
                for frame in frames
            ],
        }

    @classmethod
    def _safe_param_summary(cls, params: Optional[dict]) -> dict:
        """Describe a query without storing its values or unbounded keys."""
        if not isinstance(params, dict):
            return {"keys": [], "count": 0, "truncated": False}
        keys = list(params.keys())
        return {
            "keys": [cls._bounded_text(key, 64) for key in keys[:_LOG_MAX_PARAM_KEYS]],
            "count": len(keys),
            "truncated": len(keys) > _LOG_MAX_PARAM_KEYS
            or any(len(str(key)) > 64 for key in keys[:_LOG_MAX_PARAM_KEYS]),
        }

    @staticmethod
    def _response_metadata(response) -> dict:
        headers = getattr(response, "headers", {}) or {}
        content_type = getattr(response, "content_type", "") or ""
        return {
            "http_status": getattr(response, "status", None),
            "content_type": str(content_type)[:128],
            "content_length": str(headers.get("Content-Length", ""))[:32]
            if hasattr(headers, "get")
            else "",
        }

    @staticmethod
    def _safe_action_summary(body: dict) -> dict:
        """Keep order diagnostics while excluding signatures and credentials."""
        action = body.get("action") if isinstance(body, dict) else {}
        action = action if isinstance(action, dict) else {}
        summary = {
            "type": OKXOutcomesClient._bounded_text(action.get("type", ""), 64),
            "grouping": OKXOutcomesClient._bounded_text(
                action.get("grouping", ""), 64
            ),
            "nonce": body.get("nonce"),
            "expires_after": body.get("expiresAfter"),
        }
        for key in ("assetIds", "marketId", "size"):
            if key in action:
                value = action[key]
                if isinstance(value, list):
                    summary[key] = [
                        OKXOutcomesClient._bounded_text(item, 160)
                        for item in value[:_LOG_MAX_ACTION_ITEMS]
                    ]
                    summary[f"{key}_count"] = len(value)
                    summary[f"{key}_truncated"] = len(value) > _LOG_MAX_ACTION_ITEMS
                else:
                    summary[key] = OKXOutcomesClient._bounded_text(value, 160)
        if isinstance(action.get("orders"), list):
            summary["orders"] = [
                {
                    key: (
                        order.get(key)
                        if isinstance(order.get(key), (bool, int, float))
                        else OKXOutcomesClient._bounded_text(order.get(key), 160)
                    )
                    for key in (
                        "assetId", "side", "price", "size", "sizeType",
                        "clientOrderId", "reduceOnly", "orderType",
                    )
                    if key in order
                }
                for order in action["orders"][:_LOG_MAX_ACTION_ITEMS]
                if isinstance(order, dict)
            ]
            summary["orders_count"] = len(action["orders"])
            summary["orders_truncated"] = (
                len(action["orders"]) > _LOG_MAX_ACTION_ITEMS
            )
        if isinstance(action.get("cancels"), list):
            summary["cancels"] = [
                {
                    key: OKXOutcomesClient._bounded_text(cancel.get(key), 160)
                    for key in ("assetId", "oid", "clientOrderId")
                    if key in cancel
                }
                for cancel in action["cancels"][:_LOG_MAX_ACTION_ITEMS]
                if isinstance(cancel, dict)
            ]
            summary["cancels_count"] = len(action["cancels"])
            summary["cancels_truncated"] = (
                len(action["cancels"]) > _LOG_MAX_ACTION_ITEMS
            )
        return summary

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

    async def get_positions(self, status: str = "open", market_id: str = None,
                            page_size: int = 100) -> dict:
        """Fetch the complete position list using the documented cursor API."""
        all_positions = []
        cursor = None
        seen_cursors = set()
        for _ in range(100):
            params = {
                "status": status,
                "limit": str(min(max(int(page_size), 1), 100)),
            }
            if market_id:
                params["marketId"] = market_id
            if cursor:
                params["cursor"] = cursor
            response = await self._get("/api/v5/predictions/positions", params)
            if str(response.get("code", "")) != "0":
                return response
            data = response.get("data", {})
            if (
                not isinstance(data, dict)
                or "list" not in data
                or not isinstance(data.get("list"), list)
            ):
                return {"code": "INVALID_RESPONSE", "msg": "Invalid positions response"}
            all_positions.extend(data["list"])
            if not data.get("hasNext"):
                return {"code": "0", "data": {"list": all_positions}}
            next_cursor = str(data.get("nextCursor", "") or "")
            if not next_cursor or next_cursor in seen_cursors:
                return {
                    "code": "PAGINATION_ERROR",
                    "msg": "Positions pagination cursor missing or repeated",
                }
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return {"code": "PAGINATION_ERROR", "msg": "Positions pagination limit exceeded"}

    async def get_orders(self, status: str = "open", market_id: str = None, page_size: int = 20) -> dict:
        """Fetch orders with automatic pagination (OKX max 50/page)."""
        all_orders = []
        cursor = None
        for _ in range(50):  # Bound pagination even if the API repeats a cursor.
            params = {"status": status, "limit": str(min(max(page_size, 1), 50))}
            if market_id:
                params["marketId"] = market_id
            if cursor:
                params["cursor"] = cursor
            resp = await self._get("/api/v5/predictions/orders", params)
            if str(resp.get("code", "")) != "0":
                # Partial pagination is not a successful snapshot.  Callers
                # such as risk-control cancellation must fail closed.
                return resp
            data = resp.get("data", {})
            if (
                not isinstance(data, dict)
                or "list" not in data
                or not isinstance(data.get("list"), list)
            ):
                return {"code": "INVALID_RESPONSE", "msg": "Invalid orders response"}
            page = data["list"]
            all_orders.extend(page)
            if not data.get("hasNext"):
                return {"code": "0", "data": {"list": all_orders}}
            next_cursor = str(data.get("nextCursor", "") or "")
            if not next_cursor or next_cursor == cursor:
                return {
                    "code": "PAGINATION_ERROR",
                    "msg": "Orders pagination cursor missing or repeated",
                }
            cursor = next_cursor
        return {"code": "PAGINATION_ERROR", "msg": "Orders pagination limit exceeded"}

    async def get_order(self, order_id: str) -> dict:
        return await self._get(f"/api/v5/predictions/orders/{order_id}")

    # ─── Write APIs ───

    def generate_client_order_id(self) -> str:
        return self.signer.generate_client_order_id(region=self._region)

    async def place_order(self, asset_id: str, side: str, price: str, size: str,
                          tif: str = "gtc", reduce_only: bool = False,
                          client_order_id: str = None) -> dict:
        nonce = self.signer.now_millis()
        client_order_id = client_order_id or self.generate_client_order_id()

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
        result = await self._post("/api/v5/predictions/orders", body)
        # Attach clientOrderId to response for caller tracking
        if isinstance(result, dict):
            result["_clientOrderId"] = client_order_id
        return result

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
            from urllib.parse import urlencode as _urlencode
            query = "?" + _urlencode(params)
        full_path = path + query
        request_id = secrets.token_hex(8)
        safe_path = self._bounded_text(path, 256)
        query_summary = self._safe_param_summary(params)

        for attempt in range(2):
            started_ns = time.perf_counter_ns()
            audit_event(
                "okx.http.request",
                method="GET",
                path=safe_path,
                query=query_summary,
                request_id=request_id,
                attempt=attempt + 1,
                **self._log_context(),
            )
            try:
                session = await self._get_session()
                # Refresh time-sensitive authentication on a connection retry.
                headers = self.auth.headers("GET", full_path)
                async with session.get(self.base_url + full_path, headers=headers) as resp:
                    response_metadata = self._response_metadata(resp)
                    try:
                        data = await resp.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, UnicodeError, ValueError) as exc:
                        audit_event(
                            "okx.http.invalid_json",
                            level=logging.ERROR,
                            method="GET",
                            path=safe_path,
                            query=query_summary,
                            request_id=request_id,
                            attempt=attempt + 1,
                            elapsed_ms=self._elapsed_ms(started_ns),
                            acknowledged=False,
                            response_received=True,
                            error=self._safe_error_summary(exc),
                            **response_metadata,
                            **self._log_context(),
                        )
                        return {
                            "code": "INVALID_RESPONSE",
                            "msg": "OKX returned invalid JSON",
                        }
                    if not isinstance(data, dict) or "code" not in data:
                        audit_event(
                            "okx.http.invalid_response",
                            level=logging.ERROR,
                            method="GET",
                            path=safe_path,
                            query=query_summary,
                            request_id=request_id,
                            attempt=attempt + 1,
                            elapsed_ms=self._elapsed_ms(started_ns),
                            acknowledged=False,
                            response_received=True,
                            response_type=type(data).__name__,
                            **response_metadata,
                            **self._log_context(),
                        )
                        return {
                            "code": "INVALID_RESPONSE",
                            "msg": "OKX returned an invalid response shape",
                        }
                    code = str(data.get("code", ""))
                    response_level = (
                        logging.INFO
                        if code == "0" and int(getattr(resp, "status", 0) or 0) < 400
                        else logging.WARNING
                    )
                    audit_event(
                        "okx.http.response",
                        level=response_level,
                        method="GET",
                        path=safe_path,
                        query=query_summary,
                        request_id=request_id,
                        code=code,
                        message=self._safe_message(
                            data.get("message") or data.get("msg", "")
                        ),
                        elapsed_ms=self._elapsed_ms(started_ns),
                        attempt=attempt + 1,
                        acknowledged=True,
                        response_received=True,
                        **response_metadata,
                        **self._log_context(),
                    )
                    return data
            except asyncio.TimeoutError:
                logger.error("GET %s timed out request_id=%s", safe_path, request_id)
                audit_event(
                    "okx.http.timeout",
                    level=logging.ERROR,
                    method="GET",
                    path=safe_path,
                    query=query_summary,
                    request_id=request_id,
                    elapsed_ms=self._elapsed_ms(started_ns),
                    attempt=attempt + 1,
                    retrying=False,
                    acknowledged=False,
                    response_received=False,
                    **self._log_context(),
                )
                return {"code": "TIMEOUT", "msg": "Request timed out"}
            except (aiohttp.ClientError, OSError) as e:
                # Connection error — close stale session and retry once
                safe_error = self._safe_error_summary(e)
                audit_event(
                    "okx.http.connection_error",
                    level=logging.WARNING if attempt == 0 else logging.ERROR,
                    method="GET",
                    path=safe_path,
                    query=query_summary,
                    request_id=request_id,
                    elapsed_ms=self._elapsed_ms(started_ns),
                    attempt=attempt + 1,
                    retrying=attempt == 0,
                    acknowledged=False,
                    response_received=False,
                    error=safe_error,
                    **self._log_context(),
                )
                if attempt == 0:
                    logger.warning(
                        "GET %s connection error, rebuilding session request_id=%s: %s",
                        safe_path,
                        request_id,
                        safe_error["message"],
                    )
                    await self.close()
                    continue
                logger.error(
                    "GET %s failed after retry request_id=%s: %s",
                    safe_path,
                    request_id,
                    safe_error["message"],
                )
                return {"code": "ERROR", "msg": str(e)}
            except Exception as e:
                safe_error = self._safe_error_summary(e)
                logger.error(
                    "GET %s failed request_id=%s error_type=%s: %s",
                    safe_path,
                    request_id,
                    safe_error["type"],
                    safe_error["message"],
                )
                audit_event(
                    "okx.http.exception",
                    level=logging.ERROR,
                    method="GET",
                    path=safe_path,
                    query=query_summary,
                    request_id=request_id,
                    elapsed_ms=self._elapsed_ms(started_ns),
                    attempt=attempt + 1,
                    acknowledged=False,
                    error=safe_error,
                    **self._log_context(),
                )
                return {"code": "ERROR", "msg": str(e)}
        return {"code": "ERROR", "msg": "Unexpected"}

    async def _post(self, path: str, body: dict) -> dict:
        body_str = json.dumps(body, separators=(",", ":"))
        request_id = secrets.token_hex(8)
        safe_path = self._bounded_text(path, 256)
        action = self._safe_action_summary(body)

        for attempt in range(2):
            started_ns = time.perf_counter_ns()
            audit_event(
                "okx.write.request",
                method="POST",
                path=safe_path,
                request_id=request_id,
                attempt=attempt + 1,
                action=action,
                acknowledged=False,
                **self._log_context(),
            )
            try:
                session = await self._get_session()
                headers = self.auth.headers("POST", path, body_str)
                async with session.post(
                    self.base_url + path, headers=headers, data=body_str
                ) as resp:
                    response_metadata = self._response_metadata(resp)
                    try:
                        data = await resp.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, UnicodeError, ValueError) as exc:
                        audit_event(
                            "okx.write.invalid_json",
                            level=logging.ERROR,
                            method="POST",
                            path=safe_path,
                            request_id=request_id,
                            elapsed_ms=self._elapsed_ms(started_ns),
                            attempt=attempt + 1,
                            action=action,
                            acknowledged=False,
                            response_received=True,
                            order_status_confirmed=False,
                            error=self._safe_error_summary(exc),
                            **response_metadata,
                            **self._log_context(),
                        )
                        return {
                            "code": "INVALID_RESPONSE",
                            "msg": "OKX returned invalid JSON",
                        }
                    if not isinstance(data, dict) or "code" not in data:
                        audit_event(
                            "okx.write.invalid_response",
                            level=logging.ERROR,
                            method="POST",
                            path=safe_path,
                            request_id=request_id,
                            elapsed_ms=self._elapsed_ms(started_ns),
                            attempt=attempt + 1,
                            action=action,
                            acknowledged=False,
                            response_received=True,
                            order_status_confirmed=False,
                            response_type=type(data).__name__,
                            **response_metadata,
                            **self._log_context(),
                        )
                        return {
                            "code": "INVALID_RESPONSE",
                            "msg": "OKX returned an invalid response shape",
                        }
                    response_data = data.get("data", {})
                    code = str(data.get("code", ""))
                    http_status = int(getattr(resp, "status", 0) or 0)
                    audit_event(
                        "okx.write.response",
                        level=(
                            logging.INFO
                            if code == "0" and http_status < 400
                            else logging.WARNING
                        ),
                        method="POST",
                        path=safe_path,
                        request_id=request_id,
                        code=code,
                        message=self._safe_message(
                            data.get("message") or data.get("msg", "")
                        ),
                        tx_hash=self._bounded_text(
                            response_data.get("txHash", ""), 160
                        ) if isinstance(response_data, dict) else "",
                        elapsed_ms=self._elapsed_ms(started_ns),
                        attempt=attempt + 1,
                        action=action,
                        acknowledged=True,
                        response_received=True,
                        request_accepted=code == "0",
                        order_status_confirmed=False,
                        **response_metadata,
                        **self._log_context(),
                    )
                    return data
            except asyncio.TimeoutError:
                logger.error("POST %s timed out request_id=%s", safe_path, request_id)
                audit_event(
                    "okx.write.timeout",
                    level=logging.ERROR,
                    method="POST",
                    path=safe_path,
                    request_id=request_id,
                    elapsed_ms=self._elapsed_ms(started_ns),
                    attempt=attempt + 1,
                    action=action,
                    retrying=False,
                    acknowledged=False,
                    response_received=False,
                    order_status_confirmed=False,
                    **self._log_context(),
                )
                return {"code": "TIMEOUT", "msg": "Request timed out"}
            except (aiohttp.ClientError, OSError) as e:
                safe_error = self._safe_error_summary(e)
                audit_event(
                    "okx.write.connection_error",
                    level=logging.WARNING if attempt == 0 else logging.ERROR,
                    method="POST",
                    path=safe_path,
                    request_id=request_id,
                    elapsed_ms=self._elapsed_ms(started_ns),
                    attempt=attempt + 1,
                    retrying=attempt == 0,
                    action=action,
                    acknowledged=False,
                    response_received=False,
                    order_status_confirmed=False,
                    error=safe_error,
                    **self._log_context(),
                )
                if attempt == 0:
                    logger.warning(
                        "POST %s connection error, rebuilding session request_id=%s: %s",
                        safe_path,
                        request_id,
                        safe_error["message"],
                    )
                    await self.close()
                    continue
                logger.error(
                    "POST %s failed after retry request_id=%s: %s",
                    safe_path,
                    request_id,
                    safe_error["message"],
                )
                return {"code": "ERROR", "msg": str(e)}
            except Exception as e:
                safe_error = self._safe_error_summary(e)
                logger.error(
                    "POST %s failed request_id=%s error_type=%s: %s",
                    safe_path,
                    request_id,
                    safe_error["type"],
                    safe_error["message"],
                )
                audit_event(
                    "okx.write.exception",
                    level=logging.ERROR,
                    method="POST",
                    path=safe_path,
                    request_id=request_id,
                    elapsed_ms=self._elapsed_ms(started_ns),
                    attempt=attempt + 1,
                    action=action,
                    acknowledged=False,
                    order_status_confirmed=False,
                    error=safe_error,
                    **self._log_context(),
                )
                return {"code": "ERROR", "msg": str(e)}
        return {"code": "ERROR", "msg": "Unexpected"}
