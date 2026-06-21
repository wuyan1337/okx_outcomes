"""
WebSocket orderbook relay: connects to Poly CLOB WS + OKX WS upstream,
merges and pushes to frontend in real-time.

Poly WS: wss://ws-subscriptions-clob.polymarket.com/ws/market
  Subscribe: {"type":"market","assets_id":"<token_id>"}  (Note: uses assets_id not asset_id)
  Messages: {"event_type":"book","asset_id":"...","market":"...","bids":[...],"asks":[...]}

OKX WS: wss://wspap.okx.com:8443/ws/v5/public (paper) or wss://ws.okx.com:8443/ws/v5/public
  Subscribe: {"op":"subscribe","args":[{"channel":"pm-books5","instId":"<asset_id>"}]}
  Messages: {"arg":{"channel":"pm-books5","instId":"..."},"data":[{"bids":[["price","size"]],"asks":[...]}]}
"""
import asyncio
import json
import time
import logging
from typing import Optional
from fastapi import WebSocket, WebSocketDisconnect

import aiohttp
from aiohttp_socks import ProxyConnector

from .core.database import PROXY_NODES

logger = logging.getLogger(__name__)


def _sort_poly_book(bids, asks, limit=10):
    sorted_bids = sorted(bids, key=lambda x: float(x.get('price', 0)), reverse=True)[:limit]
    sorted_asks = sorted(asks, key=lambda x: float(x.get('price', 0)))[:limit]
    return sorted_bids, sorted_asks

# OKX WS endpoints
OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"


async def poly_ws_relay(ws_out: WebSocket, token_id: str, stop_event: asyncio.Event):
    """Connect to Polymarket WS and relay book updates to frontend."""
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    retry_delay = 1
    while not stop_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=15) as ws:
                    # Send initial REST snapshot
                    try:
                        async with session.get(
                            f"https://clob.polymarket.com/book?token_id={token_id}",
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as snap_resp:
                            if snap_resp.status == 200:
                                snap = await snap_resp.json()
                                bids = snap.get("bids", [])
                                asks = snap.get("asks", [])
                                if bids or asks:
                                    sb, sa = _sort_poly_book(bids, asks)
                                    await ws_out.send_json({
                                        "source": "poly",
                                        "ts": int(time.time() * 1000),
                                        "bids": sb,
                                        "asks": sa,
                                    })
                    except Exception:
                        pass

                    # Subscribe to WS for real-time updates
                    sub_msg = json.dumps({"type": "market", "assets_ids": [token_id]})
                    await ws.send_str(sub_msg)
                    logger.info(f"Poly WS subscribed: {token_id[:20]}...")
                    retry_delay = 1

                    async for msg in ws:
                        if stop_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                if data.get("event_type") == "book":
                                    # Normalize to our format
                                    bids = data.get("bids", [])
                                    asks = data.get("asks", [])
                                    sb, sa = _sort_poly_book(bids, asks)
                                    out = {
                                        "source": "poly",
                                        "ts": int(time.time() * 1000),
                                        "bids": sb,
                                        "asks": sa,
                                    }
                                    await ws_out.send_json(out)
                            except Exception:
                                pass
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"Poly WS error: {e}")
        if not stop_event.is_set():
            await asyncio.sleep(min(retry_delay, 10))
            retry_delay *= 2


async def okx_ws_relay(ws_out: WebSocket, asset_id: str, proxy_node: str, stop_event: asyncio.Event):
    """Connect to OKX WS and relay pm-books5 updates to frontend."""
    retry_delay = 1
    while not stop_event.is_set():
        try:
            connector = None
            if proxy_node and proxy_node in PROXY_NODES:
                node = PROXY_NODES[proxy_node]
                if node["host"] is not None:
                    connector = ProxyConnector.from_url(f"socks5://{node['host']}:{node['port']}")

            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.ws_connect(OKX_WS_PUBLIC, heartbeat=20) as ws:
                    # Send initial REST snapshot
                    try:
                        okx_url = f"https://us.okx.com/api/v5/market/pm-books?instId={asset_id}&sz=5"
                        async with session.get(
                            okx_url, timeout=aiohttp.ClientTimeout(total=5)
                        ) as snap_resp:
                            if snap_resp.status == 200:
                                snap = await snap_resp.json()
                                if snap.get("code") == "0" and snap.get("data"):
                                    book = snap["data"][0]
                                    await ws_out.send_json({
                                        "source": "okx",
                                        "ts": int(time.time() * 1000),
                                        "bids": book.get("bids", [])[:10],
                                        "asks": book.get("asks", [])[:10],
                                    })
                    except Exception:
                        pass

                    # Subscribe to pm-books5 for real-time updates
                    sub_msg = json.dumps({
                        "op": "subscribe",
                        "args": [{"channel": "pm-books5", "instId": asset_id}]
                    })
                    await ws.send_str(sub_msg)
                    logger.info(f"OKX WS subscribed: pm-books5 {asset_id} via {proxy_node}")
                    retry_delay = 1

                    async for msg in ws:
                        if stop_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                # OKX sends {"arg":...,"data":[{"bids":[...],"asks":[...]}]}
                                if "data" in data and data.get("arg", {}).get("channel") == "pm-books5":
                                    book = data["data"][0] if data["data"] else {}
                                    bids = book.get("bids", [])
                                    asks = book.get("asks", [])
                                    out = {
                                        "source": "okx",
                                        "ts": int(time.time() * 1000),
                                        "bids": bids[:10],
                                        "asks": asks[:10],
                                    }
                                    await ws_out.send_json(out)
                            except Exception:
                                pass
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"OKX WS error: {e}")
        if not stop_event.is_set():
            await asyncio.sleep(min(retry_delay, 10))
            retry_delay *= 2


async def periodic_refresh(ws_out: WebSocket, poly_token: str, okx_asset: str, proxy_node: str, stop_event: asyncio.Event):
    """Fallback: refresh via REST every 5s to keep data fresh when WS is quiet."""
    while not stop_event.is_set():
        await asyncio.sleep(3)
        if stop_event.is_set():
            break
        try:
            async with aiohttp.ClientSession() as session:
                if poly_token:
                    try:
                        url = f"https://clob.polymarket.com/book?token_id={poly_token}"
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                            if resp.status == 200:
                                snap = await resp.json()
                                bids = snap.get("bids", [])
                                asks = snap.get("asks", [])
                                if bids or asks:
                                    sb, sa = _sort_poly_book(bids, asks)
                                    await ws_out.send_json({
                                        "source": "poly",
                                        "ts": int(time.time() * 1000),
                                        "bids": sb,
                                        "asks": sa,
                                    })
                    except Exception:
                        pass
                if okx_asset:
                    try:
                        connector = None
                        if proxy_node and proxy_node in PROXY_NODES:
                            node = PROXY_NODES[proxy_node]
                            if node["host"] is not None:
                                connector = ProxyConnector.from_url(f"socks5://{node['host']}:{node['port']}")
                        async with aiohttp.ClientSession(connector=connector) as sess2:
                            url = f"https://us.okx.com/api/v5/market/pm-books?instId={okx_asset}&sz=5"
                            async with sess2.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    if data.get("code") == "0" and data.get("data"):
                                        book = data["data"][0]
                                        await ws_out.send_json({
                                            "source": "okx",
                                            "ts": int(time.time() * 1000),
                                            "bids": book.get("bids", [])[:10],
                                            "asks": book.get("asks", [])[:10],
                                        })
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return
        except Exception:
            pass


def register_ws_routes(app, manager):
    """Register WebSocket routes on the FastAPI app."""

    @app.websocket("/ws/book/{account_id}")
    async def ws_book(websocket: WebSocket, account_id: str):
        await websocket.accept()
        stop_event = asyncio.Event()
        tasks = []

        try:
            # Wait for client to send subscription message
            # Format: {"poly_token": "...", "okx_asset": "..."}
            raw = await asyncio.wait_for(websocket.receive_text(), timeout=30)
            sub = json.loads(raw)
            poly_token = sub.get("poly_token", "")
            okx_asset = sub.get("okx_asset", "")

            inst = manager.get_instance(account_id)
            proxy_node = inst.proxy_node if inst else "JP2"

            if poly_token:
                tasks.append(asyncio.create_task(
                    poly_ws_relay(websocket, poly_token, stop_event)
                ))
            if okx_asset:
                tasks.append(asyncio.create_task(
                    okx_ws_relay(websocket, okx_asset, proxy_node, stop_event)
                ))
            # Always add periodic refresh as fallback
            tasks.append(asyncio.create_task(
                periodic_refresh(websocket, poly_token, okx_asset, proxy_node, stop_event)
            ))

            if not tasks:
                await websocket.send_json({"error": "No subscriptions"})
                await websocket.close()
                return

            # Keep connection alive - listen for close or new commands
            while True:
                try:
                    msg = await asyncio.wait_for(websocket.receive_text(), timeout=45)
                    cmd = json.loads(msg)
                    if cmd.get("type") == "ping":
                        await websocket.send_json({"type": "pong", "ts": int(time.time() * 1000)})
                    elif cmd.get("type") == "close":
                        break
                except asyncio.TimeoutError:
                    # Send keepalive
                    try:
                        await websocket.send_json({"type": "pong", "ts": int(time.time() * 1000)})
                    except Exception:
                        break
                except WebSocketDisconnect:
                    break

        except (WebSocketDisconnect, asyncio.TimeoutError, Exception) as e:
            logger.debug(f"WS book closed: {e}")
        finally:
            stop_event.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await websocket.close()
            except Exception:
                pass
