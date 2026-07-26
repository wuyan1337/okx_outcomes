"""Relay Polymarket and OKX prediction-market books to the dashboard."""
import asyncio
import json
import time
import logging
from typing import Optional
from fastapi import WebSocket, WebSocketDisconnect

import aiohttp
from aiohttp_socks import ProxyConnector

from .core.database import PROXY_NODES
from .core.price_utils import as_decimal, format_okx_price, invert_binary_price

logger = logging.getLogger(__name__)


def _sort_poly_book(bids, asks, limit=10):
    sorted_bids = sorted(bids, key=lambda x: as_decimal(x.get("price", "0")), reverse=True)[:limit]
    sorted_asks = sorted(asks, key=lambda x: as_decimal(x.get("price", "0")))[:limit]
    return sorted_bids, sorted_asks


def _poly_book_maps(bids, asks):
    bid_map = {as_decimal(row["price"]): dict(row) for row in bids or [] if row.get("price") is not None}
    ask_map = {as_decimal(row["price"]): dict(row) for row in asks or [] if row.get("price") is not None}
    return bid_map, ask_map


def _apply_poly_price_changes(bid_map, ask_map, changes, token_id=""):
    """Apply Polymarket incremental price changes to an in-memory book."""
    changed = False
    for change in changes or []:
        change_asset = str(change.get("asset_id", change.get("assetId", "")))
        if token_id and change_asset and change_asset != str(token_id):
            continue
        price_value = change.get("price")
        if price_value is None:
            continue
        price = as_decimal(price_value)
        size_value = change.get("size", "0")
        size = as_decimal(size_value or "0")
        side = str(change.get("side", "")).upper()
        target = bid_map if side in ("BUY", "BID") else ask_map if side in ("SELL", "ASK") else None
        if target is None:
            continue
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = {"price": str(price_value), "size": str(size_value)}
        changed = True
    return changed


def _poly_book_from_maps(bid_map, ask_map, limit=10):
    return _sort_poly_book(list(bid_map.values()), list(ask_map.values()), limit)

OKX_WS_DEFAULT = "wss://ws.okx.com:8443/ws/v5/business"
OKX_WS_US = "wss://wsus.okx.com/ws/v5/business"


def _okx_ws_endpoint(base_url: str) -> str:
    return OKX_WS_US if "us.okx.com" in (base_url or "") else OKX_WS_DEFAULT


def _okx_book_maps(bids, asks):
    bid_map = {as_decimal(row[0]): list(row) for row in bids or [] if len(row) >= 2}
    ask_map = {as_decimal(row[0]): list(row) for row in asks or [] if len(row) >= 2}
    return bid_map, ask_map


def _apply_okx_levels(level_map, rows):
    for row in rows or []:
        if len(row) < 2:
            continue
        price = as_decimal(row[0])
        size = as_decimal(row[1])
        if size <= 0:
            level_map.pop(price, None)
        else:
            level_map[price] = list(row)


def _okx_book_from_maps(bid_map, ask_map, limit=10):
    bids = [bid_map[key] for key in sorted(bid_map, reverse=True)[:limit]]
    asks = [ask_map[key] for key in sorted(ask_map)[:limit]]
    return bids, asks


def _invert_okx_book_arrays(bids, asks):
    inv_bids = []
    inv_asks = []
    for a in asks or []:
        if not a:
            continue
        row = list(a)
        row[0] = format_okx_price(invert_binary_price(a[0]))
        inv_bids.append(row)
    for b in bids or []:
        if not b:
            continue
        row = list(b)
        row[0] = format_okx_price(invert_binary_price(b[0]))
        inv_asks.append(row)
    inv_bids.sort(key=lambda x: as_decimal(x[0]), reverse=True)
    inv_asks.sort(key=lambda x: as_decimal(x[0]))
    return inv_bids, inv_asks


async def poly_ws_relay(ws_out: WebSocket, token_id: str, stop_event: asyncio.Event):
    """Connect to Polymarket WS and relay book updates to frontend."""
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    retry_delay = 1
    while not stop_event.is_set():
        bid_map = {}
        ask_map = {}
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
                                    bid_map, ask_map = _poly_book_maps(bids, asks)
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
                                raw = json.loads(msg.data)
                                items = raw if isinstance(raw, list) else [raw]
                                for data in items:
                                    event_type = data.get("event_type")
                                    if event_type == "book":
                                        bids = data.get("bids", [])
                                        asks = data.get("asks", [])
                                        bid_map, ask_map = _poly_book_maps(bids, asks)
                                        sb, sa = _sort_poly_book(bids, asks)
                                    elif event_type == "price_change":
                                        if not _apply_poly_price_changes(
                                            bid_map, ask_map, data.get("price_changes", []), token_id
                                        ):
                                            continue
                                        sb, sa = _poly_book_from_maps(bid_map, ask_map)
                                    else:
                                        continue
                                    await ws_out.send_json({
                                        "source": "poly",
                                        "ts": int(time.time() * 1000),
                                        "bids": sb,
                                        "asks": sa,
                                    })
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


async def okx_ws_relay(ws_out: WebSocket, asset_id: str, proxy_node: str,
                       base_url: str, stop_event: asyncio.Event,
                       invert: bool = False):
    """Connect to OKX business WS and maintain the incremental pm-books book."""
    retry_delay = 1
    while not stop_event.is_set():
        bid_map = {}
        ask_map = {}
        last_seq = None
        ws_snapshot_received = False
        try:
            connector = None
            if proxy_node and proxy_node in PROXY_NODES:
                node = PROXY_NODES[proxy_node]
                if node["host"] is not None:
                    connector = ProxyConnector.from_url(f"socks5://{node['host']}:{node['port']}")

            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.ws_connect(_okx_ws_endpoint(base_url), heartbeat=20) as ws:
                    # Send initial REST snapshot
                    try:
                        okx_url = f"{base_url.rstrip('/')}/api/v5/market/pm-books?instId={asset_id}&sz=10"
                        async with session.get(
                            okx_url, timeout=aiohttp.ClientTimeout(total=5)
                        ) as snap_resp:
                            if snap_resp.status == 200:
                                snap = await snap_resp.json()
                                if snap.get("code") == "0" and snap.get("data"):
                                    book = snap["data"][0]
                                    bids = book.get("bids", [])[:10]
                                    asks = book.get("asks", [])[:10]
                                    bid_map, ask_map = _okx_book_maps(bids, asks)
                                    if invert:
                                        bids, asks = _invert_okx_book_arrays(bids, asks)
                                    await ws_out.send_json({
                                        "source": "okx",
                                        "ts": int(time.time() * 1000),
                                        "bids": bids[:10],
                                        "asks": asks[:10],
                                    })
                    except Exception:
                        pass

                    sub_msg = json.dumps({
                        "op": "subscribe",
                        "args": [{"channel": "pm-books", "instId": asset_id}]
                    })
                    await ws.send_str(sub_msg)
                    logger.info(f"OKX WS subscribed: pm-books {asset_id} via {proxy_node}")
                    retry_delay = 1

                    async for msg in ws:
                        if stop_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                if "data" in data and data.get("arg", {}).get("channel") == "pm-books":
                                    book = data["data"][0] if data["data"] else {}
                                    action = str(data.get("action", "")).lower()
                                    seq = book.get("seqId", book.get("seq_id"))
                                    prev_seq = book.get("prevSeqId", book.get("prev_seq_id"))
                                    if action == "snapshot":
                                        bid_map, ask_map = _okx_book_maps(
                                            book.get("bids", []), book.get("asks", [])
                                        )
                                        ws_snapshot_received = True
                                    elif action == "update":
                                        if not ws_snapshot_received:
                                            raise RuntimeError("OKX pm-books update before snapshot")
                                        if last_seq is not None and prev_seq is not None:
                                            try:
                                                sequence_is_contiguous = int(prev_seq) == int(last_seq)
                                            except (TypeError, ValueError) as exc:
                                                raise RuntimeError("OKX pm-books invalid sequence") from exc
                                            if not sequence_is_contiguous:
                                                raise RuntimeError("OKX pm-books sequence gap")
                                        _apply_okx_levels(bid_map, book.get("bids", []))
                                        _apply_okx_levels(ask_map, book.get("asks", []))
                                    else:
                                        continue
                                    last_seq = seq
                                    bids, asks = _okx_book_from_maps(bid_map, ask_map)
                                    if invert:
                                        bids, asks = _invert_okx_book_arrays(bids, asks)
                                    out = {
                                        "source": "okx",
                                        "ts": int(time.time() * 1000),
                                        "bids": bids[:10],
                                        "asks": asks[:10],
                                    }
                                    await ws_out.send_json(out)
                            except RuntimeError:
                                raise
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


async def periodic_refresh(ws_out: WebSocket, poly_token: str, okx_asset: str,
                           proxy_node: str, base_url: str,
                           stop_event: asyncio.Event, invert: bool = False):
    """Refresh both books via REST every three seconds."""
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
                            url = f"{base_url.rstrip('/')}/api/v5/market/pm-books?instId={okx_asset}&sz=10"
                            async with sess2.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    if data.get("code") == "0" and data.get("data"):
                                        book = data["data"][0]
                                        bids = book.get("bids", [])[:10]
                                        asks = book.get("asks", [])[:10]
                                        if invert:
                                            bids, asks = _invert_okx_book_arrays(bids, asks)
                                        await ws_out.send_json({
                                            "source": "okx",
                                            "ts": int(time.time() * 1000),
                                            "bids": bids[:10],
                                            "asks": asks[:10],
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
            yes_asset = sub.get("yes_asset", "")
            invert_okx = bool(yes_asset and okx_asset and yes_asset != okx_asset)
            okx_query_asset = yes_asset if invert_okx else okx_asset

            inst = manager.get_instance(account_id)
            proxy_node = inst.proxy_node if inst else "JP2"
            base_url = inst.client.base_url if inst else "https://us.okx.com"

            if poly_token:
                tasks.append(asyncio.create_task(
                    poly_ws_relay(websocket, poly_token, stop_event)
                ))
            if okx_asset:
                tasks.append(asyncio.create_task(
                    okx_ws_relay(
                        websocket, okx_query_asset, proxy_node, base_url,
                        stop_event, invert_okx,
                    )
                ))
            tasks.append(asyncio.create_task(
                periodic_refresh(
                    websocket, poly_token, okx_query_asset, proxy_node,
                    base_url, stop_event, invert_okx,
                )
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
