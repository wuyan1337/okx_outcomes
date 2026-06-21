import time
import json
"""FastAPI backend for OKX Outcomes Bot — Multi-Account System."""
import asyncio
import yaml
import secrets
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse

from .core.database import (
    init_db, get_accounts, get_account, get_account_decrypted,
    get_available_proxies, get_used_proxies, PROXY_NODES,
    get_global_config, set_global_config, update_account, delete_account
)
from .services.account_manager import AccountManager
from .services.notifier import TelegramNotifier

# Configure application-level logging so notifier/strategy messages appear in journalctl
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# Load config (for server auth only now)
config_path = Path(__file__).parent.parent / "config.yaml"
with open(config_path) as f:
    config = yaml.safe_load(f)

# Account manager (initialized in lifespan)
manager = AccountManager()

# Auth
security = HTTPBasic(auto_error=False)

# ─── Rate Limiting ───
from collections import defaultdict
import time as _time

_login_attempts = defaultdict(list)  # ip -> [timestamp, ...]
_MAX_ATTEMPTS = 5
_LOCKOUT_SECONDS = 60

def _check_rate_limit(request: Request):
    """Block IP after 5 failed auth attempts within 60 seconds."""
    ip = request.client.host if request.client else "unknown"
    now = _time.time()
    # Clean old entries
    _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < _LOCKOUT_SECONDS]
    if len(_login_attempts[ip]) >= _MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many attempts. Try again later.")

def _record_failed_attempt(request: Request):
    ip = request.client.host if request.client else "unknown"
    _login_attempts[ip].append(_time.time())

def _clear_attempts(request: Request):
    ip = request.client.host if request.client else "unknown"
    _login_attempts.pop(ip, None)



def verify_auth(request: Request, credentials: Optional[HTTPBasicCredentials] = Depends(security)):
    _check_rate_limit(request)
    if credentials is None:
        _record_failed_attempt(request)
        raise HTTPException(status_code=401, detail="Not authenticated")
    correct_user = secrets.compare_digest(credentials.username, config["server"]["username"])
    correct_pass = secrets.compare_digest(credentials.password, config["server"]["password"])
    if not (correct_user and correct_pass):
        _record_failed_attempt(request)
        raise HTTPException(status_code=401, detail="Incorrect credentials")
    _clear_attempts(request)
    return credentials.username


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await manager.init()

    yield
    await manager.close_all()


app = FastAPI(title="OKX Outcomes Bot", lifespan=lifespan)

# Register WebSocket routes for real-time orderbook
from .ws_book import register_ws_routes
register_ws_routes(app, manager)

# ─── Static pages ───

frontend_dir = Path(__file__).parent.parent / "frontend"


@app.get("/", response_class=HTMLResponse)
async def serve_root():
    return HTMLResponse(content=(frontend_dir / "login.html").read_text(), status_code=200)


@app.get("/login", response_class=HTMLResponse)
async def serve_login():
    return HTMLResponse(content=(frontend_dir / "login.html").read_text(), status_code=200)


@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    return HTMLResponse(content=(frontend_dir / "dashboard.html").read_text(), status_code=200)


# ─── Global APIs ───


@app.get("/api/status")
async def get_status(user: str = Depends(verify_auth)):
    """Overall status — used by control panel."""
    stats = await manager.get_aggregated_stats()
    accounts_info = []
    for inst in manager.instances.values():
        running_strategy = None
        if inst.strategy_d.running:
            running_strategy = "Manual-D"
        elif inst.strategy_s.running:
            running_strategy = "Manual-S"
        # Get per-account stats for cards
        acct_orders = 0
        acct_positions = 0
        acct_balance = "--"
        try:
            bal_data = await inst.client.get_balance()
            if isinstance(bal_data, dict) and str(bal_data.get("code", "")) == "0":
                data = bal_data.get("data", [])
                if data:
                    acct_balance = data[0].get("balance", "--")
            orders_data = await inst.client.get_orders(status="open")
            if isinstance(orders_data, dict) and str(orders_data.get("code", "")) == "0":
                raw = orders_data.get("data", {})
                if isinstance(raw, dict):
                    acct_orders = len(raw.get("list", []))
                elif isinstance(raw, list):
                    acct_orders = len(raw)
            pos_data = await inst.client.get_positions()
            if isinstance(pos_data, dict) and str(pos_data.get("code", "")) == "0":
                raw_pos = pos_data.get("data", {})
                pos_list = raw_pos.get("list", []) if isinstance(raw_pos, dict) else raw_pos if isinstance(raw_pos, list) else []
                acct_positions = len([p for p in pos_list if float(p.get("pos", "0")) != 0])
        except Exception:
            pass
        accounts_info.append({
            "id": inst.account_id,
            "name": inst.name,
            "proxy_node": inst.proxy_node,
            "running": inst.strategy_d.running or inst.strategy_s.running,
            "strategy": running_strategy,
            "orders": acct_orders,
            "positions": acct_positions,
            "balance": acct_balance,
        })
    return {
        **stats,
        "accounts": accounts_info,
    }


@app.get("/api/accounts")
async def list_accounts(user: str = Depends(verify_auth)):
    """List all accounts (no secrets)."""
    accounts = get_accounts()
    result = []
    for acc in accounts:
        inst = manager.get_instance(acc["id"])
        running = False
        strategy = None
        if inst:
            if inst.strategy_d.running:
                running = True
                strategy = "Manual-D"
            elif inst.strategy_s.running:
                running = True
                strategy = "Manual-S"
        result.append({
            "id": acc["id"],
            "name": acc["name"],
            "proxy_node": acc["proxy_node"],
            "proxy_label": PROXY_NODES.get(acc["proxy_node"], {}).get("label", acc["proxy_node"]),
            "running": running,
            "strategy": strategy,
            "created_at": acc["created_at"],
        })
    return {"accounts": result}


@app.get("/api/accounts/proxies")
async def available_proxies(user: str = Depends(verify_auth)):
    """Get available proxy nodes."""
    return {"proxies": get_available_proxies()}


@app.get("/api/proxy/nodes")
async def all_proxy_nodes(user: str = Depends(verify_auth)):
    """Get ALL proxy nodes with their assignment status."""
    used = get_used_proxies()
    nodes = []
    for k, v in PROXY_NODES.items():
        nodes.append({"id": k, "label": v["label"], "used": k in used})
    return {"nodes": nodes}


@app.post("/api/accounts")
async def create_new_account(request: Request, user: str = Depends(verify_auth)):
    """Create a new account."""
    body = await request.json()
    name = body.get("name", "").strip()
    api_key = body.get("api_key", "").strip()
    secret_key = body.get("secret_key", "").strip()
    passphrase = body.get("passphrase", "").strip()
    agent_key = body.get("agent_key", "").strip()
    proxy_node = body.get("proxy_node", "").strip()
    region = body.get("region", "AU").strip()

    if not all([name, api_key, secret_key, passphrase, agent_key, proxy_node]):
        return JSONResponse(status_code=400, content={"ok": False, "error": "All fields required"})

    try:
        result = await manager.add_account(name, api_key, secret_key, passphrase, agent_key, proxy_node, region=region)
        return {"ok": True, "account": result}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.delete("/api/accounts/{account_id}")
async def remove_account(account_id: str, user: str = Depends(verify_auth)):
    """Delete an account."""
    await manager.remove_account(account_id)
    return {"ok": True}


# ─── Per-Account APIs ───

@app.get("/api/accounts/{account_id}/balance")
async def account_balance(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        data = await inst.client.get_balance()
        if isinstance(data, dict) and str(data.get("code", "")) == "0":
            entries = data.get("data", [])
            if entries:
                return {"balance": entries[0].get("balance", "0"), "available": entries[0].get("available", "0")}
        return {"balance": "0", "available": "0", "raw": data}
    except Exception as e:
        return {"balance": "--", "available": "--", "error": str(e)}


@app.get("/api/accounts/{account_id}/orders")
async def account_orders(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        data = await inst.client.get_orders(status="open")
        orders = []
        if isinstance(data, dict) and str(data.get("code", "")) == "0":
            for o in data.get("data", {}).get("list", []):
                aid = o.get("assetId", "")
                token = "Yes" if aid.endswith("000") else "No"
                orders.append({
                    "order_id": o.get("oid", o.get("id", "")),
                    "side": o.get("side", ""),
                    "price": o.get("price", ""),
                    "size": o.get("size", ""),
                    "status": o.get("status", ""),
                    "asset_id": aid,
                    "token": token,
                    "created_at": o.get("createdAt", ""),
                })
        return {"orders": orders}
    except Exception as e:
        return {"orders": [], "error": str(e)}


@app.get("/api/accounts/{account_id}/positions")
async def account_positions(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        data = await inst.client.get_positions(status="open")
        positions = []
        if isinstance(data, dict) and str(data.get("code", "")) == "0":
            for p in data.get("data", {}).get("list", []):
                positions.append({
                    "market_id": p.get("marketId", ""),
                    "token_name": p.get("tokenName", ""),
                    "size": p.get("size", "0"),
                    "avg_price": p.get("avgPrice", "0"),
                })
        return {"positions": positions}
    except Exception as e:
        return {"positions": [], "error": str(e)}


@app.get("/api/accounts/{account_id}/markets")
async def account_markets(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        data = await inst.client.get_events(status="active", page_size=50)
        markets = []
        if isinstance(data, dict):
            events = data.get("data", {}).get("events", [])
            for ev in events:
                for mkt in ev.get("markets", []):
                    yes_out = mkt.get("yesOutcome", {})
                    no_out = mkt.get("noOutcome", {})
                    if yes_out.get("assetId"):
                        markets.append({
                            "event_title": ev.get("eventTitle", ""),
                            "question": mkt.get("question", ""),
                            "market_id": mkt.get("marketId", ""),
                            "yes_asset_id": yes_out.get("assetId", ""),
                            "no_asset_id": no_out.get("assetId", ""),
                        })
        return {"markets": markets}
    except Exception as e:
        return {"markets": [], "error": str(e)}


@app.get("/api/accounts/{account_id}/orderbook")
async def account_orderbook(account_id: str, asset_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        data = await inst.client.get_orderbook(asset_id, depth=10)
        if isinstance(data, dict) and "data" in data:
            book = data["data"]
            if isinstance(book, list) and len(book) > 0:
                return {"bids": book[0].get("bids", []), "asks": book[0].get("asks", [])}
        return {"bids": [], "asks": []}
    except Exception as e:
        return {"bids": [], "asks": [], "error": str(e)}


@app.get("/api/accounts/{account_id}/status")
async def account_status(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    strategy = None
    if inst.strategy_d.running:
        strategy = "manual"
    elif inst.strategy_s.running:
        strategy = "manual-s"
    poly_running = False
    poly_token = ''
    poly_asset = ''
    return {
        "manual_running": inst.strategy_d.running,
        "manual_s_running": inst.strategy_s.running,
            "sniper_running": inst.sniper.running,
            "sniper_status": inst.sniper.state.status,
        "manual_s_price": getattr(inst.strategy_s.state, "price_a", None) if inst.strategy_s.running else None,
        "manual_s_side": getattr(inst.strategy_s.state, "side", None) if inst.strategy_s.running else None,
        "manual_s_asset_id": getattr(inst.strategy_s.state, "yes_asset_id", None) if inst.strategy_s.running else None,
        "poly_running": poly_running,
        "poly_url": poly_token,
        "poly_asset_id": poly_asset,
        "strategy": strategy or "none",
        "running": inst.strategy_d.running or inst.strategy_s.running or inst.sniper.running,
        "cycles": inst.strategy_d.cycles + inst.strategy_s.cycles,
        "last_heartbeat": getattr(inst.client, 'last_heartbeat', None),
        "manual_asset_id": inst.strategy_d.state.yes_asset_id if inst.strategy_d.state.running else (inst.strategy_s.state.yes_asset_id if inst.strategy_s.state.running else ''),
        "manual_price": inst.strategy_d.state.price_a if inst.strategy_d.state.running else (inst.strategy_s.state.price_a if inst.strategy_s.state.running else 0),
        "monitor": inst.poly_monitor.status(),
    }


@app.post("/api/accounts/{account_id}/strategy/start")
async def start_strategy(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")

    body = await request.json()
    strategy_type = body.get("type", "")  # "manual-d" or "manual-s"
    market_id = body.get("market_id", "")
    yes_asset_id = body.get("yes_asset_id", "")
    no_asset_id = body.get("no_asset_id", "")
    price = body.get("price", 0)
    side = body.get("side", "yes")  # For Manual-S
    monitor = body.get("monitor")  # {poly_token_id, depth_threshold} or None

    if not market_id or not yes_asset_id or not price:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Missing params"})

    if not no_asset_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "no_asset_id is required"})

    price_f = float(price)
    # OKX uses cents (e.g. 33 for 0.33)
    price_decimal = price_f / 100.0  # Convert to decimal for Poly comparison

    # Pre-flight: check OKX API accessible
    try:
        bal = await inst.client.get_balance()
        if isinstance(bal, dict) and bal.get("code") and str(bal["code"]) != "0":
            return JSONResponse(status_code=503, content={"ok": False, "error": f"OKX API: {bal.get('msg')}"})
    except Exception as e:
        return JSONResponse(status_code=503, content={"ok": False, "error": f"Cannot reach OKX: {e}"})

    # If monitor enabled, verify BOTH OKX and Poly bid == strategy price
    if monitor and monitor.get("poly_token_id"):
        poly_token_id = monitor["poly_token_id"]
        depth_threshold = float(monitor.get("depth_threshold", 10000))

        # Check OKX orderbook: best bid must match strategy price
        try:
            okx_book = await inst.client.get_orderbook(yes_asset_id, depth=5)
            okx_bids = []
            if str(okx_book.get("code", "")) == "0":
                book_data = okx_book.get("data", [{}])
                if book_data:
                    okx_bids = book_data[0].get("bids", []) if isinstance(book_data, list) else book_data.get("bids", [])
            if not okx_bids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "Cannot get OKX orderbook or no bids"})
            okx_best_bid = float(okx_bids[0][0]) if okx_bids else 0
            # OKX orderbook returns decimal prices (e.g. 0.33 for 33 cents)
            if abs(okx_best_bid - price_decimal) > 0.02:
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": f"OKX best bid ({okx_best_bid:.4f}) != strategy price ({price_decimal:.4f}). Cannot start with monitor."
                })
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX orderbook check failed: {e}"})

        # Check Poly orderbook: best bid must match strategy price (decimal)
        try:
            import aiohttp as _aiohttp
            poly_url = f"https://clob.polymarket.com/book?token_id={poly_token_id}"
            async with _aiohttp.ClientSession() as _sess:
                async with _sess.get(poly_url, timeout=_aiohttp.ClientTimeout(total=5)) as _resp:
                    poly_book = await _resp.json()
            poly_bids = sorted(poly_book.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
            if not poly_bids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "Cannot get Poly orderbook or no bids"})
            poly_best_bid = float(poly_bids[0].get("price", 0))
            if abs(poly_best_bid - price_decimal) > 0.005:
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": f"Poly best bid ({poly_best_bid:.4f}) != strategy price ({price_decimal:.4f}). Cannot start with monitor."
                })
        except _aiohttp.ClientError as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly orderbook check failed: {e}"})

    # Start strategy
    try:
        if strategy_type == "manual-d":
            await inst.strategy_d.start(
                market_id=market_id,
                yes_asset_id=yes_asset_id,
                no_asset_id=no_asset_id,
                price_a=price_f,
            )
        elif strategy_type == "manual-s":
            await inst.strategy_s.start(
                market_id=market_id,
                yes_asset_id=yes_asset_id,
                no_asset_id=no_asset_id,
                side=side,
                price_a=price_f,
            )
        else:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Unknown strategy: {strategy_type}"})
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # If monitor requested, start it now (strategy is already running)
    if monitor and monitor.get("poly_token_id"):
        try:
            result = await inst.poly_monitor.start(
                poly_token_id=monitor["poly_token_id"],
                reference_price=price_decimal,
                depth_threshold=float(monitor.get("depth_threshold", 10000)),
            )
            if result.get("ok"):
                inst.poly_monitor.set_strategy_context(strategy_type, int(price_f), yes_asset_id)
            else:
                # Strategy started but monitor failed - log warning, don't fail the whole thing
                import logging
                logging.getLogger(__name__).warning(f"Monitor start failed: {result}")
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Monitor start error: {e}")

    return {"ok": True}


@app.post("/api/accounts/{account_id}/strategy/stop")
async def stop_strategy(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if inst.strategy_d.running:
        await inst.strategy_d.stop()
    if inst.strategy_s.running:
        await inst.strategy_s.stop()
    if inst.sniper.running:
        await inst.sniper.stop()
    # Always stop monitor when strategy stops (monitor is bound to strategy)
    if inst.poly_monitor.running or inst.poly_monitor.state.enabled:
        await inst.poly_monitor.stop()
    return {"ok": True}


@app.post("/api/accounts/{account_id}/cancel_all")
async def account_cancel_all(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        import asyncio as _asyncio

        # Step 1: Stop any running strategies first (prevent re-ordering)
        if inst.strategy_s.running:
            await inst.strategy_s.stop()
        if inst.strategy_d.running:
            await inst.strategy_d.stop()
        if inst.sniper.running:
            await inst.sniper.stop()
        if inst.poly_monitor.running:
            await inst.poly_monitor.stop()

        # Step 2: Get open orders
        orders_resp = await inst.client.get_orders(status="open")
        orders = []
        if str(orders_resp.get("code", "")) == "0":
            orders = orders_resp.get("data", {}).get("list", [])
        if not orders:
            return {"ok": True, "result": {"message": "No orders to cancel", "cancelled": 0}}

        # Step 3: Try batch cancel (single request, fastest)
        cancels = [{"assetId": o.get("assetId", ""), "oid": o.get("oid", o.get("id", ""))}
                   for o in orders if o.get("assetId") and (o.get("oid") or o.get("id"))]
        if cancels:
            try:
                batch_resp = await inst.client.cancel_orders_batch(cancels)
                if str(batch_resp.get("code", "")) == "0":
                    # Verify
                    await _asyncio.sleep(0.5)
                    verify = await inst.client.get_orders(status="open")
                    remaining = verify.get("data", {}).get("list", []) if str(verify.get("code","")) == "0" else []
                    if not remaining:
                        return {"ok": True, "result": {"cancelled": len(cancels), "failed": 0, "total": len(orders), "method": "batch"}}
            except Exception:
                pass  # Fall through to parallel

        # Step 4: Fallback — parallel individual cancels
        async def _cancel_one(o):
            oid = o.get("oid", o.get("id", ""))
            asset_id = o.get("assetId", "")
            if not oid or not asset_id:
                return False
            try:
                r = await inst.client.cancel_order(asset_id=asset_id, order_id=oid)
                return str(r.get("code", "")) == "0"
            except Exception:
                return False

        results = await _asyncio.gather(*[_cancel_one(o) for o in orders])
        cancelled = sum(1 for r in results if r)
        failed = sum(1 for r in results if not r)

        # Step 5: Verify + retry failures
        if failed > 0:
            await _asyncio.sleep(0.5)
            verify = await inst.client.get_orders(status="open")
            still_open = verify.get("data", {}).get("list", []) if str(verify.get("code","")) == "0" else []
            if still_open:
                results2 = await _asyncio.gather(*[_cancel_one(o) for o in still_open])
                cancelled += sum(1 for r in results2 if r)
                failed = sum(1 for r in results2 if not r)

        return {"ok": True, "result": {"cancelled": cancelled, "failed": failed, "total": len(orders), "method": "parallel"}}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"ok": False, "error": str(e)}


@app.post("/api/accounts/{account_id}/cancel_order")
async def account_cancel_order(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    order_id = body.get("order_id", "")
    asset_id = body.get("asset_id", "")
    if not order_id or not asset_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "order_id and asset_id required"})
    try:
        result = await inst.client.cancel_order(asset_id=asset_id, order_id=order_id)
        return {"ok": True, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── Account Settings (risk control) ───

# (removed dead config routes — see per-account v2 routes below)


# --- Per-account compatibility routes (prod-style API) ---




@app.post("/api/accounts/{account_id}/strategy/manual/start")
async def account_manual_start_v2(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    # Mutex check
    if inst.strategy_d.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-D already running."})
    if inst.strategy_s.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-S is running. Stop it first."})
    if inst.sniper.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Sniper is running. Stop it first."})
    body = await request.json()
    asset_id = body.get("asset_id", "")
    price = body.get("price", 0)
    market_id = body.get("market_id", "")
    no_asset_id = body.get("no_asset_id", "")
    if not asset_id or not price:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Select market and set price"})
    if not market_id:
        try:
            mkts = await inst.client.get_events(status="active", page_size=50)
            if isinstance(mkts, dict):
                for ev in mkts.get("data", {}).get("events", []):
                    for mkt in ev.get("markets", []):
                        if mkt.get("yesOutcome", {}).get("assetId") == asset_id:
                            market_id = mkt.get("marketId", "")
                            no_asset_id = mkt.get("noOutcome", {}).get("assetId", "")
                            break
                    if market_id:
                        break
        except Exception:
            pass
    if not no_asset_id:
        no_asset_id = str(int(asset_id) + 100)
    try:
        bal = await inst.client.get_balance()
        if isinstance(bal, dict) and bal.get("code") and str(bal["code"]) != "0":
            return JSONResponse(status_code=503, content={"ok": False, "error": f"OKX API: {bal.get('msg')}"})
    except Exception as e:
        return JSONResponse(status_code=503, content={"ok": False, "error": f"Cannot reach OKX: {e}"})
    # Monitor validation if enabled
    monitor = body.get("monitor")  # {poly_token_id, depth_threshold}
    price_decimal = float(price) / 100.0 if float(price) >= 1 else float(price)
    if monitor and monitor.get("poly_token_id"):
        poly_token_id = monitor["poly_token_id"]
        depth_threshold = float(monitor.get("depth_threshold", 10000))
        # Check OKX best bid == strategy price
        try:
            okx_book = await inst.client.get_orderbook(asset_id, depth=5)
            okx_bids = []
            if str(okx_book.get("code", "")) == "0":
                book_data = okx_book.get("data", [{}])
                if book_data and isinstance(book_data, list):
                    okx_bids = book_data[0].get("bids", [])
            if not okx_bids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "No OKX bids found"})
            okx_best_bid = float(okx_bids[0][0])
            if abs(okx_best_bid - price_decimal) > 0.02:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX best bid ({okx_best_bid:.4f}) != price ({price_decimal:.4f})"})
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX book check: {e}"})
        # Check Poly best bid == strategy price
        try:
            import aiohttp as _aiohttp
            poly_url_check = f"https://clob.polymarket.com/book?token_id={poly_token_id}"
            async with _aiohttp.ClientSession() as _sess:
                async with _sess.get(poly_url_check, timeout=_aiohttp.ClientTimeout(total=5)) as _resp:
                    poly_book = await _resp.json()
            poly_bids = sorted(poly_book.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
            if not poly_bids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "No Poly bids found"})
            poly_best_bid = float(poly_bids[0].get("price", 0))
            if abs(poly_best_bid - price_decimal) > 0.005:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly best bid ({poly_best_bid:.4f}) != price ({price_decimal:.4f})"})
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly book check: {e}"})

    try:
        await inst.strategy_d.start(market_id=market_id, yes_asset_id=asset_id, no_asset_id=no_asset_id, price_a=float(price))
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # Auto-start monitor if requested
    if monitor and monitor.get("poly_token_id"):
        try:
            r = await inst.poly_monitor.start(
                poly_token_id=monitor["poly_token_id"],
                reference_price=price_decimal,
                depth_threshold=float(monitor.get("depth_threshold", 10000)),
            )
            if r and r.get("ok"):
                inst.poly_monitor.set_strategy_context("manual-d", int(price), asset_id)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Monitor start error: {e}")

    return {"ok": True}


@app.post("/api/accounts/{account_id}/strategy/manual/stop")
async def account_manual_stop_v2(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if inst.strategy_d.running:
        await inst.strategy_d.stop()
    # Stop monitor (bound to strategy)
    if inst.poly_monitor.running or inst.poly_monitor.state.enabled:
        await inst.poly_monitor.stop()
    return {"ok": True}








# --- POLY MONITOR (risk control) ---
@app.post("/api/accounts/{account_id}/monitor/start")
async def account_monitor_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    poly_token_id = body.get("poly_token_id", "")
    reference_price = float(body.get("reference_price", 0))
    depth_threshold = float(body.get("depth_threshold", 10000))
    if not poly_token_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "poly_token_id required"})
    result = await inst.poly_monitor.start(poly_token_id, reference_price, depth_threshold)
    return result



@app.post("/api/accounts/{account_id}/rebalance/toggle")
async def account_rebalance_toggle(account_id: str, request: Request, user: str = Depends(verify_auth)):
    """Toggle auto-rebalance on/off for an account."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    enabled = body.get("enabled", not inst.auto_rebalance.enabled)
    inst.auto_rebalance.enabled = enabled
    return {"ok": True, "enabled": inst.auto_rebalance.enabled}


@app.get("/api/accounts/{account_id}/rebalance/status")
async def account_rebalance_status(account_id: str, user: str = Depends(verify_auth)):
    """Get auto-rebalance status including cost layers."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    return inst.auto_rebalance.status()


@app.post("/api/accounts/{account_id}/rebalance/clear-layers")
async def account_rebalance_clear(account_id: str, user: str = Depends(verify_auth)):
    """Clear cost layers (reset position tracking)."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    inst.auto_rebalance.cost_layers.clear()
    inst.auto_rebalance._save_layers()
    return {"ok": True}

# ─── Sniper Endpoints ───

@app.post("/api/accounts/{account_id}/sniper/start")
async def sniper_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if inst.strategy_s.running or inst.strategy_d.running:
        return {"ok": False, "error": "Cannot run sniper while MD/MS is active"}
    body = await request.json()
    result = await inst.sniper.start(
        yes_token_id=body.get("yes_token_id", ""),
        no_token_id=body.get("no_token_id", ""),
        yes_asset_id=body.get("yes_asset_id", ""),
        no_asset_id=body.get("no_asset_id", ""),
        market_id=body.get("market_id", ""),
        threshold_cents=int(body.get("threshold_cents", 40)),
        ask_levels=int(body.get("ask_levels", 1)),
        position_pct=int(body.get("position_pct", 100)),
        timeout_secs=int(body.get("timeout_secs", 120)),
        settle_ms=int(body.get("settle_ms", 1000)),
    )
    return result


@app.post("/api/accounts/{account_id}/sniper/stop")
async def sniper_stop(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    await inst.sniper.stop()
    return {"ok": True}


@app.get("/api/accounts/{account_id}/sniper/status")
async def sniper_status(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    return inst.sniper.get_state()




@app.post("/api/accounts/{account_id}/monitor/stop")
async def account_monitor_stop(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    await inst.poly_monitor.stop()
    return {"ok": True}


@app.get("/api/accounts/{account_id}/monitor/status")
async def account_monitor_status(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    return inst.poly_monitor.status()


@app.post("/api/accounts/{account_id}/strategy/manual-s/start")
async def account_manual_s_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    # Mutex check
    if inst.strategy_s.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-S already running."})
    if inst.strategy_d.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-D is running. Stop it first."})
    if inst.sniper.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Sniper is running. Stop it first."})
    body = await request.json()
    asset_id = body.get("asset_id", "")
    price = body.get("price", 0)
    side = body.get("side", "YES")
    market_id = body.get("market_id", "")
    no_asset_id = body.get("no_asset_id", "")
    if not asset_id or not price:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Select market and set price"})
    if not market_id:
        try:
            mkts = await inst.client.get_events(status="active", page_size=50)
            if isinstance(mkts, dict):
                for ev in mkts.get("data", {}).get("events", []):
                    for mkt in ev.get("markets", []):
                        if mkt.get("yesOutcome", {}).get("assetId") == asset_id:
                            market_id = mkt.get("marketId", "")
                            no_asset_id = mkt.get("noOutcome", {}).get("assetId", "")
                            break
                    if market_id:
                        break
        except Exception:
            pass
    if not no_asset_id:
        no_asset_id = str(int(asset_id) + 100)
    # Monitor validation if enabled
    monitor = body.get("monitor")
    price_decimal = float(price) / 100.0 if float(price) >= 1 else float(price)
    if monitor and monitor.get("poly_token_id"):
        poly_token_id = monitor["poly_token_id"]
        # Check OKX best bid == strategy price
        try:
            okx_book = await inst.client.get_orderbook(asset_id, depth=5)
            okx_bids = []
            if str(okx_book.get("code", "")) == "0":
                book_data = okx_book.get("data", [{}])
                if book_data and isinstance(book_data, list):
                    okx_bids = book_data[0].get("bids", [])
            if not okx_bids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "No OKX bids found"})
            okx_best_bid = float(okx_bids[0][0])
            if abs(okx_best_bid - price_decimal) > 0.02:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX best bid ({okx_best_bid:.4f}) != price ({price_decimal:.4f})"})
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX book check: {e}"})
        # Check Poly best bid
        try:
            import aiohttp as _aiohttp
            poly_url_check = f"https://clob.polymarket.com/book?token_id={poly_token_id}"
            async with _aiohttp.ClientSession() as _sess:
                async with _sess.get(poly_url_check, timeout=_aiohttp.ClientTimeout(total=5)) as _resp:
                    poly_book = await _resp.json()
            poly_bids = sorted(poly_book.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
            if not poly_bids:
                return JSONResponse(status_code=400, content={"ok": False, "error": "No Poly bids found"})
            poly_best_bid = float(poly_bids[0].get("price", 0))
            if abs(poly_best_bid - price_decimal) > 0.005:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly best bid ({poly_best_bid:.4f}) != price ({price_decimal:.4f})"})
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly book check: {e}"})

    try:
        await inst.strategy_s.start(market_id=market_id, yes_asset_id=asset_id, no_asset_id=no_asset_id, price_a=float(price), side=side)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # Auto-start monitor if requested
    if monitor and monitor.get("poly_token_id"):
        try:
            r = await inst.poly_monitor.start(
                poly_token_id=monitor["poly_token_id"],
                reference_price=price_decimal,
                depth_threshold=float(monitor.get("depth_threshold", 10000)),
            )
            if r and r.get("ok"):
                inst.poly_monitor.set_strategy_context("manual-s", int(price), asset_id)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Monitor start error: {e}")

    return {"ok": True}


@app.post("/api/accounts/{account_id}/strategy/manual-s/stop")
async def account_manual_s_stop(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if inst.strategy_s.running:
        await inst.strategy_s.stop()
    # Stop monitor (bound to strategy)
    if inst.poly_monitor.running or inst.poly_monitor.state.enabled:
        await inst.poly_monitor.stop()
    return {"ok": True}


@app.get("/api/accounts/{account_id}/config")
async def account_config_v2(account_id: str, user: str = Depends(verify_auth)):
    from .core.database import get_account_decrypted, get_global_config
    acc = get_account_decrypted(account_id)
    if not acc:
        raise HTTPException(404, "Account not found")
    tg_chats_str = get_global_config("tg_chats", "")
    # Derive region from base_url
    base_url = acc.get("base_url", "https://us.okx.com")
    region = "CN" if "www.okx.com" in base_url else "AU"
    return {
        "api_key": acc.get("api_key", ""),
        "secret_key": acc.get("secret_key", ""),
        "passphrase": acc.get("passphrase", ""),
        "agent_key": acc.get("agent_private_key", ""),
        "region": region,
        "tg_token": get_global_config("tg_token", ""),
        "tg_chats": [c.strip() for c in tg_chats_str.split(",") if c.strip()] if tg_chats_str else [],
        "depth_threshold": acc.get("depth_threshold", 90),
        "price_tick": acc.get("price_tick", 0),
    }


@app.post("/api/accounts/{account_id}/config")
async def save_account_config_v2(account_id: str, request: Request, user: str = Depends(verify_auth)):
    from .core.database import update_account as db_update
    body = await request.json()
    kwargs = {}
    if "depth_threshold" in body:
        kwargs["depth_threshold"] = int(body["depth_threshold"])
    if "price_tick" in body:
        kwargs["price_tick"] = int(body["price_tick"])
    # API credentials
    if body.get("api_key"):
        kwargs["api_key"] = body["api_key"]
    if body.get("secret_key"):
        kwargs["secret_key"] = body["secret_key"]
    if body.get("passphrase"):
        kwargs["passphrase"] = body["passphrase"]
    if body.get("agent_key"):
        kwargs["agent_private_key"] = body["agent_key"]
    # Region -> base_url
    if "region" in body:
        region = body["region"]
        if region == "CN":
            kwargs["base_url"] = "https://www.okx.com"
        else:
            kwargs["base_url"] = "https://us.okx.com"
    if kwargs:
        db_update(account_id, **kwargs)
        # Reload client if credentials or region changed
        cred_keys = {"api_key", "secret_key", "passphrase", "agent_private_key", "base_url"}
        if cred_keys & set(kwargs.keys()):
            inst = manager.get_instance(account_id)
            if inst:
                await inst.reload_client()
    return {"ok": True, "reloaded": True}


@app.post("/api/accounts/{account_id}/notify/test")
async def account_notify_test_v2(account_id: str, user: str = Depends(verify_auth)):
    if manager.notifier:
        try:
            await manager.notifier.send("OKX Bot notification test - OK")
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "Notifier not configured"}


@app.get("/api/accounts/{account_id}/health")
async def account_health_v2(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    errors = []
    try:
        data = await inst.client.get_balance()
        if isinstance(data, dict) and data.get("code") and str(data.get("code")) != "0":
            errors.append(f"OKX API: {data.get('msg')} ({data.get('code')})")
    except Exception as e:
        errors.append(f"OKX connection failed: {e}")
    return {"healthy": len(errors) == 0, "errors": errors}


# ─── Global routes (proxy latency, global config, notifications) ───

@app.get("/api/proxy/latency")
async def proxy_latency(user: str = Depends(verify_auth)):
    """Test latency to OKX via each proxy node."""
    import aiohttp
    results = {}
    for node_id, node in PROXY_NODES.items():
        entry = {"okx_ms": -1, "jp2_ms": 0 if node["host"] is None else -1}
        try:
            if node["host"] is None:
                # Direct connection
                async with aiohttp.ClientSession() as sess:
                    t0 = time.time()
                    async with sess.get("https://us.okx.com/api/v5/public/time", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            entry["okx_ms"] = int((time.time() - t0) * 1000)
            else:
                # Via SOCKS5 proxy
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(f"socks5://{node['host']}:{node['port']}")
                async with aiohttp.ClientSession(connector=connector) as sess:
                    t0 = time.time()
                    async with sess.get("https://us.okx.com/api/v5/public/time", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            entry["okx_ms"] = int((time.time() - t0) * 1000)
        except Exception:
            pass
        results[node_id] = entry
    return {"latencies": results}


@app.get("/api/proxy/latency/{node_id}")
async def proxy_latency_single(node_id: str, user: str = Depends(verify_auth)):
    """Test latency to OKX via a single proxy node."""
    import aiohttp
    if node_id not in PROXY_NODES:
        return {"okx_ms": -1, "error": "unknown node"}
    node = PROXY_NODES[node_id]
    ms = -1
    try:
        if node["host"] is None:
            async with aiohttp.ClientSession() as sess:
                t0 = time.time()
                async with sess.get("https://us.okx.com/api/v5/public/time", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        ms = int((time.time() - t0) * 1000)
        else:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(f"socks5://{node['host']}:{node['port']}")
            async with aiohttp.ClientSession(connector=connector) as sess:
                t0 = time.time()
                async with sess.get("https://us.okx.com/api/v5/public/time", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        ms = int((time.time() - t0) * 1000)
    except Exception:
        pass
    return {"okx_ms": ms, "node_id": node_id}



@app.get("/api/poly/resolve")
async def poly_resolve(url: str = "", user: str = Depends(verify_auth)):
    """Resolve a Polymarket URL/slug to token_ids."""
    import aiohttp, re, ast
    if not url:
        return {"ok": False, "error": "No URL provided"}
    
    # If already a raw token_id (long number), validate it
    if re.match(r'^\d{30,}$', url):
        return {"ok": True, "tokens": [{"token_id": url, "outcome": "YES"}]}
    
    # Extract slug from URL - support multiple formats:
    # /event/slug-name, /sports/category/slug-name, /markets/slug-name
    slug = None
    is_event_slug = False
    
    m = re.search(r'polymarket\.com/event/([^/?#]+)', url)
    if m:
        slug = m.group(1)
    else:
        # /sports/world-cup/fifwc-bra-hai-2026-06-19 -> last segment is event slug
        m = re.search(r'polymarket\.com/sports/[^/]+/([^/?#]+)', url)
        if m:
            slug = m.group(1)
            is_event_slug = True
        else:
            # /markets/slug or any other path
            m = re.search(r'polymarket\.com/[^/]+/([^/?#]+)', url)
            if m:
                slug = m.group(1)
            elif re.match(r'^[a-z0-9][-a-z0-9]*$', url):
                slug = url  # User pasted just the slug
    
    if not slug:
        return {"ok": False, "error": "Cannot parse URL. Paste a Polymarket event/sports URL or a token_id."}
    
    try:
        async with aiohttp.ClientSession() as sess:
            # Strategy: try markets API first, then events API
            # For sports URLs, try events first (more likely to match)
            apis = []
            if is_event_slug:
                apis = [
                    f"https://gamma-api.polymarket.com/events?slug={slug}",
                    f"https://gamma-api.polymarket.com/markets?slug={slug}",
                ]
            else:
                apis = [
                    f"https://gamma-api.polymarket.com/markets?slug={slug}",
                    f"https://gamma-api.polymarket.com/events?slug={slug}",
                ]
            
            for api_url in apis:
                async with sess.get(api_url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    if not data:
                        continue
                    
                    item = data[0]
                    
                    # Check if this is an event (has nested markets) or a market
                    if "markets" in item and item["markets"]:
                        # Event with multiple markets (e.g. sports: Win/Draw/Lose)
                        all_tokens = []
                        for mkt in item["markets"]:
                            tokens_raw = mkt.get("clobTokenIds", "[]")
                            tokens = ast.literal_eval(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
                            outcomes_raw = mkt.get("outcomes", "[]")
                            outcomes = ast.literal_eval(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                            q = mkt.get("question", "")
                            for i, tid in enumerate(tokens):
                                outcome = outcomes[i] if i < len(outcomes) else f"Outcome {i}"
                                all_tokens.append({"token_id": tid, "outcome": f"{q} - {outcome}"})
                        return {
                            "ok": True,
                            "question": item.get("title", ""),
                            "tokens": all_tokens,
                            "is_event": True
                        }
                    else:
                        # Single market
                        tokens_raw = item.get("clobTokenIds", "[]")
                        tokens = ast.literal_eval(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
                        outcomes_raw = item.get("outcomes", "[]")
                        outcomes = ast.literal_eval(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                        result = []
                        for i, tid in enumerate(tokens):
                            outcome = outcomes[i] if i < len(outcomes) else f"Outcome {i}"
                            result.append({"token_id": tid, "outcome": outcome})
                        return {
                            "ok": True,
                            "question": item.get("question", ""),
                            "tokens": result
                        }
            
            # Nothing found - try slug_contains as last resort
            api_url = f"https://gamma-api.polymarket.com/markets?slug_contains={slug}&closed=false&limit=3"
            async with sess.get(api_url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        market = data[0]
                        tokens_raw = market.get("clobTokenIds", "[]")
                        tokens = ast.literal_eval(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
                        outcomes_raw = market.get("outcomes", "[]")
                        outcomes = ast.literal_eval(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                        result = []
                        for i, tid in enumerate(tokens):
                            outcome = outcomes[i] if i < len(outcomes) else f"Outcome {i}"
                            result.append({"token_id": tid, "outcome": outcome})
                        return {
                            "ok": True,
                            "question": market.get("question", ""),
                            "tokens": result
                        }
            
            return {"ok": False, "error": f"No market found for: {slug}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/poly/book")
async def poly_book(token_id: str = "", user: str = Depends(verify_auth)):
    """Proxy to Polymarket CLOB orderbook."""
    if not token_id:
        return {"bids": [], "asks": []}
    import aiohttp
    try:
        async with aiohttp.ClientSession() as sess:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Sort: bids high->low, asks low->high
                    bids = sorted(data.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
                    asks = sorted(data.get("asks", []), key=lambda x: float(x.get("price", 0)))
                    return {"bids": bids, "asks": asks}
                return {"bids": [], "asks": [], "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"bids": [], "asks": [], "error": str(e)}


@app.get("/api/global/config")
async def get_global_config_route(user: str = Depends(verify_auth)):
    return {
        "tg_token": config.get("telegram", {}).get("bot_token", ""),
        "tg_chats": ",".join(str(c) for c in config.get("telegram", {}).get("chat_ids", []))
    }


@app.post("/api/global/config")
async def save_global_config(request: Request, user: str = Depends(verify_auth)):
    body = await request.json()
    import yaml
    if "telegram" not in config:
        config["telegram"] = {}
    config["telegram"]["bot_token"] = body.get("tg_token", "")
    chats_raw = body.get("tg_chats", "")
    if isinstance(chats_raw, str):
        config["telegram"]["chat_ids"] = [c.strip() for c in chats_raw.split(",") if c.strip()]
    else:
        config["telegram"]["chat_ids"] = chats_raw
    # Save to disk
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "w") as cf:
        yaml.dump(config, cf, default_flow_style=False)
    # Update live notifier
    if manager.notifier:
        manager.notifier.token = body.get("tg_token", "")
        manager.notifier.chat_ids = [c.strip() for c in body.get("tg_chats", "").split(",") if c.strip()]
        manager.notifier._session = None  # Force new session with new token
    return {"ok": True}


@app.post("/api/global/notify/test")
async def test_global_notify(user: str = Depends(verify_auth)):
    try:
        import aiohttp
        token = config.get("telegram", {}).get("bot_token", "")
        chats = config.get("telegram", {}).get("chat_ids", [])
        if not token or not chats:
            return {"ok": False, "error": "No token or chat_ids configured"}
        async with aiohttp.ClientSession() as client:
            for chat_id in chats:
                await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": "Health check test notification"}
                )
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

