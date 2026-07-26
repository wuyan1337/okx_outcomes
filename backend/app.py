"""FastAPI backend for OKX Outcomes Bot — Multi-Account System."""

import asyncio
import json
import logging
import os
import secrets
import time
from pathlib import Path
from contextlib import asynccontextmanager
from functools import wraps
from typing import Optional

import yaml
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse

from .core.database import (
    init_db, get_accounts, get_account, get_account_decrypted,
    get_available_proxies, get_used_proxies, PROXY_NODES,
    get_global_config, set_global_config, update_account, delete_account,
)
from .services.account_manager import AccountManager
from .services.notifier import TelegramNotifier
from .services.size_utils import normalize_size_str
from .services.latency_probe import (
    DEFAULT_WARM_SAMPLES,
    OKX_BASE_URLS,
    benchmark_proxy_node,
    benchmark_proxy_nodes,
    normalise_region,
    normalise_samples,
)
from .core.price_utils import (
    align_okx_price,
    as_decimal,
    cents_to_price,
    format_okx_price,
    invert_binary_price,
    price_to_cents,
)

# Configure application-level logging so notifier/strategy messages appear in journalctl
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# Load file-backed server settings and the one-time Telegram migration source.
config_path = Path(__file__).parent.parent / "config.yaml"
with open(config_path, encoding="utf-8") as f:
    config = yaml.safe_load(f)

manager = AccountManager()


def _sniper_busy(instance) -> bool:
    sniper = getattr(instance, "sniper", None)
    if sniper is None:
        return False
    return bool(
        sniper.running
        or getattr(sniper, "starting", False)
        or getattr(getattr(sniper, "state", None), "locked", False)
        or getattr(sniper, "_persistence_failed", False)
    )


def _account_trading_busy(instance) -> bool:
    monitor_enabled = bool(
        getattr(getattr(instance.poly_monitor, "state", None), "enabled", False)
    )
    rebalancing = bool(
        getattr(getattr(instance, "auto_rebalance", None), "_rebalancing", False)
    )
    return bool(
        instance.strategy_d.running
        or instance.strategy_s.running
        or _sniper_busy(instance)
        or instance.sniper_old.running
        or instance.poly_monitor.running
        or monitor_enabled
        or rebalancing
    )


def _risk_control_busy(instance) -> bool:
    return bool(
        instance.poly_monitor.running
        or getattr(getattr(instance.poly_monitor, "state", None), "enabled", False)
        or getattr(getattr(instance, "auto_rebalance", None), "_rebalancing", False)
    )


def _serialize_strategy_lifecycle(handler):
    """Serialize web start/stop operations for one account."""
    @wraps(handler)
    async def wrapped(account_id: str, *args, **kwargs):
        instance = manager.get_instance(account_id)
        if instance is None:
            return await handler(account_id, *args, **kwargs)
        lock = getattr(instance, "strategy_lifecycle_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            instance.strategy_lifecycle_lock = lock
        async with lock:
            return await handler(account_id, *args, **kwargs)

    return wrapped

# Persistent OKX asset -> Polymarket token/url map used by Ask Sell
POLY_ASSET_MAP_PATH = Path(__file__).parent.parent / "data" / "poly_asset_map.json"


def _load_poly_asset_map() -> dict:
    try:
        if POLY_ASSET_MAP_PATH.exists():
            with open(POLY_ASSET_MAP_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load poly asset map: {e}")
    return {}


def _save_poly_asset_map(data: dict):
    try:
        POLY_ASSET_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(POLY_ASSET_MAP_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save poly asset map: {e}")


def _record_poly_asset_mapping(account_id: str, market_id: str, yes_asset_id: str, no_asset_id: str,
                               yes_token_id: str, no_token_id: str, poly_url: str = "",
                               selected_outcome: str = ""):
    import time as _time
    data = _load_poly_asset_map()
    now = int(_time.time() * 1000)
    if yes_asset_id and yes_token_id:
        data[yes_asset_id] = {"account_id": account_id, "okx_market_id": market_id, "okx_asset_id": yes_asset_id,
                              "side": "yes", "poly_token_id": yes_token_id, "poly_url": poly_url or "",
                              "selected_outcome": selected_outcome or "", "updated_at": now}
    if no_asset_id and no_token_id:
        data[no_asset_id] = {"account_id": account_id, "okx_market_id": market_id, "okx_asset_id": no_asset_id,
                             "side": "no", "poly_token_id": no_token_id, "poly_url": poly_url or "",
                             "selected_outcome": selected_outcome or "", "updated_at": now}
    _save_poly_asset_map(data)


def _record_monitor_mapping(account_id: str, market_id: str, yes_asset_id: str, no_asset_id: str,
                            monitor: dict, fallback_poly_token_id: str = "", selected_side: str = ""):
    if not monitor:
        return
    yes_token_id = monitor.get("yes_token_id", "") or ""
    no_token_id = monitor.get("no_token_id", "") or ""
    poly_token_id = monitor.get("poly_token_id", fallback_poly_token_id) or ""
    side = (selected_side or "").lower()
    if not yes_token_id and not no_token_id and poly_token_id:
        if side == "no":
            no_token_id = poly_token_id
        else:
            yes_token_id = poly_token_id
    _record_poly_asset_mapping(
        account_id=account_id,
        market_id=market_id,
        yes_asset_id=yes_asset_id,
        no_asset_id=no_asset_id,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        poly_url=monitor.get("poly_url", ""),
        selected_outcome=monitor.get("selected_outcome", ""),
    )


def _monitor_poly_side(monitor: dict) -> str:
    explicit = str(monitor.get("poly_side", "") or "").lower()
    if explicit in {"yes", "no"}:
        return explicit
    token_id = str(monitor.get("poly_token_id", "") or "")
    if token_id and token_id == str(monitor.get("no_token_id", "") or ""):
        return "no"
    return "yes"


def _okx_price_str(price, side: str) -> str:
    return format_okx_price(price, rounding="buy" if side.lower() == "buy" else "sell")

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
    telegram_config = config.get("telegram", {})
    if not get_global_config("tg_token", "") and telegram_config.get("bot_token"):
        set_global_config("tg_token", str(telegram_config.get("bot_token", "")))
    if not get_global_config("tg_chats", "") and telegram_config.get("chat_ids"):
        set_global_config(
            "tg_chats",
            ",".join(str(chat_id) for chat_id in telegram_config.get("chat_ids", [])),
        )
    await manager.init()

    yield
    await manager.close_all()


def _normalise_base_path(value: str) -> str:
    value = (value or "").strip()
    if not value or value == "/":
        return ""
    return "/" + value.strip("/")


APP_BASE_PATH = _normalise_base_path(
    os.getenv("OKX_BASE_PATH", config.get("server", {}).get("base_path", ""))
)


class BasePathMiddleware:
    """Accept either proxy-stripped or prefix-preserving HTTP/WS requests."""

    def __init__(self, app, configured_prefix: str = ""):
        self.app = app
        self.configured_prefix = configured_prefix

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        forwarded = headers.get(b"x-forwarded-prefix", b"").decode("latin-1")
        prefix = self.configured_prefix or _normalise_base_path(forwarded)
        path = scope.get("path", "")
        if prefix and (path == prefix or path.startswith(prefix + "/")):
            scope = dict(scope)
            scope["root_path"] = prefix
            scope["path"] = path[len(prefix):] or "/"
            scope["raw_path"] = scope["path"].encode("utf-8")
        elif prefix:
            scope = dict(scope)
            scope["root_path"] = prefix
        await self.app(scope, receive, send)


app = FastAPI(title="OKX Outcomes Bot", lifespan=lifespan, root_path=APP_BASE_PATH)
app.add_middleware(BasePathMiddleware, configured_prefix=APP_BASE_PATH)

# Register WebSocket routes for real-time orderbook
from .ws_book import register_ws_routes
register_ws_routes(app, manager)

# ─── Static pages ───

frontend_dir = Path(__file__).parent.parent / "frontend"


@app.get("/", response_class=HTMLResponse)
async def serve_root():
    return HTMLResponse(content=(frontend_dir / "login.html").read_text(encoding="utf-8"), status_code=200)


@app.get("/login", response_class=HTMLResponse)
async def serve_login():
    return HTMLResponse(content=(frontend_dir / "login.html").read_text(encoding="utf-8"), status_code=200)


@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    return HTMLResponse(content=(frontend_dir / "dashboard.html").read_text(encoding="utf-8"), status_code=200)


@app.get("/sniper-help", response_class=HTMLResponse)
async def sniper_help():
    return HTMLResponse(content=(frontend_dir / "sniper-help.html").read_text(encoding="utf-8"), status_code=200)


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
            mode = getattr(inst.strategy_s.state, "mode", "classic")
            running_strategy = "Bid-Sell" if mode == "bid-sell" else "Manual-S"
        elif _sniper_busy(inst):
            running_strategy = "Sniper" if inst.sniper.running else "Sniper (locked)"
        elif inst.sniper_old.running:
            running_strategy = "Sniper-Old"
        elif getattr(inst.auto_rebalance, "_rebalancing", False):
            running_strategy = "Auto-Rebalance"
        elif inst.poly_monitor.running or getattr(inst.poly_monitor.state, "enabled", False):
            running_strategy = "Poly Monitor"
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
                for position in pos_list:
                    try:
                        if as_decimal(position.get("size", "0")) != 0:
                            acct_positions += 1
                    except ValueError:
                        continue
        except Exception:
            pass
        accounts_info.append({
            "id": inst.account_id,
            "name": inst.name,
            "proxy_node": inst.proxy_node,
            "proxy_label": PROXY_NODES.get(inst.proxy_node, {}).get("label", inst.proxy_node),
            "region": "CN" if "www.okx.com" in inst.client.base_url else "AU",
            "running": _account_trading_busy(inst),
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
            running = _account_trading_busy(inst)
            if inst.strategy_d.running:
                strategy = "Manual-D"
            elif inst.strategy_s.running:
                strategy = (
                    "Bid-Sell"
                    if getattr(inst.strategy_s.state, "mode", "classic") == "bid-sell"
                    else "Manual-S"
                )
            elif _sniper_busy(inst):
                strategy = "Sniper" if inst.sniper.running else "Sniper (locked)"
            elif inst.sniper_old.running:
                strategy = "Sniper-Old"
            elif getattr(inst.auto_rebalance, "_rebalancing", False):
                strategy = "Auto-Rebalance"
            elif inst.poly_monitor.running or getattr(inst.poly_monitor.state, "enabled", False):
                strategy = "Poly Monitor"
        result.append({
            "id": acc["id"],
            "name": acc["name"],
            "proxy_node": acc["proxy_node"],
            "proxy_label": PROXY_NODES.get(acc["proxy_node"], {}).get("label", acc["proxy_node"]),
            "region": "CN" if "www.okx.com" in acc.get("base_url", "") else "AU",
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
        nodes.append({
            "id": k,
            "label": v["label"],
            "location": v.get("location", "Unknown"),
            "route": v.get("route", "direct" if v.get("host") is None else "socks5"),
            "used": k in used,
        })
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
    if region not in {"AU", "CN"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": "region must be AU or CN"})

    try:
        from .core.auth import OKXSigner
        if not OKXSigner(agent_key).address:
            raise ValueError("Invalid agent private key")
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
        await _ensure_market_cache(inst)
        data = await inst.client.get_positions(status="open")
        positions = []
        if isinstance(data, dict) and str(data.get("code", "")) == "0":
            for p in data.get("data", {}).get("list", []):
                mid = p.get("marketId", "")
                token_name = p.get("tokenName", "Yes")
                # OKX positions don't include assetId — derive from marketId + tokenIndex
                # tokenIndex "1" = YES -> {marketId}000, tokenIndex "2" = NO -> {marketId}100
                token_idx = str(p.get("tokenIndex", "1"))
                yes_aid = f"{mid}000" if mid else ""
                no_aid = f"{mid}100" if mid else ""
                aid = no_aid if token_idx == "2" or token_name.upper() == "NO" else yes_aid
                positions.append({
                    "market_id": mid,
                    "asset_id": aid,
                    "yes_asset_id": yes_aid,
                    "token_name": token_name,
                    "size": p.get("size", "0"),
                    "available_size": p.get("availableSize", p.get("size", "0")),
                    "avg_price": p.get("avgPrice", "0"),
                    "market_name": _market_name_cache.get(mid, ""),
                })
        return {"positions": positions}
    except Exception as e:
        return {"positions": [], "error": str(e)}


@app.get("/api/markets")
async def api_markets(user: str = Depends(verify_auth)):
    accounts = get_accounts()
    if not accounts:
        return {"markets": [], "error": "No accounts configured"}
    inst = None
    for acct in accounts:
        inst = manager.get_instance(acct.get("id"))
        if inst:
            break
    if not inst:
        return {"markets": [], "error": "No active account instance"}
    try:
        data = await inst.client.get_events(status="active", page_size=50)
        markets = []
        if isinstance(data, dict):
            events = data.get("data", {}).get("events", [])
            for ev in events:
                for mkt in ev.get("markets", []):
                    yes_out = mkt.get("yesOutcome", {})
                    no_out = mkt.get("noOutcome", {})
                    if yes_out.get("assetId") and no_out.get("assetId"):
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
                    if yes_out.get("assetId") and no_out.get("assetId"):
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


def _invert_okx_book(book: dict) -> dict:
    """OKX exposes only YES pm-books; synthesize NO by inverting YES book."""
    bids = []
    asks = []
    for a in book.get("asks", []) or []:
        if not a:
            continue
        row = list(a)
        row[0] = format_okx_price(invert_binary_price(a[0]))
        bids.append(row)
    for b in book.get("bids", []) or []:
        if not b:
            continue
        row = list(b)
        row[0] = format_okx_price(invert_binary_price(b[0]))
        asks.append(row)
    bids.sort(key=lambda x: float(x[0]), reverse=True)
    asks.sort(key=lambda x: float(x[0]))
    return {"bids": bids, "asks": asks}


@app.get("/api/accounts/{account_id}/orderbook")
async def account_orderbook(account_id: str, asset_id: str, yes_asset_id: str = "", user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        query_asset = yes_asset_id or asset_id
        invert = bool(yes_asset_id and yes_asset_id != asset_id)
        data = await inst.client.get_orderbook(query_asset, depth=10)
        if isinstance(data, dict) and "data" in data:
            book = data["data"]
            if isinstance(book, list) and len(book) > 0:
                raw = {"bids": book[0].get("bids", []), "asks": book[0].get("asks", [])}
                return _invert_okx_book(raw) if invert else raw
        return {"bids": [], "asks": [], "raw": data if isinstance(data, dict) else None}
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
        mode = getattr(inst.strategy_s.state, "mode", "classic")
        strategy = "bid-sell" if mode == "bid-sell" else "manual-s"
    elif _sniper_busy(inst):
        strategy = "sniper"
    elif inst.sniper_old.running:
        strategy = "sniper-old"
    elif getattr(inst.auto_rebalance, "_rebalancing", False):
        strategy = "auto-rebalance"
    elif inst.poly_monitor.running or getattr(inst.poly_monitor.state, "enabled", False):
        strategy = "poly-monitor"
    return {
        "manual_running": inst.strategy_d.running,
        "manual_d_scan_interval": getattr(inst.strategy_d.state, "scan_interval", 0.3) if inst.strategy_d.running else 0.3,
        "manual_d_stabilize_secs": getattr(inst.strategy_d.state, "stabilize_secs", 0.3) if inst.strategy_d.running else 0.3,
        "manual_s_running": inst.strategy_s.running,
        "sniper_running": _sniper_busy(inst),
        "sniper_locked": getattr(inst.sniper.state, "locked", False),
        "sniper_old_running": inst.sniper_old.running,
        "sniper_status": inst.sniper.state.status,
        "sniper_yes_token": getattr(inst.sniper.state, "yes_token_id", ""),
        "sniper_no_token": getattr(inst.sniper.state, "no_token_id", ""),
        "sniper_yes_asset": getattr(inst.sniper.state, "yes_asset_id", ""),
        "sniper_no_asset": getattr(inst.sniper.state, "no_asset_id", ""),
        "sniper_poly_url": getattr(inst.sniper.state, "poly_url", ""),
        "sniper_selected_outcome": getattr(inst.sniper.state, "selected_outcome", ""),
        "sniper_old_status": inst.sniper_old.state.status if inst.sniper_old.running else "stopped",
        "sniper_old_yes_token": getattr(inst.sniper_old.state, "yes_token_id", "") if inst.sniper_old.running else "",
        "sniper_old_no_token": getattr(inst.sniper_old.state, "no_token_id", "") if inst.sniper_old.running else "",
        "sniper_old_yes_asset": getattr(inst.sniper_old.state, "yes_asset_id", "") if inst.sniper_old.running else "",
        "sniper_old_no_asset": getattr(inst.sniper_old.state, "no_asset_id", "") if inst.sniper_old.running else "",
        "sniper_old_poly_url": getattr(inst.sniper_old.state, "poly_url", "") if inst.sniper_old.running else "",
        "sniper_old_selected_outcome": getattr(inst.sniper_old.state, "selected_outcome", "") if inst.sniper_old.running else "",
        "manual_s_price": getattr(inst.strategy_s.state, "price_a", None) if inst.strategy_s.running else None,
        "manual_s_sell_price": getattr(inst.strategy_s.state, "sell_price", None) if inst.strategy_s.running else None,
        "manual_s_mode": getattr(inst.strategy_s.state, "mode", "classic") if inst.strategy_s.running else None,
        "manual_s_side": getattr(inst.strategy_s.state, "side", None) if inst.strategy_s.running else None,
        "manual_s_asset_id": getattr(inst.strategy_s.state, "asset_id", None) if inst.strategy_s.running else None,
        "manual_s_yes_asset_id": getattr(inst.strategy_s.state, "yes_asset_id", None) if inst.strategy_s.running else None,
        "manual_s_position_pct": getattr(inst.strategy_s.state, "position_pct", 100) if inst.strategy_s.running else 100,
        "manual_s_scan_interval": getattr(inst.strategy_s.state, "scan_interval", 0.3) if inst.strategy_s.running else 0.3,
        "manual_s_stabilize_secs": getattr(inst.strategy_s.state, "stabilize_secs", 0.3) if inst.strategy_s.running else 0.3,

        "strategy": strategy or "none",
        "running": _account_trading_busy(inst),
        "cycles": inst.strategy_d.cycles + inst.strategy_s.cycles,
        "last_heartbeat": getattr(inst.client, 'last_heartbeat', None),
        "manual_asset_id": inst.strategy_d.state.yes_asset_id if inst.strategy_d.state.running else (inst.strategy_s.state.yes_asset_id if inst.strategy_s.state.running else ''),
        "manual_price": inst.strategy_d.state.price_a if inst.strategy_d.state.running else (inst.strategy_s.state.price_a if inst.strategy_s.state.running else 0),
        "monitor": inst.poly_monitor.status(),
    }


@app.post("/api/accounts/{account_id}/strategy/start")
@_serialize_strategy_lifecycle
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
    if strategy_type not in {"manual-d", "manual-s"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"Unknown strategy: {strategy_type}"})
    if _account_trading_busy(inst):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Another strategy is already running"})
    side = str(side).lower()
    if side not in {"yes", "no"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": "side must be YES or NO"})
    try:
        price_cents = as_decimal(price)
        price_decimal = align_okx_price(cents_to_price(price_cents), "buy")
    except ValueError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

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
        expected_poly_side = "yes" if strategy_type == "manual-d" else side
        if _monitor_poly_side(monitor) != expected_poly_side:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": f"Monitor must use the Poly outcome mapped to OKX {expected_poly_side.upper()}"},
            )

        # Check OKX orderbook: best bid must match strategy price
        try:
            okx_book = await inst.client.get_orderbook(yes_asset_id, depth=5)
            okx_levels = []
            if str(okx_book.get("code", "")) == "0":
                book_data = okx_book.get("data", [{}])
                if book_data:
                    first_book = book_data[0] if isinstance(book_data, list) else book_data
                    okx_levels = first_book.get("asks" if expected_poly_side == "no" else "bids", [])
            if not okx_levels:
                return JSONResponse(status_code=400, content={"ok": False, "error": "Cannot get OKX orderbook for selected side"})
            okx_best_bid = (
                invert_binary_price(okx_levels[0][0])
                if expected_poly_side == "no"
                else as_decimal(okx_levels[0][0])
            )
            if okx_best_bid != price_decimal:
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": f"OKX best bid ({okx_best_bid}) != strategy price ({price_decimal}). Cannot start with monitor."
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
            poly_best_bid = as_decimal(poly_bids[0].get("price", 0))
            if align_okx_price(poly_best_bid, "buy") != price_decimal:
                return JSONResponse(status_code=400, content={
                    "ok": False,
                    "error": f"Poly best bid ({poly_best_bid}) != strategy price ({price_decimal}). Cannot start with monitor."
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
                price_a=str(price_cents), position_pct=int(body.get("position_pct", 100)))
        elif strategy_type == "manual-s":
            await inst.strategy_s.start(
                market_id=market_id,
                yes_asset_id=yes_asset_id,
                no_asset_id=no_asset_id,
                side=side,
                price_a=str(price_cents), position_pct=int(body.get("position_pct", 100)))
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # If monitor requested, start it now (strategy is already running)
    if monitor and monitor.get("poly_token_id"):
        try:
            result = await inst.poly_monitor.start(
                poly_token_id=monitor["poly_token_id"],
                reference_price=float(price_decimal),
                depth_threshold=float(monitor.get("depth_threshold", 3000)),
            )
            if result.get("ok"):
                strategy_asset = no_asset_id if expected_poly_side == "no" else yes_asset_id
                inst.poly_monitor.set_strategy_context(strategy_type, float(price_cents), strategy_asset)
                _record_monitor_mapping(account_id, market_id, yes_asset_id, no_asset_id, monitor, poly_token_id, side)
            else:
                if strategy_type == "manual-d":
                    await inst.strategy_d.stop()
                else:
                    await inst.strategy_s.stop()
                return JSONResponse(status_code=400, content={"ok": False, "error": result.get("error", "Monitor failed to start")})
        except Exception as e:
            if strategy_type == "manual-d":
                await inst.strategy_d.stop()
            else:
                await inst.strategy_s.stop()
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Monitor start error: {e}"})

    return {"ok": True}


@app.post("/api/accounts/{account_id}/strategy/stop")
@_serialize_strategy_lifecycle
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
    if inst.sniper_old.running:
        await inst.sniper_old.stop()
    # Always stop monitor when strategy stops (monitor is bound to strategy)
    if inst.poly_monitor.running or inst.poly_monitor.state.enabled:
        await inst.poly_monitor.stop()
    return {"ok": True}


async def _stop_account_trading(inst) -> None:
    """Stop every local task that could submit another account order."""
    auto_rebalance = getattr(inst, "auto_rebalance", None)
    if auto_rebalance is not None:
        await auto_rebalance.cancel()
    if inst.strategy_s.running:
        await inst.strategy_s.stop()
    if inst.strategy_d.running:
        await inst.strategy_d.stop()
    if inst.sniper.running:
        await inst.sniper.stop()
    if inst.sniper_old.running:
        await inst.sniper_old.stop()
    monitor_enabled = bool(
        getattr(getattr(inst.poly_monitor, "state", None), "enabled", False)
    )
    if inst.poly_monitor.running or monitor_enabled:
        await inst.poly_monitor.stop()


async def _query_open_orders_verified(client) -> list[dict]:
    response = await client.get_orders(status="open")
    if not isinstance(response, dict) or str(response.get("code", "")) != "0":
        message = (
            response.get("message", response.get("msg", "query failed"))
            if isinstance(response, dict)
            else "invalid response"
        )
        raise RuntimeError(f"Cannot verify open orders: {message}")
    data = response.get("data", {})
    if (
        not isinstance(data, dict)
        or "list" not in data
        or not isinstance(data.get("list"), list)
    ):
        raise RuntimeError("Cannot verify open orders: invalid response shape")
    return data["list"]


async def _cancel_open_orders_verified(client) -> dict:
    """Cancel the current account snapshot and prove that no order remains."""
    orders = await _query_open_orders_verified(client)
    if not orders:
        return {"ok": True, "cancelled": 0, "failed": 0, "total": 0, "method": "none"}

    cancels = [
        {"assetId": order.get("assetId", ""), "oid": order.get("oid", order.get("id", ""))}
        for order in orders
        if order.get("assetId") and (order.get("oid") or order.get("id"))
    ]
    remaining = orders
    if cancels:
        try:
            batch_response = await client.cancel_orders_batch(cancels)
            if isinstance(batch_response, dict) and str(batch_response.get("code", "")) == "0":
                await asyncio.sleep(0.5)
                try:
                    remaining = await _query_open_orders_verified(client)
                except Exception:
                    # Still make the safe, idempotent individual attempts. The
                    # final query below remains the only success condition.
                    remaining = orders
                if not remaining:
                    return {
                        "ok": True,
                        "cancelled": len(orders),
                        "failed": 0,
                        "total": len(orders),
                        "method": "batch",
                    }
        except Exception:
            remaining = orders

    async def _cancel_one(order: dict) -> bool:
        order_id = order.get("oid", order.get("id", ""))
        asset_id = order.get("assetId", "")
        if not order_id or not asset_id:
            return False
        try:
            response = await client.cancel_order(asset_id=asset_id, order_id=order_id)
            return isinstance(response, dict) and str(response.get("code", "")) == "0"
        except Exception:
            return False

    for _ in range(2):
        if remaining:
            await asyncio.gather(*[_cancel_one(order) for order in remaining])
            await asyncio.sleep(0.5)
        remaining = await _query_open_orders_verified(client)
        if not remaining:
            break

    return {
        "ok": not remaining,
        "cancelled": max(0, len(orders) - len(remaining)),
        "failed": len(remaining),
        "total": len(orders),
        "method": "parallel",
    }


async def _query_open_positions_verified(client) -> list[dict]:
    response = await client.get_positions(status="open")
    if not isinstance(response, dict) or str(response.get("code", "")) != "0":
        message = (
            response.get("message", response.get("msg", "query failed"))
            if isinstance(response, dict)
            else "invalid response"
        )
        raise RuntimeError(f"Cannot verify open positions: {message}")
    data = response.get("data", {})
    if (
        not isinstance(data, dict)
        or "list" not in data
        or not isinstance(data.get("list"), list)
    ):
        raise RuntimeError("Cannot verify open positions: invalid response shape")

    positions = []
    for position in data["list"]:
        try:
            size = as_decimal(position.get("size", position.get("availableSize", "0")))
        except ValueError as exc:
            raise RuntimeError("Cannot verify open positions: invalid position size") from exc
        if size > 0:
            positions.append(position)
    return positions


@app.post("/api/accounts/{account_id}/cancel_all")
@_serialize_strategy_lifecycle
async def account_cancel_all(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        await _stop_account_trading(inst)
        result = await _cancel_open_orders_verified(inst.client)
        if result["total"] == 0:
            return {"ok": True, "result": {"message": "No orders to cancel", "cancelled": 0}}
        if not result["ok"]:
            return {
                "ok": False,
                "error": f"Cancellation incomplete: {result['failed']} orders remain open",
                "result": result,
            }
        return {"ok": True, "result": result}
    except Exception as e:
        logger.error("Cancel All failed for account %s: %s", account_id, e)
        return {"ok": False, "error": str(e)}


@app.post("/api/accounts/{account_id}/ask_sell")
@_serialize_strategy_lifecycle
async def account_ask_sell(account_id: str, user: str = Depends(verify_auth)):
    """Place GTC sell orders for all positions at matching Polymarket ask1."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if _account_trading_busy(inst):
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": "Ask Sell is disabled while an account strategy is running or locked",
            },
        )
    import aiohttp
    import asyncio as _asyncio
    try:
        poly_map = _load_poly_asset_map()
        open_orders = await _query_open_orders_verified(inst.client)
        try:
            positions = await _query_open_positions_verified(inst.client)
        except Exception as exc:
            return {
                "ok": False,
                "error": f"Flash could not verify positions after cancellation: {exc}",
                "result": {
                    "cancelled": cancellation["cancelled"],
                    "submitted": 0,
                    "closed": 0,
                    "remaining": None,
                    "failures": [str(exc)],
                },
            }
        results = []
        placed = skipped = cancelled = 0
        async with aiohttp.ClientSession() as sess:
            for p in positions:
                mid = p.get("marketId", "")
                token_name = p.get("tokenName", "Yes")
                token_idx = str(p.get("tokenIndex", "1"))
                size = p.get("availableSize", p.get("size", "0"))
                try:
                    if not mid or float(size or 0) <= 0:
                        continue
                except Exception:
                    continue
                yes_aid = f"{mid}000"
                no_aid = f"{mid}100"
                aid = no_aid if token_idx == "2" or str(token_name).upper() == "NO" else yes_aid
                mapping = poly_map.get(aid)
                if not mapping or not mapping.get("poly_token_id"):
                    skipped += 1
                    results.append({"asset_id": aid, "size": size, "status": "skipped", "reason": "no_poly_mapping"})
                    continue
                token_id = mapping["poly_token_id"]
                try:
                    async with sess.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status != 200:
                            skipped += 1
                            results.append({"asset_id": aid, "size": size, "status": "skipped", "reason": f"poly_http_{resp.status}"})
                            continue
                        book = await resp.json()
                    asks = sorted(book.get("asks", []), key=lambda x: float(x.get("price", 0)))
                    if not asks:
                        skipped += 1
                        results.append({"asset_id": aid, "size": size, "status": "skipped", "reason": "no_poly_asks"})
                        continue
                    ask1 = float(asks[0].get("price", 0))
                    if ask1 <= 0:
                        skipped += 1
                        results.append({"asset_id": aid, "size": size, "status": "skipped", "reason": "bad_poly_ask"})
                        continue
                    price_str = _okx_price_str(ask1, "sell")
                    local_cancelled = 0
                    for o in open_orders:
                        oid = o.get("oid", o.get("id", ""))
                        if o.get("assetId") == aid and o.get("side", "").upper() == "SELL" and oid:
                            try:
                                cr = await inst.client.cancel_order(asset_id=aid, order_id=oid)
                                if str(cr.get("code", "")) == "0":
                                    cancelled += 1
                                    local_cancelled += 1
                            except Exception:
                                pass
                    if local_cancelled:
                        await _asyncio.sleep(0.2)
                    matching_open_sells = [
                        order
                        for order in await _query_open_orders_verified(inst.client)
                        if order.get("assetId") == aid
                        and str(order.get("side", "")).upper() == "SELL"
                    ]
                    if matching_open_sells:
                        skipped += 1
                        results.append({
                            "asset_id": aid,
                            "size": size,
                            "poly_ask1": price_str,
                            "status": "failed",
                            "reason": (
                                "existing sell order could not be confirmed cancelled"
                            ),
                        })
                        continue
                    sell_resp = await inst.client.place_order(asset_id=aid, side="sell", price=price_str,
                                                              size=normalize_size_str(size), tif="gtc", reduce_only=True)
                    if str(sell_resp.get("code", "")) == "0":
                        placed += 1
                        results.append({"asset_id": aid, "side": mapping.get("side", ""),
                                        "size": normalize_size_str(size), "poly_ask1": price_str,
                                        "poly_url": mapping.get("poly_url", ""), "status": "placed"})
                    else:
                        skipped += 1
                        results.append({"asset_id": aid, "size": size, "poly_ask1": price_str, "status": "failed",
                                        "reason": sell_resp.get("message", sell_resp.get("msg", str(sell_resp)))})
                except Exception as e:
                    skipped += 1
                    results.append({"asset_id": aid, "size": size, "status": "failed", "reason": str(e)})
        return {"ok": True, "placed": placed, "skipped": skipped, "cancelled": cancelled, "results": results}
    except Exception as e:
        logger.error("Ask Sell failed for account %s: %s", account_id, e)
        return {"ok": False, "error": str(e)}


@app.post("/api/accounts/{account_id}/flash_close")
@_serialize_strategy_lifecycle
async def account_flash_close(account_id: str, user: str = Depends(verify_auth)):
    """Stop account activity and prove the emergency close result."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        await _stop_account_trading(inst)
        cancellation = await _cancel_open_orders_verified(inst.client)
        if not cancellation["ok"]:
            return {
                "ok": False,
                "error": (
                    "Flash stopped before closing positions: "
                    f"{cancellation['failed']} orders remain open"
                ),
                "result": {
                    "cancelled": cancellation["cancelled"],
                    "submitted": 0,
                    "closed": 0,
                    "remaining": None,
                },
            }

        positions = await _query_open_positions_verified(inst.client)
        submitted = 0
        failures = []
        initial_assets = set()
        for position in positions:
            market_id = str(position.get("marketId", "") or "")
            token_index = str(position.get("tokenIndex", "") or "")
            token_name = str(position.get("tokenName", "") or "").upper()
            if not market_id:
                failures.append("position is missing marketId")
                continue
            if token_index == "2" or token_name == "NO":
                is_no = True
            elif token_index == "1" or token_name == "YES":
                is_no = False
            else:
                failures.append(f"{market_id}: unknown outcome")
                continue

            yes_asset_id = f"{market_id}000"
            trade_asset_id = f"{market_id}100" if is_no else yes_asset_id
            initial_assets.add(trade_asset_id)
            try:
                available_size = as_decimal(
                    position.get("availableSize", position.get("size", "0"))
                )
                if available_size <= 0:
                    failures.append(f"{trade_asset_id}: no available position to close")
                    continue

                book = await inst.client.get_orderbook(yes_asset_id, depth=1)
                if not isinstance(book, dict) or str(book.get("code", "0")) != "0":
                    raise RuntimeError("orderbook query failed")
                book_data = book.get("data", [])
                if not isinstance(book_data, list) or not book_data or not isinstance(book_data[0], dict):
                    raise RuntimeError("invalid orderbook response")
                if is_no:
                    asks = book_data[0].get("asks", [])
                    if not asks:
                        raise RuntimeError("NO best bid is unavailable")
                    best_bid = format_okx_price(invert_binary_price(asks[0][0]))
                else:
                    bids = book_data[0].get("bids", [])
                    if not bids:
                        raise RuntimeError("YES best bid is unavailable")
                    best_bid = format_okx_price(bids[0][0])

                response = await inst.client.place_order(
                    asset_id=trade_asset_id,
                    side="sell",
                    price=best_bid,
                    size=normalize_size_str(available_size),
                    tif="ioc",
                    reduce_only=True,
                )
                if not isinstance(response, dict) or str(response.get("code", "")) != "0":
                    message = (
                        response.get("message", response.get("msg", "order rejected"))
                        if isinstance(response, dict)
                        else "invalid order response"
                    )
                    raise RuntimeError(message)
                submitted += 1
            except Exception as exc:
                failures.append(f"{trade_asset_id}: {exc}")

        if submitted:
            await asyncio.sleep(0.5)

        # IOC acceptance is not proof of a fill. Re-read both account states
        # and report success only when the exchange confirms they are empty.
        progress = {
            "cancelled": cancellation["cancelled"],
            "submitted": submitted,
            "closed": 0,
            "remaining": None,
            "failures": list(failures),
        }
        try:
            final_cancellation = await _cancel_open_orders_verified(inst.client)
            progress["cancelled"] += final_cancellation["cancelled"]
            remaining_positions = await _query_open_positions_verified(inst.client)
        except Exception as exc:
            progress["failures"].append(f"final verification: {exc}")
            return {
                "ok": False,
                "error": f"Flash final verification failed: {exc}",
                "result": progress,
            }
        remaining_assets = set()
        for position in remaining_positions:
            market_id = str(position.get("marketId", "") or "")
            token_index = str(position.get("tokenIndex", "") or "")
            token_name = str(position.get("tokenName", "") or "").upper()
            if market_id:
                remaining_assets.add(
                    f"{market_id}100"
                    if token_index == "2" or token_name == "NO"
                    else f"{market_id}000"
                )
        closed = len(initial_assets - remaining_assets)
        progress["closed"] = closed
        progress["remaining"] = len(remaining_positions)
        result = progress
        if not final_cancellation["ok"]:
            return {
                "ok": False,
                "error": (
                    "Flash incomplete: "
                    f"{final_cancellation['failed']} orders remain open"
                ),
                "result": result,
            }
        if remaining_positions:
            return {
                "ok": False,
                "error": f"Flash incomplete: {len(remaining_positions)} positions remain open",
                "result": result,
            }
        sniper = getattr(inst, "sniper", None)
        if sniper is not None and getattr(
            getattr(sniper, "state", None), "locked", False
        ):
            try:
                reconciliation = await sniper.stop()
                still_locked = bool(
                    getattr(getattr(sniper, "state", None), "locked", False)
                    or (
                        isinstance(reconciliation, dict)
                        and reconciliation.get("needs_reconciliation")
                    )
                )
            except Exception as exc:
                still_locked = True
                result["failures"].append(f"Sniper reconciliation: {exc}")
            result["needs_reconciliation"] = still_locked
            if still_locked:
                return {
                    "ok": False,
                    "error": (
                        "Flash closed the verified exchange exposure, but "
                        "Sniper still requires reconciliation"
                    ),
                    "result": result,
                }
        return {"ok": True, "result": result}
    except Exception as e:
        logger.error("Flash close failed for account %s: %s", account_id, e)
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
        if not isinstance(result, dict) or str(result.get("code", "")) != "0":
            message = (
                result.get("message", result.get("msg", "cancellation rejected"))
                if isinstance(result, dict)
                else "invalid cancellation response"
            )
            return {"ok": False, "error": message, "result": result}
        return {"ok": True, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}




# ─── Manual Trading ───

# Market name cache: market_id -> question (auto-populated, persistent in memory)
_market_name_cache: dict = {}
_market_cache_populated = False  # whether we've done a full scan

async def _ensure_market_cache(inst):
    """Populate market name cache from active+settled+closed events. Only full-scans once."""
    global _market_cache_populated
    if _market_cache_populated:
        return
    try:
        for st in ("active", "settled", "closed"):
            try:
                events = await inst.client.get_events(status=st, page_size=50)
                if isinstance(events, dict):
                    for ev in events.get("data", {}).get("events", []):
                        for mkt in ev.get("markets", []):
                            _mid = mkt.get("marketId", "")
                            if _mid and _mid not in _market_name_cache:
                                q = mkt.get("question", "") or ev.get("eventTitle", "")
                                _market_name_cache[_mid] = q.replace("Will ", "").rstrip("?")
            except Exception:
                pass
        _market_cache_populated = True
    except Exception:
        pass

@app.get("/api/accounts/{account_id}/trades")
async def account_trades(account_id: str, limit: int = 50, asset_id: str = None, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    try:
        # Ensure cache is populated (once)
        await _ensure_market_cache(inst)

        data = await inst.client.get_orders(status="closed")
        trades = []
        missing_mids = set()
        if isinstance(data, dict) and str(data.get("code", "")) == "0":
            for o in data.get("data", {}).get("list", []):
                aid = o.get("assetId", "")
                if asset_id and aid != asset_id:
                    continue
                token = "Yes" if aid.endswith("000") else "No"
                mid = o.get("marketId", "")
                trades.append({
                    "order_id": o.get("oid", o.get("id", "")),
                    "side": o.get("side", ""),
                    "price": o.get("price", ""),
                    "size": o.get("size", ""),
                    "asset_id": aid,
                    "token": token,
                    "filled_at": o.get("updatedAt", o.get("createdAt", "")),
                    "market_id": mid,
                    "market_name": _market_name_cache.get(mid, ""),
                })
                if mid and mid not in _market_name_cache:
                    missing_mids.add(mid)
        # Fallback: fetch individual markets still missing from cache
        for sm in list(missing_mids)[:5]:
            try:
                mr = await inst.client.get_market(sm)
                if isinstance(mr, dict) and str(mr.get("code", "")) == "0":
                    mdata = mr.get("data", {})
                    q = mdata.get("question", "") or mdata.get("eventTitle", "")
                    if q:
                        _market_name_cache[sm] = q.replace("Will ", "").rstrip("?")
            except Exception:
                pass
        # Fill names for any that were missing
        if missing_mids:
            for t in trades:
                if not t["market_name"] and t["market_id"] in _market_name_cache:
                    t["market_name"] = _market_name_cache[t["market_id"]]
        trades.sort(key=lambda x: x["filled_at"], reverse=True)
        return {"trades": trades[:limit]}
    except Exception as e:
        return {"trades": [], "error": str(e)}
@app.post("/api/accounts/{account_id}/manual/place")
@_serialize_strategy_lifecycle
async def account_manual_place(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    asset_id = body.get("asset_id", "")
    side = body.get("side", "").upper()
    price = body.get("price", "")
    size = body.get("size", "")
    tif = body.get("tif", "gtc").lower()
    if not asset_id or side not in ("BUY", "SELL") or not price or not size:
        return JSONResponse(status_code=400, content={"ok": False, "error": "asset_id, side, price, size required"})
    if tif not in ("gtc", "ioc"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "tif must be gtc or ioc"})
    sniper = getattr(inst, "sniper", None)
    if sniper is not None and getattr(sniper, "_persistence_failed", False):
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": "Manual orders are disabled until the critical Sniper state is persisted",
            },
        )
    if sniper is not None and (
        sniper.running or getattr(sniper, "starting", False)
    ):
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": "Manual orders are disabled while Sniper is running",
            },
        )
    if (
        sniper is not None
        and getattr(getattr(sniper, "state", None), "locked", False)
        and side == "BUY"
    ):
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": "Manual buys are disabled while Sniper requires reconciliation",
            },
        )
    try:
        price_decimal = cents_to_price(price)
        price_str = _okx_price_str(price_decimal, side)
        actual_price_decimal = as_decimal(price_str)
        actual_price_cents = price_to_cents(actual_price_decimal)
    except (ValueError, TypeError):
        return JSONResponse(status_code=400, content={"ok": False, "error": f"Invalid price: {price}"})
    size_str = normalize_size_str(size)
    try:
        size_decimal = as_decimal(size_str)
    except ValueError:
        size_decimal = as_decimal(0)
    if size_decimal <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Size must be positive"})
    if side == "BUY" and actual_price_decimal * size_decimal < 1:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "OKX requires at least 1 xp notional for a buy order",
            },
        )
    if side == "SELL":
        try:
            asset_id_text = str(asset_id)
            market_id_filter = (
                asset_id_text[:-3]
                if asset_id_text.endswith(("000", "100"))
                else None
            )
            positions_response = await inst.client.get_positions(
                status="open", market_id=market_id_filter
            )
            if str(positions_response.get("code", "")) != "0":
                raise RuntimeError(
                    positions_response.get("message", positions_response.get("msg", "position query failed"))
                )
            available_size = as_decimal(0)
            total_size = as_decimal(0)
            positions_data = positions_response.get("data") or {}
            positions = (
                positions_data.get("list", [])
                if isinstance(positions_data, dict)
                else positions_data
            )
            for position in positions:
                market_id = str(position.get("marketId") or "")
                token_index = str(position.get("tokenIndex") or "")
                token_name = str(position.get("tokenName") or "").upper()
                position_asset_id = (
                    f"{market_id}100"
                    if token_index == "2" or token_name == "NO"
                    else f"{market_id}000"
                )
                if position_asset_id == asset_id_text:
                    available_size += as_decimal(
                        position.get("availableSize", position.get("size", "0"))
                    )
                    total_size += as_decimal(position.get("size", "0"))
            if size_decimal > available_size:
                return JSONResponse(
                    status_code=400,
                    content={
                        "ok": False,
                        "error": (
                            f"Sell size {size_str} exceeds available position "
                            f"{format(available_size, 'f')}"
                        ),
                    },
                )
            if (
                actual_price_decimal * size_decimal < 1
                and (
                    size_decimal != available_size
                    or available_size != total_size
                )
            ):
                return JSONResponse(
                    status_code=400,
                    content={
                        "ok": False,
                        "error": (
                            "OKX requires at least 1 xp notional unless selling "
                            "the full available position with no frozen size"
                        ),
                    },
                )
        except Exception:
            return JSONResponse(
                status_code=503,
                content={"ok": False, "error": "Cannot validate available sell size"},
            )
    logger.info(
        "Manual order asset=%s side=%s price=%s size=%s tif=%s",
        asset_id,
        side.lower(),
        price_str,
        size_str,
        tif,
    )
    try:
        result = await inst.client.place_order(
            asset_id=asset_id,
            side=side.lower(),
            price=price_str,
            size=size_str,
            tif=tif,
            reduce_only=(side.lower() == "sell"),
        )
        if isinstance(result, dict) and str(result.get("code", "")) == "0":
            return {
                "ok": True,
                "result": result,
                "price": price_str,
                "price_cents": format(actual_price_cents, "f"),
            }
        logger.warning("Manual order rejected by OKX: %s", result)
        return {"ok": False, "error": result.get("message", result.get("msg", str(result)))}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/accounts/{account_id}/ticker")
async def account_ticker(account_id: str, asset_id: str, yes_asset_id: str = "", user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if not asset_id:
        return JSONResponse(status_code=400, content={"error": "asset_id required"})
    try:
        # For NO positions: query YES asset book and invert (OKX only has YES orderbook)
        query_asset = yes_asset_id if yes_asset_id else asset_id
        data = await inst.client.get_orderbook(query_asset, depth=1)
        best_ask = None
        best_bid = None
        if isinstance(data, dict) and "data" in data:
            book = data["data"]
            if isinstance(book, list) and len(book) > 0:
                asks = book[0].get("asks", [])
                bids = book[0].get("bids", [])
                if asks:
                    best_ask = asks[0][0]
                if bids:
                    best_bid = bids[0][0]
        # If querying YES book for a NO position, invert prices
        if yes_asset_id and yes_asset_id != asset_id:
            inv_bid = format_okx_price(invert_binary_price(best_ask)) if best_ask else None
            inv_ask = format_okx_price(invert_binary_price(best_bid)) if best_bid else None
            best_bid, best_ask = inv_bid, inv_ask
        return {"best_ask": best_ask, "best_bid": best_bid}
    except Exception as e:
        return {"best_ask": None, "best_bid": None, "error": str(e)}


# ─── Manual strategy API ───


@app.post("/api/accounts/{account_id}/strategy/manual/start")
@_serialize_strategy_lifecycle
async def account_manual_start_v2(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    # Mutex check
    if inst.strategy_d.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-D already running."})
    if inst.strategy_s.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-S is running. Stop it first."})
    if _sniper_busy(inst):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Sniper is running or locked. Resolve it first."})
    if inst.sniper_old.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Sniper-Old is running. Stop it first."})
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
        return JSONResponse(status_code=400, content={"ok": False, "error": "no_asset_id is required"})
    try:
        price_cents = as_decimal(price)
        price_decimal = cents_to_price(price_cents)
        expected_okx_bid = align_okx_price(price_decimal, "buy")
    except ValueError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
    try:
        bal = await inst.client.get_balance()
        if isinstance(bal, dict) and bal.get("code") and str(bal["code"]) != "0":
            return JSONResponse(status_code=503, content={"ok": False, "error": f"OKX API: {bal.get('msg')}"})
    except Exception as e:
        return JSONResponse(status_code=503, content={"ok": False, "error": f"Cannot reach OKX: {e}"})
    # Monitor validation if enabled
    monitor = body.get("monitor")  # {poly_token_id, depth_threshold}
    if monitor and monitor.get("poly_token_id"):
        poly_token_id = monitor["poly_token_id"]
        if _monitor_poly_side(monitor) != "yes":
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Manual-D monitor must use the Poly outcome mapped to OKX YES"},
            )
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
            okx_best_bid = as_decimal(okx_bids[0][0])
            if okx_best_bid != expected_okx_bid:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX best bid ({okx_best_bid}) != YES buy bid ({expected_okx_bid})"})
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
            poly_best_bid = as_decimal(poly_bids[0].get("price", 0))
            if align_okx_price(poly_best_bid, "buy") != expected_okx_bid:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly best bid ({poly_best_bid}) does not map to YES buy bid ({expected_okx_bid})"})
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly book check: {e}"})

    try:
        await inst.strategy_d.start(market_id=market_id, yes_asset_id=asset_id, no_asset_id=no_asset_id, price_a=str(price_cents), position_pct=int(body.get("position_pct", 100)), scan_interval=float(body.get("scan_interval", 0.3)), stabilize_secs=float(body.get("stabilize_secs", 0.3)))
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # Auto-start monitor if requested
    if monitor and monitor.get("poly_token_id"):
        try:
            r = await inst.poly_monitor.start(
                poly_token_id=monitor["poly_token_id"],
                reference_price=float(price_decimal),
                depth_threshold=float(monitor.get("depth_threshold", 3000)),
            )
            if r and r.get("ok"):
                inst.poly_monitor.set_strategy_context("manual-d", float(price_cents), asset_id)
                _record_monitor_mapping(account_id, market_id, asset_id, no_asset_id, monitor, poly_token_id, "yes")
            else:
                await inst.strategy_d.stop()
                return JSONResponse(status_code=400, content={"ok": False, "error": (r or {}).get("error", "Monitor failed to start")})
        except Exception as e:
            await inst.strategy_d.stop()
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Monitor start error: {e}"})

    return {"ok": True}


@app.post("/api/accounts/{account_id}/strategy/manual/stop")
@_serialize_strategy_lifecycle
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
@_serialize_strategy_lifecycle
async def account_monitor_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    poly_token_id = body.get("poly_token_id", "")
    reference_price = float(body.get("reference_price", 0))
    depth_threshold = float(body.get("depth_threshold", 3000))
    if not poly_token_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "poly_token_id required"})
    result = await inst.poly_monitor.start(poly_token_id, reference_price, depth_threshold)
    if result.get("ok") and body.get("yes_asset_id"):
        _record_monitor_mapping(
            account_id,
            body.get("market_id", ""),
            body.get("yes_asset_id", ""),
            body.get("no_asset_id", ""),
            body.get("monitor", body),
            poly_token_id,
            body.get("side", "yes"),
        )
    return result



@app.post("/api/accounts/{account_id}/rebalance/toggle")
@_serialize_strategy_lifecycle
async def account_rebalance_toggle(account_id: str, request: Request, user: str = Depends(verify_auth)):
    """Toggle auto-rebalance on/off for an account."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    enabled = body.get("enabled", not inst.auto_rebalance.enabled)
    if not isinstance(enabled, bool):
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "enabled": inst.auto_rebalance.enabled,
                "error": "enabled must be a boolean",
            },
        )
    if not inst.auto_rebalance.set_enabled(enabled):
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "enabled": inst.auto_rebalance.enabled,
                "error": "Auto-Rebalance setting could not be persisted",
            },
        )
    return {"ok": True, "enabled": inst.auto_rebalance.enabled}


@app.get("/api/accounts/{account_id}/rebalance/status")
async def account_rebalance_status(account_id: str, user: str = Depends(verify_auth)):
    """Get auto-rebalance status including cost layers."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    return inst.auto_rebalance.status()


@app.post("/api/accounts/{account_id}/rebalance/clear-layers")
@_serialize_strategy_lifecycle
async def account_rebalance_clear(account_id: str, user: str = Depends(verify_auth)):
    """Clear cost layers (reset position tracking)."""
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if _account_trading_busy(inst):
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": "Stop all strategies and risk-control tasks before clearing cost layers",
            },
        )
    previous_layers = inst.auto_rebalance.cost_layers
    inst.auto_rebalance.cost_layers = {}
    if not inst.auto_rebalance._save_layers():
        inst.auto_rebalance.cost_layers = previous_layers
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Cost layers could not be persisted"},
        )
    return {"ok": True}

# ─── Sniper Endpoints ───

@app.post("/api/accounts/{account_id}/sniper/start")
@_serialize_strategy_lifecycle
async def sniper_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if (
        inst.strategy_s.running
        or inst.strategy_d.running
        or inst.sniper_old.running
        or _risk_control_busy(inst)
    ):
        return {"ok": False, "error": "Cannot run sniper while another strategy is active"}
    body = await request.json()
    result = await inst.sniper.start(
        targets=body.get("targets"),
        yes_token_id=body.get("yes_token_id", ""),
        no_token_id=body.get("no_token_id", ""),
        yes_asset_id=body.get("yes_asset_id", ""),
        no_asset_id=body.get("no_asset_id", ""),
        market_id=body.get("market_id", ""),
        threshold_cents=body.get("threshold_cents", 40),
        buy_limit_cents=body.get("buy_limit_cents", 50),
        position_pct=body.get("position_pct", 100),
        timeout_secs=body.get("timeout_secs", 3),
        settle_ms=int(body.get("settle_ms", 0)),
        window_ms=int(body.get("window_ms", 150)),
        poly_url=body.get("poly_url", ""),
        selected_outcome=body.get("selected_outcome", ""),
    )
    if result.get("ok"):
        for target in inst.sniper.targets:
            _record_poly_asset_mapping(
                account_id=account_id,
                market_id=target.market_id,
                yes_asset_id=target.yes_asset_id,
                no_asset_id=target.no_asset_id,
                yes_token_id=target.poly_token_id if target.side == "yes" else "",
                no_token_id=target.poly_token_id if target.side == "no" else "",
                poly_url=target.poly_url,
                selected_outcome=target.name,
            )
    return result


@app.post("/api/accounts/{account_id}/sniper/stop")
@_serialize_strategy_lifecycle
async def sniper_stop(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    result = await inst.sniper.stop()
    if not isinstance(result, dict):
        result = {"ok": True}
    return {
        **result,
        "stopped": not inst.sniper.running,
        "locked": bool(inst.sniper.state.locked),
        "status": inst.sniper.state.status,
        "needs_reconciliation": bool(
            result.get("needs_reconciliation") or inst.sniper.state.locked
        ),
    }


@app.get("/api/accounts/{account_id}/sniper/status")
async def sniper_status(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    return inst.sniper.get_state()


@app.post("/api/accounts/{account_id}/sniper-old/start")
@_serialize_strategy_lifecycle
async def sniper_old_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    if (
        inst.strategy_s.running
        or inst.strategy_d.running
        or _sniper_busy(inst)
        or _risk_control_busy(inst)
    ):
        return {"ok": False, "error": "Cannot run sniper-old while another strategy is active"}
    body = await request.json()
    result = await inst.sniper_old.start(
        yes_token_id=body.get("yes_token_id", ""),
        no_token_id=body.get("no_token_id", ""),
        yes_asset_id=body.get("yes_asset_id", ""),
        no_asset_id=body.get("no_asset_id", ""),
        market_id=body.get("market_id", ""),
        threshold_cents=int(body.get("threshold_cents", 40)),
        ask_levels=int(body.get("ask_levels", 5)),
        position_pct=int(body.get("position_pct", 100)),
        timeout_secs=int(body.get("timeout_secs", 120)),
        settle_ms=int(body.get("settle_ms", 0)),
        window_ms=int(body.get("window_ms", 150)),
        poly_url=body.get("poly_url", ""),
        selected_outcome=body.get("selected_outcome", ""),
    )
    if result.get("ok"):
        _record_poly_asset_mapping(
            account_id=account_id,
            market_id=body.get("market_id", ""),
            yes_asset_id=body.get("yes_asset_id", ""),
            no_asset_id=body.get("no_asset_id", ""),
            yes_token_id=body.get("yes_token_id", ""),
            no_token_id=body.get("no_token_id", ""),
            poly_url=body.get("poly_url", ""),
            selected_outcome=body.get("selected_outcome", ""),
        )
    return result


@app.post("/api/accounts/{account_id}/sniper-old/stop")
@_serialize_strategy_lifecycle
async def sniper_old_stop(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    await inst.sniper_old.stop()
    return {"ok": True}


@app.get("/api/accounts/{account_id}/sniper-old/status")
async def sniper_old_status(account_id: str, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    return inst.sniper_old.get_state()




@app.post("/api/accounts/{account_id}/monitor/stop")
@_serialize_strategy_lifecycle
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
@_serialize_strategy_lifecycle
async def account_manual_s_start(account_id: str, request: Request, user: str = Depends(verify_auth)):
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(404, "Account not found")
    # Mutex check
    if inst.strategy_s.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-S already running."})
    if inst.strategy_d.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Manual-D is running. Stop it first."})
    if _sniper_busy(inst):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Sniper is running or locked. Resolve it first."})
    if inst.sniper_old.running:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Sniper-Old is running. Stop it first."})
    if _risk_control_busy(inst):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Monitor or Auto-Rebalance is still active. Stop it first."})
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
        return JSONResponse(status_code=400, content={"ok": False, "error": "no_asset_id is required"})
    # Monitor validation if enabled
    monitor = body.get("monitor")
    mode = body.get("mode", "classic")
    if mode not in {"classic", "bid-sell"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": "mode must be classic or bid-sell"})
    side_normalized = str(side).lower()
    if side_normalized not in {"yes", "no"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": "side must be YES or NO"})
    try:
        price_cents = as_decimal(price)
        sell_price_input = as_decimal(body.get("sell_price", 0) or 0)
        if sell_price_input <= 0:
            sell_price_input = price_cents + 1
        buy_price_decimal = align_okx_price(cents_to_price(price_cents), "buy")
        sell_price_decimal = align_okx_price(cents_to_price(sell_price_input), "sell")
    except ValueError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
    if mode == "bid-sell" and sell_price_decimal - buy_price_decimal != as_decimal("0.01"):
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "Bid-Sell requires an executable 1 cent spread",
            },
        )
    monitor_ref = sell_price_decimal if mode == "bid-sell" else buy_price_decimal
    is_no_side = side_normalized == "no"
    if monitor and monitor.get("poly_token_id"):
        poly_token_id = monitor["poly_token_id"]
        if _monitor_poly_side(monitor) != side_normalized:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": f"Manual-S {side_normalized.upper()} must use the matching Poly {side_normalized.upper()} token"},
            )
        try:
            okx_book = await inst.client.get_orderbook(asset_id, depth=5)
            okx_levels = []
            if str(okx_book.get("code", "")) == "0":
                book_data = okx_book.get("data", [{}])
                if book_data and isinstance(book_data, list):
                    okx_levels = book_data[0].get("asks" if is_no_side else "bids", [])
            if not okx_levels:
                return JSONResponse(status_code=400, content={"ok": False, "error": "No OKX levels found for selected side"})
            okx_best_bid = (
                invert_binary_price(okx_levels[0][0])
                if is_no_side
                else as_decimal(okx_levels[0][0])
            )
            if okx_best_bid != monitor_ref:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"OKX {side_normalized.upper()} best bid ({okx_best_bid}) != monitor price ({monitor_ref})"})
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
            poly_best_bid = as_decimal(poly_bids[0].get("price", 0))
            if align_okx_price(poly_best_bid, "buy") != monitor_ref:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly best bid ({poly_best_bid}) does not map to monitor price ({monitor_ref})"})
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Poly book check: {e}"})

    try:
        await inst.strategy_s.start(market_id=market_id, yes_asset_id=asset_id, no_asset_id=no_asset_id, price_a=str(price_cents), sell_price=str(sell_price_input), mode=mode, side=side_normalized, position_pct=int(body.get("position_pct", 100)), scan_interval=float(body.get("scan_interval", 0.3)), stabilize_secs=float(body.get("stabilize_secs", 0.3)))
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # Auto-start monitor if requested
    if monitor and monitor.get("poly_token_id"):
        try:
            r = await inst.poly_monitor.start(
                poly_token_id=monitor["poly_token_id"],
                reference_price=float(monitor_ref),
                depth_threshold=float(monitor.get("depth_threshold", 3000)),
            )
            if r and r.get("ok"):
                strategy_price_cents = price_to_cents(monitor_ref)
                strategy_asset_id = no_asset_id if is_no_side else asset_id
                inst.poly_monitor.set_strategy_context("manual-s", float(strategy_price_cents), strategy_asset_id, mode=mode)
                _record_monitor_mapping(account_id, market_id, asset_id, no_asset_id, monitor, poly_token_id, side.lower())
            else:
                await inst.strategy_s.stop()
                return JSONResponse(status_code=400, content={"ok": False, "error": (r or {}).get("error", "Monitor failed to start")})
        except Exception as e:
            await inst.strategy_s.stop()
            return JSONResponse(status_code=400, content={"ok": False, "error": f"Monitor start error: {e}"})

    return {"ok": True}


@app.post("/api/accounts/{account_id}/strategy/manual-s/stop")
@_serialize_strategy_lifecycle
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
@_serialize_strategy_lifecycle
async def save_account_config_v2(account_id: str, request: Request, user: str = Depends(verify_auth)):
    from .core.database import update_account as db_update
    from .core.client import OKXOutcomesClient

    stored_account = get_account_decrypted(account_id)
    if not stored_account:
        raise HTTPException(404, "Account not found")
    body = await request.json()
    kwargs = {}
    reloaded = False
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
        if region not in {"AU", "CN"}:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "region must be AU or CN"},
            )
        if region == "CN":
            kwargs["base_url"] = "https://www.okx.com"
        else:
            kwargs["base_url"] = "https://us.okx.com"
    if kwargs:
        cred_keys = {"api_key", "secret_key", "passphrase", "agent_private_key", "base_url"}
        credentials_changed = bool(cred_keys & set(kwargs))
        replacement = None
        inst = manager.get_instance(account_id)
        if credentials_changed:
            if inst and _account_trading_busy(inst):
                return JSONResponse(
                    status_code=409,
                    content={
                        "ok": False,
                        "error": "Stop and reconcile all account strategies before changing credentials or region",
                    },
                )
            verification_client = inst.client if inst else OKXOutcomesClient(
                api_key=stored_account["api_key"],
                secret_key=stored_account["secret_key"],
                passphrase=stored_account["passphrase"],
                agent_private_key=stored_account["agent_private_key"],
                base_url=stored_account.get("base_url", "https://us.okx.com"),
                proxy_node=stored_account.get("proxy_node", "JP2"),
            )
            try:
                open_orders = await _query_open_orders_verified(verification_client)
                open_positions = await _query_open_positions_verified(verification_client)
            except Exception as exc:
                return JSONResponse(
                    status_code=503,
                    content={
                        "ok": False,
                        "error": f"Cannot verify account is flat before credential change: {exc}",
                    },
                )
            finally:
                if inst is None:
                    await verification_client.close()
            if open_orders or open_positions:
                return JSONResponse(
                    status_code=409,
                    content={
                        "ok": False,
                        "error": (
                            "Cancel all open orders and close all positions "
                            "before changing credentials or region"
                        ),
                    },
                )

            candidate = dict(stored_account)
            candidate.update(kwargs)
            replacement = OKXOutcomesClient(
                api_key=candidate["api_key"],
                secret_key=candidate["secret_key"],
                passphrase=candidate["passphrase"],
                agent_private_key=candidate["agent_private_key"],
                base_url=candidate.get("base_url", "https://us.okx.com"),
                proxy_node=candidate.get("proxy_node", "JP2"),
            )
            if not replacement.signer.address:
                await replacement.close()
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "Invalid agent private key"},
                )

        try:
            if not db_update(account_id, **kwargs):
                raise RuntimeError("Account update did not modify a row")
        except Exception:
            if replacement is not None:
                await replacement.close()
            raise

        if credentials_changed:
            if inst:
                previous = inst.install_client(replacement)
                if previous and hasattr(previous, "close"):
                    try:
                        await previous.close()
                    except Exception as exc:
                        logger.warning(
                            "Failed to close previous client for %s: %s",
                            account_id,
                            exc,
                        )
                reloaded = True
            else:
                await replacement.close()
                loaded = await manager._load_account(account_id)
                if loaded is None:
                    return JSONResponse(
                        status_code=500,
                        content={"ok": False, "error": "Account could not be loaded"},
                    )
                reloaded = True
    return {"ok": True, "reloaded": reloaded}


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
    started = time.perf_counter()
    try:
        data = await inst.client.get_balance()
        if not isinstance(data, dict) or str(data.get("code", "")) != "0":
            if isinstance(data, dict):
                errors.append(f"OKX API: {data.get('msg') or 'invalid response'} ({data.get('code', 'missing code')})")
            else:
                errors.append("OKX API returned an invalid response")
    except Exception as e:
        errors.append(f"OKX connection failed: {e}")
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    return {
        "healthy": len(errors) == 0,
        "errors": errors,
        "latency_ms": elapsed_ms,
        "region": "CN" if "www.okx.com" in inst.client.base_url else "AU",
        "proxy_node": inst.proxy_node,
        "proxy_label": PROXY_NODES.get(inst.proxy_node, {}).get("label", inst.proxy_node),
    }


# ─── Global routes (proxy latency, global config, notifications) ───

def _latency_probe_headers(
    account_id: str,
    region: str,
    yes_asset_id: str,
):
    """Return fresh signed GET headers for an account-scoped book probe."""
    if not yes_asset_id:
        return None
    account_id = str(account_id or "").strip()
    if not account_id:
        raise HTTPException(
            status_code=400,
            detail="account_id is required for authenticated OKX market probes",
        )
    inst = manager.get_instance(account_id)
    if not inst:
        raise HTTPException(status_code=404, detail="account not found")
    account_region = "CN" if "www.okx.com" in inst.client.base_url else "AU"
    if account_region != region:
        raise HTTPException(
            status_code=400,
            detail="account region does not match the requested probe region",
        )
    auth = getattr(inst.client, "auth", None)
    if auth is None or not callable(getattr(auth, "headers", None)):
        raise HTTPException(
            status_code=500,
            detail="account authentication is unavailable for the market probe",
        )

    def header_factory(target):
        signed_path = str(target.get("signed_path") or "")
        if not signed_path.startswith("/api/v5/market/pm-books?"):
            raise ValueError("market probe target has no signed path")
        return auth.headers("GET", signed_path)

    return header_factory

@app.get("/api/proxy/latency")
async def proxy_latency(
    region: str = "AU",
    samples: int = DEFAULT_WARM_SAMPLES,
    yes_asset_id: str = "",
    poly_token_id: str = "",
    account_id: str = "",
    user: str = Depends(verify_auth),
):
    """Benchmark all OKX routes and the one shared Poly direct route."""
    try:
        normalised_region = normalise_region(region)
        header_factory = _latency_probe_headers(
            account_id,
            normalised_region,
            yes_asset_id,
        )
        result = await benchmark_proxy_nodes(
            PROXY_NODES,
            region=normalised_region,
            samples=normalise_samples(samples),
            yes_asset_id=yes_asset_id,
            poly_token_id=poly_token_id,
            header_factory=header_factory,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    used = get_used_proxies()
    assignments = {
        account.get("proxy_node"): account.get("name", "")
        for account in get_accounts()
    }
    for node_id, entry in result["latencies"].items():
        entry["used"] = node_id in used
        if assignments.get(node_id):
            entry["account_name"] = assignments[node_id]
    return result


@app.get("/api/proxy/latency/{node_id}")
async def proxy_latency_single(
    node_id: str,
    region: str = "AU",
    samples: int = DEFAULT_WARM_SAMPLES,
    yes_asset_id: str = "",
    account_id: str = "",
    user: str = Depends(verify_auth),
):
    """Benchmark one node; Poly remains a separate shared batch metric."""
    if node_id not in PROXY_NODES:
        raise HTTPException(status_code=404, detail="unknown proxy node")
    try:
        normalised_region = normalise_region(region)
        sample_count = normalise_samples(samples)
        header_factory = _latency_probe_headers(
            account_id,
            normalised_region,
            yes_asset_id,
        )
        result = await benchmark_proxy_node(
            node_id,
            PROXY_NODES[node_id],
            region=normalised_region,
            samples=sample_count,
            yes_asset_id=yes_asset_id,
            header_factory=header_factory,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    result.update({
        "schema_version": 2,
        "region": normalised_region,
        "okx_base_url": OKX_BASE_URLS[normalised_region],
        "okx_authenticated": header_factory is not None,
        "execution_path": {
            "status": "not_measured",
            "reason": "Synthetic read RTTs do not measure Poly event to OKX order acknowledgement",
        },
        "used": node_id in get_used_proxies(),
    })
    return result



@app.get("/api/poly/resolve")
async def poly_resolve(url: str = "", user: str = Depends(verify_auth)):
    """Resolve a Polymarket URL/slug to token_ids.

    Sports pages can include the base event plus derived markets/events such as
    Team to Advance, Extra Time, Penalty Shootout, Total Corners and Player Props.
    Aggregate known siblings so the dropdown is not limited to the base 3
    moneyline markets (six Yes/No tokens).
    """
    import aiohttp, re, json
    if not url:
        return {"ok": False, "error": "No URL provided"}
    
    # A long numeric value is accepted directly as a token ID.
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
            
            def _loads(v, default=None):
                if default is None:
                    default = []
                if isinstance(v, str):
                    try:
                        return json.loads(v)
                    except Exception:
                        return default
                return v if v is not None else default

            def _market_tokens(mkt, prefix=None):
                tokens = _loads(mkt.get("clobTokenIds", "[]"), [])
                outcomes = _loads(mkt.get("outcomes", "[]"), [])
                q = prefix or mkt.get("question") or mkt.get("groupItemTitle") or mkt.get("slug") or "Market"
                out = []
                for i, tid in enumerate(tokens or []):
                    if not tid:
                        continue
                    outcome = outcomes[i] if i < len(outcomes) else f"Outcome {i}"
                    out.append({
                        "token_id": str(tid),
                        "outcome": f"{q} - {outcome}",
                        "market_id": str(mkt.get("id", "")),
                        "slug": mkt.get("slug", ""),
                        "market_type": mkt.get("sportsMarketType", ""),
                    })
                return out

            async def _fetch_json(api_url):
                try:
                    async with sess.get(api_url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status != 200:
                            return None
                        return await resp.json()
                except Exception:
                    return None

            def _add_market(all_tokens, seen_tokens, mkt):
                title = mkt.get("question") or mkt.get("groupItemTitle") or mkt.get("slug") or "Market"
                for tok in _market_tokens(mkt, title):
                    tid = tok["token_id"]
                    if tid in seen_tokens:
                        continue
                    seen_tokens.add(tid)
                    all_tokens.append(tok)

            async def _add_event_slug(all_tokens, seen_tokens, ev_slug):
                item = await _fetch_json(f"https://gamma-api.polymarket.com/events/slug/{ev_slug}")
                if not item or not isinstance(item, dict):
                    return
                for mkt in item.get("markets") or []:
                    _add_market(all_tokens, seen_tokens, mkt)

            async def _add_market_slug(all_tokens, seen_tokens, mkt_slug):
                data = await _fetch_json(f"https://gamma-api.polymarket.com/markets?slug={mkt_slug}")
                if isinstance(data, list) and data:
                    _add_market(all_tokens, seen_tokens, data[0])

            # Sports event URL: aggregate base event + visible sibling groups.
            if is_event_slug:
                all_tokens, seen_tokens = [], set()
                await _add_event_slug(all_tokens, seen_tokens, slug)
                for suffix in ("team-to-advance", "extra-time", "penalty-shootout"):
                    await _add_market_slug(all_tokens, seen_tokens, f"{slug}-{suffix}")
                for suffix in ("total-corners", "player-props"):
                    await _add_event_slug(all_tokens, seen_tokens, f"{slug}-{suffix}")
                if all_tokens:
                    return {
                        "ok": True,
                        "question": slug,
                        "tokens": all_tokens,
                        "is_event": True,
                        "count": len(all_tokens),
                    }

            for api_url in apis:
                data = await _fetch_json(api_url)
                if not data:
                    continue
                item = data[0] if isinstance(data, list) else data
                
                # Check if this is an event (has nested markets) or a market
                if "markets" in item and item["markets"]:
                    all_tokens, seen_tokens = [], set()
                    for mkt in item["markets"]:
                        _add_market(all_tokens, seen_tokens, mkt)
                    return {
                        "ok": True,
                        "question": item.get("title", ""),
                        "tokens": all_tokens,
                        "is_event": True,
                        "count": len(all_tokens),
                    }
                else:
                    # Single market
                    result = _market_tokens(item, None)
                    # Preserve legacy labels for single binary market dropdowns.
                    for tok in result:
                        if " - " in tok["outcome"]:
                            tok["outcome"] = tok["outcome"].split(" - ", 1)[1]
                    return {
                        "ok": True,
                        "question": item.get("question", ""),
                        "tokens": result
                    }
            
            # Retry the exact market slug with an explicit active filter.
            api_url = f"https://gamma-api.polymarket.com/markets?slug={slug}&active=true&limit=1"
            async with sess.get(api_url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        market = data[0]
                        tokens_raw = market.get("clobTokenIds", "[]")
                        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
                        outcomes_raw = market.get("outcomes", "[]")
                        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
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
        "tg_token": get_global_config("tg_token", ""),
        "tg_chats": get_global_config("tg_chats", ""),
    }


@app.post("/api/global/config")
async def save_global_config(request: Request, user: str = Depends(verify_auth)):
    body = await request.json()
    token = str(body.get("tg_token", "") or "")
    chats_raw = body.get("tg_chats", "")
    chats = (
        [c.strip() for c in chats_raw.split(",") if c.strip()]
        if isinstance(chats_raw, str)
        else [str(c).strip() for c in chats_raw if str(c).strip()]
    )
    set_global_config("tg_token", token)
    set_global_config("tg_chats", ",".join(chats))
    if manager.notifier:
        if manager.notifier._session and not manager.notifier._session.closed:
            await manager.notifier._session.close()
        manager.notifier._session = None
        manager.notifier.token = token
        manager.notifier.chat_ids = chats
        manager.notifier.enabled = bool(token and chats)
    return {"ok": True}


@app.post("/api/global/notify/test")
async def test_global_notify(user: str = Depends(verify_auth)):
    try:
        import aiohttp
        token = get_global_config("tg_token", "")
        chats = [c.strip() for c in get_global_config("tg_chats", "").split(",") if c.strip()]
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
