"""
app.py — CryptoForge FastAPI Backend
Perpetual futures algo-trading platform powered by Delta Exchange.
Production-ready: multi-engine, WebSocket, portfolio history, full CRUD.
"""

import asyncio
import inspect
import json
import os
import secrets
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

# ── Guaranteed path fix ───────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
os.chdir(_HERE)

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import alerter
import config  # must be first — calls load_dotenv()
from broker.delta import DeltaClient, get_candles_binance
from engine.backtest import DEFAULT_ENTRY_CONDITIONS, DEFAULT_EXIT_CONDITIONS, run_backtest
from engine.live import LiveEngine
from engine.paper_trading import PaperTradingEngine
from engine.scalp import ScalpEngine


# ── Shutdown hook: auto-save running engines to runs.json ─────
def _shutdown_save_engines():
    """Save all running paper/live engines to runs.json on shutdown."""
    for run_id, engine in list(paper_engines.items()):
        if engine.running:
            try:
                status = engine.get_status()
                engine.stop()
                _save_engine_run_to_history(status, "paper")
                print(f"[SHUTDOWN] Saved paper engine {run_id}")
            except Exception as e:
                print(f"[SHUTDOWN] Failed to save paper engine {run_id}: {e}")
    for run_id, engine in list(live_engines.items()):
        if engine.running:
            try:
                status = engine.get_status()
                engine.stop()
                _save_engine_run_to_history(status, "live")
                print(f"[SHUTDOWN] Saved live engine {run_id}")
            except Exception as e:
                print(f"[SHUTDOWN] Failed to save live engine {run_id}: {e}")


import atexit

atexit.register(_shutdown_save_engines)

# Initialize
app = FastAPI(title="CryptoForge", version="2.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

from error_handlers import register_error_handlers

register_error_handlers(app)

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize Delta client
delta = DeltaClient()

# ── Multi-Engine Registries (keyed by run_id) ────────────────
live_engines: Dict[str, LiveEngine] = {}
paper_engines: Dict[str, PaperTradingEngine] = {}
_live_tasks: Dict[str, asyncio.Task] = {}
_paper_tasks: Dict[str, asyncio.Task] = {}

# Stopped engine snapshots — persist on Live page after stop
_stopped_engines: Dict[str, dict] = {}

# Trade state tracker for Telegram alerts
_alert_state: Dict[str, dict] = {}  # {"in_trade": bool, "closed_count": int}


def _check_trade_alerts(run_id: str, mode_label: str, event: dict):
    """Detect trade entry/exit from engine status updates and fire Telegram alerts."""
    if event.get("type") in ("status", "price_update"):
        return
    open_positions = event.get("open_positions", 0)
    closed_count = event.get("closed_trades", 0)
    open_trades = event.get("open_trades", [])
    recent_trades = event.get("recent_trades", [])
    total_pnl = event.get("total_pnl", 0)
    prev = _alert_state.get(run_id, {"in_trade": False, "closed_count": 0})

    in_trade = open_positions > 0

    # Detect entry
    if in_trade and not prev["in_trade"]:
        pos_lines = []
        for p in open_trades:
            sym = p.get("symbol", "—")
            side = p.get("side", "")
            price = p.get("entry_price", 0)
            pos_lines.append(f"  {side} {sym} @ ${price:,.2f}")
        body = f"Strategy: {run_id}\nMode: {mode_label}\n" + "\n".join(pos_lines)
        alerter.alert("Trade Entry", body, level="info")

    # Detect exit
    if closed_count > prev["closed_count"]:
        new_trades = recent_trades[-(closed_count - prev["closed_count"]) :]
        for t in new_trades:
            sym = t.get("symbol", "—")
            pnl = round(t.get("pnl", 0), 2)
            reason = t.get("exit_reason", "") or t.get("reason", "—")
            level = "info" if pnl >= 0 else "warn"
            body = (
                f"Strategy: {run_id}\nMode: {mode_label}\n"
                f"Symbol: {sym}\nP&L: ${pnl:,.2f}\nReason: {reason}\n"
                f"Total P&L: ${round(total_pnl, 2):,.2f}"
            )
            alerter.alert("Trade Exit", body, level=level)

    _alert_state[run_id] = {"in_trade": in_trade, "closed_count": closed_count}


ws_clients: List[WebSocket] = []


# ── Authentication ────────────────────────────────────────────────
AUTH_PIN = os.getenv("CRYPTOFORGE_PIN") or os.getenv("CRYPTOFORGE_PASSWORD")
if not AUTH_PIN:
    raise RuntimeError(
        "[FATAL] CRYPTOFORGE_PIN environment variable is not set. "
        "The server refuses to start without an explicit PIN. "
        "Set it in your .env file: CRYPTOFORGE_PIN=<your-6-digit-pin>"
    )
SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(32))
_SESSION_FILE = os.path.join(_HERE, ".sessions.json")


def _load_sessions() -> dict:
    try:
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r") as f:
                data = json.loads(f.read())
            now = datetime.now().isoformat()
            return {k: v for k, v in data.items() if v > now}
    except Exception:
        pass
    return {}


def _save_sessions(sessions: dict):
    try:
        tmp = _SESSION_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(json.dumps(sessions))
        os.replace(tmp, _SESSION_FILE)
    except Exception:
        pass


def _create_session() -> str:
    sessions = _load_sessions()
    token = secrets.token_hex(32)
    sessions[token] = (datetime.now() + timedelta(hours=24)).isoformat()
    _save_sessions(sessions)
    return token


def _validate_session(token: str) -> bool:
    if not token:
        return False
    sessions = _load_sessions()
    exp_str = sessions.get(token)
    if not exp_str:
        return False
    if datetime.now() > datetime.fromisoformat(exp_str):
        sessions.pop(token, None)
        _save_sessions(sessions)
        return False
    return True


def _get_session_token(request: Request) -> str:
    token = request.cookies.get("cryptoforge_session", "")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
    return token


# ── Auth Middleware (Dependency-based) ────────────────────────────
async def require_auth(request: Request):
    path = request.url.path
    if path in ("/api/auth/login", "/api/auth/status", "/api/health", "/login", "/", "/favicon.ico"):
        return
    if path.startswith("/static"):
        return
    token = _get_session_token(request)
    if not _validate_session(token):
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach a unique request-id to every request for log tracing."""
    import uuid

    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = rid
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    return response


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    public = ("/", "/api/auth/login", "/api/auth/status", "/api/health", "/favicon.ico")
    if path in public or path.startswith("/static"):
        return await call_next(request)
    if path.startswith("/api/"):
        token = _get_session_token(request)
        if not _validate_session(token):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ── Redis (lazy singleton) ────────────────────────────────────────
_redis_client = None
_redis_checked = False


def _get_redis():
    global _redis_client, _redis_checked
    if _redis_checked:
        return _redis_client
    _redis_checked = True
    try:
        import redis as _redis_lib

        r = _redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True, socket_timeout=1)
        r.ping()
        _redis_client = r
    except Exception:
        _redis_client = None
    return _redis_client


# ── Rate Limiting ─────────────────────────────────────────────────
_rate_limits: dict = defaultdict(list)  # fallback when Redis unavailable
_RL_PREFIX = "cryptoforge:rl:"


def check_rate_limit(endpoint: str, max_calls: int = 5, window_sec: int = 10, client_ip: str = "global"):
    """Per-IP rate limiter — Redis sliding window when available, in-memory fallback."""
    r = _get_redis()
    if r is not None:
        try:
            key = f"{_RL_PREFIX}{endpoint}:{client_ip}"
            now_ms = int(time.time() * 1000)
            pipe = r.pipeline()
            pipe.zremrangebyscore(key, 0, now_ms - window_sec * 1000)
            pipe.zcard(key)
            pipe.zadd(key, {str(now_ms): now_ms})
            pipe.expire(key, window_sec + 1)
            _, count, *_ = pipe.execute()
            if count >= max_calls:
                raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Max {max_calls}/{window_sec}s.")
            return
        except HTTPException:
            raise
        except Exception as e:
            _logger.warning(f"[Redis] check_rate_limit failed, using in-memory: {e}")
    # In-memory fallback
    now = time.time()
    key = f"{endpoint}:{client_ip}"
    calls = _rate_limits[key]
    _rate_limits[key] = [t for t in calls if now - t < window_sec]
    if len(_rate_limits[key]) >= max_calls:
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Max {max_calls}/{window_sec}s.")
    _rate_limits[key].append(now)
    if len(_rate_limits) > 50_000:
        stale = [k for k, v in _rate_limits.items() if not v or now - v[-1] > window_sec]
        for k in stale[:5_000]:
            del _rate_limits[k]


# ── Brute-Force Protection ────────────────────────────────────────
_login_attempts: dict = defaultdict(list)  # fallback
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_LOCKOUT_SEC = 300  # 5 minutes
_LOGIN_RL_PREFIX = "cryptoforge:login:"


def _check_login_rate(ip: str):
    r = _get_redis()
    if r is not None:
        try:
            count = int(r.get(f"{_LOGIN_RL_PREFIX}{ip}") or 0)
            if count >= _LOGIN_MAX_ATTEMPTS:
                raise HTTPException(status_code=429, detail="Too many failed attempts. Try again in 5 minutes.")
            return
        except HTTPException:
            raise
        except Exception as e:
            _logger.warning(f"[Redis] _check_login_rate failed, using in-memory: {e}")
    now = time.time()
    _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < _LOGIN_LOCKOUT_SEC]
    if len(_login_attempts[ip]) >= _LOGIN_MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many failed attempts. Try again in 5 minutes.")


def _record_failed_login(ip: str):
    r = _get_redis()
    if r is not None:
        try:
            pipe = r.pipeline()
            pipe.incr(f"{_LOGIN_RL_PREFIX}{ip}")
            pipe.expire(f"{_LOGIN_RL_PREFIX}{ip}", _LOGIN_LOCKOUT_SEC)
            pipe.execute()
            return
        except Exception as e:
            _logger.warning(f"[Redis] _record_failed_login failed, using in-memory: {e}")
    _login_attempts[ip].append(time.time())


def _clear_login_attempts(ip: str):
    r = _get_redis()
    if r is not None:
        try:
            r.delete(f"{_LOGIN_RL_PREFIX}{ip}")
            return
        except Exception:
            pass
    _login_attempts.pop(ip, None)


# ── Models ────────────────────────────────────────────────────────
class BacktestRequest(BaseModel):
    from_date: str = config.DEFAULT_FROM
    to_date: str = config.DEFAULT_TO
    symbol: str = "BTCUSDT"
    initial_capital: float = config.DEFAULT_CAPITAL
    leverage: int = 10
    entry_conditions: Optional[List[dict]] = None
    exit_conditions: Optional[List[dict]] = None
    strategy_config: Optional[dict] = None


class StrategyPayload(BaseModel):
    run_name: str = ""
    symbol: str = "BTCUSDT"
    from_date: str = config.DEFAULT_FROM
    to_date: str = config.DEFAULT_TO
    initial_capital: float = 10000.0
    leverage: int = 10
    trade_side: str = "LONG"
    position_size_pct: float = 100.0
    stoploss_pct: float = 5.0
    target_profit_pct: float = 10.0
    trailing_sl_pct: float = 0.0
    max_trades_per_day: int = 5
    max_daily_loss: float = 0.0
    indicators: List[str] = []
    entry_conditions: Optional[List[dict]] = None
    exit_conditions: Optional[List[dict]] = None
    candle_interval: str = "5m"
    deploy_config: Optional[dict] = None


class OrderRequest(BaseModel):
    symbol: str
    size: float
    side: str = "buy"
    order_type: str = "market_order"
    limit_price: Optional[float] = None
    leverage: int = 10


# ── Favicon ───────────────────────────────────────────────────────
@app.get("/favicon.ico")
async def favicon():
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" rx="20" fill="#8b5cf6"/><text y=".9em" x="50" text-anchor="middle" font-size="70" font-family="sans-serif">⬡</text></svg>'
    return Response(content=svg, media_type="image/svg+xml")


# ── Serve Frontend ────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_frontend(request: Request):
    token = _get_session_token(request)
    if not _validate_session(token):
        login_path = os.path.join(_HERE, "login.html")
        if os.path.exists(login_path):
            with open(login_path, encoding="utf-8") as f:
                return HTMLResponse(f.read())
        return HTMLResponse("<h2>login.html not found</h2>")
    html_path = os.path.join(_HERE, "strategy.html")
    if os.path.exists(html_path):
        with open(html_path, encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h2>strategy.html not found</h2>")


# ── Auth Endpoints ────────────────────────────────────────────────
@app.post("/api/auth/login")
async def auth_login(request: Request):
    ip = request.client.host if request.client else "unknown"
    _check_login_rate(ip)
    body = await request.json()
    password = body.get("password", "")
    if password == AUTH_PIN:
        _clear_login_attempts(ip)
        token = _create_session()
        resp = JSONResponse({"status": "ok", "message": "Login successful"})
        is_https = request.headers.get("x-forwarded-proto") == "https"
        resp.set_cookie("cryptoforge_session", token, max_age=86400, httponly=True, samesite="lax", secure=is_https)
        return resp
    _record_failed_login(ip)
    raise HTTPException(status_code=401, detail="Invalid PIN")


@app.get("/api/auth/status")
async def auth_status(request: Request):
    token = _get_session_token(request)
    return {"authenticated": _validate_session(token)}


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    token = _get_session_token(request)
    sessions = _load_sessions()
    sessions.pop(token, None)
    _save_sessions(sessions)
    resp = JSONResponse({"status": "ok"})
    resp.delete_cookie("cryptoforge_session")
    return resp


# ── CSV formula-injection guard ───────────────────────────────────
_CSV_INJECT_CHARS = ("=", "+", "-", "@", "\t", "\r")


def _csv_safe(value):
    """Prefix string values that start with formula chars to prevent CSV injection."""
    if isinstance(value, str) and value.startswith(_CSV_INJECT_CHARS):
        return "'" + value
    return value


# ── Health ────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "time": str(datetime.now()),
        "delta_configured": delta._is_configured(),
        "live_running": any(e.running for e in live_engines.values()),
        "paper_running": any(e.running for e in paper_engines.values()),
    }


# ── Emergency Stop ────────────────────────────────────────────────
@app.post("/api/emergency-stop")
async def emergency_stop(request: Request):
    """Emergency kill all running engines."""
    results = {}
    stopped = 0

    for run_id, engine in list(paper_engines.items()):
        try:
            if engine.running:
                engine.stop()
                results[f"paper:{run_id}"] = "stopped"
                stopped += 1
            else:
                results[f"paper:{run_id}"] = "not_running"
        except Exception as e:
            results[f"paper:{run_id}"] = f"error: {str(e)}"

    for run_id, engine in list(live_engines.items()):
        try:
            if engine.running:
                engine.stop()
                results[f"live:{run_id}"] = "stopped"
                stopped += 1
            else:
                results[f"live:{run_id}"] = "not_running"
        except Exception as e:
            results[f"live:{run_id}"] = f"error: {str(e)}"

    # Cancel all tasks
    for name, tasks_dict in [("live", _live_tasks), ("paper", _paper_tasks)]:
        for run_id, task_ref in list(tasks_dict.items()):
            if task_ref and not task_ref.done():
                task_ref.cancel()
                try:
                    await task_ref
                except asyncio.CancelledError:
                    pass
    _live_tasks.clear()
    _paper_tasks.clear()
    live_engines.clear()
    paper_engines.clear()

    return {
        "status": "ok",
        "stopped": stopped,
        "message": f"Emergency stop executed — {stopped} engine(s) stopped",
        "results": results,
        "timestamp": str(datetime.now()),
    }


# ── Dashboard ─────────────────────────────────────────────────────
@app.get("/api/dashboard/summary")
async def dashboard_summary(request: Request):
    """Get dashboard summary data."""
    strats = _load()
    runs = _load_runs()

    paper_running = any(e.running for e in paper_engines.values())
    live_running = any(e.running for e in live_engines.values())
    paper_statuses = [e.get_status() for e in paper_engines.values() if e.running]
    live_statuses = [e.get_status() for e in live_engines.values() if e.running]

    paper_pnl_val = 0
    paper_trades_val = 0
    live_pnl_val = 0
    live_trades_val = 0

    if paper_statuses:
        paper_pnl_val = sum(s.get("total_pnl", 0) for s in paper_statuses)
        paper_trades_val = sum(s.get("trades_today", 0) for s in paper_statuses)
    else:
        from datetime import date as _date

        today_str = str(_date.today())
        for r in reversed(runs):
            if r.get("mode") == "paper":
                created = r.get("created_at", "")
                if created.startswith(today_str):
                    paper_pnl_val = r.get("total_pnl", 0)
                    paper_trades_val = r.get("trade_count", len(r.get("trades", [])))
                break  # only check most recent paper run

    if live_statuses:
        live_pnl_val = sum(s.get("total_pnl", 0) for s in live_statuses)
        live_trades_val = sum(s.get("trades_today", 0) for s in live_statuses)

    today_pnl = paper_pnl_val + live_pnl_val

    best_run = worst_run = None
    for r in runs:
        pnl = r.get("total_pnl", 0)
        if best_run is None or pnl > best_run.get("pnl", 0):
            best_run = {"id": r.get("id"), "name": r.get("run_name", ""), "pnl": pnl}
        if worst_run is None or pnl < worst_run.get("pnl", 0):
            worst_run = {"id": r.get("id"), "name": r.get("run_name", ""), "pnl": pnl}

    return {
        "strategy_count": len(strats),
        "backtest_count": len(runs),
        "paper_running": paper_running,
        "live_running": live_running,
        "paper_strategy": ", ".join(s.get("strategy_name", "") for s in paper_statuses) if paper_statuses else "",
        "live_strategy": ", ".join(s.get("strategy_name", "") for s in live_statuses) if live_statuses else "",
        "today_pnl": round(today_pnl, 2),
        "paper_pnl": round(paper_pnl_val, 2),
        "live_pnl": round(live_pnl_val, 2),
        "paper_trades": paper_trades_val,
        "live_trades": live_trades_val,
        "best_run": best_run,
        "worst_run": worst_run,
    }


# ── Broker Connection ────────────────────────────────────────────
@app.post("/api/broker/check")
async def check_broker():
    try:
        if not delta._is_configured():
            return {
                "status": "not_configured",
                "broker": "Delta Exchange",
                "message": "Delta API credentials not configured.",
            }
        wallet = delta.get_wallet()
        if isinstance(wallet, dict) and "error" not in wallet:
            return {
                "status": "connected",
                "broker": "Delta Exchange",
                "message": "Broker connection active",
                "wallet": wallet,
            }
        if isinstance(wallet, list):
            return {
                "status": "connected",
                "broker": "Delta Exchange",
                "message": "Broker connection active",
                "wallet": wallet,
            }
        return {
            "status": "error",
            "broker": "Delta Exchange",
            "message": wallet.get("error", "Unknown error") if isinstance(wallet, dict) else "Unknown error",
        }
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg or "Unauthorized" in error_msg:
            return {
                "status": "error",
                "broker": "Delta Exchange",
                "message": "Invalid API credentials (401 Unauthorized)",
            }
        elif "403" in error_msg or "Forbidden" in error_msg:
            return {"status": "error", "broker": "Delta Exchange", "message": "Access forbidden (403)"}
        elif "timeout" in error_msg.lower():
            return {"status": "error", "broker": "Delta Exchange", "message": "Connection timeout"}
        return {"status": "error", "broker": "Delta Exchange", "message": f"Connection error: {str(e)[:100]}"}


@app.post("/api/broker/connect")
async def connect_broker():
    try:
        if not delta._is_configured():
            return {
                "status": "not_configured",
                "broker": "Delta Exchange",
                "message": "API credentials not configured. Update .env file.",
            }
        wallet = delta.get_wallet()
        if isinstance(wallet, (dict, list)) and (not isinstance(wallet, dict) or "error" not in wallet):
            return {
                "status": "connected",
                "broker": "Delta Exchange",
                "message": "Connected to Delta Exchange",
                "wallet": wallet,
            }
        return {
            "status": "error",
            "broker": "Delta Exchange",
            "message": str(wallet.get("error", "Unknown error")) if isinstance(wallet, dict) else "Invalid response",
        }
    except Exception as e:
        alerter.alert("Broker Connect Failed", f"Error: {e}", level="warn")
        return {"status": "error", "broker": "Delta Exchange", "message": f"Connection failed: {str(e)[:100]}"}


# ── Products & Leverage ──────────────────────────────────────────
@app.get("/api/products")
async def get_products():
    """Get all available perpetual futures."""
    try:
        perps = delta.get_perpetual_futures()
        return {"status": "ok", "count": len(perps), "products": perps}
    except Exception as e:
        return {"status": "error", "message": str(e)[:100]}


@app.get("/api/leverage/{symbol}")
async def get_leverage(symbol: str):
    """Get leverage options for a symbol."""
    try:
        info = delta.get_leverage_info(symbol)
        return {"status": "ok", **info}
    except Exception as e:
        return {"status": "error", "message": str(e)[:100]}


@app.get("/api/cryptos")
async def get_top_cryptos():
    """Return Delta Exchange tradeable perpetual futures for the frontend."""
    return config.TOP_25_CRYPTOS


# ── Top 25 Market Data (CoinGecko) ───────────────────────────────
_market_cache = {"data": None, "timestamp": 0, "ttl": 120}  # 2 min cache
_STABLECOIN_SKIP = {
    "usdt",
    "usdc",
    "usds",
    "dai",
    "usde",
    "tusd",
    "busd",
    "usdp",
    "fdusd",
    "pyusd",
    "gusd",
    "frax",
    "eurs",
    "first-digital-usd",
    "wbt",
    "figr_heloc",
    "cc",
    "usd1",
    "rain",
    "weth",
    "steth",
    "wbtc",
    "cbbtc",
    "cbeth",
    "reth",
}

# Map CoinGecko symbol → Delta Exchange perp symbol (if tradeable)
_CG_TO_DELTA = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "doge": "DOGEUSDT",
    "paxg": "PAXGUSDT",
}


@app.get("/api/market/top25")
async def get_market_top25():
    """Fetch top 25 crypto coins by market cap from CoinGecko (excludes stablecoins)."""
    global _market_cache
    now = time.time()
    if _market_cache["data"] and (now - _market_cache["timestamp"]) < _market_cache["ttl"]:
        return _market_cache["data"]

    try:
        import requests as req

        resp = req.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h",
            },
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()

        # Filter out stablecoins / wrapped tokens
        filtered = [
            c
            for c in raw
            if c.get("symbol", "").lower() not in _STABLECOIN_SKIP and c.get("id", "") not in _STABLECOIN_SKIP
        ][:25]

        coins = []
        for c in filtered:
            sym = c.get("symbol", "").lower()
            delta_sym = _CG_TO_DELTA.get(sym)
            # Every coin gets a USDT trading symbol (Binance format)
            trade_symbol = delta_sym or (c.get("symbol", "").upper() + "USDT")
            coins.append(
                {
                    "rank": len(coins) + 1,
                    "id": c.get("id", ""),
                    "symbol": c.get("symbol", "").upper(),
                    "name": c.get("name", ""),
                    "image": c.get("image", ""),
                    "price": c.get("current_price", 0),
                    "change_24h": round(c.get("price_change_percentage_24h", 0) or 0, 2),
                    "volume_24h": c.get("total_volume", 0),
                    "market_cap": c.get("market_cap", 0),
                    "high_24h": c.get("high_24h", 0),
                    "low_24h": c.get("low_24h", 0),
                    "ath": c.get("ath", 0),
                    "ath_change_pct": round(c.get("ath_change_percentage", 0) or 0, 1),
                    "circulating_supply": c.get("circulating_supply", 0),
                    "delta_tradeable": delta_sym is not None,
                    "delta_symbol": delta_sym,
                    "trade_symbol": trade_symbol,  # always present for backtest
                }
            )

        result = {"status": "ok", "coins": coins, "timestamp": now}
        _market_cache["data"] = result
        _market_cache["timestamp"] = now
        return result

    except Exception as e:
        import traceback

        traceback.print_exc()
        return {"status": "error", "message": str(e)[:200]}


# ── Ticker ────────────────────────────────────────────────────────
_ticker_cache = {"data": None, "timestamp": 0, "ttl": 30}


def _safe_float(v, default=0.0):
    """Safely convert to float, handling None, empty str, etc."""
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


@app.get("/api/ticker")
async def get_ticker():
    """Fetch live prices for top cryptos."""
    global _ticker_cache
    if _ticker_cache["data"] and (time.time() - _ticker_cache["timestamp"]) < _ticker_cache["ttl"]:
        return _ticker_cache["data"]

    try:
        tickers = delta.get_tickers_bulk()
        # Build a map by symbol — convert Delta India symbols (BTCUSD→BTCUSDT)
        ticker_map = {}
        for t in tickers:
            sym = t.get("symbol", "")
            ticker_map[sym] = t
            # Also map USDT variant so config lookups work
            usdt_sym = delta.from_delta_symbol(sym)
            if usdt_sym != sym:
                ticker_map[usdt_sym] = t

        result = {"status": "ok", "tickers": {}}
        for crypto in config.TOP_25_CRYPTOS:
            sym = crypto["symbol"]
            t = ticker_map.get(sym, {})

            mark = _safe_float(t.get("mark_price"))
            close = _safe_float(t.get("close"))
            price = mark if mark > 0 else close
            high_24h = _safe_float(t.get("high"))
            low_24h = _safe_float(t.get("low"))
            volume = _safe_float(t.get("volume"))
            funding = _safe_float(t.get("funding_rate"))
            oi = _safe_float(t.get("open_interest"))

            # Compute 24h % change: (close - open) / open * 100
            # Delta doesn't provide percent change, so estimate from low/high midpoint
            open_price = _safe_float(t.get("open"))
            if open_price > 0 and close > 0:
                change_24h = ((close - open_price) / open_price) * 100
            elif high_24h > 0 and low_24h > 0 and close > 0:
                mid = (high_24h + low_24h) / 2
                change_24h = ((close - mid) / mid) * 100
            else:
                change_24h = 0.0

            # Turnover as dollar volume
            turnover = _safe_float(t.get("turnover"))
            dollar_vol = turnover if turnover > 0 else (volume * price)

            result["tickers"][sym] = {
                "symbol": sym,
                "ticker": crypto["ticker"],
                "name": crypto["name"],
                "price": price,
                "change_24h": round(change_24h, 2),
                "volume_24h": dollar_vol,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "funding_rate": funding,
                "open_interest": oi,
            }

        _ticker_cache["data"] = result
        _ticker_cache["timestamp"] = time.time()
        return result
    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"[TICKER] Error: {e}")
        return {"status": "error", "message": str(e)[:100]}


@app.get("/api/ticker/{symbol}")
async def get_single_ticker(symbol: str):
    """Get ticker for a single symbol."""
    try:
        return delta.get_ticker(symbol)
    except Exception as e:
        return {"status": "error", "message": str(e)[:100]}


# ── Delta Exchange symbols (have perp futures) ───────────────────
_DELTA_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "PAXGUSDT"}


# ── Data Fetch ────────────────────────────────────────────────────
def _fetch_data(symbol: str, from_date: str, to_date: str, candle_interval: str = "5m") -> pd.DataFrame:
    print(f"[DATA] Symbol={symbol}, Interval={candle_interval}, From={from_date}, To={to_date}")

    # Try Delta Exchange first for supported symbols
    if symbol in _DELTA_SYMBOLS:
        df = delta.get_candles(symbol, resolution=candle_interval, start=from_date, end=to_date)
        if not df.empty:
            print(f"[DATA] Delta: {len(df)} candles: {df.index[0]} → {df.index[-1]}")
            return df
        print(f"[DATA] Delta returned no data for {symbol}, trying Binance...")

    # Fallback: Binance public API (works for any major crypto)
    df = get_candles_binance(symbol, resolution=candle_interval, start=from_date, end=to_date)
    if not df.empty:
        print(f"[DATA] Binance: {len(df)} candles: {df.index[0]} → {df.index[-1]}")
        return df

    raise Exception(f"No candle data for {symbol} (tried Delta + Binance)")


# ── Backtest ──────────────────────────────────────────────────────
@app.post("/api/backtest")
async def api_run_backtest(payload: StrategyPayload):
    try:
        print(f"\n{'=' * 60}")
        print(f"[BACKTEST] Run: {payload.run_name}")
        print(f"[BACKTEST] Symbol: {payload.symbol}, Leverage: {payload.leverage}x")
        print(f"[BACKTEST] Side: {payload.trade_side}, Interval: {payload.candle_interval}")
        print(f"[BACKTEST] Indicators: {payload.indicators}")
        print(f"{'=' * 60}")

        df_raw = await asyncio.to_thread(
            _fetch_data,
            symbol=payload.symbol,
            from_date=payload.from_date,
            to_date=payload.to_date,
            candle_interval=payload.candle_interval,
        )

        if df_raw.empty:
            return {"status": "error", "message": "No data returned."}

        strategy_config = payload.model_dump()
        results = await asyncio.to_thread(
            run_backtest,
            df_raw=df_raw,
            entry_conditions=payload.entry_conditions or DEFAULT_ENTRY_CONDITIONS,
            exit_conditions=payload.exit_conditions or DEFAULT_EXIT_CONDITIONS,
            strategy_config=strategy_config,
        )

        if results.get("status") == "success":
            runs = _load_runs()
            max_id = max([r.get("id", 0) for r in runs], default=0)
            run_entry = {
                "id": max_id + 1,
                "run_name": payload.run_name,
                "symbol": payload.symbol,
                "from_date": payload.from_date,
                "to_date": payload.to_date,
                "leverage": payload.leverage,
                "trade_side": payload.trade_side,
                "stoploss_pct": payload.stoploss_pct,
                "target_profit_pct": payload.target_profit_pct,
                "indicators": payload.indicators,
                "entry_conditions": payload.entry_conditions,
                "exit_conditions": payload.exit_conditions,
                "candle_interval": payload.candle_interval,
                "initial_capital": payload.initial_capital,
                "position_size_pct": payload.position_size_pct,
                "stats": results["stats"],
                "monthly": results.get("monthly", []),
                "day_of_week": results.get("day_of_week", []),
                "trade_count": results["stats"]["total_trades"],
                "total_pnl": results["stats"]["total_pnl"],
                "created_at": str(datetime.now()),
                "trades": results.get("trades", []),
                "equity": results.get("equity", []),
            }
            runs.append(run_entry)
            _save_runs(runs)
            results["run_id"] = run_entry["id"]

        return results
    except Exception as e:
        import traceback

        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ── Live Engine ───────────────────────────────────────────────────
@app.post("/api/live/start")
async def live_start(payload: StrategyPayload):
    strategy_dict = {
        "run_name": payload.run_name or "Live Strategy",
        "symbol": payload.symbol,
        "leverage": payload.leverage,
        "trade_side": payload.trade_side,
        "indicators": payload.indicators or [],
        "max_trades_per_day": payload.max_trades_per_day,
        "stoploss_pct": payload.stoploss_pct,
        "target_profit_pct": payload.target_profit_pct,
        "trailing_sl_pct": payload.trailing_sl_pct,
        "initial_capital": payload.initial_capital,
        "position_size_pct": payload.position_size_pct,
        "candle_interval": payload.candle_interval,
        "max_daily_loss": payload.max_daily_loss,
        "poll_interval": 30,
    }
    deploy_config = payload.deploy_config or {}
    run_id = strategy_dict.get("run_name", "live") or "live"

    if run_id in live_engines and live_engines[run_id].running:
        return {"status": "already_running", "run_id": run_id}

    engine = LiveEngine(delta, run_id=run_id)
    engine.configure(
        strategy=strategy_dict,
        entry_conditions=payload.entry_conditions or DEFAULT_ENTRY_CONDITIONS,
        exit_conditions=payload.exit_conditions or DEFAULT_EXIT_CONDITIONS,
        deploy_config=deploy_config,
    )
    engine.running = True
    engine.event_log = []
    # Preserve any restored open_trades from _load_state() to avoid orphaning exchange positions
    if not engine.open_trades:
        engine.open_trades = []
    engine.closed_trades = []
    engine.trades_today = 0

    _alert_state[run_id] = {"in_trade": False, "closed_count": 0}

    async def broadcast(event: dict):
        for ws in ws_clients.copy():
            try:
                await ws.send_json({"source": "live", "run_id": run_id, **event})
            except Exception:
                if ws in ws_clients:
                    ws_clients.remove(ws)
        _check_trade_alerts(run_id, "Live", event)

    live_engines[run_id] = engine
    _live_tasks[run_id] = asyncio.create_task(engine.start(callback=broadcast))

    alerter.alert("Engine Started", f"Strategy: {run_id}\nMode: Live (REAL)", level="info")
    return {"status": "started", "run_id": run_id, "message": "Live trading started with REAL orders"}


@app.post("/api/live/stop")
async def live_stop(request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    run_id = body.get("run_id", "")
    if not run_id:
        running = [rid for rid, e in live_engines.items() if e.running]
        if running:
            run_id = running[0]
        else:
            return {"status": "not_running"}

    engine = live_engines.get(run_id)
    if not engine:
        return {"status": "not_found", "run_id": run_id}

    status_before = engine.get_status()
    engine.stop()
    task = _live_tasks.pop(run_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    live_engines.pop(run_id, None)
    _save_engine_run_to_history(status_before, "live")

    status_before["running"] = False
    status_before["run_id"] = run_id
    status_before["mode"] = "live"
    _stopped_engines[run_id] = status_before
    _alert_state.pop(run_id, None)

    pnl = round(status_before.get("total_pnl", 0), 2)
    trades = status_before.get("closed_trades", 0)
    alerter.alert(
        "Engine Stopped", f"Strategy: {run_id}\nMode: Live\nTrades: {trades}\nTotal P&L: ${pnl:,.2f}", level="warn"
    )

    return {"status": "stopped", "run_id": run_id}


@app.get("/api/live/status")
async def live_status(run_id: str = ""):
    if run_id and run_id in live_engines:
        return live_engines[run_id].get_status()
    for rid, engine in live_engines.items():
        if engine.running:
            return engine.get_status()
    return {
        "running": False,
        "run_id": "",
        "mode": "live",
        "open_positions": 0,
        "closed_trades": 0,
        "total_pnl": 0,
        "trades_today": 0,
        "strategy_name": "",
        "symbol": "",
        "open_trades": [],
        "recent_trades": [],
        "event_log": [],
    }


# ── Paper Trading ─────────────────────────────────────────────────
@app.post("/api/paper/start")
async def paper_start(payload: StrategyPayload):
    strategy_dict = {
        "run_name": payload.run_name or "Paper Strategy",
        "symbol": payload.symbol,
        "leverage": payload.leverage,
        "trade_side": payload.trade_side,
        "indicators": payload.indicators or [],
        "max_trades_per_day": payload.max_trades_per_day,
        "stoploss_pct": payload.stoploss_pct,
        "target_profit_pct": payload.target_profit_pct,
        "trailing_sl_pct": payload.trailing_sl_pct,
        "initial_capital": payload.initial_capital,
        "position_size_pct": payload.position_size_pct,
        "candle_interval": payload.candle_interval,
        "max_daily_loss": payload.max_daily_loss,
        "poll_interval": 30,
    }
    run_id = strategy_dict.get("run_name", "paper") or "paper"

    if run_id in paper_engines and paper_engines[run_id].running:
        return {"status": "already_running", "run_id": run_id}

    # When the caller sends no conditions, fall back to EMA-crossover defaults.
    # Ensure the required EMA indicator is in the indicators list so compute_dynamic_indicators
    # actually produces the column — without this the column is missing and the condition
    # always evaluates False, leaving the engine stuck in "Scanning" forever.
    effective_entry = payload.entry_conditions or DEFAULT_ENTRY_CONDITIONS
    effective_exit = payload.exit_conditions or DEFAULT_EXIT_CONDITIONS
    if not payload.entry_conditions:
        interval = payload.candle_interval or "1m"
        ema_col = f"EMA_20_{interval}"
        # Patch both the condition references and the indicators list to match
        effective_entry = [{"left": "current_close", "operator": "is_above", "right": ema_col, "connector": "AND"}]
        effective_exit = [{"left": "current_close", "operator": "is_below", "right": ema_col, "connector": "AND"}]
        inds = list(strategy_dict.get("indicators") or [])
        if ema_col not in inds:
            inds.append(ema_col)
        strategy_dict["indicators"] = inds

    engine = PaperTradingEngine(delta, run_id=run_id)
    engine.configure(
        strategy=strategy_dict,
        entry_conditions=effective_entry,
        exit_conditions=effective_exit,
    )
    engine.running = True
    engine.event_log = []
    # Preserve any restored open_trades from _load_state()
    if not engine.open_trades:
        engine.open_trades = []
    engine.closed_trades = []
    engine.trades_today = 0

    _alert_state[run_id] = {"in_trade": False, "closed_count": 0}

    async def broadcast(event: dict):
        for ws in ws_clients.copy():
            try:
                await ws.send_json({"source": "paper", "run_id": run_id, **event})
            except Exception:
                if ws in ws_clients:
                    ws_clients.remove(ws)
        _check_trade_alerts(run_id, "Paper", event)

    paper_engines[run_id] = engine
    _paper_tasks[run_id] = asyncio.create_task(engine.start(callback=broadcast))

    alerter.alert("Engine Started", f"Strategy: {run_id}\nMode: Paper", level="info")
    return {"status": "started", "run_id": run_id, "message": "Paper trading started with live data"}


@app.post("/api/paper/stop")
async def paper_stop(request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    run_id = body.get("run_id", "")
    if not run_id:
        running = [rid for rid, e in paper_engines.items() if e.running]
        if running:
            run_id = running[0]
        else:
            return {"status": "not_running"}

    engine = paper_engines.get(run_id)
    if not engine:
        return {"status": "not_found", "run_id": run_id}

    status_before = engine.get_status()
    engine.stop()
    task = _paper_tasks.pop(run_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    paper_engines.pop(run_id, None)
    _save_engine_run_to_history(status_before, "paper")

    status_before["running"] = False
    status_before["run_id"] = run_id
    status_before["mode"] = "paper"
    _stopped_engines[run_id] = status_before
    _alert_state.pop(run_id, None)

    pnl = round(status_before.get("total_pnl", 0), 2)
    trades = status_before.get("closed_trades", 0)
    alerter.alert(
        "Engine Stopped", f"Strategy: {run_id}\nMode: Paper\nTrades: {trades}\nTotal P&L: ${pnl:,.2f}", level="warn"
    )

    return {"status": "stopped", "run_id": run_id}


@app.get("/api/paper/status")
async def paper_status(run_id: str = ""):
    if run_id and run_id in paper_engines:
        return paper_engines[run_id].get_status()
    for rid, engine in paper_engines.items():
        if engine.running:
            return engine.get_status()

    status = {
        "running": False,
        "run_id": "",
        "mode": "paper",
        "open_positions": 0,
        "closed_trades": 0,
        "total_pnl": 0,
        "trades_today": 0,
        "strategy_name": "",
        "symbol": "",
        "open_trades": [],
        "recent_trades": [],
        "event_log": [],
    }
    try:
        runs = _load_runs()
        paper_runs = [r for r in runs if r.get("mode") == "paper"]
        if paper_runs:
            last = paper_runs[-1]
            trades = last.get("trades", [])
            status["strategy_name"] = last.get("run_name", "Last Paper Run")
            status["symbol"] = last.get("symbol", "")
            status["closed_trades"] = len(trades)
            status["trades_today"] = len(trades)
            status["total_pnl"] = last.get("total_pnl", 0)
            status["recent_trades"] = trades[-10:]
            status["_from_history"] = True
    except Exception:
        pass
    return status


# ── Orders / Positions ────────────────────────────────────────────
@app.post("/api/orders/place")
async def place_order(req: OrderRequest):
    check_rate_limit("place_order", max_calls=3, window_sec=5)
    try:
        product = delta.get_product_by_symbol(req.symbol)
        if not product:
            raise HTTPException(status_code=404, detail=f"Product {req.symbol} not found")
        result = delta.place_order(
            product_id=product["id"],
            size=req.size,
            side=req.side,
            order_type=req.order_type,
            limit_price=req.limit_price,
            leverage=req.leverage,
        )
        return result
    except Exception as e:
        alerter.alert("Order Failed", f"Symbol: {req.symbol}\nSide: {req.side}\nSize: {req.size}\nError: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/orders")
async def get_orders():
    try:
        orders = delta.get_orders()
        return {"status": "success", "data": orders}
    except Exception as e:
        return {"status": "error", "message": str(e)[:100], "data": []}


@app.get("/api/positions")
async def get_positions():
    try:
        positions = delta.get_positions()
        return {"status": "success", "data": positions}
    except Exception as e:
        return {"status": "error", "message": str(e)[:100], "data": []}


@app.get("/api/wallet")
async def get_wallet():
    try:
        return delta.get_wallet()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Broker Trade History ─────────────────────────────────────────
@app.get("/api/broker/trades")
async def get_broker_trades():
    """Get filled order history from Delta Exchange."""
    try:
        orders = delta.get_order_history()
        return {"status": "ok", "trades": orders}
    except Exception as e:
        return {"status": "error", "trades": [], "message": str(e)[:100]}


@app.get("/api/portfolio/summary")
async def get_portfolio_summary():
    """Aggregated portfolio data: balance, positions, recent trades."""
    try:
        wallet = delta.get_wallet()
        positions = delta.get_positions()
        orders = delta.get_order_history()

        # Extract USDT balance
        usdt_balance = 0.0
        if isinstance(wallet, list):
            for w in wallet:
                if w.get("asset_symbol", "").upper() in ("USDT", "USD"):
                    usdt_balance = float(w.get("available_balance", 0))
        elif isinstance(wallet, dict):
            if "available_balance" in wallet:
                usdt_balance = float(wallet["available_balance"])
            elif isinstance(wallet.get("result"), list):
                for w in wallet["result"]:
                    if w.get("asset_symbol", "").upper() in ("USDT", "USD"):
                        usdt_balance = float(w.get("available_balance", 0))

        # Calc unrealized P&L from open positions
        unrealized_pnl = 0.0
        open_positions = []
        for p in positions or []:
            upnl = float(p.get("unrealized_pnl", 0))
            unrealized_pnl += upnl
            if float(p.get("size", 0)) != 0:
                open_positions.append(
                    {
                        "symbol": p.get("product_symbol", p.get("symbol", "")),
                        "size": p.get("size", 0),
                        "side": "LONG" if float(p.get("size", 0)) > 0 else "SHORT",
                        "entry_price": p.get("entry_price", 0),
                        "mark_price": p.get("mark_price", p.get("mark_price", 0)),
                        "unrealized_pnl": upnl,
                        "realized_pnl": float(p.get("realized_pnl", 0)),
                        "margin": p.get("margin", 0),
                        "liquidation_price": p.get("liquidation_price", 0),
                        "leverage": p.get("leverage", ""),
                    }
                )

        return {
            "status": "ok",
            "balance": round(usdt_balance, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "open_positions": open_positions,
            "filled_orders": orders[:50] if orders else [],
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)[:200],
            "balance": 0,
            "unrealized_pnl": 0,
            "open_positions": [],
            "filled_orders": [],
        }


def _save_engine_run_to_history(status: dict, mode: str):
    """Save a completed paper/live trading run to runs.json for history."""
    try:
        closed = status.get("recent_trades", [])
        if not closed:
            closed = status.get("closed_trades", [])
            if isinstance(closed, int):
                closed = []
        if not closed:
            print(f"[{mode.upper()}] No trades to save — skipping runs.json")
            return

        runs = _load_runs()
        max_id = max([r.get("id", 0) for r in runs], default=0)

        total_pnl = round(sum(t.get("pnl", 0) for t in closed), 2)
        winners = [t for t in closed if t.get("pnl", 0) > 0]
        losers = [t for t in closed if t.get("pnl", 0) <= 0]
        win_rate = round(len(winners) / len(closed) * 100, 2) if closed else 0

        run_entry = {
            "id": max_id + 1,
            "mode": mode,
            "run_name": status.get("strategy_name", status.get("run_name", f"{mode.title()} Run")),
            "symbol": status.get("symbol", ""),
            "leverage": status.get("leverage", 10),
            "trade_side": status.get("trade_side", status.get("side", "LONG")),
            "status": "completed",
            "started_at": str(datetime.now()),
            "stopped_at": str(datetime.now()),
            "trade_count": len(closed),
            "total_pnl": total_pnl,
            "stats": {
                "total_trades": len(closed),
                "winning_trades": len(winners),
                "losing_trades": len(losers),
                "win_rate": win_rate,
                "total_pnl": total_pnl,
                "avg_profit": round(sum(t["pnl"] for t in winners) / len(winners), 2) if winners else 0,
                "avg_loss": round(sum(t["pnl"] for t in losers) / len(losers), 2) if losers else 0,
            },
            "trades": closed,
            "created_at": str(datetime.now()),
        }

        runs.append(run_entry)
        _save_runs(runs)
        print(f"[{mode.upper()}] Saved run #{run_entry['id']} to runs.json: {len(closed)} trades, P&L=${total_pnl}")
    except Exception as e:
        print(f"[{mode.upper()}] Failed to save run to history: {e}")


def _save_scalp_trade_to_history(trade: dict) -> None:
    """Save a single closed scalp trade as a run entry in runs.json."""
    try:
        pnl = round(trade.get("pnl", 0), 2)
        runs = _load_runs()
        max_id = max((r.get("id", 0) for r in runs), default=0)
        symbol = trade.get("symbol", "")
        side = trade.get("side", "")
        run_entry = {
            "id": max_id + 1,
            "mode": "scalp",
            "run_name": f"Scalp {symbol} {side}",
            "symbol": symbol,
            "leverage": trade.get("leverage", 1),
            "trade_side": side,
            "status": "completed",
            "started_at": str(trade.get("entry_time", "")),
            "stopped_at": str(trade.get("exit_time", "")),
            "trade_count": 1,
            "total_pnl": pnl,
            "stats": {
                "total_trades": 1,
                "winning_trades": 1 if pnl > 0 else 0,
                "losing_trades": 1 if pnl <= 0 else 0,
                "win_rate": 100.0 if pnl > 0 else 0.0,
                "total_pnl": pnl,
            },
            "trades": [trade],
            "created_at": str(datetime.now()),
        }
        runs.append(run_entry)
        _save_runs(runs)
        print(f"[SCALP] Saved trade #{trade.get('trade_id')} to runs.json: P&L=${pnl}")
    except Exception as e:
        print(f"[SCALP] Failed to save trade to history: {e}")


# ── Combined Engines Status (Multi-Strategy Monitor) ─────────────
@app.get("/api/engines/all")
async def engines_all():
    engines = []
    for run_id, engine in paper_engines.items():
        if engine.running:
            st = engine.get_status()
            st["run_id"] = run_id
            st["mode"] = "paper"
            engines.append(st)
    for run_id, engine in live_engines.items():
        if engine.running:
            st = engine.get_status()
            st["run_id"] = run_id
            st["mode"] = "live"
            engines.append(st)
    if not engines:
        try:
            runs = _load_runs()
            paper_runs = [r for r in runs if r.get("mode") == "paper"]
            if paper_runs:
                last = paper_runs[-1]
                trades = last.get("trades", [])
                engines.append(
                    {
                        "running": False,
                        "run_id": "",
                        "mode": "paper",
                        "open_positions": 0,
                        "closed_trades": len(trades),
                        "closed_trade_rows": trades[-200:],
                        "total_pnl": last.get("total_pnl", 0),
                        "trades_today": len(trades),
                        "strategy_name": last.get("run_name", "Last Paper Run"),
                        "symbol": last.get("symbol", ""),
                        "open_trades": [],
                        "recent_trades": trades[-10:],
                        "event_log": [],
                        "_from_history": True,
                    }
                )
        except Exception:
            pass
    return {"engines": engines, "count": len(engines)}


@app.get("/api/portfolio/history")
async def get_portfolio_history():
    """Return combined historical P&L from real trades + paper runs for monthly/yearly charts."""
    try:
        daily = {}
        runs = _load_runs()
        for r in runs:
            mode = r.get("mode", "backtest")
            if mode not in ("paper", "live"):
                continue
            started = r.get("started_at", r.get("created_at", ""))
            run_date = str(started)[:10] if started else ""
            trades = r.get("trades", [])
            if trades:
                by_date = {}
                for t in trades:
                    t_date = str(t.get("exit_time", t.get("entry_time", "")))[:10]
                    if not t_date or len(t_date) < 10:
                        t_date = run_date or ""
                    if not t_date:
                        continue
                    if t_date not in by_date:
                        by_date[t_date] = {"pnl": 0, "count": 0, "wins": 0}
                    pnl = t.get("pnl", 0)
                    by_date[t_date]["pnl"] += pnl
                    by_date[t_date]["count"] += 1
                    if pnl > 0:
                        by_date[t_date]["wins"] += 1
                for d, data in by_date.items():
                    if d not in daily:
                        daily[d] = {
                            "real_pnl": 0,
                            "real_net_pnl": 0,
                            "paper_pnl": 0,
                            "real_trades": 0,
                            "paper_trades": 0,
                            "real_wins": 0,
                            "paper_wins": 0,
                        }
                    if mode == "live":
                        daily[d]["real_pnl"] += round(data["pnl"], 2)
                        daily[d]["real_net_pnl"] += round(data["pnl"], 2)
                        daily[d]["real_trades"] += data["count"]
                        daily[d]["real_wins"] += data["wins"]
                    else:
                        daily[d]["paper_pnl"] += round(data["pnl"], 2)
                        daily[d]["paper_trades"] += data["count"]
                        daily[d]["paper_wins"] += data["wins"]
            elif run_date:
                if run_date not in daily:
                    daily[run_date] = {
                        "real_pnl": 0,
                        "real_net_pnl": 0,
                        "paper_pnl": 0,
                        "real_trades": 0,
                        "paper_trades": 0,
                        "real_wins": 0,
                        "paper_wins": 0,
                    }
                if mode == "live":
                    daily[run_date]["real_pnl"] += r.get("total_pnl", 0)
                    daily[run_date]["real_net_pnl"] += r.get("total_pnl", 0)
                    daily[run_date]["real_trades"] += r.get("trade_count", 0)
                else:
                    daily[run_date]["paper_pnl"] += r.get("total_pnl", 0)
                    daily[run_date]["paper_trades"] += r.get("trade_count", 0)

        monthly = {}
        yearly = {}
        for date_str, d in daily.items():
            ym = date_str[:7]
            y = date_str[:4]
            if ym not in monthly:
                monthly[ym] = {"real_pnl": 0, "real_net_pnl": 0, "paper_pnl": 0, "total_pnl": 0, "trades": 0, "wins": 0}
            monthly[ym]["real_pnl"] += d["real_pnl"]
            monthly[ym]["real_net_pnl"] += d.get("real_net_pnl", d["real_pnl"])
            monthly[ym]["paper_pnl"] += d["paper_pnl"]
            monthly[ym]["total_pnl"] += d["real_pnl"] + d["paper_pnl"]
            monthly[ym]["trades"] += d["real_trades"] + d["paper_trades"]
            monthly[ym]["wins"] += d["real_wins"] + d["paper_wins"]
            if y not in yearly:
                yearly[y] = {"real_pnl": 0, "real_net_pnl": 0, "paper_pnl": 0, "total_pnl": 0, "trades": 0, "wins": 0}
            yearly[y]["real_pnl"] += d["real_pnl"]
            yearly[y]["real_net_pnl"] += d.get("real_net_pnl", d["real_pnl"])
            yearly[y]["paper_pnl"] += d["paper_pnl"]
            yearly[y]["total_pnl"] += d["real_pnl"] + d["paper_pnl"]
            yearly[y]["trades"] += d["real_trades"] + d["paper_trades"]
            yearly[y]["wins"] += d["real_wins"] + d["paper_wins"]

        for m in monthly.values():
            for k in ["real_pnl", "real_net_pnl", "paper_pnl", "total_pnl"]:
                m[k] = round(m[k], 2)
        for y_val in yearly.values():
            for k in ["real_pnl", "real_net_pnl", "paper_pnl", "total_pnl"]:
                y_val[k] = round(y_val[k], 2)

        return {"status": "success", "daily": daily, "monthly": monthly, "yearly": yearly}
    except Exception as e:
        print(f"[PORTFOLIO] History error: {e}")
        return {"status": "error", "message": str(e), "daily": {}, "monthly": {}, "yearly": {}}


# ── Strategy CRUD ─────────────────────────────────────────────────
STRAT_FILE = "strategies.json"
RUNS_FILE = "runs.json"


def _load():
    if os.path.exists(STRAT_FILE):
        try:
            with open(STRAT_FILE) as f:
                return json.load(f)
        except:
            return []
    return []


def _save(d):
    tmp = STRAT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, indent=2)
    os.replace(tmp, STRAT_FILE)


def _load_runs():
    if os.path.exists(RUNS_FILE):
        try:
            with open(RUNS_FILE) as f:
                return json.load(f)
        except:
            return []
    return []


def _save_runs(d):
    tmp = RUNS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, indent=2)
    os.replace(tmp, RUNS_FILE)


@app.get("/api/strategies")
async def get_strategies():
    return _load()


@app.post("/api/strategies")
async def save_strategy(strategy: dict):
    strats = _load()
    max_id = max([s.get("id", 0) for s in strats], default=0)
    strategy.update(
        {
            "id": max_id + 1,
            "created_at": str(datetime.now()),
            "version": 1,
            "versions": [{"version": 1, "saved_at": str(datetime.now()), "changes": "Initial save"}],
        }
    )
    strats.append(strategy)
    _save(strats)
    return strategy


@app.delete("/api/strategies/{sid}")
async def delete_strategy(sid: int):
    _save([s for s in _load() if s.get("id") != sid])
    return {"deleted": sid}


@app.put("/api/strategies/{sid}")
async def update_strategy(sid: int, updates: dict):
    strats = _load()
    for s in strats:
        if s.get("id") == sid:
            ver = s.get("version", 1) + 1
            versions = s.get("versions", [])
            versions.append(
                {
                    "version": ver,
                    "saved_at": str(datetime.now()),
                    "changes": updates.get("_change_note", f"Updated to v{ver}"),
                }
            )
            if len(versions) > 20:
                versions = versions[-20:]
            updates.pop("_change_note", None)
            s.update(updates)
            s["version"] = ver
            s["versions"] = versions
            s["updated_at"] = str(datetime.now())
            break
    _save(strats)
    return {"updated": sid}


@app.get("/api/strategies/{sid}/versions")
async def get_strategy_versions(sid: int):
    strats = _load()
    for s in strats:
        if s.get("id") == sid:
            return {"versions": s.get("versions", [])}
    raise HTTPException(status_code=404, detail="Strategy not found")


@app.get("/api/runs")
async def get_runs():
    runs = _load_runs()
    return [{k: v for k, v in r.items() if k not in ("trades", "equity")} for r in runs]


@app.get("/api/runs/{rid}")
async def get_run(rid: int):
    for r in _load_runs():
        if r.get("id") == rid:
            return r
    raise HTTPException(status_code=404, detail="Run not found")


@app.delete("/api/runs/{rid}")
async def delete_run(rid: int):
    runs = _load_runs()
    _save_runs([r for r in runs if r.get("id") != rid])
    return {"deleted": rid}


@app.get("/api/runs/{rid}/csv")
async def export_run_csv(rid: int):
    import csv
    import io

    runs = _load_runs()
    run = None
    for r in runs:
        if r.get("id") == rid:
            run = r
            break
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    trades = run.get("trades", [])
    if not trades:
        raise HTTPException(status_code=404, detail="No trades")
    output = io.StringIO()
    fields = [
        "id",
        "entry_time",
        "exit_time",
        "entry_price",
        "exit_price",
        "pnl",
        "cumulative",
        "exit_reason",
        "side",
        "leverage",
        "size",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for t in trades:
        writer.writerow({k: _csv_safe(t.get(k, "")) for k in fields})
    output.seek(0)
    name = run.get("run_name", f"run_{rid}").replace(" ", "_")
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={name}_trades.csv"},
    )


@app.get("/api/funding/{symbol}")
async def get_funding_rates(symbol: str):
    """Get funding rate history."""
    try:
        rates = delta.get_funding_history(symbol)
        return {"status": "ok", "data": rates}
    except Exception as e:
        return {"status": "error", "message": str(e)[:100]}


# ── Strategy Validation ───────────────────────────────────────────
@app.post("/api/validate-strategy")
async def validate_strategy(request: Request):
    """Deep validation of strategy before deployment."""
    body = await request.json()
    errors = []
    warnings = []

    symbol = body.get("symbol", "")
    if not symbol:
        errors.append("No trading symbol selected")

    entry = body.get("entry_conditions", [])
    exit_conds = body.get("exit_conditions", [])
    if not entry:
        errors.append("No entry conditions defined")
    if not exit_conds:
        warnings.append("No exit conditions — trades will only close at SL/TP")

    leverage = body.get("leverage", 10)
    if leverage > 50:
        warnings.append(f"High leverage ({leverage}x) — liquidation risk is significant")
    if leverage > 100:
        errors.append(f"Extreme leverage ({leverage}x) — very high liquidation risk")

    sl_pct = body.get("stoploss_pct", 0)
    tp_pct = body.get("target_profit_pct", 0)
    if sl_pct and tp_pct and tp_pct < sl_pct:
        warnings.append(f"Risk:Reward unfavorable — SL {sl_pct}% vs Target {tp_pct}%")
    if sl_pct == 0:
        warnings.append("No stop-loss set — unlimited downside risk")

    max_trades = body.get("max_trades_per_day", 5)
    if max_trades > 20:
        warnings.append(f"High trade frequency ({max_trades}/day) — check for overtrading")

    position_size = body.get("position_size_pct", 100)
    if position_size > 100:
        warnings.append(f"Position size {position_size}% exceeds capital")

    # Check for contradictory conditions
    for c in entry:
        lhs = c.get("left", "")
        op = c.get("operator", "")
        rhs = c.get("right", "")
        for c2 in entry:
            if c2 is c:
                continue
            if c2.get("left") == lhs and c2.get("right") == rhs:
                if op in ("is_above", "crosses_above") and c2.get("operator") in ("is_below", "crosses_below"):
                    errors.append(f"Contradictory: {lhs} both above and below {rhs}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "symbol": symbol,
            "entry_conditions": len(entry),
            "exit_conditions": len(exit_conds),
            "leverage": leverage,
            "sl_pct": sl_pct,
            "tp_pct": tp_pct,
        },
    }


# ── CSV Exports ───────────────────────────────────────────────────
@app.get("/api/paper/trades/csv")
async def export_paper_trades_csv(run_id: str = ""):
    """Export paper trading trades to CSV."""
    import csv
    import io

    trades = []
    engine = paper_engines.get(run_id) if run_id else None
    if not engine:
        for e in paper_engines.values():
            if hasattr(e, "closed_trades") and e.closed_trades:
                engine = e
                break
    if engine and hasattr(engine, "closed_trades") and engine.closed_trades:
        trades = engine.closed_trades
    else:
        # Fallback: load from runs.json
        runs = _load_runs()
        paper_runs = [r for r in runs if r.get("mode") == "paper" and r.get("trades")]
        if paper_runs:
            trades = paper_runs[-1]["trades"]
    if not trades:
        raise HTTPException(status_code=404, detail="No paper trades available")
    output = io.StringIO()
    fields = [
        "id",
        "symbol",
        "side",
        "entry_time",
        "exit_time",
        "entry_price",
        "exit_price",
        "notional",
        "leverage",
        "margin",
        "pnl",
        "exit_reason",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for t in trades:
        writer.writerow(
            {k: _csv_safe(str(t.get(k, "")) if k in ("entry_time", "exit_time") else t.get(k, "")) for k in fields}
        )
    output.seek(0)
    name = f"paper_trades_{datetime.now().strftime('%Y%m%d')}"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={name}.csv"},
    )


@app.get("/api/live/trades/csv")
async def export_live_trades_csv(run_id: str = ""):
    """Export live trading trades to CSV."""
    import csv
    import io

    trades = []
    engine = live_engines.get(run_id) if run_id else None
    if not engine:
        for e in live_engines.values():
            if hasattr(e, "closed_trades") and e.closed_trades:
                engine = e
                break
    if engine and hasattr(engine, "closed_trades") and engine.closed_trades:
        trades = engine.closed_trades
    else:
        # Fallback: load from runs.json
        runs = _load_runs()
        live_runs = [r for r in runs if r.get("mode") == "live" and r.get("trades")]
        if live_runs:
            trades = live_runs[-1]["trades"]
    if not trades:
        raise HTTPException(status_code=404, detail="No live trades available")
    output = io.StringIO()
    fields = [
        "id",
        "symbol",
        "side",
        "entry_time",
        "exit_time",
        "entry_price",
        "exit_price",
        "notional",
        "leverage",
        "margin",
        "pnl",
        "exit_reason",
        "order_id",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for t in trades:
        writer.writerow(
            {k: _csv_safe(str(t.get(k, "")) if k in ("entry_time", "exit_time") else t.get(k, "")) for k in fields}
        )
    output.seek(0)
    name = f"live_trades_{datetime.now().strftime('%Y%m%d')}"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={name}.csv"},
    )


# ── Scalp Engine ──────────────────────────────────────────────────
_scalp_engine: Optional[ScalpEngine] = None
_SCALP_FILE = os.path.join(_HERE, "scalp_trades.json")


def _load_scalp_trades():
    if os.path.exists(_SCALP_FILE):
        try:
            with open(_SCALP_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def _save_scalp_trades(trades):
    tmp = _SCALP_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(trades, f, indent=2, default=str)
    os.replace(tmp, _SCALP_FILE)


def _scalp_persist_trade(trade: dict) -> None:
    """Persist a single closed scalp trade to disk (auto + manual exits)."""
    try:
        trades = _load_scalp_trades()
        # Deduplicate by trade_id + entry_time (trade_id can repeat across restarts)
        key = (trade.get("trade_id"), trade.get("entry_time"))
        if not any((t.get("trade_id"), t.get("entry_time")) == key for t in trades):
            trades.append(trade)
            _save_scalp_trades(trades)
    except Exception as e:
        print(f"[SCALP] Failed to persist trade: {e}")

    # Telegram alert for auto-exits (TP/SL hits from monitor loop)
    reason = trade.get("exit_reason", trade.get("reason", "auto"))
    if reason != "manual":  # manual exits already alerted in scalp_exit
        pnl = round(trade.get("pnl", 0), 2)
        alerter.alert(
            "Scalp Exit",
            f"Symbol: {trade.get('symbol', '—')}\nSide: {trade.get('side', '')}\n"
            f"P&L: ${pnl:,.2f}\nReason: {reason}\n"
            f"Entry: ${trade.get('entry_price', 0):,.2f}\nExit: ${trade.get('exit_price', 0):,.2f}",
            level="info" if pnl >= 0 else "warn",
        )


def _get_scalp_engine():
    global _scalp_engine
    if _scalp_engine is None:
        _scalp_engine = ScalpEngine(delta, on_trade_closed=_scalp_persist_trade)
    return _scalp_engine


@app.get("/api/scalp/status")
async def scalp_status():
    eng = _get_scalp_engine()
    status = eng.get_status()
    status["file_trades"] = list(reversed(_load_scalp_trades()[-50:]))
    return status


@app.get("/api/scalp/trades")
async def scalp_trades():
    return _load_scalp_trades()


@app.post("/api/scalp/enter")
async def scalp_enter(request: Request):
    body = await request.json()
    eng = _get_scalp_engine()
    symbol = body.get("symbol", "BTCUSDT")
    raw_side = body.get("side", "BUY").upper()
    side = "LONG" if raw_side == "BUY" else "SHORT"
    qty_usdt = float(body.get("qty_usdt", 100))
    leverage = int(body.get("leverage", 10))
    mode = body.get("mode", "paper")

    # Convert USDT qty to contract size (1 contract = 1 USD on Delta)
    size = int(qty_usdt * leverage)

    try:
        result = await eng.enter_trade(
            symbol=symbol,
            side=side,
            size=size,
            leverage=leverage,
            target_pct=float(body.get("take_profit_pct", 0)),
            sl_pct=float(body.get("stop_loss_pct", 0)),
            target_usd=float(body.get("tp_usd", 0)),
            sl_usd=float(body.get("sl_usd", 0)),
            mode=mode,
        )
        if result.get("status") == "error":
            alerter.alert(
                "Scalp Entry Failed",
                f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nError: {result.get('message', 'unknown')}",
            )
        elif result.get("status") == "ok":
            price = result.get("trade", {}).get("entry_price", 0)
            alerter.alert(
                "Scalp Entry",
                f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nSize: {size} contracts\nLeverage: {leverage}x\nEntry: ${price:,.2f}",
                level="info",
            )
        return result
    except Exception as e:
        alerter.alert("Scalp Entry Error", f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nError: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scalp/exit")
async def scalp_exit(request: Request):
    body = await request.json()
    trade_id = int(body.get("trade_id", 0))
    eng = _get_scalp_engine()
    try:
        result = await eng.exit_trade(trade_id, reason="manual")
        # Persistence is handled by the engine's on_trade_closed callback.
        # _save_scalp_trade_to_history for the Results page:
        if result.get("status") == "ok" and result.get("trade"):
            _save_scalp_trade_to_history(result["trade"])
            t = result["trade"]
            pnl = round(t.get("pnl", 0), 2)
            alerter.alert(
                "Scalp Exit",
                f"Symbol: {t.get('symbol', '—')}\nSide: {t.get('side', '')}\nP&L: ${pnl:,.2f}\nReason: manual\nEntry: ${t.get('entry_price', 0):,.2f}\nExit: ${t.get('exit_price', 0):,.2f}",
                level="info" if pnl >= 0 else "warn",
            )
        elif result.get("status") == "error":
            alerter.alert("Scalp Exit Failed", f"Trade ID: {trade_id}\nError: {result.get('message', 'unknown')}")
        return result
    except Exception as e:
        alerter.alert("Scalp Exit Error", f"Trade ID: {trade_id}\nError: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/scalp/trades/{trade_id}/targets")
async def update_scalp_targets(trade_id: int, request: Request):
    """Modify TP/SL for an active scalp trade."""
    body = await request.json()
    eng = _get_scalp_engine()
    kwargs = {}
    for key in ("target_price", "sl_price", "target_usd", "sl_usd"):
        if key in body and body[key] is not None:
            kwargs[key] = float(body[key])
    if not kwargs:
        raise HTTPException(status_code=400, detail="No target fields provided")
    result = await eng.update_trade_targets(trade_id, **kwargs)
    if result.get("status") == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result


# ── WebSocket ─────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # Authenticate WebSocket via session cookie
    token = ws.cookies.get("cryptoforge_session", "")
    if not _validate_session(token):
        await ws.close(code=4001, reason="Unauthorized")
        return
    await ws.accept()
    ws_clients.append(ws)
    try:
        while True:
            paper_sts = {rid: e.get_status() for rid, e in paper_engines.items()}
            live_sts = {rid: e.get_status() for rid, e in live_engines.items()}
            await ws.send_json(
                {
                    "type": "status",
                    "paper_engines": paper_sts,
                    "live_engines": live_sts,
                    "paper_running": any(s.get("running") for s in paper_sts.values()),
                    "live_running": any(s.get("running") for s in live_sts.values()),
                }
            )
            await asyncio.sleep(5)
    except (WebSocketDisconnect, Exception):
        if ws in ws_clients:
            ws_clients.remove(ws)


# ── Run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("  CryptoForge — Starting Backend")
    print(f"  Open: http://{config.APP_HOST}:{config.APP_PORT}")
    print("=" * 60)
    uvicorn.run("app:app", host=config.APP_HOST, port=config.APP_PORT, reload=False, log_level="info")
