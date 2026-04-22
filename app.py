"""
app.py — CryptoForge FastAPI Backend
Perpetual futures algo-trading platform powered by Delta Exchange.
Production-ready: multi-engine, WebSocket, portfolio history, full CRUD.
"""

import asyncio
import hashlib
import inspect
import json
import logging
import os
import secrets
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urlparse

# ── Module-level logger ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("cryptoforge")

import pandas as pd

# ── Guaranteed path fix ───────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
os.chdir(_HERE)

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import alerter
import config  # must be first — calls load_dotenv()
from broker.delta import DeltaClient, get_candles_binance
from engine.backtest import run_backtest
from engine.live import LiveEngine
from engine.paper_trading import PaperTradingEngine
from engine.scalp import PendingScalpEntry, ScalpEngine, ScalpTrade
from state_store import get_json_store


# ── Shutdown hook: auto-save running engines to runs.json ─────
def _shutdown_save_engines():
    """Save all running paper/live engines to runs.json on shutdown."""
    for run_id, engine in list(paper_engines.items()):
        if engine.running:
            try:
                status = engine.get_status()
                engine.stop()
                _save_engine_run_to_history(status, "paper")
                _logger.info("Saved paper engine %s on shutdown", run_id)
            except Exception as e:
                _logger.error("Failed to save paper engine %s on shutdown: %s", run_id, e)
    for run_id, engine in list(live_engines.items()):
        if engine.running:
            try:
                status = engine.get_status()
                engine.stop()
                _save_engine_run_to_history(status, "live")
                _logger.info("Saved live engine %s on shutdown", run_id)
            except Exception as e:
                _logger.error("Failed to save live engine %s on shutdown: %s", run_id, e)


import atexit

atexit.register(_shutdown_save_engines)


async def _shutdown_runtime_engines() -> None:
    scalp_engine = globals().get("_scalp_engine")
    if scalp_engine is not None:
        try:
            await scalp_engine.shutdown()
        except Exception as exc:
            _logger.warning("Failed to shutdown scalp engine during app shutdown: %s", exc)
    _shutdown_save_engines()


@asynccontextmanager
async def _app_lifespan(_: FastAPI):
    try:
        yield
    finally:
        await _shutdown_runtime_engines()


# Initialize
app = FastAPI(title="CryptoForge", version="2.0.0", lifespan=_app_lifespan)
_ALLOWED_ORIGINS = [
    "https://crypto.philforge.in",
    "https://www.crypto.philforge.in",
    "http://localhost:9000",
    "http://127.0.0.1:9000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-CSRF-Token", "X-Requested-With"],
)


from error_handlers import register_error_handlers

register_error_handlers(app)

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize Delta client
delta = DeltaClient()
APP_BOOT_TS = time.time()

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


def _state_dir_candidates() -> List[str]:
    explicit = (os.getenv("CRYPTOFORGE_STATE_DIR") or "").strip()
    if explicit:
        yield os.path.abspath(os.path.expanduser(explicit))
    if sys.platform == "darwin":
        yield os.path.join(os.path.expanduser("~"), "Library", "Application Support", "CryptoForge")
    yield os.path.join(os.path.expanduser("~"), ".cryptoforge")
    yield os.path.join(tempfile.gettempdir(), "cryptoforge")
    yield _HERE


def _state_dir_is_writable(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        return os.access(path, os.W_OK)
    except Exception:
        return False


def _resolve_state_dir() -> str:
    for path in _state_dir_candidates():
        if _state_dir_is_writable(path):
            return path
    return _HERE


_STATE_DIR = _resolve_state_dir()
_STATE_DB_FILE = (os.getenv("CRYPTOFORGE_STATE_DB_PATH") or "").strip()
_LEGACY_SESSION_FILE = os.path.join(_HERE, ".sessions.json")
_SESSION_FILE = os.path.join(_STATE_DIR, "sessions.json")

_BUCKET_SESSIONS = "sessions"
_BUCKET_STRATEGIES = "strategies"
_BUCKET_RUNS = "runs"
_BUCKET_SCALP_TRADES = "scalp_trades"
_BUCKET_SCALP_EVENTS = "scalp_events"
_BUCKET_SCALP_RUNTIME = "scalp_runtime"


def _bootstrap_session_store() -> None:
    _seed_mapping_bucket(_BUCKET_SESSIONS, _SESSION_FILE, _LEGACY_SESSION_FILE)


def _resolve_state_file(filename: str, *subdirs: str) -> tuple[str, str]:
    legacy_path = os.path.join(_HERE, filename)
    cleaned_subdirs = [str(part).strip().strip("/\\") for part in subdirs if str(part).strip()]
    state_dir = os.path.join(_STATE_DIR, *cleaned_subdirs) if cleaned_subdirs else _STATE_DIR
    state_path = os.path.join(state_dir, filename)
    if state_path == legacy_path:
        return legacy_path, legacy_path

    migration_sources = [legacy_path]
    flat_state_path = os.path.join(_STATE_DIR, filename)
    if cleaned_subdirs and flat_state_path not in {legacy_path, state_path}:
        migration_sources.append(flat_state_path)

    for source_path in migration_sources:
        if os.path.exists(state_path) or not os.path.exists(source_path):
            continue
        try:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            shutil.copy2(source_path, state_path)
        except Exception as exc:
            _logger.warning("Failed to migrate %s to %s: %s", source_path, state_path, exc)
            return source_path, source_path
    return legacy_path, state_path


def _current_state_db_file() -> str:
    override = str(globals().get("_STATE_DB_FILE") or "").strip()
    if override:
        return os.path.abspath(os.path.expanduser(override))
    return os.path.join(_STATE_DIR, "cryptoforge_state.db")


def _get_state_store():
    return get_json_store(_current_state_db_file())


def _candidate_state_paths(*paths: str) -> list[str]:
    seen = set()
    candidates = []
    for raw in paths:
        path = str(raw or "").strip()
        if not path:
            continue
        resolved = os.path.abspath(os.path.expanduser(path))
        if resolved in seen:
            continue
        seen.add(resolved)
        candidates.append(resolved)
    return candidates


def _load_legacy_json(path: str):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError, IOError) as exc:
        _logger.warning("Failed to read legacy state %s: %s", path, exc)
        return None


def _seed_mapping_bucket(bucket: str, *paths: str):
    store = _get_state_store()
    if store.count(bucket) > 0:
        return store
    for path in _candidate_state_paths(*paths):
        payload = _load_legacy_json(path)
        if isinstance(payload, dict):
            store.replace_mapping(bucket, payload)
            break
    return store


def _seed_list_bucket(bucket: str, *paths: str, key_fn):
    store = _get_state_store()
    if store.count(bucket) > 0:
        return store
    for path in _candidate_state_paths(*paths):
        payload = _load_legacy_json(path)
        if isinstance(payload, list):
            store.replace_list(bucket, payload, key_fn=key_fn)
            break
    return store


def _seed_singleton_bucket(bucket: str, key: str, *paths: str):
    store = _get_state_store()
    if store.count(bucket) > 0:
        return store
    for path in _candidate_state_paths(*paths):
        payload = _load_legacy_json(path)
        if isinstance(payload, dict):
            store.put(bucket, key, payload)
            break
    return store


_bootstrap_session_store()


CSRF_COOKIE_NAME = "cryptoforge_csrf"
_SESSION_ABSOLUTE_SEC = int(os.getenv("CRYPTOFORGE_SESSION_ABSOLUTE_SEC", "86400"))
_SESSION_IDLE_SEC = int(os.getenv("CRYPTOFORGE_SESSION_IDLE_SEC", "14400"))
_SESSION_TOUCH_SEC = int(os.getenv("CRYPTOFORGE_SESSION_TOUCH_SEC", "300"))


def _session_now() -> datetime:
    return datetime.now()


def _normalize_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        raw = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            for fmt, sample in (("%Y-%m-%d %H:%M:%S", raw[:19]), ("%Y-%m-%d", raw[:10])):
                try:
                    dt = datetime.strptime(sample, fmt)
                    break
                except ValueError:
                    dt = None
            if dt is None:
                return None
    if dt.tzinfo is not None:
        return dt.astimezone().replace(tzinfo=None)
    return dt


def _client_ip(request) -> str:
    if request is None:
        return "unknown"
    forwarded = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if forwarded:
        return forwarded
    client = getattr(request, "client", None)
    return client.host if client else "unknown"


def _session_user_agent_hash(request) -> str:
    if request is None:
        return ""
    user_agent = (request.headers.get("user-agent") or "").strip()
    if not user_agent:
        return ""
    return hashlib.sha256(user_agent.encode("utf-8")).hexdigest()


def _normalize_session_record(value, now: Optional[datetime] = None) -> Optional[dict]:
    now = _normalize_datetime(now or _session_now()) or _session_now()
    if isinstance(value, str):
        expires_at = _normalize_datetime(value)
        if not expires_at:
            return None
        baseline = min(now, expires_at)
        return {
            "created_at": baseline.isoformat(),
            "last_seen_at": baseline.isoformat(),
            "expires_at": expires_at.isoformat(),
            "ua_hash": "",
        }
    if not isinstance(value, dict):
        return None
    expires_at = _normalize_datetime(value.get("expires_at") or value.get("expires") or value.get("exp"))
    if not expires_at:
        return None
    created_at = _normalize_datetime(value.get("created_at")) or min(now, expires_at)
    last_seen_at = _normalize_datetime(value.get("last_seen_at")) or created_at
    return {
        "created_at": created_at.isoformat(),
        "last_seen_at": last_seen_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "ua_hash": str(value.get("ua_hash") or ""),
    }


def _session_expired(record: dict, now: Optional[datetime] = None) -> bool:
    now = _normalize_datetime(now or _session_now()) or _session_now()
    expires_at = _normalize_datetime(record.get("expires_at"))
    if not expires_at or now > expires_at:
        return True
    last_seen_at = _normalize_datetime(record.get("last_seen_at"))
    if last_seen_at and (now - last_seen_at).total_seconds() > _SESSION_IDLE_SEC:
        return True
    return False


def _load_sessions() -> dict:
    try:
        store = _seed_mapping_bucket(_BUCKET_SESSIONS, _SESSION_FILE, _LEGACY_SESSION_FILE)
        data = store.get_mapping(_BUCKET_SESSIONS)
        now = _session_now()
        sessions = {}
        changed = False
        for token, raw in data.items():
            record = _normalize_session_record(raw, now=now)
            if not record or _session_expired(record, now=now):
                changed = True
                continue
            sessions[token] = record
            if record != raw:
                changed = True
        if changed:
            _save_sessions(sessions)
        return sessions
    except Exception as exc:
        _logger.warning("Failed to load session store %s: %s", _current_state_db_file(), exc)
        return {}


def _save_sessions(sessions: dict):
    try:
        store = _get_state_store()
        store.replace_mapping(_BUCKET_SESSIONS, dict(sessions or {}))
    except Exception as exc:
        _logger.warning("Failed to save session store %s: %s", _current_state_db_file(), exc)


def _create_session(request=None) -> str:
    sessions = _load_sessions()
    token = secrets.token_hex(32)
    now = _session_now()
    sessions[token] = {
        "created_at": now.isoformat(),
        "last_seen_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=_SESSION_ABSOLUTE_SEC)).isoformat(),
        "ua_hash": _session_user_agent_hash(request),
    }
    _save_sessions(sessions)
    return token


def _destroy_session(token: str) -> None:
    if not token:
        return
    sessions = _load_sessions()
    if token in sessions:
        sessions.pop(token, None)
        _save_sessions(sessions)


def _validate_session(token: str, request=None, touch: bool = True) -> bool:
    if not token:
        return False
    sessions = _load_sessions()
    record = sessions.get(token)
    if not record:
        return False
    now = _session_now()
    if _session_expired(record, now=now):
        sessions.pop(token, None)
        _save_sessions(sessions)
        return False
    expected_ua = str(record.get("ua_hash") or "")
    actual_ua = _session_user_agent_hash(request)
    if expected_ua and actual_ua and not secrets.compare_digest(expected_ua, actual_ua):
        _logger.warning("Rejecting session due to user-agent mismatch")
        sessions.pop(token, None)
        _save_sessions(sessions)
        return False
    if request is not None and touch:
        last_seen = _normalize_datetime(record.get("last_seen_at"))
        should_touch = not last_seen or (now - last_seen).total_seconds() >= _SESSION_TOUCH_SEC
        if not expected_ua and actual_ua:
            record["ua_hash"] = actual_ua
            should_touch = True
        if should_touch:
            record["last_seen_at"] = now.isoformat()
            sessions[token] = record
            _save_sessions(sessions)
    return True


def _get_session_token(request: Request) -> str:
    token = request.cookies.get("cryptoforge_session", "")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
    return token


def _create_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def _get_csrf_token(request: Request) -> str:
    return request.cookies.get(CSRF_COOKIE_NAME, "")


def _set_csrf_cookie(response: Response, token: str, request: Request) -> None:
    response.set_cookie(
        CSRF_COOKIE_NAME,
        token,
        max_age=_SESSION_ABSOLUTE_SEC,
        httponly=False,
        samesite="strict",
        secure=_is_https_request(request),
        path="/",
    )


def _ensure_csrf_cookie(response: Response, request: Request) -> str:
    token = _get_csrf_token(request) or _create_csrf_token()
    _set_csrf_cookie(response, token, request)
    return token


def _is_https_request(request: Request) -> bool:
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "").lower()
    return proto == "https"


def _is_same_origin_request(request: Request) -> bool:
    expected_origin = str(request.base_url).rstrip("/")
    origin = (request.headers.get("origin") or "").rstrip("/")
    if origin:
        return origin == expected_origin
    referer = request.headers.get("referer") or ""
    if referer:
        parsed = urlparse(referer)
        referer_origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        return referer_origin == expected_origin
    return True


def _is_no_response_returned_error(exc: Exception) -> bool:
    return isinstance(exc, RuntimeError) and str(exc).strip().rstrip(".") == "No response returned"


def _client_closed_response(request: Request) -> Response:
    response = Response(status_code=499)
    rid = str(getattr(getattr(request, "state", None), "request_id", "") or "")
    if rid:
        response.headers["X-Request-ID"] = rid
    return response


async def _call_next_or_client_closed(request: Request, call_next):
    try:
        return await call_next(request)
    except RuntimeError as exc:
        if _is_no_response_returned_error(exc):
            _logger.info(
                "[%s] Client closed request before response completed: %s %s",
                getattr(getattr(request, "state", None), "request_id", "-"),
                request.method,
                request.url.path,
            )
            return _client_closed_response(request)
        raise


def _apply_security_headers(request: Request, response: Response) -> Response:
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=(), browsing-topics=()"
    csp = (
        "default-src 'self'; "
        "script-src 'self'; "
        "script-src-elem 'self'; "
        "script-src-attr 'none'; "
        "style-src 'self' https://fonts.googleapis.com; "
        "style-src-elem 'self' https://fonts.googleapis.com; "
        "style-src-attr 'unsafe-inline'; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "img-src 'self' data: blob: https:; "
        "connect-src 'self' ws: wss:; "
        "worker-src 'self' blob:; "
        "media-src 'self' data: blob:; "
        "manifest-src 'self'; "
        "frame-src 'none'; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "object-src 'none'"
    )
    response.headers["Content-Security-Policy"] = csp
    if _is_https_request(request):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# ── Auth Middleware (Dependency-based) ────────────────────────────
async def require_auth(request: Request):
    path = request.url.path
    if path in (
        "/api/auth/login",
        "/api/auth/status",
        "/api/health",
        "/api/ready",
        "/login",
        "/",
        "/favicon.ico",
        "/manifest.webmanifest",
        "/site.webmanifest",
        "/sw.js",
        "/apple-touch-icon.png",
    ):
        return
    if path.startswith("/static"):
        return
    token = _get_session_token(request)
    if not _validate_session(token, request=request):
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach a unique request-id to every request for log tracing."""
    import uuid

    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = rid
    response = await _call_next_or_client_closed(request, call_next)
    response.headers["X-Request-ID"] = rid
    return response


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    public = (
        "/",
        "/api/auth/login",
        "/api/auth/status",
        "/api/health",
        "/api/ready",
        "/favicon.ico",
        "/manifest.webmanifest",
        "/site.webmanifest",
        "/sw.js",
        "/apple-touch-icon.png",
    )
    if path in public or path.startswith("/static"):
        return await _call_next_or_client_closed(request, call_next)
    if path.startswith("/api/"):
        token = _get_session_token(request)
        if not _validate_session(token, request=request):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await _call_next_or_client_closed(request, call_next)


@app.middleware("http")
async def csrf_middleware(request: Request, call_next):
    if request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"} and request.url.path.startswith("/api/"):
        if request.url.path != "/api/auth/login":
            token = _get_session_token(request)
            if _validate_session(token, request=request):
                cookie_token = _get_csrf_token(request)
                header_token = request.headers.get("X-CSRF-Token", "")
                if not cookie_token or not header_token or not secrets.compare_digest(cookie_token, header_token):
                    return JSONResponse({"detail": "CSRF validation failed"}, status_code=403)
                if not _is_same_origin_request(request):
                    return JSONResponse({"detail": "Origin validation failed"}, status_code=403)
    return await _call_next_or_client_closed(request, call_next)


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await _call_next_or_client_closed(request, call_next)
    return _apply_security_headers(request, response)


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
    if len(_rate_limits) > 10_000:
        stale = [k for k, v in _rate_limits.items() if not v or now - v[-1] > window_sec]
        for k in stale:
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
    position_size_mode: str = "pct"  # "pct" or "fixed_qty"
    fixed_qty: float = 0.0  # quantity in base asset (e.g. 0.1 BTC)
    stoploss_pct: float = 5.0
    target_profit_pct: float = 10.0
    trailing_sl_pct: float = 0.0
    max_trades_per_day: int = 5
    max_daily_loss: float = 0.0
    fee_pct: float = 0.0
    slippage_bps: float = 0.0
    spread_bps: float = 0.0
    funding_bps_per_8h: float = 0.0
    compounding: bool = False
    indicators: List[str] = []
    entry_conditions: Optional[List[dict]] = None
    exit_conditions: Optional[List[dict]] = None
    candle_interval: str = "5m"
    deploy_config: Optional[dict] = None


def _build_backtest_assumptions(payload: StrategyPayload) -> List[str]:
    assumptions = [
        f"Fees are modeled at {payload.fee_pct:g}% per side.",
    ]
    if payload.spread_bps > 0 or payload.slippage_bps > 0:
        assumptions.append(
            "Execution impact applies "
            f"{payload.spread_bps:g} bps spread and {payload.slippage_bps:g} bps slippage per side."
        )
    else:
        assumptions.append("Execution impact is disabled; fills use raw trigger and next-open prices.")
    if payload.funding_bps_per_8h != 0:
        assumptions.append(
            f"Funding is prorated at {payload.funding_bps_per_8h:g} bps per 8h using average position notional."
        )
    else:
        assumptions.append("Funding is disabled for this run.")
    assumptions.append("Stops and targets still assume first-touch execution within each candle.")
    return assumptions


class OrderRequest(BaseModel):
    symbol: str
    size: float
    side: str = "buy"
    order_type: str = "market_order"
    limit_price: Optional[float] = None
    leverage: int = 10


class StateRestoreRequest(BaseModel):
    snapshot: dict
    replace: bool = True
    force: bool = False


_ALWAYS_AVAILABLE_FIELDS = {
    "current_open",
    "current_high",
    "current_low",
    "current_close",
    "current_volume",
    "yesterday_high",
    "yesterday_low",
    "yesterday_close",
    "yesterday_open",
    "Time_Of_Day",
    "Day_Of_Week",
    "number",
    "true",
    "false",
}
_CPR_FIELDS = {
    "CPR_Pivot",
    "CPR_TC",
    "CPR_BC",
    "CPR_R1",
    "CPR_R2",
    "CPR_R3",
    "CPR_R4",
    "CPR_R5",
    "CPR_S1",
    "CPR_S2",
    "CPR_S3",
    "CPR_S4",
    "CPR_S5",
    "CPR_width_pct",
    "CPR_is_narrow",
    "CPR_is_moderate",
    "CPR_is_wide",
}
_ORB_FIELDS = {
    "ORB_high",
    "ORB_low",
    "ORB_Range",
    "ORB_is_breakout_up",
    "ORB_is_breakout_down",
    "ORB_is_inside",
}
_SUPPORTED_INDICATOR_PREFIXES = (
    "EMA_",
    "SMA_",
    "RSI_",
    "Supertrend_",
    "MACD_",
    "BB_",
    "VWAP_",
    "ATR_",
    "ADX_",
    "StochRSI_",
)


def _normalize_condition(cond: dict) -> dict:
    normalized = dict(cond or {})
    if "left" not in normalized and "lhs" in normalized:
        normalized["left"] = normalized.get("lhs")
    if "right" not in normalized and "rhs" in normalized:
        normalized["right"] = normalized.get("rhs")
    if "connector" not in normalized and "logic" in normalized:
        normalized["connector"] = normalized.get("logic")
    return normalized


def _parse_interval_minutes(interval: str) -> int:
    raw = str(interval or "5m").strip().lower()
    try:
        qty = int(raw[:-1])
        unit = raw[-1]
    except (TypeError, ValueError):
        return 5
    if unit == "m":
        return qty
    if unit == "h":
        return qty * 60
    if unit == "d":
        return qty * 1440
    if unit == "w":
        return qty * 10080
    return 5


def _base_indicator_for_field(field: str) -> Optional[str]:
    if not isinstance(field, str) or not field:
        return None
    if field in _ALWAYS_AVAILABLE_FIELDS or field in _CPR_FIELDS or field in _ORB_FIELDS:
        return None
    if field.startswith("Signal_Candle"):
        return None
    base = field.split("__", 1)[0]
    if base.startswith(_SUPPORTED_INDICATOR_PREFIXES):
        return base
    return None


def _normalize_strategy_runtime(
    *,
    indicators: Optional[List[str]],
    entry_conditions: Optional[List[dict]],
    exit_conditions: Optional[List[dict]],
    candle_interval: str,
) -> dict:
    effective_indicators = list(dict.fromkeys(indicators or []))
    effective_entry = [_normalize_condition(c) for c in (entry_conditions or [])]
    effective_exit = [_normalize_condition(c) for c in (exit_conditions or [])]
    added_indicators = []
    unsupported_fields = []
    unresolved_fields = []
    warnings = []
    errors = []

    if not effective_entry:
        ema_col = f"EMA_20_{candle_interval or '5m'}"
        effective_entry = [{"left": "current_close", "operator": "is_above", "right": ema_col, "connector": "AND"}]
        if ema_col not in effective_indicators:
            effective_indicators.append(ema_col)
            added_indicators.append(ema_col)
        warnings.append(f"No entry conditions — default Close > {ema_col} will be used")
    if not effective_exit:
        warnings.append("No exit conditions — trades will only close at SL/TP or manual stop")

    cpr_present = any(str(ind).startswith("CPR_") for ind in effective_indicators)
    orb_present = any(str(ind).startswith("ORB_") for ind in effective_indicators)

    for cond in effective_entry + effective_exit:
        op = cond.get("operator", "")
        for key in ("left", "right"):
            field = cond.get(key, "")
            if not field or field in _ALWAYS_AVAILABLE_FIELDS:
                continue
            if key == "right" and op in ("is_true", "is_false"):
                continue
            if field.startswith("Signal_Candle"):
                if field not in unsupported_fields:
                    unsupported_fields.append(field)
                continue
            if field in _CPR_FIELDS:
                if not cpr_present and field not in unresolved_fields:
                    unresolved_fields.append(field)
                continue
            if field in _ORB_FIELDS:
                if not orb_present and field not in unresolved_fields:
                    unresolved_fields.append(field)
                continue
            base_indicator = _base_indicator_for_field(field)
            if base_indicator:
                if base_indicator not in effective_indicators:
                    effective_indicators.append(base_indicator)
                    added_indicators.append(base_indicator)
                continue
            if field not in unresolved_fields:
                unresolved_fields.append(field)

    return {
        "indicators": effective_indicators,
        "entry_conditions": effective_entry,
        "exit_conditions": effective_exit,
        "added_indicators": added_indicators,
        "unsupported_fields": unsupported_fields,
        "unresolved_fields": unresolved_fields,
        "warnings": warnings,
        "errors": errors,
    }


def _estimate_warmup_days(candle_interval: str, indicators: List[str]) -> int:
    base_minutes = max(_parse_interval_minutes(candle_interval), 1)
    warmup_days = 3

    def _target_minutes(parts: List[str]) -> int:
        if parts and parts[-1].endswith(("m", "h", "d", "w")):
            return max(_parse_interval_minutes(parts[-1]), base_minutes)
        return base_minutes

    for ind in indicators or []:
        parts = str(ind or "").split("_")
        name = parts[0] if parts else ""
        target_minutes = _target_minutes(parts)
        days_for_indicator = 0

        try:
            if name in {"EMA", "SMA", "RSI", "ATR", "ADX", "StochRSI"}:
                period = int(parts[1]) if len(parts) > 1 else 14
                candles = period * 2
                days_for_indicator = int((candles * target_minutes) / 1440) + 2
            elif name == "MACD":
                slow = int(parts[2]) if len(parts) > 2 else 26
                signal = int(parts[3]) if len(parts) > 3 else 9
                candles = (slow + signal) * 2
                days_for_indicator = int((candles * target_minutes) / 1440) + 2
            elif name == "BB":
                period = int(parts[1]) if len(parts) > 1 else 20
                candles = period * 2
                days_for_indicator = int((candles * target_minutes) / 1440) + 2
            elif name == "Supertrend":
                period = int(parts[1]) if len(parts) > 1 else 10
                candles = period * 3
                days_for_indicator = int((candles * target_minutes) / 1440) + 2
            elif name in {"VWAP", "Previous"}:
                days_for_indicator = 2
            elif name == "ORB":
                days_for_indicator = 2
            elif name == "CPR":
                timeframe = parts[1].lower() if len(parts) > 1 else "day"
                if timeframe == "month":
                    days_for_indicator = 70
                elif timeframe == "week":
                    days_for_indicator = 16
                else:
                    days_for_indicator = 3
        except (TypeError, ValueError):
            days_for_indicator = 3

        warmup_days = max(warmup_days, days_for_indicator)

    return warmup_days


# ── Favicon ───────────────────────────────────────────────────────
@app.api_route("/favicon.ico", methods=["GET", "HEAD"])
async def favicon():
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" rx="20" fill="#8b5cf6"/><text y=".9em" x="50" text-anchor="middle" font-size="70" font-family="sans-serif">⬡</text></svg>'
    return Response(content=svg, media_type="image/svg+xml")


@app.api_route("/apple-touch-icon.png", methods=["GET", "HEAD"])
async def apple_touch_icon():
    return FileResponse(
        os.path.join(_HERE, "static", "pwa-icons", "apple-touch-icon.png"),
        media_type="image/png",
    )


@app.api_route("/manifest.webmanifest", methods=["GET", "HEAD"])
async def manifest_webmanifest():
    return FileResponse(
        os.path.join(_HERE, "static", "manifest.webmanifest"),
        media_type="application/manifest+json",
    )


@app.api_route("/site.webmanifest", methods=["GET", "HEAD"])
async def site_webmanifest():
    return FileResponse(
        os.path.join(_HERE, "static", "manifest.webmanifest"),
        media_type="application/manifest+json",
    )


@app.api_route("/sw.js", methods=["GET", "HEAD"])
async def service_worker():
    return FileResponse(
        os.path.join(_HERE, "static", "sw.js"),
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )


# ── Serve Frontend ────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_frontend(request: Request):
    token = _get_session_token(request)
    if not _validate_session(token, request=request):
        login_path = os.path.join(_HERE, "login.html")
        if os.path.exists(login_path):
            with open(login_path, encoding="utf-8") as f:
                resp = HTMLResponse(f.read())
                resp.headers["Cache-Control"] = "no-store"
                return resp
        return HTMLResponse("<h2>login.html not found</h2>")
    html_path = os.path.join(_HERE, "strategy.html")
    if os.path.exists(html_path):
        with open(html_path, encoding="utf-8") as f:
            resp = HTMLResponse(f.read())
            resp.headers["Cache-Control"] = "no-store"
            _ensure_csrf_cookie(resp, request)
            return resp
    return HTMLResponse("<h2>strategy.html not found</h2>")


# ── Auth Endpoints ────────────────────────────────────────────────
@app.post("/api/auth/login")
async def auth_login(request: Request):
    ip = _client_ip(request)
    _check_login_rate(ip)
    body = await request.json()
    password = str(body.get("password", ""))
    if secrets.compare_digest(password, str(AUTH_PIN)):
        _clear_login_attempts(ip)
        token = _create_session(request=request)
        resp = JSONResponse({"status": "ok", "message": "Login successful"})
        is_https = _is_https_request(request)
        resp.headers["Cache-Control"] = "no-store"
        resp.set_cookie(
            "cryptoforge_session",
            token,
            max_age=_SESSION_ABSOLUTE_SEC,
            httponly=True,
            samesite="lax",
            secure=is_https,
            path="/",
        )
        _set_csrf_cookie(resp, _create_csrf_token(), request)
        return resp
    _record_failed_login(ip)
    raise HTTPException(status_code=401, detail="Invalid PIN")


@app.get("/api/auth/status")
async def auth_status(request: Request):
    token = _get_session_token(request)
    authenticated = _validate_session(token, request=request)
    resp = JSONResponse({"authenticated": authenticated})
    resp.headers["Cache-Control"] = "no-store"
    if authenticated:
        _ensure_csrf_cookie(resp, request)
    return resp


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    token = _get_session_token(request)
    _destroy_session(token)
    resp = JSONResponse({"status": "ok"})
    resp.headers["Cache-Control"] = "no-store"
    resp.delete_cookie("cryptoforge_session", path="/")
    resp.delete_cookie(CSRF_COOKIE_NAME, path="/")
    return resp


# ── CSV formula-injection guard ───────────────────────────────────
_CSV_INJECT_CHARS = ("=", "+", "-", "@", "\t", "\r")


def _csv_safe(value):
    """Prefix string values that start with formula chars to prevent CSV injection."""
    if isinstance(value, str) and value.startswith(_CSV_INJECT_CHARS):
        return "'" + value
    return value


def _today_local_date():
    return _session_now().date()


def _run_record_date(run: dict):
    dt = _normalize_datetime(run.get("created_at") or run.get("started_at"))
    return dt.date() if dt else None


def _saved_runs_for_date(runs: List[dict], mode: str, target_date):
    target_mode = str(mode or "").lower()
    return [r for r in runs if str(r.get("mode", "")).lower() == target_mode and _run_record_date(r) == target_date]


def _trade_signature(trade: dict) -> tuple:
    """Stable signature for deduplicating persisted trades across runs/history saves."""

    def _rounded(value, digits: int = 8):
        try:
            return round(float(value), digits)
        except (TypeError, ValueError):
            return 0.0

    return (
        str(trade.get("symbol", "")),
        str(trade.get("side", trade.get("trade_side", ""))).upper(),
        str(trade.get("entry_time", "")),
        str(trade.get("exit_time", "")),
        _rounded(trade.get("entry_price")),
        _rounded(trade.get("exit_price")),
        _rounded(trade.get("pnl"), 2),
        str(trade.get("exit_reason", trade.get("reason", ""))),
    )


def _run_trade_signatures(run: dict) -> set[tuple]:
    trades = run.get("trades", []) or []
    return {_trade_signature(trade) for trade in trades if isinstance(trade, dict)}


_OPS_BUCKETS = (
    _BUCKET_SESSIONS,
    _BUCKET_STRATEGIES,
    _BUCKET_RUNS,
    _BUCKET_SCALP_TRADES,
    _BUCKET_SCALP_EVENTS,
    _BUCKET_SCALP_RUNTIME,
    "engine_live_state",
    "engine_paper_state",
)


def _bucket_counts_snapshot() -> dict:
    store = _get_state_store()
    return {bucket: store.count(bucket) for bucket in _OPS_BUCKETS}


def _engine_recovery_candidates(bucket: str) -> list[dict]:
    snapshot = _get_state_store().export_snapshot()
    candidates = []
    for state_key, entry in dict(((snapshot.get("buckets") or {}).get(bucket) or {})).items():
        payload = dict((entry or {}).get("payload") or {})
        open_trades = payload.get("open_trades") or []
        if payload.get("in_trade") or open_trades:
            candidates.append(
                {
                    "state_key": str(state_key),
                    "run_name": str(payload.get("strategy_name") or state_key),
                    "symbol": str(payload.get("symbol") or ""),
                    "open_trades": len(open_trades),
                    "saved_at": payload.get("saved_at"),
                }
            )
    return candidates


def _persisted_scalp_runtime_summary() -> dict:
    runtime = dict(_load_scalp_runtime() or {})
    feed = dict(runtime.get("feed_metrics") or {})
    return {
        "open_trades": len(runtime.get("open_trades") or []),
        "pending_entries": len(runtime.get("pending_entries") or []),
        "feed_state": str(feed.get("state") or "waiting"),
        "feed_age_ms": feed.get("age_ms"),
        "feed_symbol": feed.get("symbol"),
        "updated_at": feed.get("updated_at"),
    }


def _runtime_registry_summary() -> dict:
    scalp_engine = globals().get("_scalp_engine")
    scalp_running = bool(scalp_engine and getattr(scalp_engine, "_running", False))
    scalp_open = len(getattr(scalp_engine, "open_trades", {}) or {}) if scalp_engine else 0
    scalp_pending = len(getattr(scalp_engine, "pending_entries", {}) or {}) if scalp_engine else 0
    return {
        "live_running_runs": sorted(
            [run_id for run_id, engine in live_engines.items() if getattr(engine, "running", False)]
        ),
        "paper_running_runs": sorted(
            [run_id for run_id, engine in paper_engines.items() if getattr(engine, "running", False)]
        ),
        "stopped_engine_snapshots": len(_stopped_engines),
        "scalp_running": scalp_running,
        "scalp_open_trades": scalp_open,
        "scalp_pending_entries": scalp_pending,
    }


def _runtime_recovery_summary() -> dict:
    registry = _runtime_registry_summary()
    scalp_state = _persisted_scalp_runtime_summary()
    return {
        "live_candidates": _engine_recovery_candidates("engine_live_state"),
        "paper_candidates": _engine_recovery_candidates("engine_paper_state"),
        "scalp_persisted_open_trades": scalp_state["open_trades"],
        "scalp_persisted_pending_entries": scalp_state["pending_entries"],
        "scalp_recovery_required": bool(
            scalp_state["open_trades"] and not registry["scalp_running"] and registry["scalp_open_trades"] == 0
        ),
    }


def _ops_state_summary() -> dict:
    store = _get_state_store()
    store_health = store.health()
    redis_client = _get_redis()
    registry = _runtime_registry_summary()
    recovery = _runtime_recovery_summary()
    scalp_state = _persisted_scalp_runtime_summary()
    delta_configured = bool(delta._is_configured())
    ready_checks = {
        "auth_pin_configured": bool(AUTH_PIN),
        "state_store_writable": bool(store_health.get("writable")),
        "state_store_exists": bool(store_health.get("exists")),
        "delta_configured": delta_configured,
    }
    recovery_required = bool(
        recovery["live_candidates"] or recovery["paper_candidates"] or recovery["scalp_recovery_required"]
    )
    return {
        "status": "degraded" if recovery_required else "ok",
        "uptime_sec": round(max(time.time() - APP_BOOT_TS, 0.0), 1),
        "time": str(datetime.now()),
        "ready": bool(ready_checks["auth_pin_configured"] and ready_checks["state_store_writable"]),
        "ready_checks": ready_checks,
        "delta_configured": delta_configured,
        "state_store": store_health,
        "bucket_counts": _bucket_counts_snapshot(),
        "runtime": registry,
        "recovery": recovery,
        "scalp_runtime": scalp_state,
        "rate_limit_backend": "redis" if redis_client is not None else "memory",
    }


def _runtime_has_activity(summary: Optional[dict] = None) -> bool:
    payload = dict(summary or _runtime_registry_summary())
    return bool(
        payload.get("live_running_runs")
        or payload.get("paper_running_runs")
        or payload.get("scalp_running")
        or payload.get("scalp_open_trades")
        or payload.get("scalp_pending_entries")
    )


async def _reset_runtime_memory() -> None:
    global _scalp_engine

    scalp_engine = globals().get("_scalp_engine")
    if scalp_engine is not None:
        try:
            await scalp_engine.shutdown()
        except Exception as exc:
            _logger.warning("Failed to shutdown scalp engine during runtime reset: %s", exc)
    _scalp_engine = None

    for tasks_dict in (_live_tasks, _paper_tasks):
        for run_id, task_ref in list(tasks_dict.items()):
            if task_ref and not task_ref.done():
                task_ref.cancel()
                try:
                    await task_ref
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    _logger.warning("Failed to cancel task %s during runtime reset: %s", run_id, exc)
        tasks_dict.clear()

    live_engines.clear()
    paper_engines.clear()
    _stopped_engines.clear()
    _alert_state.clear()


def _validate_state_snapshot(snapshot: dict) -> dict:
    payload = dict(snapshot or {})
    if payload.get("format") != "cryptoforge-state-snapshot/v1":
        raise HTTPException(status_code=400, detail="Unsupported state snapshot format")
    buckets = payload.get("buckets")
    if not isinstance(buckets, dict):
        raise HTTPException(status_code=400, detail="State snapshot buckets are missing or invalid")
    return payload


# ── Health ────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    summary = _ops_state_summary()
    return {
        "status": summary["status"],
        "time": summary["time"],
        "ready": summary["ready"],
        "delta_configured": summary["delta_configured"],
        "live_running": bool(summary["runtime"]["live_running_runs"]),
        "paper_running": bool(summary["runtime"]["paper_running_runs"]),
        "scalp_running": bool(summary["runtime"]["scalp_running"]),
        "state_store": {
            "exists": summary["state_store"]["exists"],
            "writable": summary["state_store"]["writable"],
            "size_bytes": summary["state_store"]["size_bytes"],
        },
        "recovery_required": bool(
            summary["recovery"]["live_candidates"]
            or summary["recovery"]["paper_candidates"]
            or summary["recovery"]["scalp_recovery_required"]
        ),
    }


@app.get("/api/ready")
async def ready():
    summary = _ops_state_summary()
    return {
        "status": "ready" if summary["ready"] and summary["status"] == "ok" else summary["status"],
        "ready": summary["ready"],
        "checks": summary["ready_checks"],
        "runtime": summary["runtime"],
        "recovery": summary["recovery"],
        "state_store": summary["state_store"],
        "time": summary["time"],
    }


@app.get("/api/ops/state/summary")
async def ops_state_summary():
    return _ops_state_summary()


@app.get("/api/ops/state/backup")
async def ops_state_backup():
    snapshot = _get_state_store().export_snapshot()
    snapshot["meta"] = {
        "bucket_counts": _bucket_counts_snapshot(),
        "runtime": _runtime_registry_summary(),
        "recovery": _runtime_recovery_summary(),
        "generated_at": str(datetime.now()),
    }
    return snapshot


@app.post("/api/ops/state/restore")
async def ops_state_restore(payload: StateRestoreRequest, request: Request):
    snapshot = _validate_state_snapshot(payload.snapshot)
    runtime_summary = _runtime_registry_summary()
    if _runtime_has_activity(runtime_summary):
        if not payload.force:
            raise HTTPException(status_code=409, detail="Active runtime detected. Stop engines before restoring state.")
        await emergency_stop(request)
    _get_state_store().import_snapshot(snapshot, replace=payload.replace)
    await _reset_runtime_memory()
    restored = _ops_state_summary()
    return {
        "status": "ok",
        "message": "State snapshot restored",
        "replace": bool(payload.replace),
        "bucket_counts": restored["bucket_counts"],
        "recovery": restored["recovery"],
        "time": restored["time"],
    }


# ── Emergency Stop ────────────────────────────────────────────────
@app.post("/api/emergency-stop")
async def emergency_stop(request: Request):
    """Emergency kill all running engines."""
    results = {}
    stopped = 0
    scalp_closed = 0

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

    # Stop scalp mode too, closing any open scalp positions before halting the engine.
    scalp_engine = globals().get("_scalp_engine")
    scalp_run_saver = globals().get("_save_scalp_trade_to_history")
    if scalp_engine is not None:
        for trade_id in list(getattr(scalp_engine, "open_trades", {}).keys()):
            try:
                result = await scalp_engine.exit_trade(int(trade_id), reason="kill_switch")
                if result.get("status") == "ok" and result.get("trade"):
                    if callable(scalp_run_saver):
                        scalp_run_saver(result["trade"])
                    results[f"scalp:trade:{trade_id}"] = "closed"
                    scalp_closed += 1
                else:
                    results[f"scalp:trade:{trade_id}"] = result.get("message", "error")
            except Exception as e:
                results[f"scalp:trade:{trade_id}"] = f"error: {str(e)}"
        try:
            if getattr(scalp_engine, "_running", False):
                scalp_engine.stop()
                results["scalp:engine"] = "stopped"
        except Exception as e:
            results["scalp:engine"] = f"error: {str(e)}"

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
        "scalp_closed": scalp_closed,
        "message": f"Emergency stop executed — {stopped} engine(s) stopped, {scalp_closed} scalp trade(s) closed",
        "results": results,
        "timestamp": str(datetime.now()),
    }


# ── Dashboard ─────────────────────────────────────────────────────
@app.get("/api/dashboard/summary")
async def dashboard_summary(request: Request):
    """Get dashboard summary data."""
    strats = _load()
    runs = _load_runs()
    backtest_runs = [r for r in runs if str(r.get("mode", "backtest")).lower() == "backtest"]

    paper_running = any(e.running for e in paper_engines.values())
    live_running = any(e.running for e in live_engines.values())
    paper_statuses = [e.get_status() for e in paper_engines.values() if e.running]
    live_statuses = [e.get_status() for e in live_engines.values() if e.running]

    paper_pnl_val = 0
    paper_trades_val = 0
    live_pnl_val = 0
    live_trades_val = 0
    today_date = _today_local_date()

    if paper_statuses:
        paper_pnl_val = sum(s.get("total_pnl", 0) for s in paper_statuses)
        paper_trades_val = sum(s.get("trades_today", 0) for s in paper_statuses)
    else:
        today_paper_runs = _saved_runs_for_date(runs, "paper", today_date)
        if today_paper_runs:
            paper_pnl_val = sum(r.get("total_pnl", 0) for r in today_paper_runs)
            paper_trades_val = sum(r.get("trade_count", len(r.get("trades", []))) for r in today_paper_runs)

    if live_statuses:
        live_pnl_val = sum(s.get("total_pnl", 0) for s in live_statuses)
        live_trades_val = sum(s.get("trades_today", 0) for s in live_statuses)
    else:
        today_live_runs = _saved_runs_for_date(runs, "live", today_date)
        if today_live_runs:
            live_pnl_val = sum(r.get("total_pnl", 0) for r in today_live_runs)
            live_trades_val = sum(r.get("trade_count", len(r.get("trades", []))) for r in today_live_runs)

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
        "backtest_count": len(backtest_runs),
        "paper_running": paper_running,
        "live_running": live_running,
        "paper_count": len(paper_statuses),
        "live_count": len(live_statuses),
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
async def check_broker(request: Request = None):
    check_rate_limit(
        "broker_check",
        max_calls=6,
        window_sec=30,
        client_ip=request.client.host if request and request.client else "global",
    )
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
async def connect_broker(request: Request = None):
    check_rate_limit(
        "broker_connect",
        max_calls=4,
        window_sec=30,
        client_ip=request.client.host if request and request.client else "global",
    )
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
    "paxg": "PAXGUSD",
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


def _json_object(body):
    if isinstance(body, dict):
        return body
    raise HTTPException(status_code=400, detail="JSON body must be an object")


async def _read_json_body(request: Request) -> Dict:
    try:
        return _json_object(await request.json())
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc


def _body_value(body: Dict, *keys, default=None):
    for key in keys:
        if key in body:
            return body.get(key)
    return default


def _parse_float_field(body: Dict, *keys, default=0.0, min_value=None):
    key = keys[0] if keys else "value"
    raw = _body_value(body, *keys, default=default)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {key}") from exc
    if min_value is not None and value < min_value:
        raise HTTPException(status_code=400, detail=f"{key} must be >= {min_value}")
    return value


def _parse_int_field(body: Dict, *keys, default=0, min_value=None):
    key = keys[0] if keys else "value"
    raw = _body_value(body, *keys, default=default)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {key}") from exc
    if min_value is not None and value < min_value:
        raise HTTPException(status_code=400, detail=f"{key} must be >= {min_value}")
    return value


def _normalize_scalp_symbol(symbol: str, *, allow_blank: bool = False) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "" if allow_blank else "BTCUSDT"
    if raw in {"GOLD", "GOLDUSDT"}:
        raw = "PAXGUSD"
    if raw not in _DELTA_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Unsupported scalp symbol: {raw}")
    return raw


def _parse_scalp_mode(body: Dict, *keys, default: str = "paper") -> str:
    raw = str(_body_value(body, *keys, default=default) or default).strip().lower()
    if raw not in {"paper", "live"}:
        raise HTTPException(status_code=400, detail="mode must be paper or live")
    return raw


def _parse_scalp_qty_mode(body: Dict, *keys, default: str = "usdt") -> str:
    raw = str(_body_value(body, *keys, default=default) or default).strip().lower()
    aliases = {
        "usdt": "usdt",
        "usd": "usdt",
        "margin": "usdt",
        "base": "base",
        "qty": "base",
        "coin": "base",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise HTTPException(status_code=400, detail="qty_mode must be usdt or base")
    return normalized


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
        _logger.exception("Ticker error: %s", e)
        return {"status": "error", "message": str(e)[:100]}


@app.get("/api/ticker/{symbol}")
async def get_single_ticker(symbol: str):
    """Get ticker for a single symbol."""
    try:
        return delta.get_ticker(symbol)
    except Exception as e:
        return {"status": "error", "message": str(e)[:100]}


# ── Delta Exchange symbols (have perp futures) ───────────────────
_DELTA_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "PAXGUSD"}


# ── Local Candle Cache ────────────────────────────────────────────
_CACHE_DIR = os.path.join(_HERE, "cache", "candles")
os.makedirs(_CACHE_DIR, exist_ok=True)


def _cache_path(symbol: str, interval: str) -> str:
    """Return path to the pickle cache file for a symbol/interval pair."""
    return os.path.join(_CACHE_DIR, f"{symbol}_{interval}.pkl")


def _load_cache(symbol: str, interval: str) -> pd.DataFrame:
    """Load cached candle data from disk. Returns empty DataFrame if no cache."""
    path = _cache_path(symbol, interval)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_pickle(path)  # nosec B301 — only reads self-generated cache files
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        return df
    except Exception as e:
        _logger.warning("Cache read failed %s: %s", path, e)
        return pd.DataFrame()


def _save_cache(df: pd.DataFrame, symbol: str, interval: str):
    """Save candle data to disk (pickle). Merges with existing cache."""
    if df.empty:
        return
    path = _cache_path(symbol, interval)
    try:
        existing = _load_cache(symbol, interval)
        if not existing.empty:
            merged = pd.concat([existing, df])
            merged = merged[~merged.index.duplicated(keep="last")]
            merged.sort_index(inplace=True)
            df = merged
        df.to_pickle(path)
        _logger.info("Cache saved %d candles to %s", len(df), path)
    except Exception as e:
        _logger.warning("Cache save failed %s: %s", path, e)


# ── Data Fetch ────────────────────────────────────────────────────
def _fetch_data(symbol: str, from_date: str, to_date: str, candle_interval: str = "5m") -> pd.DataFrame:
    _logger.info("Data fetch: %s %s %s→%s", symbol, candle_interval, from_date, to_date)

    from_ts = pd.Timestamp(from_date)
    to_ts = pd.Timestamp(to_date)

    # ── 1. Check local cache first ────────────────────────────
    cached = _load_cache(symbol, candle_interval)
    if not cached.empty:
        # Normalize both to tz-naive for comparison
        idx = cached.index.tz_localize(None) if cached.index.tz else cached.index
        cache_start, cache_end = idx.min(), idx.max()
        if cache_start <= from_ts and cache_end >= to_ts:
            sliced = cached.loc[(idx >= from_ts) & (idx <= to_ts)]
            if len(sliced) > 0:
                _logger.info("Cache HIT: %d candles for %s/%s", len(sliced), symbol, candle_interval)
                return sliced
        _logger.info("Cache partial: has %s→%s, need %s→%s", cache_start, cache_end, from_date, to_date)

    # ── 2. Fetch from API (Binance first — fast, free, no key) ──
    df = get_candles_binance(symbol, resolution=candle_interval, start=from_date, end=to_date)
    if not df.empty:
        _logger.info("Binance: %d candles fetched", len(df))
        _save_cache(df, symbol, candle_interval)
        return df

    # ── 3. Fallback to Delta Exchange ──────────────────────────
    if symbol in _DELTA_SYMBOLS:
        df = delta.get_candles(symbol, resolution=candle_interval, start=from_date, end=to_date)
        if not df.empty:
            _logger.info("Delta: %d candles fetched", len(df))
            _save_cache(df, symbol, candle_interval)
            return df

    raise Exception(f"No candle data for {symbol} (tried cache + Binance + Delta)")


# ── Backtest ──────────────────────────────────────────────────────
@app.post("/api/backtest")
async def api_run_backtest(payload: StrategyPayload, request: Request = None):
    check_rate_limit(
        "backtest",
        max_calls=3,
        window_sec=30,
        client_ip=request.client.host if request and request.client else "global",
    )
    try:
        _logger.info(
            "Backtest: %s | %s %sx | %s %s",
            payload.run_name,
            payload.symbol,
            payload.leverage,
            payload.trade_side,
            payload.candle_interval,
        )

        runtime = _normalize_strategy_runtime(
            indicators=payload.indicators,
            entry_conditions=payload.entry_conditions,
            exit_conditions=payload.exit_conditions,
            candle_interval=payload.candle_interval,
        )
        if runtime["errors"]:
            return {
                "status": "error",
                "message": runtime["errors"][0],
                "warnings": runtime["warnings"],
            }
        if runtime["unsupported_fields"]:
            return {
                "status": "error",
                "message": "Unsupported condition fields: " + ", ".join(runtime["unsupported_fields"]),
                "warnings": runtime["warnings"],
            }
        if runtime["unresolved_fields"]:
            return {
                "status": "error",
                "message": "Missing indicator coverage for fields: " + ", ".join(runtime["unresolved_fields"]),
                "warnings": runtime["warnings"],
            }

        # Fetch extra data before from_date for indicator warm-up.
        from datetime import datetime, timedelta

        warmup_days = _estimate_warmup_days(payload.candle_interval, runtime["indicators"])
        try:
            actual_from = datetime.strptime(payload.from_date, "%Y-%m-%d")
            warmup_from = (actual_from - timedelta(days=warmup_days)).strftime("%Y-%m-%d")
        except Exception:
            warmup_from = payload.from_date

        df_raw = await asyncio.to_thread(
            _fetch_data,
            symbol=payload.symbol,
            from_date=warmup_from,
            to_date=payload.to_date,
            candle_interval=payload.candle_interval,
        )

        if df_raw.empty:
            return {"status": "error", "message": "No data returned."}

        strategy_config = payload.model_dump()
        strategy_config["indicators"] = runtime["indicators"]
        results = await asyncio.to_thread(
            run_backtest,
            df_raw=df_raw,
            entry_conditions=runtime["entry_conditions"],
            exit_conditions=runtime["exit_conditions"],
            strategy_config=strategy_config,
        )

        if results.get("status") == "success":
            results["strategy_warnings"] = runtime["warnings"]
            results["model_assumptions"] = _build_backtest_assumptions(payload)
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
                "indicators": runtime["indicators"],
                "entry_conditions": runtime["entry_conditions"],
                "exit_conditions": runtime["exit_conditions"],
                "candle_interval": payload.candle_interval,
                "initial_capital": payload.initial_capital,
                "position_size_pct": payload.position_size_pct,
                "position_size_mode": payload.position_size_mode,
                "fixed_qty": payload.fixed_qty,
                "fee_pct": payload.fee_pct,
                "slippage_bps": payload.slippage_bps,
                "spread_bps": payload.spread_bps,
                "funding_bps_per_8h": payload.funding_bps_per_8h,
                "compounding": payload.compounding,
                "trailing_sl_pct": payload.trailing_sl_pct,
                "max_trades_per_day": payload.max_trades_per_day,
                "max_daily_loss": payload.max_daily_loss,
                "strategy_warnings": results.get("strategy_warnings", []),
                "model_assumptions": results.get("model_assumptions", []),
                "stats": results["stats"],
                "monthly": results.get("monthly", []),
                "yearly": results.get("yearly", []),
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
    runtime = _normalize_strategy_runtime(
        indicators=payload.indicators,
        entry_conditions=payload.entry_conditions,
        exit_conditions=payload.exit_conditions,
        candle_interval=payload.candle_interval,
    )
    if runtime["errors"]:
        return {
            "status": "error",
            "message": runtime["errors"][0],
            "warnings": runtime["warnings"],
        }
    if runtime["unsupported_fields"]:
        return {
            "status": "error",
            "message": "Unsupported condition fields: " + ", ".join(runtime["unsupported_fields"]),
            "warnings": runtime["warnings"],
        }
    if runtime["unresolved_fields"]:
        return {
            "status": "error",
            "message": "Missing indicator coverage for fields: " + ", ".join(runtime["unresolved_fields"]),
            "warnings": runtime["warnings"],
        }

    strategy_dict = {
        "run_name": payload.run_name or "Live Strategy",
        "symbol": payload.symbol,
        "leverage": payload.leverage,
        "trade_side": payload.trade_side,
        "indicators": runtime["indicators"],
        "max_trades_per_day": payload.max_trades_per_day,
        "stoploss_pct": payload.stoploss_pct,
        "target_profit_pct": payload.target_profit_pct,
        "trailing_sl_pct": payload.trailing_sl_pct,
        "initial_capital": payload.initial_capital,
        "position_size_pct": payload.position_size_pct,
        "position_size_mode": payload.position_size_mode,
        "fixed_qty": payload.fixed_qty,
        "fee_pct": payload.fee_pct,
        "slippage_bps": payload.slippage_bps,
        "spread_bps": payload.spread_bps,
        "funding_bps_per_8h": payload.funding_bps_per_8h,
        "compounding": payload.compounding,
        "candle_interval": payload.candle_interval,
        "max_daily_loss": payload.max_daily_loss,
        "poll_interval": 30,
    }
    deploy_config = payload.deploy_config or {}
    run_id = strategy_dict.get("run_name", "live") or "live"

    if run_id in live_engines and live_engines[run_id].running:
        return {"status": "already_running", "run_id": run_id}

    try:
        product = delta.get_product_by_symbol(payload.symbol)
    except Exception as e:
        return {"status": "error", "message": f"Live start preflight failed for {payload.symbol}: {e}"}
    if not product:
        return {"status": "error", "message": f"Product not found for {payload.symbol}. Live engine was not started."}

    engine = LiveEngine(delta, run_id=run_id, state_db_path=_current_state_db_file())
    engine.configure(
        strategy=strategy_dict,
        entry_conditions=runtime["entry_conditions"],
        exit_conditions=runtime["exit_conditions"],
        deploy_config=deploy_config,
    )
    engine.running = True

    _alert_state[run_id] = {"in_trade": bool(engine.open_trades), "closed_count": len(engine.closed_trades)}

    async def broadcast(event: dict):
        for ws in ws_clients.copy():
            try:
                await ws.send_json({"source": "live", "run_id": run_id, **event})
            except Exception:
                if ws in ws_clients:
                    ws_clients.remove(ws)
        _check_trade_alerts(run_id, "Live", event)
        # Save each closed trade to runs.json for the All Results page
        if event.get("type") == "exit" and event.get("trade"):
            _save_trade_to_history(event["trade"], "live", run_name=run_id)

    live_engines[run_id] = engine
    _live_tasks[run_id] = asyncio.create_task(engine.start(callback=broadcast))

    alerter.alert("Engine Started", f"Strategy: {run_id}\nMode: Live (REAL)", level="info")
    return {
        "status": "started",
        "run_id": run_id,
        "message": "Live trading started with REAL orders",
        "warnings": runtime["warnings"],
    }


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
    if run_id:
        stopped = _stopped_engines.get(run_id)
        if stopped and stopped.get("mode") == "live":
            return {**stopped, "running": False, "run_id": run_id, "mode": "live"}
        return _empty_engine_status("live", run_id=run_id)
    for rid, engine in live_engines.items():
        if engine.running:
            return engine.get_status()
    return _history_engine_status("live")


# ── Paper Trading ─────────────────────────────────────────────────
@app.post("/api/paper/start")
async def paper_start(payload: StrategyPayload):
    runtime = _normalize_strategy_runtime(
        indicators=payload.indicators,
        entry_conditions=payload.entry_conditions,
        exit_conditions=payload.exit_conditions,
        candle_interval=payload.candle_interval,
    )
    if runtime["errors"]:
        return {
            "status": "error",
            "message": runtime["errors"][0],
            "warnings": runtime["warnings"],
        }
    if runtime["unsupported_fields"]:
        return {
            "status": "error",
            "message": "Unsupported condition fields: " + ", ".join(runtime["unsupported_fields"]),
            "warnings": runtime["warnings"],
        }
    if runtime["unresolved_fields"]:
        return {
            "status": "error",
            "message": "Missing indicator coverage for fields: " + ", ".join(runtime["unresolved_fields"]),
            "warnings": runtime["warnings"],
        }

    strategy_dict = {
        "run_name": payload.run_name or "Paper Strategy",
        "symbol": payload.symbol,
        "leverage": payload.leverage,
        "trade_side": payload.trade_side,
        "indicators": runtime["indicators"],
        "max_trades_per_day": payload.max_trades_per_day,
        "stoploss_pct": payload.stoploss_pct,
        "target_profit_pct": payload.target_profit_pct,
        "trailing_sl_pct": payload.trailing_sl_pct,
        "initial_capital": payload.initial_capital,
        "position_size_pct": payload.position_size_pct,
        "position_size_mode": payload.position_size_mode,
        "fixed_qty": payload.fixed_qty,
        "fee_pct": payload.fee_pct,
        "slippage_bps": payload.slippage_bps,
        "spread_bps": payload.spread_bps,
        "funding_bps_per_8h": payload.funding_bps_per_8h,
        "compounding": payload.compounding,
        "candle_interval": payload.candle_interval,
        "max_daily_loss": payload.max_daily_loss,
        "poll_interval": 30,
    }
    run_id = strategy_dict.get("run_name", "paper") or "paper"

    if run_id in paper_engines and paper_engines[run_id].running:
        return {"status": "already_running", "run_id": run_id}

    engine = PaperTradingEngine(delta, run_id=run_id, state_db_path=_current_state_db_file())
    engine.configure(
        strategy=strategy_dict,
        entry_conditions=runtime["entry_conditions"],
        exit_conditions=runtime["exit_conditions"],
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
        # Save each closed trade to runs.json for the All Results page
        if event.get("type") == "exit" and event.get("trade"):
            _save_trade_to_history(event["trade"], "paper", run_name=run_id)

    paper_engines[run_id] = engine
    _paper_tasks[run_id] = asyncio.create_task(engine.start(callback=broadcast))

    alerter.alert("Engine Started", f"Strategy: {run_id}\nMode: Paper", level="info")
    return {
        "status": "started",
        "run_id": run_id,
        "message": "Paper trading started with live data",
        "warnings": runtime["warnings"],
    }


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


def _empty_engine_status(mode: str, run_id: str = "") -> dict:
    return {
        "running": False,
        "run_id": run_id,
        "mode": mode,
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


def _history_engine_status(mode: str) -> dict:
    status = _empty_engine_status(mode)
    try:
        runs = [r for r in _load_runs() if r.get("mode") == mode]
        if runs:
            last = runs[-1]
            trades = last.get("trades", [])
            status["strategy_name"] = last.get("run_name", f"Last {mode.title()} Run")
            status["symbol"] = last.get("symbol", "")
            status["closed_trades"] = len(trades)
            status["trades_today"] = len(trades)
            status["total_pnl"] = last.get("total_pnl", 0)
            status["recent_trades"] = trades[-10:]
            status["_from_history"] = True
    except Exception:
        pass
    return status


@app.get("/api/paper/status")
async def paper_status(run_id: str = ""):
    if run_id and run_id in paper_engines:
        return paper_engines[run_id].get_status()
    if run_id:
        stopped = _stopped_engines.get(run_id)
        if stopped and stopped.get("mode") == "paper":
            return {**stopped, "running": False, "run_id": run_id, "mode": "paper"}
        return _empty_engine_status("paper", run_id=run_id)
    for rid, engine in paper_engines.items():
        if engine.running:
            return engine.get_status()
    return _history_engine_status("paper")


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
    """Save a completed paper/live trading run to runs.json for history.

    NOTE: Individual trades are now saved in real-time via _save_trade_to_history()
    in the broadcast callback. This function is kept as a fallback for edge cases
    (e.g. shutdown hook) where trades may not have been saved individually.
    It skips saving if the trade count matches already-saved individual trades.
    """
    try:
        closed = status.get("recent_trades", [])
        if not closed:
            closed = status.get("closed_trades", [])
            if isinstance(closed, int):
                closed = []
        if not closed:
            _logger.info("[%s] No trades to save — skipping", mode.upper())
            return

        run_name = status.get("strategy_name", status.get("run_name", ""))
        runs = _load_runs()
        closed_signatures = {_trade_signature(trade) for trade in closed if isinstance(trade, dict)}
        if not closed_signatures:
            _logger.info("[%s] No valid trade signatures to save — skipping", mode.upper())
            return

        single_trade_signatures = set()
        for run in runs:
            if run.get("mode") != mode or int(run.get("trade_count", 0) or 0) != 1:
                continue
            single_trade_signatures.update(_run_trade_signatures(run))
        if closed_signatures.issubset(single_trade_signatures):
            _logger.info("[%s] All %d trades already saved individually — skipping", mode.upper(), len(closed))
            return

        for run in runs:
            if run.get("mode") != mode:
                continue
            run_signatures = _run_trade_signatures(run)
            if (
                run.get("run_name") == run_name
                and int(run.get("trade_count", 0) or 0) == len(closed_signatures)
                and run_signatures
                and run_signatures == closed_signatures
            ):
                _logger.info("[%s] Matching aggregate run already saved for %s — skipping", mode.upper(), run_name)
                return

        max_id = max([r.get("id", 0) for r in runs], default=0)

        ordered_closed = []
        seen_closed = set()
        for trade in closed:
            sig = _trade_signature(trade)
            if sig in seen_closed:
                continue
            seen_closed.add(sig)
            ordered_closed.append(trade)

        if not ordered_closed:
            _logger.info("[%s] No unique trades left to save — skipping", mode.upper())
            return

        total_pnl = round(sum(t.get("pnl", 0) for t in ordered_closed), 2)
        winners = [t for t in ordered_closed if t.get("pnl", 0) > 0]
        losers = [t for t in ordered_closed if t.get("pnl", 0) <= 0]
        win_rate = round(len(winners) / len(ordered_closed) * 100, 2) if ordered_closed else 0

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
            "trade_count": len(ordered_closed),
            "total_pnl": total_pnl,
            "stats": {
                "total_trades": len(ordered_closed),
                "winning_trades": len(winners),
                "losing_trades": len(losers),
                "win_rate": win_rate,
                "total_pnl": total_pnl,
                "avg_profit": round(sum(t["pnl"] for t in winners) / len(winners), 2) if winners else 0,
                "avg_loss": round(sum(t["pnl"] for t in losers) / len(losers), 2) if losers else 0,
            },
            "trades": ordered_closed,
            "created_at": str(datetime.now()),
        }

        runs.append(run_entry)
        _save_runs(runs)
        _logger.info("[%s] Saved run #%s: %d trades, P&L=$%s", mode.upper(), run_entry["id"], len(closed), total_pnl)
    except Exception as e:
        _logger.error("[%s] Failed to save run to history: %s", mode.upper(), e)


def _save_trade_to_history(trade: dict, mode: str, run_name: str = "") -> None:
    """Save a single closed trade (paper/live/scalp) as a run entry in runs.json."""
    try:
        pnl = round(trade.get("pnl", 0), 2)
        runs = _load_runs()
        signature = _trade_signature(trade)
        for run in runs:
            if run.get("mode") == mode and signature in _run_trade_signatures(run):
                _logger.info("[%s] Trade already present in history — skipping duplicate save", mode.upper())
                return
        max_id = max((r.get("id", 0) for r in runs), default=0)
        symbol = trade.get("symbol", "")
        side = trade.get("side", trade.get("trade_side", ""))
        label = mode.title()
        name = run_name or f"{label} {symbol} {side}"
        run_entry = {
            "id": max_id + 1,
            "mode": mode,
            "run_name": name,
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
        _logger.info("[%s] Saved trade: %s %s P&L=$%s", mode.upper(), symbol, side, pnl)
    except Exception as e:
        _logger.error("[%s] Failed to save trade to history: %s", mode.upper(), e)


def _save_scalp_trade_to_history(trade: dict) -> None:
    """Save a single closed scalp trade as a run entry in runs.json."""
    try:
        pnl = round(trade.get("pnl", 0), 2)
        runs = _load_runs()
        signature = _trade_signature(trade)
        for run in runs:
            if run.get("mode") == "scalp" and signature in _run_trade_signatures(run):
                _logger.info("[SCALP] Trade already present in history — skipping duplicate save")
                return
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
        _logger.info("[SCALP] Saved trade #%s: P&L=$%s", trade.get("trade_id"), pnl)
    except Exception as e:
        _logger.error("[SCALP] Failed to save trade to history: %s", e)


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
        seen_trade_signatures = set()
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
                    sig = _trade_signature(t)
                    if sig in seen_trade_signatures:
                        continue
                    seen_trade_signatures.add(sig)
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

        for data in daily.values():
            data["pnl"] = round(data["real_pnl"] + data["paper_pnl"], 2)
            data["trades"] = int(data["real_trades"] + data["paper_trades"])
            data["wins"] = int(data["real_wins"] + data["paper_wins"])

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
            m["pnl"] = m["total_pnl"]
        for y_val in yearly.values():
            for k in ["real_pnl", "real_net_pnl", "paper_pnl", "total_pnl"]:
                y_val[k] = round(y_val[k], 2)
            y_val["pnl"] = y_val["total_pnl"]

        return {"status": "success", "daily": daily, "monthly": monthly, "yearly": yearly}
    except Exception as e:
        _logger.error("Portfolio history error: %s", e)
        return {"status": "error", "message": str(e), "daily": {}, "monthly": {}, "yearly": {}}


# ── Strategy CRUD ─────────────────────────────────────────────────
_LEGACY_STRAT_FILE, STRAT_FILE = _resolve_state_file("strategies.json")
_LEGACY_RUNS_FILE, RUNS_FILE = _resolve_state_file("runs.json")


def _load():
    try:
        store = _seed_list_bucket(
            _BUCKET_STRATEGIES,
            STRAT_FILE,
            _LEGACY_STRAT_FILE,
            key_fn=lambda row, idx: str(int((row or {}).get("id", 0) or idx + 1)),
        )
        records = list(store.list(_BUCKET_STRATEGIES, order_by="doc_key"))
        return sorted(records, key=lambda row: int((row or {}).get("id", 0) or 0))
    except Exception as e:
        _logger.warning("Failed to load strategies from %s: %s", _current_state_db_file(), e)
        return []


def _save(d):
    store = _get_state_store()
    store.replace_list(
        _BUCKET_STRATEGIES,
        list(d or []),
        key_fn=lambda row, idx: str(int((row or {}).get("id", 0) or idx + 1)),
    )


def _load_runs():
    try:
        store = _seed_list_bucket(
            _BUCKET_RUNS,
            RUNS_FILE,
            _LEGACY_RUNS_FILE,
            key_fn=lambda row, idx: str(int((row or {}).get("id", 0) or idx + 1)),
        )
        records = list(store.list(_BUCKET_RUNS, order_by="doc_key"))
        return sorted(records, key=lambda row: int((row or {}).get("id", 0) or 0))
    except Exception as e:
        _logger.warning("Failed to load runs from %s: %s", _current_state_db_file(), e)
        return []


def _save_runs(d):
    store = _get_state_store()
    store.replace_list(
        _BUCKET_RUNS,
        list(d or []),
        key_fn=lambda row, idx: str(int((row or {}).get("id", 0) or idx + 1)),
    )


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


# ── Cache Management ──────────────────────────────────────────────
@app.get("/api/cache/status")
async def cache_status():
    """Show cached candle files and sizes."""
    files = []
    if os.path.exists(_CACHE_DIR):
        for f in os.listdir(_CACHE_DIR):
            path = os.path.join(_CACHE_DIR, f)
            size_mb = os.path.getsize(path) / 1024 / 1024
            files.append({"file": f, "size_mb": round(size_mb, 2)})
    return {"cache_dir": _CACHE_DIR, "files": files}


@app.delete("/api/cache")
async def clear_cache():
    """Clear all cached candle data."""
    cleared = 0
    if os.path.exists(_CACHE_DIR):
        for f in os.listdir(_CACHE_DIR):
            os.remove(os.path.join(_CACHE_DIR, f))
            cleared += 1
    return {"cleared": cleared}


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

    runtime = _normalize_strategy_runtime(
        indicators=body.get("indicators", []),
        entry_conditions=body.get("entry_conditions", []),
        exit_conditions=body.get("exit_conditions", []),
        candle_interval=body.get("candle_interval", "5m"),
    )

    entry = runtime["entry_conditions"]
    exit_conds = runtime["exit_conditions"]
    errors.extend(runtime["errors"])
    warnings.extend(runtime["warnings"])
    if runtime["added_indicators"]:
        warnings.append("Auto-added indicator dependencies: " + ", ".join(runtime["added_indicators"]))
    if runtime["unsupported_fields"]:
        errors.append("Unsupported condition fields: " + ", ".join(runtime["unsupported_fields"]))
    if runtime["unresolved_fields"]:
        errors.append("Missing indicator coverage for fields: " + ", ".join(runtime["unresolved_fields"]))

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
    seen_contradictions = set()
    for c in entry:
        lhs = c.get("left", "")
        op = c.get("operator", "")
        rhs = c.get("right", "")
        for c2 in entry:
            if c2 is c:
                continue
            if c2.get("left") == lhs and c2.get("right") == rhs:
                if op in ("is_above", "crosses_above") and c2.get("operator") in ("is_below", "crosses_below"):
                    key = (lhs, rhs)
                    if key not in seen_contradictions:
                        errors.append(f"Contradictory: {lhs} both above and below {rhs}")
                        seen_contradictions.add(key)

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
_LEGACY_SCALP_FILE, _SCALP_FILE = _resolve_state_file("scalp_trades.json", "scalp")
_LEGACY_SCALP_EVENTS_FILE, _SCALP_EVENTS_FILE = _resolve_state_file("scalp_events.json", "scalp")
_LEGACY_SCALP_RUNTIME_FILE, _SCALP_RUNTIME_FILE = _resolve_state_file("scalp_runtime.json", "scalp")


def _load_scalp_trades():
    store = _seed_list_bucket(
        _BUCKET_SCALP_TRADES,
        _SCALP_FILE,
        _LEGACY_SCALP_FILE,
        key_fn=lambda row, idx: "|".join(
            [
                str((row or {}).get("trade_id") or idx + 1),
                str((row or {}).get("entry_time") or ""),
                str((row or {}).get("exit_time") or ""),
            ]
        ),
    )
    trades = list(store.list(_BUCKET_SCALP_TRADES, order_by="updated_at"))
    return sorted(
        trades,
        key=lambda row: (
            _normalize_datetime((row or {}).get("exit_time") or (row or {}).get("entry_time")) or datetime.min
        ),
    )


def _save_scalp_trades(trades):
    store = _get_state_store()
    store.replace_list(
        _BUCKET_SCALP_TRADES,
        list(trades or []),
        key_fn=lambda row, idx: "|".join(
            [
                str((row or {}).get("trade_id") or idx + 1),
                str((row or {}).get("entry_time") or ""),
                str((row or {}).get("exit_time") or ""),
            ]
        ),
    )


def _load_scalp_events():
    store = _seed_list_bucket(
        _BUCKET_SCALP_EVENTS,
        _SCALP_EVENTS_FILE,
        _LEGACY_SCALP_EVENTS_FILE,
        key_fn=lambda row, idx: "|".join(
            [
                str((row or {}).get("ts") or idx),
                str((row or {}).get("level") or ""),
                str((row or {}).get("msg") or ""),
            ]
        ),
    )
    events = list(store.list(_BUCKET_SCALP_EVENTS, order_by="updated_at"))
    return sorted(events, key=lambda row: str((row or {}).get("ts") or ""))


def _save_scalp_events(events):
    trimmed = list(events or [])[-300:]
    store = _get_state_store()
    store.replace_list(
        _BUCKET_SCALP_EVENTS,
        trimmed,
        key_fn=lambda row, idx: "|".join(
            [
                str((row or {}).get("ts") or idx),
                str((row or {}).get("level") or ""),
                str((row or {}).get("msg") or ""),
            ]
        ),
    )


def _load_scalp_runtime() -> dict:
    store = _seed_singleton_bucket(
        _BUCKET_SCALP_RUNTIME,
        "current",
        _SCALP_RUNTIME_FILE,
        _LEGACY_SCALP_RUNTIME_FILE,
    )
    data = store.get(_BUCKET_SCALP_RUNTIME, "current", default={})
    return data if isinstance(data, dict) else {}


def _save_scalp_runtime(runtime_state: dict) -> None:
    store = _get_state_store()
    store.put(_BUCKET_SCALP_RUNTIME, "current", dict(runtime_state or {}))


def _restore_scalp_runtime(engine: ScalpEngine) -> bool:
    runtime_state = _load_scalp_runtime()
    open_rows = runtime_state.get("open_trades") or []
    pending_rows = runtime_state.get("pending_entries") or []
    if not open_rows and not pending_rows:
        return False

    restored_open = {}
    for row in open_rows:
        try:
            trade = ScalpTrade.from_dict(row)
            restored_open[int(trade.trade_id)] = trade
        except Exception as exc:
            _logger.warning("[SCALP] Failed to restore open trade %s: %s", row.get("trade_id"), exc)
    restored_pending = {}
    for row in pending_rows:
        try:
            pending = PendingScalpEntry.from_dict(row)
            restored_pending[int(pending.entry_id)] = pending
        except Exception as exc:
            _logger.warning("[SCALP] Failed to restore pending entry %s: %s", row.get("entry_id"), exc)

    engine.open_trades = restored_open
    engine.pending_entries = restored_pending
    engine.closed_trades = list(runtime_state.get("closed_trades") or [])[-50:]
    engine._last_execution = dict(runtime_state.get("execution_metrics") or {})
    if not engine._last_execution:
        restored_rows = list(engine.open_trades.values()) + list(engine.pending_entries.values())
        for row in restored_rows:
            metrics = dict(getattr(row, "execution_metrics", {}) or {})
            if metrics:
                engine._last_execution = metrics
                break
    event_log = runtime_state.get("event_log") or []
    if event_log:
        engine.event_log = list(event_log)[-100:]
    for trade in engine.open_trades.values():
        if trade.current_price > 0:
            engine._record_price(trade.symbol, trade.current_price, source=trade.last_price_source or "restored")
    tracked = {engine._canonical_symbol(t.symbol) for t in engine.open_trades.values()} | {
        engine._canonical_symbol(p.symbol) for p in engine.pending_entries.values()
    }
    engine._watch_symbols = {sym for sym in tracked if sym}
    if (engine.open_trades or engine.pending_entries) and not getattr(engine, "_running", False):
        engine.start()
    return bool(engine.open_trades or engine.pending_entries)


def _snapshot_scalp_runtime(status: dict) -> dict:
    return {
        "saved_at": str(datetime.now()),
        "open_trades": list(status.get("open_trades") or []),
        "pending_entries": list(status.get("pending_entries") or []),
        "event_log": list(status.get("event_log") or [])[-80:],
        "execution_metrics": dict(status.get("execution_metrics") or {}),
    }


def _persist_scalp_runtime_snapshot(engine: ScalpEngine, symbol_hint: str = "") -> None:
    try:
        _save_scalp_runtime(_snapshot_scalp_runtime(engine.get_status(symbol_hint)))
    except Exception as exc:
        _logger.error("[SCALP] Failed to persist runtime snapshot: %s", exc)


def _attach_scalp_runtime_metrics(result: dict, engine: ScalpEngine, symbol_hint: str = "") -> dict:
    if not isinstance(result, dict):
        return result
    status = engine.get_status(symbol_hint)
    result.setdefault("running", bool(status.get("running")))
    result.setdefault("in_trade", bool(status.get("in_trade")))
    if "session_pnl" in status:
        result.setdefault("session_pnl", status.get("session_pnl"))
    result.setdefault("open_trades", list(status.get("open_trades") or []))
    result.setdefault("pending_entries", list(status.get("pending_entries") or []))
    result.setdefault("closed_trades", list(status.get("closed_trades") or [])[-50:])
    result.setdefault("event_log", list(status.get("event_log") or [])[-100:])
    result.setdefault("execution_metrics", dict(status.get("execution_metrics") or {}))
    result.setdefault("feed_metrics", dict(status.get("feed_metrics") or {}))
    result.setdefault("entry_controls", dict(status.get("entry_controls") or {}))
    return result


def _scalp_action_status_code(result: dict, *, default_status: int = 400) -> int:
    error_code = str((result or {}).get("error_code") or "").strip().lower()
    if not error_code:
        message = str((result or {}).get("message") or "Unknown scalp action error")
        lowered = message.lower()
        if "not found" in lowered or "already closed" in lowered:
            error_code = "trade_not_found"
        elif "action in progress" in lowered:
            error_code = "action_in_progress"
        elif "no live price available" in lowered:
            error_code = "no_live_price"
        elif "not confirmed" in lowered:
            error_code = "exit_not_confirmed"
        elif "must be greater than zero" in lowered:
            error_code = "invalid_quantity"
        elif "no target fields provided" in lowered:
            error_code = "no_target_fields"
        elif "unable to resolve add quantity" in lowered:
            error_code = "quantity_resolution_failed"
        elif "could not be verified" in lowered or "rejected" in lowered:
            error_code = "broker_rejected"
        elif "broker does not support" in lowered:
            error_code = "unsupported_broker"
        elif lowered:
            error_code = "unexpected_error"
    if error_code == "trade_not_found":
        return 404
    if error_code in {"action_in_progress", "no_live_price", "exit_not_confirmed"}:
        return 409
    if error_code in {"invalid_quantity", "no_target_fields"}:
        return 400
    if error_code == "quantity_resolution_failed":
        return 422
    if error_code in {"broker_rejected", "broker_error", "unsupported_broker"}:
        return 502
    return default_status


def _scalp_action_error_response(
    result: dict, engine: ScalpEngine, symbol_hint: str = "", *, default_status: int = 400
) -> JSONResponse:
    payload = _attach_scalp_runtime_metrics(dict(result or {}), engine, symbol_hint)
    message = str(payload.get("message") or "Unknown scalp action error")
    error_code = str(payload.get("error_code") or "").strip().lower()
    if not error_code:
        provisional_status = _scalp_action_status_code(payload, default_status=default_status)
        error_code_map = {
            404: "trade_not_found",
            409: "action_in_progress",
            422: "quantity_resolution_failed",
            502: "broker_rejected",
        }
        error_code = error_code_map.get(provisional_status, "unexpected_error")
        payload["error_code"] = error_code
    retryable = bool(payload.get("retryable"))
    if "retryable" not in payload:
        retryable = error_code in {"action_in_progress", "no_live_price", "exit_not_confirmed", "broker_error"}
        payload["retryable"] = retryable
    payload["status"] = "error"
    payload["message"] = message
    payload["error"] = {"detail": message, "code": error_code, "retryable": retryable}
    return JSONResponse(payload, status_code=_scalp_action_status_code(payload, default_status=default_status))


def _scalp_persist_event(event: dict) -> None:
    try:
        events = _load_scalp_events()
        key = (event.get("ts"), event.get("level"), event.get("msg"))
        if not any((e.get("ts"), e.get("level"), e.get("msg")) == key for e in events):
            events.append(event)
            _save_scalp_events(events)
    except Exception as e:
        _logger.error("[SCALP] Failed to persist event: %s", e)


def _scalp_persist_trade(trade: dict) -> None:
    """Persist a single closed scalp trade to disk (auto + manual exits)."""
    try:
        trades = _load_scalp_trades()
        # Deduplicate by trade_id + entry_time (trade_id can repeat across restarts)
        key = (trade.get("trade_id"), trade.get("entry_time"))
        if not any((t.get("trade_id"), t.get("entry_time")) == key for t in trades):
            trades.append(trade)
            _save_scalp_trades(trades)
        _save_scalp_trade_to_history(trade)
    except Exception as e:
        _logger.error("[SCALP] Failed to persist trade: %s", e)

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


async def _broadcast_scalp_update(status: dict) -> None:
    try:
        _save_scalp_runtime(_snapshot_scalp_runtime(status))
    except Exception as exc:
        _logger.error("[SCALP] Failed to persist runtime state: %s", exc)
    payload = {"source": "scalp", "type": "scalp_status", "status": status}
    for ws in ws_clients.copy():
        try:
            await ws.send_json(payload)
        except Exception:
            if ws in ws_clients:
                ws_clients.remove(ws)


def _get_scalp_engine():
    global _scalp_engine
    if _scalp_engine is None:
        _scalp_engine = ScalpEngine(
            delta,
            on_trade_closed=_scalp_persist_trade,
            on_event=_scalp_persist_event,
            on_update=_broadcast_scalp_update,
        )
        _restore_scalp_runtime(_scalp_engine)
    return _scalp_engine


@app.get("/api/scalp/status")
async def scalp_status(symbol: str = "", include_activity: bool = False):
    eng = _get_scalp_engine()
    if symbol:
        symbol = _normalize_scalp_symbol(symbol, allow_blank=True)
        eng.watch_symbol(symbol)
    if not eng.open_trades and not eng.pending_entries:
        _restore_scalp_runtime(eng)
    if any(str(getattr(trade, "mode", "paper")).lower() != "paper" for trade in eng.open_trades.values()):
        try:
            await eng.reconcile_broker_positions(force=False)
        except Exception as exc:
            _logger.warning("[SCALP] Reconcile during status refresh failed: %s", exc)
    status = eng.get_status(symbol)
    if include_activity:
        status["file_trades"] = list(reversed(_load_scalp_trades()[-100:]))
        status["file_events"] = list(reversed(_load_scalp_events()[-200:]))
    return status


@app.get("/api/scalp/trades")
async def scalp_trades():
    return _load_scalp_trades()


@app.get("/api/scalp/activity")
async def scalp_activity():
    eng = _get_scalp_engine()
    stored_trades = list(reversed(_load_scalp_trades()[-100:]))
    stored_events = list(reversed(_load_scalp_events()[-200:]))
    return {
        "closed_trades": list(reversed(eng.closed_trades[-50:])),
        "event_log": list(reversed(eng.event_log[-100:])),
        "stored_trades": stored_trades,
        "stored_events": stored_events,
        "file_trades": stored_trades,
        "file_events": stored_events,
    }


@app.post("/api/scalp/enter")
async def scalp_enter(request: Request):
    check_rate_limit(
        "scalp_enter", max_calls=6, window_sec=10, client_ip=request.client.host if request.client else "global"
    )
    body = await _read_json_body(request)
    eng = _get_scalp_engine()
    symbol = _normalize_scalp_symbol(_body_value(body, "symbol", default="BTCUSDT"))
    eng.watch_symbol(symbol)
    raw_side = str(_body_value(body, "side", default="BUY") or "BUY").upper()
    if raw_side not in {"BUY", "SELL", "LONG", "SHORT"}:
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")
    side = "LONG" if raw_side in {"BUY", "LONG"} else "SHORT"
    leverage = _parse_int_field(body, "leverage", default=50, min_value=1)
    mode = _parse_scalp_mode(body, "mode", default="paper")
    qty_mode = _parse_scalp_qty_mode(body, "qty_mode", default="usdt")
    default_qty = 0.0015 if qty_mode == "base" else 1000.0
    qty_value = _parse_float_field(body, "qty_value", "qty_base", "qty_usdt", default=default_qty, min_value=0.0)
    entry_stop_price = _parse_float_field(body, "entry_stop_price", "guardrail_price", default=0.0, min_value=0.0)
    entry_limit_price = _parse_float_field(body, "entry_limit_price", default=0.0, min_value=0.0)
    target_price = _parse_float_field(body, "target_price", "tp_price", default=0.0, min_value=0.0)
    sl_price = _parse_float_field(body, "sl_price", default=0.0, min_value=0.0)
    legacy_size = _parse_int_field(body, "size", default=0, min_value=0)
    target_pct = _parse_float_field(body, "take_profit_pct", default=0.0, min_value=0.0)
    sl_pct = _parse_float_field(body, "stop_loss_pct", default=0.0, min_value=0.0)
    target_usd = _parse_float_field(body, "tp_usd", default=0.0, min_value=0.0)
    sl_usd = _parse_float_field(body, "sl_usd", default=0.0, min_value=0.0)
    if qty_value <= 0:
        raise HTTPException(status_code=400, detail="qty_value must be greater than zero")
    if legacy_size <= 0 and qty_mode != "base" and qty_value > 0:
        legacy_size = int(qty_value * leverage)

    entry_controls = eng.get_status(symbol).get("entry_controls", {})
    pending_requested = entry_stop_price > 0 or entry_limit_price > 0
    if not pending_requested:
        allowed = entry_controls.get("paper_allowed") if mode == "paper" else entry_controls.get("live_allowed")
        if not allowed:
            message = entry_controls.get("reason") or "Awaiting a fresh market price before entry."
            alerter.alert(
                "Scalp Entry Blocked",
                f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nReason: {message}",
                level="warn",
            )
            return _scalp_action_error_response(
                {"status": "error", "message": message, "entry_controls": entry_controls},
                eng,
                symbol,
                default_status=409,
            )

    try:
        result = await eng.enter_trade(
            symbol=symbol,
            side=side,
            size=legacy_size,
            leverage=leverage,
            qty_mode=qty_mode,
            qty_value=qty_value,
            target_price=target_price,
            sl_price=sl_price,
            target_pct=target_pct,
            sl_pct=sl_pct,
            target_usd=target_usd,
            sl_usd=sl_usd,
            guardrail_price=entry_stop_price,
            entry_limit_price=entry_limit_price,
            entry_stop_price=entry_stop_price,
            mode=mode,
        )
        if result.get("status") == "error":
            alerter.alert(
                "Scalp Entry Failed",
                f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nError: {result.get('message', 'unknown')}",
            )
        elif result.get("status") == "ok":
            trade = result.get("trade", {})
            qty_text = (
                f"{trade.get('base_qty', 0):,.6f} qty"
                if trade.get("qty_mode") == "base"
                else f"${trade.get('qty_usdt', 0):,.2f} margin"
            )
            price = trade.get("entry_price", 0)
            alerter.alert(
                "Scalp Entry",
                f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nExposure: {qty_text}\nLeverage: {leverage}x\nEntry: ${price:,.2f}",
                level="info",
            )
        elif result.get("status") == "pending":
            pending = result.get("pending_entry", {})
            trigger = pending.get("trigger_summary") or "Pending price trigger"
            qty_text = (
                f"{pending.get('base_qty', 0):,.6f} qty"
                if pending.get("qty_mode") == "base"
                else f"${pending.get('qty_usdt', 0):,.2f} margin"
            )
            alerter.alert(
                "Scalp Entry Armed",
                f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nExposure: {qty_text}\nLeverage: {leverage}x\nTrigger: {trigger}",
                level="info",
            )
        _persist_scalp_runtime_snapshot(eng, symbol)
        if result.get("status") == "error":
            return _scalp_action_error_response(result, eng, symbol)
        return _attach_scalp_runtime_metrics(result, eng, symbol)
    except Exception as e:
        alerter.alert("Scalp Entry Error", f"Symbol: {symbol}\nSide: {side}\nMode: {mode}\nError: {e}")
        return _scalp_action_error_response(
            {"status": "error", "message": "Unexpected scalp entry error"}, eng, symbol, default_status=500
        )


@app.post("/api/scalp/exit")
async def scalp_exit(request: Request):
    check_rate_limit(
        "scalp_exit", max_calls=8, window_sec=10, client_ip=request.client.host if request.client else "global"
    )
    body = await _read_json_body(request)
    trade_id = _parse_int_field(body, "trade_id", default=0, min_value=1)
    eng = _get_scalp_engine()
    try:
        result = await eng.exit_trade(trade_id, reason="manual")
        _persist_scalp_runtime_snapshot(eng)
        # Persistence is handled by the engine's on_trade_closed callback.
        if result.get("status") == "ok" and result.get("trade"):
            t = result["trade"]
            pnl = round(t.get("pnl", 0), 2)
            alerter.alert(
                "Scalp Exit",
                f"Symbol: {t.get('symbol', '—')}\nSide: {t.get('side', '')}\nP&L: ${pnl:,.2f}\nReason: manual\nEntry: ${t.get('entry_price', 0):,.2f}\nExit: ${t.get('exit_price', 0):,.2f}",
                level="info" if pnl >= 0 else "warn",
            )
            return _attach_scalp_runtime_metrics(result, eng, t.get("symbol", ""))
        if result.get("status") == "error":
            alerter.alert("Scalp Exit Failed", f"Trade ID: {trade_id}\nError: {result.get('message', 'unknown')}")
            symbol_hint = (result.get("trade") or {}).get("symbol", "")
            return _scalp_action_error_response(result, eng, symbol_hint)
        return _attach_scalp_runtime_metrics(result, eng)
    except HTTPException:
        raise
    except Exception as e:
        alerter.alert("Scalp Exit Error", f"Trade ID: {trade_id}\nError: {e}")
        return _scalp_action_error_response(
            {"status": "error", "message": "Unexpected scalp exit error"}, eng, default_status=500
        )


@app.put("/api/scalp/trades/{trade_id}/targets")
async def update_scalp_targets(trade_id: int, request: Request):
    """Modify TP/SL for an active scalp trade."""
    check_rate_limit(
        "scalp_targets", max_calls=10, window_sec=15, client_ip=request.client.host if request.client else "global"
    )
    body = await _read_json_body(request)
    eng = _get_scalp_engine()
    kwargs = {}
    for key in ("target_price", "sl_price", "target_usd", "sl_usd"):
        if key in body and body[key] is not None:
            kwargs[key] = _parse_float_field(body, key, default=0.0, min_value=0.0)
    if not kwargs:
        raise HTTPException(status_code=400, detail="No target fields provided")
    try:
        result = await eng.update_trade_targets(trade_id, **kwargs)
        _persist_scalp_runtime_snapshot(eng)
        symbol_hint = (result.get("trade") or {}).get("symbol", "")
        if result.get("status") == "error":
            return _scalp_action_error_response(result, eng, symbol_hint)
        return _attach_scalp_runtime_metrics(result, eng, symbol_hint)
    except Exception as e:
        alerter.alert("Scalp Targets Error", f"Trade ID: {trade_id}\nError: {e}")
        return _scalp_action_error_response(
            {"status": "error", "message": "Unexpected scalp target update error"}, eng, default_status=500
        )


@app.post("/api/scalp/trades/{trade_id}/add")
async def add_scalp_quantity(trade_id: int, request: Request):
    check_rate_limit(
        "scalp_add", max_calls=8, window_sec=15, client_ip=request.client.host if request.client else "global"
    )
    body = await _read_json_body(request)
    qty_mode = _parse_scalp_qty_mode(body, "qty_mode", default="base")
    qty_value = _parse_float_field(body, "qty_value", "qty_base", "qty_usdt", default=0.0, min_value=0.0)
    if qty_value <= 0:
        raise HTTPException(status_code=400, detail="qty_value must be greater than zero")
    eng = _get_scalp_engine()
    try:
        result = await eng.add_to_trade(trade_id, qty_mode=qty_mode, qty_value=qty_value)
        _persist_scalp_runtime_snapshot(eng)
        trade = result.get("trade", {})
        if result.get("status") == "error":
            return _scalp_action_error_response(result, eng, trade.get("symbol", ""))
        added_text = f"{qty_value:,.6f} qty" if qty_mode == "base" else f"${qty_value:,.2f} margin"
        exposure_text = (
            f"{trade.get('base_qty', 0):,.6f} qty • ${trade.get('qty_usdt', 0):,.2f} margin"
            if trade.get("qty_mode") == "base"
            else f"${trade.get('qty_usdt', 0):,.2f} margin • {trade.get('base_qty', 0):,.6f} qty"
        )
        alerter.alert(
            "Scalp Add Quantity",
            f"Trade ID: {trade_id}\nSymbol: {trade.get('symbol', '—')}\nMode: {trade.get('mode', 'paper')}\nAdded: {added_text}\nNew exposure: {exposure_text}",
            level="info",
        )
        return _attach_scalp_runtime_metrics(result, eng, trade.get("symbol", ""))
    except Exception as e:
        alerter.alert("Scalp Add Error", f"Trade ID: {trade_id}\nError: {e}")
        return _scalp_action_error_response(
            {"status": "error", "message": "Unexpected scalp add error"}, eng, default_status=500
        )


# ── WebSocket ─────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # Authenticate WebSocket via session cookie
    token = ws.cookies.get("cryptoforge_session", "")
    if not _validate_session(token, request=ws):
        await ws.close(code=4001, reason="Unauthorized")
        return
    await ws.accept()
    ws_clients.append(ws)
    try:
        while True:
            paper_sts = {rid: e.get_status() for rid, e in paper_engines.items()}
            live_sts = {rid: e.get_status() for rid, e in live_engines.items()}
            try:
                await asyncio.wait_for(
                    ws.send_json(
                        {
                            "type": "status",
                            "paper_engines": paper_sts,
                            "live_engines": live_sts,
                            "paper_running": any(s.get("running") for s in paper_sts.values()),
                            "live_running": any(s.get("running") for s in live_sts.values()),
                        }
                    ),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                _logger.warning("WebSocket send timed out, closing connection")
                break
            await asyncio.sleep(5)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)


# ── Run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    _logger.info("CryptoForge starting — http://%s:%s", config.APP_HOST, config.APP_PORT)
    uvicorn.run("app:app", host=config.APP_HOST, port=config.APP_PORT, reload=False, log_level="info")

_ORIG_SNAPSHOT_SCALP_RUNTIME = _snapshot_scalp_runtime


def _snapshot_scalp_runtime(*args, **kwargs):
    snapshot = _ORIG_SNAPSHOT_SCALP_RUNTIME(*args, **kwargs)
    source = args[0] if args else kwargs.get("engine")
    if isinstance(snapshot, dict) and source is not None:
        try:
            if isinstance(source, dict):
                closed_rows = list(source.get("closed_trades") or [])
            else:
                status = source.get_status() if hasattr(source, "get_status") else {}
                closed_rows = list((status or {}).get("closed_trades") or getattr(source, "closed_trades", []) or [])
            snapshot["closed_trades"] = closed_rows[-50:]
        except Exception:
            snapshot.setdefault("closed_trades", [])
    return snapshot


@app.post("/api/scalp/reconcile")
async def scalp_reconcile():
    eng = _get_scalp_engine()
    symbol_hint = ""
    try:
        if getattr(eng, "open_trades", None):
            first_trade = next(iter(eng.open_trades.values()), None)
            symbol_hint = str(getattr(first_trade, "symbol", "") or "")
        if not symbol_hint and getattr(eng, "_watch_symbols", None):
            symbol_hint = str(next(iter(eng._watch_symbols), "") or "")
        result = await eng.reconcile_broker_positions(force=True)
        if isinstance(result, dict) and result.get("status") == "error":
            return _scalp_action_error_response(result, eng, symbol_hint, default_status=502)
        if "_persist_scalp_runtime_snapshot" in globals():
            _persist_scalp_runtime_snapshot(eng)
        if "_attach_scalp_runtime_metrics" in globals():
            return _attach_scalp_runtime_metrics({"status": "ok", "reconciliation": result}, eng, symbol_hint)
        return {"status": "ok", "reconciliation": result}
    except Exception as exc:
        _logger.warning("[SCALP] Reconcile route failed: %s", exc)
        return _scalp_action_error_response(
            {
                "status": "error",
                "action": "reconcile",
                "message": f"Broker sync failed: {exc}",
                "error_code": "broker_error",
                "retryable": True,
            },
            eng,
            symbol_hint,
            default_status=502,
        )
