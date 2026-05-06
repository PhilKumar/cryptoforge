"""
app.py — CryptoForge FastAPI Backend
Perpetual futures algo-trading platform powered by a configurable broker.
Production-ready: multi-engine, market feed, portfolio history, full CRUD.
"""

import asyncio
import hashlib
import inspect
import json
import logging
import os
import re
import secrets
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib import error as urllib_error
from urllib.parse import urlparse
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

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

from dotenv import dotenv_values, load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import alerter
import config  # must be first — calls load_dotenv()
from broker import get_broker_client, get_supported_brokers
from broker.delta import get_candles_binance
from engine.backtest import run_backtest
from engine.live import LiveEngine
from engine.paper_trading import PaperTradingEngine
from engine.scalp import PendingScalpEntry, ScalpEngine, ScalpTrade, normalize_scalp_order_type
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

# Initialize broker client
broker = get_broker_client()
# Backwards-compatible alias retained while the internal rename is staged.
delta = broker
APP_BOOT_TS = time.time()


def _broker_label() -> str:
    return str(getattr(delta, "display_name", "Broker") or "Broker")


_PORTFOLIO_USD_INR_CACHE: dict = {}
_DELTA_INDIA_USD_INR_RATE = 85.0
_DELTA_INDIA_USD_INR_SOURCE = "https://guides.delta.exchange/delta-exchange-india-user-guide/account-setup/usd-inr-rate"


def _portfolio_is_delta_india() -> bool:
    return _active_broker_name(delta) == "delta" and bool(
        getattr(delta, "_is_india", getattr(config, "DELTA_REGION", "india").lower() == "india")
    )


def _portfolio_delta_india_usd_inr_rate() -> float:
    raw = os.getenv("DELTA_INDIA_USD_INR_RATE") or os.getenv("CRYPTOFORGE_DELTA_INDIA_USD_INR_RATE")
    try:
        rate = float(raw) if raw not in (None, "") else _DELTA_INDIA_USD_INR_RATE
    except (TypeError, ValueError):
        rate = _DELTA_INDIA_USD_INR_RATE
    return round(rate if rate > 0 else _DELTA_INDIA_USD_INR_RATE, 4)


def _portfolio_fx_cache_ttl_sec() -> int:
    raw = os.getenv("CRYPTOFORGE_FX_CACHE_TTL_SEC", "1800")
    try:
        return max(60, int(float(raw)))
    except (TypeError, ValueError):
        return 1800


def _portfolio_fx_timeout_sec() -> float:
    raw = os.getenv("CRYPTOFORGE_FX_TIMEOUT_SEC", "2.5")
    try:
        return max(0.5, min(10.0, float(raw)))
    except (TypeError, ValueError):
        return 2.5


def _portfolio_fx_urls() -> list[str]:
    raw = (os.getenv("CRYPTOFORGE_USD_INR_RATE_URL") or "").strip()
    if raw:
        return [part.strip() for part in raw.split(",") if part.strip()]
    return [
        "https://open.er-api.com/v6/latest/USD",
        "https://api.frankfurter.dev/v2/rate/USD/INR",
    ]


def _parse_portfolio_fx_payload(payload: dict | list, url: str) -> tuple[float, str, str]:
    if isinstance(payload, dict):
        rates = payload.get("rates") if isinstance(payload.get("rates"), dict) else {}
        rate = _safe_float(rates.get("INR"), 0.0)
        if rate > 0:
            source = str(payload.get("provider") or urlparse(url).netloc or "fx_api")
            provider_date = str(payload.get("time_last_update_utc") or payload.get("date") or "")
            return rate, provider_date, source
        rate = _safe_float(payload.get("rate"), 0.0)
        if rate > 0:
            return rate, str(payload.get("date") or ""), urlparse(url).netloc or "fx_api"
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("base") or "").upper() != "USD" or str(item.get("quote") or "").upper() != "INR":
                continue
            rate = _safe_float(item.get("rate"), 0.0)
            if rate > 0:
                return rate, str(item.get("date") or ""), urlparse(url).netloc or "fx_api"
    raise ValueError("USD/INR rate missing from FX payload")


def _fetch_portfolio_usd_inr_rate() -> dict:
    errors = []
    for url in _portfolio_fx_urls():
        try:
            parsed_url = urlparse(url)
            if parsed_url.scheme != "https":
                raise ValueError("FX provider URL must use HTTPS")
            req = UrlRequest(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "CryptoForge/2.0 (+https://crypto.philforge.in)",
                },
            )
            with urlopen(req, timeout=_portfolio_fx_timeout_sec()) as response:  # nosec B310 - HTTPS-only URL.
                raw = response.read(32768)
            payload = json.loads(raw.decode("utf-8"))
            rate, provider_date, source = _parse_portfolio_fx_payload(payload, url)
            return {
                "rate": round(rate, 4),
                "source": source,
                "source_url": url,
                "provider_date": provider_date,
            }
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError, urllib_error.URLError) as exc:
            errors.append(f"{urlparse(url).netloc or url}: {str(exc)[:120]}")
    raise RuntimeError("; ".join(errors) or "No FX providers configured")


def _portfolio_usd_inr_rate() -> dict:
    now = time.time()
    cached = _PORTFOLIO_USD_INR_CACHE.get("meta")
    if cached and now < float(_PORTFOLIO_USD_INR_CACHE.get("expires_at", 0) or 0):
        return dict(cached)

    try:
        meta = _fetch_portfolio_usd_inr_rate()
        meta.update(
            {
                "live": True,
                "stale": False,
                "fallback": False,
                "error": "",
                "fetched_at": datetime.now().isoformat(timespec="seconds"),
                "ttl_sec": _portfolio_fx_cache_ttl_sec(),
            }
        )
        _PORTFOLIO_USD_INR_CACHE["meta"] = dict(meta)
        _PORTFOLIO_USD_INR_CACHE["expires_at"] = now + _portfolio_fx_cache_ttl_sec()
        return dict(meta)
    except (OSError, TimeoutError, RuntimeError, ValueError, json.JSONDecodeError, urllib_error.URLError) as exc:
        if cached:
            meta = dict(cached)
            last_rate = meta.get("rate")
            meta.update(
                {
                    "rate": 0.0,
                    "last_rate": last_rate,
                    "live": False,
                    "stale": True,
                    "fallback": False,
                    "error": str(exc)[:160],
                    "ttl_sec": _portfolio_fx_cache_ttl_sec(),
                }
            )
            _PORTFOLIO_USD_INR_CACHE["meta"] = dict(meta)
            _PORTFOLIO_USD_INR_CACHE["expires_at"] = now + min(300, _portfolio_fx_cache_ttl_sec())
            return meta
        return {
            "rate": 0.0,
            "last_rate": 0.0,
            "source": "unavailable",
            "source_url": ",".join(_portfolio_fx_urls()),
            "provider_date": "",
            "live": False,
            "stale": True,
            "fallback": False,
            "error": str(exc)[:160],
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "ttl_sec": _portfolio_fx_cache_ttl_sec(),
        }


def _portfolio_currency_meta() -> dict:
    if _portfolio_is_delta_india():
        fx = dict(_PORTFOLIO_USD_INR_CACHE.get("meta") or {})
        settlement_rate = _portfolio_delta_india_usd_inr_rate()
        return {
            "base": "USD",
            "settlement": "INR",
            "quote": "INR",
            "usd_inr_rate": settlement_rate,
            "rate_available": True,
            "rate_kind": "broker_settlement",
            "rate_label": "Delta settlement",
            "rate_source": "Delta Exchange India fixed settlement rate",
            "rate_source_url": _DELTA_INDIA_USD_INR_SOURCE,
            "rate_provider_date": "",
            "rate_fetched_at": datetime.now().isoformat(timespec="seconds"),
            "rate_live": True,
            "rate_stale": False,
            "rate_fallback": False,
            "rate_error": "",
            "rate_ttl_sec": _portfolio_fx_cache_ttl_sec(),
            "live_fx_usd_inr_rate": fx.get("rate", 0.0),
            "live_fx_source": fx.get("source", ""),
            "live_fx_source_url": fx.get("source_url", ""),
            "live_fx_provider_date": fx.get("provider_date", ""),
            "live_fx_available": bool(
                fx.get("live") and not fx.get("stale") and not fx.get("fallback") and fx.get("rate", 0) > 0
            ),
            "rate_note": (
                "Delta Exchange India uses a fixed USD-INR settlement rate for balances and P&L; live FX "
                "is reference-only."
            ),
        }
    fx = _portfolio_usd_inr_rate()
    return {
        "base": "USD",
        "settlement": "USDT",
        "quote": "INR",
        "usd_inr_rate": fx["rate"],
        "rate_available": bool(
            fx.get("live") and not fx.get("stale") and not fx.get("fallback") and fx.get("rate", 0) > 0
        ),
        "rate_kind": "live_fx",
        "rate_label": "Live FX",
        "rate_source": fx.get("source", ""),
        "rate_source_url": fx.get("source_url", ""),
        "rate_provider_date": fx.get("provider_date", ""),
        "rate_fetched_at": fx.get("fetched_at", ""),
        "rate_live": bool(fx.get("live")),
        "rate_stale": bool(fx.get("stale")),
        "rate_fallback": bool(fx.get("fallback")),
        "rate_error": fx.get("error", ""),
        "rate_ttl_sec": fx.get("ttl_sec", _portfolio_fx_cache_ttl_sec()),
        "rate_note": (
            "Fetched from live FX API; P&L accounting remains in USDT."
            if fx.get("live")
            else "FX API unavailable; INR display is unavailable until a live rate is fetched. P&L accounting remains in USDT."
        ),
    }


def _portfolio_sync_meta(
    status: str,
    *,
    order_count: int = 0,
    realized_count: int = 0,
    loaded: bool = False,
    message: str = "",
) -> dict:
    broker_info = _broker_summary()
    return {
        "status": status,
        "loaded": bool(loaded),
        "source": "broker_fills",
        "broker": broker_info,
        "broker_label": broker_info.get("label") or _broker_label(),
        "order_count": int(order_count or 0),
        "realized_count": int(realized_count or 0),
        "last_synced_at": datetime.now().isoformat(timespec="seconds"),
        "message": message,
    }


def _supported_trade_symbols() -> set[str]:
    try:
        symbols = set(getattr(delta, "get_supported_symbols", lambda: set())() or set())
        if symbols:
            return symbols
    except Exception:
        pass
    return {str(item.get("symbol", "")).upper() for item in config.TOP_25_CRYPTOS if item.get("symbol")}


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
_BUCKET_APP_SETTINGS = "app_settings"
_APP_SETTINGS_BROKER_KEY = "selected_broker"


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


def _normalize_broker_name(raw, *, default: str = "delta") -> str:
    broker_name = str(raw or "").strip().lower()
    supported = set(get_supported_brokers())
    if broker_name in supported:
        return broker_name
    return default if default in supported else sorted(supported)[0]


def _active_broker_name(client=None) -> str:
    current = client or globals().get("delta")
    return _normalize_broker_name(getattr(current, "broker_name", None), default="delta")


def _broker_is_configured(client=None) -> bool:
    current = client or globals().get("delta")
    checker = getattr(current, "_is_configured", None)
    return bool(checker()) if callable(checker) else False


def _available_broker_defs() -> list[dict]:
    brokers = []
    for name in get_supported_brokers():
        client = get_broker_client(name)
        feed_kind = getattr(client, "get_market_feed_kind", lambda: "polling")()
        brokers.append(
            {
                "name": name,
                "label": str(getattr(client, "display_name", name.title()) or name.title()),
                "feed_kind": str(feed_kind or "polling"),
                "configured": _broker_is_configured(client),
            }
        )
    return brokers


def _broker_summary(client=None) -> dict:
    current = client or globals().get("delta")
    return {
        "name": _active_broker_name(current),
        "label": str(getattr(current, "display_name", "Broker") or "Broker"),
        "configured": _broker_is_configured(current),
        "feed_kind": str(getattr(current, "get_market_feed_kind", lambda: "polling")() or "polling"),
    }


def _load_selected_broker_name() -> str:
    payload = _get_state_store().get(_BUCKET_APP_SETTINGS, _APP_SETTINGS_BROKER_KEY, default={})
    if isinstance(payload, dict):
        raw = payload.get("broker") or payload.get("name")
    else:
        raw = payload
    env_default = os.getenv("CRYPTOFORGE_BROKER") or os.getenv("BROKER") or "delta"
    return _normalize_broker_name(raw, default=_normalize_broker_name(env_default))


def _persist_selected_broker_name(name: str) -> None:
    normalized = _normalize_broker_name(name)
    _get_state_store().put(
        _BUCKET_APP_SETTINGS,
        _APP_SETTINGS_BROKER_KEY,
        {"broker": normalized, "updated_at": str(datetime.now())},
    )


def _set_active_broker(name: str, *, persist: bool) -> dict:
    normalized = _normalize_broker_name(name)
    client = get_broker_client(normalized)
    globals()["broker"] = client
    globals()["delta"] = client
    os.environ["CRYPTOFORGE_BROKER"] = normalized
    if persist:
        _persist_selected_broker_name(normalized)
    return _broker_summary(client)


try:
    _set_active_broker(_load_selected_broker_name(), persist=False)
except Exception as exc:
    _logger.warning("Broker state bootstrap failed, keeping startup broker: %s", exc)


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


class BrokerSettingsRequest(BaseModel):
    broker: str


class AdminConfigUpdateRequest(BaseModel):
    values: Dict[str, Optional[str]] = {}
    clear_keys: List[str] = []
    active_broker: Optional[str] = None


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
        headers={"Cache-Control": "public, max-age=604800, immutable"},
    )


@app.api_route("/manifest.webmanifest", methods=["GET", "HEAD"])
async def manifest_webmanifest():
    return FileResponse(
        os.path.join(_HERE, "static", "manifest.webmanifest"),
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-store"},
    )


@app.api_route("/site.webmanifest", methods=["GET", "HEAD"])
async def site_webmanifest():
    return FileResponse(
        os.path.join(_HERE, "static", "manifest.webmanifest"),
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-store"},
    )


@app.api_route("/sw.js", methods=["GET", "HEAD"])
async def service_worker():
    return FileResponse(
        os.path.join(_HERE, "static", "sw.js"),
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-store"},
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
    _BUCKET_APP_SETTINGS,
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


def _broker_runtime_lock_summary() -> dict:
    registry = _runtime_registry_summary()
    scalp_engine = globals().get("_scalp_engine")
    scalp_engine_running = bool(scalp_engine is not None and getattr(scalp_engine, "_running", False))
    reasons = []
    if registry["live_running_runs"]:
        reasons.append("Stop live engines before switching brokers.")
    if registry["paper_running_runs"]:
        reasons.append("Stop paper engines before switching brokers.")
    if registry["scalp_open_trades"]:
        reasons.append("Close scalp open trades before switching brokers.")
    if registry["scalp_pending_entries"]:
        reasons.append("Clear scalp pending entries before switching brokers.")
    return {
        "live_running_runs": registry["live_running_runs"],
        "paper_running_runs": registry["paper_running_runs"],
        "scalp_engine_running": scalp_engine_running,
        "scalp_open_trades": registry["scalp_open_trades"],
        "scalp_pending_entries": registry["scalp_pending_entries"],
        "switchable": not reasons,
        "reasons": reasons,
    }


def _broker_settings_payload() -> dict:
    broker_info = _broker_summary()
    runtime_locks = _broker_runtime_lock_summary()
    return {
        "current_broker": broker_info["name"],
        "current_label": broker_info["label"],
        "configured": broker_info["configured"],
        "feed_kind": broker_info["feed_kind"],
        "available_brokers": _available_broker_defs(),
        "switchable": runtime_locks["switchable"],
        "runtime_locks": runtime_locks,
    }


_ENV_PATH = os.path.abspath(os.path.expanduser(os.getenv("CRYPTOFORGE_ENV_PATH") or os.path.join(_HERE, ".env")))
_ENV_KEY_RE = re.compile(r"^\s*(export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")
_ADMIN_CONFIG_FIELDS = [
    {
        "key": "CRYPTOFORGE_BROKER",
        "label": "Startup Broker",
        "section": "routing",
        "kind": "select",
        "options": get_supported_brokers(),
        "secret": False,
    },
    {"key": "CRYPTOFORGE_PIN", "label": "Unlock PIN", "section": "security", "kind": "password", "secret": True},
    {"key": "DELTA_API_KEY", "label": "API Key", "section": "delta", "kind": "password", "secret": True},
    {"key": "DELTA_API_SECRET", "label": "API Secret", "section": "delta", "kind": "password", "secret": True},
    {
        "key": "DELTA_REGION",
        "label": "Region",
        "section": "delta",
        "kind": "select",
        "options": ["india", "global"],
        "secret": False,
    },
    {
        "key": "DELTA_TESTNET",
        "label": "Testnet",
        "section": "delta",
        "kind": "boolean",
        "options": ["false", "true"],
        "secret": False,
    },
    {"key": "COINDCX_API_KEY", "label": "API Key", "section": "coindcx", "kind": "password", "secret": True},
    {"key": "COINDCX_API_SECRET", "label": "API Secret", "section": "coindcx", "kind": "password", "secret": True},
    {"key": "COINDCX_BASE_URL", "label": "API Base URL", "section": "coindcx", "kind": "text", "secret": False},
    {"key": "COINDCX_PUBLIC_URL", "label": "Public Data URL", "section": "coindcx", "kind": "text", "secret": False},
    {
        "key": "COINDCX_MARGIN_CURRENCY",
        "label": "Margin Currency",
        "section": "coindcx",
        "kind": "text",
        "secret": False,
    },
]
_ADMIN_CONFIG_FIELD_BY_KEY = {field["key"]: field for field in _ADMIN_CONFIG_FIELDS}


def _mask_secret(value: str) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    if len(raw) <= 8:
        return "****"
    return raw[:4] + "..." + raw[-4:]


def _env_file_values() -> dict:
    try:
        return {key: str(value) for key, value in dotenv_values(_ENV_PATH).items() if value is not None}
    except Exception as exc:
        _logger.warning("Failed to read env file %s: %s", _ENV_PATH, exc)
        return {}


def _admin_env_value(key: str, values: Optional[dict] = None) -> str:
    source = values if values is not None else _env_file_values()
    if key in source:
        return str(source.get(key) or "")
    return str(os.getenv(key, "") or "")


def _admin_config_field_payload(field: dict, values: dict) -> dict:
    key = field["key"]
    value = _admin_env_value(key, values)
    secret = bool(field.get("secret"))
    payload = {
        "key": key,
        "label": field.get("label") or key,
        "section": field.get("section") or "app",
        "kind": field.get("kind") or ("password" if secret else "text"),
        "secret": secret,
        "configured": bool(str(value).strip()),
        "masked": _mask_secret(value) if secret else "",
        "value": "" if secret else value,
    }
    if field.get("options"):
        payload["options"] = list(field.get("options") or [])
    return payload


def _admin_config_payload() -> dict:
    values = _env_file_values()
    return {
        "status": "ok",
        "env_path": _ENV_PATH,
        "env_exists": os.path.exists(_ENV_PATH),
        "env_writable": os.access(os.path.dirname(_ENV_PATH) or _HERE, os.W_OK),
        "fields": [_admin_config_field_payload(field, values) for field in _ADMIN_CONFIG_FIELDS],
        "broker_settings": _broker_settings_payload(),
        "runtime": _runtime_registry_summary(),
        "health": _admin_health_payload(),
        "updated_at": str(datetime.now()),
    }


def _serialize_env_value(value: str) -> str:
    raw = str(value or "")
    if raw == "":
        return ""
    if re.search(r"\s|#|['\"]", raw):
        return json.dumps(raw)
    return raw


def _normalize_admin_env_update(key: str, value: Optional[str]) -> str:
    field = _ADMIN_CONFIG_FIELD_BY_KEY.get(key)
    if not field:
        raise HTTPException(status_code=400, detail=f"Unsupported env key: {key}")
    raw = "" if value is None else str(value).strip()
    if "\n" in raw or "\r" in raw:
        raise HTTPException(status_code=400, detail=f"{key} cannot contain newlines")
    kind = field.get("kind") or "text"
    if key == "CRYPTOFORGE_PIN":
        if not raw:
            raise HTTPException(status_code=400, detail="CRYPTOFORGE_PIN cannot be empty")
        if len(raw) < 4:
            raise HTTPException(status_code=400, detail="CRYPTOFORGE_PIN must be at least 4 characters")
    if kind == "boolean":
        lowered = raw.lower()
        if lowered not in {"true", "false"}:
            raise HTTPException(status_code=400, detail=f"{key} must be true or false")
        return lowered
    if key == "CRYPTOFORGE_BROKER":
        return _normalize_broker_name(raw)
    if key == "DELTA_REGION":
        lowered = raw.lower()
        if lowered not in {"india", "global"}:
            raise HTTPException(status_code=400, detail="DELTA_REGION must be india or global")
        return lowered
    if key == "COINDCX_MARGIN_CURRENCY":
        return raw.upper()
    return raw


def _write_env_updates(updates: dict) -> str:
    if not updates:
        return ""
    env_dir = os.path.dirname(_ENV_PATH) or _HERE
    os.makedirs(env_dir, exist_ok=True)
    existing_lines = []
    if os.path.exists(_ENV_PATH):
        with open(_ENV_PATH, encoding="utf-8") as fh:
            existing_lines = fh.readlines()

    seen = set()
    output = []
    for line in existing_lines:
        match = _ENV_KEY_RE.match(line)
        key = match.group(2) if match else ""
        if key in updates:
            prefix = "export " if match and match.group(1) else ""
            output.append(f"{prefix}{key}={_serialize_env_value(updates[key])}\n")
            seen.add(key)
        else:
            output.append(line)

    missing = [key for key in updates if key not in seen]
    if missing and output and output[-1].strip():
        output.append("\n")
    for key in missing:
        output.append(f"{key}={_serialize_env_value(updates[key])}\n")

    backup_path = ""
    if os.path.exists(_ENV_PATH):
        backup_path = _ENV_PATH + ".bak-" + datetime.now().strftime("%Y%m%d%H%M%S")
        shutil.copy2(_ENV_PATH, backup_path)
    fd, tmp_path = tempfile.mkstemp(prefix=".env.", suffix=".tmp", dir=env_dir, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.writelines(output)
        os.replace(tmp_path, _ENV_PATH)
        try:
            os.chmod(_ENV_PATH, 0o600)
        except OSError:
            pass
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return backup_path


def _reload_runtime_config_from_env() -> None:
    global AUTH_PIN, SESSION_SECRET

    load_dotenv(_ENV_PATH, override=True)

    config.DELTA_API_KEY = os.getenv("DELTA_API_KEY", "YOUR_API_KEY_HERE")
    config.DELTA_API_SECRET = os.getenv("DELTA_API_SECRET", "YOUR_API_SECRET_HERE")
    config.COINDCX_API_KEY = os.getenv("COINDCX_API_KEY", "YOUR_COINDCX_API_KEY_HERE")
    config.COINDCX_API_SECRET = os.getenv("COINDCX_API_SECRET", "YOUR_COINDCX_API_SECRET_HERE")
    config.COINDCX_BASE_URL = os.getenv("COINDCX_BASE_URL", "https://api.coindcx.com")
    config.COINDCX_PUBLIC_URL = os.getenv("COINDCX_PUBLIC_URL", "https://public.coindcx.com")
    config.COINDCX_MARGIN_CURRENCY = os.getenv("COINDCX_MARGIN_CURRENCY", "USDT").upper()
    config.CRYPTOFORGE_BROKER = os.getenv("CRYPTOFORGE_BROKER", os.getenv("BROKER", "delta")).lower()
    config.DELTA_TESTNET = os.getenv("DELTA_TESTNET", "false").lower() == "true"
    config.DELTA_REGION = os.getenv("DELTA_REGION", "india").lower()
    if config.DELTA_TESTNET:
        config.DELTA_BASE_URL = "https://testnet-api.delta.exchange/v2"
        config.DELTA_WS_URL = "wss://testnet-socket.delta.exchange"
    elif config.DELTA_REGION == "global":
        config.DELTA_BASE_URL = "https://api.delta.exchange/v2"
        config.DELTA_WS_URL = "wss://socket.delta.exchange"
    else:
        config.DELTA_BASE_URL = "https://api.india.delta.exchange/v2"
        config.DELTA_WS_URL = "wss://socket.india.delta.exchange"
    AUTH_PIN = os.getenv("CRYPTOFORGE_PIN") or os.getenv("CRYPTOFORGE_PASSWORD") or AUTH_PIN
    SESSION_SECRET = os.getenv("SESSION_SECRET", SESSION_SECRET)


async def _apply_admin_config_update(payload: AdminConfigUpdateRequest) -> dict:
    runtime_locks = _broker_runtime_lock_summary()
    requested_active = str(payload.active_broker or "").strip().lower()
    value_updates = dict(payload.values or {})
    clear_keys = {str(key or "").strip() for key in (payload.clear_keys or []) if str(key or "").strip()}

    if requested_active:
        requested_active = _normalize_broker_name(requested_active)
        value_updates["CRYPTOFORGE_BROKER"] = requested_active

    updates = {}
    for key, value in value_updates.items():
        key = str(key or "").strip()
        if not key:
            continue
        if key not in _ADMIN_CONFIG_FIELD_BY_KEY:
            raise HTTPException(status_code=400, detail=f"Unsupported env key: {key}")
        if _ADMIN_CONFIG_FIELD_BY_KEY[key].get("secret") and (value is None or str(value).strip() == ""):
            continue
        updates[key] = _normalize_admin_env_update(key, value)

    for key in clear_keys:
        if key not in _ADMIN_CONFIG_FIELD_BY_KEY:
            raise HTTPException(status_code=400, detail=f"Unsupported env key: {key}")
        if key == "CRYPTOFORGE_PIN":
            raise HTTPException(status_code=400, detail="CRYPTOFORGE_PIN cannot be cleared from the admin console")
        updates[key] = ""

    if not updates and not requested_active:
        return {"status": "ok", "message": "No configuration changes submitted", **_admin_config_payload()}

    broker_sensitive_keys = set(updates) - {"CRYPTOFORGE_PIN"}
    target_switch = requested_active and requested_active != _active_broker_name()
    if (broker_sensitive_keys or target_switch) and not runtime_locks["switchable"]:
        return JSONResponse(
            status_code=409,
            content={
                "status": "locked",
                "message": runtime_locks["reasons"][0]
                if runtime_locks["reasons"]
                else "Stop active broker workflows before changing environment settings.",
                **_admin_config_payload(),
            },
        )

    backup_path = _write_env_updates(updates)
    _reload_runtime_config_from_env()
    active_target = requested_active or _active_broker_name()
    _set_active_broker(active_target, persist=True)
    _market_cache["data"] = None
    _market_cache["timestamp"] = 0
    _ticker_cache["data"] = None
    _ticker_cache["timestamp"] = 0
    _logger.info("Admin environment configuration updated: %s", ", ".join(sorted(updates)))
    return {
        "status": "ok",
        "message": "Environment settings saved",
        "backup_path": backup_path,
        **_admin_config_payload(),
    }


async def _switch_active_broker(name: str) -> dict:
    global _scalp_engine, _market_cache, _ticker_cache

    target = _normalize_broker_name(name, default=_active_broker_name())
    current = _active_broker_name()
    if target == current:
        return _broker_summary()

    runtime_locks = _broker_runtime_lock_summary()
    if not runtime_locks["switchable"]:
        raise HTTPException(status_code=409, detail="Active runtime prevents broker switching")

    scalp_engine = globals().get("_scalp_engine")
    if scalp_engine is not None:
        try:
            await scalp_engine.shutdown()
        except Exception as exc:
            _logger.warning("Failed to shutdown scalp engine before broker switch: %s", exc)
    _scalp_engine = None

    broker_info = _set_active_broker(target, persist=True)
    _market_cache["data"] = None
    _market_cache["timestamp"] = 0
    _ticker_cache["data"] = None
    _ticker_cache["timestamp"] = 0
    _logger.info("Active broker switched to %s (%s)", broker_info["label"], broker_info["name"])
    return broker_info


def _ops_state_summary() -> dict:
    store = _get_state_store()
    store_health = store.health()
    redis_client = _get_redis()
    registry = _runtime_registry_summary()
    recovery = _runtime_recovery_summary()
    scalp_state = _persisted_scalp_runtime_summary()
    broker_info = _broker_summary()
    ready_checks = {
        "auth_pin_configured": bool(AUTH_PIN),
        "state_store_writable": bool(store_health.get("writable")),
        "state_store_exists": bool(store_health.get("exists")),
        "broker_configured": broker_info["configured"],
        "delta_configured": broker_info["configured"],
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
        "broker": broker_info,
        "broker_configured": broker_info["configured"],
        "delta_configured": broker_info["configured"],
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


def _route_available(path: str, method: str = "GET") -> bool:
    target_method = str(method or "GET").upper()
    for route in app.routes:
        if getattr(route, "path", "") != path:
            continue
        methods = {str(item).upper() for item in (getattr(route, "methods", None) or set())}
        if target_method in methods:
            return True
    return False


def _read_repo_file(*parts: str) -> str:
    try:
        with open(os.path.join(_HERE, *parts), encoding="utf-8") as handle:
            return handle.read()
    except Exception:
        return ""


def _first_regex_group(pattern: str, text: str) -> str:
    match = re.search(pattern, text or "")
    return match.group(1) if match else ""


def _admin_health_payload() -> dict:
    ops = _ops_state_summary()
    strategy_html = _read_repo_file("strategy.html")
    sw_js = _read_repo_file("static", "sw.js")
    cache_version = _first_regex_group(r"cryptoforge-app\.js\?v=([0-9-]+)", strategy_html)
    service_worker = _first_regex_group(r"CACHE_NAME\s*=\s*'([^']+)'", sw_js)
    ready = bool(ops.get("ready") and ops.get("status") == "ok")
    checks = [
        _portfolio_check(
            "Readiness",
            "ok" if ready else "warn",
            "API readiness and state-store write checks.",
            str(ops.get("status")),
        ),
        _portfolio_check(
            "Broker",
            "ok" if ops.get("broker_configured") else "warn",
            "Active broker credential state.",
            str((ops.get("broker") or {}).get("label") or ""),
        ),
        _portfolio_check(
            "State Store",
            "ok" if (ops.get("state_store") or {}).get("writable") else "warn",
            "Persistent state database health.",
            str((ops.get("state_store") or {}).get("size_bytes") or 0),
        ),
        _portfolio_check(
            "Runtime", "ok", "Current engine registry.", json.dumps(ops.get("runtime") or {}, sort_keys=True)
        ),
        _portfolio_check(
            "Assets",
            "ok" if cache_version and service_worker else "warn",
            "Browser cache and service worker versions.",
            f"{cache_version} / {service_worker}",
        ),
    ]
    return {
        "status": "ok" if all(check["status"] == "ok" for check in checks) else "warn",
        "uptime_sec": ops.get("uptime_sec", 0),
        "active_port": os.getenv("PORT", ""),
        "cache_version": cache_version,
        "service_worker": service_worker,
        "rate_limit_backend": ops.get("rate_limit_backend"),
        "checks": checks,
    }


def _production_check(check_id: str, title: str, ok: bool, details: dict, warnings: Optional[list[str]] = None) -> dict:
    warning_list = [str(item) for item in (warnings or []) if str(item)]
    return {
        "id": check_id,
        "title": title,
        "status": "ok" if ok and not warning_list else "warn" if ok else "fail",
        "details": details,
        "warnings": warning_list,
    }


def _production_readiness_payload() -> dict:
    ops_summary = _ops_state_summary()
    strategy_html = _read_repo_file("strategy.html")
    login_html = _read_repo_file("login.html")
    broker_settings = _broker_settings_payload()

    required_shell_controls = {
        "dashboard": 'id="nav-dashboard"',
        "portfolio": 'id="nav-portfolio"',
        "builder": 'id="nav-builder"',
        "live": 'id="nav-live"',
        "scalp": 'id="nav-scalp"',
        "market": 'id="nav-market"',
        "results": 'id="nav-results"',
        "admin_console": 'id="topbar-admin-btn"',
        "appearance": 'id="topbar-appearance-btn"',
        "app_back": 'id="topbar-back-btn"',
        "app_refresh": 'id="topbar-refresh-btn"',
        "scalp_buy": 'id="cf-scalp-buy-btn"',
        "scalp_sell": 'id="cf-scalp-sell-btn"',
    }
    missing_controls = [name for name, marker in required_shell_controls.items() if marker not in strategy_html]
    login_controls = {"login_pin": 'data-val="', "login_appearance": 'id="login-appearance-toggle"'}
    missing_login_controls = [name for name, marker in login_controls.items() if marker not in login_html]

    required_get_routes = [
        "/api/dashboard/summary",
        "/api/portfolio/summary",
        "/api/engines/all",
        "/api/strategies",
        "/api/runs",
        "/api/live/status",
        "/api/paper/status",
        "/api/scalp/status",
        "/api/scalp/diagnostics",
        "/api/market/top25",
        "/api/admin/config",
        "/api/broker/settings",
        "/api/ops/state/backup",
        "/api/audit/production-readiness",
    ]
    missing_get_routes = [path for path in required_get_routes if not _route_available(path, "GET")]
    required_write_routes = [
        ("POST", "/api/backtest"),
        ("POST", "/api/live/start"),
        ("POST", "/api/live/stop"),
        ("POST", "/api/paper/start"),
        ("POST", "/api/paper/stop"),
        ("POST", "/api/scalp/enter"),
        ("POST", "/api/scalp/exit"),
        ("PUT", "/api/scalp/trades/{trade_id}/targets"),
        ("POST", "/api/scalp/trades/{trade_id}/add"),
        ("POST", "/api/scalp/reconcile"),
        ("PUT", "/api/admin/config"),
        ("PUT", "/api/broker/settings"),
        ("POST", "/api/ops/state/restore"),
        ("POST", "/api/emergency-stop"),
    ]
    missing_write_routes = [
        f"{method} {path}" for method, path in required_write_routes if not _route_available(path, method)
    ]

    broker_names = {item["name"] for item in broker_settings.get("available_brokers", [])}
    admin_keys = {field["key"] for field in _ADMIN_CONFIG_FIELDS}
    required_admin_keys = {
        "CRYPTOFORGE_BROKER",
        "DELTA_API_KEY",
        "DELTA_API_SECRET",
        "DELTA_REGION",
        "DELTA_TESTNET",
        "COINDCX_API_KEY",
        "COINDCX_API_SECRET",
        "COINDCX_BASE_URL",
        "COINDCX_PUBLIC_URL",
    }
    pwa_files = ["manifest.webmanifest", "sw.js", "pwa.js", "cryptoforge-pwa.css"]
    missing_pwa_files = [name for name in pwa_files if not os.path.exists(os.path.join(_HERE, "static", name))]
    static_files = ["cryptoforge-app.css", "cryptoforge-app.js", "cryptoforge-admin.js", "cryptoforge-boot.js"]
    missing_static_files = [name for name in static_files if not os.path.exists(os.path.join(_HERE, "static", name))]

    e2e_dir = os.path.join(_HERE, "e2e-tests", "tests")
    unit_dir = os.path.join(_HERE, "tests")
    e2e_specs = sorted(name for name in os.listdir(e2e_dir)) if os.path.isdir(e2e_dir) else []
    unit_specs = sorted(name for name in os.listdir(unit_dir)) if os.path.isdir(unit_dir) else []

    checks = [
        _production_check(
            "button_route_audit",
            "Button and route audit",
            not missing_controls and not missing_login_controls and not missing_get_routes and not missing_write_routes,
            {
                "shell_controls": sorted(required_shell_controls),
                "missing_shell_controls": missing_controls,
                "missing_login_controls": missing_login_controls,
                "get_routes_checked": required_get_routes,
                "missing_get_routes": missing_get_routes,
                "write_routes_checked": [f"{method} {path}" for method, path in required_write_routes],
                "missing_write_routes": missing_write_routes,
            },
        ),
        _production_check(
            "scalp_production_hardening",
            "Scalp production hardening",
            _route_available("/api/scalp/diagnostics", "GET")
            and not any("scalp" in item.lower() for item in missing_write_routes),
            {
                "routes": {
                    "status": _route_available("/api/scalp/status", "GET"),
                    "diagnostics": _route_available("/api/scalp/diagnostics", "GET"),
                    "enter": _route_available("/api/scalp/enter", "POST"),
                    "exit": _route_available("/api/scalp/exit", "POST"),
                    "targets": _route_available("/api/scalp/trades/{trade_id}/targets", "PUT"),
                    "add": _route_available("/api/scalp/trades/{trade_id}/add", "POST"),
                    "reconcile": _route_available("/api/scalp/reconcile", "POST"),
                },
                "freshness_thresholds_ms": {
                    "ws": ScalpEngine._entry_freshness_thresholds("ws"),
                    "rest": ScalpEngine._entry_freshness_thresholds("rest_quote"),
                    "fill_snapshot": ScalpEngine._entry_freshness_thresholds("broker_fill"),
                },
                "runtime": ops_summary.get("runtime", {}),
                "scalp_runtime": ops_summary.get("scalp_runtime", {}),
            },
        ),
        _production_check(
            "broker_readiness",
            "Broker readiness",
            {"delta", "coindcx"}.issubset(broker_names) and required_admin_keys.issubset(admin_keys),
            {
                "current_broker": broker_settings.get("current_broker"),
                "available_brokers": sorted(broker_names),
                "admin_keys_present": sorted(admin_keys & required_admin_keys),
                "missing_admin_keys": sorted(required_admin_keys - admin_keys),
                "switchable": broker_settings.get("switchable"),
                "runtime_locks": broker_settings.get("runtime_locks", {}),
            },
        ),
        _production_check(
            "security_pass",
            "Security pass",
            bool(AUTH_PIN)
            and _route_available("/api/auth/login", "POST")
            and _route_available("/api/auth/logout", "POST"),
            {
                "session_auth": True,
                "csrf_for_writes": True,
                "rate_limited_write_routes": ["admin_config_update", "broker_switch", "scalp_enter", "scalp_exit"],
                "headers": [
                    "Content-Security-Policy",
                    "X-Frame-Options",
                    "X-Content-Type-Options",
                    "Referrer-Policy",
                    "Cross-Origin-Opener-Policy",
                    "Permissions-Policy",
                ],
                "allowed_origins": list(_ALLOWED_ORIGINS),
            },
        ),
        _production_check(
            "data_safety",
            "Data safety",
            bool(ops_summary.get("ready_checks", {}).get("state_store_writable"))
            and _route_available("/api/ops/state/backup", "GET")
            and _route_available("/api/ops/state/restore", "POST"),
            {
                "state_store": ops_summary.get("state_store", {}),
                "bucket_counts": ops_summary.get("bucket_counts", {}),
                "backup_route": "/api/ops/state/backup",
                "restore_route": "/api/ops/state/restore",
                "recovery": ops_summary.get("recovery", {}),
            },
        ),
        _production_check(
            "performance",
            "Performance",
            _route_available("/api/cache/status", "GET") and _route_available("/api/ticker", "GET"),
            {
                "cache_status_route": "/api/cache/status",
                "ticker_bulk_route": "/api/ticker",
                "market_feed": broker_settings.get("feed_kind"),
                "rate_limit_backend": ops_summary.get("rate_limit_backend"),
                "uptime_sec": ops_summary.get("uptime_sec"),
            },
        ),
        _production_check(
            "mobile_desktop_app_qa",
            "Mobile and desktop app QA",
            not missing_pwa_files and not missing_static_files and not missing_controls,
            {
                "pwa_files": pwa_files,
                "missing_pwa_files": missing_pwa_files,
                "static_files": static_files,
                "missing_static_files": missing_static_files,
                "manifest_route": _route_available("/manifest.webmanifest", "GET"),
                "service_worker_route": _route_available("/sw.js", "GET"),
            },
        ),
        _production_check(
            "test_coverage",
            "Test coverage",
            bool(e2e_specs) and bool(unit_specs),
            {
                "e2e_specs": e2e_specs,
                "unit_specs": unit_specs,
                "production_audit_route_in_e2e": "05-site-audit.spec.ts" in e2e_specs,
            },
        ),
    ]
    fail_count = sum(1 for item in checks if item["status"] == "fail")
    warn_count = sum(1 for item in checks if item["status"] == "warn")
    return {
        "status": "fail" if fail_count else "warn" if warn_count else "ok",
        "ready": fail_count == 0,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
        "generated_at": str(datetime.now()),
    }


def _scalp_diagnostics_payload(symbol: str = "") -> dict:
    eng = _get_scalp_engine()
    selected_symbol = ""
    if symbol:
        selected_symbol = _normalize_scalp_symbol(symbol, allow_blank=True)
        if selected_symbol:
            eng.watch_symbol(selected_symbol)
    if not eng.open_trades and not eng.pending_entries:
        _restore_scalp_runtime(eng)
    status = eng.get_status(selected_symbol)
    feed = dict(status.get("feed_metrics") or {})
    entry_controls = dict(status.get("entry_controls") or {})
    return {
        "status": "ok",
        "symbol": entry_controls.get("symbol") or selected_symbol or "",
        "broker": _broker_summary(),
        "runtime": {
            "running": bool(status.get("running")),
            "open_trades": len(status.get("open_trades") or []),
            "pending_entries": len(status.get("pending_entries") or []),
            "session_pnl": status.get("session_pnl", 0),
        },
        "feed": feed,
        "entry_controls": entry_controls,
        "execution": dict(status.get("execution_metrics") or {}),
        "run_policy": dict(status.get("run_policy") or {}),
        "guards": {
            "ws_fresh_ms": ScalpEngine._entry_freshness_thresholds("ws")[0],
            "ws_paper_ms": ScalpEngine._entry_freshness_thresholds("ws")[1],
            "rest_fresh_ms": ScalpEngine._entry_freshness_thresholds("rest_quote")[0],
            "rest_paper_ms": ScalpEngine._entry_freshness_thresholds("rest_quote")[1],
            "paper_allowed": bool(entry_controls.get("paper_allowed")),
            "live_allowed": bool(entry_controls.get("live_allowed")),
            "reason": entry_controls.get("reason", ""),
        },
        "persistence": {
            "state_dir": _STATE_DIR,
            "state_db": _current_state_db_file(),
            "runtime_file": _SCALP_RUNTIME_FILE,
            "trades_file": _SCALP_FILE,
            "events_file": _SCALP_EVENTS_FILE,
        },
        "routes": {
            "status": "/api/scalp/status",
            "enter": "/api/scalp/enter",
            "exit": "/api/scalp/exit",
            "targets": "/api/scalp/trades/{trade_id}/targets",
            "add": "/api/scalp/trades/{trade_id}/add",
            "reconcile": "/api/scalp/reconcile",
        },
    }


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
        "broker": summary["broker"],
        "broker_configured": summary["broker_configured"],
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
        "broker": summary["broker"],
        "runtime": summary["runtime"],
        "recovery": summary["recovery"],
        "state_store": summary["state_store"],
        "time": summary["time"],
    }


@app.get("/api/audit/production-readiness")
async def production_readiness():
    return _production_readiness_payload()


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

    broker_settings = _broker_settings_payload()

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
        **broker_settings,
    }


# ── Broker Connection ────────────────────────────────────────────


@app.get("/api/admin/config")
async def get_admin_config():
    return _admin_config_payload()


@app.get("/api/admin/health")
async def get_admin_health():
    return {"status": "ok", "health": _admin_health_payload(), "ops": _ops_state_summary()}


@app.put("/api/admin/config")
async def update_admin_config(payload: AdminConfigUpdateRequest, request: Request = None):
    check_rate_limit(
        "admin_config_update",
        max_calls=6,
        window_sec=60,
        client_ip=request.client.host if request and request.client else "global",
    )
    return await _apply_admin_config_update(payload)


@app.get("/api/broker/settings")
async def get_broker_settings():
    return {"status": "ok", **_broker_settings_payload()}


@app.put("/api/broker/settings")
async def update_broker_settings(payload: BrokerSettingsRequest, request: Request = None):
    check_rate_limit(
        "broker_switch",
        max_calls=4,
        window_sec=60,
        client_ip=request.client.host if request and request.client else "global",
    )
    requested_name = str(payload.broker or "").strip().lower()
    supported = {item["name"] for item in _available_broker_defs()}
    if requested_name not in supported:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": f"Unsupported broker '{requested_name or 'unknown'}'.",
                **_broker_settings_payload(),
            },
        )
    target_name = requested_name
    runtime_locks = _broker_runtime_lock_summary()
    if target_name != _active_broker_name() and not runtime_locks["switchable"]:
        return JSONResponse(
            status_code=409,
            content={
                "status": "locked",
                "message": runtime_locks["reasons"][0]
                if runtime_locks["reasons"]
                else "Active runtime prevents broker switching.",
                **_broker_settings_payload(),
            },
        )
    broker_info = await _switch_active_broker(target_name)
    return {
        "status": "ok",
        "message": f"Broker switched to {broker_info['label']}",
        **_broker_settings_payload(),
    }


@app.post("/api/broker/check")
async def check_broker(request: Request = None):
    check_rate_limit(
        "broker_check",
        max_calls=6,
        window_sec=30,
        client_ip=request.client.host if request and request.client else "global",
    )
    broker_settings = _broker_settings_payload()
    try:
        if not _broker_is_configured():
            return {
                "status": "not_configured",
                "broker": broker_settings["current_label"],
                "message": f"{_broker_label()} API credentials not configured.",
                **broker_settings,
            }
        wallet = delta.get_wallet()
        if isinstance(wallet, dict) and "error" not in wallet:
            return {
                "status": "connected",
                "broker": broker_settings["current_label"],
                "message": "Broker connection active",
                "wallet": wallet,
                **broker_settings,
            }
        if isinstance(wallet, list):
            return {
                "status": "connected",
                "broker": broker_settings["current_label"],
                "message": "Broker connection active",
                "wallet": wallet,
                **broker_settings,
            }
        return {
            "status": "error",
            "broker": broker_settings["current_label"],
            "message": wallet.get("error", "Unknown error") if isinstance(wallet, dict) else "Unknown error",
            **broker_settings,
        }
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg or "Unauthorized" in error_msg:
            return {
                "status": "error",
                "broker": broker_settings["current_label"],
                "message": "Invalid API credentials (401 Unauthorized)",
                **broker_settings,
            }
        elif "403" in error_msg or "Forbidden" in error_msg:
            return {
                "status": "error",
                "broker": broker_settings["current_label"],
                "message": "Access forbidden (403)",
                **broker_settings,
            }
        elif "timeout" in error_msg.lower():
            return {
                "status": "error",
                "broker": broker_settings["current_label"],
                "message": "Connection timeout",
                **broker_settings,
            }
        return {
            "status": "error",
            "broker": broker_settings["current_label"],
            "message": f"Connection error: {str(e)[:100]}",
            **broker_settings,
        }


@app.post("/api/broker/connect")
async def connect_broker(request: Request = None):
    check_rate_limit(
        "broker_connect",
        max_calls=4,
        window_sec=30,
        client_ip=request.client.host if request and request.client else "global",
    )
    broker_settings = _broker_settings_payload()
    try:
        if not _broker_is_configured():
            return {
                "status": "not_configured",
                "broker": broker_settings["current_label"],
                "message": "API credentials not configured. Update .env file.",
                **broker_settings,
            }
        wallet = delta.get_wallet()
        if isinstance(wallet, (dict, list)) and (not isinstance(wallet, dict) or "error" not in wallet):
            return {
                "status": "connected",
                "broker": broker_settings["current_label"],
                "message": f"Connected to {_broker_label()}",
                "wallet": wallet,
                **broker_settings,
            }
        return {
            "status": "error",
            "broker": broker_settings["current_label"],
            "message": str(wallet.get("error", "Unknown error")) if isinstance(wallet, dict) else "Invalid response",
            **broker_settings,
        }
    except Exception as e:
        alerter.alert("Broker Connect Failed", f"Error: {e}", level="warn")
        return {
            "status": "error",
            "broker": broker_settings["current_label"],
            "message": f"Connection failed: {str(e)[:100]}",
            **broker_settings,
        }


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
    """Return configured-broker tradeable perpetual futures for the frontend."""
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

# Map CoinGecko symbol → app trade symbol
_CG_TO_TRADE = {
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

        broker_symbols = _supported_trade_symbols()
        coins = []
        for c in filtered:
            sym = c.get("symbol", "").lower()
            broker_sym = _CG_TO_TRADE.get(sym)
            # Every coin gets a USDT trading symbol (Binance format)
            trade_symbol = broker_sym or (c.get("symbol", "").upper() + "USDT")
            is_tradeable = trade_symbol in broker_symbols
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
                    "delta_tradeable": is_tradeable,
                    "delta_symbol": trade_symbol if is_tradeable else None,
                    "broker_tradeable": is_tradeable,
                    "broker_symbol": trade_symbol if is_tradeable else None,
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
    if raw not in _supported_trade_symbols():
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


def _parse_scalp_order_type(body: Dict, *, entry_stop_price: float, entry_limit_price: float) -> str:
    raw = _body_value(body, "order_type", "entry_order_type", default="")
    order_type = normalize_scalp_order_type(
        raw,
        entry_stop_price=entry_stop_price,
        entry_limit_price=entry_limit_price,
    )
    if order_type == "invalid":
        raise HTTPException(status_code=400, detail="Unsupported scalp order type")
    return order_type


def _parse_scalp_trail_mode(body: Dict) -> str:
    raw = str(_body_value(body, "trail_mode", "trail_type", default="usd") or "usd").strip().lower()
    aliases = {
        "usd": "usd",
        "usdt": "usd",
        "$": "usd",
        "price": "usd",
        "pct": "pct",
        "%": "pct",
        "percent": "pct",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise HTTPException(status_code=400, detail="trail_mode must be usd or pct")
    return normalized


@app.get("/api/ticker")
async def get_ticker():
    """Fetch live prices for top cryptos."""
    global _ticker_cache
    if _ticker_cache["data"] and (time.time() - _ticker_cache["timestamp"]) < _ticker_cache["ttl"]:
        return _ticker_cache["data"]

    try:
        tickers = delta.get_tickers_bulk()
        # Build a map by broker symbol and normalized app symbol.
        ticker_map = {}
        for t in tickers:
            sym = t.get("symbol", "")
            ticker_map[sym] = t
            app_sym = delta.from_delta_symbol(sym)
            if app_sym and app_sym != sym:
                ticker_map[app_sym] = t

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


# ── Local Candle Cache ────────────────────────────────────────────
_CACHE_DIR = os.path.join(_HERE, "cache", "candles")
os.makedirs(_CACHE_DIR, exist_ok=True)


def _cache_path(symbol: str, interval: str, broker_name: str | None = None) -> str:
    """Return path to the pickle cache file for a broker/symbol/interval tuple."""
    broker_dir = _normalize_broker_name(broker_name, default=_active_broker_name())
    base_dir = os.path.join(_CACHE_DIR, broker_dir)
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, f"{symbol}_{interval}.pkl")


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

    # ── 3. Fallback to configured broker ───────────────────────
    if symbol in _supported_trade_symbols():
        df = delta.get_candles(symbol, resolution=candle_interval, start=from_date, end=to_date)
        if not df.empty:
            _logger.info("%s: %d candles fetched", _broker_label(), len(df))
            _save_cache(df, symbol, candle_interval)
            return df

    raise Exception(f"No candle data for {symbol} (tried cache + Binance + {_broker_label()})")


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
        wallet = delta.get_wallet()
        if isinstance(wallet, dict) and wallet.get("error"):
            return {"status": "error", "message": str(wallet.get("error"))[:100], "data": []}
        return wallet
    except Exception as e:
        return {"status": "error", "message": str(e)[:100], "data": []}


# ── Broker Trade History ─────────────────────────────────────────
_MISSING_ORDER_FLOAT = object()


def _first_order_float(order: dict, keys: tuple[str, ...], default=None):
    for key in keys:
        value = order.get(key)
        if value in (None, ""):
            continue
        parsed = _safe_float(value, _MISSING_ORDER_FLOAT)
        if parsed is not _MISSING_ORDER_FLOAT:
            return parsed
    return default


def _order_fill_price(order: dict) -> float | None:
    return _first_order_float(
        order,
        ("average_fill_price", "avg_fill_price", "fill_price", "average_price", "avg_price", "price"),
        None,
    )


def _order_fill_size(order: dict) -> float:
    return _first_order_float(order, ("filled_size", "size", "quantity", "qty"), 0.0) or 0.0


def _order_fee(order: dict) -> float:
    return _first_order_float(
        order,
        (
            "paid_commission",
            "commission",
            "commission_paid",
            "total_commission",
            "fee",
            "fees",
            "fee_amount",
            "trading_fee",
            "brokerage",
        ),
        0.0,
    )


def _order_fill_time(order: dict) -> str:
    return str(
        order.get("filled_at") or order.get("updated_at") or order.get("created_at") or order.get("timestamp") or ""
    )


def _order_symbol_key(order: dict) -> str:
    return str(order.get("product_id") or order.get("product_symbol") or order.get("symbol") or "")


def _order_side_sign(order: dict) -> int:
    side = str(order.get("side") or "").lower()
    if side in {"buy", "long"}:
        return 1
    if side in {"sell", "short"}:
        return -1
    return 0


def _order_contract_value(order: dict, default: float = 1.0) -> float:
    product = order.get("product") if isinstance(order.get("product"), dict) else {}
    value = _safe_float(product.get("contract_value"), 0.0)
    return value if value > 0 else default


def _order_notional_type(order: dict) -> str:
    product = order.get("product") if isinstance(order.get("product"), dict) else {}
    return str(product.get("notional_type") or "").lower()


def _matched_order_gross_pnl(entry: dict, exit_order: dict, qty: float, open_sign: int) -> float:
    entry_price = _safe_float(entry.get("entry_price"), 0.0)
    exit_price = _safe_float(exit_order.get("fill_price"), 0.0)
    contract_value = _order_contract_value(exit_order, 0.0) or _order_contract_value(entry)
    notional_type = _order_notional_type(exit_order) or _order_notional_type(entry)
    if qty <= 0 or entry_price <= 0 or exit_price <= 0:
        return 0.0
    if notional_type == "inverse":
        return open_sign * ((1 / entry_price) - (1 / exit_price)) * qty * contract_value
    return open_sign * (exit_price - entry_price) * qty * contract_value


def _normalize_filled_order(order: dict) -> dict:
    """Attach normalized fill fields to raw broker filled-order rows."""
    if not isinstance(order, dict):
        return order

    normalized = dict(order)
    fees = _order_fee(normalized)
    fill_price = _order_fill_price(normalized)
    fill_size = _order_fill_size(normalized)
    gross_pnl = _first_order_float(
        normalized,
        (
            "gross_pnl",
            "realized_pnl",
            "realised_pnl",
            "realized_profit",
            "realised_profit",
            "profit_loss",
            "profit_and_loss",
            "pnl",
        ),
        None,
    )
    existing_net_pnl = _first_order_float(
        normalized,
        (
            "net_pnl",
            "realized_net_pnl",
            "realised_net_pnl",
            "net_profit_loss",
            "net_profit",
        ),
        None,
    )

    normalized["fees"] = round(fees, 8)
    normalized["fill_price"] = round(fill_price, 8) if fill_price is not None else None
    normalized["filled_size"] = round(fill_size, 8)
    normalized["filled_at"] = _order_fill_time(normalized)
    if existing_net_pnl is not None:
        normalized["net_pnl"] = round(existing_net_pnl, 8)
        normalized["gross_pnl"] = round(gross_pnl if gross_pnl is not None else existing_net_pnl + fees, 8)
        normalized["realized_fees"] = fees
        normalized["pnl_status"] = "broker"
        normalized["pnl_audit"] = {
            "source": "broker_reported",
            "gross_pnl": normalized["gross_pnl"],
            "fees_subtracted": round(fees, 8),
            "net_pnl": normalized["net_pnl"],
            "formula": "net_pnl = broker_reported_net_pnl; gross_pnl shown as broker gross or net + fees",
        }
    elif gross_pnl is not None:
        normalized["gross_pnl"] = round(gross_pnl, 8)
        normalized["net_pnl"] = round(gross_pnl - fees, 8)
        normalized["realized_fees"] = fees
        normalized["pnl_status"] = "broker"
        normalized["pnl_audit"] = {
            "source": "broker_reported",
            "gross_pnl": normalized["gross_pnl"],
            "fees_subtracted": round(fees, 8),
            "net_pnl": normalized["net_pnl"],
            "formula": "net_pnl = broker_reported_gross_pnl - broker_fees",
        }
    else:
        normalized["gross_pnl"] = None
        normalized["net_pnl"] = None
        normalized["realized_fees"] = None
        normalized["pnl_status"] = "unmatched"
    return normalized


def _is_filled_order(order: dict) -> bool:
    if not isinstance(order, dict):
        return False
    if order.get("net_pnl") is not None or order.get("gross_pnl") is not None:
        return True
    return _safe_float(order.get("fill_price"), 0.0) > 0 and _safe_float(order.get("filled_size"), 0.0) > 0


def _attach_matched_order_pnl(rows: list[dict]) -> list[dict]:
    """Derive realized P/L for broker rows that only expose raw fills."""
    lots_by_symbol: dict[str, list[dict]] = defaultdict(list)
    sorted_rows = sorted(rows, key=lambda row: (_order_fill_time(row), str(row.get("id") or "")))
    for order in sorted_rows:
        if order.get("net_pnl") is not None:
            continue
        side_sign = _order_side_sign(order)
        fill_price = _safe_float(order.get("fill_price"), 0.0)
        qty = _safe_float(order.get("filled_size"), 0.0)
        if side_sign == 0 or fill_price <= 0 or qty <= 0:
            order["pnl_status"] = "unmatched"
            continue

        symbol_key = _order_symbol_key(order)
        lots = lots_by_symbol[symbol_key]
        remaining = qty
        order_fee = _safe_float(order.get("fees"), 0.0)
        matched_qty = 0.0
        gross_pnl = 0.0
        realized_fees = 0.0

        while remaining > 1e-12 and lots and lots[0]["sign"] != side_sign:
            lot = lots[0]
            close_qty = min(remaining, lot["remaining_qty"])
            if close_qty <= 0:
                lots.pop(0)
                continue

            lot_ratio = close_qty / lot["remaining_qty"] if lot["remaining_qty"] else 0.0
            order_ratio = close_qty / qty if qty else 0.0
            entry_fee_part = lot["remaining_fee"] * lot_ratio
            exit_fee_part = order_fee * order_ratio

            gross_pnl += _matched_order_gross_pnl(lot, order, close_qty, lot["sign"])
            realized_fees += entry_fee_part + exit_fee_part
            matched_qty += close_qty
            remaining -= close_qty

            lot["remaining_qty"] -= close_qty
            lot["remaining_fee"] -= entry_fee_part
            if lot["remaining_qty"] <= 1e-12:
                lots.pop(0)

        if matched_qty > 0:
            order["matched_size"] = round(matched_qty, 8)
            order["gross_pnl"] = round(gross_pnl, 8)
            order["realized_fees"] = round(realized_fees, 8)
            order["net_pnl"] = round(gross_pnl - realized_fees, 8)
            order["pnl_status"] = "realized" if remaining <= 1e-12 else "partial_realized"
            order["pnl_audit"] = {
                "source": "matched_fills",
                "matched_size": round(matched_qty, 8),
                "gross_pnl": round(gross_pnl, 8),
                "fees_subtracted": round(realized_fees, 8),
                "net_pnl": round(gross_pnl - realized_fees, 8),
                "formula": "net_pnl = matched_fill_gross_pnl - entry_fee_share - exit_fee_share",
            }

        if remaining > 1e-12:
            remaining_fee = order_fee * (remaining / qty if qty else 0.0)
            lots.append(
                {
                    "sign": side_sign,
                    "remaining_qty": remaining,
                    "remaining_fee": remaining_fee,
                    "entry_price": fill_price,
                    "product": order.get("product"),
                }
            )
            if matched_qty <= 0:
                order["pnl_status"] = "entry"
    return rows


def _normalize_filled_orders(orders, limit: int | None = None) -> list:
    rows = [_normalize_filled_order(order) for order in list(orders or [])]
    rows = [row for row in rows if _is_filled_order(row)]
    _attach_matched_order_pnl(rows)
    return rows[:limit] if limit else rows


def _wallet_rows(wallet) -> list[dict]:
    if isinstance(wallet, list):
        return [row for row in wallet if isinstance(row, dict)]
    if isinstance(wallet, dict):
        if isinstance(wallet.get("result"), list):
            return [row for row in wallet["result"] if isinstance(row, dict)]
        if isinstance(wallet.get("data"), list):
            return [row for row in wallet["data"] if isinstance(row, dict)]
        if "error" not in wallet:
            return [wallet]
    return []


def _wallet_asset_row(wallet, assets: tuple[str, ...] = ("USDT", "USD", "INR")) -> dict:
    wanted = {asset.upper() for asset in assets}
    rows = _wallet_rows(wallet)
    for row in rows:
        symbol = str(
            row.get("asset_symbol")
            or row.get("asset")
            or row.get("currency")
            or row.get("margin_currency")
            or row.get("margin_currency_short_name")
            or ""
        ).upper()
        if symbol in wanted:
            return row
    return rows[0] if rows else {}


def _wallet_amount(row: dict, *keys: str) -> float:
    for key in keys:
        parsed = _safe_float(row.get(key), _MISSING_ORDER_FLOAT)
        if parsed is not _MISSING_ORDER_FLOAT:
            return parsed
    return 0.0


def _portfolio_inr_value(usd_value: float, currency: dict) -> float | None:
    rate = _safe_float(currency.get("usd_inr_rate"), 0.0)
    if not currency.get("rate_available") or rate <= 0:
        return None
    return round(_safe_float(usd_value, 0.0) * rate, 2)


def _portfolio_money_pair(usd_value: float, currency: dict) -> dict:
    usd = round(_safe_float(usd_value, 0.0), 2)
    return {"usd": usd, "inr": _portfolio_inr_value(usd, currency)}


def _portfolio_accounting_payload(wallet, positions: list, filled_orders: list, currency: dict) -> dict:
    asset_row = _wallet_asset_row(wallet)
    asset = str(asset_row.get("asset_symbol") or asset_row.get("asset") or asset_row.get("currency") or "USD").upper()
    available = _wallet_amount(asset_row, "available_balance", "available_margin", "free_balance", "free")
    wallet_balance = _wallet_amount(asset_row, "balance", "wallet_balance", "total_balance", "equity")
    blocked_margin = _wallet_amount(asset_row, "blocked_margin", "locked_margin", "hold_balance", "locked_balance")
    order_margin = _wallet_amount(asset_row, "order_margin", "open_order_margin")
    position_margin = _wallet_amount(asset_row, "position_margin", "used_margin")
    position_margin_sum = sum(_safe_float(pos.get("margin"), 0.0) for pos in positions or [])
    unrealized = sum(_safe_float(pos.get("unrealized_pnl"), 0.0) for pos in positions or [])
    realized_fees = sum(
        _safe_float(order.get("realized_fees"), _safe_float(order.get("fees"), 0.0)) for order in filled_orders
    )
    realized_net = sum(
        _safe_float(order.get("net_pnl"), 0.0) for order in filled_orders if order.get("net_pnl") is not None
    )
    realized_gross = sum(
        _safe_float(order.get("gross_pnl"), 0.0) for order in filled_orders if order.get("gross_pnl") is not None
    )
    wallet_equity = wallet_balance + unrealized
    margin_total = max(blocked_margin + order_margin + position_margin, position_margin_sum)
    return {
        "asset": asset,
        "rate_label": currency.get("rate_label") or "INR display",
        "rate_kind": currency.get("rate_kind") or "",
        "rate_note": currency.get("rate_note") or "",
        "available_balance": _portfolio_money_pair(available, currency),
        "wallet_balance": _portfolio_money_pair(wallet_balance, currency),
        "wallet_equity": _portfolio_money_pair(wallet_equity, currency),
        "blocked_margin": _portfolio_money_pair(blocked_margin, currency),
        "order_margin": _portfolio_money_pair(order_margin, currency),
        "position_margin": _portfolio_money_pair(position_margin, currency),
        "position_margin_from_positions": _portfolio_money_pair(position_margin_sum, currency),
        "total_margin_locked": _portfolio_money_pair(margin_total, currency),
        "unrealized_pnl": _portfolio_money_pair(unrealized, currency),
        "recent_realized_gross": _portfolio_money_pair(realized_gross, currency),
        "recent_realized_fees": _portfolio_money_pair(realized_fees, currency),
        "recent_realized_net": _portfolio_money_pair(realized_net, currency),
        "open_positions": len(positions or []),
        "recent_fills": len(filled_orders or []),
    }


def _portfolio_check(label: str, status: str, detail: str, value: str = "") -> dict:
    return {"label": label, "status": status, "detail": detail, "value": value}


def _portfolio_reconciliation_payload(
    wallet, positions: list, filled_orders: list, accounting: dict, currency: dict, broker_sync: dict
) -> dict:
    wallet_loaded = bool(_wallet_rows(wallet))
    total_orders = len(filled_orders or [])
    realized_orders = sum(1 for order in filled_orders or [] if order.get("net_pnl") is not None)
    unmatched_orders = sum(1 for order in filled_orders or [] if order.get("net_pnl") is None)
    position_margin_wallet = _safe_float(accounting.get("position_margin", {}).get("usd"), 0.0)
    position_margin_rows = _safe_float(accounting.get("position_margin_from_positions", {}).get("usd"), 0.0)
    margin_delta = round(abs(position_margin_wallet - position_margin_rows), 2)
    checks = [
        _portfolio_check(
            "Wallet",
            "ok" if wallet_loaded else "warn",
            "Broker wallet payload loaded." if wallet_loaded else "Wallet payload is empty.",
            accounting.get("asset", ""),
        ),
        _portfolio_check(
            "Positions",
            "ok" if margin_delta <= 1 else "warn",
            f"Wallet position margin and open-position margin differ by ${margin_delta:,.2f}.",
            str(len(positions or [])),
        ),
        _portfolio_check(
            "Filled Orders",
            "ok" if total_orders else "warn",
            f"{realized_orders}/{total_orders} recent fills have realized net P&L.",
            str(total_orders),
        ),
        _portfolio_check(
            "Unmatched Fills",
            "ok" if unmatched_orders == 0 else "warn",
            "All recent fills are matched."
            if unmatched_orders == 0
            else f"{unmatched_orders} recent fills are entries/open legs.",
            str(unmatched_orders),
        ),
        _portfolio_check(
            "INR Rate",
            "ok" if currency.get("rate_available") else "warn",
            currency.get("rate_note") or "INR conversion metadata loaded.",
            str(currency.get("rate_label") or ""),
        ),
    ]
    state = "ok" if all(check["status"] == "ok" for check in checks) else "warn"
    if broker_sync.get("status") == "error":
        state = "error"
    return {
        "status": state,
        "checks": checks,
        "wallet_asset": accounting.get("asset", ""),
        "order_count": total_orders,
        "realized_count": realized_orders,
        "unmatched_count": unmatched_orders,
    }


def _portfolio_freshness_payload(generated_at: str, broker_sync: dict, currency: dict) -> dict:
    now_dt = _normalize_datetime(generated_at) or datetime.now()
    provider_dt = _normalize_datetime(currency.get("rate_fetched_at") or currency.get("rate_provider_date"))
    fx_age = round((now_dt - provider_dt).total_seconds()) if provider_dt else None
    return {
        "generated_at": generated_at,
        "items": [
            {"label": "Wallet", "state": "fresh", "age_sec": 0, "detail": "Fetched with portfolio summary."},
            {"label": "Positions", "state": "fresh", "age_sec": 0, "detail": "Fetched with portfolio summary."},
            {"label": "Filled Orders", "state": "fresh", "age_sec": 0, "detail": "Fetched with portfolio summary."},
            {
                "label": "Portfolio History",
                "state": "fresh" if broker_sync.get("loaded") else "warn",
                "age_sec": 0,
                "detail": broker_sync.get("message") or "Calendar sync pending.",
            },
            {
                "label": "INR Rate",
                "state": "fresh" if currency.get("rate_available") else "warn",
                "age_sec": fx_age,
                "detail": currency.get("rate_label") or "FX metadata pending.",
            },
        ],
    }


def _portfolio_parity_payload(currency: dict) -> dict:
    return {
        "items": [
            {
                "label": "Fee Model",
                "detail": "Broker P&L subtracts entry and exit commissions from matched fills.",
                "status": "ok",
            },
            {
                "label": "Settlement",
                "detail": currency.get("rate_note") or "INR values use the active broker currency policy.",
                "status": "ok" if currency.get("rate_available") else "warn",
            },
            {
                "label": "Backtest/Live",
                "detail": "Calendar totals prefer broker realized fills for live trades and saved runs for paper trades.",
                "status": "ok",
            },
            {
                "label": "CSV Export",
                "detail": "Filled-order export includes USD and INR fee/P&L columns from the same display rate.",
                "status": "ok",
            },
        ]
    }


def _portfolio_safety_payload(accounting: dict) -> dict:
    registry = _runtime_registry_summary()
    available = _safe_float(accounting.get("available_balance", {}).get("usd"), 0.0)
    open_margin = _safe_float(accounting.get("total_margin_locked", {}).get("usd"), 0.0)
    margin_usage = round((open_margin / available * 100) if available > 0 else 0.0, 2)
    checks = [
        _portfolio_check(
            "Kill Switch",
            "ok" if not registry["live_running_runs"] and not registry["scalp_running"] else "warn",
            "Idle."
            if not registry["live_running_runs"] and not registry["scalp_running"]
            else "Visible while live/scalp workflows run.",
            "armed" if registry["live_running_runs"] or registry["scalp_running"] else "idle",
        ),
        _portfolio_check(
            "Live Engines",
            "ok" if not registry["live_running_runs"] else "warn",
            "Active live run count.",
            str(len(registry["live_running_runs"])),
        ),
        _portfolio_check(
            "Scalp Exposure",
            "ok" if not registry["scalp_open_trades"] else "warn",
            "Open scalp trades.",
            str(registry["scalp_open_trades"]),
        ),
        _portfolio_check(
            "Margin Usage",
            "ok" if margin_usage < 50 else "warn",
            "Open/order margin as a share of available balance.",
            f"{margin_usage:.2f}%",
        ),
    ]
    return {"checks": checks, "margin_usage_pct": margin_usage, "runtime": registry}


def _portfolio_alerts_payload(reconciliation: dict, safety: dict, currency: dict) -> list[dict]:
    alerts = []
    if not currency.get("rate_available"):
        alerts.append({"level": "warning", "title": "INR rate unavailable", "message": currency.get("rate_note", "")})
    for check in reconciliation.get("checks", []):
        if check.get("status") != "ok":
            alerts.append(
                {"level": "warning", "title": check.get("label", "Reconciliation"), "message": check.get("detail", "")}
            )
    for check in safety.get("checks", []):
        if check.get("status") != "ok":
            alerts.append(
                {"level": "warning", "title": check.get("label", "Safety"), "message": check.get("detail", "")}
            )
    if not alerts:
        alerts.append(
            {
                "level": "ok",
                "title": "No active portfolio alerts",
                "message": "Broker accounting, rate policy, and runtime state are in expected ranges.",
            }
        )
    return alerts[:8]


def _portfolio_journal_payload(
    filled_orders: list, positions: list, broker_sync: dict, generated_at: str
) -> list[dict]:
    entries = [
        {
            "time": generated_at,
            "type": "sync",
            "title": "Portfolio sync",
            "detail": broker_sync.get("message") or "Portfolio summary refreshed.",
            "amount": "",
        }
    ]
    for pos in positions or []:
        entries.append(
            {
                "time": generated_at,
                "type": "position",
                "title": f"{pos.get('symbol', '')} open position",
                "detail": f"{pos.get('side', '')} size {pos.get('size', 0)} @ {pos.get('entry_price', 0)}",
                "amount": round(_safe_float(pos.get("unrealized_pnl"), 0.0), 2),
            }
        )
    for order in (filled_orders or [])[:8]:
        entries.append(
            {
                "time": order.get("filled_at") or order.get("updated_at") or generated_at,
                "type": "fill",
                "title": f"{_portfolio_history_order_date(order) or 'Recent'} {order.get('side', '').upper()} {order.get('product_symbol') or order.get('symbol') or ''}",
                "detail": order.get("pnl_status") or "filled",
                "amount": None if order.get("net_pnl") is None else round(_safe_float(order.get("net_pnl"), 0.0), 2),
            }
        )
    return entries[:12]


@app.get("/api/broker/trades")
async def get_broker_trades():
    """Get filled order history from the active broker."""
    try:
        orders = _normalize_filled_orders(delta.get_order_history())
        return {"status": "ok", "trades": orders}
    except Exception as e:
        return {"status": "error", "trades": [], "message": str(e)[:100]}


@app.get("/api/portfolio/summary")
async def get_portfolio_summary():
    """Aggregated portfolio data: balance, positions, recent trades."""
    try:
        generated_at = datetime.now().isoformat(timespec="seconds")
        wallet = delta.get_wallet()
        positions = delta.get_positions()
        orders = delta.get_order_history()
        currency = _portfolio_currency_meta()

        wallet_asset = _wallet_asset_row(wallet)
        usdt_balance = _wallet_amount(wallet_asset, "available_balance", "available_margin", "free_balance", "free")

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

        filled_orders = _normalize_filled_orders(orders, limit=50)
        realized_count = sum(1 for order in filled_orders if order.get("net_pnl") is not None)
        broker_sync = _portfolio_sync_meta(
            "ok" if filled_orders else "empty",
            order_count=len(filled_orders),
            realized_count=realized_count,
            loaded=realized_count > 0,
            message=(
                f"{realized_count} realized broker fills reconciled."
                if realized_count
                else "No realized broker fills available in the recent order window."
            ),
        )
        accounting = _portfolio_accounting_payload(wallet, open_positions, filled_orders, currency)
        reconciliation = _portfolio_reconciliation_payload(
            wallet, open_positions, filled_orders, accounting, currency, broker_sync
        )
        freshness = _portfolio_freshness_payload(generated_at, broker_sync, currency)
        parity = _portfolio_parity_payload(currency)
        safety = _portfolio_safety_payload(accounting)
        alerts = _portfolio_alerts_payload(reconciliation, safety, currency)

        return {
            "status": "ok",
            "generated_at": generated_at,
            "currency": currency,
            "broker_sync": broker_sync,
            "balance": round(usdt_balance, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "open_positions": open_positions,
            "filled_orders": filled_orders,
            "accounting": accounting,
            "reconciliation": reconciliation,
            "freshness": freshness,
            "parity": parity,
            "safety": safety,
            "alerts": alerts,
            "journal": _portfolio_journal_payload(filled_orders, open_positions, broker_sync, generated_at),
        }
    except Exception as e:
        generated_at = datetime.now().isoformat(timespec="seconds")
        currency = _portfolio_currency_meta()
        broker_sync = _portfolio_sync_meta("error", message=str(e)[:200])
        accounting = _portfolio_accounting_payload([], [], [], currency)
        reconciliation = _portfolio_reconciliation_payload([], [], [], accounting, currency, broker_sync)
        safety = _portfolio_safety_payload(accounting)
        return {
            "status": "error",
            "message": str(e)[:200],
            "generated_at": generated_at,
            "currency": currency,
            "broker_sync": broker_sync,
            "balance": 0,
            "unrealized_pnl": 0,
            "open_positions": [],
            "filled_orders": [],
            "accounting": accounting,
            "reconciliation": reconciliation,
            "freshness": _portfolio_freshness_payload(generated_at, broker_sync, currency),
            "parity": _portfolio_parity_payload(currency),
            "safety": safety,
            "alerts": _portfolio_alerts_payload(reconciliation, safety, currency),
            "journal": _portfolio_journal_payload([], [], broker_sync, generated_at),
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


def _portfolio_history_empty_day() -> dict:
    return {
        "real_pnl": 0,
        "real_net_pnl": 0,
        "real_gross_pnl": 0,
        "real_fees": 0,
        "paper_pnl": 0,
        "real_trades": 0,
        "paper_trades": 0,
        "real_wins": 0,
        "paper_wins": 0,
    }


def _portfolio_history_day(daily: dict, date_str: str) -> dict:
    if date_str not in daily:
        daily[date_str] = _portfolio_history_empty_day()
    return daily[date_str]


def _portfolio_history_order_date(order: dict) -> str:
    dt = _normalize_datetime(order.get("filled_at") or order.get("updated_at") or order.get("created_at"))
    if dt:
        return dt.strftime("%Y-%m-%d")
    raw = str(order.get("filled_at") or order.get("updated_at") or order.get("created_at") or "")[:10]
    return raw if len(raw) == 10 else ""


def _add_broker_fills_to_portfolio_history(daily: dict) -> dict:
    """Add realized broker fills to daily history using matched-fill net P/L."""
    try:
        orders = _normalize_filled_orders(delta.get_order_history())
    except Exception as exc:
        _logger.warning("Broker fills unavailable for portfolio history: %s", exc)
        return _portfolio_sync_meta("error", message=str(exc)[:200])

    realized_count = 0
    for order in orders:
        net_pnl = order.get("net_pnl")
        if net_pnl is None:
            continue
        date_str = _portfolio_history_order_date(order)
        if not date_str:
            continue
        net = _safe_float(net_pnl, 0.0)
        gross = _safe_float(order.get("gross_pnl"), net)
        fees = _safe_float(order.get("realized_fees"), _safe_float(order.get("fees"), 0.0))
        day = _portfolio_history_day(daily, date_str)
        day["real_pnl"] += net
        day["real_net_pnl"] += net
        day["real_gross_pnl"] += gross
        day["real_fees"] += fees
        day["real_trades"] += 1
        if net > 0:
            day["real_wins"] += 1
        realized_count += 1
    return _portfolio_sync_meta(
        "ok" if realized_count else "empty",
        order_count=len(orders),
        realized_count=realized_count,
        loaded=realized_count > 0,
        message=(
            f"{realized_count} realized broker fills loaded into the calendar."
            if realized_count
            else "No realized broker fills were available for the calendar."
        ),
    )


def _portfolio_extreme_period(values: dict, value_key: str, *, prefer_max: bool) -> dict | None:
    rows = [
        (key, data) for key, data in values.items() if isinstance(data, dict) and int(data.get("trades", 0) or 0) > 0
    ]
    if not rows:
        return None
    key, data = (
        max(rows, key=lambda item: _safe_float(item[1].get(value_key), 0.0))
        if prefer_max
        else min(rows, key=lambda item: _safe_float(item[1].get(value_key), 0.0))
    )
    return {
        "period": key,
        "pnl": round(_safe_float(data.get(value_key), 0.0), 2),
        "trades": int(data.get("trades", 0) or 0),
    }


def _portfolio_history_analytics(daily: dict, monthly: dict, yearly: dict, broker_sync: dict) -> dict:
    trades = sum(int(data.get("trades", 0) or 0) for data in daily.values())
    wins = sum(int(data.get("wins", 0) or 0) for data in daily.values())
    losses = max(trades - wins, 0)
    real_pnl = sum(_safe_float(data.get("real_pnl"), 0.0) for data in daily.values())
    paper_pnl = sum(_safe_float(data.get("paper_pnl"), 0.0) for data in daily.values())
    fees = sum(_safe_float(data.get("real_fees"), 0.0) for data in daily.values())
    total_pnl = real_pnl + paper_pnl
    day_count = sum(1 for data in daily.values() if int(data.get("trades", 0) or 0) > 0)
    active_year = max(yearly.keys()) if yearly else ""
    active_year_data = yearly.get(active_year, {}) if active_year else {}
    return {
        "total_pnl": round(total_pnl, 2),
        "real_pnl": round(real_pnl, 2),
        "paper_pnl": round(paper_pnl, 2),
        "real_fees": round(fees, 2),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / trades * 100) if trades else 0, 2),
        "avg_trade_pnl": round((total_pnl / trades) if trades else 0, 2),
        "avg_day_pnl": round((total_pnl / day_count) if day_count else 0, 2),
        "best_day": _portfolio_extreme_period(daily, "pnl", prefer_max=True),
        "worst_day": _portfolio_extreme_period(daily, "pnl", prefer_max=False),
        "best_month": _portfolio_extreme_period(monthly, "total_pnl", prefer_max=True),
        "worst_month": _portfolio_extreme_period(monthly, "total_pnl", prefer_max=False),
        "active_year": active_year,
        "active_year_pnl": round(_safe_float(active_year_data.get("total_pnl"), 0.0), 2),
        "broker_realized_loaded": bool(broker_sync.get("loaded")),
        "source": "broker_fills_plus_paper_runs" if broker_sync.get("loaded") else "saved_runs",
    }


@app.get("/api/portfolio/history")
async def get_portfolio_history():
    """Return combined historical P&L from real trades + paper runs for monthly/yearly charts."""
    try:
        daily = {}
        broker_sync = _add_broker_fills_to_portfolio_history(daily)
        broker_real_loaded = bool(broker_sync.get("loaded"))
        runs = _load_runs()
        seen_trade_signatures = set()
        for r in runs:
            mode = r.get("mode", "backtest")
            if mode not in ("paper", "live"):
                continue
            if mode == "live" and broker_real_loaded:
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
                    pnl = _safe_float(t.get("net_pnl", t.get("pnl", 0)), 0.0)
                    by_date[t_date]["pnl"] += pnl
                    by_date[t_date]["count"] += 1
                    if pnl > 0:
                        by_date[t_date]["wins"] += 1
                for d, data in by_date.items():
                    day = _portfolio_history_day(daily, d)
                    if mode == "live":
                        day["real_pnl"] += round(data["pnl"], 2)
                        day["real_net_pnl"] += round(data["pnl"], 2)
                        day["real_trades"] += data["count"]
                        day["real_wins"] += data["wins"]
                    else:
                        day["paper_pnl"] += round(data["pnl"], 2)
                        day["paper_trades"] += data["count"]
                        day["paper_wins"] += data["wins"]
            elif run_date:
                day = _portfolio_history_day(daily, run_date)
                if mode == "live":
                    pnl = _safe_float(r.get("total_pnl"), 0.0)
                    day["real_pnl"] += pnl
                    day["real_net_pnl"] += pnl
                    day["real_trades"] += r.get("trade_count", 0)
                else:
                    day["paper_pnl"] += _safe_float(r.get("total_pnl"), 0.0)
                    day["paper_trades"] += r.get("trade_count", 0)

        for data in daily.values():
            for key in ("real_pnl", "real_net_pnl", "real_gross_pnl", "real_fees", "paper_pnl"):
                data[key] = round(data.get(key, 0), 2)
            data["pnl"] = round(data["real_pnl"] + data["paper_pnl"], 2)
            data["trades"] = int(data["real_trades"] + data["paper_trades"])
            data["wins"] = int(data["real_wins"] + data["paper_wins"])
            data["sources"] = []
            if data["real_trades"]:
                data["sources"].append("broker")
            if data["paper_trades"]:
                data["sources"].append("paper")

        monthly = {}
        yearly = {}
        for date_str, d in daily.items():
            ym = date_str[:7]
            y = date_str[:4]
            if ym not in monthly:
                monthly[ym] = {
                    "real_pnl": 0,
                    "real_net_pnl": 0,
                    "real_gross_pnl": 0,
                    "real_fees": 0,
                    "paper_pnl": 0,
                    "total_pnl": 0,
                    "trades": 0,
                    "wins": 0,
                }
            monthly[ym]["real_pnl"] += d["real_pnl"]
            monthly[ym]["real_net_pnl"] += d.get("real_net_pnl", d["real_pnl"])
            monthly[ym]["real_gross_pnl"] += d.get("real_gross_pnl", 0)
            monthly[ym]["real_fees"] += d.get("real_fees", 0)
            monthly[ym]["paper_pnl"] += d["paper_pnl"]
            monthly[ym]["total_pnl"] += d["real_pnl"] + d["paper_pnl"]
            monthly[ym]["trades"] += d["real_trades"] + d["paper_trades"]
            monthly[ym]["wins"] += d["real_wins"] + d["paper_wins"]
            if y not in yearly:
                yearly[y] = {
                    "real_pnl": 0,
                    "real_net_pnl": 0,
                    "real_gross_pnl": 0,
                    "real_fees": 0,
                    "paper_pnl": 0,
                    "total_pnl": 0,
                    "trades": 0,
                    "wins": 0,
                }
            yearly[y]["real_pnl"] += d["real_pnl"]
            yearly[y]["real_net_pnl"] += d.get("real_net_pnl", d["real_pnl"])
            yearly[y]["real_gross_pnl"] += d.get("real_gross_pnl", 0)
            yearly[y]["real_fees"] += d.get("real_fees", 0)
            yearly[y]["paper_pnl"] += d["paper_pnl"]
            yearly[y]["total_pnl"] += d["real_pnl"] + d["paper_pnl"]
            yearly[y]["trades"] += d["real_trades"] + d["paper_trades"]
            yearly[y]["wins"] += d["real_wins"] + d["paper_wins"]

        for m in monthly.values():
            for k in ["real_pnl", "real_net_pnl", "real_gross_pnl", "real_fees", "paper_pnl", "total_pnl"]:
                m[k] = round(m[k], 2)
            m["pnl"] = m["total_pnl"]
        for y_val in yearly.values():
            for k in ["real_pnl", "real_net_pnl", "real_gross_pnl", "real_fees", "paper_pnl", "total_pnl"]:
                y_val[k] = round(y_val[k], 2)
            y_val["pnl"] = y_val["total_pnl"]

        analytics = _portfolio_history_analytics(daily, monthly, yearly, broker_sync)
        return {
            "status": "success",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "currency": _portfolio_currency_meta(),
            "broker_sync": broker_sync,
            "analytics": analytics,
            "daily": daily,
            "monthly": monthly,
            "yearly": yearly,
        }
    except Exception as e:
        _logger.error("Portfolio history error: %s", e)
        return {
            "status": "error",
            "message": str(e),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "currency": _portfolio_currency_meta(),
            "broker_sync": _portfolio_sync_meta("error", message=str(e)[:200]),
            "analytics": {},
            "daily": {},
            "monthly": {},
            "yearly": {},
        }


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
        for root, _, names in os.walk(_CACHE_DIR):
            for filename in names:
                path = os.path.join(root, filename)
                rel_path = os.path.relpath(path, _CACHE_DIR)
                size_mb = os.path.getsize(path) / 1024 / 1024
                files.append({"file": rel_path, "size_mb": round(size_mb, 2)})
    return {"cache_dir": _CACHE_DIR, "files": files}


@app.delete("/api/cache")
async def clear_cache():
    """Clear all cached candle data."""
    cleared = 0
    if os.path.exists(_CACHE_DIR):
        for root, _, names in os.walk(_CACHE_DIR):
            for filename in names:
                os.remove(os.path.join(root, filename))
                cleared += 1
    return {"cleared": cleared}


@app.get("/api/funding/{symbol}")
async def get_funding_rates(symbol: str):
    """Get funding rate history."""
    try:
        get_funding_history = getattr(delta, "get_funding_history", None)
        rates = get_funding_history(symbol) if callable(get_funding_history) else []
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


@app.get("/api/scalp/diagnostics")
async def scalp_diagnostics(symbol: str = ""):
    return _scalp_diagnostics_payload(symbol)


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
    explicit_order_type = "order_type" in body or "entry_order_type" in body
    order_type = _parse_scalp_order_type(
        body,
        entry_stop_price=entry_stop_price,
        entry_limit_price=entry_limit_price,
    )
    if explicit_order_type and order_type == "market":
        entry_stop_price = 0.0
        entry_limit_price = 0.0
    trail_mode = _parse_scalp_trail_mode(body)
    trail_value = _parse_float_field(body, "trail_value", "trail_amount", "trailing_value", default=0.0, min_value=0.0)
    target_price = _parse_float_field(body, "target_price", "tp_price", default=0.0, min_value=0.0)
    sl_price = _parse_float_field(body, "sl_price", default=0.0, min_value=0.0)
    legacy_size = _parse_int_field(body, "size", default=0, min_value=0)
    target_pct = _parse_float_field(body, "take_profit_pct", default=0.0, min_value=0.0)
    sl_pct = _parse_float_field(body, "stop_loss_pct", default=0.0, min_value=0.0)
    target_usd = _parse_float_field(body, "tp_usd", default=0.0, min_value=0.0)
    sl_usd = _parse_float_field(body, "sl_usd", default=0.0, min_value=0.0)
    if qty_value <= 0:
        raise HTTPException(status_code=400, detail="qty_value must be greater than zero")
    if order_type == "maker_only" and entry_limit_price <= 0:
        raise HTTPException(status_code=400, detail="entry_limit_price is required for Maker Only orders")
    if order_type in {"stop_market", "stop_limit", "take_profit_market", "take_profit_limit"} and entry_stop_price <= 0:
        raise HTTPException(
            status_code=400, detail="entry_stop_price is required as the trigger price for this order type"
        )
    if order_type in {"stop_limit", "take_profit_limit"} and entry_limit_price <= 0:
        raise HTTPException(status_code=400, detail="entry_limit_price is required for this order type")
    if order_type == "trailing_stop" and trail_value <= 0:
        raise HTTPException(status_code=400, detail="trail_value must be greater than zero for Trailing Stop orders")
    if legacy_size <= 0 and qty_mode != "base" and qty_value > 0:
        legacy_size = int(qty_value * leverage)

    entry_controls = eng.get_status(symbol).get("entry_controls", {})
    pending_requested = order_type != "market" or entry_stop_price > 0 or entry_limit_price > 0
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
            order_type=order_type,
            maker_only=order_type == "maker_only",
            trail_value=trail_value,
            trail_mode=trail_mode,
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
