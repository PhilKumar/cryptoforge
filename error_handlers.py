"""
error_handlers.py — AlgoForge Standardised Error Handling
==========================================================
Register with FastAPI in app.py:

    from error_handlers import register_error_handlers
    register_error_handlers(app)

Call AFTER app = FastAPI(...) and BEFORE any route definitions.
"""

from __future__ import annotations

import logging
import os
import traceback

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

_log = logging.getLogger("algoforge.errors")

# ── Friendly message map ───────────────────────────────────────────
# (title, user-facing message)
# Title is shown in the UI toast header.
# Message never contains technical detail.
_FRIENDLY: dict[int, tuple[str, str]] = {
    400: (
        "Bad Request",
        "The request couldn't be understood. Please check your inputs and try again.",
    ),
    401: (
        "Session Expired",
        "Your session has expired. Please log in again to continue.",
    ),
    403: (
        "Access Denied",
        "You don't have permission to perform this action.",
    ),
    404: (
        "Not Found",
        "The requested resource doesn't exist or has been removed.",
    ),
    405: (
        "Not Allowed",
        "This action isn't supported here.",
    ),
    408: (
        "Request Timeout",
        "The server took too long to respond. Please try again.",
    ),
    409: (
        "Conflict",
        "This action conflicts with the current state. Please refresh and try again.",
    ),
    422: (
        "Invalid Input",
        "One or more fields have invalid values. Please check your inputs.",
    ),
    429: (
        "Slow Down",
        "You're moving too fast! Please wait a moment before retrying.",
    ),
    500: (
        "Server Error",
        "Something went wrong on our end. Your positions and strategies are safe. "
        "If this persists, check the server logs.",
    ),
    502: (
        "Broker Unreachable",
        "The broker API is temporarily unreachable. Please try again in a moment.",
    ),
    503: (
        "Service Unavailable",
        "This feature is temporarily offline. Please try again shortly.",
    ),
    504: (
        "Broker Timeout",
        "The broker took too long to respond. Your order may or may not have been placed. "
        "Please check your positions before retrying.",
    ),
}

_DEFAULT_TITLE = "Unexpected Error"
_DEFAULT_MESSAGE = "An unexpected error occurred. Please refresh and try again."

_DEBUG = os.getenv("DEBUG", "false").lower() == "true"


def _build_response(
    status_code: int,
    *,
    detail: str = "",
    exc: Exception | None = None,
) -> dict:
    """
    Build a standardised error response dict.

    Shape:
        {
          "success": false,
          "error": {
            "code":    <int>,
            "title":   <str>,   # short label, safe to show as toast header
            "message": <str>,   # human sentence, safe to show in UI
            "detail":  <str>,   # present only for 4xx — the specific reason
            "debug":   <str>,   # present only when DEBUG=true
          }
        }
    """
    title, message = _FRIENDLY.get(status_code, (_DEFAULT_TITLE, _DEFAULT_MESSAGE))

    error: dict = {
        "code": status_code,
        "title": title,
        "message": message,
    }

    # 4xx errors are user-triggered — surface the specific reason
    if 400 <= status_code < 500 and detail:
        # Sanitise: strip raw Python repr chars that might confuse users
        safe_detail = str(detail).replace("<", "").replace(">", "").strip()
        if safe_detail and safe_detail.lower() not in ("none", "null", ""):
            error["detail"] = safe_detail

    # Debug mode: include full traceback on 5xx
    if _DEBUG and exc and status_code >= 500:
        error["debug"] = traceback.format_exc()

    return {"success": False, "error": error}


# ── Exception handlers ─────────────────────────────────────────────


async def _http_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """Catches all FastAPI / Starlette HTTPException raises."""
    _log.warning(
        "[%s] HTTP %d on %s %s — %s",
        getattr(request.state, "request_id", "-"),
        exc.status_code,
        request.method,
        request.url.path,
        exc.detail,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=_build_response(exc.status_code, detail=str(exc.detail or "")),
    )


async def _validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Catches Pydantic v2 request-body validation failures."""
    errors = exc.errors()
    if errors:
        first = errors[0]
        # loc is e.g. ("body", "initial_capital") — skip "body" prefix
        loc = " → ".join(str(part) for part in first.get("loc", [])[1:])
        msg = first.get("msg", "invalid value")
        detail = f"'{loc}': {msg}" if loc else msg
    else:
        detail = "Invalid request body"

    _log.warning(
        "[%s] Validation error on %s %s — %s",
        getattr(request.state, "request_id", "-"),
        request.method,
        request.url.path,
        detail,
    )
    return JSONResponse(
        status_code=422,
        content=_build_response(422, detail=detail),
    )


async def _generic_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catches any unhandled exception — last resort, never exposes internals."""
    _log.error(
        "[%s] Unhandled exception on %s %s",
        getattr(request.state, "request_id", "-"),
        request.method,
        request.url.path,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content=_build_response(500, exc=exc),
    )


# ── Public registration ────────────────────────────────────────────


def register_error_handlers(app: FastAPI) -> None:
    """
    Register all exception handlers on the FastAPI app.

    Usage in app.py:
        from error_handlers import register_error_handlers
        app = FastAPI(...)
        register_error_handlers(app)
    """
    app.add_exception_handler(StarletteHTTPException, _http_handler)
    app.add_exception_handler(RequestValidationError, _validation_handler)
    app.add_exception_handler(Exception, _generic_handler)
    _log.info("Error handlers registered (DEBUG=%s)", _DEBUG)
