"""
alerter.py — Async Telegram & Discord alerting for CryptoForge.

Sends fire-and-forget notifications on broker failures, order errors,
and critical events. Non-blocking — never delays the API response.

Configure via .env:
  TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
  TELEGRAM_CHAT_ID=-100123456789
  DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

_log = logging.getLogger("alerter")

# ── Config (from env) ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "")

_TELEGRAM_OK = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
_DISCORD_OK = bool(DISCORD_WEBHOOK_URL)

# Shared async client — connection-pooled, reused across calls
_client: Optional[httpx.AsyncClient] = None

# In-flight sends. The event loop keeps only a weak reference to a task, so an
# alert nobody holds on to can be collected before it is delivered — and a
# dropped alert is invisible by definition: you find out by not being told.
_inflight: set = set()

# Alerts go to people, not servers.  Keep their clock aligned with the chart
# and Cascade event log even when the application host itself runs in UTC.
_IST = timezone(timedelta(hours=5, minutes=30))


def reload_from_env() -> None:
    """Refresh alert destinations after the runtime environment is updated."""
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DISCORD_WEBHOOK_URL, _TELEGRAM_OK, _DISCORD_OK

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
    _TELEGRAM_OK = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    _DISCORD_OK = bool(DISCORD_WEBHOOK_URL)


def _ist_timestamp() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S IST")


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=10, limits=httpx.Limits(max_connections=5))
    return _client


# ── Low-level senders ─────────────────────────────────────────────


async def _send_telegram(text: str) -> None:
    if not _TELEGRAM_OK:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = await _get_client().post(url, json=payload)
        if resp.status_code != 200:
            _log.warning("Telegram send failed: %s %s", resp.status_code, resp.text[:200])
    except Exception as e:
        _log.warning("Telegram error: %s", e)


async def _send_discord(text: str) -> None:
    if not _DISCORD_OK:
        return
    payload = {"content": text}
    try:
        resp = await _get_client().post(DISCORD_WEBHOOK_URL, json=payload)
        if resp.status_code not in (200, 204):
            _log.warning("Discord send failed: %s %s", resp.status_code, resp.text[:200])
    except Exception as e:
        _log.warning("Discord error: %s", e)


async def _dispatch(text_html: str, text_plain: str) -> None:
    """Send to all configured channels in parallel."""
    tasks = []
    if _TELEGRAM_OK:
        tasks.append(_send_telegram(text_html))
    if _DISCORD_OK:
        tasks.append(_send_discord(text_plain))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ── Public API ────────────────────────────────────────────────────


# What the alert IS, not merely how loud it is. Three coloured circles and a
# white one for anything unmapped told Phil nothing at a glance on a phone —
# a filled entry and a hit target both arrived as the same dot, and "success"
# was not in the map at all, so the two things that went RIGHT came through as
# the blank ⚪ (2026-08-24: "The red sphere and the white sphere... Make
# meaningful icons for target, entry and status and warning").
#
# Matched on the words the engine already uses in its own headlines, longest
# intent first, so a new alert inherits a sensible icon without being listed.
_INTENT_ICONS = (
    ("target", "🎯"),
    ("entry filled", "✅"),
    ("bought", "✅"),
    ("filled", "✅"),
    ("stalled", "🛑"),
    ("failed", "❌"),
    ("cancelled", "🚫"),
    ("blocked", "🚫"),
    ("missing", "🔍"),
    ("escalated", "⏫"),
    ("restarted", "🔄"),
    ("retired", "🏁"),
    ("count high", "📈"),
    ("truncated", "✂️"),
)
_LEVEL_ICONS = {"error": "🔴", "warn": "⚠️", "info": "ℹ️", "success": "✅"}


def _icon_for(title: str, level: str) -> str:
    """The icon says what happened; the level only says how loud."""
    lowered = str(title or "").lower()
    for word, icon in _INTENT_ICONS:
        if word in lowered:
            return icon
    return _LEVEL_ICONS.get(level, "ℹ️")


def alert(title: str, body: str, level: str = "error") -> None:
    """Fire-and-forget alert. Safe to call from any async context.

    Args:
        title: Short heading, e.g. "BTCUSDT #12 — TARGET hit"
        body:  Details — error message, prices, etc.
        level: "error" | "warn" | "info" | "success"
    """
    if not (_TELEGRAM_OK or _DISCORD_OK):
        return

    icon = _icon_for(title, level)
    ts = _ist_timestamp()

    # HTML for Telegram
    html = f"{icon} <b>[CryptoForge] {title}</b>\n<code>{ts}</code>\n\n{body}"
    # Plain for Discord
    plain = f"{icon} **[CryptoForge] {title}**\n`{ts}`\n\n{body}"

    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_dispatch(html, plain))
        _inflight.add(task)
        task.add_done_callback(_inflight.discard)
    except RuntimeError:
        _log.debug("No event loop — alert skipped: %s", title)


async def shutdown() -> None:
    """Close the shared HTTP client. Call on app shutdown."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None
