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
from datetime import datetime
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


def alert(title: str, body: str, level: str = "error") -> None:
    """Fire-and-forget alert. Safe to call from any async context.

    Args:
        title: Short heading, e.g. "Order Failed"
        body:  Details — symbol, error message, etc.
        level: "error" | "warn" | "info"  (controls emoji prefix)
    """
    if not (_TELEGRAM_OK or _DISCORD_OK):
        return

    icon = {"error": "🔴", "warn": "🟡", "info": "🟢"}.get(level, "⚪")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # HTML for Telegram
    html = f"{icon} <b>[CryptoForge] {title}</b>\n" f"<code>{ts}</code>\n\n" f"{body}"
    # Plain for Discord
    plain = f"{icon} **[CryptoForge] {title}**\n`{ts}`\n\n{body}"

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_dispatch(html, plain))
    except RuntimeError:
        _log.debug("No event loop — alert skipped: %s", title)


async def shutdown() -> None:
    """Close the shared HTTP client. Call on app shutdown."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None
