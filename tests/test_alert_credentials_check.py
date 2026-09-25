"""The app must notice a dead Telegram token by itself.

Phil, 25-Sep-2026: "now cryptoforge telegram alerts are not coming". The bot
token had been revoked six days earlier, and nothing anywhere said so — the
app only talks to Telegram when it has something to alert about, and no
alert-worthy event happened in between. So startup now asks Telegram whether
the token still works, and logs loudly when it does not.
"""

import asyncio
import logging

import alerter


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.content = b"x"

    def json(self):
        return self._payload


class _Client:
    def __init__(self, resp):
        self._resp = resp
        self.calls = []

    async def get(self, url):
        self.calls.append(url)
        if isinstance(self._resp, Exception):
            raise self._resp
        return self._resp


def _run(monkeypatch, resp, *, token="123:abc", chat="42"):
    client = _Client(resp)
    monkeypatch.setattr(alerter, "TELEGRAM_BOT_TOKEN", token)
    monkeypatch.setattr(alerter, "TELEGRAM_CHAT_ID", chat)
    monkeypatch.setattr(alerter, "_TELEGRAM_OK", bool(token and chat))
    monkeypatch.setattr(alerter, "_get_client", lambda: client)
    return asyncio.run(alerter.check_telegram_credentials()), client


def test_a_working_token_is_reported_with_the_bot_name(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="alerter"):
        result, _ = _run(monkeypatch, _Resp(200, {"ok": True, "result": {"username": "Cryptoforge_phil_bot"}}))
    assert result == {"ok": True, "bot": "Cryptoforge_phil_bot"}
    assert "Cryptoforge_phil_bot" in caplog.text


def test_a_revoked_token_is_shouted_about(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="alerter"):
        result, _ = _run(monkeypatch, _Resp(401, {"ok": False, "description": "Unauthorized"}))
    assert result["ok"] is False
    assert result["reason"] == "Unauthorized"
    assert any(r.levelno >= logging.ERROR for r in caplog.records)
    assert "NO ALERTS WILL REACH YOU" in caplog.text


def test_an_unconfigured_bot_says_so_and_asks_telegram_nothing(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="alerter"):
        result, client = _run(monkeypatch, _Resp(200, {"ok": True}), token="", chat="")
    assert result["ok"] is False
    assert client.calls == []
    assert "not configured" in caplog.text


def test_telegram_being_unreachable_does_not_raise(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="alerter"):
        result, _ = _run(monkeypatch, TimeoutError("timed out"))
    assert result["ok"] is False
    assert "Could not reach Telegram" in caplog.text


def test_the_check_never_sends_a_message_and_never_logs_the_token(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="alerter"):
        _, client = _run(monkeypatch, _Resp(200, {"ok": True, "result": {"username": "b"}}), token="987:SECRETVALUE")
    assert all("sendMessage" not in url for url in client.calls)
    assert "SECRETVALUE" not in caplog.text


def test_startup_runs_the_check():
    src = open("app.py", encoding="utf-8").read()
    body = src[src.index("async def _app_lifespan") : src.index("app = FastAPI(")]
    assert "alerter.check_telegram_credentials()" in body
