"""Every Telegram headline names the book, the coin and the campaign.

2026-08-24, Phil: "in Telegram I am unable to get the instrument on the
headline", then "I need which strategy made profit printed on the telegram
headline". Three engines write into one chat, so "Campaign closed" on its own
could have come from any of them — and the alert that reports the money was
the one still sending a bare "Cascade Campaign Closed".
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402
from engine.auto_cascade_fib import STRATEGY as AUTO_FIB  # noqa: E402


class Recorder:
    """Captures both the phone alert and the in-app one."""

    def __init__(self, monkeypatch):
        self.telegram = []
        self.push = []
        monkeypatch.setattr(
            app_module.alerter,
            "alert",
            lambda title, body, level="error": self.telegram.append((title, level)),
        )
        monkeypatch.setattr(
            app_module,
            "_notify_push",
            lambda kind, title, body, **kw: self.push.append((title, kw.get("level"))),
        )


# ── the builder ───────────────────────────────────────────────────────────


def test_headline_names_the_book_the_coin_and_the_number():
    assert (
        app_module._cascade_headline("TARGET closed", "", "BTCUSDT", 424)
        == "Cascade-Hybrid · BTCUSDT #424 — TARGET closed"
    )
    assert (
        app_module._cascade_headline("Campaign started", AUTO_FIB, "ETHUSDT", 46)
        == "Cascade-Auto · ETHUSDT #46 — Campaign started"
    )
    assert app_module._cascade_headline("Entry filled", "v-rule", "SOLUSDT", 3) == "V-Rule · SOLUSDT #3 — Entry filled"


def test_missing_pieces_drop_out_rather_than_printing_a_placeholder():
    # No campaign number yet — no dangling "#None".
    assert (
        app_module._cascade_headline("Campaign started", "", "BTCUSDT", None)
        == "Cascade-Hybrid · BTCUSDT — Campaign started"
    )
    # An unknown rule keeps its own name rather than being relabelled.
    assert app_module._cascade_headline("x", "mystery-rule", "", None) == "mystery-rule — x"


# ── the alert that reports the money ──────────────────────────────────────


class FakeStore:
    def __init__(self):
        self.data = {}

    def get(self, bucket, key, default=None):
        return self.data.get((bucket, key), default)

    def put(self, bucket, key, value):
        self.data[(bucket, key)] = value


def _closed(monkeypatch, **over):
    rec = Recorder(monkeypatch)
    monkeypatch.setattr(app_module, "_get_state_store", FakeStore)
    row = {
        "campaign_id": "abc123",
        "seq": 424,
        "symbol": "BTCUSDT",
        "strategy": "",
        "close_reason": "tp_filled",
        "realized_pnl": 0.27,
        "mode": "live",
        "avg_entry_price": 76492.71,
        "tp_price": 77244.53,
    }
    row.update(over)
    app_module._cascade_persist_closed(row)
    return rec


def test_a_target_that_filled_says_which_book_earned_and_how_much(monkeypatch):
    rec = _closed(monkeypatch)
    title, level = rec.telegram[0]
    assert title == "Cascade-Hybrid · BTCUSDT #424 — TARGET closed +0.27 USD"
    # "target" is what earns the 🎯 in alerter._icon_for; "success" the ✅.
    assert "target" in title.lower()
    assert level == "success"


def test_the_sandbox_book_is_named_as_itself(monkeypatch):
    rec = _closed(monkeypatch, strategy=AUTO_FIB, seq=17, symbol="ETHUSDT")
    assert rec.telegram[0][0].startswith("Cascade-Auto · ETHUSDT #17 — TARGET closed")


def test_a_loss_prints_its_sign(monkeypatch):
    rec = _closed(monkeypatch, realized_pnl=-1.1)
    assert rec.telegram[0][0].endswith("TARGET closed -1.10 USD")


def test_a_break_with_nothing_bought_says_so_without_inventing_a_number(monkeypatch):
    rec = _closed(monkeypatch, close_reason="mother_broken", realized_pnl=None)
    title, level = rec.telegram[0]
    assert title == "Cascade-Hybrid · BTCUSDT #424 — Closed on mother break"
    # No amount invented for a campaign that never bought. ("USD" alone would
    # match the coin.)
    assert not re.search(r"[-+][\d,.]+ USD$", title)
    assert level == "warn"


def test_stopped_by_hand_is_labelled_as_such(monkeypatch):
    rec = _closed(monkeypatch, close_reason="stopped", realized_pnl=0.0)
    assert rec.telegram[0][0] == "Cascade-Hybrid · BTCUSDT #424 — Closed by hand +0.00 USD"


def test_the_in_app_alert_carries_the_same_headline(monkeypatch):
    rec = _closed(monkeypatch)
    assert rec.push[0][0] == rec.telegram[0][0]


def test_a_bad_pnl_value_does_not_lose_the_alert(monkeypatch):
    rec = _closed(monkeypatch, realized_pnl="not a number")
    assert rec.telegram[0][0] == "Cascade-Hybrid · BTCUSDT #424 — TARGET closed"


# ── started / stopped, which come through the event log ───────────────────


def test_the_event_alert_names_the_book_too(monkeypatch):
    rec = Recorder(monkeypatch)
    app_module._cascade_notify(
        {
            "level": "start",
            "symbol": "SOLUSDT",
            "message": "Campaign armed",
            "campaign_id": "c1",
            "strategy": AUTO_FIB,
            "seq": 40,
        }
    )
    assert rec.push[0][0] == "Cascade-Auto · SOLUSDT #40 — Campaign started"


def test_an_event_from_an_older_build_still_alerts(monkeypatch):
    """Events already on disk have no strategy field; they are the live book."""
    rec = Recorder(monkeypatch)
    app_module._cascade_notify({"level": "stop", "symbol": "BTCUSDT", "message": "m", "campaign_id": "c2"})
    assert rec.push[0][0] == "Cascade-Hybrid · BTCUSDT — Campaign stopped"
