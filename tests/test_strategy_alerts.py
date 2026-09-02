"""A strategy's own engine may speak — but only when real money moved.

Auto-Cascade_Fib and V-Rule were built deliberately mute: each was given
`on_update` alone, so `CascadeEngine._alert()` returned at its first line and
neither book could reach Telegram — no fill, no target, no failure. Correct for
a sandbox, wrong the moment a book trades real money, and that silence was the
thing standing between both strategies and going live (2026-09-02).

They now carry the same three callbacks the Cascade page has, with two limits
this file pins down:

  * paper stays silent, so a strategy that runs in paper day and night cannot
    bury the one alert worth waking up for;
  * a strategy's history never lands in the Cascade page's own closed list or
    event log — mixing those was one of the three things the sandbox split
    ended after the 2026-08-21 runaway.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402
from engine.auto_cascade_fib import STRATEGY as AUTO_FIB  # noqa: E402
from engine.cascade import Campaign, CascadeEngine  # noqa: E402


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


class FakeStore:
    def __init__(self):
        self.data = {}

    def get(self, bucket, key, default=None):
        return self.data.get((bucket, key), default)

    def put(self, bucket, key, value):
        self.data[(bucket, key)] = value


def _campaign(mode="live", strategy=AUTO_FIB, symbol="BTCUSDT", seq=12):
    return Campaign(
        campaign_id="c1",
        symbol=symbol,
        capital_usd=2000.0,
        mother_high=100.0,
        mother_low=90.0,
        mother_timestamp=0,
        mode=mode,
        strategy=strategy,
        seq=seq,
    )


def _engine(live_only, sent):
    return CascadeEngine(
        object(),
        on_alert=lambda title, body, level: sent.append((title, level)),
        alerts_live_only=live_only,
    )


# ── the engine gate ───────────────────────────────────────────────────────


def test_a_paper_campaign_on_a_strategy_engine_says_nothing():
    sent = []
    _engine(True, sent)._alert("Entry filled", "body", campaign=_campaign(mode="paper"))
    assert sent == []


def test_a_live_campaign_on_a_strategy_engine_is_announced_by_book():
    sent = []
    _engine(True, sent)._alert("Entry filled", "body", level="success", campaign=_campaign())
    assert sent == [("Cascade-Auto · BTCUSDT #12 — Entry filled", "success")]


def test_the_v_rule_book_names_itself_too():
    sent = []
    _engine(True, sent)._alert("TARGET hit", "body", campaign=_campaign(strategy="v-rule", symbol="SOLUSDT", seq=3))
    assert sent[0][0] == "V-Rule · SOLUSDT #3 — TARGET hit"


def test_an_engine_wide_alarm_stays_quiet_while_every_book_is_paper():
    """A stall matters because orders are resting on an exchange and nothing is
    stepping them. With every book in paper there is nothing to go wrong — and
    Cascade-Auto sits above MAX_ACTIVE_BEFORE_ALERT (10) on paper alone, so
    without this it would raise "Campaign count high" hourly about nothing."""
    sent = []
    eng = _engine(True, sent)
    eng.campaigns["c1"] = _campaign(mode="paper")
    eng._alert("Campaign count high", "14 campaigns are active", level="warn")
    assert sent == []


def test_an_engine_wide_alarm_fires_once_the_engine_holds_real_money():
    sent = []
    eng = _engine(True, sent)
    eng.campaigns["c1"] = _campaign(mode="paper")
    eng.campaigns["c2"] = _campaign(mode="live")
    eng._alert("Cascade engine STALLED", "no candles for 20 minutes", level="error")
    assert sent == [("Cascade engine STALLED", "error")]


def test_a_closed_live_campaign_does_not_keep_the_alarm_armed():
    sent = []
    eng = _engine(True, sent)
    done = _campaign(mode="live")
    done.closed_at = "2026-09-02 11:00:00"
    eng.campaigns["c1"] = done
    eng._alert("Cascade engine STALLED", "no candles", level="error")
    assert sent == []


def test_the_cascade_page_alarm_is_never_gated():
    """The live engine passes nothing, so an engine-wide alarm there fires
    exactly as it always has — including when it is running only paper."""
    sent = []
    eng = _engine(False, sent)
    eng.campaigns["c1"] = _campaign(mode="paper")
    eng._alert("Cascade engine STALLED", "no candles", level="error")
    assert sent == [("Cascade engine STALLED", "error")]


def test_the_cascade_page_keeps_announcing_its_paper_campaigns():
    """The flag is opt-in. The live engine passes nothing, so nothing changes
    for the book that has always announced paper and live alike."""
    sent = []
    _engine(False, sent)._alert("Entry filled", "body", campaign=_campaign(mode="paper", strategy=""))
    assert sent == [("Cascade-Hybrid · BTCUSDT #12 — Entry filled", "warn")]


def test_the_default_engine_is_not_live_only():
    assert CascadeEngine(object()).alerts_live_only is False


# ── the event log line carries its mode ───────────────────────────────────


def test_an_event_says_whether_money_moved():
    seen = []
    eng = CascadeEngine(object(), on_event=seen.append)
    eng._log_event(_campaign(mode="paper"), "start", "Campaign armed")
    assert seen[0]["mode"] == "paper"
    assert seen[0]["strategy"] == AUTO_FIB
    assert seen[0]["seq"] == 12


def test_an_engine_wide_event_has_no_mode():
    seen = []
    CascadeEngine(object(), on_event=seen.append)._log_event(None, "error", "boom")
    assert seen[0]["mode"] is None


# ── the app-side callbacks ────────────────────────────────────────────────


def test_a_paper_event_raises_no_notification(monkeypatch):
    rec = Recorder(monkeypatch)
    app_module._strategy_event(
        {"level": "start", "symbol": "BTCUSDT", "message": "m", "campaign_id": "c1", "mode": "paper"}
    )
    assert rec.push == []


def test_a_live_event_does(monkeypatch):
    rec = Recorder(monkeypatch)
    app_module._strategy_event(
        {
            "level": "start",
            "symbol": "SOLUSDT",
            "message": "m",
            "campaign_id": "c1",
            "mode": "live",
            "strategy": AUTO_FIB,
            "seq": 40,
        }
    )
    assert rec.push[0][0] == "Cascade-Auto · SOLUSDT #40 — Campaign started"


def test_an_event_with_no_mode_is_treated_as_paper(monkeypatch):
    """A line written by an older build carries no mode. Silence is the safe
    reading: this callback is only ever reached from a strategy engine."""
    rec = Recorder(monkeypatch)
    app_module._strategy_event({"level": "start", "symbol": "BTCUSDT", "message": "m", "campaign_id": "c1"})
    assert rec.push == []


def _closed_row(**over):
    row = {
        "campaign_id": "abc123",
        "seq": 17,
        "symbol": "ETHUSDT",
        "strategy": AUTO_FIB,
        "close_reason": "tp_filled",
        "realized_pnl": 1.25,
        "mode": "live",
        "avg_entry_price": 3100.0,
        "tp_price": 3140.0,
    }
    row.update(over)
    return row


def test_a_paper_campaign_ending_is_not_announced(monkeypatch):
    rec = Recorder(monkeypatch)
    app_module._strategy_closed(_closed_row(mode="paper"))
    assert rec.telegram == []
    assert rec.push == []


def test_a_live_campaign_ending_is_announced_with_its_book_and_money(monkeypatch):
    rec = Recorder(monkeypatch)
    app_module._strategy_closed(_closed_row())
    assert rec.telegram[0] == ("Cascade-Auto · ETHUSDT #17 — TARGET closed +1.25 USD", "success")


def test_a_strategy_campaign_never_lands_in_the_cascade_page_closed_list(monkeypatch):
    """The Cascade page's closed list is its own. A strategy's history is
    persisted by that engine's on_update, into that engine's bucket."""
    Recorder(monkeypatch)
    store = FakeStore()
    monkeypatch.setattr(app_module, "_get_state_store", lambda: store)
    app_module._strategy_closed(_closed_row())
    assert store.data == {}


def test_the_cascade_page_still_persists_and_announces_its_own(monkeypatch):
    """The refactor that split the alert out must not have cost the live book
    either half."""
    rec = Recorder(monkeypatch)
    store = FakeStore()
    monkeypatch.setattr(app_module, "_get_state_store", lambda: store)
    app_module._cascade_persist_closed(_closed_row(strategy="", symbol="BTCUSDT", seq=424))
    saved = store.data[(app_module._BUCKET_CASCADE_CLOSED, "campaigns")]
    assert [row["campaign_id"] for row in saved] == ["abc123"]
    assert rec.telegram[0][0] == "Cascade-Hybrid · BTCUSDT #424 — TARGET closed +1.25 USD"
