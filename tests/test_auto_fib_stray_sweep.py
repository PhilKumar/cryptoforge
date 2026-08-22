"""The one-time sweep that takes Auto-Cascade_Fib's debris out of the live books.

Before 2026-08-21 the strategy started its campaigns inside the live engine.
The runaway that day left 55 dead paper campaigns in Phil's Closed Campaigns
with thousands of event rows behind them. This sweep removes them — and must
never remove anything of his.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402
from engine.auto_cascade_fib import STRATEGY  # noqa: E402


class FakeStore:
    def __init__(self, data):
        self.data = data

    def get(self, bucket, key, default=None):
        return self.data.get((bucket, key), default)

    def put(self, bucket, key, value):
        self.data[(bucket, key)] = value


def _seed(monkeypatch):
    data = {
        ("cascade_runtime", "current"): {
            "campaigns": [
                {"campaign_id": "hand1", "strategy": "", "state": "TRENDLINE_ACTIVE"},
                {"campaign_id": "bot1", "strategy": STRATEGY, "state": "MOTHER_BROKEN"},
                {"campaign_id": "bot2", "strategy": STRATEGY, "state": "STOPPED"},
            ],
            "closed_campaigns": [
                {"campaign_id": "hand2", "strategy": ""},
                {"campaign_id": "bot3", "strategy": STRATEGY},
            ],
            "capital_groups": {
                # written by the strategy: exactly half BTC's purse
                "binance:BTCUSDT": {"budget_usd": 1000.0, "exchange": "binance"},
                # Phil's own: not half of SOL's $500 purse
                "binance:SOLUSDT": {"budget_usd": 900.0, "exchange": "binance"},
                # a symbol with no book at all
                "binance:XRPUSDT": {"budget_usd": 250.0, "exchange": "binance"},
            },
        },
        ("cascade_closed", "campaigns"): [
            {"campaign_id": "hand2", "strategy": ""},
            {"campaign_id": "bot3", "strategy": STRATEGY},
            {"campaign_id": "bot1", "strategy": STRATEGY},
        ],
        ("auto_cascade_fib", "books"): {
            "books": [
                {"symbol": "BTCUSDT", "purse_usd": 2000.0},  # wallet cap 1000
                {"symbol": "SOLUSDT", "purse_usd": 500.0},  # wallet cap 250
            ]
        },
        ("cascade_events", "log"): [
            {"campaign_id": "hand1", "message": "real"},
            {"campaign_id": "bot1", "message": "debris"},
            {"campaign_id": "bot3", "message": "debris"},
            {"campaign_id": "hand2", "message": "real"},
        ],
    }
    store = FakeStore(data)
    monkeypatch.setattr(app_module, "_get_state_store", lambda: store)
    return store


def test_the_sweep_removes_only_the_strategys_own_campaigns(monkeypatch):
    store = _seed(monkeypatch)
    app_module._purge_stray_auto_fib_from_live()

    runtime = store.get("cascade_runtime", "current")
    assert [c["campaign_id"] for c in runtime["campaigns"]] == ["hand1"]
    assert [c["campaign_id"] for c in runtime["closed_campaigns"]] == ["hand2"]
    # Only the budget the strategy itself wrote goes.
    assert set(runtime["capital_groups"]) == {"binance:SOLUSDT", "binance:XRPUSDT"}
    assert [c["campaign_id"] for c in store.get("cascade_closed", "campaigns")] == ["hand2"]
    assert [e["campaign_id"] for e in store.get("cascade_events", "log")] == ["hand1", "hand2"]


def test_a_hand_started_campaign_is_never_swept(monkeypatch):
    """Empty strategy means Phil started it. That is most of his history."""
    store = _seed(monkeypatch)
    app_module._purge_stray_auto_fib_from_live()
    kept = {c["campaign_id"] for c in store.get("cascade_runtime", "current")["campaigns"]}
    assert "hand1" in kept


def test_the_sweep_is_idempotent(monkeypatch):
    """It runs on every boot, so the second run must change nothing."""
    store = _seed(monkeypatch)
    app_module._purge_stray_auto_fib_from_live()
    first = {k: list(v) if isinstance(v, list) else v for k, v in store.data.items()}
    app_module._purge_stray_auto_fib_from_live()
    assert store.data == first


def test_a_broken_store_never_stops_the_engine_coming_up(monkeypatch):
    """Debris is cosmetic; tidying it must never be able to block a boot."""

    class Exploding:
        def get(self, *_a, **_k):
            raise RuntimeError("store is down")

        def put(self, *_a, **_k):
            raise RuntimeError("store is down")

    monkeypatch.setattr(app_module, "_get_state_store", lambda: Exploding())
    app_module._purge_stray_auto_fib_from_live()  # must not raise


def test_only_the_budget_the_strategy_wrote_is_removed(monkeypatch):
    """It caps a book by setting the symbol's group to HALF the purse.

    While it shared the live engine it wrote those onto Phil's own campaigns —
    a $2,000 live BTC campaign silently limited to $1,000 of funding. Matched
    narrowly so a budget Phil sets by hand is never touched: the symbol must
    have a book AND the budget must equal that book's wallet cap exactly.
    """
    store = _seed(monkeypatch)
    app_module._purge_stray_auto_fib_from_live()
    groups = store.get("cascade_runtime", "current")["capital_groups"]
    assert "binance:BTCUSDT" not in groups  # the strategy's own cap
    assert groups["binance:SOLUSDT"]["budget_usd"] == 900.0  # not half of $500 — Phil's
    assert groups["binance:XRPUSDT"]["budget_usd"] == 250.0  # no book on this symbol


def test_a_budget_survives_when_the_books_cannot_be_read(monkeypatch):
    """A budget is money. If the books are unreadable, remove nothing."""
    store = _seed(monkeypatch)
    store.data[("auto_cascade_fib", "books")] = {"books": "not a list"}
    app_module._purge_stray_auto_fib_from_live()
    groups = store.get("cascade_runtime", "current")["capital_groups"]
    assert set(groups) == {"binance:BTCUSDT", "binance:SOLUSDT", "binance:XRPUSDT"}
