"""The journal links a trade to whichever engine placed it.

Each strategy runs its own CascadeEngine now. A journal that only indexed the
live Cascade's left the strategies' real trades in the book with no chart and
no name (Phil, 2026-08-22).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402


class _Fill:
    def __init__(self, order_id):
        self.order_id = order_id


class _Round:
    def __init__(self, fills):
        self.fills = fills


class _Campaign:
    def __init__(self, cid, seq, fills, rounds=()):
        self.campaign_id = cid
        self.seq = seq
        self.all_fills = fills
        self.rounds = list(rounds)


class _Engine:
    def __init__(self, campaigns=(), closed=()):
        self.campaigns = {c.campaign_id: c for c in campaigns}
        self.closed_campaigns = list(closed)


@pytest.fixture
def engines(monkeypatch):
    monkeypatch.setattr(
        app_module, "_cascade_engine", _Engine([_Campaign("casc1", 11, [_Fill("100"), _Fill("PAPER")])]), raising=False
    )
    monkeypatch.setattr(
        app_module,
        "_auto_fib_engine",
        _Engine([_Campaign("af1", 22, [_Fill("200")], [_Round([_Fill("201")])])]),
        raising=False,
    )
    monkeypatch.setattr(
        app_module,
        "_vrule_engine",
        _Engine(
            closed=[
                {
                    "campaign_id": "vr1",
                    "seq": 33,
                    "all_fills": [{"order_id": "300"}],
                    "rounds": [{"fills": [{"order_id": "301"}]}],
                }
            ]
        ),
        raising=False,
    )


def test_every_engines_orders_are_indexed_with_its_strategy(engines):
    index = app_module._cascade_order_id_index()
    assert index["100"]["strategy"] == "Cascade"
    assert index["200"]["strategy"] == "Auto-Cascade_Fib"
    assert index["201"]["strategy"] == "Auto-Cascade_Fib"  # a closed round's fills too
    assert index["300"]["strategy"] == "V-Rule"  # from closed history
    assert index["301"]["strategy"] == "V-Rule"


def test_paper_fills_are_never_indexed(engines):
    """A paper fill has no exchange order behind it to match."""
    assert "PAPER" not in app_module._cascade_order_id_index()


def test_a_trade_is_linked_to_the_strategy_that_placed_it(engines):
    trades = [{"buy_order_ids": ["200"]}, {"buy_order_ids": ["301"]}, {"buy_order_ids": ["999"]}]
    app_module._link_trades_to_campaigns(trades)
    assert trades[0] == {"campaign_id": "af1", "campaign_seq": 22, "strategy": "Auto-Cascade_Fib"}
    assert trades[1] == {"campaign_id": "vr1", "campaign_seq": 33, "strategy": "V-Rule"}
    assert trades[2] == {}  # a hand-placed trade gets no chart and no strategy


def test_the_internal_key_is_always_stripped(engines):
    """buy_order_ids is plumbing; it must never reach the page."""
    trades = [{"buy_order_ids": ["100"]}, {"buy_order_ids": ["nope"]}]
    app_module._link_trades_to_campaigns(trades)
    assert all("buy_order_ids" not in t for t in trades)


def test_no_engines_yet_is_not_an_error(monkeypatch):
    """The journal must never be the thing that wakes a trading engine."""
    for attr in ("_cascade_engine", "_auto_fib_engine", "_vrule_engine"):
        monkeypatch.setattr(app_module, attr, None, raising=False)
    assert app_module._cascade_order_id_index() == {}
    trades = [{"buy_order_ids": ["1"]}]
    app_module._link_trades_to_campaigns(trades)
    assert trades == [{}]
