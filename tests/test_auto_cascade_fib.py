"""Auto-Cascade_Fib's driver: the money rules and the one-working-line rule.

Driven against a stand-in engine rather than the real one, so these tests say
what the DRIVER decides — when to fold, what the wallet cap becomes, when a
line graduates and when a fresh one is anchored — without dragging a broker,
candles or order placement into it.
"""

import asyncio
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

import engine.auto_cascade_fib as auto_fib  # noqa: E402
from engine.auto_cascade_fib import (  # noqa: E402
    CAP_TIMEFRAME,
    STRATEGY,
    TP_FIB_LEVEL,
    AutoCascadeFib,
    Book,
    latest_swing_high,
)


@pytest.fixture(autouse=True)
def armed(monkeypatch):
    """The driver ships DISARMED after the 2026-08-21 runaway.

    These tests are about what it decides WHEN it is allowed to run, so they
    arm it. The tests that prove the kill switch itself turn it back off.
    """
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", True)


# ── stand-ins ─────────────────────────────────────────────────────


@dataclass
class FakeRound:
    pnl: float


@dataclass
class FakeCampaign:
    campaign_id: str
    symbol: str = "BTCUSDT"
    exchange: str = ""
    strategy: str = STRATEGY
    mc_kind: str = "minor"
    timeframe: str = "5m"
    state: str = "TRENDLINE_ACTIVE"
    spent_usd: float = 0.0
    mode: str = "paper"
    filled_base_qty: float = 0.0
    residual_base_qty: float = 0.0
    rounds: List[FakeRound] = field(default_factory=list)


@dataclass
class FakeCandle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float


class FakeBroker:
    """Stands in for PaperOnlyBroker: armed or not, keyed or not."""

    def __init__(self, live_armed=False, configured=True):
        self.live_armed = live_armed
        self._configured = configured
        self.display_name = "Fake"

    def _is_configured(self):
        return self.live_armed and self._configured


class FakeEngine:
    def __init__(self, candles=None, candles_1m=None, broker=None):
        self.campaigns: Dict[str, FakeCampaign] = {}
        self.capital_groups: Dict[str, float] = {}
        self.broker = broker or FakeBroker()
        self.started: List[dict] = []
        self._candles = candles or []
        # The 1m lane feeds the freshness guard. Empty means "no 1m data",
        # which skips the guard — most tests are not about it.
        self._candles_1m = candles_1m or []
        self.start_error: Optional[str] = None

    def add(self, campaign: FakeCampaign) -> FakeCampaign:
        self.campaigns[campaign.campaign_id] = campaign
        return campaign

    def set_capital_group(self, symbol, budget, exchange=""):
        self.capital_groups[f"{symbol}:{exchange}".lower()] = budget

    def venue_of(self, campaign):
        return str(getattr(campaign, "exchange", "") or "")

    async def _fetch_closed_candles(self, symbol, since_ts, timeframe="5m"):
        if timeframe == "1m":
            return self._candles_1m
        return self._candles

    async def start_campaign(self, **kwargs):
        if self.start_error:
            return {"error": self.start_error}
        self.started.append(kwargs)
        campaign = FakeCampaign(
            campaign_id=f"new{len(self.started)}",
            symbol=kwargs["symbol"],
            mc_kind=kwargs.get("mc_kind", "minor"),
        )
        self.add(campaign)
        return {"campaign_id": campaign.campaign_id}


def _driver(engine, **book_kwargs) -> AutoCascadeFib:
    driver = AutoCascadeFib(engine)
    driver.set_book("BTCUSDT", enabled=True, capital_usd=2000.0, **book_kwargs)
    return driver


def _book(driver) -> Book:
    return driver.books["btcusdt:"]


# ── the money ─────────────────────────────────────────────────────


def test_wallet_cap_is_half_the_purse():
    driver = _driver(FakeEngine())
    assert _book(driver).wallet_cap_usd == 1000.0


def test_profit_waits_in_the_pocket_until_it_is_worth_a_quarter():
    engine = FakeEngine()
    driver = _driver(engine)
    campaign = engine.add(FakeCampaign("c1"))
    book = _book(driver)

    campaign.rounds.append(FakeRound(pnl=100.0))  # a quarter of 2000 is 500
    driver._bank_and_fold(book)
    assert book.pocket_usd == 100.0
    assert book.purse_usd == 2000.0
    assert book.folds == 0

    campaign.rounds.append(FakeRound(pnl=400.0))  # pocket now 500 — folds
    driver._bank_and_fold(book)
    assert book.pocket_usd == 0.0
    assert book.purse_usd == 2500.0
    assert book.folds == 1
    assert book.wallet_cap_usd == 1250.0


def test_a_huge_round_folds_in_whole_leaving_nothing_behind():
    """Phil's rule folds the WHOLE pocket, not a quarter-sized slice of it. A
    round paying ten times the threshold still folds once, and the purse jumps
    by the full amount with no remainder left over."""
    engine = FakeEngine()
    driver = _driver(engine)
    campaign = engine.add(FakeCampaign("c1"))
    book = _book(driver)
    campaign.rounds.append(FakeRound(pnl=5000.0))
    driver._bank_and_fold(book)
    assert book.folds == 1
    assert book.purse_usd == 7000.0
    assert book.pocket_usd == 0.0


def test_the_same_round_is_never_banked_twice():
    engine = FakeEngine()
    driver = _driver(engine)
    campaign = engine.add(FakeCampaign("c1"))
    book = _book(driver)
    campaign.rounds.append(FakeRound(pnl=50.0))
    driver._bank_and_fold(book)
    driver._bank_and_fold(book)
    driver._bank_and_fold(book)
    assert book.pocket_usd == 50.0


def test_a_losing_round_reduces_the_pocket():
    engine = FakeEngine()
    driver = _driver(engine)
    campaign = engine.add(FakeCampaign("c1"))
    book = _book(driver)
    campaign.rounds.extend([FakeRound(pnl=80.0), FakeRound(pnl=-30.0)])
    driver._bank_and_fold(book)
    assert book.pocket_usd == 50.0


def test_the_capital_group_is_kept_at_the_wallet_cap():
    engine = FakeEngine()
    driver = _driver(engine)
    book = _book(driver)
    driver._apply_wallet_cap(book)
    assert engine.capital_groups["btcusdt:"] == 1000.0
    book.purse_usd = 4000.0
    driver._apply_wallet_cap(book)
    assert engine.capital_groups["btcusdt:"] == 2000.0


def test_resizing_after_a_fold_does_not_shrink_a_compounded_purse():
    engine = FakeEngine()
    driver = _driver(engine)
    book = _book(driver)
    book.purse_usd = 5000.0
    book.folds = 2
    driver.set_book("BTCUSDT", capital_usd=2000.0)
    assert book.purse_usd == 5000.0


# ── the working line ──────────────────────────────────────────────


def test_a_line_that_reaches_1h_becomes_a_major():
    engine = FakeEngine()
    driver = _driver(engine)
    campaign = engine.add(FakeCampaign("c1", mc_kind="minor", timeframe="1h"))
    assert driver._graduate(_book(driver)) is True
    assert campaign.mc_kind == "major"


def test_a_line_below_1h_is_left_alone():
    engine = FakeEngine()
    driver = _driver(engine)
    for tf in ("5m", "15m"):
        campaign = engine.add(FakeCampaign(f"c{tf}", mc_kind="minor", timeframe=tf))
        driver._graduate(_book(driver))
        assert campaign.mc_kind == "minor"


def test_a_fresh_line_is_anchored_when_none_is_working():
    candles = _rising_then_falling()
    engine = FakeEngine(candles)
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is True
    started = engine.started[0]
    assert started["mc_kind"] == "minor"
    assert started["timeframe"] == "5m"
    assert started["strategy"] == STRATEGY
    assert started["tp_fib_level"] == TP_FIB_LEVEL
    assert started["cap_timeframe"] == CAP_TIMEFRAME


def test_only_one_line_works_at_a_time():
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    engine.add(FakeCampaign("c1", mc_kind="minor", timeframe="5m"))
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert engine.started == []


def test_a_graduated_major_frees_the_slot():
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    engine.add(FakeCampaign("c1", mc_kind="minor", timeframe="1h"))
    book = _book(driver)
    driver._graduate(book)
    assert asyncio.run(driver._seed_working_line(book)) is True


def test_no_new_line_when_the_wallet_is_already_full():
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    engine.add(FakeCampaign("c1", mc_kind="major", timeframe="4h", spent_usd=1000.0))
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False


def test_another_strategys_campaigns_are_never_touched():
    """The live Cascade's campaigns share the engine. They must be invisible."""
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    hand = engine.add(FakeCampaign("hand", strategy="", mc_kind="minor", timeframe="1h"))
    book = _book(driver)
    driver._graduate(book)
    assert hand.mc_kind == "minor"  # untouched
    hand.rounds.append(FakeRound(pnl=999.0))
    driver._bank_and_fold(book)
    assert book.pocket_usd == 0.0  # its profit is not ours either
    assert asyncio.run(driver._seed_working_line(book)) is True  # slot still free


def test_a_start_failure_is_recorded_not_raised():
    engine = FakeEngine(_rising_then_falling())
    engine.start_error = "Binance API keys are not configured"
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert "Binance" in _book(driver).last_error


def test_a_disabled_book_does_nothing_at_all():
    engine = FakeEngine(_rising_then_falling())
    driver = AutoCascadeFib(engine)
    driver.set_book("BTCUSDT", enabled=False, capital_usd=2000.0)
    assert asyncio.run(driver.tick()) is False
    assert engine.started == []
    assert engine.capital_groups == {}


def test_state_survives_a_save_and_load():
    engine = FakeEngine()
    driver = _driver(engine)  # paper — the only mode the sandbox has
    book = _book(driver)
    book.purse_usd = 3300.0
    book.pocket_usd = 12.5
    book.folds = 3
    revived = AutoCascadeFib(FakeEngine())
    revived.load(driver.dump())
    back = revived.books["btcusdt:"]
    assert back.purse_usd == 3300.0
    assert back.pocket_usd == 12.5
    assert back.folds == 3
    assert back.mode == "paper"
    assert back.enabled is True


# ── the swing-high anchor ─────────────────────────────────────────


def _rising_then_falling() -> List[FakeCandle]:
    """A clean peak in the middle, with price below it at the end."""
    out = []
    for i in range(60):
        high = 100.0 + i
        out.append(FakeCandle(timestamp=i * 300, open=high - 1, high=high, low=high - 2, close=high - 1))
    for i in range(60):
        high = 159.0 - i
        out.append(FakeCandle(timestamp=(60 + i) * 300, open=high, high=high, low=high - 2, close=high - 1))
    return out


def test_the_anchor_is_a_confirmed_high_above_the_price():
    candles = _rising_then_falling()
    anchor = latest_swing_high(candles, candles[-1].close)
    assert anchor is not None
    assert anchor.high > candles[-1].close


def test_no_anchor_when_price_is_making_new_highs():
    candles = [FakeCandle(timestamp=i * 300, open=100 + i, high=101 + i, low=99 + i, close=100 + i) for i in range(60)]
    assert latest_swing_high(candles, candles[-1].close) is None


def test_the_anchor_is_never_the_newest_bars():
    """It has to be CONFIRMED, so the campaign starts strictly in the past."""
    candles = _rising_then_falling()
    anchor = latest_swing_high(candles, candles[-1].close)
    assert anchor.timestamp <= candles[-1 - 12].timestamp


def test_no_candles_no_anchor():
    assert latest_swing_high([], 100.0) is None


def test_the_status_counts_only_lines_still_running():
    """A closed campaign is history, not a running line.

    The page reads this number straight out, so counting the dead ones made it
    say "1 line running" over an empty panel.
    """
    engine = FakeEngine()
    driver = _driver(engine)
    engine.add(FakeCampaign("alive", state="TRENDLINE_ACTIVE"))
    engine.add(FakeCampaign("dead", state="MOTHER_BROKEN"))
    engine.add(FakeCampaign("done", state="COMPLETED"))
    row = driver.status()["books"][0]
    assert row["campaigns"] == 1


# ── the kill switch ───────────────────────────────────────────────


def test_a_disarmed_driver_starts_nothing_however_the_books_read(monkeypatch):
    """The exact 2026-08-21 failure: a book left On in saved state.

    It must not be able to wake the driver, because saved state is restored on
    every boot and there is no click involved.
    """
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    assert _book(driver).enabled is True
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", False)
    assert asyncio.run(driver.tick()) is False
    assert engine.started == []
    assert engine.capital_groups == {}


def test_a_disarmed_book_says_so_rather_than_looking_armed(monkeypatch):
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", False)
    asyncio.run(driver.tick())
    assert _book(driver).note == auto_fib.DISARMED_NOTE
    assert driver.status()["armed"] is False


def test_turning_a_book_on_while_disarmed_is_refused(monkeypatch):
    """Refused, not accepted-and-ignored — the page must never read On."""
    engine = FakeEngine()
    driver = AutoCascadeFib(engine)
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", False)
    with pytest.raises(ValueError, match="switched off"):
        driver.set_book("BTCUSDT", enabled=True, capital_usd=2000.0)
    assert driver.books == {}


def test_a_book_can_still_be_turned_OFF_while_disarmed(monkeypatch):
    """Whatever else is true, the off switch must always work."""
    engine = FakeEngine()
    driver = _driver(engine)
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", False)
    driver.set_book("BTCUSDT", enabled=False)
    assert _book(driver).enabled is False


# ── the 2026-08-21 runaway, closed for good ───────────────────────


def test_live_is_refused_when_the_server_is_not_armed():
    """A click must never be able to reach real money on its own."""
    engine = FakeEngine()  # its broker is not armed
    driver = AutoCascadeFib(engine)
    with pytest.raises(ValueError, match="switched off"):
        driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=2000.0)
    assert driver.books == {}


def test_a_used_anchor_is_never_seeded_twice():
    """The runaway's engine: the same dead high, re-anchored every cycle."""
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    book = _book(driver)
    assert asyncio.run(driver._seed_working_line(book)) is True
    first_anchor = engine.started[0]["mother_timestamp"]
    assert first_anchor in book.tried_anchors
    # The campaign dies instantly (as it did live) and the cooldown lapses.
    engine.campaigns.clear()
    book.next_seed_ts = 0.0
    started_again = asyncio.run(driver._seed_working_line(book))
    # Either an OLDER anchor was found, or nothing was — but never the same one.
    if started_again:
        assert engine.started[1]["mother_timestamp"] != first_anchor
    assert book.tried_anchors.count(first_anchor) == 1


def test_every_start_opens_a_full_bar_cooldown():
    """One start per closed 5m bar, however fast campaigns die."""
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    book = _book(driver)
    assert asyncio.run(driver._seed_working_line(book)) is True
    assert book.next_seed_ts > __import__("time").time()
    engine.campaigns.clear()  # instant death — the runaway's tempo
    assert asyncio.run(driver._seed_working_line(book)) is False
    assert book.note == "cooling down after the last start"
    assert len(engine.started) == 1


def test_an_anchor_the_1m_tape_has_reached_does_not_start():
    """The stale-anchor bug: 91.33 anchored while the 1m tape printed 91.38."""
    candles = _rising_then_falling()
    anchor = latest_swing_high(candles, candles[-1].close)
    hot_1m = [
        FakeCandle(timestamp=999_000, open=anchor.high, high=anchor.high + 0.05, low=anchor.high - 1, close=anchor.high)
    ]
    engine = FakeEngine(candles, candles_1m=hot_1m)
    driver = _driver(engine)
    book = _book(driver)
    assert asyncio.run(driver._seed_working_line(book)) is False
    assert engine.started == []
    assert "retested" in book.note
    assert book.tried_anchors == []  # not blacklisted — it may become honest again


def test_a_failed_start_cools_down_but_keeps_the_anchor():
    engine = FakeEngine(_rising_then_falling())
    engine.start_error = "venue hiccup"
    driver = _driver(engine)
    book = _book(driver)
    assert asyncio.run(driver._seed_working_line(book)) is False
    assert book.next_seed_ts > 0  # no retry storm at monitor pace
    assert book.tried_anchors == []  # transient errors do not burn the anchor


def test_the_paper_only_broker_refuses_orders_and_forwards_data():
    class RealBroker:
        broker_name = "binance"
        display_name = "Binance Spot"
        min_timeframe = "5m"

        def get_ticker(self, symbol):
            return {"symbol": symbol, "last": 100.0}

        def place_order(self, *a, **k):  # pragma: no cover — must never run
            raise AssertionError("the sandbox reached the exchange")

    from engine.auto_cascade_fib import PaperOnlyBroker

    sandbox = PaperOnlyBroker(RealBroker())
    assert sandbox.get_ticker("BTCUSDT")["last"] == 100.0
    assert sandbox._is_configured() is False
    with pytest.raises(RuntimeError, match="refused place_order"):
        sandbox.place_order("BTCUSDT", 1.0, "buy")
    with pytest.raises(RuntimeError, match="refused get_wallet"):
        sandbox.get_wallet()
    assert getattr(sandbox, "no_such_feature", None) is None  # unknowns stay missing


def test_tried_anchors_and_cooldown_survive_a_save_and_load():
    engine = FakeEngine(_rising_then_falling())
    driver = _driver(engine)
    book = _book(driver)
    asyncio.run(driver._seed_working_line(book))
    assert book.tried_anchors
    reloaded = AutoCascadeFib(FakeEngine())
    reloaded.load(driver.dump())
    twin = reloaded.books["btcusdt:"]
    assert twin.tried_anchors == book.tried_anchors
    assert twin.next_seed_ts == book.next_seed_ts


# ── live trading: the three locks, and the shared account ─────────


def _live_driver(monkeypatch, engine=None, capital=2000.0):
    monkeypatch.setattr(auto_fib, "LIVE_ARMED", True)
    engine = engine or FakeEngine(_rising_then_falling(), broker=FakeBroker(live_armed=True))
    driver = AutoCascadeFib(engine)
    driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=capital)
    return driver


def test_live_is_refused_when_the_keys_are_missing(monkeypatch):
    """Armed on the server is not enough — the venue must be able to trade."""
    monkeypatch.setattr(auto_fib, "LIVE_ARMED", True)
    engine = FakeEngine(broker=FakeBroker(live_armed=True, configured=False))
    driver = AutoCascadeFib(engine)
    with pytest.raises(ValueError, match="switched off"):
        driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=2000.0)


def test_live_is_accepted_when_every_lock_is_open(monkeypatch):
    driver = _live_driver(monkeypatch)
    assert _book(driver).mode == "live"
    assert driver.live_available() is True
    assert driver.status()["live_available"] is True


def test_a_live_purse_over_the_ceiling_is_refused(monkeypatch):
    monkeypatch.setattr(auto_fib, "LIVE_ARMED", True)
    monkeypatch.setattr(auto_fib, "LIVE_CEILING_USD", 2000.0)
    engine = FakeEngine(broker=FakeBroker(live_armed=True))
    driver = AutoCascadeFib(engine)
    with pytest.raises(ValueError, match="at most"):
        driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=50_000.0)


def test_the_ceiling_is_rechecked_at_every_start_not_only_when_saved(monkeypatch):
    """The purse grows by folding profit in, so a saved book can drift past."""
    driver = _live_driver(monkeypatch)
    book = _book(driver)
    monkeypatch.setattr(auto_fib, "LIVE_CEILING_USD", 100.0)  # as if lowered on the server
    assert asyncio.run(driver._seed_working_line(book)) is False
    assert "ceiling" in book.note
    assert driver.engine.started == []


def test_a_live_book_restored_while_disarmed_comes_back_as_paper(monkeypatch):
    """Saved state must never resume real trading the operator switched off."""
    driver = _live_driver(monkeypatch)
    dumped = driver.dump()
    assert dumped["books"][0]["mode"] == "live"
    monkeypatch.setattr(auto_fib, "LIVE_ARMED", False)
    revived = AutoCascadeFib(FakeEngine())
    revived.load(dumped)
    assert revived.books["btcusdt:"].mode == "paper"


def test_a_disarmed_broker_still_refuses_every_order():
    from engine.auto_cascade_fib import PaperOnlyBroker

    class Real:
        broker_name = "binance"
        display_name = "Binance Spot"
        min_timeframe = "5m"

        def _is_configured(self):
            return True

        def get_ticker(self, symbol):
            return {"last": 1.0}

        def place_order(self, *a, **k):  # pragma: no cover
            raise AssertionError("reached the exchange while disarmed")

    broker = PaperOnlyBroker(Real(), live_armed=False)
    assert broker._is_configured() is False
    with pytest.raises(RuntimeError, match="not armed for live"):
        broker.place_order("BTCUSDT", 1.0, "buy")
    assert broker.get_ticker("BTCUSDT")["last"] == 1.0


def test_an_armed_broker_forwards_orders_and_reports_configured():
    from engine.auto_cascade_fib import PaperOnlyBroker

    calls = []

    class Real:
        broker_name = "binance"
        display_name = "Binance Spot"
        min_timeframe = "5m"

        def _is_configured(self):
            return True

        def place_order(self, *a, **k):
            calls.append(a)
            return {"order_id": "1"}

    broker = PaperOnlyBroker(Real(), live_armed=True)
    assert broker._is_configured() is True
    assert broker.place_order("BTCUSDT", 1.0, "buy") == {"order_id": "1"}
    assert calls


def test_an_armed_broker_with_no_keys_is_still_not_configured():
    from engine.auto_cascade_fib import PaperOnlyBroker

    class Real:
        broker_name = "binance"
        display_name = "Binance Spot"
        min_timeframe = "5m"

        def _is_configured(self):
            return False

    assert PaperOnlyBroker(Real(), live_armed=True)._is_configured() is False


def test_the_strategy_declares_only_its_LIVE_coin_to_the_other_engine(monkeypatch):
    """Paper coin is imaginary. Declaring it would make the live Cascade think
    its own holding had vanished."""
    driver = _live_driver(monkeypatch)
    engine = driver.engine
    engine.add(FakeCampaign("live1", symbol="BTCUSDT", mode="live", filled_base_qty=0.5, residual_base_qty=0.1))
    engine.add(FakeCampaign("paper1", symbol="BTCUSDT", mode="paper", filled_base_qty=99.0))
    engine.add(FakeCampaign("other", symbol="SOLUSDT", mode="live", filled_base_qty=7.0))
    assert driver.claimed_base_qty("BTCUSDT") == pytest.approx(0.6)
    assert driver.claimed_base_qty("SOLUSDT") == pytest.approx(7.0)
    assert driver.claimed_base_qty("ETHUSDT") == 0.0
