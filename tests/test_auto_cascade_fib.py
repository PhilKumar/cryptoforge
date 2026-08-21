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

from engine.auto_cascade_fib import (  # noqa: E402
    CAP_TIMEFRAME,
    STRATEGY,
    TP_FIB_LEVEL,
    AutoCascadeFib,
    Book,
    latest_swing_high,
)

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
    rounds: List[FakeRound] = field(default_factory=list)


@dataclass
class FakeCandle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float


class FakeEngine:
    def __init__(self, candles=None):
        self.campaigns: Dict[str, FakeCampaign] = {}
        self.capital_groups: Dict[str, float] = {}
        self.started: List[dict] = []
        self._candles = candles or []
        self.start_error: Optional[str] = None

    def add(self, campaign: FakeCampaign) -> FakeCampaign:
        self.campaigns[campaign.campaign_id] = campaign
        return campaign

    def set_capital_group(self, symbol, budget, exchange=""):
        self.capital_groups[f"{symbol}:{exchange}".lower()] = budget

    async def _fetch_closed_candles(self, symbol, since_ts, timeframe="5m"):
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
    driver = _driver(engine, mode="live")
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
    assert back.mode == "live"
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
