"""The born-broken guard must BLOCK when it cannot read the tape.

17-Sep-2026, Phil: "I am not getting the correct start of the PAXGUSDT trade as
it restarts continuously".

What actually happened: the live PAXGUSDT book anchored a line on a swing high
of 4,303.43 while the 1m tape was at ~4,357. The campaign was born already
broken, so it froze on its own mother immediately, spent its 15-minute
confirmation on candles that had closed hours earlier, restarted, and marched
through six generations replaying about eight hours of history — one generation
per monitor tick, each one a fresh campaign and two phone alerts.

`_latest_1m_high` exists precisely to stop that; its own docstring calls it
"the runaway's opening move". But it swallowed every exception and answered
None, and the caller read:

    if fresh_high is not None and anchor.high <= fresh_high:

so ONE failed 1m fetch turned the guard off and waved the trade through. The
bare `except` logged nothing, which is why prod showed a born-broken LIVE
campaign with no error anywhere near it.

Worse, the test double defaulted its 1m lane to empty — which is the same
None — so all 51 driver tests ran with the freshness check disabled and could
never have caught this.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

import engine.auto_cascade_fib as auto_fib  # noqa: E402
from tests.test_auto_cascade_fib import FakeCandle, FakeEngine, _book, _driver  # noqa: E402


@pytest.fixture(autouse=True)
def armed(monkeypatch):
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", True)


def _rising_to_a_failed_high():
    """A high at 110 that fails, then price falls back to 100 — an honest anchor."""
    rows = []
    for i in range(40):
        rows.append(FakeCandle(i * 300, 100.0, 100.5, 99.5, 100.0))
    rows.append(FakeCandle(40 * 300, 100.0, 110.0, 100.0, 109.0))  # the failed high
    for i in range(41, 60):
        rows.append(FakeCandle(i * 300, 100.0, 100.5, 99.5, 100.0))
    return rows


def test_an_honest_anchor_still_seeds():
    """The control: with a readable tape below the anchor, a line starts."""
    engine = FakeEngine(_rising_to_a_failed_high())
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is True
    assert engine.started, "an anchor above the tape is the whole point of the strategy"


def test_a_tape_that_cannot_be_read_blocks_the_start():
    """THE BUG: no 1m candles used to mean "skip the check"."""
    engine = FakeEngine(_rising_to_a_failed_high(), candles_1m=[], ticker_price=0.0)
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert engine.started == [], "a line was started without proving its anchor still stands"


def test_an_illiquid_pair_with_no_recent_1m_candle_still_trades():
    """The regression the strict version shipped, caught on prod within a minute.

    PAXG prints NO 1m candle in a minute with no trade, so "no candle in the
    last 10 minutes" is its NORMAL state. Demanding one blocked the live book
    for ever — a strategy that never trades. The ticker is the second
    observation, and it always answers.
    """
    engine = FakeEngine(_rising_to_a_failed_high(), candles_1m=[], ticker_price=100.0)
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is True
    assert engine.started, "an honest anchor must still seed when only the ticker can be read"


def test_the_ticker_alone_can_condemn_an_anchor():
    """No 1m candle, but the ticker is already above the anchor: still refused."""
    engine = FakeEngine(_rising_to_a_failed_high(), candles_1m=[], ticker_price=111.0)
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert engine.started == [], "the ticker had already passed the anchor"


def test_the_higher_of_the_two_observations_wins():
    """A stale 1m candle below the anchor must not excuse a ticker above it."""
    engine = FakeEngine(_rising_to_a_failed_high(), ticker_price=115.0)
    engine._candles_1m = [FakeCandle(0, 100.0, 100.5, 99.5, 100.0)]  # stale, below
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert engine.started == [], "either observation reaching the anchor breaks it"


def test_a_failing_1m_fetch_blocks_the_start():
    """Same guard, but the read raises instead of coming back empty."""
    engine = FakeEngine(_rising_to_a_failed_high())

    async def boom(symbol, since_ts, timeframe="5m", venue=None):
        if timeframe == "1m":
            raise TimeoutError("venue did not answer")
        return engine._candles

    engine._fetch_closed_candles = boom
    engine.ticker_price = None  # and the ticker is down too — nothing readable
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert engine.started == []


def test_the_refusal_says_why_and_does_not_blacklist_the_anchor():
    """A transient read failure must not burn a good anchor for ever."""
    engine = FakeEngine(_rising_to_a_failed_high(), candles_1m=[], ticker_price=0.0)
    driver = _driver(engine)
    book = _book(driver)
    asyncio.run(driver._seed_working_line(book))
    assert "tape" in book.note, f"the note must explain the refusal, got {book.note!r}"
    assert book.tried_anchors == [], "a tape failure is not the anchor's fault"


def test_the_anchor_is_seeded_once_the_tape_comes_back():
    """Blocking is a WAIT, not a stop: the next readable tick starts the line."""
    engine = FakeEngine(_rising_to_a_failed_high(), candles_1m=[], ticker_price=0.0)
    driver = _driver(engine)
    book = _book(driver)
    assert asyncio.run(driver._seed_working_line(book)) is False
    engine.ticker_price = 100.0
    last = engine._candles[-1]
    engine._candles_1m = [FakeCandle(last.timestamp, last.close, last.close, last.close, last.close)]
    book.next_seed_ts = 0  # the cooldown is not what is under test
    assert asyncio.run(driver._seed_working_line(book)) is True
    assert engine.started, "the line must start as soon as the anchor can be proven honest"


def test_an_anchor_the_tape_has_already_reached_is_still_refused():
    """The original guard must keep working — this is the PAXGUSDT case."""
    engine = FakeEngine(_rising_to_a_failed_high())
    # tape at 4,357 against an anchor of 4,303 — in miniature: 111 against 110
    engine._candles_1m = [FakeCandle(60 * 300, 111.0, 111.0, 110.5, 111.0)]
    driver = _driver(engine)
    assert asyncio.run(driver._seed_working_line(_book(driver))) is False
    assert engine.started == [], "an anchor below the tape is born broken"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
