"""A book must recognise its working line even when the label lies.

17-Sep-2026, Phil, from the Cascade-Auto page:

    "Campaign #359 is already running on PAXGUSDT from this exact mother
     candle (4,373.52 / 4,350.17). Stop or delete it first, or pick a
     different mother candle. - Why this error?"

He had not clicked anything. That string was the PAXGUSDT book's standing
`last_error`, written by the DRIVER, and the live book was deadlocked:

  1. #359 was running at 5m stamped `mc_kind="major"` and was NOT in
     `book.graduated`. A restart inherits its parent's mc_kind
     (engine/cascade.py _spawn_child), and that chain's parent had graduated,
     so every child came back down to 5m still wearing the major label.
  2. `_working_line_id` asked only `mc_kind == "minor"`, so it answered "no
     working line".
  3. The book therefore tried to seed one. `latest_swing_high` picked
     4,373.52 — the very candle #359 was sitting on.
  4. The engine refused it: duplicate mother.
  5. A duplicate-mother refusal is PERMANENT while the twin lives, but the
     seeder does not blacklist an anchor after an error ("it may be
     transient"), so the identical start was retried every cooldown for ever.

Nothing was at risk — #359 held no coin and placed no orders — but the book
could not start anything either. The fix recognises the working line by what
it is (live, not graduated, still below GRADUATE_TIMEFRAME) rather than by its
pill.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

import engine.auto_cascade_fib as auto_fib  # noqa: E402
from engine.auto_cascade_fib import GRADUATE_TIMEFRAME  # noqa: E402
from tests.test_auto_cascade_fib import FakeCampaign, FakeEngine, _book, _driver  # noqa: E402


@pytest.fixture(autouse=True)
def armed(monkeypatch):
    monkeypatch.setattr(auto_fib, "DRIVER_ARMED", True)


def test_a_5m_line_stamped_major_is_still_the_working_line():
    """THE DEADLOCK: the label said major, the timeframe said 5m."""
    engine = FakeEngine()
    driver = _driver(engine)
    engine.add(FakeCampaign("c359", mc_kind="major", timeframe="5m"))
    assert driver._working_line_id(_book(driver)) == "c359"


def test_a_line_that_really_graduated_is_not_the_working_line():
    """A line at 1h has genuinely become a major — the book may seed again."""
    engine = FakeEngine()
    driver = _driver(engine)
    engine.add(FakeCampaign("c307", mc_kind="major", timeframe=GRADUATE_TIMEFRAME))
    assert driver._working_line_id(_book(driver)) == ""


def test_a_line_the_book_recorded_as_graduated_is_not_the_working_line():
    """book.graduated is honoured even if the timeframe has not caught up."""
    engine = FakeEngine()
    driver = _driver(engine)
    engine.add(FakeCampaign("c1", mc_kind="minor", timeframe="5m"))
    book = _book(driver)
    book.graduated.append("c1")
    assert driver._working_line_id(book) == ""


def test_a_minor_at_5m_is_still_recognised():
    """The ordinary case must not regress."""
    engine = FakeEngine()
    driver = _driver(engine)
    engine.add(FakeCampaign("c1", mc_kind="minor", timeframe="5m"))
    assert driver._working_line_id(_book(driver)) == "c1"


def test_the_book_does_not_try_to_seed_over_its_own_line():
    """The whole point: no second start while a line is working.

    Without the fix the book seeds, the engine refuses it as a duplicate
    mother, and that refusal becomes the book's standing error.
    """
    from tests.test_autofib_guard_fails_closed import _rising_to_a_failed_high

    engine = FakeEngine(_rising_to_a_failed_high())
    driver = _driver(engine)
    engine.add(FakeCampaign("c359", mc_kind="major", timeframe="5m"))
    book = _book(driver)
    assert asyncio.run(driver._seed_working_line(book)) is False
    assert engine.started == [], "it tried to start a second line over the one it already had"
    assert "working" in book.note, f"the note should say it is working a line, got {book.note!r}"


def test_a_graduated_line_lets_a_fresh_one_start():
    """And the strategy must still get its fresh 5m line after graduation."""
    from tests.test_autofib_guard_fails_closed import _rising_to_a_failed_high

    engine = FakeEngine(_rising_to_a_failed_high())
    driver = _driver(engine)
    engine.add(FakeCampaign("c307", mc_kind="major", timeframe=GRADUATE_TIMEFRAME))
    assert asyncio.run(driver._seed_working_line(_book(driver))) is True
    assert engine.started, "a graduated major must not block the next working line"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))


def test_the_page_decides_graduated_the_way_the_engine_does():
    """22-Sep-2026: "Why it is showing graduated 1H even if it is at 5m?" —
    #459 BTCUSDT, a 33rd-generation restart at 5m that inherited "major". The
    pill must use the book's graduated list or the line's own timeframe, never
    mc_kind alone."""
    import re

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    js = open(os.path.join(root, "static", "cryptoforge-app.js"), encoding="utf-8").read()
    body = re.search(r"function _cfCascadeMcKindPill\(.*?\n\}\n", js, re.S).group(0)
    auto = body[body.index("owner === 'auto-cascade-fib'") :]
    auto = auto[: auto.index("if (owner) {")]
    assert "afBook.graduated" in auto
    assert "campaign.timeframe" in auto
    assert "if (!minor)" not in auto, "mc_kind alone must not decide GRADUATED"
