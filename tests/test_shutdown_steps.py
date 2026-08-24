"""The shutdown sequence's clock and cap.

2026-08-24: a forced deploy spent the whole 30s systemd allows inside
`_shutdown_runtime_engines()` and the worker was SIGKILLed mid-shutdown. The
journal showed "Waiting for application shutdown.", then a silent half minute,
then the kill — nothing said WHICH step was slow, because no step logged
anything.

These pin the two properties that fix: a step that hangs is abandoned and
NAMED rather than being allowed to spend the whole budget, and the one piece
of state that is only written at shutdown is written FIRST, before anything
that can hang.
"""

import asyncio
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402


def test_a_hanging_step_is_abandoned_and_named(caplog):
    """The cap is what keeps one bad step from costing the whole budget."""

    async def _never():
        await asyncio.sleep(60)

    with caplog.at_level(logging.WARNING):
        loop_ran = asyncio.run(_run_step("wedged step", _never, timeout=0.2))

    assert loop_ran is None  # it returns rather than raising
    assert "wedged step" in caplog.text
    assert "did not finish" in caplog.text


def test_a_step_that_raises_does_not_stop_the_ones_after_it(caplog):
    def _boom():
        raise RuntimeError("state store is gone")

    with caplog.at_level(logging.WARNING):
        asyncio.run(_run_step("broken step", _boom))

    assert "broken step" in caplog.text and "state store is gone" in caplog.text


def test_a_synchronous_step_runs_and_an_async_one_is_awaited():
    seen = []

    def _sync():
        seen.append("sync")

    async def _async():
        seen.append("async")

    asyncio.run(_run_step("sync", _sync))
    asyncio.run(_run_step("async", _async))
    assert seen == ["sync", "async"]


def test_the_runs_are_saved_before_anything_that_can_hang(monkeypatch):
    """`_shutdown_save_engines` writes runs.json and is the ONLY state a
    SIGKILL would lose — everything after it re-persists what is already on
    disk. So it must go first, and it must still run when a later step hangs."""
    order = []

    monkeypatch.setattr(app_module, "_shutdown_save_engines", lambda: order.append("save runs"))

    class WedgedScalp:
        async def shutdown(self):
            order.append("scalp")
            await asyncio.sleep(60)

    monkeypatch.setitem(app_module.__dict__, "_scalp_engine", WedgedScalp())
    for name in ("_cascade_engine", "_auto_fib_engine", "_vrule_engine"):
        monkeypatch.setitem(app_module.__dict__, name, None)
    monkeypatch.setitem(app_module.__dict__, "_rule3070_services", {})
    monkeypatch.setitem(app_module.__dict__, "_rule3070_service", None)

    original = app_module._shutdown_step

    async def _quick(name, call, timeout=6.0):
        return await original(name, call, timeout=0.2)

    monkeypatch.setattr(app_module, "_shutdown_step", _quick)
    asyncio.run(app_module._shutdown_runtime_engines())

    assert order == ["save runs", "scalp"]  # saved first, and the wedge did not prevent it


async def _run_step(name, call, timeout=6.0):
    return await app_module._shutdown_step(name, call, timeout=timeout)


@pytest.mark.parametrize("missing", ["_scalp_engine", "_cascade_engine", "_auto_fib_engine", "_vrule_engine"])
def test_an_engine_that_was_never_built_is_skipped(monkeypatch, missing):
    """Most deploys have never touched most of these engines."""
    for name in ("_scalp_engine", "_cascade_engine", "_auto_fib_engine", "_vrule_engine"):
        monkeypatch.setitem(app_module.__dict__, name, None)
    monkeypatch.setitem(app_module.__dict__, "_rule3070_services", {})
    monkeypatch.setitem(app_module.__dict__, "_rule3070_service", None)
    monkeypatch.setattr(app_module, "_shutdown_save_engines", lambda: None)
    asyncio.run(app_module._shutdown_runtime_engines())  # must not raise
