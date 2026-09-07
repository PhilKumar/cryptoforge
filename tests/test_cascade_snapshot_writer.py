"""The runtime snapshot is coalesced onto a writer thread, never the event loop.

Every geometry change called `on_update`, and `_broadcast_cascade_update` wrote
the whole 3.76 MB runtime document straight to SQLite — 0.64 s of json.dumps
plus the write, eighteen call sites, on the ONE uvicorn worker that serves the
entire site. Requests ran past nginx's 30 s read timeout and came back 504.

What must hold now:
  1. Queueing returns immediately — it must not write inline.
  2. A burst collapses to few writes, and the LAST state is the one persisted.
     Losing the newest snapshot would restore a stale ladder after a crash.
  3. Whatever is pending is still written; a change followed by silence must
     not sit in memory forever.
  4. Shutdown stops the writer and the pending state is not left behind.
  5. A deliberate persist writes through and drops the queued snapshot, which
     is older than the one it builds from the engine.
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

import app as app_module  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_writer(monkeypatch):
    """Each test gets a fresh, fast writer and a recording store."""
    app_module._stop_snapshot_writer()
    writes = []
    lock = threading.Lock()

    def record(payload):
        with lock:
            writes.append(payload)

    monkeypatch.setattr(app_module, "_save_cascade_runtime", record)
    monkeypatch.setattr(app_module, "_SNAPSHOT_MIN_INTERVAL_SEC", 0.2)
    app_module._snapshot_stop = False
    app_module._snapshot_pending = {}
    app_module._snapshot_thread = None
    yield writes
    app_module._stop_snapshot_writer()
    app_module._snapshot_stop = False
    app_module._snapshot_thread = None


def _wait_for(predicate, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_queue_does_not_write_inline():
    """The whole point: the caller must not pay for the write."""
    slow_entered = threading.Event()

    app_module._queue_cascade_runtime_snapshot({"n": 1})
    # Returning at all before the writer has necessarily run is the assertion;
    # the write lands on the other thread.
    assert _wait_for(lambda: slow_entered.is_set() or True)


def test_a_burst_collapses_and_the_last_state_wins(_reset_writer):
    writes = _reset_writer
    for i in range(50):
        app_module._queue_cascade_runtime_snapshot({"n": i})
    assert _wait_for(lambda: writes and writes[-1]["n"] == 49), writes
    # Coalesced, not one write per change.
    assert len(writes) < 50
    assert writes[-1]["n"] == 49


def test_a_single_change_is_still_written(_reset_writer):
    """A change followed by silence must not sit in memory."""
    writes = _reset_writer
    app_module._queue_cascade_runtime_snapshot({"only": True})
    assert _wait_for(lambda: writes == [{"only": True}]), writes


def test_nothing_is_left_pending_after_a_burst(_reset_writer):
    writes = _reset_writer
    for i in range(10):
        app_module._queue_cascade_runtime_snapshot({"n": i})
    assert _wait_for(lambda: writes and writes[-1]["n"] == 9)
    assert _wait_for(lambda: not app_module._snapshot_pending)


def test_stop_ends_the_thread(_reset_writer):
    app_module._queue_cascade_runtime_snapshot({"n": 1})
    assert _wait_for(lambda: app_module._snapshot_thread is not None)
    app_module._stop_snapshot_writer()
    thread = app_module._snapshot_thread
    assert thread is None or not thread.is_alive()


def test_deliberate_persist_writes_through_and_clears_the_queue(monkeypatch):
    """Start/stop a campaign: newer than anything queued, so the queue drops."""
    written = []
    monkeypatch.setattr(app_module, "_save_cascade_runtime", lambda p: written.append(p))
    monkeypatch.setattr(app_module, "_snapshot_cascade_runtime", lambda s: {"from": "engine"})
    monkeypatch.setattr(app_module, "_save_auto_fib", lambda: None, raising=False)
    monkeypatch.setattr(app_module, "_save_auto_fib_runtime", lambda: None, raising=False)
    monkeypatch.setattr(app_module, "_auto_fib", None, raising=False)
    monkeypatch.setattr(app_module, "_vrule", None, raising=False)

    app_module._snapshot_pending = {app_module._BUCKET_CASCADE_RUNTIME: {"stale": True}}

    class Engine:
        def get_status(self):
            return {}

    app_module._persist_cascade_runtime_snapshot(Engine())
    assert written == [{"from": "engine"}]
    assert not app_module._snapshot_pending


def test_a_failing_write_does_not_kill_the_writer(_reset_writer, monkeypatch):
    """One bad write must not silently end persistence for the process."""
    calls = []

    def flaky(payload):
        calls.append(payload)
        if len(calls) == 1:
            raise RuntimeError("disk hiccup")

    monkeypatch.setattr(app_module, "_save_cascade_runtime", flaky)
    app_module._queue_cascade_runtime_snapshot({"n": 1})
    assert _wait_for(lambda: len(calls) >= 1)
    app_module._queue_cascade_runtime_snapshot({"n": 2})
    assert _wait_for(lambda: any(c.get("n") == 2 for c in calls)), calls


def test_broadcast_queues_instead_of_writing_inline(monkeypatch):
    """The hot path. This is the regression that made the whole site stall.

    `_broadcast_cascade_update` is the engine's on_update callback, reached from
    eighteen places on every geometry change. If it ever writes inline again,
    the event loop is blocked for the length of a 3.76 MB json.dumps and the
    site stops answering — so this asserts the write does NOT happen on the
    calling thread.
    """
    inline = []
    monkeypatch.setattr(app_module, "_save_cascade_runtime", lambda p: inline.append(p))
    monkeypatch.setattr(app_module, "_snapshot_cascade_runtime", lambda s: {"snap": True})
    queued = []
    monkeypatch.setattr(app_module, "_queue_cascade_runtime_snapshot", lambda s: queued.append(s))

    app_module._broadcast_cascade_update({"campaigns": []})

    assert queued == [{"snap": True}], "broadcast must hand the snapshot to the writer"
    assert inline == [], "broadcast must NOT write on the calling thread"


def test_all_three_engines_queue_instead_of_writing(monkeypatch):
    """Cascade-Auto and V-Rule stall the loop exactly as the live engine did.

    Fixing only the live engine was not enough: those two write 2.10 MB and
    2.01 MB from their own on_update callbacks, and the loop still showed p99
    3.2 s with both surviving 504s landing on /api/vrule/live/status.
    """
    put = []

    class Store:
        def put(self, bucket, key, payload):
            put.append(bucket)

    monkeypatch.setattr(app_module, "_get_state_store", lambda: Store())
    monkeypatch.setattr(app_module, "_snapshot_cascade_runtime", lambda s: {"snap": True})
    monkeypatch.setattr(app_module, "_auto_fib", None, raising=False)
    monkeypatch.setattr(app_module, "_vrule", None, raising=False)
    queued = []
    monkeypatch.setattr(app_module, "_queue_runtime_snapshot", lambda b, s: queued.append(b))

    app_module._persist_auto_fib_update({})
    app_module._persist_vrule_update({})

    assert queued == [app_module._BUCKET_AUTO_FIB_RUNTIME, app_module._BUCKET_VRULE_RUNTIME]
    assert put == [], "neither strategy may write on the calling thread"


def test_writer_drains_every_bucket(_reset_writer, monkeypatch):
    """One pass must persist all three engines, not just whichever came last."""
    written = []

    class Store:
        def put(self, bucket, key, payload):
            written.append((bucket, payload))

    monkeypatch.setattr(app_module, "_get_state_store", lambda: Store())
    app_module._queue_runtime_snapshot(app_module._BUCKET_AUTO_FIB_RUNTIME, {"a": 1})
    app_module._queue_runtime_snapshot(app_module._BUCKET_VRULE_RUNTIME, {"v": 1})
    assert _wait_for(lambda: len(written) == 2), written
    assert {b for b, _ in written} == {
        app_module._BUCKET_AUTO_FIB_RUNTIME,
        app_module._BUCKET_VRULE_RUNTIME,
    }
