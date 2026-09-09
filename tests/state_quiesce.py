"""Stop the background writers before a test's temporary state dir is removed.

A test that points `app._STATE_DB_FILE` at a TemporaryDirectory shares that
directory with threads it did not start: the coalescing snapshot writer
(app._snapshot_writer_loop) wakes every few seconds and writes whatever the
engines have queued. Connections are opened and closed per operation, so
SQLite's -wal and -shm sidecars come and go — and a write that lands while
`rmtree` is walking the directory recreates them after the walk and before the
final `rmdir`, which then fails:

    OSError: [Errno 39] Directory not empty: '/tmp/tmpcdei1hnw'

That is how CI failed on 2026-09-09 in
test_cascade_routes.py::FeedPublisherWiringTests — on teardown, with every
assertion in the test already passed.

Register it so it runs BEFORE the directory goes. unittest runs cleanups in
reverse, so it is added AFTER the TemporaryDirectory's own cleanup:

    self._tmp = tempfile.TemporaryDirectory()
    self.addCleanup(self._tmp.cleanup)
    self.addCleanup(quiesce_state_writers, self.app_module._STATE_DB_FILE)

`ignore_cleanup_errors=True` would have hidden this instead of fixing it, and
with it every genuine "something is still writing here" would be hidden too.
"""

import app as app_module
import state_store


def quiesce_state_writers(db_path: str = "") -> None:
    """Drop queued snapshots, stop the writer thread, release the store."""
    # Dropped, not flushed. The writer persists to whatever _STATE_DB_FILE
    # says AT THE MOMENT IT WAKES, so flushing here — after a test has begun
    # tearing down — is how a test's fixture data would end up written into
    # the developer's real state database.
    with app_module._snapshot_cv:
        app_module._snapshot_pending.clear()

    app_module._stop_snapshot_writer()

    # _snapshot_stop is a module global that nothing resets, and
    # _ensure_snapshot_writer only starts a thread when none is alive. Leave it
    # set and the next test to queue a snapshot gets a writer that drains one
    # batch and exits — a different app from the one being tested.
    app_module._snapshot_stop = False
    app_module._snapshot_thread = None

    if db_path:
        state_store.close_json_store(db_path)
