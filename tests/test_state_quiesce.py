"""The teardown helper that keeps a temp state dir removable.

The race it prevents cannot be summoned on demand, so this pins the
properties that close it: no writer thread still running, nothing queued that
could be written after the path is restored, no cached store pointing at a
directory about to be deleted — and the writer left able to start again, so
the next test gets the same app everyone else is testing.
"""

import os
import tempfile
import threading
import time
import unittest

from state_quiesce import quiesce_state_writers

import app as app_module
import state_store


class QuiesceStopsTheWriterTests(unittest.TestCase):
    def setUp(self):
        self._orig_db = app_module._STATE_DB_FILE
        self.addCleanup(lambda: setattr(app_module, "_STATE_DB_FILE", self._orig_db))
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db = os.path.join(self._tmp.name, "state.db")
        app_module._STATE_DB_FILE = self.db
        self.addCleanup(quiesce_state_writers, self.db)

    def test_the_writer_thread_is_not_left_running(self):
        app_module._queue_runtime_snapshot("cascade_runtime", {"campaigns": []})
        # Give the thread a moment to be real before asking it to stop.
        for _ in range(50):
            if app_module._snapshot_thread and app_module._snapshot_thread.is_alive():
                break
            time.sleep(0.01)
        self.assertTrue(app_module._snapshot_thread.is_alive(), "the writer never started")

        quiesce_state_writers(self.db)
        names = [t.name for t in threading.enumerate() if t.is_alive()]
        self.assertNotIn("cascade-snapshot-writer", names, "the writer outlived the teardown")

    def test_a_queued_snapshot_is_dropped_not_flushed(self):
        """Flushing after teardown writes a test's fixture into the REAL db."""
        with app_module._snapshot_cv:
            app_module._snapshot_pending[("cascade_runtime", "current")] = {"campaigns": []}
        quiesce_state_writers(self.db)
        self.assertEqual(app_module._snapshot_pending, {})

    def test_the_store_for_that_path_is_released(self):
        state_store.get_json_store(self.db)
        resolved = os.path.abspath(self.db)
        self.assertIn(resolved, state_store._STORE_CACHE)
        quiesce_state_writers(self.db)
        self.assertNotIn(resolved, state_store._STORE_CACHE, "a store still points at the temp dir")

    def test_the_writer_can_start_again_afterwards(self):
        """_snapshot_stop is a module global that nothing else resets."""
        quiesce_state_writers(self.db)
        self.assertFalse(app_module._snapshot_stop, "the next test would get a writer that exits at once")
        app_module._queue_runtime_snapshot("cascade_runtime", {"campaigns": []})
        for _ in range(50):
            if app_module._snapshot_thread and app_module._snapshot_thread.is_alive():
                break
            time.sleep(0.01)
        self.assertTrue(app_module._snapshot_thread.is_alive(), "the writer could not be restarted")

    def test_it_is_safe_to_call_twice(self):
        quiesce_state_writers(self.db)
        quiesce_state_writers(self.db)

    def test_it_works_without_a_path(self):
        quiesce_state_writers()


class ClosingAStoreTests(unittest.TestCase):
    def test_closing_reports_whether_one_was_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "state.db")
            state_store.get_json_store(db)
            self.assertTrue(state_store.close_json_store(db))
            self.assertFalse(state_store.close_json_store(db), "a second close must not claim a store")

    def test_the_next_get_builds_a_fresh_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "state.db")
            first = state_store.get_json_store(db)
            state_store.close_json_store(db)
            self.assertIsNot(state_store.get_json_store(db), first)
            state_store.close_json_store(db)


if __name__ == "__main__":
    unittest.main()
