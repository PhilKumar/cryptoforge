"""Only one executor per machine.

Irrelevant from a terminal, where you can see the one you already started. It
becomes essential the moment the program is an icon: people double-click twice.
Two copies share a Binance account and a state directory but NOT a book, so
both collect the same rungs, both arm against the same fall, and both place —
double the intended position, on top of a book.json each is overwriting.
"""

import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

from executor.singleton import InstanceLock, lock_path, running_elsewhere


class InstanceLockTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = lock_path(self._dir.name)

    def test_the_first_holder_gets_it(self):
        with InstanceLock(self.path):
            self.assertTrue(os.path.exists(self.path))

    def test_a_second_holder_in_another_process_is_refused(self):
        """Another process, not another object — a lock that only works
        in-process protects nothing, since the whole point is a second launch."""
        script = textwrap.dedent(
            f"""
            import sys
            sys.path.insert(0, {os.getcwd()!r})
            from executor.singleton import AlreadyRunning, InstanceLock
            try:
                InstanceLock({self.path!r}).acquire()
            except AlreadyRunning:
                sys.exit(3)
            sys.exit(0)
            """
        )
        with InstanceLock(self.path):
            done = subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=60)
        self.assertEqual(done.returncode, 3, done.stderr.decode())

    def test_releasing_lets_the_next_one_in(self):
        lock = InstanceLock(self.path)
        lock.acquire()
        lock.release()
        InstanceLock(self.path).acquire()  # must not raise

    def test_the_lock_survives_a_crash_without_blocking_the_next_start(self):
        """Chosen over a pid file for exactly this. The kernel releases the
        lock however the process died, whereas a pid file left by a crash is
        indistinguishable from one left by a running program."""
        script = textwrap.dedent(
            f"""
            import os, sys
            sys.path.insert(0, {os.getcwd()!r})
            from executor.singleton import InstanceLock
            InstanceLock({self.path!r}).acquire()
            os._exit(1)          # killed, no cleanup, no release
            """
        )
        subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=60)
        InstanceLock(self.path).acquire()  # must not raise

    def test_taking_the_lock_does_not_empty_an_existing_file(self):
        with open(self.path, "w", encoding="utf-8") as handle:
            handle.write("99999\n")
        with InstanceLock(self.path):
            self.assertEqual(running_elsewhere(self._dir.name), os.getpid())

    def test_the_refusal_tells_the_buyer_what_to_do(self):
        with InstanceLock(self.path):
            script = f"import sys; sys.path.insert(0, {os.getcwd()!r})\nfrom executor.singleton import InstanceLock\nInstanceLock({self.path!r}).acquire()"
            done = subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=60)
        message = done.stderr.decode()
        self.assertIn("already running", message)
        self.assertIn("twice on the same account", message)

    def test_no_lock_file_means_nobody_is_running(self):
        self.assertIsNone(running_elsewhere(self._dir.name))

    def test_a_lock_file_with_rubbish_in_it_is_not_a_pid(self):
        with open(self.path, "w", encoding="utf-8") as handle:
            handle.write("not a number")
        self.assertIsNone(running_elsewhere(self._dir.name))


class AppleScriptQuotingTests(unittest.TestCase):
    """The crash dialog's message is an exception's text — arbitrary, and full
    of quotes, because exchange errors are. Interpolated raw, a stray `"` ends
    the AppleScript literal and the rest is parsed as code, in a program that
    holds a Keychain entry."""

    def _quote(self):
        from installer.app_entry import _as_applescript_string

        return _as_applescript_string

    def test_a_plain_string_is_just_quoted(self):
        self.assertEqual(self._quote()("plain"), '"plain"')

    def test_a_quote_cannot_end_the_literal_early(self):
        out = self._quote()('he said "no"')
        self.assertEqual(out, '"he said \\"no\\""')
        self.assertFalse(out[1:-1].replace('\\"', "").count('"'))

    def test_a_backslash_cannot_escape_the_escaping(self):
        """Backslashes first, or `\\"` becomes `\\\\"` and the quote is loose again."""
        self.assertEqual(self._quote()('a\\"b'), '"a\\\\\\"b"')

    def test_a_newline_stays_inside_the_string(self):
        self.assertNotIn("\n", self._quote()("line one\nline two"))


if __name__ == "__main__":
    unittest.main()
