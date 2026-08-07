"""The menu-bar icon, and the thread swap underneath it.

Cocoa only pumps its event loop on the main thread, so giving the tray that
thread pushes the executor's asyncio loop onto another one. Everything below is
a consequence of that move rather than a fact about icons, which is why it is
tested without ever drawing one.
"""

import asyncio
import threading
import unittest

from executor.tray import BackgroundExecutor, Tray, run_with_tray


class FakeRuntime:
    def __init__(self, **status):
        self.opening_paused = False
        self._status = {
            "following": 2,
            "armed_exposure_usd": 0.0,
            "paused": False,
            "awaiting_confirmation": "",
        }
        self._status.update(status)

    def status(self):
        return dict(self._status, paused=self.opening_paused)

    def pause_opening(self):
        self.opening_paused = True

    def resume_opening(self):
        self.opening_paused = False


class FakeExecutor:
    def __init__(self, runtime=None):
        self.runtime = runtime
        self.stop_requested = threading.Event()

    def request_stop(self):
        self.stop_requested.set()


class FakeIcon:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


class BackgroundExecutorTests(unittest.TestCase):
    def test_it_runs_on_another_thread_and_reports_its_exit_code(self):
        where = {}

        def runner():
            where["thread"] = threading.current_thread().name
            return 0

        supervisor = BackgroundExecutor(FakeExecutor(), runner=runner)
        supervisor.start()
        self.assertTrue(supervisor.finished.wait(5))
        self.assertEqual(supervisor.exit_code, 0)
        self.assertNotEqual(where["thread"], threading.main_thread().name)

    def test_stop_asks_the_executor_and_waits_for_it(self):
        """The wait is not a courtesy. Shutdown cancels resting buys and puts a
        target on anything held; a host that exits without waiting turns the
        graceful path into the crash path the next start has to recover from."""
        executor = FakeExecutor()
        finished = []

        def runner():
            executor.stop_requested.wait(5)
            finished.append(True)
            return 0

        supervisor = BackgroundExecutor(executor, runner=runner)
        supervisor.start()
        supervisor.stop(timeout=5)
        self.assertEqual(finished, [True], "stop returned before shutdown had finished")

    def test_a_crash_is_kept_rather_than_swallowed(self):
        """A tray sitting in the menu bar over a dead executor is the worst of
        the available outcomes."""

        def runner():
            raise RuntimeError("the key set would not verify")

        supervisor = BackgroundExecutor(FakeExecutor(), runner=runner)
        supervisor.start()
        self.assertTrue(supervisor.finished.wait(5))
        self.assertIsInstance(supervisor.failure, RuntimeError)
        self.assertEqual(supervisor.exit_code, 1)


class MenuTests(unittest.TestCase):
    def _tray(self, runtime=None, **kw):
        executor = FakeExecutor(runtime)
        supervisor = BackgroundExecutor(executor, runner=lambda: 0)
        return Tray(executor, supervisor, **kw), executor, supervisor

    def test_before_the_first_tick_it_says_so(self):
        tray, _, supervisor = self._tray()
        supervisor.start()
        supervisor.finished.wait(5)
        self.assertEqual(tray.status_line(), "Stopped")

    def test_it_counts_what_is_being_watched(self):
        tray, _, supervisor = self._tray(FakeRuntime(following=3))
        supervisor._thread = threading.current_thread()  # "running"
        self.assertEqual(tray.status_line(), "Watching 3 campaigns")

    def test_money_that_could_fill_unwatched_is_the_headline(self):
        """The single most useful number in the product belongs where a buyer
        glances before closing a lid."""
        tray, _, supervisor = self._tray(FakeRuntime(armed_exposure_usd=42.5))
        supervisor._thread = threading.current_thread()
        self.assertIn("$42.50 could fill", tray.status_line())

    def test_a_paused_machine_says_exits_are_still_managed(self):
        tray, executor, supervisor = self._tray(FakeRuntime())
        supervisor._thread = threading.current_thread()
        executor.runtime.pause_opening()
        self.assertEqual(tray.status_line(), "Paused — exits still managed")

    def test_an_unconfirmed_wake_outranks_the_count(self):
        tray, _, supervisor = self._tray(FakeRuntime(awaiting_confirmation="Away for 9h."))
        supervisor._thread = threading.current_thread()
        self.assertEqual(tray.status_line(), "Waiting for you to review")

    def test_a_broken_tick_does_not_break_the_menu(self):
        class Exploding(FakeRuntime):
            def status(self):
                raise RuntimeError("no")

        tray, _, supervisor = self._tray(Exploding())
        supervisor._thread = threading.current_thread()
        self.assertEqual(tray.status_line(), "Running")

    def test_pause_toggles_both_ways(self):
        tray, executor, _ = self._tray(FakeRuntime())
        tray.toggle_pause()
        self.assertTrue(tray.is_paused())
        tray.toggle_pause()
        self.assertFalse(tray.is_paused())

    def test_pausing_before_there_is_a_runtime_does_nothing_rather_than_raising(self):
        tray, _, _ = self._tray(None)
        tray.toggle_pause()
        self.assertFalse(tray.is_paused())

    def test_open_console_opens_the_console(self):
        opened = []
        tray, _, _ = self._tray(None, console_url="http://127.0.0.1:7757", open_url=opened.append)
        tray.open_console()
        self.assertEqual(opened, ["http://127.0.0.1:7757"])

    def test_quit_winds_down_before_the_icon_disappears(self):
        """In that order. An icon that vanishes while orders are still being
        cancelled tells the buyer it is safe to close the lid at the one moment
        it is not."""
        order = []
        executor = FakeExecutor()

        def runner():
            executor.stop_requested.wait(5)
            order.append("wound down")
            return 0

        supervisor = BackgroundExecutor(executor, runner=runner)
        tray = Tray(executor, supervisor)
        icon = FakeIcon()
        icon.stop = lambda: order.append("icon gone")
        tray._icon = icon
        supervisor.start()
        tray.quit()
        self.assertEqual(order, ["wound down", "icon gone"])


class HostTests(unittest.TestCase):
    def test_the_tray_comes_down_when_the_executor_dies_on_its_own(self):
        """A bad config or a refused key set stops the executor. The icon must
        not stay in the menu bar advertising a program that is not there."""
        executor = FakeExecutor()
        supervisor = BackgroundExecutor(executor, runner=lambda: 2)

        class RecordingTray(Tray):
            def __init__(self, *args, **kw):
                super().__init__(*args, **kw)
                self._icon = FakeIcon()
                self.ran = threading.Event()

            def run(self):
                # Stands in for pystray's blocking run: returns once something
                # has asked the icon to stop.
                while not self._icon.stopped:
                    self.ran.wait(0.01)

        tray = RecordingTray(executor, supervisor)
        code = run_with_tray(executor, background=supervisor, tray=tray)
        self.assertEqual(code, 2)
        self.assertTrue(tray._icon.stopped)

    def test_a_crash_inside_the_executor_reaches_the_caller(self):
        executor = FakeExecutor()

        def runner():
            raise RuntimeError("boom")

        supervisor = BackgroundExecutor(executor, runner=runner)

        class QuietTray(Tray):
            def __init__(self, *args, **kw):
                super().__init__(*args, **kw)
                self._icon = FakeIcon()

            def run(self):
                while not self._icon.stopped:
                    threading.Event().wait(0.01)

        with self.assertRaises(RuntimeError):
            run_with_tray(executor, background=supervisor, tray=QuietTray(executor, supervisor))


class RequestStopTests(unittest.IsolatedAsyncioTestCase):
    """`Executor.request_stop` is what every off-thread caller goes through."""

    async def test_it_wakes_a_waiter_on_another_loop_thread(self):
        from executor.__main__ import Executor

        class Shell:
            pass

        shell = Shell()
        shell._stopping = asyncio.Event()
        shell._loop = asyncio.get_running_loop()

        done = asyncio.get_running_loop().create_future()

        async def waiter():
            await shell._stopping.wait()
            done.set_result(True)

        task = asyncio.ensure_future(waiter())
        await asyncio.sleep(0)
        # From a real other thread, which is the case that matters: setting an
        # asyncio.Event from off-loop can leave its waiter asleep.
        threading.Thread(target=lambda: Executor.request_stop(shell)).start()
        self.assertTrue(await asyncio.wait_for(done, timeout=5))
        await task

    async def test_before_there_is_a_loop_it_sets_the_event_directly(self):
        from executor.__main__ import Executor

        class Shell:
            pass

        shell = Shell()
        shell._stopping = asyncio.Event()
        shell._loop = None
        Executor.request_stop(shell)
        self.assertTrue(shell._stopping.is_set())


if __name__ == "__main__":
    unittest.main()
