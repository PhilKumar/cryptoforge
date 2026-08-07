"""
executor/tray.py — the thing a buyer can actually close the window on.

The console is a web page, which is the right shape for showing positions and
the wrong shape for *being* the program: closing a tab must not stop a machine
that is managing exits, and no buyer should have to keep a terminal open to
keep their orders watched. A menu-bar icon on macOS, a system-tray icon on
Windows, is what says "this is still running" without occupying a window.

**The whole architectural point of this file is which thread owns what.** Cocoa
will only pump its event loop on the process's main thread, so the tray has to
have it — which means the executor's asyncio loop moves to a worker thread. Two
consequences follow, and both are handled here rather than left to whoever
reads the stack trace later:

- `signal.signal` only installs from the main thread. `Executor.run()` tries and
  quietly gets a `ValueError` off-thread, so Ctrl-C and SIGTERM would do nothing
  at all. The handler is installed here instead, on the main thread, and routed
  through `Executor.request_stop`.
- Nothing on the menu may touch the loop directly. Every item goes through
  `request_stop` or through a `call_soon_threadsafe` hop, because an
  `asyncio.Event` set from the wrong thread can leave its waiter asleep.

pystray and Pillow are imported lazily and are optional. The command-line path
this folder shipped with does not need them, and a buyer running from source on
a headless box should not be stopped at import by a package for drawing icons.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import io
import logging
import signal
import threading
import webbrowser
from typing import Callable, Optional

from executor import pwa

_log = logging.getLogger("cascade.executor.tray")

# Big enough for a Retina menu bar and for Windows' 32px tray, small enough that
# generating it costs nothing at startup.
ICON_PX = 64


class TrayUnavailable(Exception):
    """No tray on this machine. Message is for the buyer, not a log."""


def _pystray():
    try:
        import pystray
        from PIL import Image
    except Exception as exc:  # pragma: no cover - depends on the buyer's machine
        raise TrayUnavailable(
            "This build has no menu-bar support installed (pystray and Pillow). "
            "It runs fine without one — the console is at the address printed above."
        ) from exc
    return pystray, Image


class BackgroundExecutor:
    """Runs the executor on its own thread so something else can own the main one.

    Deliberately thin. It owns the thread and the exit code and nothing else:
    every decision about what the executor does still belongs to the executor,
    and a supervisor that starts making them is a second place where the
    shutdown rules live.
    """

    def __init__(self, executor, runner: Optional[Callable] = None):
        self._executor = executor
        # Injectable so a test can drive the thread lifecycle without an event
        # loop, a feed, or an exchange.
        self._runner = runner or self._default_runner
        self._thread: Optional[threading.Thread] = None
        self.exit_code: Optional[int] = None
        self.failure: Optional[BaseException] = None
        self.finished = threading.Event()

    def _default_runner(self) -> int:
        import asyncio

        return asyncio.run(self._executor.run())

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="cascade-executor", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            self.exit_code = self._runner()
        except BaseException as exc:  # noqa: BLE001 - recorded, then re-raised to the host
            # Kept rather than printed. The tray is on another thread and would
            # otherwise sit there with a cheerful icon over a dead executor,
            # which is the worst of the available outcomes.
            self.failure = exc
            self.exit_code = 1
            _log.exception("the executor stopped with an error")
        finally:
            self.finished.set()

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def stop(self, timeout: float = 30.0) -> None:
        """Ask it to wind down and wait for the sleep invariants to finish.

        The wait is not a courtesy. Shutdown cancels resting buys and makes sure
        anything held has a target; a host that exits without waiting turns the
        graceful path into the crash path the next start has to recover from.
        """
        self._executor.request_stop()
        if self._thread:
            self._thread.join(timeout)


class Tray:
    """The icon, its menu, and the labels that keep it honest.

    The menu says what is true right now rather than what was true when the
    program started: a buyer glancing at it needs "is it watching?" answered,
    and an icon that means nothing is worse than no icon.
    """

    def __init__(self, executor, background: BackgroundExecutor, *, console_url: str = "", open_url=None):
        self._executor = executor
        self._background = background
        self._console_url = console_url
        self._open = open_url or webbrowser.open
        self._icon = None

    # ── what the menu asks the executor ──────────────────────────

    def status_line(self) -> str:
        """One line, top of the menu, not clickable."""
        if not self._background.running:
            return "Stopped"
        runtime = self._executor.runtime
        if runtime is None:
            return "Starting…"
        try:
            status = runtime.status()
        except Exception:  # pragma: no cover - a broken tick must not break the menu
            return "Running"
        if status.get("awaiting_confirmation"):
            return "Waiting for you to review"
        exposure = float(status.get("armed_exposure_usd") or 0.0)
        if status.get("paused"):
            return "Paused — exits still managed"
        following = status.get("following") or 0
        if exposure > 0:
            return f"Watching {following} · ${exposure:,.2f} could fill"
        return f"Watching {following} campaign{'s' if following != 1 else ''}"

    def is_paused(self) -> bool:
        runtime = self._executor.runtime
        return bool(runtime is not None and runtime.opening_paused)

    def toggle_pause(self) -> None:
        runtime = self._executor.runtime
        if runtime is None:
            return
        # Both are plain attribute writes on the runtime, which the tick reads
        # once a pass — the same path the web console's buttons already use, and
        # the reason they are bools rather than loop state.
        if runtime.opening_paused:
            runtime.resume_opening()
        else:
            runtime.pause_opening()

    def open_console(self) -> None:
        if self._console_url:
            self._open(self._console_url)

    def quit(self) -> None:
        """Wind down first, then take the icon away.

        In that order on purpose. Removing the icon first tells the buyer it has
        stopped while it is still cancelling orders, and the one moment they
        must not close the lid is the moment they have just been told it is safe
        to.
        """
        self._background.stop()
        if self._icon is not None:
            self._icon.stop()

    # ── the icon ─────────────────────────────────────────────────

    def build(self):
        """The pystray Icon, menu and all."""
        pystray, Image = _pystray()
        image = Image.open(io.BytesIO(pwa.icon(ICON_PX))).convert("RGBA")
        menu = pystray.Menu(
            # Disabled: a status line is a readout, and a menu entry that looks
            # pressable and does nothing reads as a broken button.
            pystray.MenuItem(lambda _item: self.status_line(), None, enabled=False),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Open console", lambda *_: self.open_console(), default=True),
            pystray.MenuItem(
                "Pause opening",
                lambda *_: self.toggle_pause(),
                checked=lambda _item: self.is_paused(),
            ),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit Cascade", lambda *_: self.quit()),
        )
        self._icon = pystray.Icon("cascade", image, "Cascade", menu)
        return self._icon

    def run(self) -> None:
        """Blocks on the calling thread, which must be the main one."""
        self.build().run()


def run_with_tray(executor, *, console_url: str = "", background=None, tray=None) -> int:
    """Run the executor under a tray icon. Returns its exit code.

    The signal handlers are installed HERE, on the main thread, because
    `Executor.run()` can no longer install them from the worker it now lives on
    — `signal.signal` raises off-thread and that raise is caught and ignored, so
    without this a SIGTERM would be swallowed in silence.
    """
    supervisor = background or BackgroundExecutor(executor)
    icon = tray or Tray(executor, supervisor, console_url=console_url)

    def _on_signal(_signum, _frame):
        icon.quit()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError):  # pragma: no cover - platform without it
            pass

    supervisor.start()
    # If the executor dies on its own — a bad config, a refused key set — the
    # tray must come down with it rather than sit in the menu bar advertising a
    # program that is not there.
    watcher = threading.Thread(target=_follow, args=(supervisor, icon), daemon=True, name="cascade-tray-watch")
    watcher.start()

    icon.run()
    supervisor.stop()
    if supervisor.failure is not None:
        raise supervisor.failure
    return int(supervisor.exit_code or 0)


def _follow(supervisor: BackgroundExecutor, icon) -> None:
    supervisor.finished.wait()
    try:
        if icon._icon is not None:
            icon._icon.stop()
    except Exception:  # pragma: no cover - the icon may already be gone
        pass
