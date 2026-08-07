"""
executor/singleton.py — only one of these per machine.

Irrelevant when you launch from a terminal, because you can see the one you
already started. It becomes essential the moment the program is an icon someone
double-clicks: people double-click twice, and they click again next week having
forgotten it is already in the menu bar.

**Two executors on one account is not a cosmetic bug.** They share a Binance
account and a `state/` directory but not a book, so both would collect the same
rungs, both would arm a stop against the same fall, and both would place. The
second one's `book.json` writes would land on top of the first's, so whichever
wrote last would decide what the other believed it held. That is double the
intended position and a corrupted record of it.

The lock is an exclusive `flock` on a file in the state directory, held for the
life of the process. Chosen over a pid file because the kernel releases it on
exit however the process died — a pid file left by a crash is indistinguishable
from one left by a running program, and the usual fix (check whether the pid is
alive) is wrong the first time that pid is reused.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

_log = logging.getLogger("cascade.executor.singleton")


class AlreadyRunning(Exception):
    """Another executor holds the lock. Message is for the buyer, not a log."""


class InstanceLock:
    """Held for the life of the process, released by the kernel however it ends."""

    def __init__(self, path: str):
        self.path = os.path.expanduser(path)
        self._handle = None

    def acquire(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        # Opened 'a' so taking the lock never truncates what is already there:
        # on Windows the open itself would fail against a running holder, and
        # emptying the file on the way to finding that out is a poor trade.
        handle = open(self.path, "a+", encoding="utf-8")
        try:
            _lock(handle)
        except OSError as exc:
            handle.close()
            raise AlreadyRunning(
                "Cascade is already running on this computer. Open its console from the "
                "menu-bar icon rather than starting a second one — two copies would place "
                "the same orders twice on the same account."
            ) from exc
        self._handle = handle
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(f"{os.getpid()}\n")
            handle.flush()
        except OSError:  # pragma: no cover - the pid is a courtesy, not the lock
            pass

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            _unlock(self._handle)
        except OSError:  # pragma: no cover - closing releases it anyway
            pass
        try:
            self._handle.close()
        finally:
            self._handle = None

    def __enter__(self) -> "InstanceLock":
        self.acquire()
        return self

    def __exit__(self, *_exc) -> None:
        self.release()


def _lock(handle) -> None:
    """Take an exclusive, non-blocking lock, on whichever platform this is."""
    try:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return
    except ImportError:
        pass
    import msvcrt  # pragma: no cover - Windows only

    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)


def _unlock(handle) -> None:
    try:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return
    except ImportError:
        pass
    import msvcrt  # pragma: no cover - Windows only

    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def lock_path(state_dir: str) -> str:
    return os.path.join(os.path.expanduser(state_dir), "running.lock")


def running_elsewhere(state_dir: str) -> Optional[int]:
    """The pid of the running instance, or None. Never used to decide the lock.

    Only for telling the buyer which program to look for. The lock decides
    whether we may run; asking about a pid is exactly the race this design
    avoids, and using the answer for anything else would reintroduce it.
    """
    try:
        with open(lock_path(state_dir), encoding="utf-8") as handle:
            return int((handle.read().strip() or "0")) or None
    except (OSError, ValueError):
        return None
