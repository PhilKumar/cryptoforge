"""
executor/power.py — keeping the machine awake, and knowing when it won't be.

`recovery.py` has the policy. This is the part that hears the OS say "I am
about to sleep" and holds it open long enough to act.

The three platforms are not equal, and pretending otherwise would be the bug:

| | Prevent idle sleep | Warning before forced sleep |
|---|---|---|
| macOS | IOPMAssertion | kIOMessageSystemWillSleep, ack within ~30s |
| Linux | systemd-inhibit --mode=delay | InhibitDelayMaxSec, default 5s |
| Windows | SetThreadExecutionState | PBT_APMSUSPEND, ~2s |

Two consequences shape everything here.

**Lid close cannot be prevented anywhere.** An idle-sleep assertion stops the
machine dozing off; it does not stop a person shutting the lid. What you get is
a WINDOW, not a veto — so the design is "do the cancels inside the window",
never "keep it awake".

**Windows cannot reliably cancel on suspend.** Two seconds is not enough for a
round trip to an exchange plus confirmation. So on Windows the invariants are
held CONTINUOUSLY instead: the execution-state lock is taken whenever any entry
rests, forced sleep is treated as equivalent to a crash, and the armed exposure
is surfaced more prominently — because there, that number is what the buyer is
actually relying on.

That asymmetry is disclosed in the UI rather than hidden. A Windows buyer is
running with less protection than a Mac buyer and deserves to know.
"""

from __future__ import annotations

import logging
import platform
import subprocess  # nosec B404 - fixed argv, no shell; see _spawn
from dataclasses import dataclass
from typing import Callable, Optional

_log = logging.getLogger("cascade.executor.power")


@dataclass(frozen=True)
class PlatformPower:
    name: str
    can_prevent_idle_sleep: bool
    suspend_warning_sec: float
    # Whether the suspend window is long enough to cancel orders in. Below
    # this, the invariants have to be held continuously instead.
    can_cancel_on_suspend: bool
    note: str


MACOS = PlatformPower(
    "macOS",
    can_prevent_idle_sleep=True,
    suspend_warning_sec=30.0,
    can_cancel_on_suspend=True,
    note="Comfortable: about 30 seconds' warning before sleep, which is plenty to cancel entries.",
)
LINUX = PlatformPower(
    "Linux",
    can_prevent_idle_sleep=True,
    suspend_warning_sec=5.0,
    can_cancel_on_suspend=True,
    note="Adequate: systemd gives a 5-second delay by default, and it can be configured higher.",
)
WINDOWS = PlatformPower(
    "Windows",
    can_prevent_idle_sleep=True,
    suspend_warning_sec=2.0,
    can_cancel_on_suspend=False,
    note=(
        "Limited: Windows gives about 2 seconds before suspending, which is not enough to cancel "
        "orders and confirm it. Entries are instead kept from resting while the machine is idle, "
        "and a forced sleep is treated as a crash on wake."
    ),
)
UNKNOWN = PlatformPower(
    "this platform",
    can_prevent_idle_sleep=False,
    suspend_warning_sec=0.0,
    can_cancel_on_suspend=False,
    note="Unrecognised platform: no sleep protection is available, so treat every stop as a crash.",
)


def detect(system: Optional[str] = None) -> PlatformPower:
    name = (system or platform.system() or "").lower()
    if name == "darwin":
        return MACOS
    if name == "linux":
        return LINUX
    if name == "windows":
        return WINDOWS
    return UNKNOWN


class SleepInhibitor:
    """
    Holds an idle-sleep assertion while entries are armed.

    Deliberately best-effort and silent on failure: not being able to hold the
    machine awake is a degradation, not a reason to stop trading. What must
    never happen is the executor believing it is protected when it is not, so
    `held` reports the truth and the UI reads it.
    """

    def __init__(self, *, power: Optional[PlatformPower] = None, runner: Optional[Callable] = None):
        self._power = power or detect()
        self._runner = runner or _spawn
        self._process = None

    @property
    def held(self) -> bool:
        return self._process is not None

    @property
    def platform(self) -> PlatformPower:
        return self._power

    def acquire(self, reason: str = "Cascade executor has orders resting") -> bool:
        if self.held or not self._power.can_prevent_idle_sleep:
            return self.held
        command = self._command(reason)
        if not command:
            return False
        try:
            self._process = self._runner(command)
        except Exception as exc:
            _log.warning("could not hold the machine awake: %s", exc)
            self._process = None
        return self.held

    def release(self) -> None:
        process = self._process
        self._process = None
        if process is None:
            return
        try:
            process.terminate()
        except Exception:
            pass

    def _command(self, reason: str):
        if self._power is MACOS:
            # -i prevents idle sleep only. It cannot and should not fight a lid
            # close; the point is a window, not a veto.
            return ["caffeinate", "-i", "-w", str(_own_pid())]
        if self._power is LINUX:
            return ["systemd-inhibit", "--what=sleep", "--mode=delay", f"--why={reason}", "sleep", "infinity"]
        return None


def sync_inhibitor(inhibitor: SleepInhibitor, *, armed_exposure_usd: float) -> bool:
    """
    Hold the machine awake exactly while something can fill unwatched.

    Tied to the exposure rather than to "is the app running" on purpose: a
    laptop that never sleeps because a trading app is open is a laptop whose
    owner turns the trading app off.
    """
    if armed_exposure_usd > 0:
        return inhibitor.acquire()
    inhibitor.release()
    return False


def suspend_advice(power: PlatformPower, *, armed_exposure_usd: float) -> Optional[str]:
    """What to tell the buyer about this machine, before it matters."""
    if not power.can_cancel_on_suspend and armed_exposure_usd > 0:
        return (
            f"{power.name} gives about {power.suspend_warning_sec:.0f} seconds' warning before sleeping — "
            f"not enough to cancel orders and confirm it. ${armed_exposure_usd:,.2f} could fill if this "
            f"machine suspends now, and that would be handled as a crash on wake."
        )
    if not power.can_prevent_idle_sleep:
        return power.note
    return None


def _own_pid() -> int:
    import os

    return os.getpid()


def _spawn(command):
    # nosec B603 - argv is built here from module constants ("caffeinate",
    # "systemd-inhibit"), shell=False, and the only interpolated value is our
    # own `reason` string, which travels as one argv element and is never
    # parsed by a shell. Nothing from the feed or the exchange reaches this.
    return subprocess.Popen(  # nosec B603
        command,
        shell=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
