"""
installer/app_entry.py — what happens when a buyer double-clicks Cascade.

The command-line program stays exactly as it is; this only changes the defaults
for the one context that cannot express them. A double-clicked app has no argv
to put `--tray` in, no terminal for `print` to land in, and no shell to report
an exit code to, so all three are decided here instead.

Three things it must get right, all of them consequences of there being no
window to read:

- **Somewhere for output to go.** `_say` and every log line go to a file in the
  state directory, or a crash becomes an icon that silently never appears.
- **A visible failure.** If the program dies before the tray exists, the buyer
  double-clicked something that did nothing at all. The last resort is a native
  dialog, because there is nowhere else to say it.
- **One instance.** People double-click twice. `main()` takes the lock, and the
  second copy is told where the first one is rather than starting beside it.
"""

from __future__ import annotations

import os
import sys


def log_path() -> str:
    """Beside the buyer's other state, so support can ask for one folder."""
    state = os.environ.get("CASCADE_STATE_DIR") or "~/.cascade-executor"
    return os.path.join(os.path.expanduser(state), "cascade.log")


def _tee_output(path: str):
    """Send stdout and stderr to a file, keeping any real terminal too.

    Appended, never rotated by us beyond a size cap: a buyer who is asked for
    this file should find the whole of the run that went wrong in it, not the
    tail of the one after.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        if os.path.exists(path) and os.path.getsize(path) > 8 * 1024 * 1024:
            os.replace(path, path + ".1")
    except OSError:
        pass
    handle = open(path, "a", encoding="utf-8", buffering=1)

    class Tee:
        def __init__(self, *streams):
            self._streams = [s for s in streams if s is not None]

        def write(self, text):
            for stream in self._streams:
                try:
                    stream.write(text)
                except Exception:
                    pass
            return len(text)

        def flush(self):
            for stream in self._streams:
                try:
                    stream.flush()
                except Exception:
                    pass

    # sys.stdout is None in a windowed build, which is why every write above is
    # guarded rather than assumed.
    sys.stdout = Tee(handle, sys.__stdout__)
    sys.stderr = Tee(handle, sys.__stderr__)
    return handle


def _as_applescript_string(text: str) -> str:
    """Quote a Python string so AppleScript reads it as one string.

    The message is an exception's text — arbitrary, and quite likely to contain
    a quote, since exchange errors are full of them. Interpolated raw, a
    stray `"` ends the literal and the rest is parsed as AppleScript. That is
    someone else's text becoming someone else's code, in a program that has a
    Keychain entry, so it is escaped rather than trusted.
    """
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return '"' + escaped.replace("\n", "\\n") + '"'


def alert(title: str, message: str) -> None:
    """Say something when there is no window to say it in. Best effort."""
    try:
        if sys.platform == "darwin":
            import subprocess  # nosec B404 - fixed argv below, never a shell

            script = (
                f"display alert {_as_applescript_string(title)} message {_as_applescript_string(message)} as critical"
            )
            # Absolute path, no shell, literal argv. The absolute path is not
            # pedantry here: a signed app inherits whatever PATH it is launched
            # with, and "osascript" is a name anything could answer to.
            subprocess.run(  # nosec B603 - argv is a literal list, shell=False
                ["/usr/bin/osascript", "-e", script], timeout=30, check=False
            )
            return
        if sys.platform == "win32":  # pragma: no cover - Windows only
            import ctypes

            ctypes.windll.user32.MessageBoxW(None, message, title, 0x10)
            return
    except Exception:
        pass
    print(f"{title}: {message}", file=sys.__stderr__ or sys.stderr)


def main() -> int:
    handle = _tee_output(log_path())
    try:
        from executor.__main__ import main as run

        # --tray is the whole difference, and only when nothing was asked for.
        # A double-click passes no argv; running the same binary from a
        # terminal is how support says "try `Cascade --check`", and swallowing
        # their arguments would make that impossible.
        return run(sys.argv[1:] or ["--tray"])
    except SystemExit as exc:  # argparse, or a deliberate exit
        return int(exc.code or 0)
    except BaseException as exc:  # noqa: BLE001 - last resort before a silent death
        import traceback

        traceback.print_exc()
        alert(
            "Cascade could not start",
            f"{exc}\n\nThe details are in:\n{log_path()}\n\nNothing was traded.",
        )
        return 1
    finally:
        try:
            handle.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
