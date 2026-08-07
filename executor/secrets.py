"""
executor/secrets.py — where the exchange key lives on a buyer's machine.

Until now the answer was "an environment variable you export in a terminal",
which works and is safe enough, but is also one of the two reasons setting this
up needs a terminal at all. A double-clickable app has no terminal to export
anything in, so the key has to go somewhere the operating system already knows
how to guard: **Keychain on macOS, Credential Manager on Windows, Secret
Service on Linux.**

Three rules this file exists to keep:

1. **A secret is never returned in anything printable.** `redacted()` on the
   config, the logs, the support bundle — none of them route through here, and
   nothing here has a `__repr__` that could leak one by accident.
2. **A store that is not available fails loudly at WRITE time, never silently
   at read time.** Writing a key to a file the buyer thinks is a Keychain is
   worse than refusing, because they would never find out.
3. **The environment still wins.** Existing buyers exported `CASCADE_API_KEY`,
   and a launch agent or container sets it that way too. Nothing here takes
   that away; the store is only consulted when the environment is silent.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

_log = logging.getLogger("cascade.executor.secrets")

# One service name, so a buyer looking in Keychain sees a single recognisable
# entry rather than a scatter of them. The account is the buyer id, so two
# accounts on one machine do not overwrite each other.
SERVICE = "Cascade Executor"

KEY_FIELD = "api_key"
SECRET_FIELD = "api_secret"


class SecretsUnavailable(Exception):
    """No OS credential store here. Message is for the buyer, not a log."""


def _keyring():
    """The backend, or None. Imported lazily and never at module scope.

    `keyring` is an optional dependency: the command-line path this folder
    shipped with does not need it, and a buyer running from source on a box
    with no Secret Service should not be stopped at import by a package they
    have no use for.

    `CASCADE_NO_KEYRING=1` turns it off outright. That is for test runs and CI,
    which must never reach into the real login keychain of whoever is running
    them — a test that prompts for a password is a test that hangs.
    """
    if os.environ.get("CASCADE_NO_KEYRING"):
        return None
    try:
        import keyring
        from keyring.backends import fail
        from keyring.errors import NoKeyringError
    except Exception:  # pragma: no cover - depends on the buyer's machine
        return None
    try:
        backend = keyring.get_keyring()
    except NoKeyringError:  # pragma: no cover
        return None
    # keyring installs a "fail" backend rather than raising when nothing real is
    # available, and writing into that one loses the value — the silent failure
    # rule 2 exists to prevent.
    #
    # Tested by IDENTITY, not by class name. The fail backend's class is called
    # `Keyring` — and so is the macOS one, `keyring.backends.macOS.Keyring`.
    # Matching on the name rejected the real Keychain on every Mac, which is
    # exactly the machine this was written for.
    if isinstance(backend, fail.Keyring):
        return None
    # A chainer with nothing viable behind it reports priority 0 or less.
    if getattr(backend, "priority", 1) <= 0:  # pragma: no cover - Linux, mostly
        return None
    return keyring


def available() -> bool:
    """Whether this machine can keep a secret for us."""
    return _keyring() is not None


def describe() -> str:
    """What the setup page tells the buyer their key is being put into."""
    if not available():
        return ""
    import sys

    return {
        "darwin": "your macOS Keychain",
        "win32": "Windows Credential Manager",
    }.get(sys.platform, "your system keyring")


def store(buyer_id: str, api_key: str, api_secret: str) -> str:
    """Put the pair away. Returns where, for the buyer to be told.

    Raises rather than falling back to a file. A buyer who was told their key
    went into the Keychain, and whose key is actually sitting in JSON next to
    the program, has been given a false sense of where their money's front door
    is — and would have no reason to go looking.
    """
    backend = _keyring()
    if backend is None:
        raise SecretsUnavailable(
            "This machine has no credential store I can use, so there is nowhere safe to keep "
            "your exchange key. Set CASCADE_API_KEY and CASCADE_API_SECRET in the environment "
            "instead, and the program will read them from there."
        )
    account = _account(buyer_id)
    backend.set_password(SERVICE, f"{account}:{KEY_FIELD}", api_key)
    backend.set_password(SERVICE, f"{account}:{SECRET_FIELD}", api_secret)
    return describe()


def load(buyer_id: str) -> Tuple[str, str]:
    """The stored pair, or two empty strings.

    Never raises. A missing store at read time is the ordinary case for every
    buyer who exports the variables instead, and this is called on the startup
    path where a raise would be a machine that will not boot.
    """
    backend = _keyring()
    if backend is None:
        return "", ""
    account = _account(buyer_id)
    try:
        return (
            backend.get_password(SERVICE, f"{account}:{KEY_FIELD}") or "",
            backend.get_password(SERVICE, f"{account}:{SECRET_FIELD}") or "",
        )
    except Exception as exc:  # pragma: no cover - depends on the buyer's machine
        # A locked Keychain, a denied prompt, a broken Secret Service. Logged
        # without the account name and without any part of a value.
        _log.warning("could not read the stored credentials: %s", exc)
        return "", ""


def forget(buyer_id: str) -> None:
    """Remove the pair. Used when a buyer re-keys, so the old one does not linger."""
    backend = _keyring()
    if backend is None:
        return
    account = _account(buyer_id)
    for field in (KEY_FIELD, SECRET_FIELD):
        try:
            backend.delete_password(SERVICE, f"{account}:{field}")
        except Exception:  # pragma: no cover - already absent is the usual reason
            pass


def resolve(buyer_id: str, *, environ: Optional[dict] = None) -> Tuple[str, str]:
    """The credentials to actually use, environment first.

    Environment first because that is what every existing buyer already has,
    and what a launch agent, a container or a support session can set without
    touching a keychain. The store is the answer for the packaged app, not a
    replacement for the way this has always worked.

    Both halves must come from the same place. A key from the environment
    paired with a secret from the store is a signature that will not verify,
    and the error the exchange returns for that says nothing useful.
    """
    env = os.environ if environ is None else environ
    key = str(env.get("CASCADE_API_KEY") or "")
    secret = str(env.get("CASCADE_API_SECRET") or "")
    if key and secret:
        return key, secret
    if key or secret:
        _log.warning(
            "only one of CASCADE_API_KEY / CASCADE_API_SECRET is set — ignoring it and "
            "using the stored pair, if there is one"
        )
    return load(buyer_id)


def _account(buyer_id: str) -> str:
    return str(buyer_id or "default").strip() or "default"
