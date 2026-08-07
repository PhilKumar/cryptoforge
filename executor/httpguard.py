"""
executor/httpguard.py — who is allowed to talk to a page bound to loopback.

Both local servers in this folder — the console and the first-run setup screen —
answer on 127.0.0.1, and both can move money or write credentials. "It is only
localhost" is not a boundary a browser respects: any page on the internet can
point its own domain at 127.0.0.1, or blind-POST a form there without asking
anyone's permission.

The rules live here rather than in each server because a security check that
exists twice is a security check that will one day be fixed once.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

# Sent by our own fetch() and by nothing else. A CUSTOM header forces a CORS
# preflight, and neither server answers OPTIONS, so a cross-origin caller can
# never obtain one. A plain <form> POST cannot set it at all, which is the
# attack this closes.
UI_HEADER = "X-Cascade-UI"
UI_HEADER_VALUE = "1"

_LOOPBACK_PEERS = ("127.0.0.1", "::1")
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "[::1]", "::1")


def is_local(handler) -> bool:
    """Loopback peer AND a loopback Host header.

    The peer check is a belt on top of the bind. The Host check is DNS-rebinding
    defence: a hostile page can resolve its own domain to 127.0.0.1 and then
    fetch us same-origin, and the one thing the browser reports faithfully is
    the Host it asked for.
    """
    if handler.client_address[0] not in _LOOPBACK_PEERS:
        return False
    host = (handler.headers.get("Host") or "").split(":")[0].lower()
    return host in _LOOPBACK_HOSTS


def is_ours(handler) -> bool:
    """Whether this request carries the header only our own page can set."""
    return handler.headers.get(UI_HEADER) == UI_HEADER_VALUE


def refuse(handler) -> bool:
    """Send the right error and return True when the request must not proceed.

    Written as a guard the caller returns on, so the two servers cannot drift
    into checking different things or checking them in a different order:

        if httpguard.refuse(self):
            return
    """
    if not is_local(handler):
        handler.send_error(403)
        return True
    if handler.command == "POST" and not is_ours(handler):
        handler.send_error(403, f"missing {UI_HEADER} header")
        return True
    return False
