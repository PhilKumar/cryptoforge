"""
executor/transport.py — what actually holds the socket open.

`feed_client.py` decides what a frame means. This decides when to be connected,
when to give up, and where the trust comes from. Kept apart because the client
is pure and testable and this is neither: it has a network, a clock, and a
reconnect loop.

Trust starts at exactly one place — a root public key compiled into the build.
Everything else is fetched: the key set is signed by that root, and the feed
keys inside it sign the messages. Nothing here ever trusts a key because the
server sent it.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import stat
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from executor.feed_client import (
    KEYSET_CACHE_TTL_SEC,
    FeedClient,
    active_keys_from_keyset,
    new_nonce,
    verify_frame_local,
)

_log = logging.getLogger("cascade.executor")

# Reconnect backoff. Jittered, because a server restart otherwise brings every
# executor back at the same instant.
BACKOFF_START_SEC = 1.0
BACKOFF_MAX_SEC = 60.0

# Close codes the server uses. Two of them mean "stop", and which two matters.
CLOSE_NOT_ENTITLED = 4003
CLOSE_DISPLACED = 4009
CLOSE_FEED_OFF = 4004


class TransportStopped(Exception):
    """Reconnecting would not help. Carries what to tell the buyer."""


@dataclass
class ExecutorIdentity:
    """
    The buyer's own key. Generated here, at install, and never sent — only its
    public half is registered. That is the same principle that keeps us from
    holding their exchange credentials: there is no secret on the server side
    that can leak and become them.
    """

    buyer_id: str
    # gitleaks reads `<name>_key: <high-entropy-token>` as a finding, and the
    # high-entropy token here is the TYPE. Nothing secret is in this repo — the
    # real key is generated on the buyer's machine and never leaves it.
    signing_key: Ed25519PrivateKey  # gitleaks:allow

    @classmethod
    def load_or_create(cls, path: str, buyer_id: str) -> "ExecutorIdentity":
        resolved = os.path.abspath(os.path.expanduser(path))
        if os.path.exists(resolved):
            with open(resolved, "rb") as handle:
                key = serialization.load_pem_private_key(handle.read(), password=None)
            if not isinstance(key, Ed25519PrivateKey):
                raise ValueError(f"{resolved} is not an ed25519 key")
            return cls(buyer_id, key)

        key = Ed25519PrivateKey.generate()
        os.makedirs(os.path.dirname(resolved) or ".", exist_ok=True)
        fd = os.open(resolved, os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(fd, "wb") as handle:
            handle.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
        return cls(buyer_id, key)

    def public_key_b64(self) -> str:
        import base64

        return base64.b64encode(
            self.signing_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
        ).decode("ascii")

    def handshake(self, *, now: Optional[float] = None) -> dict:
        import base64

        signed = {
            "buyer_id": self.buyer_id,
            "nonce": new_nonce(),
            "timestamp": time.time() if now is None else now,
        }
        msg = json.dumps(signed, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        sig = base64.b64encode(self.signing_key.sign(msg.encode("utf-8"))).decode("ascii")
        return {**signed, "sig": f"ed25519:{self.buyer_id}:{sig}"}


class KeySetStore:
    """
    Fetches and caches the root-signed key set.

    The cache is what makes a revocation reach a machine that was switched off
    when it was published: a cached set past its TTL is refused on its own
    terms, so an executor that cannot reach us stops opening campaigns rather
    than trusting whatever it last saw.
    """

    def __init__(
        self,
        *,
        root_public_b64: str,
        cache_path: str,
        fetch_fn: Optional[Callable[[str], dict]] = None,
        now_fn: Callable[[], float] = time.time,
    ):
        self._root = root_public_b64
        self._cache_path = os.path.abspath(os.path.expanduser(cache_path))
        self._fetch = fetch_fn or _http_fetch_keyset
        self._now = now_fn

    def _verify(self, frame: dict) -> dict:
        """Root signature, then the document's own expiry. Fail closed on both."""
        document = verify_frame_local(frame, {"root": self._root})
        if int(document.get("expires_at") or 0) <= self._now():
            raise TransportStopped("The signing key set has expired. Update the executor or contact support.")
        return document

    def load_cached(self) -> Optional[tuple[Dict[str, str], float]]:
        try:
            with open(self._cache_path, encoding="utf-8") as handle:
                cached = json.load(handle)
            fetched_at = float(cached["fetched_at"])
            if self._now() - fetched_at > KEYSET_CACHE_TTL_SEC:
                return None
            document = self._verify(cached["frame"])
        except TransportStopped:
            raise
        except Exception:
            return None
        return active_keys_from_keyset(document, now=self._now()), fetched_at

    def refresh(self, base_url: str) -> tuple[Dict[str, str], float]:
        frame = self._fetch(base_url.rstrip("/") + "/api/cascade/feed/keys")
        document = self._verify(frame)
        fetched_at = self._now()
        try:
            os.makedirs(os.path.dirname(self._cache_path) or ".", exist_ok=True)
            with open(self._cache_path, "w", encoding="utf-8") as handle:
                json.dump({"fetched_at": fetched_at, "frame": frame}, handle)
        except Exception as exc:
            # A cache we cannot write is a nuisance, not a failure: we have the
            # keys in hand. It only costs us the offline start next time.
            _log.warning("could not cache the key set: %s", exc)
        return active_keys_from_keyset(document, now=fetched_at), fetched_at


def _http_fetch_keyset(url: str) -> dict:
    import httpx

    response = httpx.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


class FeedTransport:
    """
    Holds the socket open, and knows when not to.

    The reconnect loop deliberately distinguishes three endings:

    - **displaced (4009)** — another copy of this executor connected with the
      same key. STOP. Two instances that both reconnect would displace each
      other forever, and neither would manage its positions properly. The
      newest connection wins and this one steps aside, loudly.
    - **not entitled (4003)** — the subscription lapsed, or the key is not
      registered. Stop and tell the buyer; retrying will not change it, and a
      tight loop against an auth failure is how you get rate-limited.
    - **anything else** — a restart, a dropped wifi, a deploy. Back off and
      come back.

    Whatever the ending, the FeedClient keeps its picture and stays responsible
    for open positions. Disconnected is not flat.
    """

    def __init__(
        self,
        *,
        base_url: str,
        identity: ExecutorIdentity,
        keyset: KeySetStore,
        connect_fn: Callable,
        on_status: Optional[Callable[[str, dict], None]] = None,
        now_fn: Callable[[], float] = time.time,
        sleep_fn: Optional[Callable] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._identity = identity
        self._keyset = keyset
        self._connect = connect_fn
        self._on_status = on_status
        self._now = now_fn
        self._sleep = sleep_fn or asyncio.sleep
        self.client: Optional[FeedClient] = None
        self.stopped_reason: str = ""

    @property
    def ws_url(self) -> str:
        scheme = "wss" if self._base_url.startswith("https") else "ws"
        host = self._base_url.split("://", 1)[-1]
        return f"{scheme}://{host}/ws/cascade-feed"

    async def run(self, *, max_sessions: Optional[int] = None) -> str:
        """
        The reconnect loop. Returns the reason it gave up, or "" if it ran out
        of `max_sessions` (which only tests pass).
        """
        backoff = BACKOFF_START_SEC
        sessions = 0
        while max_sessions is None or sessions < max_sessions:
            sessions += 1
            try:
                await self._session()
                backoff = BACKOFF_START_SEC
            except TransportStopped as stop:
                self.stopped_reason = str(stop)
                self._status("stopped", {"reason": self.stopped_reason})
                return self.stopped_reason
            except Exception as exc:
                self._status("disconnected", {"error": str(exc), "retry_in": round(backoff, 1)})
                # Jittered: a server restart otherwise brings every executor
                # back on the same second.
                await self._sleep(backoff * (0.5 + random.random()))  # noqa: S311 - not cryptographic
                backoff = min(backoff * 2, BACKOFF_MAX_SEC)
        return ""

    async def _session(self) -> None:
        keys, fetched_at = self._current_keys()
        self.client = FeedClient(
            public_keys=keys,
            keyset_fetched_at=fetched_at,
            now_fn=self._now,
            on_event=lambda kind, detail: self._status(kind, detail),
        )

        async with self._connect(self.ws_url) as socket:
            await socket.send(json.dumps(self._identity.handshake(now=self._now())))
            while True:
                try:
                    raw = await socket.recv()
                except Exception as exc:
                    # Every websocket library signals a close by raising, and
                    # the close CODE is the whole message: it is the difference
                    # between "come back in a second" and "do not come back".
                    stop = stop_reason_for_close(int(getattr(exc, "code", 0) or 0), str(getattr(exc, "reason", "")))
                    if stop:
                        raise TransportStopped(stop) from exc
                    raise
                message = json.loads(raw)
                kind = message.get("type")

                if kind == "welcome":
                    self._status("connected", message)
                    if message.get("clock_warning"):
                        # Said loudly: a skewed clock makes join-at-start skip
                        # every campaign, silently, on a healthy-looking feed.
                        self._status("clock_warning", {"message": message["clock_warning"]})
                elif kind in ("snapshot", "event", "heartbeat"):
                    self.client.handle_frame(message["frame"])
                elif kind == "snapshot.end":
                    self.client.needs_resnapshot = False
                    self._status("synced", {"campaigns": len(self.client.campaigns)})
                elif kind == "feed.revoked":
                    raise TransportStopped(message.get("reason") or "Subscription is no longer active.")

                if self.client.needs_resnapshot:
                    # Reconnecting is how a re-snapshot is asked for: the
                    # server sends one on every subscribe.
                    self._status("resnapshot", {"reason": "sequence gap"})
                    return

    def _current_keys(self) -> tuple[Dict[str, str], float]:
        cached = self._keyset.load_cached()
        if cached:
            return cached
        return self._keyset.refresh(self._base_url)

    def _status(self, kind: str, detail: dict) -> None:
        if self._on_status:
            try:
                self._on_status(kind, detail)
            except Exception:
                pass


def websockets_connect(url: str):
    """
    The real connect_fn, over the `websockets` library.

    Injected rather than hardcoded so the reconnect policy can be tested
    against a scripted socket — the interesting cases here are close codes and
    sequence gaps, and none of them are worth a real server to reproduce.

    `websockets.ConnectionClosed` carries `.code` and `.reason`, which is
    exactly what `stop_reason_for_close` reads.
    """
    import websockets

    return websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5)


def stop_reason_for_close(code: int, reason: str = "") -> Optional[str]:
    """
    Whether a close code means "do not come back".

    Kept separate from the loop so the policy is readable in one place, and so
    a transport built on a different websocket library can reuse the judgement
    rather than reinvent it.
    """
    if code == CLOSE_DISPLACED:
        return (
            "Another copy of this executor connected with the same key and took over. "
            "Only one may run at a time — this one has stepped aside."
        )
    if code == CLOSE_NOT_ENTITLED:
        return reason or "This subscription is not active."
    if code == CLOSE_FEED_OFF:
        return reason or "The signal feed is not enabled on the server."
    return None
