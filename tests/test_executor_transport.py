"""What holds the socket open, and when it knows not to.

Trust starts at exactly one place — a root public key compiled into the build.
The key set is fetched and verified against it; the feed keys inside sign the
messages. Nothing is ever trusted because the server sent it, and the tests
here try to make it trust something anyway.

The other half is the reconnect policy, where the interesting failure is not a
crash. Two copies of an executor running the same key would displace each
other forever, each reconnecting, neither ever settling long enough to manage
its positions. So "displaced" has to mean stop, not retry.
"""

import json
import os
import tempfile
import unittest

from engine.cascade import MODEL_VERSION
from engine.cascade_feed import (
    ROOT_KID,
    FeedSigner,
    FeedSubscribers,
    build_envelope,
    build_key_set,
    campaign_opened_payload,
    sign_key_set,
    verify_subscriber_handshake,
)
from executor.feed_client import KEYSET_CACHE_TTL_SEC
from executor.transport import (
    CLOSE_DISPLACED,
    CLOSE_NOT_ENTITLED,
    ExecutorIdentity,
    FeedTransport,
    KeySetStore,
    TransportStopped,
    stop_reason_for_close,
)

NOW = 1785770000.0


class FakeClose(Exception):
    def __init__(self, code, reason=""):
        super().__init__(f"closed {code}")
        self.code = code
        self.reason = reason


class FakeSocket:
    """Scripted server side. Each item is a dict to send, or an exception."""

    def __init__(self, script):
        self.script = list(script)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    async def recv(self):
        if not self.script:
            raise FakeClose(1000, "script exhausted")
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return json.dumps(item)


def _connect_returning(*scripts):
    """A connect_fn that hands out one scripted socket per session."""
    sockets = [FakeSocket(script) for script in scripts]
    handed = []

    def connect(url):
        socket = sockets.pop(0) if sockets else FakeSocket([])
        handed.append(socket)
        return socket

    connect.handed = handed
    return connect


class Fixture:
    """A root, a feed key, a key set, and frames signed by it."""

    def __init__(self, *, issued_at=int(NOW), valid_days=30):
        self.root = FeedSigner.generate(ROOT_KID)
        self.feed = FeedSigner.generate("cf-feed-2026a")
        document = build_key_set(
            [
                {
                    "kid": self.feed.kid,
                    "public": self.feed.public_key_b64(),
                    "not_before": issued_at,
                    "not_after": issued_at + 90 * 86400,
                }
            ],
            issued_at=issued_at,
            valid_days=valid_days,
        )
        self.keyset_frame = sign_key_set(document, self.root)

    def campaign_frame(self, seq=1, created_at=int(NOW) - 30):
        payload = campaign_opened_payload(
            {
                "campaign_id": "casc_SOLUSDT_1",
                "symbol": "SOLUSDT",
                "exchange": "binance",
                "created_at": created_at,
                "mother_high": 178.42,
                "mother_low": 174.10,
                "mother_timestamp": 1785400800,
                "state": "TRENDLINE_ACTIVE",
                "timeframe": "5m",
                "tick_size": 0.01,
                "min_notional_usd": 5.0,
            }
        )
        return self.feed.frame(
            build_envelope(
                msg_type="campaign.opened",
                symbol="SOLUSDT",
                campaign_id="casc_SOLUSDT_1",
                payload=payload,
                seq=seq,
                model_version=MODEL_VERSION,
                emitted_at=int(NOW),
            )
        )

    def heartbeat_frame(self, seq=1):
        return self.feed.frame(
            build_envelope(
                msg_type="heartbeat",
                symbol="SOLUSDT",
                campaign_id="",
                payload={"running_campaigns": 1},
                seq=seq,
                model_version=MODEL_VERSION,
                emitted_at=int(NOW),
            )
        )


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = os.path.join(self._tmp.name, "sub", "buyer_key.pem")

    def test_the_key_is_generated_locally_and_kept_private(self):
        """Only the public half is ever registered — nothing on our side to leak."""
        identity = ExecutorIdentity.load_or_create(self.path, "buyer-7")
        self.assertEqual(oct(os.stat(self.path).st_mode & 0o777), "0o600")
        self.assertTrue(identity.public_key_b64())

    def test_it_reloads_the_same_key_rather_than_minting_a_new_one(self):
        """A new key each start would deregister the buyer from their own feed."""
        first = ExecutorIdentity.load_or_create(self.path, "buyer-7")
        second = ExecutorIdentity.load_or_create(self.path, "buyer-7")
        self.assertEqual(first.public_key_b64(), second.public_key_b64())

    def test_the_handshake_verifies_against_the_real_server_check(self):
        """The executor's signer against the server's verifier, end to end."""
        identity = ExecutorIdentity.load_or_create(self.path, "buyer-7")

        class Store:
            def __init__(self):
                self.rows = {}

            def get(self, bucket, key, default=None):
                return self.rows.get((bucket, str(key)), default)

            def put(self, bucket, key, payload):
                self.rows[(bucket, str(key))] = payload

        subs = FeedSubscribers(Store())
        subs.add("buyer-7", identity.public_key_b64())
        result = verify_subscriber_handshake(identity.handshake(now=NOW), subs, now=NOW)
        self.assertEqual(result["subscriber"]["buyer_id"], "buyer-7")

    def test_a_handshake_signed_by_a_different_key_is_refused(self):
        identity = ExecutorIdentity.load_or_create(self.path, "buyer-7")
        other = ExecutorIdentity.load_or_create(os.path.join(self._tmp.name, "other.pem"), "buyer-7")

        class Store:
            def __init__(self):
                self.rows = {}

            def get(self, bucket, key, default=None):
                return self.rows.get((bucket, str(key)), default)

            def put(self, bucket, key, payload):
                self.rows[(bucket, str(key))] = payload

        subs = FeedSubscribers(Store())
        subs.add("buyer-7", identity.public_key_b64())
        from engine.cascade_feed import NotEntitled

        with self.assertRaises(NotEntitled):
            verify_subscriber_handshake(other.handshake(now=NOW), subs, now=NOW)


class KeySetStoreTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.cache = os.path.join(self._tmp.name, "keyset.json")
        self.clock = [NOW]
        self.fx = Fixture()

    def _store(self, *, frame=None, root=None):
        return KeySetStore(
            root_public_b64=root or self.fx.root.public_key_b64(),
            cache_path=self.cache,
            fetch_fn=lambda url: frame or self.fx.keyset_frame,
            now_fn=lambda: self.clock[0],
        )

    def test_a_root_signed_set_is_accepted_and_cached(self):
        keys, fetched_at = self._store().refresh("https://crypto.example")
        self.assertEqual(keys, {self.fx.feed.kid: self.fx.feed.public_key_b64()})
        self.assertTrue(os.path.exists(self.cache))
        self.assertEqual(fetched_at, NOW)

    def test_a_set_signed_by_anything_but_our_root_is_refused(self):
        """The attack the offline root exists for."""
        attacker = FeedSigner.generate(ROOT_KID)
        forged = sign_key_set(json.loads(self.fx.keyset_frame["msg"]), attacker)
        from cryptography.exceptions import InvalidSignature

        with self.assertRaises(InvalidSignature):
            self._store(frame=forged).refresh("https://crypto.example")

    def test_an_expired_document_stops_rather_than_degrades(self):
        store = self._store(frame=sign_key_set(build_key_set([], issued_at=int(NOW) - 40 * 86400), self.fx.root))
        with self.assertRaises(TransportStopped):
            store.refresh("https://crypto.example")

    def test_a_cache_past_its_ttl_is_not_used(self):
        """This is what makes a revocation reach a machine that was switched off."""
        store = self._store()
        store.refresh("https://crypto.example")
        self.assertIsNotNone(store.load_cached())
        self.clock[0] += KEYSET_CACHE_TTL_SEC + 1
        self.assertIsNone(store.load_cached())

    def test_a_corrupt_cache_is_ignored_rather_than_fatal(self):
        with open(self.cache, "w", encoding="utf-8") as handle:
            handle.write("{ not json")
        self.assertIsNone(self._store().load_cached())

    def test_a_revoked_kid_never_reaches_the_client(self):
        document = json.loads(self.fx.keyset_frame["msg"])
        document["revoked"] = [self.fx.feed.kid]
        keys, _ = self._store(frame=sign_key_set(document, self.fx.root)).refresh("https://crypto.example")
        self.assertEqual(keys, {})


class ReconnectPolicyTests(unittest.TestCase):
    """Which endings mean come back, and which mean stop."""

    def test_being_displaced_means_stop(self):
        """Two instances that both retry would displace each other forever."""
        reason = stop_reason_for_close(CLOSE_DISPLACED)
        self.assertIn("stepped aside", reason)

    def test_not_entitled_means_stop(self):
        self.assertEqual(
            stop_reason_for_close(CLOSE_NOT_ENTITLED, "This subscription is lapsed."), "This subscription is lapsed."
        )

    def test_an_ordinary_close_means_come_back(self):
        self.assertIsNone(stop_reason_for_close(1006))
        self.assertIsNone(stop_reason_for_close(1000))


class TransportSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.clock = [NOW]
        self.fx = Fixture()
        self.identity = ExecutorIdentity.load_or_create(os.path.join(self._tmp.name, "buyer.pem"), "buyer-7")
        self.keyset = KeySetStore(
            root_public_b64=self.fx.root.public_key_b64(),
            cache_path=os.path.join(self._tmp.name, "keyset.json"),
            fetch_fn=lambda url: self.fx.keyset_frame,
            now_fn=lambda: self.clock[0],
        )
        self.status = []

    def _transport(self, connect_fn):
        return FeedTransport(
            base_url="https://crypto.example",
            identity=self.identity,
            keyset=self.keyset,
            connect_fn=connect_fn,
            on_status=lambda kind, detail: self.status.append((kind, detail)),
            now_fn=lambda: self.clock[0],
            sleep_fn=self._no_sleep,
        )

    async def _no_sleep(self, seconds):
        return None

    def _welcome(self, **overrides):
        message = {
            "type": "welcome",
            "v": 1,
            "model_version": MODEL_VERSION,
            "heartbeat_sec": 30,
            "heads": {"SOLUSDT": 0},
            "clock_skew_sec": 0.0,
            "clock_warning": None,
        }
        message.update(overrides)
        return message

    async def test_it_handshakes_then_follows_the_snapshot(self):
        connect = _connect_returning(
            [
                self._welcome(),
                {"type": "snapshot", "frame": self.fx.campaign_frame()},
                {"type": "snapshot.end", "heads": {"SOLUSDT": 1}},
                FakeClose(1000),
            ]
        )
        transport = self._transport(connect)
        await transport.run(max_sessions=1)

        handshake = connect.handed[0].sent[0]
        self.assertEqual(handshake["buyer_id"], "buyer-7")
        self.assertIn("casc_SOLUSDT_1", transport.client.campaigns)
        self.assertIn("synced", [kind for kind, _ in self.status])

    async def test_the_ws_url_follows_the_base_url_scheme(self):
        transport = self._transport(_connect_returning([]))
        self.assertEqual(transport.ws_url, "wss://crypto.example/ws/cascade-feed")

    async def test_a_clock_warning_is_surfaced_not_swallowed(self):
        """A skewed clock joins nothing at all, on a feed that looks healthy."""
        connect = _connect_returning([self._welcome(clock_warning="Sync your clock."), FakeClose(1000)])
        await self._transport(connect).run(max_sessions=1)
        self.assertIn("clock_warning", [kind for kind, _ in self.status])

    async def test_being_displaced_stops_the_loop(self):
        connect = _connect_returning(
            [self._welcome(), FakeClose(CLOSE_DISPLACED, "Replaced by a newer connection")],
            [self._welcome(), FakeClose(1000)],
        )
        transport = self._transport(connect)
        reason = await transport.run(max_sessions=5)
        self.assertIn("stepped aside", reason)
        # It really stopped: the second script was never opened.
        self.assertEqual(len(connect.handed), 1)

    async def test_a_revocation_message_stops_the_loop(self):
        connect = _connect_returning(
            [self._welcome(), {"type": "feed.revoked", "reason": "This subscription has expired."}],
            [self._welcome(), FakeClose(1000)],
        )
        transport = self._transport(connect)
        reason = await transport.run(max_sessions=5)
        self.assertEqual(reason, "This subscription has expired.")
        self.assertEqual(len(connect.handed), 1)

    async def test_an_ordinary_drop_reconnects(self):
        connect = _connect_returning(
            [self._welcome(), FakeClose(1006, "network died")],
            [self._welcome(), {"type": "snapshot.end", "heads": {}}, FakeClose(1000)],
        )
        transport = self._transport(connect)
        await transport.run(max_sessions=2)
        self.assertEqual(len(connect.handed), 2)
        self.assertIn("disconnected", [kind for kind, _ in self.status])

    async def test_a_sequence_gap_ends_the_session_to_get_a_fresh_snapshot(self):
        """Reconnecting IS the re-snapshot request: subscribe always sends one."""
        connect = _connect_returning(
            [
                self._welcome(),
                {"type": "snapshot.end", "heads": {"SOLUSDT": 0}},
                {"type": "heartbeat", "frame": self.fx.heartbeat_frame(seq=99)},
                FakeClose(1000),
            ],
            [self._welcome(), {"type": "snapshot.end", "heads": {}}, FakeClose(1000)],
        )
        transport = self._transport(connect)
        await transport.run(max_sessions=2)
        self.assertIn("resnapshot", [kind for kind, _ in self.status])
        self.assertEqual(len(connect.handed), 2)

    async def test_a_frame_signed_by_an_unknown_key_is_not_followed(self):
        """A server that starts sending frames we cannot verify gets nowhere."""
        stranger = FeedSigner.generate("cf-feed-rogue")
        rogue = stranger.frame(
            build_envelope(
                msg_type="campaign.opened",
                symbol="SOLUSDT",
                campaign_id="casc_SOLUSDT_9",
                payload=campaign_opened_payload(
                    {
                        "campaign_id": "casc_SOLUSDT_9",
                        "symbol": "SOLUSDT",
                        "created_at": int(NOW),
                        "mother_high": 1.0,
                        "mother_low": 0.5,
                        "mother_timestamp": 1,
                        "state": "TRENDLINE_ACTIVE",
                        "timeframe": "5m",
                    }
                ),
                seq=1,
                model_version=MODEL_VERSION,
            )
        )
        connect = _connect_returning([self._welcome(), {"type": "event", "frame": rogue}, FakeClose(1000)])
        transport = self._transport(connect)
        await transport.run(max_sessions=1)
        self.assertNotIn("casc_SOLUSDT_9", transport.client.campaigns)

    async def test_it_starts_offline_from_a_fresh_cache(self):
        """A cached set inside its TTL means a reconnect needs no HTTP at all."""
        self.keyset.refresh("https://crypto.example")

        def refuse(url):
            raise AssertionError("should not have refetched")

        self.keyset._fetch = refuse
        connect = _connect_returning([self._welcome(), FakeClose(1000)])
        await self._transport(connect).run(max_sessions=1)
        self.assertIn("connected", [kind for kind, _ in self.status])


if __name__ == "__main__":
    unittest.main()


class ClientContinuityTests(TransportSessionTests):
    """A reconnect must not forget what it was managing.

    The runtime's order book finds a campaign by asking the client about it, so
    a fresh client after a dropped wifi answers "never heard of it" for every
    open position — and those positions silently stop having their exits
    managed, while the executor looks perfectly healthy.
    """

    async def test_the_same_client_survives_a_reconnect(self):
        connect = _connect_returning(
            [self._welcome(), {"type": "snapshot", "frame": self.fx.campaign_frame()}, FakeClose(1006)],
            [self._welcome(), FakeClose(1000)],
        )
        transport = self._transport(connect)
        await transport.run(max_sessions=1)
        first = transport.client
        self.assertIn("casc_SOLUSDT_1", first.campaigns)

        await transport.run(max_sessions=1)
        self.assertIs(transport.client, first)
        self.assertIn("casc_SOLUSDT_1", transport.client.campaigns)

    async def test_a_reconnect_still_refreshes_the_keys(self):
        connect = _connect_returning([self._welcome(), FakeClose(1000)], [self._welcome(), FakeClose(1000)])
        transport = self._transport(connect)
        await transport.run(max_sessions=1)
        self.clock[0] += 600
        await transport.run(max_sessions=1)
        # The cached set is still inside its TTL, so the client's own staleness
        # clock moved with it rather than ageing out mid-run.
        self.assertFalse(transport.client.keyset_expired)
