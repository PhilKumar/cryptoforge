"""
executor/__main__.py — the program.

    python -m executor              run it
    python -m executor --check      verify the config and the connection, trade nothing
    python -m executor --register   print the public key to register, then stop

Two asyncio tasks. One holds the feed socket open and keeps the picture
current; the other ticks on a timer, asks the exchange what happened, and
places what is due. They are separate because they fail differently: a feed
that drops should not stop exits being managed, and an exchange that rate-limits
should not drop the feed.

Shutdown runs the sleep invariants before exiting — cancel resting entries,
make sure anything held has a target — and writes the record that the next
start reads. A SIGTERM that skipped that would leave the very state the whole
recovery design exists to prevent. A second interrupt overrides it anyway: a
buyer who asks twice gets an immediate exit, because a program that will not
stop is worse than a shutdown record the next start already knows how to treat
as a crash.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
import webbrowser
from typing import Optional

from executor import setup
from executor.config import SAMPLE, ConfigError, ExecutorConfig, build_adapter, load
from executor.market import ExchangeMarketData, MarketStrip
from executor.power import SleepInhibitor, detect, sync_inhibitor
from executor.report import irreducible_risk
from executor.runtime import ExecutorRuntime, RuntimeConfig
from executor.singleton import AlreadyRunning, InstanceLock, lock_path
from executor.transport import ExecutorIdentity, FeedTransport, KeySetStore, TransportStopped

_log = logging.getLogger("cascade.executor")


def _say(*lines) -> None:
    for line in lines:
        print(line, flush=True)


def signal_handler(stopping, schedule, *, exit_fn=os._exit, announce=_say):
    """Build the SIGINT/SIGTERM handler: ask once, then take the exit.

    Installed with `signal.signal`, not `loop.add_signal_handler`, and that is
    the whole point. `tick()` is synchronous — it walks the book making blocking
    HTTP calls to the venue, each with a 15s timeout — so while a tick runs the
    event loop is not turning. An asyncio signal handler is a loop callback, so
    in exactly the moments a stop is slowest to arrive it cannot run at all, and
    a second Ctrl-C only set an Event that was already set. To the buyer that is
    a program refusing to close.

    An OS-level handler runs in the main thread at the next bytecode, which a
    socket wait yields to, so it lands mid-tick. The first interrupt asks for
    the graceful path — the sleep invariants matter, they cancel resting buys
    and leave targets in place. The second says the buyer has waited long
    enough, at the price of an incomplete shutdown record, which the next start
    already treats as a crash and recovers from.
    """

    def _on_signal(_signum, _frame):
        if stopping.is_set():
            announce("", "Interrupted again — exiting now. The next start will treat this as a crash.")
            exit_fn(130)
            return
        schedule(stopping.set)

    return _on_signal


class Executor:
    def __init__(self, config: ExecutorConfig):
        self.config = config
        self.identity = ExecutorIdentity.load_or_create(config.buyer_key_path, config.buyer_id)
        self.adapter = build_adapter(config)
        # Campaigns this machine was in when it last stopped. Loaded BEFORE the
        # transport exists so the very first snapshot of a session can resume
        # them; the join window otherwise reads a reboot as having missed the
        # start of a campaign we were already laddering into.
        self._joined_ids = self._load_joined()
        self.transport = FeedTransport(
            base_url=config.server_url,
            identity=self.identity,
            keyset=KeySetStore(
                root_public_b64=config.root_public_key,
                cache_path=config.keyset_cache_path,
            ),
            connect_fn=_connect,
            on_status=self._on_status,
            resumed_campaign_ids=self._joined_ids,
            timeframes=config.timeframes,
            source_exchanges=config.signal_exchanges,
        )
        self.runtime: Optional[ExecutorRuntime] = None
        self.inhibitor = SleepInhibitor()
        self._stopping = asyncio.Event()
        self._ui_state = None  # set by ui.wire() when the page is on
        self._strip = None  # the top quotes, built on the first tick that has an adapter
        # Set once `run()` is on a loop, so a host that owns the main thread —
        # the tray — can ask this to stop from outside it. See `request_stop`.
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _on_status(self, kind: str, detail: dict) -> None:
        if kind in ("connected", "synced", "stopped", "clock_warning", "halt", "bad_signature", "disconnected"):
            _say(f"[{kind}] {json.dumps(detail, default=str)[:300]}")
        # The joined set is written AS campaigns join, not at shutdown — a
        # crash is exactly when the file matters, and one written on a clean
        # exit would be missing in the one case it exists for.
        if kind == "campaign" and detail.get("joined") and detail.get("campaign_id"):
            if detail["campaign_id"] not in self._joined_ids:
                self._joined_ids.add(detail["campaign_id"])
                self._save_joined()
        elif kind == "closed" and detail.get("campaign_id") in self._joined_ids:
            # An ended campaign must not resume on the next start: its ladder
            # is history, and holding its id forever would grow the file and
            # re-follow anything a stale feed replayed.
            self._joined_ids.discard(detail["campaign_id"])
            self._save_joined()

    def _load_joined(self) -> set:
        try:
            if os.path.exists(self.config.joined_path):
                with open(self.config.joined_path, encoding="utf-8") as handle:
                    return {str(cid) for cid in json.load(handle) if cid}
        except Exception as exc:
            # Unreadable is treated as empty: the cost is the old behaviour
            # (a resumed campaign reads as late), never anything unsafe.
            _log.warning("could not read the joined-campaigns record: %s", exc)
        return set()

    def _save_joined(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.config.joined_path), exist_ok=True)
            with open(self.config.joined_path, "w", encoding="utf-8") as handle:
                json.dump(sorted(self._joined_ids), handle)
        except Exception as exc:
            _log.warning("could not write the joined-campaigns record: %s", exc)

    def _load_book(self) -> Optional[dict]:
        try:
            if os.path.exists(self.config.book_path):
                with open(self.config.book_path, encoding="utf-8") as handle:
                    return json.load(handle)
        except Exception as exc:
            # Unreadable is treated as absent: the cost is the old behaviour —
            # the pot has to be earned again — and never a refusal to start.
            _log.warning("could not read the saved book: %s", exc)
        return None

    def _save_book(self) -> None:
        """Written every tick, not only at shutdown.

        The case this exists for is the crash, and a file written on the way out
        is missing in exactly that case. Written through a temp file and
        `os.replace` so a machine losing power mid-write leaves either the old
        book or the new one, never half of either — a truncated book reads as no
        book, which silently discards a position.
        """
        if self.runtime is None:
            return
        path = self.config.book_path
        temp = f"{path}.tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(temp, "w", encoding="utf-8") as handle:
                json.dump(self.runtime.book_snapshot(), handle)
            os.replace(temp, path)
        except Exception as exc:
            _log.warning("could not write the saved book: %s", exc)

    # ── the two tasks ────────────────────────────────────────────

    async def _feed(self) -> None:
        reason = await self.transport.run()
        if reason:
            _say(f"\nFeed stopped: {reason}")
        self._stopping.set()

    async def _ticker(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.sleep(self.config.tick_seconds)
                if self.transport.client is None:
                    continue
                if self.runtime is None:
                    self.runtime = self._build_runtime()
                    # Build, restore, then wake — in that order. `on_wake`'s
                    # protect pass asks every campaign whether it holds coin
                    # with no exit resting, and against the empty book a fresh
                    # process starts with, that question has no answers. The
                    # step that exists to put a target back on unprotected coin
                    # needs the book restored before it runs.
                    for note in self.runtime.restore_book(self._load_book()):
                        self._note(note)
                    self._resume()
                for note in self.runtime.poll_fills():
                    self._note(note)
                report = self.runtime.tick()
                for note in report.notes:
                    self._note(note)
                for order_id, why in report.skipped:
                    self._note(f"[not placed] {order_id}: {why}")
                status = self.runtime.status()
                # Awake exactly while something can fill unwatched.
                sync_inhibitor(self.inhibitor, armed_exposure_usd=status["armed_exposure_usd"])
                if self._ui_state is not None:
                    from executor.ui import campaigns_view, journal_view, portfolio_view

                    self._ui_state.set_status(
                        status,
                        campaigns_view(self.runtime),
                        self.runtime.rounds_view(),
                        journal_view(self.runtime),
                        portfolio_view(self.runtime, self.adapter),
                    )
                    if self._strip is None:
                        self._strip = MarketStrip(self._market_for_ui())
                    self._ui_state.set_market(self._strip.snapshot())
                # Last, so what is on disk is the book as it stood after this
                # pass placed and noticed everything it was going to.
                self._save_book()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A bad tick is not a reason to stop managing positions. The
                # next one re-reads the exchange, which is the source of truth.
                _log.exception("tick failed")
                _say(f"[tick failed] {exc}")

    def _note(self, line: str) -> None:
        _say(line)
        if self._ui_state is not None:
            self._ui_state.add_event(line)

    def _market_for_ui(self):
        """The same venue feed the tick reads, so the chart cannot disagree
        with the levels the executor is actually trading against."""
        return ExchangeMarketData(self.adapter, self.config.exchange)

    def _build_runtime(self) -> ExecutorRuntime:
        return ExecutorRuntime(
            client=self.transport.client,
            adapter=self.adapter,
            market=ExchangeMarketData(self.adapter, self.config.exchange),
            config=RuntimeConfig(
                capital_usd=self.config.capital_usd,
                quote_asset=self.config.quote_asset,
                symbols=list(self.config.symbols),
                exchange=self.config.exchange,
                subscription_line=self.config.subscription_line,
            ),
        )

    # ── going away and coming back ───────────────────────────────

    def _resume(self) -> None:
        saved = None
        try:
            if os.path.exists(self.config.shutdown_record_path):
                with open(self.config.shutdown_record_path, encoding="utf-8") as handle:
                    saved = json.load(handle)
        except Exception as exc:
            # An unreadable record reads as a crash, which is the safe side.
            _log.warning("could not read the shutdown record: %s", exc)
        # Read BEFORE the marker is written, or every run looks like the first.
        first_run = not os.path.exists(self.config.started_marker_path)
        self._mark_started()
        report = self.runtime.on_wake(saved, first_run=first_run)
        _say("", report["message"])
        # getattr: tests drive _resume on stubs that never ran __init__, and
        # the page is an optional attachment either way.
        ui_state = getattr(self, "_ui_state", None)
        if ui_state is not None:
            ui_state.set_wake_message(report["message"])
        if report["protected"]:
            _say(f"Placed a missing target on: {', '.join(report['protected'])}")
        if report["requires_confirmation"]:
            _say("No new entries will go out until you have looked at what changed.")

    def _mark_started(self) -> None:
        """Record that this machine has run, so the next missing shutdown
        record reads as the crash it would then be.

        A failure here is logged and swallowed: the worst it costs is one more
        run that introduces itself as a first run, which is a cosmetic loss.
        Refusing to start over it would not be.
        """
        try:
            os.makedirs(os.path.dirname(self.config.started_marker_path), exist_ok=True)
            with open(self.config.started_marker_path, "w", encoding="utf-8") as handle:
                json.dump({"first_started_at": int(time.time())}, handle)
        except Exception as exc:
            _log.warning("could not write the first-start marker: %s", exc)

    def _shutdown(self) -> None:
        if self.runtime is None:
            return
        result = self.runtime.prepare_for_sleep(reason="clean")
        try:
            os.makedirs(os.path.dirname(self.config.shutdown_record_path), exist_ok=True)
            with open(self.config.shutdown_record_path, "w", encoding="utf-8") as handle:
                json.dump(result["record"], handle)
        except Exception as exc:
            _log.error("could not write the shutdown record: %s", exc)
        # After the invariants ran, so the saved book agrees with the exchange
        # about which entries were just cancelled.
        self._save_book()
        _say("", result["message"])

    def request_stop(self) -> None:
        """Ask this to wind down, from any thread.

        Both callers are off the loop: the tray's Quit item, and the OS signal
        handler when the tray owns the main thread. `asyncio.Event` is not
        thread-safe, and setting one from outside its loop can leave the waiter
        asleep — the program would then look like it is refusing to close, which
        is the exact failure `signal_handler` exists to prevent.

        Before the loop exists there is nothing to hand work to, so the Event is
        set directly: nothing is waiting on it yet, and `run()` checks it as
        soon as it starts.
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            self._stopping.set()
            return
        try:
            loop.call_soon_threadsafe(self._stopping.set)
        except RuntimeError:  # loop closed between the check and the call
            self._stopping.set()

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        self._loop = loop

        handler = signal_handler(self._stopping, loop.call_soon_threadsafe)
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass  # not the main thread, or a platform without it

        _say(f"Cascade executor — {self.config.exchange}, ${self.config.capital_usd:,.0f}", "")
        feed = asyncio.ensure_future(self._feed())
        ticker = asyncio.ensure_future(self._ticker())
        await self._stopping.wait()
        for task in (feed, ticker):
            task.cancel()
        await asyncio.gather(feed, ticker, return_exceptions=True)
        self.inhibitor.release()
        self._shutdown()
        return 0


async def _check(config: ExecutorConfig) -> int:
    """
    Prove the config and the connection without placing anything.

    Every leg is tried and reported even after one fails. Stopping at the first
    problem would mean a buyer with a typo in their API key never finds out
    whether their feed works — and they would fix one thing, run it again, and
    find the next.
    """
    executor = Executor(config)
    # ensure_ascii=False: the redacted key reads "…1234", which escapes to
    # "…1234" by default and makes the one output we ask buyers to send us
    # look like something has gone wrong. Keep it readable.
    _say("Config:", json.dumps(config.redacted(), indent=2, ensure_ascii=False))
    _say("", f"Public key to register: {executor.identity.public_key_b64()}", "")

    results = []

    try:
        keys, _ = executor.transport._keyset.refresh(config.server_url)
        results.append(("Key set", True, f"verified against your root key, {len(keys)} active signing key(s)"))
    except TransportStopped as exc:
        results.append(("Key set", False, f"refused — {exc}"))
    except Exception as exc:
        results.append(("Key set", False, f"could not fetch — {exc}"))

    try:
        balance = executor.adapter.free_balance(config.quote_asset)
        results.append(("Exchange", True, f"reachable, {balance:,.2f} {config.quote_asset} free"))
    except Exception as exc:
        results.append(("Exchange", False, str(exc)[:160]))

    connected = asyncio.Event()
    problem = {}

    def watch(kind, detail):
        if kind in ("connected", "synced"):
            connected.set()
        elif kind in ("stopped", "disconnected"):
            problem.setdefault("why", detail.get("reason") or detail.get("error") or "closed")
            connected.set()

    executor.transport._on_status = watch
    feed = asyncio.ensure_future(executor.transport.run(max_sessions=1))
    try:
        await asyncio.wait_for(connected.wait(), timeout=20)
        if problem:
            results.append(("Feed", False, str(problem["why"])[:160]))
        else:
            campaigns = len(executor.transport.client.campaigns) if executor.transport.client else 0
            results.append(("Feed", True, f"connected and snapshotted, {campaigns} campaign(s) following"))
    except asyncio.TimeoutError:
        results.append(("Feed", False, "no welcome within 20s"))
    finally:
        feed.cancel()
        await asyncio.gather(feed, return_exceptions=True)

    for name, ok, detail in results:
        _say(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")

    failed = [name for name, ok, _ in results if not ok]
    if failed:
        _say("", f"Not ready: {', '.join(failed)}. Nothing was placed.")
        return 1
    _say("", "All good. Nothing was placed.", "", irreducible_risk())
    return 0


def _connect(url: str):
    from executor.transport import websockets_connect

    return websockets_connect(url)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="executor", description=__doc__)
    parser.add_argument("--config", help="path to config.json")
    parser.add_argument("--check", action="store_true", help="verify config and connection, trade nothing")
    parser.add_argument("--register", action="store_true", help="print the public key to register, then stop")
    parser.add_argument("--sample-config", action="store_true", help="print a starter config file")
    parser.add_argument("--no-ui", action="store_true", help="run without the local status page")
    parser.add_argument("--ui-port", type=int, default=7757, help="port for the local status page")
    parser.add_argument("--setup", action="store_true", help="fill in the config from a page in your browser")
    parser.add_argument("--setup-port", type=int, default=setup.DEFAULT_PORT, help="port for the setup page")
    parser.add_argument("--tray", action="store_true", help="keep running under a menu-bar icon")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.sample_config:
        _say(json.dumps(SAMPLE, indent=2))
        return 0

    try:
        config = load(args.config)
        # Asked for, or simply needed. A machine with nothing configured used to
        # print "Missing: server_url, buyer_id, …" and exit 2, which is a
        # correct message and a dead end for anyone who was never going to open
        # a text editor. It now offers the page instead.
        # Offered automatically only on the plain run path. `--check` and
        # `--register` are things a buyer runs deliberately, often from a
        # script or while reading the guide, and a command that answers by
        # opening a browser and blocking is not one they asked for.
        wants_setup = args.setup or (not args.register and not args.check and setup.needs_setup(config))
        if wants_setup:
            done = _run_setup(config, port=args.setup_port, path=args.config, asked=args.setup)
            if done != 0:
                return done
            config = load(args.config)
        if args.register:
            # Deliberately before validate(): a buyer registers their key
            # BEFORE they have a subscription, so demanding a full config here
            # would be asking them to finish step three to complete step one.
            identity = ExecutorIdentity.load_or_create(config.buyer_key_path, config.buyer_id or "unset")
            _say(identity.public_key_b64())
            return 0
        config.validate()
    except ConfigError as exc:
        _say(f"Config problem: {exc}")
        return 2

    power = detect()
    if not power.can_cancel_on_suspend:
        _say(f"Note: {power.note}", "")

    if args.check:
        return asyncio.run(_check(config))

    # Before anything opens a socket or a key. Two executors share an account
    # and a state directory but not a book, so both would collect the same
    # rungs and both would place — double the intended position, on top of a
    # record neither can trust.
    lock = InstanceLock(lock_path(config.state_dir))
    try:
        lock.acquire()
    except AlreadyRunning as exc:
        _say(str(exc))
        return 3

    executor = Executor(config)
    server = None
    if not args.no_ui:
        from executor.ui import wire

        server = wire(executor, port=args.ui_port, say=_say)
    try:
        if args.tray:
            from executor.tray import TrayUnavailable, run_with_tray

            try:
                return run_with_tray(executor, console_url=server.url if server else "")
            except TrayUnavailable as exc:
                # Not fatal. A machine that can trade but cannot draw an icon
                # should trade — refusing to start over the menu bar would be
                # the tail wagging the dog.
                _say(f"{exc}", "")
        return asyncio.run(executor.run())
    finally:
        if server:
            server.stop()
        lock.release()


def _run_setup(config, *, port: int, path: Optional[str], asked: bool) -> int:
    """Serve the setup page and wait for the buyer to finish it.

    Blocks rather than returning to a half-configured start. There is nothing
    useful to do in the meantime: without a server URL there is no feed to
    connect, and without a key there is nothing to place.
    """
    server = setup.SetupServer(port=port, config_path=path)
    problem = server.start()
    if problem:
        _say(problem)
        return 2
    if asked:
        _say("", "Setup is open at " + server.url)
    else:
        _say(
            "",
            "This machine has not been set up yet, so I have opened the page that does it:",
            f"    {server.url}",
            "",
            "It asks for six things and takes about a minute. Nothing trades until it is done.",
        )
    try:
        webbrowser.open(server.url)
    except Exception:  # pragma: no cover - a headless box has nothing to open
        pass
    try:
        server.wait()
    except KeyboardInterrupt:
        _say("", "Setup cancelled. Nothing was saved.")
        return 130
    finally:
        server.stop()
    _say("", server.result.get("message", "Set up."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
