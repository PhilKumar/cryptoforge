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
recovery design exists to prevent.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from typing import Optional

from executor.config import SAMPLE, ConfigError, ExecutorConfig, build_adapter, load
from executor.market import ExchangeMarketData
from executor.power import SleepInhibitor, detect, sync_inhibitor
from executor.report import irreducible_risk
from executor.runtime import ExecutorRuntime, RuntimeConfig
from executor.transport import ExecutorIdentity, FeedTransport, KeySetStore, TransportStopped

_log = logging.getLogger("cascade.executor")


def _say(*lines) -> None:
    for line in lines:
        print(line, flush=True)


class Executor:
    def __init__(self, config: ExecutorConfig):
        self.config = config
        self.identity = ExecutorIdentity.load_or_create(config.buyer_key_path, config.buyer_id)
        self.adapter = build_adapter(config)
        self.transport = FeedTransport(
            base_url=config.server_url,
            identity=self.identity,
            keyset=KeySetStore(
                root_public_b64=config.root_public_key,
                cache_path=config.keyset_cache_path,
            ),
            connect_fn=_connect,
            on_status=self._on_status,
        )
        self.runtime: Optional[ExecutorRuntime] = None
        self.inhibitor = SleepInhibitor()
        self._stopping = asyncio.Event()

    def _on_status(self, kind: str, detail: dict) -> None:
        if kind in ("connected", "synced", "stopped", "clock_warning", "halt", "bad_signature", "disconnected"):
            _say(f"[{kind}] {json.dumps(detail, default=str)[:300]}")

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
                    self._resume()
                for note in self.runtime.poll_fills():
                    _say(note)
                report = self.runtime.tick()
                for note in report.notes:
                    _say(note)
                for order_id, why in report.skipped:
                    _say(f"[not placed] {order_id}: {why}")
                status = self.runtime.status()
                # Awake exactly while something can fill unwatched.
                sync_inhibitor(self.inhibitor, armed_exposure_usd=status["armed_exposure_usd"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A bad tick is not a reason to stop managing positions. The
                # next one re-reads the exchange, which is the source of truth.
                _log.exception("tick failed")
                _say(f"[tick failed] {exc}")

    def _build_runtime(self) -> ExecutorRuntime:
        return ExecutorRuntime(
            client=self.transport.client,
            adapter=self.adapter,
            market=ExchangeMarketData(self.adapter, self.config.exchange),
            config=RuntimeConfig(
                capital_usd=self.config.capital_usd,
                quote_asset=self.config.quote_asset,
                symbols=list(self.config.symbols),
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
        report = self.runtime.on_wake(saved)
        _say("", report["message"])
        if report["protected"]:
            _say(f"Placed a missing target on: {', '.join(report['protected'])}")
        if report["requires_confirmation"]:
            _say("No new entries will go out until you have looked at what changed.")

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
        _say("", result["message"])

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stopping.set)
            except NotImplementedError:
                pass  # Windows without a proactor loop; Ctrl-C still raises.

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
    _say("Config:", json.dumps(config.redacted(), indent=2))
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
    return asyncio.run(Executor(config).run())


if __name__ == "__main__":
    sys.exit(main())
