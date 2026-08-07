"""
executor/config.py — everything the buyer has to tell their own machine.

One JSON file plus environment overrides. Secrets — the exchange API key and
secret — are read from the environment by default rather than the file, so the
ordinary case leaves nothing sensitive on disk in a file people paste into
support chats.

Nothing here is sent anywhere. The config describes what this machine does; the
server learns only the buyer's public key, at connect.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

DEFAULT_DIR = "~/.cascade-executor"
DEFAULT_CONFIG = f"{DEFAULT_DIR}/config.json"


class ConfigError(Exception):
    """The config cannot be used. Message is meant for the buyer, not a log."""


def subscription_phrase(timeframes, signal_exchanges, symbols) -> str:
    """What a subscription covers, in one line a buyer can check at a glance.

    Shown wherever a quiet console might otherwise be read as a broken one:
    "I did not buy that" and "something is wrong" look identical until
    somebody says which.
    """
    tf = "/".join(timeframes) if timeframes else "all timeframes"
    venue = "/".join(signal_exchanges) if signal_exchanges else "all venues"
    coins = ", ".join(symbols) if symbols else "all coins"
    return f"{tf} · drawn on {venue} · {coins}"


@dataclass
class ExecutorConfig:
    server_url: str
    buyer_id: str
    root_public_key: str
    exchange: str = "binance"
    capital_usd: float = 0.0
    symbols: List[str] = field(default_factory=list)
    # Which signals this subscription is for. Empty means all of them.
    #
    # A 5m campaign and a 15m one are different products, not settings: they
    # differ in pace, in how many entries a day they ask for, and in which
    # venues can carry them at all. A buyer whose exchange suits the slower
    # one should not be handed the faster one because nobody asked.
    timeframes: List[str] = field(default_factory=list)
    # The venue whose CANDLES drew the geometry. NOT a free choice: it always
    # equals `exchange`, because a buyer fills at their own venue's prices and
    # geometry drawn on a different series would be a different trade. `load()`
    # derives it; the settings page shows it and does not offer to change it.
    signal_exchanges: List[str] = field(default_factory=list)
    quote_asset: str = "USDT"
    tick_seconds: int = 20
    # Secrets, never written back out. See `redacted()`.
    api_key: str = ""
    api_secret: str = ""
    # Where this machine keeps its own things.
    state_dir: str = DEFAULT_DIR
    # The file this was read from, so a settings change can be written back to
    # the same place rather than guessing. Empty when nothing was on disk.
    source_path: str = ""

    @property
    def buyer_key_path(self) -> str:
        return os.path.join(os.path.expanduser(self.state_dir), "buyer_key.pem")

    @property
    def keyset_cache_path(self) -> str:
        return os.path.join(os.path.expanduser(self.state_dir), "keyset.json")

    @property
    def shutdown_record_path(self) -> str:
        return os.path.join(os.path.expanduser(self.state_dir), "shutdown.json")

    @property
    def joined_path(self) -> str:
        """Campaigns this machine is in, so a restart does not abandon them.

        Written as they are joined rather than at shutdown: a crash is exactly
        when this matters, and a file only written on a clean exit would be
        missing in the one case it exists for.
        """
        return os.path.join(os.path.expanduser(self.state_dir), "joined.json")

    @property
    def book_path(self) -> str:
        """The pot, the position and the round history, so a restart keeps them.

        None of it can be rebuilt from anywhere else. The feed publishes
        geometry, not what this buyer collected against it; the exchange knows
        the coin but not which rungs paid for it, where the floor from their
        last round sits, or how much of a fall is already in the pot. Losing the
        file means the fall has to be earned again from wherever price is now.
        """
        return os.path.join(os.path.expanduser(self.state_dir), "book.json")

    @property
    def started_marker_path(self) -> str:
        """Written once, on the first start, and never removed.

        A missing shutdown record means a crash — except on a first install,
        where it only means there has never been a shutdown. Nothing else in
        the state dir separates the two: the buyer key is created before the
        wake ladder runs, so it exists in both cases.
        """
        return os.path.join(os.path.expanduser(self.state_dir), "started.json")

    @property
    def subscription_line(self) -> str:
        """The signals this machine is subscribed to, in one phrase."""
        return subscription_phrase(self.timeframes, self.signal_exchanges, self.symbols)

    def redacted(self) -> dict:
        """Safe to print, log, or paste into a support conversation."""
        return {
            "server_url": self.server_url,
            "buyer_id": self.buyer_id,
            "exchange": self.exchange,
            "capital_usd": self.capital_usd,
            "symbols": self.symbols or ["(all)"],
            "timeframes": self.timeframes or ["(all)"],
            "signal_exchanges": self.signal_exchanges or ["(all)"],
            "tick_seconds": self.tick_seconds,
            "state_dir": self.state_dir,
            "api_key": f"…{self.api_key[-4:]}" if self.api_key else "(not set)",
            "api_secret": "(set)" if self.api_secret else "(not set)",
        }

    def validate(self) -> None:
        """
        Refuse to start on anything that would fail later and less clearly.

        Every check here is one that would otherwise surface as a confusing
        runtime error — an unsigned handshake, an order rejected for no
        balance, a ladder that cannot place its shallowest rung — at a moment
        when the buyer is less able to do anything about it.
        """
        missing = [
            name
            for name, value in (
                ("server_url", self.server_url),
                ("buyer_id", self.buyer_id),
                ("root_public_key", self.root_public_key),
                ("api_key", self.api_key),
                ("api_secret", self.api_secret),
            )
            if not str(value or "").strip()
        ]
        if missing:
            raise ConfigError(
                "Missing: " + ", ".join(missing) + ". API credentials come from "
                "CASCADE_API_KEY / CASCADE_API_SECRET unless they are in the config file."
            )
        if self.exchange not in ("binance", "coindcx"):
            raise ConfigError(f"Unknown exchange {self.exchange!r}. Supported: binance, coindcx.")
        if self.capital_usd <= 0:
            raise ConfigError("capital_usd must be set — it is what every ladder is sized from.")
        if self.tick_seconds < 5:
            raise ConfigError("tick_seconds under 5 will rate-limit the exchange without trading better.")


def load(path: Optional[str] = None, *, environ: Optional[dict] = None) -> ExecutorConfig:
    """
    Read the config file, then let the environment win.

    Environment last on purpose: it is where the secrets belong, and it is what
    a container or a launch agent can set without rewriting a file.
    """
    env = os.environ if environ is None else environ
    resolved = os.path.abspath(os.path.expanduser(path or env.get("CASCADE_CONFIG") or DEFAULT_CONFIG))
    data = {}
    if os.path.exists(resolved):
        try:
            with open(resolved, encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{resolved} is not valid JSON: {exc}") from exc
    elif path:
        raise ConfigError(f"No config at {resolved}")

    config = ExecutorConfig(
        server_url=str(env.get("CASCADE_SERVER_URL") or data.get("server_url") or ""),
        buyer_id=str(env.get("CASCADE_BUYER_ID") or data.get("buyer_id") or ""),
        root_public_key=str(env.get("CASCADE_ROOT_PUBLIC_KEY") or data.get("root_public_key") or ""),
        exchange=str(env.get("CASCADE_EXCHANGE") or data.get("exchange") or "binance").lower(),
        capital_usd=float(env.get("CASCADE_CAPITAL_USD") or data.get("capital_usd") or 0.0),
        symbols=list(data.get("symbols") or []),
        timeframes=[str(tf).strip().lower() for tf in (data.get("timeframes") or []) if str(tf).strip()],
        signal_exchanges=[str(x).strip().lower() for x in (data.get("signal_exchanges") or []) if str(x).strip()],
        quote_asset=str(data.get("quote_asset") or "USDT").upper(),
        tick_seconds=int(env.get("CASCADE_TICK_SECONDS") or data.get("tick_seconds") or 20),
        api_key=str(env.get("CASCADE_API_KEY") or data.get("api_key") or ""),
        api_secret=str(env.get("CASCADE_API_SECRET") or data.get("api_secret") or ""),
        state_dir=str(env.get("CASCADE_STATE_DIR") or data.get("state_dir") or DEFAULT_DIR),
        source_path=resolved,
    )
    if env.get("CASCADE_SYMBOLS"):
        config.symbols = [s.strip().upper() for s in str(env["CASCADE_SYMBOLS"]).split(",") if s.strip()]
    # Derived, not chosen. Geometry drawn on another venue's candles is not the
    # trade this machine can make — they fill at THEIR exchange's prices, so
    # the signals they follow are the ones drawn on it. Forced at load so a
    # config left inconsistent by hand, or by an older version, corrects itself
    # on the next start rather than quietly following the wrong series.
    config.signal_exchanges = [config.exchange]
    if env.get("CASCADE_TIMEFRAMES"):
        config.timeframes = [t.strip().lower() for t in str(env["CASCADE_TIMEFRAMES"]).split(",") if t.strip()]
    if env.get("CASCADE_SIGNAL_EXCHANGES"):
        config.signal_exchanges = [
            x.strip().lower() for x in str(env["CASCADE_SIGNAL_EXCHANGES"]).split(",") if x.strip()
        ]
    return config


SETTABLE_KEYS = ("timeframes", "signal_exchanges", "symbols", "exchange", "capital_usd")


def save_settings(config: ExecutorConfig, changes: dict) -> str:
    """
    Write changed settings back to the buyer's config file.

    Merged into the file rather than rewritten from the live config, for two
    reasons. The environment overrides the file at load, so a config whose
    exchange came from `CASCADE_EXCHANGE` would otherwise have that value
    burned into the file, silently outliving the variable. And the file may
    hold keys we do not model — comments people add, fields from a newer
    version — which are not ours to delete.

    Secrets are never written. If a buyer put their API key in the file
    themselves, it stays exactly as they left it; nothing here adds one.
    """
    path = config.source_path or os.path.abspath(os.path.expanduser(DEFAULT_CONFIG))
    data = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{path} is not valid JSON, so it cannot be updated safely: {exc}") from exc
    for key, value in changes.items():
        if key not in SETTABLE_KEYS:
            raise ConfigError(f"{key} is not a setting this page may change.")
        data[key] = value
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Written via a neighbour and renamed: a half-written config is one a
    # restart cannot read, and the restart is exactly when it is read.
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
    os.replace(temporary, path)
    return path


def build_adapter(config: ExecutorConfig):
    """The venue's adapter, with the buyer's own credentials."""
    if config.exchange == "coindcx":
        from executor.coindcx import CoinDCXSpotAdapter

        return CoinDCXSpotAdapter(api_key=config.api_key, api_secret=config.api_secret)
    from executor.binance import BinanceSpotAdapter

    return BinanceSpotAdapter(api_key=config.api_key, api_secret=config.api_secret)


SAMPLE = {
    "server_url": "https://crypto.philforge.in",
    "buyer_id": "buyer-your-name",
    "root_public_key": "(the base64 root key from your subscription email)",
    "exchange": "binance",
    "capital_usd": 3000,
    "symbols": [],
    # Empty means every signal. Narrow it to the product you subscribed to:
    # e.g. ["15m"] with "signal_exchanges": ["coindcx"], or ["5m"] on binance.
    "timeframes": [],
    "signal_exchanges": [],
    "tick_seconds": 20,
}
