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


@dataclass
class ExecutorConfig:
    server_url: str
    buyer_id: str
    root_public_key: str
    exchange: str = "binance"
    capital_usd: float = 0.0
    symbols: List[str] = field(default_factory=list)
    quote_asset: str = "USDT"
    tick_seconds: int = 20
    # Secrets, never written back out. See `redacted()`.
    api_key: str = ""
    api_secret: str = ""
    # Where this machine keeps its own things.
    state_dir: str = DEFAULT_DIR

    @property
    def buyer_key_path(self) -> str:
        return os.path.join(os.path.expanduser(self.state_dir), "buyer_key.pem")

    @property
    def keyset_cache_path(self) -> str:
        return os.path.join(os.path.expanduser(self.state_dir), "keyset.json")

    @property
    def shutdown_record_path(self) -> str:
        return os.path.join(os.path.expanduser(self.state_dir), "shutdown.json")

    def redacted(self) -> dict:
        """Safe to print, log, or paste into a support conversation."""
        return {
            "server_url": self.server_url,
            "buyer_id": self.buyer_id,
            "exchange": self.exchange,
            "capital_usd": self.capital_usd,
            "symbols": self.symbols or ["(all)"],
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
        quote_asset=str(data.get("quote_asset") or "USDT").upper(),
        tick_seconds=int(env.get("CASCADE_TICK_SECONDS") or data.get("tick_seconds") or 20),
        api_key=str(env.get("CASCADE_API_KEY") or data.get("api_key") or ""),
        api_secret=str(env.get("CASCADE_API_SECRET") or data.get("api_secret") or ""),
        state_dir=str(env.get("CASCADE_STATE_DIR") or data.get("state_dir") or DEFAULT_DIR),
    )
    if env.get("CASCADE_SYMBOLS"):
        config.symbols = [s.strip().upper() for s in str(env["CASCADE_SYMBOLS"]).split(",") if s.strip()]
    return config


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
    "tick_seconds": 20,
}
