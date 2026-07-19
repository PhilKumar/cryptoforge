import os

from .base import BaseBroker
from .binance import BinanceSpotClient
from .coindcx import CoinDCXClient
from .delta import DeltaClient

DEFAULT_BROKER = "binance"

_BROKER_FACTORIES = {
    "delta": DeltaClient,
    "coindcx": CoinDCXClient,
    "binance": BinanceSpotClient,
}

# Legacy names kept so previously persisted selections keep working.
_BROKER_NAME_ALIASES = {
    "binance_spot": "binance",
    "binance_futures": "binance",
}


def get_broker_name() -> str:
    raw = os.getenv("CRYPTOFORGE_BROKER") or os.getenv("BROKER") or DEFAULT_BROKER
    name = str(raw).strip().lower()
    return _BROKER_NAME_ALIASES.get(name, name)


def get_broker_client(name: str | None = None) -> BaseBroker:
    broker_name = str(name or get_broker_name()).strip().lower()
    broker_name = _BROKER_NAME_ALIASES.get(broker_name, broker_name)
    factory = _BROKER_FACTORIES.get(broker_name)
    if factory is None:
        supported = ", ".join(sorted(_BROKER_FACTORIES))
        raise ValueError(f"Unsupported broker '{broker_name}'. Supported brokers: {supported}")
    return factory()


def get_supported_brokers() -> list[str]:
    return sorted(_BROKER_FACTORIES)


__all__ = [
    "BaseBroker",
    "BinanceSpotClient",
    "CoinDCXClient",
    "DEFAULT_BROKER",
    "DeltaClient",
    "get_broker_client",
    "get_broker_name",
    "get_supported_brokers",
]
