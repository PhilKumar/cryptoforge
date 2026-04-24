import os

from .base import BaseBroker
from .coindcx import CoinDCXClient
from .delta import DeltaClient

_BROKER_FACTORIES = {
    "delta": DeltaClient,
    "coindcx": CoinDCXClient,
}


def get_broker_name() -> str:
    raw = os.getenv("CRYPTOFORGE_BROKER") or os.getenv("BROKER") or "delta"
    return str(raw).strip().lower()


def get_broker_client(name: str | None = None) -> BaseBroker:
    broker_name = str(name or get_broker_name()).strip().lower()
    factory = _BROKER_FACTORIES.get(broker_name)
    if factory is None:
        supported = ", ".join(sorted(_BROKER_FACTORIES))
        raise ValueError(f"Unsupported broker '{broker_name}'. Supported brokers: {supported}")
    return factory()


def get_supported_brokers() -> list[str]:
    return sorted(_BROKER_FACTORIES)


__all__ = [
    "BaseBroker",
    "CoinDCXClient",
    "DeltaClient",
    "get_broker_client",
    "get_broker_name",
    "get_supported_brokers",
]
