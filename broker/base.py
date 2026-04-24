"""
broker/base.py — shared broker contract for CryptoForge.

The existing engines were written against Delta-specific method names.
This base class provides generic broker metadata plus backwards-compatible
symbol helpers so multiple brokers can satisfy the same runtime contract.
"""

from __future__ import annotations

from typing import Iterable, Set


class BaseBroker:
    broker_name = "broker"
    display_name = "Broker"

    _SYMBOL_ALIASES = {
        "GOLD": "PAXGUSD",
        "GOLDUSDT": "PAXGUSD",
        "PAXGUSDT": "PAXGUSD",
    }

    @classmethod
    def normalize_app_symbol(cls, symbol: str) -> str:
        raw = str(symbol or "").strip().upper()
        return cls._SYMBOL_ALIASES.get(raw, raw)

    def to_broker_symbol(self, symbol: str) -> str:
        return self.normalize_app_symbol(symbol)

    def from_broker_symbol(self, symbol: str) -> str:
        return self.normalize_app_symbol(symbol)

    # Backwards-compatible aliases kept so the current engines and app
    # can switch brokers without a risky full rename in the same patch.
    def to_delta_symbol(self, symbol: str) -> str:
        return self.to_broker_symbol(symbol)

    def from_delta_symbol(self, symbol: str) -> str:
        return self.from_broker_symbol(symbol)

    def get_supported_symbols(self) -> Set[str]:
        products = []
        get_products = getattr(self, "get_perpetual_futures", None)
        if callable(get_products):
            try:
                products = list(get_products() or [])
            except Exception:
                products = []
        symbols = set()
        for product in products:
            symbol = self.from_broker_symbol(product.get("symbol", ""))
            if symbol:
                symbols.add(symbol)
        return symbols

    def get_market_feed_kind(self) -> str:
        return "polling"

    @staticmethod
    def build_standard_leverage_options(max_leverage: int) -> list[int]:
        standard = [1, 2, 3, 5, 10, 15, 20, 25, 50, 75, 100, 125, 150, 200]
        max_lev = max(int(max_leverage or 1), 1)
        options = [value for value in standard if value <= max_lev]
        if max_lev not in options:
            options.append(max_lev)
        return sorted(set(options))

    @staticmethod
    def coerce_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def unique(values: Iterable[str]) -> list[str]:
        seen = set()
        ordered = []
        for value in values:
            if not value or value in seen:
                continue
            seen.add(value)
            ordered.append(value)
        return ordered
