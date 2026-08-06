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
    # Funding and open interest only exist on perpetual/derivatives venues.
    # Spot clients set this False so the UI can hide meaningless chips.
    supports_funding = True
    # Whether Cascade can actually trade this venue. It needs stop-limit buys
    # carrying a client order id, and a spot wallet reporting free vs locked.
    # Default False so a new or legacy adapter is never offered as a Cascade
    # venue by accident — the futures and Delta clients would raise on their
    # first entry, mid-campaign, with money committed.
    supports_cascade = False
    # The fastest candle a campaign may run on here. Fees are charged per
    # round trip, so a dearer venue needs a deeper fall before the target
    # beats its own commission — and the fastest timeframes are where falls
    # are shallowest. "5m" means no restriction.
    min_timeframe = "5m"
    # Commission per side, in percent, used to model fees when the venue does
    # not report a per-order figure. It belongs to the exchange, not the engine:
    # a target priced with one venue's rate on another venue's book sells below
    # its own commission. Adapters override with their measured rate.
    fee_pct_per_side = 0.1

    def get_convert_history(self, days: int = 30) -> list:
        """Off-orderbook conversions. Only Binance Spot has these; the default
        keeps every caller free of hasattr checks."""
        return []

    def get_order_commission(self, symbol: str, order_id) -> float | None:
        """What one order actually cost in commission, in quote currency.

        None means "could not be established" and must never be read as zero —
        a missing figure is not a free trade. Callers fall back to their own
        modelled rate and mark the result as an estimate.
        """
        return None

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
