"""
engine/trade_journal.py — turn raw exchange fills into journal trades.

The journal was a static export of a Google Sheet, so anything traded by hand on
Binance simply never appeared. This pairs the account's own fills into the same
shape the sheet used: a run of buys that accumulate a position, then the sell
(or sells) that close it.

That shape is not arbitrary — it is how these trades are actually placed. Buy in
pieces on the way down, sell the lot at a target. So a "trade" here is one
round trip from flat to flat, not one fill.

**Fees are subtracted.** The exchange reports commission per fill, and
`pnl_usd` here is net of it. The Cascade engine's own P&L is gross, so the same
round can read differently in the two places — see AUDIT.md §1.2. This module
also carries `pnl_gross_usd` so the difference stays visible rather than being
quietly reconciled.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

IST = timezone(timedelta(hours=5, minutes=30))

# When a position counts as closed.
#
# The test for leftover coin is its VALUE, not its share of the position. A
# fraction rule gets BTC wrong: one lot step there is around 12% of a minimum
# order (RUNBOOK §7), so selling everything the exchange would let you sell can
# still leave 9% of the quantity behind — and a 1% fraction rule called that a
# half-open trade forever. What actually matters is whether the remainder could
# be sold at all: under Binance's ~$5 minNotional it cannot, so the trade is
# done whether the number is zero or not.
_MIN_SELLABLE_USD = 5.0
_FLAT_EPSILON = 1e-9


def _f(value, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result else default  # NaN check


def _is_buy(fill: dict) -> bool:
    if "isBuyer" in fill and fill.get("isBuyer") is not None:
        return bool(fill.get("isBuyer"))
    return str(fill.get("side") or "").strip().lower() in {"buy", "bid"}


def _fill_time_ms(fill: dict) -> int:
    for key in ("time", "timestamp", "created_at"):
        value = fill.get(key)
        if value is not None:
            return int(_f(value))
    return 0


def _ist_date(ms: int) -> str:
    if ms <= 0:
        return ""
    return datetime.fromtimestamp(ms / 1000, IST).strftime("%Y-%m-%d")


def _fee_usd(fill: dict) -> float:
    """Commission in quote currency.

    The broker layer already converts BNB-paid commission for us; prefer that
    over the raw native figure, which would otherwise be added as if it were
    dollars.
    """
    fee = fill.get("paid_commission")
    if fee is not None:
        return abs(_f(fee))
    asset = str(fill.get("commissionAsset") or "").upper()
    symbol = str(fill.get("symbol") or "").upper()
    native = abs(_f(fill.get("commission")))
    if not native:
        return 0.0
    # Only safe when the commission was charged in the quote asset itself.
    if asset and symbol.endswith(asset):
        return native
    if asset and asset in {"USDT", "USDC", "BUSD", "FDUSD"}:
        return native
    return 0.0


class _OpenPosition:
    """Buys accumulated since the last time this symbol was flat."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.buys: list[dict] = []
        self.qty = 0.0
        self.cost = 0.0  # gross spent on base, excluding fees
        self.fees = 0.0
        self.sell_qty = 0.0
        self.sell_proceeds = 0.0
        self.opened_ms = 0
        self.closed_ms = 0

    @property
    def bought_qty(self) -> float:
        return sum(_f(b["quantity"]) for b in self.buys)

    def add_buy(self, fill: dict) -> None:
        qty = abs(_f(fill.get("qty") or fill.get("size") or fill.get("quantity")))
        price = _f(fill.get("price") or fill.get("fill_price"))
        if qty <= 0 or price <= 0:
            return
        quote = abs(_f(fill.get("quoteQty") or fill.get("quote_size"))) or qty * price
        if not self.buys:
            self.opened_ms = _fill_time_ms(fill)
        self.qty += qty
        self.cost += quote
        self.fees += _fee_usd(fill)
        first_price = _f(self.buys[0]["buy_price"]) if self.buys else price
        self.buys.append(
            {
                "buy_no": len(self.buys) + 1,
                "buy_price": round(price, 8),
                "quantity": round(qty, 8),
                "amount_usd": round(quote, 4),
                # How far under the first buy this one landed — the sheet's
                # "market_down_pct" column, which is what makes a ladder
                # readable at a glance.
                "market_down_pct": round((first_price - price) / first_price * 100, 3) if first_price else 0.0,
                "ts": _fill_time_ms(fill),
            }
        )

    def add_sell(self, fill: dict) -> None:
        qty = abs(_f(fill.get("qty") or fill.get("size") or fill.get("quantity")))
        price = _f(fill.get("price") or fill.get("fill_price"))
        if qty <= 0 or price <= 0:
            return
        quote = abs(_f(fill.get("quoteQty") or fill.get("quote_size"))) or qty * price
        self.qty -= qty
        self.sell_qty += qty
        self.sell_proceeds += quote
        self.fees += _fee_usd(fill)
        self.closed_ms = _fill_time_ms(fill)

    def is_flat(self) -> bool:
        bought = self.bought_qty
        if bought <= 0 or self.sell_qty <= 0:
            return False
        if self.qty <= _FLAT_EPSILON:
            return True
        # Value the remainder at what it just sold for — that is the price it
        # would have to be sold at, and the only one we know.
        last_price = (self.sell_proceeds / self.sell_qty) if self.sell_qty > 0 else 0.0
        if last_price <= 0:
            return False
        return self.qty * last_price < _MIN_SELLABLE_USD

    def to_trade(self, index: int, closed: bool) -> dict:
        bought = self.bought_qty
        avg_buy = (self.cost / bought) if bought > 0 else 0.0
        # Only the part actually sold has a realised result. Dust left behind is
        # reported separately rather than valued at the sell price, which would
        # book a profit on coin still sitting in the account.
        sold = min(self.sell_qty, bought)
        matched_cost = avg_buy * sold
        gross = self.sell_proceeds - matched_cost
        net = gross - self.fees
        avg_sell = (self.sell_proceeds / self.sell_qty) if self.sell_qty > 0 else 0.0
        date = _ist_date(self.closed_ms or self.opened_ms)
        stamp = (date or "").replace("-", "")[2:]
        return {
            "trade_id": f"{self.symbol}-{stamp}-{index}",
            "date": date,
            "coin": self.symbol,
            "avg_buy_price": round(avg_buy, 8),
            "total_qty": round(bought, 8),
            "invested_usd": round(self.cost, 4),
            "sell_price": round(avg_sell, 8),
            "pnl_usd": round(net, 4),
            "pnl_gross_usd": round(gross, 4),
            "fees_usd": round(self.fees, 4),
            "roi_pct": round(net / matched_cost * 100, 3) if matched_cost > 0 else 0.0,
            "status": "Closed" if closed else "Open",
            "buy_count": len(self.buys),
            "buys": [{k: v for k, v in b.items() if k != "ts"} for b in self.buys],
            "residual_qty": round(max(self.qty, 0.0), 8),
            "opened_ts": int(self.opened_ms),
            "closed_ts": int(self.closed_ms),
            "source": "binance",
        }


def pair_fills_into_trades(fills: Iterable[dict], *, include_open: bool = True) -> list[dict]:
    """Round trips, oldest first, from a flat list of exchange fills.

    Fills may arrive in any order and for any mix of symbols; each symbol is
    tracked on its own. A sell with no position behind it is ignored rather than
    inventing a short — this is a spot account, so that only happens when the
    history window starts mid-position.
    """
    by_symbol: dict[str, list[dict]] = {}
    for fill in fills or []:
        if not isinstance(fill, dict):
            continue
        symbol = str(fill.get("symbol") or fill.get("product_symbol") or "").upper()
        if not symbol:
            continue
        by_symbol.setdefault(symbol, []).append(fill)

    trades: list[dict] = []
    for symbol in sorted(by_symbol):
        ordered = sorted(by_symbol[symbol], key=_fill_time_ms)
        position: Optional[_OpenPosition] = None
        counter = 0
        for fill in ordered:
            if _is_buy(fill):
                if position is None:
                    position = _OpenPosition(symbol)
                position.add_buy(fill)
                continue
            if position is None or position.bought_qty <= 0:
                continue  # a sell with nothing behind it: history starts mid-position
            position.add_sell(fill)
            if position.is_flat():
                counter += 1
                trades.append(position.to_trade(counter, closed=True))
                position = None
        if position is not None and position.bought_qty > 0 and include_open:
            counter += 1
            trades.append(position.to_trade(counter, closed=False))

    trades.sort(key=lambda t: t.get("closed_ts") or t.get("opened_ts") or 0)
    return trades


def merge_with_sheet(sheet_trades: Iterable[dict], broker_trades: Iterable[dict]) -> list[dict]:
    """Broker record wins wherever it reaches; the sheet fills in the past.

    The exchange is authoritative for anything it still has history for, and it
    knows the fees. But `myTrades` only returns a bounded window per symbol, so
    the hand-kept sheet is still the only record of anything older. Splitting on
    the earliest broker fill keeps exactly one row per real trade instead of
    trying to fuzzy-match two sources against each other.
    """
    broker = [dict(t) for t in broker_trades or []]
    sheet = [dict(t) for t in sheet_trades or []]
    for row in sheet:
        row.setdefault("source", "sheet")

    if not broker:
        return sheet
    earliest = min((str(t.get("date") or "") for t in broker if t.get("date")), default="")
    if not earliest:
        return sheet + broker
    kept = [row for row in sheet if str(row.get("date") or "") < earliest]
    merged = kept + broker
    merged.sort(key=lambda t: str(t.get("date") or ""))
    return merged
