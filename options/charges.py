"""options/charges.py — Indian options charges, dated.

A five-year options backtest cannot use one charge rate. The statutory rates
moved inside the window, and they moved in the direction that matters: STT on
the sale of options went from 0.0625% to 0.1% of premium on 1 October 2024, and
NSE's transaction charge changed on the same date. A book that prices 2021
trades at 2026 rates — or 2026 trades at 2021 rates — is wrong in a way that
compounds with trade count, and this strategy trades often.

So rates are effective-dated and looked up per trade date. Every rate carries a
`source_note`; the ones marked VERIFY were taken from memory rather than from a
reachable primary source, and must be checked against the exchange circular
before any number computed here is published.

Charges modelled, per leg:
  brokerage           flat per executed order
  STT                 sell side only, on premium
  exchange txn        both sides, on premium turnover
  SEBI turnover       both sides
  stamp duty          buy side only
  GST                 on (brokerage + exchange txn + SEBI)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional


@dataclass(frozen=True)
class ChargeRates:
    effective_from: date
    brokerage_per_order: float
    stt_sell_pct: float  # % of sell premium turnover
    exchange_txn_pct: float  # % of premium turnover, both sides
    sebi_turnover_pct: float  # % of premium turnover, both sides
    stamp_duty_buy_pct: float  # % of buy premium turnover
    gst_pct: float  # % on brokerage + txn + sebi
    source_note: str


# Ordered oldest first. Extend rather than edit: an old backtest must keep
# reproducing the number it produced, and editing history silently re-prices it.
RATE_TABLE: List[ChargeRates] = [
    ChargeRates(
        effective_from=date(2021, 1, 1),
        brokerage_per_order=20.0,
        stt_sell_pct=0.05,
        exchange_txn_pct=0.053,
        sebi_turnover_pct=0.0001,
        stamp_duty_buy_pct=0.003,
        gst_pct=18.0,
        source_note="VERIFY: pre-2024 NSE options rates, from memory",
    ),
    ChargeRates(
        effective_from=date(2024, 10, 1),
        brokerage_per_order=20.0,
        stt_sell_pct=0.1,
        exchange_txn_pct=0.03503,
        sebi_turnover_pct=0.0001,
        stamp_duty_buy_pct=0.003,
        gst_pct=18.0,
        source_note="VERIFY: Budget 2024 raised options STT to 0.1% on 01-Oct-2024",
    ),
]


def rates_for(trade_date: date) -> ChargeRates:
    """The rates in force on a date. Raises below the table rather than
    extrapolating backwards — an unpriced era should stop a backtest, not be
    guessed at."""
    applicable = [r for r in RATE_TABLE if r.effective_from <= trade_date]
    if not applicable:
        raise ValueError(
            f"no charge rates defined on or before {trade_date}; extend RATE_TABLE "
            f"rather than letting the backtest price this era by accident"
        )
    return applicable[-1]


@dataclass(frozen=True)
class ChargeBreakdown:
    brokerage: float
    stt: float
    exchange_txn: float
    sebi: float
    stamp_duty: float
    gst: float

    @property
    def total(self) -> float:
        return round(
            self.brokerage + self.stt + self.exchange_txn + self.sebi + self.stamp_duty + self.gst,
            2,
        )

    def as_dict(self) -> dict:
        d = {
            "brokerage": round(self.brokerage, 2),
            "stt": round(self.stt, 2),
            "exchange_txn": round(self.exchange_txn, 2),
            "sebi": round(self.sebi, 2),
            "stamp_duty": round(self.stamp_duty, 2),
            "gst": round(self.gst, 2),
        }
        d["total"] = self.total
        return d


def round_trip_charges(
    *,
    trade_date: date,
    buy_premium: float,
    sell_premium: float,
    quantity: int,
    rates: Optional[ChargeRates] = None,
) -> ChargeBreakdown:
    """Charges on one round trip of `quantity` units (lots x lot size).

    Premiums are per unit, as quoted. Turnover for Indian options charges is
    premium turnover, not notional — pricing charges off strike x quantity is a
    common and very expensive modelling error.
    """
    r = rates or rates_for(trade_date)
    buy_turnover = buy_premium * quantity
    sell_turnover = sell_premium * quantity
    turnover = buy_turnover + sell_turnover

    brokerage = 2 * r.brokerage_per_order  # one order each way
    stt = sell_turnover * r.stt_sell_pct / 100.0
    exchange_txn = turnover * r.exchange_txn_pct / 100.0
    sebi = turnover * r.sebi_turnover_pct / 100.0
    stamp = buy_turnover * r.stamp_duty_buy_pct / 100.0
    gst = (brokerage + exchange_txn + sebi) * r.gst_pct / 100.0

    return ChargeBreakdown(brokerage, stt, exchange_txn, sebi, stamp, gst)
