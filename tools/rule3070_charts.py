"""tools/rule3070_charts.py — render 30-70 Rule campaigns as PNGs for Phil's
visual adjudication against TradingView. All times IST.

Usage: python3 tools/rule3070_charts.py [--n 6] [--out out/rule3070]
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.rule3070_sim import Campaign, run_ladder  # noqa: E402

CACHE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "candles", "BTCUSDT_5m.pkl")


def draw_candles(ax, df: pd.DataFrame):
    x = range(len(df))
    for i, (_, bar) in enumerate(df.iterrows()):
        up = bar["close"] >= bar["open"]
        color = "#1f8a4c" if up else "#c0392b"
        ax.plot([i, i], [bar["low"], bar["high"]], color=color, linewidth=0.6, zorder=1)
        body_low, body_high = sorted((bar["open"], bar["close"]))
        ax.add_patch(
            plt.Rectangle(
                (i - 0.35, body_low), 0.7, max(body_high - body_low, 1e-9), facecolor=color, edgecolor=color, zorder=2
            )
        )


def hline(ax, y, x0, x1, color, label, style="-", lw=1.2):
    ax.plot([x0, x1], [y, y], color=color, linestyle=style, linewidth=lw, zorder=3)
    ax.annotate(
        f"{label} {y:,.0f}", xy=(x1, y), xytext=(4, 0), textcoords="offset points", va="center", fontsize=7, color=color
    )


def render(df: pd.DataFrame, c: Campaign, path: str):
    pad = 24
    start = max(0, df.index.get_loc(c.mother_ts) - pad)
    end_ts = c.end_ts or c.swing_high_ts
    end = min(len(df), df.index.get_loc(end_ts) + pad)
    win = df.iloc[start:end]
    pos = {ts: i for i, ts in enumerate(win.index)}

    fig, ax = plt.subplots(figsize=(16, 8))
    draw_candles(ax, win)
    x1 = len(win) - 1

    m_x = pos.get(c.mother_ts, 0)
    l_x = pos.get(c.swing_low_ts, 0)
    h_x = pos.get(c.swing_high_ts, 0)

    hline(ax, c.mother_high, m_x, x1, "#7b2d8e", "MOTHER")
    hline(ax, c.swing_low, l_x, x1, "#2c3e50", "SWING LOW", style="--", lw=0.9)
    hline(ax, c.swing_high, h_x, x1, "#2c3e50", "SWING HIGH", style="--", lw=0.9)
    hline(ax, c.fibS2, h_x, x1, "#e67e22", "S2")
    hline(ax, c.fibB2, h_x, x1, "#c0392b", "B2")
    ref_label = "REF (=S2)" if c.reference == c.fibS2 else "REF (=B2)"
    hline(ax, c.reference, h_x, x1, "#000000", ref_label, style=":", lw=1.6)
    s4 = c.level("S", 4)
    lo_vis, hi_vis = win["low"].min(), win["high"].max()
    if lo_vis <= s4 <= hi_vis:
        hline(ax, s4, h_x, x1, "#e67e22", "S4", style="--", lw=0.8)

    for ts, tag, color in [(c.touch_ts, "touch", "#2980b9"), (c.trigger_ts, "close<REF", "#8e44ad")]:
        if ts in pos:
            ax.axvline(pos[ts], color=color, linewidth=0.8, linestyle=":", zorder=0)
            ax.annotate(tag, xy=(pos[ts], hi_vis), fontsize=7, color=color, rotation=90, va="top")

    for f in c.fills:
        if f.ts in pos:
            ax.scatter([pos[f.ts]], [f.price], marker="^", s=90, color="#1f8a4c", zorder=5)
            ax.annotate(
                f"BUY {f.label} @ {f.price:,.0f} (${f.usd:.0f})",
                xy=(pos[f.ts], f.price),
                xytext=(6, -12),
                textcoords="offset points",
                fontsize=8,
                color="#1f8a4c",
            )
    if c.fibB_low_anchor:
        hline(ax, c.fibB_low_anchor, l_x, x1, "#c0392b", "B-anchor(stretched)", style="--", lw=0.8)
    for kind, band, price, arm_ts in c.band_lines:
        if band > 1 and lo_vis <= price <= hi_vis:
            hline(ax, price, pos.get(arm_ts, 0), x1, "#8e44ad", f"{kind} line b{band}", style="--", lw=0.9)
    if c.target:
        hline(ax, c.target, pos.get(c.fills[0].ts, l_x) if c.fills else l_x, x1, "#1f8a4c", "TARGET", lw=1.4)
        if c.target_ts in pos:
            ax.scatter([pos[c.target_ts]], [c.target], marker="v", s=90, color="#0b6b3a", zorder=5)

    ist = win.index.tz_convert("Asia/Kolkata")
    ticks = list(range(0, len(win), max(1, len(win) // 10)))
    ax.set_xticks(ticks)
    ax.set_xticklabels([ist[i].strftime("%d %b\n%H:%M") for i in ticks], fontsize=7)
    ax.set_xlim(-1, len(win) + 14)
    title_ist = c.mother_ts.tz_convert("Asia/Kolkata").strftime("%Y-%m-%d %H:%M IST")
    ax.set_title(
        f"30-70 Rule — BTCUSDT 5m — {c.v_type} — mother {title_ist} — fall {c.fall_pct:.2f}% — pot ${c.pot_usd:.2f} — {c.status}",
        fontsize=10,
    )
    ax.grid(alpha=0.15)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=8)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(CACHE), "..", "..", "out", "rule3070"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    df = pd.read_pickle(CACHE)  # nosec B301 - our own cache file
    done = run_ladder(df)
    print(f"{len(done)} V campaigns across {df.index[0]} .. {df.index[-1]}")
    hits = [c for c in done if c.status == "TARGET HIT"]
    with_70 = [c for c in hits if len(c.fills) == 2]
    only_30 = [c for c in hits if len(c.fills) == 1]
    deep = sorted([c for c in done if len(c.fills) > 2], key=lambda c: -len(c.fills))
    cancelled = [c for c in done if c.status.startswith("CANCELLED")]
    open_ = [c for c in done if c.status != "TARGET HIT" and c not in cancelled]
    kinds = {k: len([c for c in done if c.v_type == k]) for k in ("failed V", "extra V", "equal V")}
    print(
        f"target hit: {len(hits)} ({len(only_30)} on 30% only, {len(with_70)} needed the 70%, {len([c for c in hits if len(c.fills) > 2])} went deeper) | cancelled (mother broken): {len(cancelled)} | open/pending: {len(open_)} | {kinds}"
    )

    # a diverse sample: 30%-only, 70% cases, DEEP ladders, one of each V type, a cancelled, an open one
    extra = [c for c in done if c.v_type == "extra V"]
    equal = [c for c in done if c.v_type == "equal V"]
    sample, seen = [], set()
    for c in only_30[:1] + with_70[:2] + deep[:2] + extra[:1] + equal[:1] + cancelled[:1] + open_[:1]:
        if id(c) not in seen:
            seen.add(id(c))
            sample.append(c)
    sample = sample[: args.n]
    for i, c in enumerate(sample, 1):
        path = os.path.join(args.out, f"campaign_{i:02d}.png")
        render(df, c, path)
        print(f"\n--- campaign {i} -> {path}")
        print(c.describe())


if __name__ == "__main__":
    main()
