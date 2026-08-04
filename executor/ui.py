"""
executor/ui.py — the page the buyer keeps open.

A localhost-only web page served by the executor itself, from the standard
library. No framework, no build step, no new dependency: this ships to
strangers' machines, and every package it pulls in is something else a buyer
has to trust and we have to keep patched.

**Bound to 127.0.0.1 and refuses anything else.** The page shows positions and
exposure, and a page that leaks onto the LAN is a page somebody's flatmate can
read. There is no auth because there is no remote access to authenticate; the
boundary IS the loopback interface.

The layout follows report.py's ordering rule: the armed exposure leads, because
"if this machine stops now, at most $X can fill unwatched" is the number the
buyer is actually relying on. Everything else explains why the executor is or
is not doing something.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional

from executor.power import PlatformPower, suspend_advice
from executor.report import irreducible_risk, running_status

_log = logging.getLogger("cascade.executor.ui")

DEFAULT_PORT = 7757
EVENT_KEEP = 100

# The setup guide, shipped beside this file and served on the Guide tab. It
# lives next to the executor rather than inside PAGE because a buyer who has
# broken something needs to READ it — often on a second machine, from an
# email — and a guide that only exists inside a program you cannot start is
# not a guide.
GUIDE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "guide.html")

_GUIDE_MISSING = (
    "<p style='font:16px/1.7 system-ui,sans-serif;color:#8a93a6;padding:48px;max-width:34em'>"
    "The setup guide (<code>guide.html</code>) is not next to the executor any more. "
    "Nothing is wrong with your trading — ask us for another copy.</p>"
)


def guide_document() -> bytes:
    """
    The buyer's guide, wrapped as a document.

    `guide.html` is stored as body content so the same file can be published
    on the web unchanged, which means the doctype has to be added here: an
    iframe without one renders in quirks mode, and the guide is a thousand
    lines of layout that assumes it is not.

    A missing file is a note, never an error page. The guide going astray must
    not look like the executor breaking.
    """
    try:
        with open(GUIDE_FILE, encoding="utf-8") as handle:
            content = handle.read()
    except OSError as exc:
        _log.warning("the setup guide could not be read: %s", exc)
        content = _GUIDE_MISSING
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        "<title>Cascade — setup guide</title></head><body>" + content + "</body></html>"
    ).encode("utf-8")


class UIState:
    """
    What the page reads. Written by the runtime's thread, read by the server's;
    everything crosses under one lock and out as plain dicts.

    The event log keeps the last hundred lines rather than everything: this is
    a "what just happened" strip, not the audit trail — that is the exchange's
    order history, which is the one record nobody can quietly edit.
    """

    def __init__(self, *, power: Optional[PlatformPower] = None):
        self._lock = threading.Lock()
        self._status: dict = {}
        self._campaigns: list = []
        self._rounds: list = []
        self._identity: dict = {}
        self._started_at = time.time()
        self._events: deque = deque(maxlen=EVENT_KEEP)
        self._wake_message: str = ""
        self._connection: dict = {"state": "starting"}
        self._power = power

    def set_status(self, status: dict, campaigns: Optional[list] = None, rounds: Optional[list] = None) -> None:
        with self._lock:
            self._status = dict(status or {})
            if campaigns is not None:
                self._campaigns = list(campaigns)
            if rounds is not None:
                self._rounds = list(rounds)

    def set_identity(self, identity: dict) -> None:
        with self._lock:
            self._identity = dict(identity or {})

    def set_connection(self, state: str, detail: str = "") -> None:
        with self._lock:
            self._connection = {"state": state, "detail": detail, "at": int(time.time())}

    def set_wake_message(self, message: str) -> None:
        with self._lock:
            self._wake_message = str(message or "")

    def add_event(self, line: str) -> None:
        with self._lock:
            self._events.appendleft({"at": int(time.time()), "line": str(line)})

    def snapshot(self) -> dict:
        with self._lock:
            status = dict(self._status)
            return {
                "status": status,
                "lines": running_status(status, power=self._power) if status else [],
                "campaigns": list(self._campaigns),
                "rounds": list(self._rounds),
                "identity": dict(self._identity),
                "uptime_sec": int(time.time() - self._started_at),
                "events": list(self._events),
                "wake_message": self._wake_message,
                "connection": dict(self._connection),
                "disclosure": irreducible_risk(),
                "advice": (
                    suspend_advice(self._power, armed_exposure_usd=float(status.get("armed_exposure_usd") or 0))
                    if self._power
                    else None
                ),
                "now": int(time.time()),
            }


def campaigns_view(runtime) -> list:
    """The per-campaign rows, from the runtime's own book and client — plus the
    planned ladder, so the buyer can see WHERE their money is waiting to go,
    not just that some of it is."""
    rows = []
    for campaign_id, orders in runtime.book.campaigns.items():
        followed = runtime._client.campaigns.get(campaign_id)
        last = float(runtime.last_prices.get(orders.symbol) or 0.0)
        ladder, fidelity = [], ""
        plan = runtime._client.plan(
            campaign_id,
            capital_usd=runtime._config.capital_usd,
            funded_bands=runtime._birth_bands.get(campaign_id, []),
        )
        if plan and not plan.get("refused"):
            for leg in plan["legs"]:
                fidelity = leg["fidelity"]
                for rung in leg["rungs"]:
                    ladder.append(
                        {
                            "level": rung["level"],
                            "price": rung["price"],
                            "usd": round(rung["usd"], 2),
                            "style": rung["entry_style"],
                            "reached": bool(last and rung["price"] and last <= rung["price"]),
                        }
                    )
        rows.append(
            {
                "campaign_id": campaign_id,
                "symbol": orders.symbol,
                "exchange": orders.exchange,
                "state": followed.state if followed else "?",
                "halted": followed.halted if followed else "",
                "timeframe": followed.timeframe if followed else "",
                "mother_high": orders.mother_high,
                "last_price": last or None,
                "position_qty": orders.base_qty,
                "avg_entry": orders.avg_entry,
                "target": orders.exit_price,
                "target_away_pct": (
                    round((orders.exit_price - last) / last * 100, 2) if orders.exit_price and last else None
                ),
                "pot_usd": orders.pot_usd,
                "stop_price": orders.stop_price,
                "entry_resting": orders.entry_resting,
                "exit_resting": orders.exit_resting,
                "held_reason": orders.held_reason,
                "reuse_below": orders.reuse_below,
                "ladder": ladder,
                "fidelity": fidelity,
                "rounds": len(orders.closed_rounds),
                "rounds_net_est_usd": round(sum(r["net_est_usd"] for r in orders.closed_rounds), 2),
            }
        )
    for campaign_id, followed in runtime._client.campaigns.items():
        if campaign_id in runtime.book.campaigns or not followed.skip_reason:
            continue
        rows.append(
            {
                "campaign_id": campaign_id,
                "symbol": followed.symbol,
                "exchange": followed.exchange,
                "state": "skipped",
                "skip_reason": followed.skip_reason,
            }
        )
    return rows


def chart_view(runtime, market, campaign_id: str) -> Optional[dict]:
    """
    Everything a buyer's chart may draw, and nothing else.

    Candles come from THEIR venue — they trade its prices, so they chart its
    prices. Geometry (mother, trendline, fib rungs) is the published feed. The
    money marks (fills, average, target) are their own. Our fills, our target
    and our capital are not here because this machine never had them.
    """
    followed = runtime._client.campaigns.get(campaign_id)
    orders = runtime.book.get(campaign_id)
    if not followed:
        return None
    try:
        candles = market.closed_candles_since(followed.symbol, followed.timeframe or "5m", 0)
    except Exception as exc:
        _log.warning("chart candles failed for %s: %s", campaign_id, exc)
        candles = []
    trendline = None
    if followed.standing_trendline_id is not None:
        raw = followed.trendlines.get(followed.standing_trendline_id) or {}
        if raw.get("anchor1_price") and raw.get("anchor2_price"):
            trendline = {
                "a1_ts": raw.get("anchor1_timestamp"),
                "a1_p": raw.get("anchor1_price"),
                "a2_ts": raw.get("anchor2_timestamp"),
                "a2_p": raw.get("anchor2_price"),
            }
    fib_levels = []
    for leg in followed.legs.values():
        if leg.finalized:
            continue
        for level, price in leg.level_prices().items():
            fib_levels.append({"leg": leg.leg_id, "level": level, "price": price})
    return {
        "campaign_id": campaign_id,
        "symbol": followed.symbol,
        "timeframe": followed.timeframe,
        "candles": [[c.timestamp, c.open, c.high, c.low, c.close] for c in candles[-160:]],
        "mother_high": followed.mother_high,
        "mother_timestamp": followed.mother_timestamp,
        "trendline": trendline,
        "fib_levels": fib_levels,
        "fills": [{"ts": f.timestamp, "price": f.price} for f in (orders.fills if orders else [])],
        "avg_entry": orders.avg_entry if orders else None,
        "target": orders.exit_price if orders else None,
        "stop_price": orders.stop_price if orders else None,
        "reuse_below": orders.reuse_below if orders else None,
    }


class UIServer:
    def __init__(
        self,
        state: UIState,
        *,
        port: int = DEFAULT_PORT,
        actions: Optional[dict] = None,
        chart_fn: Optional[Callable[[str], Optional[dict]]] = None,
    ):
        self._state = state
        self._port = port
        # name -> zero-arg callable returning a message for the buyer. The
        # runtime hands these over; the server never reaches into it directly.
        self._actions = dict(actions or {})
        self._chart_fn = chart_fn
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def start(self) -> Optional[str]:
        """Serve, or explain why not. A busy port is a note, not a crash."""
        state = self._state
        actions = self._actions
        chart_fn = self._chart_fn

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # quiet; the executor has its own log
                pass

            def _local(self) -> bool:
                """Loopback peer AND a loopback Host header.

                The peer check is a belt on top of the bind. The Host check is
                DNS-rebinding defence: a hostile page can point its own domain
                at 127.0.0.1 and then fetch it same-origin, and the one thing
                the browser faithfully reports is the Host it asked for.
                """
                if self.client_address[0] not in ("127.0.0.1", "::1"):
                    return False
                host = (self.headers.get("Host") or "").split(":")[0].lower()
                return host in ("127.0.0.1", "localhost", "[::1]", "::1")

            def do_GET(self):
                if not self._local():
                    self.send_error(403)
                    return
                if self.path.startswith("/api/chart"):
                    from urllib.parse import parse_qs, urlparse

                    cid = (parse_qs(urlparse(self.path).query).get("cid") or [""])[0]
                    chart = chart_fn(cid) if chart_fn else None
                    if not chart:
                        self.send_error(404, "no chart for that campaign")
                        return
                    body = json.dumps(chart, default=str).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                elif self.path.startswith("/api/state"):
                    body = json.dumps(state.snapshot(), default=str).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                elif self.path in ("/", "/index.html"):
                    body = PAGE.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                elif self.path in ("/guide", "/guide.html"):
                    body = guide_document()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                else:
                    self.send_error(404)
                    return
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                """
                /api/action {"action": name}. Three gates, in order:

                loopback peer + Host (as GET), then a custom header. Any web
                page can blind-POST to localhost — a form needs no permission —
                but a CUSTOM header forces a CORS preflight, and we never
                answer OPTIONS, so a cross-origin caller can never get one
                through. These buttons cancel and place real orders; "it is
                only localhost" is not a boundary a browser respects.
                """
                if not self._local():
                    self.send_error(403)
                    return
                if self.headers.get("X-Cascade-UI") != "1":
                    self.send_error(403, "missing X-Cascade-UI header")
                    return
                if self.path != "/api/action":
                    self.send_error(404)
                    return
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                    request = json.loads(self.rfile.read(length) or b"{}")
                    name = str(request.get("action") or "")
                except Exception:
                    self.send_error(400)
                    return
                handler = actions.get(name)
                if not handler:
                    self.send_error(404, f"unknown action {name!r}")
                    return
                try:
                    message = handler()
                except Exception as exc:
                    _log.exception("action %s failed", name)
                    message = f"{name} failed: {exc}"
                state.add_event(message)
                body = json.dumps({"message": message}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        try:
            self._server = ThreadingHTTPServer(("127.0.0.1", self._port), Handler)
        except OSError as exc:
            return f"UI not started: port {self._port} is busy ({exc}). The executor runs fine without it."
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="cascade-ui")
        self._thread.start()
        return None

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server = None


# One self-contained page. Inline styles and script on purpose — it is served
# by a stdlib handler with no static directory, and the whole point is that
# there is nothing else to fetch and nowhere else to fetch it from.
PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cascade — by CryptoForge</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg viewBox='0 0 32 32' xmlns='http%3A//www.w3.org/2000/svg'%3E%3Crect width='32' height='32' rx='7' fill='%23040814'/%3E%3Crect x='7' y='10' width='4' height='12' rx='1' fill='%23f59e0b'/%3E%3Crect x='14' y='6' width='4' height='16' rx='1' fill='%2322d3ee'/%3E%3Crect x='21' y='13' width='4' height='9' rx='1' fill='%232dd4bf'/%3E%3C/svg%3E">
<style>
  /* ══ CryptoForge DNA — same tokens as the terminal, same ink as the landing ══ */
  :root {
    --bg:#040814; --ink:#05060a;
    --surface:rgba(7,14,28,.92); --card:rgba(10,19,36,.90); --card2:rgba(13,24,44,.92);
    --border:rgba(148,178,211,.11); --border-hi:rgba(218,242,255,.18); --border-acc:rgba(34,211,238,.26);
    --text:#e8f2ff; --dim:rgba(216,232,250,.72); --muted:rgba(174,191,213,.58);
    --accent:#22d3ee; --amber:#f59e0b; --green:#2dd4bf; --green-bg:rgba(45,212,191,.12);
    --red:#fb7185; --red-bg:rgba(251,113,133,.11); --yellow:#fbbf24; --mint:#86efb8;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  html { scroll-behavior:smooth; }
  body { background:var(--bg); color:var(--text); overflow-x:hidden;
         font:15px/1.55 'Sora','Segoe UI',-apple-system,Roboto,sans-serif; }
  body::before { content:""; position:fixed; inset:0; z-index:0; pointer-events:none;
    background:radial-gradient(58% 42% at 82% -8%, rgba(34,211,238,.09), transparent 60%),
               radial-gradient(50% 38% at 8% 108%, rgba(245,158,11,.07), transparent 60%); }
  .kicker { font-size:10.5px; letter-spacing:.42em; text-transform:uppercase;
            color:var(--amber); font-weight:600; }
  .display { font-weight:700; letter-spacing:.045em; text-transform:uppercase; line-height:1.2; }
  .mono { font-family:'Azeret Mono',ui-monospace,Menlo,monospace; }

  /* ══ Top bar — the terminal's nav, inherited ══ */
  .topbar { position:sticky; top:0; z-index:50; background:var(--surface);
            border-bottom:1px solid var(--border); backdrop-filter:blur(14px); }
  .topbar-inner { max-width:1080px; margin:0 auto; padding:0 20px;
                  display:flex; align-items:center; gap:6px; flex-wrap:wrap; }
  .brand { display:flex; align-items:center; gap:10px; padding:12px 14px 12px 0; margin-right:8px; }
  .brand-mark { width:32px; height:32px; border-radius:8px; flex:none;
    background:linear-gradient(145deg,rgba(13,24,44,.95),rgba(4,8,20,.95));
    border:1px solid var(--border-acc); display:grid; place-items:center; }
  .brand-mark svg { width:18px; height:18px; }
  .brand-text { font-size:10px; letter-spacing:.2em; line-height:1.45; color:var(--text);
                text-transform:uppercase; font-weight:700; }
  .brand-text small { display:block; color:var(--muted); letter-spacing:.24em; font-size:8.5px; font-weight:600; }
  .nav-tab { background:none; border:none; border-bottom:2px solid transparent; color:var(--muted);
    padding:16px 16px; cursor:pointer; font:600 13px 'Sora',sans-serif;
    display:inline-flex; align-items:center; gap:7px; white-space:nowrap;
    transition:color .18s,border-color .18s,background .18s; }
  .nav-tab:hover { color:var(--text); background:rgba(255,255,255,.03); }
  .nav-tab.active { color:var(--accent); border-bottom-color:var(--accent);
                    background:rgba(34,211,238,.07); }
  .live-dot { width:7px; height:7px; border-radius:50%; background:#4a4d55; flex:none; }
  .live-dot.on { background:var(--green); animation:livePulse 2s ease-in-out infinite; }
  .live-dot.err { background:var(--red); }
  @keyframes livePulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.35;transform:scale(.75)} }

  .page { display:none; position:relative; z-index:1; }
  .page.on { display:block; animation:fadeIn .25s ease; }
  /* The guide fills what is left under the sticky bar and scrolls inside
     itself, so the nav never leaves while a buyer is following a step. The
     height is set from the bar's measured height rather than a constant —
     the bar wraps to two rows on a narrow window. */
  .guide-frame { display:block; width:100%; height:calc(100vh - 57px); border:0; background:#05060a; }
  @keyframes fadeIn { from{opacity:0;transform:translateY(5px)} to{opacity:1;transform:none} }
  .wrap { max-width:1080px; margin:0 auto; padding:22px 20px 72px; }

  /* ══ HOME — the front door, in the landing's voice ══ */
  .hero { position:relative; min-height:calc(100vh - 61px); display:flex; flex-direction:column;
          overflow:hidden; background:var(--ink); }
  .hero-scene { position:absolute; inset:0; z-index:0; }
  .hero-scene svg { width:100%; height:100%; }
  .hero-veil { position:absolute; inset:0; z-index:1; pointer-events:none;
    background:radial-gradient(70% 45% at 50% 76%, rgba(5,6,10,.78) 0%, rgba(5,6,10,.45) 46%, transparent 78%),
      linear-gradient(180deg, rgba(5,6,10,.55) 0%, rgba(5,6,10,.12) 26%, rgba(5,6,10,.38) 55%, rgba(5,6,10,.92) 86%, var(--bg) 100%); }
  .embers { position:absolute; inset:0; z-index:2; pointer-events:none; overflow:hidden; }
  .embers i { position:absolute; bottom:-12px; width:4px; height:4px; border-radius:50%;
    background:var(--amber); opacity:0; animation:rise linear infinite;
    box-shadow:0 0 10px rgba(245,158,11,.9),0 0 24px rgba(245,158,11,.4); }
  .embers i:nth-child(odd) { background:var(--accent);
    box-shadow:0 0 10px rgba(34,211,238,.8),0 0 22px rgba(34,211,238,.35); }
  .embers i:nth-child(1){left:9%;animation-duration:15s} .embers i:nth-child(2){left:22%;animation-duration:19s;animation-delay:3s}
  .embers i:nth-child(3){left:36%;animation-duration:14s;animation-delay:6s} .embers i:nth-child(4){left:52%;animation-duration:21s;animation-delay:1s}
  .embers i:nth-child(5){left:66%;animation-duration:16s;animation-delay:8s} .embers i:nth-child(6){left:79%;animation-duration:18s;animation-delay:4s}
  .embers i:nth-child(7){left:90%;animation-duration:15s;animation-delay:10s}
  @keyframes rise { 0%{transform:translateY(0);opacity:0} 8%{opacity:.85} 70%{opacity:.5}
                    100%{transform:translateY(-105vh);opacity:0} }
  .hero-copy { position:relative; z-index:4; text-align:center; margin-top:auto; padding:0 24px 9vh; }
  .hero-kicker { font-size:11px; letter-spacing:.42em; text-transform:uppercase; color:var(--amber);
                 font-weight:600; margin-bottom:18px; }
  .hero-copy h1 { font-size:clamp(30px,5vw,62px); font-weight:700; letter-spacing:.04em;
                  text-transform:uppercase; line-height:1.16; }
  .hero-copy h1 em { font-style:normal; color:transparent;
    background:linear-gradient(92deg,var(--amber) 8%,var(--mint) 52%,var(--accent) 92%);
    -webkit-background-clip:text; background-clip:text; }
  .hero-sub { color:var(--dim); max-width:560px; margin:18px auto 30px; font-size:15.5px; }
  .hero-ctas { display:flex; gap:14px; justify-content:center; flex-wrap:wrap; }
  .cta { font:600 12px 'Sora',sans-serif; letter-spacing:.14em; text-transform:uppercase;
    padding:13px 26px; border-radius:10px; cursor:pointer; transition:all .2s;
    border:1px solid var(--border-hi); background:rgba(10,19,36,.6); color:var(--text); }
  .cta:hover { border-color:var(--accent); color:var(--accent); }
  .cta.solid { background:linear-gradient(120deg,#0891b2,#22d3ee); border-color:transparent; color:#03242b; }
  .cta.solid:hover { filter:brightness(1.12); color:#03242b; }
  .hero-stats { position:relative; z-index:4; display:flex; justify-content:center; gap:14px;
                flex-wrap:wrap; padding:0 20px 42px; }
  .hero-stat { background:var(--card); border:1px solid var(--border); border-radius:12px;
               padding:12px 22px; text-align:center; min-width:150px; }
  .hero-stat .v { font-size:21px; font-weight:700; font-variant-numeric:tabular-nums; }
  .hero-stat .l { color:var(--muted); font-size:10.5px; letter-spacing:.18em; text-transform:uppercase; margin-top:2px; }
  .hero-stat .v.up { color:var(--green); } .hero-stat .v.down { color:var(--red); }
  .home-cards { position:relative; z-index:4; background:var(--bg); padding:44px 20px 64px; }
  .home-cards-inner { max-width:1080px; margin:0 auto; display:grid;
                      grid-template-columns:repeat(auto-fit,minmax(250px,1fr)); gap:14px; }
  .home-card { background:var(--card); border:1px solid var(--border); border-radius:14px; padding:20px; }
  .home-card:hover { border-color:var(--border-acc); }
  .home-card h3 { font-size:13px; letter-spacing:.14em; text-transform:uppercase; margin:12px 0 8px; }
  .home-card p { color:var(--dim); font-size:13.5px; }
  .home-card .glyph { font-size:20px; }

  /* ══ Shared surfaces — the terminal's cards ══ */
  .panel { background:var(--card); border:1px solid var(--border); border-radius:14px; }
  .stats-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
                gap:12px; margin:0 0 16px; }
  .stat { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px 16px; }
  .stat .l { color:var(--muted); font-size:10.5px; letter-spacing:.16em; text-transform:uppercase; }
  .stat .v { font-size:20px; font-weight:700; margin-top:3px; font-variant-numeric:tabular-nums; }
  .stat .s { color:var(--muted); font-size:11.5px; margin-top:2px; }
  .stat .v.up { color:var(--green); } .stat .v.down { color:var(--red); } .stat .v.acc { color:var(--accent); }
  .exposure { padding:22px; margin-bottom:16px; display:flex; gap:26px; align-items:center;
    flex-wrap:wrap; background:linear-gradient(150deg,var(--card2),var(--card));
    border:1px solid var(--border-acc); border-radius:16px; position:relative; overflow:hidden; }
  .exposure::after { content:""; position:absolute; inset:0; pointer-events:none;
    background:radial-gradient(48% 90% at 96% 8%, rgba(34,211,238,.10), transparent 70%); }
  .exposure .num { font-size:40px; font-weight:700; font-variant-numeric:tabular-nums; }
  .exposure .why { color:var(--dim); max-width:340px; }
  .controls { display:flex; gap:10px; flex-wrap:wrap; margin:0 0 6px; }
  button.act { font:600 12.5px 'Sora',sans-serif; letter-spacing:.06em; padding:10px 18px;
    border-radius:9px; cursor:pointer; border:1px solid var(--border-hi);
    background:var(--card); color:var(--text); transition:all .18s; }
  button.act:hover { border-color:var(--accent); color:var(--accent); }
  button.act.solid { background:linear-gradient(120deg,#0891b2,#22d3ee); border-color:transparent; color:#03242b; }
  button.act.solid:hover { filter:brightness(1.1); color:#03242b; }
  button.act.danger { border-color:rgba(251,113,133,.4); color:var(--red); }
  button.act.danger:hover { background:var(--red-bg); }
  button[disabled] { opacity:.45; cursor:default; }
  .toast { color:var(--muted); font-size:13px; min-height:20px; margin:6px 2px 10px; }
  .wake { margin:0 0 16px; padding:16px; background:rgba(251,191,36,.07);
    border:1px solid rgba(251,191,36,.3); border-radius:12px; color:var(--yellow);
    font-size:14px; display:flex; gap:16px; align-items:center; flex-wrap:wrap; }
  .lines { display:flex; flex-direction:column; gap:8px; margin-bottom:16px; }
  .line { padding:11px 15px; background:var(--card); border:1px solid var(--border);
    border-left:3px solid var(--accent); border-radius:8px; font-size:13.5px; }
  .line.warn { border-left-color:var(--yellow); } .line.bad { border-left-color:var(--red); }

  /* ══ Campaign cards ══ */
  .camp { margin-bottom:14px; overflow:hidden; }
  .camp .head { display:flex; align-items:center; gap:12px; padding:14px 18px; flex-wrap:wrap; }
  .camp .sym { font-weight:700; font-size:15px; letter-spacing:.04em; }
  .camp .venue { color:var(--muted); font-size:12px; }
  .camp .last { margin-left:auto; color:var(--dim); font-variant-numeric:tabular-nums; font-size:13.5px; }
  .pill { display:inline-block; padding:2px 10px; border-radius:99px; font-size:11.5px;
          font-weight:600; letter-spacing:.05em; border:1px solid var(--border-hi); }
  .pill.live { color:var(--green); background:var(--green-bg); border-color:rgba(45,212,191,.3); }
  .pill.halt { color:var(--red); background:var(--red-bg); border-color:rgba(251,113,133,.35); }
  .pill.skip { color:var(--muted); }
  .pill.coarse { color:var(--yellow); background:rgba(251,191,36,.08); border-color:rgba(251,191,36,.3); }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:1px;
          background:var(--border); border-top:1px solid var(--border); }
  .cell { background:var(--card2); padding:11px 15px; }
  .cell .l { color:var(--muted); font-size:10.5px; text-transform:uppercase; letter-spacing:.12em; }
  .cell .v { font-variant-numeric:tabular-nums; margin-top:3px; font-size:14px; }
  .cell .v.up { color:var(--green); }
  details { border-top:1px solid var(--border); }
  summary { padding:10px 18px; color:var(--muted); font-size:12.5px; cursor:pointer;
            list-style:none; letter-spacing:.06em; }
  summary:hover { color:var(--accent); }
  summary::before { content:"▸ "; color:var(--accent); } details[open] summary::before { content:"▾ "; }
  .rungs { padding:2px 18px 14px; }
  .rung { display:flex; gap:16px; font:12.5px/2 'Azeret Mono',ui-monospace,Menlo,monospace; color:var(--muted); }
  .rung.reached { color:var(--text); }
  .rung .dot { color:#4a4d55; } .rung.reached .dot { color:var(--green); }

  table { width:100%; border-collapse:collapse; font-size:13.5px; }
  th { text-align:left; color:var(--muted); font-weight:600; padding:10px 12px;
       border-bottom:1px solid var(--border-hi); font-size:11px; text-transform:uppercase; letter-spacing:.1em; }
  td { padding:10px 12px; border-bottom:1px solid var(--border); font-variant-numeric:tabular-nums; }
  td.up { color:var(--green); } td.down { color:var(--red); }
  .section-h { display:flex; align-items:baseline; gap:12px; margin:26px 0 12px; }
  .section-h h2 { font-size:13px; letter-spacing:.2em; text-transform:uppercase; color:var(--dim); }
  .empty { color:var(--muted); font-size:13.5px; padding:16px; }
  .events div { color:var(--muted); font:12px/1.8 'Azeret Mono',ui-monospace,Menlo,monospace; }
  .events time { color:#4a5568; margin-right:10px; }
  .disclosure { margin-top:36px; color:var(--muted); font-size:12.5px;
                border-top:1px solid var(--border); padding-top:18px; max-width:760px; }

  /* ══ Chart modal ══ */
  .modal { position:fixed; inset:0; z-index:200; display:none; align-items:center;
           justify-content:center; padding:20px; background:rgba(2,5,12,.82);
           backdrop-filter:blur(6px); }
  .modal.on { display:flex; }
  .modal-box { width:min(1040px,100%); max-height:92vh; overflow:auto;
    background:var(--surface); border:1px solid var(--border-hi); border-radius:16px; }
  .modal-head { display:flex; align-items:center; gap:12px; padding:16px 20px;
                border-bottom:1px solid var(--border); flex-wrap:wrap; }
  .modal-head .sym { font-weight:700; font-size:16px; letter-spacing:.04em; }
  .modal-close { margin-left:auto; background:none; border:1px solid var(--border-hi);
    color:var(--dim); width:30px; height:30px; border-radius:8px; cursor:pointer; font-size:16px; }
  .modal-close:hover { color:var(--red); border-color:rgba(251,113,133,.4); }
  .chart-wrap { padding:16px 20px 6px; }
  canvas { width:100%; height:380px; display:block; }
  .legend { display:flex; gap:16px; flex-wrap:wrap; padding:4px 20px 14px;
            color:var(--muted); font-size:11.5px; letter-spacing:.06em; }
  .legend span { display:inline-flex; align-items:center; gap:6px; }
  .legend i { width:14px; height:2px; display:inline-block; }
  .chart-note { padding:0 20px 18px; color:var(--muted); font-size:12.5px; }
  .btn-chart { background:none; border:1px solid var(--border-hi); color:var(--dim);
    font:600 11.5px 'Sora',sans-serif; letter-spacing:.06em; padding:5px 12px;
    border-radius:7px; cursor:pointer; }
  .btn-chart:hover { color:var(--accent); border-color:var(--accent); }

  /* ══ Setup ══ */
  .steps { counter-reset:step; display:flex; flex-direction:column; gap:14px; margin:16px 0 26px; }
  .step { counter-increment:step; display:flex; gap:16px; padding:18px; }
  .step::before { content:counter(step); flex:none; width:34px; height:34px; border-radius:50%;
    display:grid; place-items:center; font-weight:700; color:var(--accent);
    border:1px solid var(--border-acc); background:rgba(34,211,238,.07); }
  .step h3 { font-size:14px; margin-bottom:4px; }
  .step p { color:var(--dim); font-size:13.5px; }
  .keybox { margin-top:10px; padding:12px 14px; background:var(--ink); border:1px solid var(--border);
    border-radius:9px; font:12.5px 'Azeret Mono',ui-monospace,Menlo,monospace; color:var(--mint);
    word-break:break-all; user-select:all; }
  @media (max-width:640px) {
    .brand-text small { display:none; }
    .nav-tab { padding:14px 10px; font-size:12px; }
    .exposure .num { font-size:32px; }
  }
</style>
</head>
<body>

<div class="topbar"><div class="topbar-inner">
  <div class="brand">
    <div class="brand-mark"><svg viewBox="0 0 32 32">
      <rect x="4" y="12" width="5" height="13" rx="1.5" fill="#f59e0b"/>
      <rect x="13" y="6" width="5" height="19" rx="1.5" fill="#22d3ee"/>
      <rect x="22" y="15" width="5" height="10" rx="1.5" fill="#2dd4bf"/>
      <line x1="6.5" y1="8" x2="6.5" y2="12" stroke="#f59e0b" stroke-width="1.6"/>
      <line x1="15.5" y1="2.5" x2="15.5" y2="6" stroke="#22d3ee" stroke-width="1.6"/>
      <line x1="24.5" y1="11" x2="24.5" y2="15" stroke="#2dd4bf" stroke-width="1.6"/>
    </svg></div>
    <div class="brand-text">Cascade<small>by CryptoForge</small></div>
  </div>
  <button class="nav-tab active" data-page="home">Home</button>
  <button class="nav-tab" data-page="console"><span class="live-dot" id="dot"></span>Console</button>
  <button class="nav-tab" data-page="campaigns">Campaigns</button>
  <button class="nav-tab" data-page="rounds">Rounds</button>
  <button class="nav-tab" data-page="setup">Setup</button>
  <button class="nav-tab" data-page="guide">Guide</button>
</div></div>

<!-- ══════════ HOME ══════════ -->
<section class="page on" id="page-home">
  <div class="hero">
    <div class="hero-scene"><svg viewBox="0 0 1440 780" preserveAspectRatio="xMidYMid slice" aria-hidden="true">
      <defs>
        <linearGradient id="sky" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stop-color="#0a1226"/><stop offset=".55" stop-color="#070d1c"/>
          <stop offset="1" stop-color="#05060a"/>
        </linearGradient>
        <linearGradient id="oldc" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stop-color="#f5a623"/><stop offset="1" stop-color="#7a4c0d"/>
        </linearGradient>
        <linearGradient id="upc" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stop-color="#67e8f9"/><stop offset="1" stop-color="#0e7490"/>
        </linearGradient>
        <linearGradient id="dnc" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stop-color="#fda4af"/><stop offset="1" stop-color="#9f1239"/>
        </linearGradient>
        <filter id="glow" x="-80%" y="-80%" width="260%" height="260%">
          <feGaussianBlur stdDeviation="7" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
        <filter id="soft" x="-80%" y="-80%" width="260%" height="260%">
          <feGaussianBlur stdDeviation="14"/>
        </filter>
      </defs>
      <rect width="1440" height="780" fill="url(#sky)"/>
      <!-- the old market: wax candles guttering on the left -->
      <g filter="url(#glow)" opacity=".85">
        <rect x="96"  y="430" width="26" height="190" rx="5" fill="url(#oldc)" opacity=".8"/>
        <rect x="168" y="392" width="26" height="228" rx="5" fill="url(#oldc)" opacity=".66"/>
        <rect x="240" y="452" width="26" height="168" rx="5" fill="url(#oldc)" opacity=".74"/>
        <ellipse cx="109" cy="416" rx="7" ry="14" fill="#ffd166"/>
        <ellipse cx="181" cy="378" rx="6" ry="12" fill="#ffd166" opacity=".9"/>
        <ellipse cx="253" cy="438" rx="6" ry="12" fill="#ffd166" opacity=".85"/>
      </g>
      <ellipse cx="180" cy="640" rx="230" ry="46" fill="#f5a623" opacity=".05" filter="url(#soft)"/>
      <!-- the transition: wax becomes data mid-frame -->
      <g opacity=".9">
        <rect x="392" y="410" width="20" height="150" rx="4" fill="url(#oldc)" opacity=".5"/>
        <line x1="402" y1="380" x2="402" y2="410" stroke="#f5a623" stroke-width="3" opacity=".5"/>
        <rect x="472" y="368" width="20" height="150" rx="3" fill="url(#upc)" opacity=".55"/>
        <line x1="482" y1="332" x2="482" y2="546" stroke="#22d3ee" stroke-width="2.5" opacity=".45"/>
      </g>
      <!-- the modern tape: a cascade fall and the turn back up -->
      <g filter="url(#glow)">
        <line x1="562" y1="300" x2="562" y2="520" stroke="#22d3ee" stroke-width="2.5" opacity=".6"/>
        <rect x="552" y="330" width="20" height="120" rx="3" fill="url(#upc)"/>
        <line x1="642" y1="356" x2="642" y2="560" stroke="#fb7185" stroke-width="2.5" opacity=".6"/>
        <rect x="632" y="380" width="20" height="128" rx="3" fill="url(#dnc)"/>
        <line x1="722" y1="420" x2="722" y2="620" stroke="#fb7185" stroke-width="2.5" opacity=".6"/>
        <rect x="712" y="448" width="20" height="130" rx="3" fill="url(#dnc)"/>
        <line x1="802" y1="470" x2="802" y2="656" stroke="#fb7185" stroke-width="2.5" opacity=".6"/>
        <rect x="792" y="500" width="20" height="118" rx="3" fill="url(#dnc)"/>
        <line x1="882" y1="520" x2="882" y2="688" stroke="#fb7185" stroke-width="2.5" opacity=".6"/>
        <rect x="872" y="556" width="20" height="104" rx="3" fill="url(#dnc)"/>
        <line x1="962" y1="512" x2="962" y2="700" stroke="#22d3ee" stroke-width="2.5" opacity=".65"/>
        <rect x="952" y="540" width="20" height="132" rx="3" fill="url(#upc)"/>
        <line x1="1042" y1="440" x2="1042" y2="640" stroke="#22d3ee" stroke-width="2.5" opacity=".65"/>
        <rect x="1032" y="468" width="20" height="144" rx="3" fill="url(#upc)"/>
        <line x1="1122" y1="380" x2="1122" y2="570" stroke="#22d3ee" stroke-width="2.5" opacity=".65"/>
        <rect x="1112" y="404" width="20" height="138" rx="3" fill="url(#upc)"/>
        <line x1="1202" y1="330" x2="1202" y2="510" stroke="#22d3ee" stroke-width="2.5" opacity=".65"/>
        <rect x="1192" y="352" width="20" height="130" rx="3" fill="url(#upc)"/>
        <line x1="1282" y1="290" x2="1282" y2="452" stroke="#22d3ee" stroke-width="2.5" opacity=".65"/>
        <rect x="1272" y="310" width="20" height="118" rx="3" fill="url(#upc)"/>
      </g>
      <!-- the fib ladder under the fall, faint, like the terminal draws it -->
      <g stroke="#22d3ee" stroke-dasharray="3 7" stroke-width="1" opacity=".28">
        <line x1="600" y1="500" x2="1330" y2="500"/>
        <line x1="600" y1="580" x2="1330" y2="580"/>
        <line x1="600" y1="668" x2="1330" y2="668"/>
      </g>
      <ellipse cx="1120" cy="500" rx="330" ry="120" fill="#22d3ee" opacity=".045" filter="url(#soft)"/>
    </svg></div>
    <div class="hero-veil"></div>
    <div class="embers"><i></i><i></i><i></i><i></i><i></i><i></i><i></i></div>
    <div class="hero-copy">
      <div class="hero-kicker">Cascade · Signal Executor</div>
      <h1>The fall is charted.<br><em>Your machine takes it.</em></h1>
      <p class="hero-sub">CryptoForge's live geometry, executed on your own exchange account,
        with your own keys, on your own laptop. The signal crosses the wire — your money never does.</p>
      <div class="hero-ctas">
        <button class="cta solid" data-goto="console">Open console</button>
        <button class="cta" data-goto="setup">Set up this machine</button>
      </div>
    </div>
    <div class="hero-stats">
      <div class="hero-stat"><div class="v" id="h-exposure">—</div><div class="l">at risk unwatched</div></div>
      <div class="hero-stat"><div class="v" id="h-following">—</div><div class="l">campaigns live</div></div>
      <div class="hero-stat"><div class="v" id="h-pnl">—</div><div class="l">net, all rounds</div></div>
      <div class="hero-stat"><div class="v" id="h-conn">—</div><div class="l">signal</div></div>
    </div>
  </div>
  <div class="home-cards"><div class="home-cards-inner">
    <div class="home-card"><div class="glyph">🕯️</div><h3>Geometry, not custody</h3>
      <p>Every message is signed and verified against a root key baked into this app.
         Capital, orders and balances never cross the wire in either direction.</p></div>
    <div class="home-card"><div class="glyph">🪜</div><h3>Your ladder, your size</h3>
      <p>Levels arrive as percentages of the fall. This machine sizes every rung from
         your capital and nets it against your own campaigns — not ours.</p></div>
    <div class="home-card"><div class="glyph">🌙</div><h3>Sleeps honestly</h3>
      <p>Close the lid: buys are cancelled, sells stay protecting. The one number that
         matters — what can fill unwatched — is always on screen.</p></div>
  </div></div>
</section>

<!-- ══════════ CONSOLE ══════════ -->
<section class="page" id="page-console"><div class="wrap">
  <div class="exposure">
    <div>
      <div class="num" id="exposure">—</div>
      <div class="why" id="exposure-why">reading…</div>
    </div>
    <div style="margin-left:auto;text-align:right" id="pnl" hidden>
      <div style="font-size:24px;font-weight:700" id="pnl-v">—</div>
      <div style="color:var(--muted);font-size:12px" id="pnl-l"></div>
    </div>
  </div>
  <div class="controls">
    <button class="act" id="btn-pause" data-action="pause" hidden>Pause opening</button>
    <button class="act solid" id="btn-resume" data-action="resume" hidden>Resume opening</button>
    <button class="act danger" id="btn-stand-down" data-action="stand_down"
      title="Cancel all buy orders now, leave every sell protecting. Pauses opening.">Stand down</button>
  </div>
  <div class="toast" id="toast"></div>
  <div class="wake" id="wake" hidden>
    <span id="wake-text"></span>
    <button class="act solid" id="btn-confirm" data-action="confirm_wake">I've reviewed — resume trading</button>
  </div>
  <div class="stats-grid">
    <div class="stat"><div class="l">Signal</div><div class="v acc" id="s-conn">—</div><div class="s" id="s-conn-d"></div></div>
    <div class="stat"><div class="l">Following</div><div class="v" id="s-following">—</div><div class="s">live campaigns</div></div>
    <div class="stat"><div class="l">Capital</div><div class="v" id="s-capital">—</div><div class="s" id="s-buyer"></div></div>
    <div class="stat"><div class="l">Uptime</div><div class="v" id="s-uptime">—</div><div class="s">this session</div></div>
  </div>
  <div class="lines" id="lines"></div>
  <div class="section-h"><h2>Recent activity</h2></div>
  <div class="panel events" id="events" style="padding:14px 18px"></div>
  <div class="disclosure" id="disclosure"></div>
</div></section>

<!-- ══════════ CAMPAIGNS ══════════ -->
<section class="page" id="page-campaigns"><div class="wrap">
  <div class="section-h"><h2>Campaigns</h2><span style="color:var(--muted);font-size:12.5px">
    the geometry this machine is following, and where its money is waiting</span></div>
  <div id="cards"></div>
  <div class="empty panel" id="cards-empty">Nothing followed yet — campaigns join as they start on the feed.</div>
</div></section>

<!-- ══════════ ROUNDS ══════════ -->
<section class="page" id="page-rounds"><div class="wrap">
  <div class="section-h"><h2>Closed rounds</h2><span style="color:var(--muted);font-size:12.5px">
    estimated, after your venue's headline commission</span></div>
  <div class="stats-grid">
    <div class="stat"><div class="l">Rounds closed</div><div class="v" id="r-count">0</div></div>
    <div class="stat"><div class="l">Net (est)</div><div class="v" id="r-net">$0.00</div></div>
  </div>
  <div class="panel" style="overflow-x:auto">
    <table id="rounds-table" hidden>
      <thead><tr><th>Closed</th><th>Symbol</th><th>Qty</th><th>Entry → Exit</th><th>Gross</th><th>Fees (est)</th><th>Net (est)</th></tr></thead>
      <tbody id="rounds"></tbody>
    </table>
    <div class="empty" id="rounds-empty">No rounds have closed yet on this machine.</div>
  </div>
</div></section>

<!-- ══════════ SETUP ══════════ -->
<section class="page" id="page-setup"><div class="wrap">
  <div class="section-h"><h2>Setup</h2></div>
  <div class="steps">
    <div class="step panel"><div><h3>Register this machine</h3>
      <p>This machine generated its own key when it first ran. Send the public half to
         CryptoForge — it is how the feed knows you, and it is all we ever hold.</p>
      <div class="keybox" id="setup-key">run the executor once to generate a key</div></div></div>
    <div class="step panel"><div><h3>Point it at your exchange</h3>
      <p>Your API keys live in this machine's environment and are used only to place
         your own orders. Give them trade permission and nothing else — never withdrawal.</p></div></div>
    <div class="step panel"><div><h3>Subscribe, then watch it work</h3>
      <p>Once your key is registered and your subscription is active, campaigns join as
         they start. Everything this machine does shows in the console as it happens.</p></div></div>
  </div>
  <div class="section-h"><h2>This machine</h2></div>
  <div class="stats-grid">
    <div class="stat"><div class="l">Buyer</div><div class="v" id="su-buyer" style="font-size:15px">—</div></div>
    <div class="stat"><div class="l">Exchange</div><div class="v" id="su-exchange" style="font-size:15px">—</div></div>
    <div class="stat"><div class="l">Signal</div><div class="v" id="su-conn" style="font-size:15px">—</div></div>
    <div class="stat"><div class="l">Platform note</div><div class="v" id="su-advice" style="font-size:12.5px;font-weight:400;color:var(--dim)">—</div></div>
  </div>
  <div class="disclosure" id="setup-disclosure"></div>
</div></section>

<!-- ══════════ GUIDE ══════════ -->
<section class="page" id="page-guide">
  <iframe class="guide-frame" id="guide-frame" src="/guide.html" title="Cascade setup guide"></iframe>
</section>

<div class="modal" id="modal">
  <div class="modal-box">
    <div class="modal-head">
      <span class="sym" id="ch-sym">—</span>
      <span class="venue" id="ch-tf" style="color:var(--muted);font-size:12px"></span>
      <span class="pill live" id="ch-pos" hidden></span>
      <button class="modal-close" id="ch-close">×</button>
    </div>
    <div class="chart-wrap"><canvas id="chart"></canvas></div>
    <div class="legend">
      <span><i style="background:#f59e0b"></i>mother high</span>
      <span><i style="background:#22d3ee"></i>trendline</span>
      <span><i style="background:rgba(34,211,238,.45)"></i>fib rungs</span>
      <span><i style="background:#2dd4bf"></i>your target</span>
      <span><i style="background:#a78bfa"></i>your average</span>
      <span><i style="background:#fbbf24"></i>working stop</span>
    </div>
    <div class="chart-note" id="ch-note"></div>
  </div>
</div>

<script>
"use strict";
const $ = id => document.getElementById(id);
const money = v => (v < 0 ? "-$" : "$") + Math.abs(Number(v || 0)).toLocaleString(undefined,
                   {minimumFractionDigits: 2, maximumFractionDigits: 2});
const px = v => v == null ? "—" : Number(v).toLocaleString(undefined, {maximumFractionDigits: 4});
const ago = s => s < 90 ? s + "s" : s < 5400 ? Math.round(s / 60) + "m" : (s / 3600).toFixed(1) + "h";
const openLadders = new Set();

/* tabs */
function show(name) {
  document.querySelectorAll(".page").forEach(p => p.classList.toggle("on", p.id === "page-" + name));
  document.querySelectorAll(".nav-tab").forEach(t => t.classList.toggle("active", t.dataset.page === name));
  history.replaceState(null, "", "#" + name);
  window.scrollTo(0, 0);
}
document.querySelectorAll(".nav-tab").forEach(t => t.addEventListener("click", () => show(t.dataset.page)));

/* The guide fills the window under the top bar. Measured, not assumed: the
   bar wraps to two rows on a narrow window, and a guessed height leaves
   either a dead strip or a second scrollbar. */
function sizeGuide() {
  const frame = $("guide-frame"), bar = document.querySelector(".topbar");
  if (frame && bar) frame.style.height = (window.innerHeight - bar.getBoundingClientRect().height) + "px";
}
window.addEventListener("resize", sizeGuide);
sizeGuide();
document.querySelectorAll("[data-goto]").forEach(b => b.addEventListener("click", () => show(b.dataset.goto)));
if (location.hash.length > 1) show(location.hash.slice(1));

async function act(name, button) {
  button.disabled = true;
  try {
    const r = await fetch("/api/action", {method: "POST",
      headers: {"Content-Type": "application/json", "X-Cascade-UI": "1"},
      body: JSON.stringify({action: name})});
    const d = await r.json();
    $("toast").textContent = d.message || "done";
  } catch (e) { $("toast").textContent = "action failed: " + e; }
  button.disabled = false;
  poll();
}
document.querySelectorAll("button[data-action]").forEach(b =>
  b.addEventListener("click", () => act(b.dataset.action, b)));

function cell(label, value, cls) {
  return `<div class="cell"><div class="l">${label}</div><div class="v ${cls || ""}">${value}</div></div>`;
}

function render(s) {
  const st = s.status || {}, c = s.connection || {}, id = s.identity || {};
  const live = c.state === "connected" || c.state === "synced";
  const exp = Number(st.armed_exposure_usd || 0);
  const net = Number(st.rounds_net_est_usd || 0);

  /* nav + hero */
  $("dot").className = "live-dot " + (live ? "on" : c.state === "stopped" ? "err" : "");
  $("h-exposure").textContent = money(exp);
  $("h-following").textContent = st.following != null ? st.following : "—";
  $("h-pnl").textContent = st.rounds_closed ? money(net) : "—";
  $("h-pnl").className = "v " + (net > 0 ? "up" : net < 0 ? "down" : "");
  $("h-conn").textContent = c.state || "—";
  $("h-conn").className = "v " + (live ? "up" : c.state === "stopped" ? "down" : "");

  /* console */
  $("exposure").textContent = money(exp);
  $("exposure-why").textContent = exp > 0
    ? "can fill unwatched if this machine stops now"
    : "nothing can fill while this machine is away — no buy orders are resting";
  if (st.rounds_closed > 0) {
    $("pnl").hidden = false;
    $("pnl-v").textContent = money(net);
    $("pnl-v").style.color = net >= 0 ? "var(--green)" : "var(--red)";
    $("pnl-l").textContent = st.rounds_closed + " round(s) · est., after venue fees";
  }
  $("btn-pause").hidden = !!st.paused;
  $("btn-resume").hidden = !st.paused;
  const waiting = st.awaiting_confirmation || s.wake_message;
  $("wake").hidden = !waiting;
  $("wake-text").textContent = st.awaiting_confirmation || s.wake_message || "";
  $("btn-confirm").hidden = !st.awaiting_confirmation;

  $("s-conn").textContent = c.state || "—";
  $("s-conn-d").textContent = c.detail || "";
  $("s-following").textContent = st.following != null ? st.following : "—";
  $("s-capital").textContent = st.capital_usd ? money(st.capital_usd) : "—";
  $("s-buyer").textContent = id.buyer_id ? id.buyer_id + " · " + (id.exchange || "") : "";
  $("s-uptime").textContent = ago(s.uptime_sec || 0);

  const lines = $("lines"); lines.replaceChildren();
  (s.lines || []).slice(1).forEach(text => {
    const div = document.createElement("div");
    div.className = "line" + (/stale|stopped|no sell|contradicted/i.test(text) ? " bad"
                   : /paused|not enough|fewer|warning|seconds|review/i.test(text) ? " warn" : "");
    div.textContent = text; lines.appendChild(div);
  });

  /* campaigns */
  const cards = $("cards"); cards.replaceChildren();
  const campaigns = s.campaigns || [];
  $("cards-empty").hidden = campaigns.length > 0;
  campaigns.forEach(cp => {
    const card = document.createElement("div"); card.className = "camp panel";
    const tag = cp.halted ? ["halt", "halted"] : cp.state === "skipped" ? ["skip", "skipped"]
              : ["live", (cp.state || "").toLowerCase().replace("_", " ")];
    card.innerHTML = `<div class="head"><span class="sym">${cp.symbol}</span>` +
      `<span class="venue">${cp.exchange || ""}${cp.timeframe ? " · " + cp.timeframe : ""}</span>` +
      `<span class="pill ${tag[0]}" title="${cp.halted || cp.skip_reason || ""}">${tag[1]}</span>` +
      (cp.fidelity === "coarse" ? `<span class="pill coarse" title="Shallow rungs cannot clear the exchange minimum at your capital — fewer, deeper entries than the signal.">coarse</span>` : "") +
      (cp.last_price ? `<span class="last">last ${px(cp.last_price)}</span>` : "") +
      (cp.state === "skipped" ? "" : `<button class="btn-chart" data-chart="${cp.campaign_id}" data-sym="${cp.symbol}">Chart</button>`) +
      `</div>`;
    if (cp.state === "skipped") {
      card.innerHTML += `<div class="cell" style="border-top:1px solid var(--border)">` +
        `<div class="l">why</div><div class="v">${cp.skip_reason || ""}</div></div>`;
      cards.appendChild(card); return;
    }
    card.innerHTML += `<div class="grid">` +
      cell("Position", cp.position_qty > 0 ? px(cp.position_qty) + " @ " + px(cp.avg_entry) : "—") +
      cell("Target", cp.exit_resting
        ? px(cp.target) + (cp.target_away_pct != null ? " (" + cp.target_away_pct + "% away)" : "")
        : cp.position_qty > 0 ? "placing…" : "—", cp.exit_resting ? "up" : "") +
      cell("Working entry", cp.entry_resting ? "stop " + px(cp.stop_price)
        : cp.held_reason ? "held — no new low" : cp.pot_usd > 0 ? money(cp.pot_usd) + " collected" : "—") +
      cell("Mother high", px(cp.mother_high)) +
      cell("Floor", cp.reuse_below != null ? "below " + px(cp.reuse_below) : "—") +
      cell("Rounds here", cp.rounds > 0 ? cp.rounds + " · " + money(cp.rounds_net_est_usd) : "—",
           cp.rounds > 0 && cp.rounds_net_est_usd >= 0 ? "up" : "") + `</div>`;
    if ((cp.ladder || []).length) {
      const rungs = cp.ladder.map(r =>
        `<div class="rung ${r.reached ? "reached" : ""}"><span class="dot">${r.reached ? "●" : "○"}</span>` +
        `<span>L${r.level}</span><span>${px(r.price)}</span><span>${money(r.usd)}</span>` +
        `<span>${r.style}</span></div>`).join("");
      card.innerHTML += `<details ${openLadders.has(cp.campaign_id) ? "open" : ""}>` +
        `<summary>ladder — where your money is waiting</summary><div class="rungs">${rungs}</div></details>`;
      card.querySelector("details").addEventListener("toggle", e => {
        if (e.target.open) openLadders.add(cp.campaign_id); else openLadders.delete(cp.campaign_id);
      });
    }
    const chartBtn = card.querySelector("button[data-chart]");
    if (chartBtn) chartBtn.addEventListener("click", () => openChart(chartBtn.dataset.chart, chartBtn.dataset.sym));
    cards.appendChild(card);
  });

  /* rounds */
  const rounds = s.rounds || [];
  $("r-count").textContent = st.rounds_closed || 0;
  $("r-net").textContent = money(net);
  $("r-net").className = "v " + (net > 0 ? "up" : net < 0 ? "down" : "");
  $("rounds-empty").hidden = rounds.length > 0;
  $("rounds-table").hidden = rounds.length === 0;
  const tbody = $("rounds"); tbody.replaceChildren();
  rounds.forEach(r => {
    const tr = document.createElement("tr");
    const rn = Number(r.net_est_usd || 0);
    tr.innerHTML = `<td>${r.closed_ts ? new Date(r.closed_ts * 1000).toLocaleString() : "—"}</td>` +
      `<td>${r.symbol}</td><td>${px(r.quantity)}</td>` +
      `<td>${px(r.avg_entry)} → ${px(r.exit_price)}</td>` +
      `<td>${money(r.gross_usd)}</td><td>${money(r.fees_est_usd)}</td>` +
      `<td class="${rn >= 0 ? "up" : "down"}">${money(rn)}</td>`;
    tbody.appendChild(tr);
  });

  /* events + setup */
  const ev = $("events"); ev.replaceChildren();
  (s.events || []).forEach(e => {
    const div = document.createElement("div");
    const t = document.createElement("time");
    t.textContent = new Date(e.at * 1000).toLocaleTimeString();
    div.appendChild(t); div.appendChild(document.createTextNode(e.line));
    ev.appendChild(div);
  });
  $("disclosure").textContent = s.disclosure || "";
  $("setup-disclosure").textContent = s.disclosure || "";
  if (id.public_key) $("setup-key").textContent = id.public_key;
  $("su-buyer").textContent = id.buyer_id || "—";
  $("su-exchange").textContent = id.exchange || "—";
  $("su-conn").textContent = c.state || "—";
  $("su-advice").textContent = s.advice || "Nothing to flag on this platform.";
}
/* ══ chart ══ */
let chartData = null;
function drawChart() {
  const d = chartData, cv = $("chart");
  if (!d || !cv) return;
  const dpr = window.devicePixelRatio || 1;
  const W = cv.clientWidth, H = cv.clientHeight;
  cv.width = W * dpr; cv.height = H * dpr;
  const g = cv.getContext("2d");
  g.setTransform(dpr, 0, 0, dpr, 0, 0);
  g.clearRect(0, 0, W, H);

  const candles = d.candles || [];
  if (!candles.length) {
    g.fillStyle = "rgba(174,191,213,.58)"; g.font = "13px Sora, sans-serif";
    g.fillText("No candles from your exchange yet.", 16, H / 2);
    return;
  }
  const padL = 8, padR = 68, padT = 14, padB = 22;
  // Scale to PRICE, never to the deepest fib rung: L8 is eight leg-ranges down
  // and would flatten every candle into a line. Rungs inside the price band
  // are drawn; ones far below are named in the note instead.
  let lo = Infinity, hi = -Infinity;
  candles.forEach(c => { lo = Math.min(lo, c[3]); hi = Math.max(hi, c[2]); });
  [d.mother_high, d.avg_entry, d.target, d.stop_price].forEach(v => {
    if (v) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  });
  const span = (hi - lo) || 1;
  lo -= span * 0.06; hi += span * 0.06;
  const X = i => padL + (i + 0.5) * ((W - padL - padR) / candles.length);
  const Y = p => padT + (hi - p) / (hi - lo) * (H - padT - padB);
  const bw = Math.max(1.5, Math.min(9, (W - padL - padR) / candles.length * 0.62));

  // Labels are nudged apart, because the interesting case is levels that sit
  // ON TOP of each other: an average entry lands within cents of the rung it
  // filled from, and two overlapping labels are less readable than none.
  const taken = [];
  const line = (p, color, dash, label) => {
    if (p == null || p < lo || p > hi) return;
    const y = Y(p);
    g.save(); g.strokeStyle = color; g.lineWidth = 1; g.setLineDash(dash || []);
    g.beginPath(); g.moveTo(padL, y); g.lineTo(W - padR, y); g.stroke(); g.restore();
    let ly = y;
    while (taken.some(t => Math.abs(t - ly) < 11)) ly += 11;
    taken.push(ly);
    if (Math.abs(ly - y) > 2) {
      g.save(); g.strokeStyle = color; g.globalAlpha = .45; g.lineWidth = .8;
      g.beginPath(); g.moveTo(W - padR, y); g.lineTo(W - padR + 4, ly - 3.5); g.stroke(); g.restore();
    }
    g.fillStyle = color; g.font = "10.5px 'Azeret Mono', ui-monospace, monospace";
    g.fillText(label, W - padR + 6, ly + 3.5);
  };

  (d.fib_levels || []).forEach(f => line(f.price, "rgba(34,211,238,.34)", [3, 6], "L" + f.level));
  if (d.reuse_below) line(d.reuse_below, "rgba(174,191,213,.35)", [2, 5], "floor");

  if (d.trendline && d.trendline.a1_ts && d.trendline.a2_ts) {
    const t = d.trendline, t0 = candles[0][0], t1 = candles[candles.length - 1][0];
    const at = ts => t.a1_p + (t.a2_p - t.a1_p) * ((ts - t.a1_ts) / ((t.a2_ts - t.a1_ts) || 1));
    g.save(); g.strokeStyle = "#22d3ee"; g.lineWidth = 1.4; g.globalAlpha = .85;
    g.beginPath(); g.moveTo(X(0), Y(at(t0))); g.lineTo(X(candles.length - 1), Y(at(t1)));
    g.stroke(); g.restore();
  }

  candles.forEach((c, i) => {
    const up = c[4] >= c[1], x = X(i);
    g.strokeStyle = up ? "#2dd4bf" : "#fb7185"; g.fillStyle = g.strokeStyle;
    g.lineWidth = 1;
    g.beginPath(); g.moveTo(x, Y(c[2])); g.lineTo(x, Y(c[3])); g.stroke();
    const yO = Y(c[1]), yC = Y(c[4]);
    g.fillRect(x - bw / 2, Math.min(yO, yC), bw, Math.max(1.2, Math.abs(yC - yO)));
  });

  line(d.mother_high, "#f59e0b", [], "mother");
  line(d.stop_price, "#fbbf24", [5, 4], "stop");
  line(d.avg_entry, "#a78bfa", [], "avg");
  line(d.target, "#2dd4bf", [], "target");

  const first = candles[0][0], last = candles[candles.length - 1][0], spanT = (last - first) || 1;
  (d.fills || []).forEach(f => {
    const x = padL + ((f.ts - first) / spanT) * (W - padL - padR);
    const y = Y(f.price);
    if (y < padT || y > H - padB) return;
    g.save(); g.fillStyle = "#a78bfa"; g.strokeStyle = "#040814"; g.lineWidth = 1.5;
    g.beginPath(); g.arc(x, y, 4.5, 0, Math.PI * 2); g.fill(); g.stroke(); g.restore();
  });
}

async function openChart(cid, symbol) {
  $("modal").classList.add("on");
  $("ch-sym").textContent = symbol || "";
  $("ch-tf").textContent = "loading…"; $("ch-note").textContent = "";
  try {
    const r = await fetch("/api/chart?cid=" + encodeURIComponent(cid), {cache: "no-store"});
    if (!r.ok) throw new Error("no chart for that campaign");
    chartData = await r.json();
  } catch (e) {
    $("ch-tf").textContent = ""; $("ch-note").textContent = String(e.message || e);
    chartData = null; drawChart(); return;
  }
  const d = chartData;
  $("ch-tf").textContent = (d.timeframe || "") + " · your exchange's candles";
  const pos = $("ch-pos");
  pos.hidden = !d.avg_entry;
  if (d.avg_entry) pos.textContent = "holding @ " + px(d.avg_entry);
  const deep = (d.fib_levels || []).filter(f => f.price < Math.min(...d.candles.map(c => c[3])));
  $("ch-note").textContent =
    "Geometry from the signal; candles, fills and target are your own machine's." +
    (deep.length ? "  " + deep.length + " deeper rung(s) sit below this view — the chart scales to price, not to L8."
                 : "");
  drawChart();
}
$("ch-close").addEventListener("click", () => $("modal").classList.remove("on"));
$("modal").addEventListener("click", e => { if (e.target.id === "modal") $("modal").classList.remove("on"); });
document.addEventListener("keydown", e => { if (e.key === "Escape") $("modal").classList.remove("on"); });
window.addEventListener("resize", drawChart);

async function poll() {
  try {
    const r = await fetch("/api/state", {cache: "no-store"});
    render(await r.json());
  } catch (e) {
    $("dot").className = "live-dot err";
    $("h-conn").textContent = "no executor"; $("s-conn").textContent = "no executor";
  }
}
poll(); setInterval(poll, 3000);
</script>
</body>
</html>
"""


def wire(executor, *, port: int = DEFAULT_PORT, say: Optional[Callable] = None) -> Optional[UIServer]:
    """
    Attach the UI to a running Executor: wrap its status callback so every
    event also lands on the page, and refresh the snapshot after each tick.

    Wrapping rather than replacing, so the terminal keeps saying exactly what
    it said before — the page is an addition, not a migration.
    """
    from executor.power import detect

    state = UIState(power=detect())
    state.set_identity(
        {
            "buyer_id": executor.config.buyer_id,
            "exchange": executor.config.exchange,
            # The public half only — it is the one thing the server holds, and
            # the Setup page shows it so "send us your key" is a copy, not a
            # terminal session.
            "public_key": executor.identity.public_key_b64(),
        }
    )

    def _runtime_action(name):
        def run():
            runtime = executor.runtime
            if runtime is None:
                return "Not connected yet — nothing to act on."
            return getattr(runtime, name)()

        return run

    server = UIServer(
        state,
        port=port,
        actions={
            "pause": _runtime_action("pause_opening"),
            "resume": _runtime_action("resume_opening"),
            "confirm_wake": _runtime_action("confirm_wake"),
            "stand_down": _runtime_action("request_stand_down"),
        },
        chart_fn=lambda cid: chart_view(executor.runtime, executor._market_for_ui(), cid) if executor.runtime else None,
    )
    problem = server.start()
    if problem:
        if say:
            say(problem)
        return None

    original_status = executor._on_status

    def on_status(kind, detail):
        original_status(kind, detail)
        if kind in ("connected", "synced"):
            state.set_connection(kind, "")
        elif kind == "disconnected":
            state.set_connection("reconnecting", str(detail.get("error") or "")[:80])
        elif kind == "stopped":
            state.set_connection("stopped", str(detail.get("reason") or "")[:120])
        if kind in ("halt", "bad_signature", "clock_warning", "stopped", "campaign", "closed"):
            state.add_event(f"[{kind}] {json.dumps(detail, default=str)[:160]}")

    executor._on_status = on_status
    executor.transport._on_status = on_status

    executor._ui_state = state  # the ticker refreshes this after each tick
    if say:
        say(f"Watching at {server.url}")
    return server
