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


class UIServer:
    def __init__(self, state: UIState, *, port: int = DEFAULT_PORT, actions: Optional[dict] = None):
        self._state = state
        self._port = port
        # name -> zero-arg callable returning a message for the buyer. The
        # runtime hands these over; the server never reaches into it directly.
        self._actions = dict(actions or {})
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def start(self) -> Optional[str]:
        """Serve, or explain why not. A busy port is a note, not a crash."""
        state = self._state
        actions = self._actions

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
                if self.path.startswith("/api/state"):
                    body = json.dumps(state.snapshot(), default=str).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                elif self.path in ("/", "/index.html"):
                    body = PAGE.encode("utf-8")
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
<title>Cascade Executor</title>
<style>
  :root { --bg:#0d0e12; --card:#15171e; --card2:#1a1d26; --line:#262a35; --text:#e8e9ec;
          --dim:#8b8f9c; --faint:#565b68; --good:#4ade80; --warn:#fbbf24; --bad:#f87171;
          --accent:#7c3aed; --accent2:#a78bfa; }
  * { box-sizing:border-box; margin:0; }
  body { background:var(--bg); color:var(--text);
         font:15px/1.5 -apple-system,'Segoe UI',Roboto,sans-serif;
         max-width:960px; margin:0 auto; padding:24px 16px 64px; }
  header { display:flex; align-items:baseline; gap:14px; flex-wrap:wrap; }
  h1 { font-size:16px; font-weight:600; letter-spacing:.05em; text-transform:uppercase; color:var(--dim); }
  .chips { display:flex; gap:8px; flex-wrap:wrap; margin-left:auto; }
  .chip { font-size:12.5px; color:var(--dim); border:1px solid var(--line); border-radius:99px;
          padding:2px 10px; background:var(--card); }
  .chip b { color:var(--text); font-weight:600; }
  .conn.ok b { color:var(--good); } .conn.bad b { color:var(--bad); } .conn.warn b { color:var(--warn); }
  .hero { margin:20px 0 14px; padding:20px; background:linear-gradient(160deg,var(--card),var(--card2));
          border:1px solid var(--line); border-radius:14px; display:flex; gap:24px;
          align-items:center; flex-wrap:wrap; }
  .hero .num { font-size:38px; font-weight:700; font-variant-numeric:tabular-nums; }
  .hero .why { color:var(--dim); margin-top:2px; max-width:340px; }
  .hero .pnl { margin-left:auto; text-align:right; }
  .hero .pnl .v { font-size:22px; font-weight:600; font-variant-numeric:tabular-nums; }
  .hero .pnl .v.up { color:var(--good); } .hero .pnl .v.down { color:var(--bad); }
  .hero .pnl .l { color:var(--faint); font-size:12.5px; }
  .controls { display:flex; gap:10px; flex-wrap:wrap; margin:14px 0; }
  button { font:14px/1 inherit; padding:9px 16px; border-radius:8px; cursor:pointer;
           border:1px solid var(--line); background:var(--card); color:var(--text); }
  button:hover { border-color:var(--accent2); }
  button.primary { background:var(--accent); border-color:var(--accent); }
  button.danger { border-color:#5b2a2a; color:var(--bad); }
  button[disabled] { opacity:.45; cursor:default; }
  .toast { color:var(--dim); font-size:13.5px; padding:4px 2px; min-height:22px; }
  .wake { margin:14px 0; padding:14px; background:#1c1a10; border:1px solid #3a3418;
          border-radius:10px; color:var(--warn); font-size:14px; display:flex; gap:14px;
          align-items:center; flex-wrap:wrap; }
  .lines { margin:14px 0; display:flex; flex-direction:column; gap:8px; }
  .line { padding:10px 14px; background:var(--card); border-left:3px solid var(--accent);
          border-radius:6px; font-size:14px; }
  .line.warn { border-left-color:var(--warn); } .line.bad { border-left-color:var(--bad); }
  .card { margin:14px 0; background:var(--card); border:1px solid var(--line); border-radius:12px;
          overflow:hidden; }
  .card > .head { display:flex; align-items:center; gap:12px; padding:12px 16px; flex-wrap:wrap; }
  .card .sym { font-weight:600; font-size:15px; }
  .card .venue { color:var(--faint); font-size:12.5px; }
  .card .last { margin-left:auto; color:var(--dim); font-variant-numeric:tabular-nums; }
  .tag { display:inline-block; padding:1px 9px; border-radius:99px; font-size:12px;
         border:1px solid var(--line); }
  .tag.live { color:var(--good); } .tag.halt { color:var(--bad); } .tag.skip { color:var(--dim); }
  .tag.coarse { color:var(--warn); }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:1px;
          background:var(--line); border-top:1px solid var(--line); }
  .cell { background:var(--card2); padding:10px 14px; }
  .cell .l { color:var(--faint); font-size:11.5px; text-transform:uppercase; letter-spacing:.05em; }
  .cell .v { font-variant-numeric:tabular-nums; margin-top:2px; }
  details { border-top:1px solid var(--line); }
  summary { padding:9px 16px; color:var(--dim); font-size:13px; cursor:pointer; list-style:none; }
  summary::before { content:"▸ "; } details[open] summary::before { content:"▾ "; }
  .rungs { padding:0 16px 12px; }
  .rung { display:flex; gap:14px; font:13px/1.9 ui-monospace,Menlo,monospace; color:var(--dim); }
  .rung.reached { color:var(--text); }
  .rung .dot { color:var(--faint); } .rung.reached .dot { color:var(--good); }
  table { width:100%; border-collapse:collapse; margin:6px 0 0; font-size:13.5px; }
  th { text-align:left; color:var(--faint); font-weight:500; padding:7px 10px;
       border-bottom:1px solid var(--line); font-size:12px; text-transform:uppercase;
       letter-spacing:.04em; }
  td { padding:7px 10px; border-bottom:1px solid var(--line); font-variant-numeric:tabular-nums; }
  td.up { color:var(--good); } td.down { color:var(--bad); }
  h2 { margin:26px 0 4px; font-size:13px; color:var(--dim); text-transform:uppercase;
       letter-spacing:.05em; font-weight:600; }
  .events div { color:var(--dim); font:12.5px/1.7 ui-monospace,Menlo,monospace; }
  .events time { color:var(--faint); margin-right:8px; }
  .disclosure { margin-top:32px; color:var(--dim); font-size:13px;
                border-top:1px solid var(--line); padding-top:16px; }
  .empty { color:var(--faint); font-size:13.5px; padding:8px 2px; }
</style>
</head>
<body>
<header>
  <h1>Cascade Executor</h1>
  <div class="chips">
    <span class="chip conn" id="conn"><b>starting…</b></span>
    <span class="chip" id="chip-id" hidden></span>
    <span class="chip" id="chip-capital" hidden></span>
    <span class="chip" id="chip-uptime" hidden></span>
  </div>
</header>

<div class="hero">
  <div>
    <div class="num" id="exposure">—</div>
    <div class="why" id="exposure-why">reading…</div>
  </div>
  <div class="pnl" id="pnl" hidden>
    <div class="v" id="pnl-v">—</div>
    <div class="l" id="pnl-l"></div>
  </div>
</div>

<div class="controls">
  <button id="btn-pause" data-action="pause" hidden>Pause opening</button>
  <button id="btn-resume" data-action="resume" class="primary" hidden>Resume opening</button>
  <button id="btn-stand-down" data-action="stand_down" class="danger"
          title="Cancel all buy orders now, leave every sell protecting. Pauses opening.">Stand down</button>
</div>
<div class="toast" id="toast"></div>

<div class="wake" id="wake" hidden>
  <span id="wake-text"></span>
  <button id="btn-confirm" data-action="confirm_wake" class="primary">I've reviewed — resume trading</button>
</div>

<div class="lines" id="lines"></div>
<div id="cards"></div>

<h2>Closed rounds</h2>
<div class="empty" id="rounds-empty">No rounds have closed yet on this machine.</div>
<table id="rounds-table" hidden>
  <thead><tr><th>Closed</th><th>Symbol</th><th>Qty</th><th>Entry → Exit</th><th>Net (est)</th></tr></thead>
  <tbody id="rounds"></tbody>
</table>

<h2>Recent activity</h2>
<div class="events" id="events"></div>
<div class="disclosure" id="disclosure"></div>

<script>
"use strict";
const $ = id => document.getElementById(id);
// The page repaints every poll, which wipes per-element state — an open
// ladder would slam shut three seconds after being opened. Remember which are
// open by campaign id and re-apply on every render.
const openLadders = new Set();
const money = v => (v < 0 ? "-$" : "$") + Math.abs(Number(v || 0)).toLocaleString(undefined,
                   {minimumFractionDigits: 2, maximumFractionDigits: 2});
const px = v => v == null ? "—" : Number(v).toLocaleString(undefined, {maximumFractionDigits: 4});
const ago = s => s < 90 ? s + "s" : s < 5400 ? Math.round(s / 60) + "m" : (s / 3600).toFixed(1) + "h";

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
  const st = s.status || {}, conn = $("conn"), c = s.connection || {};
  conn.innerHTML = "<b>" + c.state + "</b>" + (c.detail ? " — " + c.detail : "");
  conn.className = "chip conn " + (c.state === "connected" || c.state === "synced" ? "ok"
                    : c.state === "stopped" ? "bad" : "warn");
  const id = s.identity || {};
  if (id.buyer_id) { $("chip-id").hidden = false;
    $("chip-id").innerHTML = `${id.buyer_id} · <b>${id.exchange || ""}</b>`; }
  if (st.capital_usd) { $("chip-capital").hidden = false;
    $("chip-capital").innerHTML = "capital <b>" + money(st.capital_usd) + "</b>"; }
  $("chip-uptime").hidden = false;
  $("chip-uptime").textContent = "up " + ago(s.uptime_sec || 0);

  const exp = Number(st.armed_exposure_usd || 0);
  $("exposure").textContent = money(exp);
  $("exposure-why").textContent = exp > 0
    ? "can fill unwatched if this machine stops now"
    : "nothing can fill while this machine is away — no buy orders are resting";

  if (st.rounds_closed > 0) {
    $("pnl").hidden = false;
    const net = Number(st.rounds_net_est_usd || 0);
    $("pnl-v").textContent = money(net);
    $("pnl-v").className = "v " + (net >= 0 ? "up" : "down");
    $("pnl-l").textContent = st.rounds_closed + " round(s) closed · est., after venue fees";
  }

  $("btn-pause").hidden = !!st.paused;
  $("btn-resume").hidden = !st.paused;
  const waiting = st.awaiting_confirmation || s.wake_message;
  $("wake").hidden = !waiting;
  $("wake-text").textContent = st.awaiting_confirmation || s.wake_message || "";
  $("btn-confirm").hidden = !st.awaiting_confirmation;

  const lines = $("lines"); lines.replaceChildren();
  (s.lines || []).slice(1).forEach(text => {
    const div = document.createElement("div");
    div.className = "line" + (/stale|stopped|no sell|contradicted/i.test(text) ? " bad"
                   : /paused|not enough|fewer|warning|seconds|review/i.test(text) ? " warn" : "");
    div.textContent = text; lines.appendChild(div);
  });

  const cards = $("cards"); cards.replaceChildren();
  (s.campaigns || []).forEach(cp => {
    const card = document.createElement("div"); card.className = "card";
    const tag = cp.halted ? ["halt", "halted"] : cp.state === "skipped" ? ["skip", "skipped"]
              : ["live", (cp.state || "").toLowerCase().replace("_", " ")];
    let head = `<div class="head"><span class="sym">${cp.symbol}</span>` +
      `<span class="venue">${cp.exchange || ""}${cp.timeframe ? " · " + cp.timeframe : ""}</span>` +
      `<span class="tag ${tag[0]}" title="${cp.halted || cp.skip_reason || ""}">${tag[1]}</span>` +
      (cp.fidelity === "coarse" ? `<span class="tag coarse" title="Shallow rungs cannot clear the exchange minimum at your capital — fewer, deeper entries than the signal.">coarse</span>` : "") +
      (cp.last_price ? `<span class="last">last ${px(cp.last_price)}</span>` : "") + `</div>`;
    card.innerHTML = head;
    if (cp.state === "skipped") {
      card.innerHTML += `<div class="cell" style="border-top:1px solid var(--line)">` +
        `<div class="l">why</div><div class="v">${cp.skip_reason || ""}</div></div>`;
      cards.appendChild(card); return;
    }
    let grid = `<div class="grid">`;
    grid += cell("Position", cp.position_qty > 0 ? px(cp.position_qty) + " @ " + px(cp.avg_entry) : "—");
    grid += cell("Target", cp.exit_resting
      ? px(cp.target) + (cp.target_away_pct != null ? " (" + cp.target_away_pct + "% away)" : "")
      : cp.position_qty > 0 ? "placing…" : "—", cp.exit_resting ? "up" : "");
    grid += cell("Working entry", cp.entry_resting ? "stop " + px(cp.stop_price)
      : cp.held_reason ? "held — no new low" : cp.pot_usd > 0 ? money(cp.pot_usd) + " collected" : "—");
    grid += cell("Mother high", px(cp.mother_high));
    grid += cell("Floor", cp.reuse_below != null ? "below " + px(cp.reuse_below) : "—");
    grid += cell("Rounds here", cp.rounds > 0 ? cp.rounds + " · " + money(cp.rounds_net_est_usd) : "—",
                 cp.rounds > 0 ? (cp.rounds_net_est_usd >= 0 ? "up" : "down") : "");
    grid += `</div>`;
    card.innerHTML += grid;
    if ((cp.ladder || []).length) {
      const rungs = cp.ladder.map(r =>
        `<div class="rung ${r.reached ? "reached" : ""}"><span class="dot">${r.reached ? "●" : "○"}</span>` +
        `<span>L${r.level}</span><span>${px(r.price)}</span><span>${money(r.usd)}</span>` +
        `<span>${r.style}</span></div>`).join("");
      card.innerHTML += `<details ${openLadders.has(cp.campaign_id) ? "open" : ""}>` +
        `<summary>ladder — where your money is waiting</summary>` +
        `<div class="rungs">${rungs}</div></details>`;
      card.querySelector("details").addEventListener("toggle", e => {
        if (e.target.open) openLadders.add(cp.campaign_id); else openLadders.delete(cp.campaign_id);
      });
    }
    cards.appendChild(card);
  });

  const rounds = s.rounds || [];
  $("rounds-empty").hidden = rounds.length > 0;
  $("rounds-table").hidden = rounds.length === 0;
  const tbody = $("rounds"); tbody.replaceChildren();
  rounds.forEach(r => {
    const tr = document.createElement("tr");
    const net = Number(r.net_est_usd || 0);
    tr.innerHTML = `<td>${r.closed_ts ? new Date(r.closed_ts * 1000).toLocaleString() : "—"}</td>` +
      `<td>${r.symbol}</td><td>${px(r.quantity)}</td>` +
      `<td>${px(r.avg_entry)} → ${px(r.exit_price)}</td>` +
      `<td class="${net >= 0 ? "up" : "down"}">${money(net)}</td>`;
    tbody.appendChild(tr);
  });

  const ev = $("events"); ev.replaceChildren();
  (s.events || []).forEach(e => {
    const div = document.createElement("div");
    const t = document.createElement("time");
    t.textContent = new Date(e.at * 1000).toLocaleTimeString();
    div.appendChild(t); div.appendChild(document.createTextNode(e.line));
    ev.appendChild(div);
  });
  $("disclosure").textContent = s.disclosure || "";
}
async function poll() {
  try {
    const r = await fetch("/api/state", {cache: "no-store"});
    render(await r.json());
  } catch (e) {
    $("conn").innerHTML = "<b>executor not responding</b>"; $("conn").className = "chip conn bad";
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
    state.set_identity({"buyer_id": executor.config.buyer_id, "exchange": executor.config.exchange})

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
