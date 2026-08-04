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
        self._events: deque = deque(maxlen=EVENT_KEEP)
        self._wake_message: str = ""
        self._connection: dict = {"state": "starting"}
        self._power = power

    def set_status(self, status: dict, campaigns: Optional[list] = None) -> None:
        with self._lock:
            self._status = dict(status or {})
            if campaigns is not None:
                self._campaigns = list(campaigns)

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
    """The per-campaign rows, from the runtime's own book and client."""
    rows = []
    for campaign_id, orders in runtime.book.campaigns.items():
        followed = runtime._client.campaigns.get(campaign_id)
        rows.append(
            {
                "campaign_id": campaign_id,
                "symbol": orders.symbol,
                "exchange": orders.exchange,
                "state": followed.state if followed else "?",
                "halted": followed.halted if followed else "",
                "position_qty": orders.base_qty,
                "avg_entry": orders.avg_entry,
                "target": orders.exit_price,
                "pot_usd": orders.pot_usd,
                "stop_price": orders.stop_price,
                "entry_resting": orders.entry_resting,
                "exit_resting": orders.exit_resting,
                "held_reason": orders.held_reason,
                "reuse_below": orders.reuse_below,
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
    def __init__(self, state: UIState, *, port: int = DEFAULT_PORT):
        self._state = state
        self._port = port
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def start(self) -> Optional[str]:
        """Serve, or explain why not. A busy port is a note, not a crash."""
        state = self._state

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # quiet; the executor has its own log
                pass

            def do_GET(self):
                # Belt on top of the loopback bind: never answer a request that
                # arrived from anywhere else, even via a forwarded socket.
                if self.client_address[0] not in ("127.0.0.1", "::1"):
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
  :root { --bg:#0e0f13; --card:#16181f; --line:#262a35; --text:#e8e9ec; --dim:#8b8f9c;
          --good:#4ade80; --warn:#fbbf24; --bad:#f87171; --accent:#7c3aed; }
  * { box-sizing:border-box; margin:0; }
  body { background:var(--bg); color:var(--text); font:15px/1.5 -apple-system,'Segoe UI',Roboto,sans-serif;
         max-width:860px; margin:0 auto; padding:24px 16px 64px; }
  h1 { font-size:16px; font-weight:600; letter-spacing:.04em; text-transform:uppercase; color:var(--dim); }
  .conn { float:right; font-size:13px; }
  .conn.ok { color:var(--good); } .conn.bad { color:var(--bad); } .conn.warn { color:var(--warn); }
  .exposure { margin:20px 0; padding:20px; background:var(--card); border:1px solid var(--line); border-radius:12px; }
  .exposure .num { font-size:34px; font-weight:700; font-variant-numeric:tabular-nums; }
  .exposure .why { color:var(--dim); margin-top:4px; }
  .lines { margin:16px 0; display:flex; flex-direction:column; gap:8px; }
  .line { padding:10px 14px; background:var(--card); border-left:3px solid var(--accent);
          border-radius:6px; font-size:14px; }
  .line.warn { border-left-color:var(--warn); } .line.bad { border-left-color:var(--bad); }
  table { width:100%; border-collapse:collapse; margin:16px 0; font-size:14px; }
  th { text-align:left; color:var(--dim); font-weight:500; padding:8px 10px; border-bottom:1px solid var(--line); }
  td { padding:8px 10px; border-bottom:1px solid var(--line); font-variant-numeric:tabular-nums; }
  .tag { display:inline-block; padding:1px 8px; border-radius:99px; font-size:12px; border:1px solid var(--line); }
  .tag.live { color:var(--good); } .tag.halt { color:var(--bad); } .tag.skip { color:var(--dim); }
  .events { margin-top:24px; }
  .events div { color:var(--dim); font:12.5px/1.7 ui-monospace,Menlo,monospace; }
  .events time { color:#565b68; margin-right:8px; }
  .disclosure { margin-top:32px; color:var(--dim); font-size:13px; border-top:1px solid var(--line); padding-top:16px; }
  .wake { margin:16px 0; padding:12px 14px; background:#1c1a10; border:1px solid #3a3418; border-radius:8px;
          color:var(--warn); font-size:14px; }
</style>
</head>
<body>
<h1>Cascade Executor <span class="conn" id="conn">starting…</span></h1>
<div class="exposure">
  <div class="num" id="exposure">—</div>
  <div class="why" id="exposure-why">reading…</div>
</div>
<div class="wake" id="wake" hidden></div>
<div class="lines" id="lines"></div>
<table id="table" hidden>
  <thead><tr><th>Campaign</th><th>Position</th><th>Target</th><th>Working entry</th><th>State</th></tr></thead>
  <tbody id="rows"></tbody>
</table>
<div class="events" id="events"></div>
<div class="disclosure" id="disclosure"></div>
<script>
"use strict";
const $ = id => document.getElementById(id);
const money = v => "$" + Number(v || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
const px = v => v == null ? "—" : Number(v).toLocaleString(undefined, {maximumFractionDigits: 4});
function render(s) {
  const conn = $("conn"), st = s.connection || {};
  conn.textContent = st.state + (st.detail ? " — " + st.detail : "");
  conn.className = "conn " + (st.state === "connected" || st.state === "synced" ? "ok"
                    : st.state === "stopped" ? "bad" : "warn");
  const exp = Number((s.status || {}).armed_exposure_usd || 0);
  $("exposure").textContent = money(exp);
  $("exposure-why").textContent = exp > 0
    ? "can fill unwatched if this machine stops now"
    : "nothing can fill while this machine is away — no buy orders are resting";
  $("wake").hidden = !s.wake_message; $("wake").textContent = s.wake_message || "";
  const lines = $("lines"); lines.replaceChildren();
  (s.lines || []).slice(1).forEach(text => {           // line 0 is the exposure, shown above
    const div = document.createElement("div");
    div.className = "line" + (/stale|stopped|no sell|contradicted/i.test(text) ? " bad"
                   : /not enough|fewer|warning|seconds/i.test(text) ? " warn" : "");
    div.textContent = text; lines.appendChild(div);
  });
  const rows = $("rows"); rows.replaceChildren();
  (s.campaigns || []).forEach(c => {
    const tr = document.createElement("tr");
    const cell = t => { const td = document.createElement("td"); td.textContent = t; return td; };
    tr.appendChild(cell(c.symbol + "  ·  " + (c.exchange || "")));
    tr.appendChild(cell(c.position_qty > 0 ? px(c.position_qty) + " @ " + px(c.avg_entry) : "—"));
    tr.appendChild(cell(c.exit_resting ? px(c.target) : (c.position_qty > 0 ? "placing…" : "—")));
    tr.appendChild(cell(c.entry_resting ? "stop " + px(c.stop_price) + " (" + money(c.pot_usd) + ")"
                       : c.held_reason ? "held" : c.pot_usd > 0 ? money(c.pot_usd) + " collected" : "—"));
    const state = document.createElement("td");
    const tag = document.createElement("span");
    tag.className = "tag " + (c.halted ? "halt" : c.state === "skipped" ? "skip" : "live");
    tag.textContent = c.halted ? "halted" : c.state === "skipped" ? "skipped" : (c.state || "").toLowerCase();
    tag.title = c.halted || c.skip_reason || "";
    state.appendChild(tag); tr.appendChild(state);
    rows.appendChild(tr);
  });
  $("table").hidden = !(s.campaigns || []).length;
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
    $("conn").textContent = "executor not responding"; $("conn").className = "conn bad";
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
    server = UIServer(state, port=port)
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
