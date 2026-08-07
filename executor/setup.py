"""
executor/setup.py — the first run, without a terminal.

Everything a buyer used to do by hand: create the config file, paste four
values into it, export two environment variables, and work out why the program
still says "Missing: api_key". This serves one page on loopback that asks the
same questions and writes the same things — the config through `config.py`, the
credentials through `secrets.py`, never both in the same place.

Three things it deliberately does NOT do:

- **It does not trade, and it does not connect the feed.** Setup finishes by
  handing a validated config back to the caller; starting the executor is the
  caller's job. Nothing here can place an order, which is why it is safe to
  serve this before anyone has been authenticated by anything.
- **It does not save half a setup.** The config file is written only once every
  field has been checked, so a buyer who closes the tab midway has changed
  nothing and starts again from a clean state rather than from a file that
  looks finished and is not.
- **It never echoes a secret back.** The page can write an API key and can be
  told one is stored; it cannot read one out. A local page that will hand back
  a key on request is a local page worth attacking.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from executor import httpguard, model, secrets
from executor.config import DEFAULT_CONFIG, ConfigError, ExecutorConfig, build_adapter

_log = logging.getLogger("cascade.executor.setup")

DEFAULT_PORT = 7756

# Where a buyer sends their public key to be registered. Set this and the page
# grows an "Email it to us" button with the key already in the body; leave it
# empty and they get the Copy button alone, which still works but makes them
# find the address themselves.
#
# Registration is deliberately NOT automatic. `POST /api/cascade/feed/subscribers`
# is an authenticated route, because letting any machine that can reach the
# server enrol itself would make the subscriber list something strangers can
# add to. A human turning a key into an entitlement is the gate.
SUPPORT_EMAIL = ""


def needs_setup(config: ExecutorConfig) -> bool:
    """Whether this machine has enough to run at all.

    Deliberately the same question `validate()` asks, rather than "does a config
    file exist". A file with three of the five fields filled in is not a set-up
    machine, and answering yes to the easier question is how a buyer ends up at
    a console that cannot connect instead of at a page that tells them why.
    """
    try:
        config.validate()
    except ConfigError:
        return True
    return False


# ── checking what was typed ──────────────────────────────────────────


def check_fields(form: dict) -> list:
    """Everything wrong with the form, in the order the fields appear.

    All of them, not the first: a buyer who fixes one field, presses Save and
    is told about the next one has been made to do the work four times.
    """
    problems = []
    url = str(form.get("server_url") or "").strip()
    if not url:
        problems.append(("server_url", "Where the signal comes from. This is in your subscription email."))
    elif not url.startswith(("http://", "https://")):
        problems.append(("server_url", "Should start with https://"))

    if not str(form.get("buyer_id") or "").strip():
        problems.append(("buyer_id", "The name your subscription is under."))
    if not str(form.get("root_public_key") or "").strip():
        problems.append(("root_public_key", "The long code from your subscription email."))

    exchange = str(form.get("exchange") or "").strip().lower()
    if exchange not in ("binance", "coindcx"):
        problems.append(("exchange", "Choose Binance or CoinDCX."))

    try:
        capital = float(form.get("capital_usd") or 0)
    except (TypeError, ValueError):
        capital = 0.0
    # The gate's own refusal, verbatim. It already explains WHY the floor
    # exists, and rewording it here would give a buyer two different sentences
    # for the same rule depending on which screen they hit it from.
    allowed, _, warning = model.capital_gate(capital)
    if not allowed:
        problems.append(("capital_usd", warning))

    if not str(form.get("api_key") or "").strip():
        problems.append(("api_key", "From your exchange. Trading permission only — never withdrawals."))
    if not str(form.get("api_secret") or "").strip():
        problems.append(("api_secret", "Shown once when you created the key."))
    return problems


def check_exchange(config: ExecutorConfig) -> str:
    """Ask the venue whether these credentials actually work. '' means yes.

    Worth the round trip before anything is written. A key typed with a missing
    character fails at the first tick with a number from the exchange, hours
    later, on a page the buyer has already walked away from — whereas here it is
    still on screen next to the box they typed it into.
    """
    try:
        adapter = build_adapter(config)
        adapter.free_balance(config.quote_asset)
    except Exception as exc:
        return (
            f"Your exchange refused these credentials: {exc}. Check the key and secret are "
            "the right way round, that trading is ticked, and that any IP restriction on the "
            "key includes this computer."
        )
    return ""


def write_setup(form: dict, *, path: Optional[str] = None, store=None, verify=None) -> dict:
    """Validate, then write. Returns what to tell the buyer.

    The order matters and is the whole of this function's job: check the fields,
    then check them against the exchange, and only then touch the disk. Writing
    first and validating after is how a machine ends up holding a config it
    cannot use and a buyer ends up editing JSON after all — the thing this page
    exists to prevent.
    """
    problems = check_fields(form)
    if problems:
        return {"ok": False, "problems": [{"field": f, "message": m} for f, m in problems]}

    config = ExecutorConfig(
        server_url=str(form["server_url"]).strip().rstrip("/"),
        buyer_id=str(form["buyer_id"]).strip(),
        root_public_key=str(form["root_public_key"]).strip(),
        exchange=str(form["exchange"]).strip().lower(),
        capital_usd=float(form["capital_usd"]),
        api_key=str(form["api_key"]).strip(),
        api_secret=str(form["api_secret"]).strip(),
    )
    config.signal_exchanges = [config.exchange]
    config.timeframes = [
        tf for tf in _as_list(form.get("timeframes")) if model.timeframe_allowed_on(tf, config.exchange)
    ]

    refusal = (verify or check_exchange)(config)
    if refusal:
        return {"ok": False, "problems": [{"field": "api_key", "message": refusal}]}

    try:
        where = (store or secrets.store)(config.buyer_id, config.api_key, config.api_secret)
    except secrets.SecretsUnavailable as exc:
        return {"ok": False, "problems": [{"field": "api_key", "message": str(exc)}]}

    written = _write_config(config, path=path)
    return {
        "ok": True,
        "config_path": written,
        "secrets_where": where,
        "message": (
            f"Set up. Your settings are in {written}, and your exchange key is in {where} — "
            "not in that file, and not anywhere this program can print it."
        ),
    }


def _write_config(config: ExecutorConfig, *, path: Optional[str] = None) -> str:
    """The config file, with the credentials pointedly left out of it.

    They went to the credential store a moment ago. Writing them here as well
    would mean a buyer who was told their key is in the Keychain also has it in
    a JSON file they will one day paste into a support chat.
    """
    resolved = os.path.abspath(os.path.expanduser(path or DEFAULT_CONFIG))
    data = {
        "server_url": config.server_url,
        "buyer_id": config.buyer_id,
        "root_public_key": config.root_public_key,
        "exchange": config.exchange,
        "capital_usd": config.capital_usd,
        "symbols": [],
        "timeframes": list(config.timeframes),
        "signal_exchanges": list(config.signal_exchanges),
        "tick_seconds": config.tick_seconds,
    }
    os.makedirs(os.path.dirname(resolved), exist_ok=True)
    temporary = f"{resolved}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
    os.replace(temporary, resolved)
    # Readable by this user only. It carries no secret, but it does carry the
    # buyer id and the server they answer to, and there is no reason for the
    # rest of the machine to have either.
    try:
        os.chmod(resolved, 0o600)
    except OSError:  # pragma: no cover - filesystems that do not carry modes
        pass
    return resolved


def _as_list(value) -> list:
    if isinstance(value, str):
        return [part.strip().lower() for part in value.split(",") if part.strip()]
    return [str(part).strip().lower() for part in (value or []) if str(part).strip()]


# ── the server ───────────────────────────────────────────────────────


class SetupServer:
    """Serves the setup page until it is finished, then gets out of the way.

    Separate from `UIServer` on purpose. That one is wired to a running
    executor and every button on it acts on live orders; this one runs when
    there is no executor at all. Keeping them apart means the page a
    not-yet-configured machine exposes cannot reach anything that trades.
    """

    def __init__(
        self,
        *,
        port: int = DEFAULT_PORT,
        config_path: Optional[str] = None,
        key_path: Optional[str] = None,
    ):
        self._port = port
        self._config_path = config_path
        # Where this machine's own key lives. Given, so the page can show the
        # public half and retire `python -m executor --register` — the last
        # command a buyer had to type before any of this existed.
        self._key_path = key_path
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._done = threading.Event()
        self.result: Optional[dict] = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def public_key(self) -> str:
        """This machine's public key, creating it on first ask.

        The key is generated locally and its private half never leaves — which
        is the same principle that keeps the exchange credentials off our
        server. It does not depend on the buyer id, so making it before they
        have one is safe: the id is a label carried alongside, not an input to
        the key.

        Returns '' rather than raising when there is nowhere to keep it. The
        rest of the page still works, and a buyer with a read-only home
        directory has a bigger problem to be told about than this one.
        """
        if not self._key_path:
            return ""
        try:
            from executor.transport import ExecutorIdentity

            return ExecutorIdentity.load_or_create(self._key_path, "unset").public_key_b64()
        except Exception as exc:
            _log.warning("could not create this machine's key: %s", exc)
            return ""

    def start(self) -> Optional[str]:
        """Serve, or say why not."""
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                if httpguard.refuse(self):
                    return
                if self.path in ("/", "/index.html"):
                    body = PAGE.encode("utf-8")
                    content = "text/html; charset=utf-8"
                elif self.path == "/api/setup-state":
                    body = json.dumps(
                        {
                            "keyring": secrets.available(),
                            "keyring_name": secrets.describe(),
                            "public_key": outer.public_key(),
                            "support_email": SUPPORT_EMAIL,
                            "timeframes": {
                                venue: list(model.timeframes_for(venue)) for venue in ("binance", "coindcx")
                            },
                        }
                    ).encode("utf-8")
                    content = "application/json"
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", content)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                if httpguard.refuse(self):
                    return
                if self.path != "/api/setup":
                    self.send_error(404)
                    return
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                    form = json.loads(self.rfile.read(length) or b"{}")
                except Exception:
                    self.send_error(400)
                    return
                try:
                    result = write_setup(form, path=outer._config_path)
                except Exception as exc:
                    _log.exception("setup failed")
                    result = {"ok": False, "problems": [{"field": "", "message": f"Could not save: {exc}"}]}
                body = json.dumps(result).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                if result.get("ok"):
                    # Released only after the response is on the wire, or the
                    # caller can tear the server down before the browser has
                    # been told it worked.
                    outer.result = result
                    outer._done.set()

        try:
            self._server = ThreadingHTTPServer(("127.0.0.1", self._port), Handler)
        except OSError as exc:
            return f"Setup page could not start: port {self._port} is busy ({exc})."
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="cascade-setup")
        self._thread.start()
        return None

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until the buyer finishes. False if the timeout ran out first."""
        return self._done.wait(timeout)

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server = None


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cascade — setting up</title>
<style>
  :root { color-scheme: dark; --bg:#040814; --card:#0b1225; --line:#1e2b45;
          --text:#e7edf7; --muted:#94a3b8; --accent:#22d3ee; --bad:#fb7185; --good:#34d399; }
  * { box-sizing: border-box; }
  body { margin:0; padding:32px 16px 64px; background:var(--bg); color:var(--text);
         font:15px/1.6 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
  main { max-width: 620px; margin: 0 auto; }
  h1 { font-size:24px; margin:0 0 6px; letter-spacing:-0.01em; }
  .sub { color:var(--muted); margin:0 0 28px; }
  fieldset { border:1px solid var(--line); border-radius:12px; background:var(--card);
             padding:20px; margin:0 0 18px; }
  legend { padding:0 8px; color:var(--accent); font-size:13px; letter-spacing:.08em;
           text-transform:uppercase; }
  label { display:block; margin:0 0 16px; }
  label:last-child { margin-bottom:0; }
  .name { display:block; margin-bottom:6px; font-weight:600; }
  .hint { display:block; color:var(--muted); font-size:13px; font-weight:400; margin-top:2px; }
  input, select { width:100%; padding:10px 12px; border-radius:8px; border:1px solid var(--line);
                  background:#050a18; color:var(--text); font:inherit; font-size:16px; }
  input:focus, select:focus { outline:2px solid var(--accent); outline-offset:1px; }
  .err { color:var(--bad); font-size:13px; margin-top:6px; display:none; }
  .err.on { display:block; }
  button { width:100%; padding:14px; border-radius:10px; border:0; background:var(--accent);
           color:#04212a; font:inherit; font-weight:700; font-size:16px; cursor:pointer; }
  button[disabled] { opacity:.55; cursor:progress; }
  #done { display:none; border:1px solid var(--good); border-radius:12px; padding:20px;
          background:rgba(52,211,153,.08); }
  #done.on { display:block; }
  #done h2 { margin:0 0 8px; font-size:18px; color:var(--good); }
  code { background:#050a18; padding:2px 6px; border-radius:4px; font-size:13px; }
  .warn { color:var(--bad); font-size:13px; margin-top:10px; }
  .key { font:13px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; word-break:break-all;
         background:#050a18; border:1px solid var(--line); border-radius:8px; padding:12px;
         color:var(--text); user-select:all; }
  .row { display:flex; gap:10px; margin:12px 0 0; flex-wrap:wrap; }
  .row button, .row .btn { width:auto; flex:1 1 160px; padding:10px 14px; font-size:15px;
         border-radius:8px; text-align:center; text-decoration:none; }
  .row .btn { background:transparent; border:1px solid var(--accent); color:var(--accent);
              font-weight:600; line-height:1.6; }
  #copy.done { background:var(--good); }
</style>
</head>
<body>
<main>
  <h1>Set up Cascade</h1>
  <p class="sub">Two steps, once. Nothing on this page is sent to us except the code in step one.</p>

  <fieldset id="reg">
    <legend>Step 1 — send us this code</legend>
    <p class="sub" style="margin-top:0">This machine has just made itself a key. Send us the public half —
      the private half stays here and never leaves, which is what stops us from ever being able to act as you.</p>
    <div class="key" id="pk">…</div>
    <div class="row">
      <button type="button" id="copy">Copy the code</button>
      <a id="mail" class="btn" style="display:none">Email it to us</a>
    </div>
    <p class="sub">We'll reply with two things — <b>your buyer name</b> and <b>your subscription code</b> —
      which are the first two boxes in step two. If you already have that email, carry straight on.</p>
  </fieldset>

  <form id="f">
    <fieldset>
      <legend>Step 2 — your subscription</legend>
      <label><span class="name">Server address<span class="hint">From your subscription email.</span></span>
        <input name="server_url" value="https://crypto.philforge.in" autocomplete="off"><span class="err" data-for="server_url"></span></label>
      <label><span class="name">Your buyer name<span class="hint">The name your subscription is under.</span></span>
        <input name="buyer_id" placeholder="buyer-your-name" autocomplete="off"><span class="err" data-for="buyer_id"></span></label>
      <label><span class="name">Subscription code<span class="hint">The long code in the same email. It is how this machine knows our signal is really ours.</span></span>
        <input name="root_public_key" autocomplete="off"><span class="err" data-for="root_public_key"></span></label>
    </fieldset>

    <fieldset>
      <legend>Step 2 — your exchange</legend>
      <label><span class="name">Exchange</span>
        <select name="exchange"><option value="binance">Binance</option><option value="coindcx">CoinDCX</option></select>
        <span class="err" data-for="exchange"></span></label>
      <label><span class="name">Money to trade with (USD)<span class="hint">What every ladder is sized from. It is not moved anywhere — it is the number this machine divides up.</span></span>
        <input name="capital_usd" type="number" min="0" step="100" value="1000"><span class="err" data-for="capital_usd"></span></label>
      <label><span class="name">API key<span class="hint">Trading permission only. Never tick withdrawals.</span></span>
        <input name="api_key" autocomplete="off" spellcheck="false"><span class="err" data-for="api_key"></span></label>
      <label><span class="name">API secret<span class="hint">Shown once when you created the key.</span></span>
        <input name="api_secret" type="password" autocomplete="off" spellcheck="false"><span class="err" data-for="api_secret"></span></label>
      <p class="sub" id="where" style="margin:0"></p>
    </fieldset>

    <button type="submit" id="go">Check and save</button>
  </form>

  <div id="done"><h2>All set</h2><p id="donemsg"></p><p class="sub">You can close this tab.</p></div>
</main>
<script>
const f = document.getElementById("f"), go = document.getElementById("go");
fetch("/api/setup-state", {headers:{"X-Cascade-UI":"1"}}).then(r=>r.json()).then(s=>{
  const pk = document.getElementById("pk");
  if (s.public_key) {
    pk.textContent = s.public_key;
    if (s.support_email) {
      const mail = document.getElementById("mail");
      mail.href = "mailto:" + s.support_email
        + "?subject=" + encodeURIComponent("Cascade registration")
        + "&body=" + encodeURIComponent("Please register this machine.\\n\\n" + s.public_key + "\\n");
      mail.style.display = "";
    }
  } else {
    // No key means nowhere to write one. Saying so beats a box of dots that
    // never fills in.
    pk.textContent = "This machine could not create its key — check that your user account can write to your home folder.";
    pk.className = "key warn";
  }
  const w = document.getElementById("where");
  w.textContent = s.keyring
    ? "Your key and secret go into " + s.keyring_name + ", not into a file."
    : "";
  if (!s.keyring) { w.className = "warn"; w.textContent =
    "This computer has no password store I can use, so I cannot keep your key safely. Setting it up from the command line is the safe route here."; }
}).catch(()=>{});

document.getElementById("copy").addEventListener("click", async (e) => {
  const text = document.getElementById("pk").textContent;
  try { await navigator.clipboard.writeText(text); }
  catch (err) {
    // Clipboard permission can be refused. Select it instead so one keystroke
    // still does the job, rather than a button that silently does nothing.
    const range = document.createRange();
    range.selectNodeContents(document.getElementById("pk"));
    const sel = window.getSelection(); sel.removeAllRanges(); sel.addRange(range);
  }
  e.target.textContent = "Copied"; e.target.className = "done";
  setTimeout(() => { e.target.textContent = "Copy the code"; e.target.className = ""; }, 2000);
});

f.addEventListener("submit", async (e) => {
  e.preventDefault();
  document.querySelectorAll(".err").forEach(n => { n.className = "err"; n.textContent = ""; });
  go.disabled = true; go.textContent = "Checking with your exchange…";
  const form = Object.fromEntries(new FormData(f).entries());
  try {
    const r = await fetch("/api/setup", {
      method: "POST",
      headers: {"Content-Type":"application/json", "X-Cascade-UI":"1"},
      body: JSON.stringify(form)
    });
    const out = await r.json();
    if (out.ok) {
      f.style.display = "none";
      document.getElementById("donemsg").textContent = out.message;
      document.getElementById("done").className = "on";
      return;
    }
    for (const p of (out.problems || [])) {
      const n = document.querySelector('.err[data-for="' + p.field + '"]');
      if (n) { n.className = "err on"; n.textContent = p.message; }
      else { alert(p.message); }
    }
  } catch (err) {
    alert("Could not save: " + err);
  }
  go.disabled = false; go.textContent = "Check and save";
});
</script>
</body>
</html>
"""
