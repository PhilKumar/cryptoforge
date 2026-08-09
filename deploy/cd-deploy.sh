#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
#  CryptoForge — Zero-Downtime Blue-Green Deployment
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

APP="cryptoforge"
APP_DIR="/home/ec2-user/cryptoforge"
VENV="$APP_DIR/venv"

BLUE_PORT=9000
GREEN_PORT=9001
PORT_FILE="$HOME/.${APP}-active-port"
UPSTREAM_CONF="/etc/nginx/conf.d/${APP}-upstream.conf"
STATE_DIR="${CRYPTOFORGE_STATE_DIR:-$HOME/.cryptoforge}"

HEALTH_PATH="/api/health"
HEALTH_TIMEOUT=30
DRAIN_TIMEOUT=30

LOG_TAG="[DEPLOY]"

log()  { echo "$LOG_TAG $(date '+%H:%M:%S') $*"; }
die()  { log "ERROR: $*"; exit 1; }

health_check() {
    local port=$1
    for i in $(seq 1 "$HEALTH_TIMEOUT"); do
        if curl -sf --max-time 3 "http://127.0.0.1:${port}${HEALTH_PATH}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

runtime_is_active() {
    local payload
    if ! payload=$(curl -sf --max-time 5 "http://127.0.0.1:${1}/api/ready"); then
        return 2
    fi
    printf '%s' "$payload" | "$VENV/bin/python" -c '
import json, sys
runtime = json.load(sys.stdin).get("runtime", {})
active = bool(
    runtime.get("live_running_runs")
    or runtime.get("paper_running_runs")
    or runtime.get("scalp_running")
    or runtime.get("scalp_open_trades")
    or runtime.get("scalp_pending_entries")
    or runtime.get("cascade_active_campaigns")
)
raise SystemExit(0 if active else 1)
'
}

restore_upstream() {
    local backup=$1
    if [[ -s "$backup" ]]; then
        sudo cp "$backup" "$UPSTREAM_CONF"
        sudo nginx -t >/dev/null
        sudo nginx -s reload
    fi
}

# ── Determine active/standby ─────────────────────────────────
if [[ -f "$PORT_FILE" ]]; then
    ACTIVE_PORT=$(cat "$PORT_FILE")
else
    ACTIVE_PORT=$BLUE_PORT
    echo "$ACTIVE_PORT" > "$PORT_FILE"
fi

if [[ "$ACTIVE_PORT" == "$BLUE_PORT" ]]; then
    STANDBY_PORT=$GREEN_PORT
else
    STANDBY_PORT=$BLUE_PORT
fi

log "Active: port $ACTIVE_PORT → Deploying to: port $STANDBY_PORT"

# A source deploy restarts the process and rebinds broker clients. Never do
# that while any engine, pending scalp entry, or Cascade campaign is active.
runtime_status=0
runtime_is_active "$ACTIVE_PORT" || runtime_status=$?
if [[ "$runtime_status" -eq 0 ]]; then
    die "Active trading runtime detected on port $ACTIVE_PORT. Stop it and verify broker state before deploying."
fi
if [[ "$runtime_status" -eq 2 ]] && sudo fuser "${ACTIVE_PORT}/tcp" >/dev/null 2>&1; then
    die "The active worker on port $ACTIVE_PORT did not return runtime state. Deployment is blocked closed."
fi

# The state database contains sessions, broker-derived trading records and
# recovery snapshots. Existing installations pre-date systemd's UMask and may
# still have inherited world-readable modes.
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"
find "$STATE_DIR" -maxdepth 1 -type f -name '*.db*' -exec chmod 600 {} +

# ── 1. Install dependencies ──────────────────────────────────
log "Installing dependencies..."
source "$VENV/bin/activate"
pip install -q --disable-pip-version-check --upgrade pip setuptools wheel
pip install -q --disable-pip-version-check -r "$APP_DIR/requirements.txt"

# ── 2. Stop standby if somehow still running ──────────────────
sudo systemctl stop "${APP}@${STANDBY_PORT}" 2>/dev/null || true

# ── 2b. Kill any stale process holding the standby port ───────
if sudo fuser "${STANDBY_PORT}/tcp" >/dev/null 2>&1; then
    log "⚠ Stale process on port $STANDBY_PORT — killing..."
    sudo fuser -k "${STANDBY_PORT}/tcp" 2>/dev/null || true
    sleep 1
fi
sleep 1

# ── 3. Start standby instance ────────────────────────────────
log "Starting standby on port $STANDBY_PORT..."
sudo systemctl start "${APP}@${STANDBY_PORT}"

# ── 4. Health check standby ──────────────────────────────────
log "Waiting for standby health check..."
if ! health_check "$STANDBY_PORT"; then
    log "ROLLBACK — standby failed health check! Stopping standby."
    log "── Last 40 lines of journal for ${APP}@${STANDBY_PORT} ──"
    sudo journalctl -u "${APP}@${STANDBY_PORT}" --no-pager -n 40 || true
    sudo systemctl stop "${APP}@${STANDBY_PORT}" 2>/dev/null || true
    die "Deploy aborted. Active instance on port $ACTIVE_PORT unchanged."
fi
log "Standby is healthy!"

# ── 5. Swap nginx upstream ───────────────────────────────────
log "Switching nginx to port $STANDBY_PORT..."
UPSTREAM_BACKUP=$(mktemp)
sudo cp "$UPSTREAM_CONF" "$UPSTREAM_BACKUP"
echo "upstream ${APP}_backend { server 127.0.0.1:${STANDBY_PORT}; }" \
    | sudo tee "$UPSTREAM_CONF" >/dev/null

if ! sudo nginx -t 2>/dev/null; then
    restore_upstream "$UPSTREAM_BACKUP"
    sudo systemctl stop "${APP}@${STANDBY_PORT}" 2>/dev/null || true
    die "Nginx config test failed. The previous upstream was restored."
fi

sudo nginx -s reload
log "Nginx reloaded. New traffic → port $STANDBY_PORT"

# Verify the route through nginx, not only the worker's loopback port. A valid
# worker behind a broken upstream is still a failed deployment.
if ! curl -sf --max-time 5 -H 'Host: crypto.philforge.in' "http://127.0.0.1${HEALTH_PATH}" >/dev/null; then
    restore_upstream "$UPSTREAM_BACKUP"
    sudo systemctl stop "${APP}@${STANDBY_PORT}" 2>/dev/null || true
    die "Routed health check failed. The previous upstream was restored."
fi

# From here the new route is authoritative. Persist it before draining so an
# old-worker shutdown problem cannot leave the port file disagreeing with
# nginx.
echo "$STANDBY_PORT" > "$PORT_FILE"

# ── 6. Drain old connections ─────────────────────────────────
log "Draining old connections for ${DRAIN_TIMEOUT}s..."
sleep "$DRAIN_TIMEOUT"

# ── 7. Stop old instance ─────────────────────────────────────
log "Stopping old instance on port $ACTIVE_PORT..."
if ! sudo systemctl stop "${APP}@${ACTIVE_PORT}"; then
    log "Old worker did not stop cleanly; forcing the failed unit down to preserve single-writer safety."
    sudo systemctl kill --kill-who=all "${APP}@${ACTIVE_PORT}" 2>/dev/null || true
fi
if sudo fuser "${ACTIVE_PORT}/tcp" >/dev/null 2>&1; then
    log "A process still owns old port $ACTIVE_PORT; terminating that stale listener."
    sudo fuser -k "${ACTIVE_PORT}/tcp" 2>/dev/null || true
    sleep 1
fi
if sudo fuser "${ACTIVE_PORT}/tcp" >/dev/null 2>&1; then
    die "Old worker still owns port $ACTIVE_PORT. The new worker remains active, but manual cleanup is required."
fi

# ── 8. Clean temporary deployment state ─────────────────────
rm -f "$UPSTREAM_BACKUP"

log "═══════════════════════════════════════════════"
log "  DEPLOY COMPLETE — $APP active on port $STANDBY_PORT"
log "═══════════════════════════════════════════════"
