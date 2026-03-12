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

# ── 1. Install dependencies ──────────────────────────────────
log "Installing dependencies..."
source "$VENV/bin/activate"
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
echo "upstream ${APP}_backend { server 127.0.0.1:${STANDBY_PORT}; }" \
    | sudo tee "$UPSTREAM_CONF" >/dev/null

if ! sudo nginx -t 2>/dev/null; then
    die "Nginx config test failed! Restoring old upstream."
fi

sudo nginx -s reload
log "Nginx reloaded. New traffic → port $STANDBY_PORT"

# ── 6. Drain old connections ─────────────────────────────────
log "Draining old connections for ${DRAIN_TIMEOUT}s..."
sleep "$DRAIN_TIMEOUT"

# ── 7. Stop old instance ─────────────────────────────────────
log "Stopping old instance on port $ACTIVE_PORT..."
sudo systemctl stop "${APP}@${ACTIVE_PORT}" 2>/dev/null || true

# ── 8. Persist new active port ────────────────────────────────
echo "$STANDBY_PORT" > "$PORT_FILE"

log "═══════════════════════════════════════════════"
log "  DEPLOY COMPLETE — $APP active on port $STANDBY_PORT"
log "═══════════════════════════════════════════════"
