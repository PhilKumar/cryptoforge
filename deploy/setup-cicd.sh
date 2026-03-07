#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
#  One-time server setup for Blue-Green CI/CD
#  Run this ONCE on the EC2 server to migrate from the old
#  single-service model to template-based blue-green deploys.
#
#  Usage: bash deploy/setup-cicd.sh
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

echo "╔══════════════════════════════════════════════╗"
echo "║   CI/CD Blue-Green Setup — CryptoForge       ║"
echo "╚══════════════════════════════════════════════╝"

APP_DIR="/home/ec2-user/cryptoforge"
BLUE_PORT=9000

# ── 1. Install systemd template service ──────────────────────
echo "==> Installing cryptoforge@.service template..."
sudo cp "$APP_DIR/deploy/cryptoforge.service" /etc/systemd/system/cryptoforge@.service
sudo systemctl daemon-reload

# ── 2. Stop old monolithic service (if running) ──────────────
if systemctl is-active --quiet cryptoforge 2>/dev/null; then
    echo "==> Stopping old cryptoforge.service..."
    sudo systemctl stop cryptoforge
    sudo systemctl disable cryptoforge 2>/dev/null || true
fi

# ── 3. Create initial upstream config ────────────────────────
echo "==> Creating nginx upstream config..."
echo "upstream cryptoforge_backend { server 127.0.0.1:${BLUE_PORT}; }" \
    | sudo tee /etc/nginx/conf.d/cryptoforge-upstream.conf >/dev/null

# ── 4. Install new nginx site config ─────────────────────────
echo "==> Installing nginx site config..."
sudo cp "$APP_DIR/deploy/nginx.conf" /etc/nginx/conf.d/cryptoforge.conf
sudo nginx -t && sudo nginx -s reload
echo "    Nginx config OK and reloaded."

# ── 5. Start blue instance ───────────────────────────────────
echo "==> Starting cryptoforge@${BLUE_PORT}..."
sudo systemctl start "cryptoforge@${BLUE_PORT}"

# ── 6. Initialize port state file ────────────────────────────
echo "$BLUE_PORT" > "$HOME/.cryptoforge-active-port"

# ── 7. Make deploy script executable ─────────────────────────
chmod +x "$APP_DIR/deploy/cd-deploy.sh"

echo ""
echo "==> DONE! CryptoForge blue-green is ready."
echo "    Active: port $BLUE_PORT"
echo "    State:  ~/.cryptoforge-active-port"
echo "    Test:   curl http://127.0.0.1:${BLUE_PORT}/health"
