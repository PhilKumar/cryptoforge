#!/bin/bash
# CryptoForge EC2 Deployment Script
# Run this once on a fresh Ubuntu 22.04 t3.micro (or similar)
# Usage: bash deploy.sh YOUR_ELASTIC_IP
#
# Prerequisites:
#   - Ubuntu 22.04+
#   - SSH access to EC2 instance
#   - Delta Exchange API key and secret

set -e
ELASTIC_IP=${1:-"YOUR_ELASTIC_IP"}
APP_DIR="/home/ubuntu/cryptoforge"

echo "╔══════════════════════════════════════════════╗"
echo "║       CryptoForge — Deployment Script        ║"
echo "╚══════════════════════════════════════════════╝"

echo ""
echo "==> Updating system packages..."
sudo apt-get update -y && sudo apt-get upgrade -y

echo "==> Installing Python 3.11, nginx, git..."
sudo apt-get install -y python3.11 python3.11-venv python3-pip nginx git

echo "==> Cloning / pulling repo..."
if [ -d "$APP_DIR" ]; then
  cd "$APP_DIR" && git pull
else
  git clone https://github.com/YOUR_GITHUB_USERNAME/CryptoForge.git "$APP_DIR"
  cd "$APP_DIR"
fi

echo "==> Creating virtual environment..."
python3.11 -m venv venv
source venv/bin/activate

echo "==> Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "==> Setting up .env file..."
if [ ! -f "$APP_DIR/.env" ]; then
  cat > "$APP_DIR/.env" <<EOF
# CryptoForge Environment Variables
# ──────────────────────────────────
# Delta Exchange API (https://www.delta.exchange/app/account/api-keys)
DELTA_API_KEY=YOUR_API_KEY_HERE
DELTA_API_SECRET=YOUR_API_SECRET_HERE

# App settings
APP_HOST=0.0.0.0
APP_PORT=9000
DEBUG=false

# Auth PIN (change this!)
CRYPTOFORGE_PIN=202603

# Session secret (auto-generated if empty)
# SESSION_SECRET=
EOF
  echo ""
  echo "!! IMPORTANT: Edit $APP_DIR/.env and add your Delta Exchange credentials"
  echo "!!            before starting the service."
  echo ""
fi

echo "==> Installing systemd service..."
sudo cp "$APP_DIR/deploy/cryptoforge.service" /etc/systemd/system/cryptoforge.service
sudo systemctl daemon-reload
sudo systemctl enable cryptoforge
sudo systemctl restart cryptoforge

echo "==> Configuring nginx..."
sudo sed "s/YOUR_ELASTIC_IP/$ELASTIC_IP/g" "$APP_DIR/deploy/nginx.conf" \
  > /tmp/cryptoforge_nginx.conf
sudo cp /tmp/cryptoforge_nginx.conf /etc/nginx/sites-available/cryptoforge
sudo ln -sf /etc/nginx/sites-available/cryptoforge /etc/nginx/sites-enabled/cryptoforge
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl restart nginx

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║             Deployment Complete!              ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "  CryptoForge is running at http://$ELASTIC_IP"
echo ""
echo "  Useful commands:"
echo "    Check status:  sudo systemctl status cryptoforge"
echo "    View logs:     sudo journalctl -u cryptoforge -f"
echo "    Restart:       sudo systemctl restart cryptoforge"
echo "    Edit config:   nano $APP_DIR/.env"
echo ""
