#!/usr/bin/env bash
# CoinDCX paper instance — runs beside the live Binance app, fully isolated.
# State lives in ~/cryptoforge-coindcx so the mainnet app's campaigns are
# never restored here; broker order calls are paper-guarded in the engine.
# Login: PIN 246810, authenticator off — localhost paper sandbox only.
# Every value can be overridden from the environment before launch.
set -euo pipefail
cd "$(dirname "$0")/.."

export CRYPTOFORGE_BROKER="${CRYPTOFORGE_BROKER:-coindcx}"
export APP_PORT="${APP_PORT:-9001}"
export CRYPTOFORGE_STATE_DIR="${CRYPTOFORGE_STATE_DIR:-$HOME/cryptoforge-coindcx}"
export CRYPTOFORGE_PIN="${CRYPTOFORGE_PIN:-246810}"
# Exported empty on purpose: config.py's load_dotenv() never overrides real
# env vars, so this blanks the .env TOTP secret and PIN-only login works.
export CRYPTOFORGE_TOTP_SECRET="${CRYPTOFORGE_TOTP_SECRET:-}"

exec python3 -m uvicorn app:app --host 127.0.0.1 --port "$APP_PORT"
