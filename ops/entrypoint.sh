#!/usr/bin/env bash
set -euo pipefail

cd /app

echo "[entrypoint] Installing Python dependencies..."
pip install --no-cache-dir -r requirements.txt

echo "[entrypoint] Running database migrations..."
python /app/ops/migrate.py

echo "[entrypoint] Starting Prefect worker..."
exec prefect worker start -q datatreasury
