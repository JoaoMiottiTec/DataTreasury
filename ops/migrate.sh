#!/usr/bin/env bash

set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/datatreasury}"

echo "[migrate] Applying 0001_init.sql..."
psql "$DB_URL" -f warehouse/migrations/0001_init.sql

echo "[migrate] Applying 0002_init.sql..."
psql "$DB_URL" -f warehouse/migrations/0002_init.sql

echo "[migrate] Done"