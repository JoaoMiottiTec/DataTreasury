#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/datatreasury}"
MIGRATIONS_DIR="warehouse/migrations"

echo "[migrate] Starting database migrations..."
echo "[migrate] Using database: $DB_URL"
echo "[migrate] Using directory: $MIGRATIONS_DIR"
echo "----------------------------------------------"

if ! command -v psql &> /dev/null; then
  echo "[error] psql command not found. Please install PostgreSQL client tools."
  exit 1
fi

if ! psql "$DB_URL" -c "SELECT 1;" >/dev/null 2>&1; then
  echo "[error] Failed to connect to database at $DB_URL"
  exit 1
fi

for file in $(ls "$MIGRATIONS_DIR"/*.sql | sort); do
  echo "[migrate] Applying $(basename "$file")..."
  psql "$DB_URL" -f "$file"
done

echo "----------------------------------------------"
echo "[migrate] ✅ All migrations applied successfully!"
