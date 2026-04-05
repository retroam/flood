#!/usr/bin/env bash
set -euo pipefail

docker compose up -d clickhouse

echo "Waiting for ClickHouse to accept connections..."
until curl --silent --fail http://127.0.0.1:8123/ping >/dev/null; do
  sleep 2
done

.venv/bin/python -m quake_sql.bootstrap
