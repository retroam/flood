#!/usr/bin/env bash
set -euo pipefail

.venv/bin/uvicorn quake_sql.main:app --host 0.0.0.0 --port 8000
