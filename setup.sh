#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
[[ -f .env ]] || { echo 'Crie .env a partir de .env.example e configure POSTGRES_PASSWORD.' >&2; exit 1; }
command -v python3 >/dev/null
docker compose version >/dev/null
docker info >/dev/null
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
mkdir -p artifacts
docker compose up -d --build --wait --wait-timeout 240
.venv/bin/python scripts/validate.py 2>&1 | tee artifacts/validation.txt
printf '%s\n' 'Pipeline validado. Para remover os recursos: ./teardown.sh'
