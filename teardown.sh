#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
[[ -f .env ]] || { echo 'O .env do laboratorio e necessario para resolver o Compose.' >&2; exit 1; }
mkdir -p artifacts
docker compose --profile "*" down -v --remove-orphans 2>&1 | tee artifacts/teardown.txt
remaining="$(docker ps -aq --filter label=com.docker.compose.project=payments-cdc-flink)"
[[ -z "$remaining" ]] || { echo 'FAIL: ainda existem containers do projeto'; exit 1; }
remaining="$(docker volume ls -q --filter label=com.docker.compose.project=payments-cdc-flink)"
[[ -z "$remaining" ]] || { echo 'FAIL: ainda existem volumes do projeto'; exit 1; }
printf '%s\n' 'PASS: project containers=[] volumes=[]' | tee -a artifacts/teardown.txt
# Preserva o .env e os logs locais para permitir inspecao. Nao remove recursos Cloud.
