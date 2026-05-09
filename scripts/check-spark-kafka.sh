#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose --env-file "$ROOT_DIR/.env" -f "$ROOT_DIR/docker/docker-compose.yml" up -d kafka spark-master spark-worker
docker compose --env-file "$ROOT_DIR/.env" -f "$ROOT_DIR/docker/docker-compose.yml" exec -T spark-master getent hosts kafka
