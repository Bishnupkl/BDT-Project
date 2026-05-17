#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"
ENV_FILE="$ROOT_DIR/.env"

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" build spark-app

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" run --rm \
  --no-deps \
  --entrypoint /opt/spark/bin/spark-submit \
  spark-app \
  --master local[*] \
  /opt/spark-app/jobs/check_hive_crypto_analytics.py "$@"
