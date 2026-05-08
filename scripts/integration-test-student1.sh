#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka kafka-setup
docker compose -f "$COMPOSE_FILE" run --rm \
  -e PRODUCER_MAX_EVENTS=5 \
  -e PRODUCER_INTERVAL_MS=100 \
  crypto-producer

docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic crypto-topic \
  --from-beginning \
  --max-messages 5
