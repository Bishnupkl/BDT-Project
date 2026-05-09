#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"
ENV_FILE="$ROOT_DIR/.env"
TEST_TOPIC="crypto-topic-integration-$(date +%s)"

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d zookeeper kafka kafka-setup
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic "$TEST_TOPIC" \
  --partitions 1 \
  --replication-factor 1

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" build crypto-producer
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" run --rm \
  -e KAFKA_TOPIC="$TEST_TOPIC" \
  -e PRODUCER_MAX_EVENTS=5 \
  -e PRODUCER_INTERVAL_MS=100 \
  crypto-producer

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic "$TEST_TOPIC" \
  --from-beginning \
  --max-messages 5

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --delete \
  --if-exists \
  --topic "$TEST_TOPIC"
