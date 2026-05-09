#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The -v flag ensures that named volumes (like kafka-data) are removed.
# This prevents corrupted state issues when restarting Kafka.
docker compose --env-file "$ROOT_DIR/.env" -f "$ROOT_DIR/docker/docker-compose.yml" down -v