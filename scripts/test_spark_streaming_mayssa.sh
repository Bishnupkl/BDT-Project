#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"
ENV_FILE="$ROOT_DIR/.env"
TEST_ID="-$(date +%s)"
TEST_TOPIC="crypto-topic-${TEST_ID}"
TABLE_DIR="$ROOT_DIR/data/crypto_analytics.db"
CHECKPOINT_DIR="$ROOT_DIR/data/checkpoints"
CONTAINER_TABLE_DIR="/opt/spark-app/storage/crypto_analytics.db"
CONTAINER_CHECKPOINT_DIR="/opt/spark-app/storage/checkpoints"

cleanup() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" rm -sf spark-app >/dev/null 2>&1 || true
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --delete \
    --if-exists \
    --topic "$TEST_TOPIC" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_file() {
  local label="$1"
  local path="$2"
  local timeout_seconds="${3:-90}"
  local elapsed=0

  while (( elapsed < timeout_seconds )); do
    if find "$path" -name '*.parquet' -type f 2>/dev/null | grep -q .; then
      echo "PASS: $label Parquet files found in $path"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "FAIL: Timed out waiting for $label Parquet files in $path" >&2
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" logs --tail=200 spark-app >&2 || true
  return 1
}

wait_for_rows() {
  local label="$1"
  local path="$2"
  local timeout_seconds="${3:-90}"
  local elapsed=0

  while (( elapsed < timeout_seconds )); do
    local rows
    rows=$(/opt/anaconda3/bin/python - "$path" <<'PY' 2>/dev/null || echo 0
from pathlib import Path
import sys
import pandas as pd

path = Path(sys.argv[1])
if not path.exists() or not list(path.glob("*.parquet")):
    print(0)
else:
    print(len(pd.read_parquet(path)))
PY
)
    if [[ "$rows" =~ ^[0-9]+$ ]] && (( rows > 0 )); then
      echo "PASS: $label has $rows rows"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "FAIL: Timed out waiting for rows in $path" >&2
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" logs --tail=200 spark-app >&2 || true
  return 1
}

echo "Starting Kafka and Spark infrastructure..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d zookeeper kafka kafka-setup spark-master spark-worker

echo "Creating test topic: $TEST_TOPIC"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic "$TEST_TOPIC" \
  --partitions 1 \
  --replication-factor 1

rm -rf "$TABLE_DIR" "$CHECKPOINT_DIR"
mkdir -p "$TABLE_DIR"

echo "Building and starting Spark streaming app..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" build spark-app
KAFKA_TOPIC="$TEST_TOPIC" \
SPARK_PARQUET_OUTPUT_PATH="$CONTAINER_TABLE_DIR" \
SPARK_CHECKPOINT_PATH="$CONTAINER_CHECKPOINT_DIR" \
SPARK_WAREHOUSE_DIR="/opt/spark-app/storage" \
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" --profile spark-app up -d \
  spark-app

echo "Waiting for Spark query startup..."
sleep 25

echo "Publishing deterministic test records..."
python3 - <<'PY' | docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic "$TEST_TOPIC"
from datetime import datetime, timedelta, timezone
import json

now = datetime.now(timezone.utc).replace(microsecond=0)
old = now - timedelta(minutes=5)
events = [
    {"symbol": "BTC", "price": 100.0, "timestamp": old.isoformat().replace("+00:00", "Z")},
    {"symbol": "BTC", "price": 120.0, "timestamp": (old + timedelta(seconds=10)).isoformat().replace("+00:00", "Z")},
    {"symbol": "ETH", "price": 50.0, "timestamp": (old + timedelta(seconds=20)).isoformat().replace("+00:00", "Z")},
    {"symbol": "BTC", "price": 150.0, "timestamp": now.isoformat().replace("+00:00", "Z")},
]
for event in events:
    print(json.dumps(event), flush=True)
PY

wait_for_file "crypto_analytics.crypto_info" "$TABLE_DIR" 90
wait_for_rows "crypto_analytics.crypto_info" "$TABLE_DIR" 90

if find "$TABLE_DIR" -mindepth 1 -type d | grep -q .; then
  echo "FAIL: Expected Parquet files directly in $TABLE_DIR, but found nested folders." >&2
  find "$TABLE_DIR" -mindepth 1 -type d >&2
  exit 1
fi

if find "$TABLE_DIR" -mindepth 1 -maxdepth 1 -type f ! -name '*.parquet' | grep -q .; then
  echo "FAIL: Expected only .parquet files in $TABLE_DIR." >&2
  find "$TABLE_DIR" -mindepth 1 -maxdepth 1 -type f ! -name '*.parquet' >&2
  exit 1
fi

echo "Spark streaming test completed successfully."
echo "Test topic: $TEST_TOPIC"
echo "Hive table: crypto_analytics.crypto_info"
echo "Table files: $TABLE_DIR"
