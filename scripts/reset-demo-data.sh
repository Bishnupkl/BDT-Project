#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DATA_DIR="$ROOT_DIR/data"
HIVE_TABLE_DIR="$DATA_DIR/crypto_analytics.db"
CHECKPOINT_DIR="$DATA_DIR/checkpoints"
LOCAL_SPARK_TABLE_DIR="$ROOT_DIR/spark-app/storage/crypto_analytics.db"

reset_with_docker() {
  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    return 1
  fi

  docker rm -f crypto-spark-app crypto-producer >/dev/null 2>&1 || true
  docker run --rm \
    -v "$ROOT_DIR:/workspace" \
    --entrypoint /bin/sh \
    spark:3.5.8 \
    -c "chmod -R 777 /workspace/data/crypto_analytics.db /workspace/data/checkpoints /workspace/spark-app/storage/crypto_analytics.db 2>/dev/null || true; rm -rf /workspace/data/crypto_analytics.db /workspace/data/checkpoints /workspace/spark-app/storage/crypto_analytics.db && mkdir -p /workspace/data/crypto_analytics.db /workspace/data/checkpoints && chmod -R 777 /workspace/data/crypto_analytics.db /workspace/data/checkpoints"
}

cat <<EOF
This will clear local demo output so Hive starts empty:
  $HIVE_TABLE_DIR
  $CHECKPOINT_DIR
  $LOCAL_SPARK_TABLE_DIR

Kafka/Spark containers should be stopped before running this reset.
EOF

if [[ "${1:-}" != "--force" ]]; then
  printf "Type RESET to continue: "
  read -r confirmation
  if [[ "$confirmation" != "RESET" ]]; then
    echo "Reset cancelled."
    exit 0
  fi
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  echo "Using Docker cleanup so container-owned Spark files can be removed."
  reset_with_docker
else
  echo "Docker is not reachable. Trying normal host cleanup..."
  if ! rm -rf "$HIVE_TABLE_DIR" "$CHECKPOINT_DIR" "$LOCAL_SPARK_TABLE_DIR" 2>/tmp/reset-demo-data.err; then
    echo "ERROR: Cleanup failed because some files are not writable by this user." >&2
    echo "Fix permissions, then run this script again:" >&2
    echo "  sudo chown -R \"\$(id -u):\$(id -g)\" \"$DATA_DIR\" \"$ROOT_DIR/spark-app/storage\"" >&2
    rm -f /tmp/reset-demo-data.err
    exit 1
  fi
  mkdir -p "$HIVE_TABLE_DIR" "$CHECKPOINT_DIR"
  chmod -R 777 "$DATA_DIR" "$ROOT_DIR/spark-app/storage"
fi

echo "Demo data reset complete. You can now run:"
echo "  bash scripts/start-cluster.sh"
echo "  bash scripts/check-hive.sh"
