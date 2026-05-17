#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${CHECK_HIVE_CONTAINER:-}"
DATABASE="${CHECK_HIVE_DATABASE:-crypto_analytics}"
TABLE="${CHECK_HIVE_TABLE:-crypto_info}"
WAREHOUSE_DIR="${SPARK_WAREHOUSE_DIR:-/opt/spark-app/storage}"
TABLE_PATH="${SPARK_PARQUET_OUTPUT_PATH:-/opt/spark-app/storage/crypto_analytics.db}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required to run this verification script." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: Docker is not reachable from this shell.

Make sure Docker is running and your user has permission to access it.
EOF
  exit 1
fi

if [[ -z "$CONTAINER" ]]; then
  for candidate in crypto-spark-app spark-worker; do
    if docker ps --format '{{.Names}}' | grep -qx "$candidate"; then
      CONTAINER="$candidate"
      break
    fi
  done
fi

if [[ -z "$CONTAINER" ]]; then
  cat >&2 <<'EOF'
ERROR: No Spark container is running.

Start the cluster first:
  scripts/start-cluster.sh

Then run this script before and after streaming to show:
  empty Hive table -> streaming starts -> Hive table populated
EOF
  exit 1
fi

if ! docker exec "$CONTAINER" test -x /opt/spark/bin/spark-submit; then
  echo "ERROR: /opt/spark/bin/spark-submit was not found in container '$CONTAINER'." >&2
  exit 1
fi

echo "Hive verification container: $CONTAINER"
echo "Table location: $TABLE_PATH"
echo

docker exec "$CONTAINER" mkdir -p "$TABLE_PATH"

docker exec \
  -i \
  -e CHECK_HIVE_DATABASE="$DATABASE" \
  -e CHECK_HIVE_TABLE="$TABLE" \
  -e SPARK_WAREHOUSE_DIR="$WAREHOUSE_DIR" \
  -e SPARK_PARQUET_OUTPUT_PATH="$TABLE_PATH" \
  "$CONTAINER" \
  /bin/sh -c 'cat > /tmp/check_hive.py && /opt/spark/bin/spark-submit --master local[*] --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=1 --conf "spark.sql.warehouse.dir=$SPARK_WAREHOUSE_DIR" /tmp/check_hive.py 2>/tmp/check_hive.err; status=$?; if [ "$status" -ne 0 ]; then cat /tmp/check_hive.err >&2; fi; rm -f /tmp/check_hive.py /tmp/check_hive.err; exit "$status"' <<'PYSPARK'
import os
import re

from pyspark.sql import SparkSession


def checked_identifier(value, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} must be a simple SQL identifier, got: {value!r}")
    return value


database = checked_identifier(os.environ.get("CHECK_HIVE_DATABASE", "crypto_analytics"), "database")
table = checked_identifier(os.environ.get("CHECK_HIVE_TABLE", "crypto_info"), "table")
warehouse_dir = os.environ.get("SPARK_WAREHOUSE_DIR", "/opt/spark-app/storage")
table_path = os.environ.get("SPARK_PARQUET_OUTPUT_PATH", "/opt/spark-app/storage/crypto_analytics.db")
table_path_sql = table_path.replace("'", "\\'")
full_table_name = f"{database}.{table}"

spark = (
    SparkSession.builder.appName("Hive Verification")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.warehouse.dir", warehouse_dir)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
          symbol STRING,
          coin_name STRING,
          category STRING,
          price DOUBLE,
          timestamp TIMESTAMP
        )
        USING PARQUET
        LOCATION '{table_path_sql}'
        """
    )

    print("\n== SHOW DATABASES ==")
    spark.sql("SHOW DATABASES").show(truncate=False)

    print(f"\n== SHOW TABLES IN {database} ==")
    spark.sql(f"SHOW TABLES IN {database}").show(truncate=False)

    print(f"\n== SELECT COUNT(*) FROM {full_table_name} ==")
    spark.sql(f"SELECT COUNT(*) AS record_count FROM {full_table_name}").show(truncate=False)

    print(f"\n== SELECT * FROM {full_table_name} LIMIT 10 ==")
    spark.sql(f"SELECT * FROM {full_table_name} LIMIT 10").show(10, truncate=False)
finally:
    spark.stop()
PYSPARK

