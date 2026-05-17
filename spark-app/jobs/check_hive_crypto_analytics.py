#!/usr/bin/env python3
"""Inspect the Hive-compatible crypto analytics table written by Spark."""

from __future__ import annotations

import argparse
from typing import Iterable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_DATABASE = "crypto_analytics"
DEFAULT_TABLE = "crypto_info"
DEFAULT_TABLE_PATH = "/opt/spark-app/storage/crypto_analytics.db"
DEFAULT_WAREHOUSE_DIR = "/opt/spark-app/storage"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the contents of the Hive crypto_analytics database."
    )
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--table-path", default=DEFAULT_TABLE_PATH)
    parser.add_argument("--warehouse-dir", default=DEFAULT_WAREHOUSE_DIR)
    parser.add_argument("--sample-size", type=int, default=20)
    return parser.parse_args()


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def full_table_name(database: str, table: str) -> str:
    return f"{quote_identifier(database)}.{quote_identifier(table)}"


def count_parquet_files(spark: SparkSession, table_path: str) -> int:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(table_path)
    fs = path.getFileSystem(hadoop_conf)
    if not fs.exists(path):
        return 0

    statuses: Iterable = fs.listStatus(path)
    return sum(
        status.isFile() and status.getPath().getName().endswith(".parquet")
        for status in statuses
    )


def register_external_table(
    spark: SparkSession, database: str, table: str, table_path: str
) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database)}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name(database, table)} (
            symbol STRING,
            coin_name STRING,
            category STRING,
            price DOUBLE,
            timestamp TIMESTAMP
        )
        USING PARQUET
        LOCATION '{table_path}'
        """
    )


def show_section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("Check Hive Crypto Analytics")
        .config("spark.sql.warehouse.dir", args.warehouse_dir)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    table_name = full_table_name(args.database, args.table)

    show_section("Hive Database")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(args.database)}")
    spark.sql(f"SHOW TABLES IN {quote_identifier(args.database)}").show(
        truncate=False
    )

    show_section("Storage Path")
    print(args.table_path)

    parquet_file_count = count_parquet_files(spark, args.table_path)
    if parquet_file_count == 0:
        print("No Parquet files found. Run the Spark streaming app before checking data.")
        spark.stop()
        return

    register_external_table(spark, args.database, args.table, args.table_path)

    show_section("Registered Tables")
    spark.sql(f"SHOW TABLES IN {quote_identifier(args.database)}").show(
        truncate=False
    )

    df = spark.table(table_name)

    show_section("Schema")
    df.printSchema()

    show_section("Row Count")
    row_count = df.count()
    print(row_count)

    show_section("Timestamp Range")
    df.select(
        F.min("timestamp").alias("first_event_timestamp"),
        F.max("timestamp").alias("latest_event_timestamp"),
    ).show(truncate=False)

    show_section("Rows By Symbol")
    df.groupBy("symbol", "coin_name", "category").count().orderBy(
        F.desc("count"), "symbol"
    ).show(truncate=False)

    show_section("Recent Sample")
    df.orderBy(F.desc("timestamp")).show(args.sample_size, truncate=False)

    show_section("Parquet Files")
    print(parquet_file_count)

    spark.stop()


if __name__ == "__main__":
    main()
