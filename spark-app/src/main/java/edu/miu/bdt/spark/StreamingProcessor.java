package edu.miu.bdt.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.window;

public class StreamingProcessor {
    private final SparkSession spark;
    private final String bootstrapServers;
    private final String topic;

    public StreamingProcessor(SparkSession spark, String bootstrapServers, String topic) {
        this.spark = spark;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public Dataset<Row> readKafkaJsonStream() {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) AS json");
    }

    public Dataset<Row> parseCryptoMessages(Dataset<Row> jsonMessages) {
        return jsonMessages
                .select(from_json(col("json"), CryptoSchema.schema()).alias("data"))
                .filter(col("data").isNotNull())
                .select(
                        col("data.symbol").alias("symbol"),
                        col("data.price").alias("price"),
                        col("data.timestamp").alias("timestamp")
                )
                .na()
                .drop(new String[]{"symbol", "price", "timestamp"});
    }

    public Dataset<Row> enrichWithMetadata(Dataset<Row> prices, String metadataPath) {
        Dataset<Row> metadata = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .csv(metadataPath)
                .select(
                        col("symbol"),
                        col("coin_name"),
                        col("category")
                );
        return prices
                .join(metadata, "symbol", "left")
                .select(
                        col("symbol"),
                        col("coin_name"),
                        col("category"),
                        col("price"),
                        col("timestamp")
                );
    }

    public Dataset<Row> aggregateOneMinutePrices(Dataset<Row> prices) {
        return prices
                .withWatermark("timestamp", "2 minutes")
                .groupBy(
                        window(col("timestamp"), "1 minute"),
                        col("symbol"),
                        col("coin_name"),
                        col("category")
                )
                .agg(
                        avg("price").alias("average_price"),
                        max("price").alias("max_price"),
                        min("price").alias("min_price"),
                        count("price").alias("event_count")
                )
                .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("symbol"),
                        col("coin_name"),
                        col("category"),
                        col("average_price"),
                        col("max_price"),
                        col("min_price"),
                        col("event_count")
                );
    }

    public void registerHiveTables(String outputPath) {
        spark.sql("CREATE DATABASE IF NOT EXISTS crypto_analytics");
        spark.sql("DROP TABLE IF EXISTS crypto_analytics.crypto_info");
        spark.sql(String.format(
                "CREATE TABLE crypto_analytics.crypto_info ("
                        + "symbol STRING, "
                        + "coin_name STRING, "
                        + "category STRING, "
                        + "price DOUBLE, "
                        + "timestamp TIMESTAMP"
                        + ") USING PARQUET LOCATION '%s'",
                outputPath
        ));
    }
}
