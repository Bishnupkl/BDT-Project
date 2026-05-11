package edu.miu.bdt.spark;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.List;
import java.util.concurrent.TimeoutException;

public class SparkStreamingApp {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String DEFAULT_TOPIC = "crypto-topic";
    private static final String DEFAULT_METADATA_PATH = "/opt/spark-app/metadata/crypto_metadata.csv";
    private static final String DEFAULT_OUTPUT_PATH = "/opt/spark-app/storage/crypto_analytics.db";
    private static final String DEFAULT_CHECKPOINT_PATH = "/opt/spark-app/storage/checkpoints";
    private static final String DEFAULT_WAREHOUSE_PATH = "/opt/spark-app/storage";

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        String bootstrapServers = readConfig("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = readConfig("KAFKA_TOPIC", DEFAULT_TOPIC);
        String metadataPath = readConfig("CRYPTO_METADATA_PATH", DEFAULT_METADATA_PATH);
        String outputPath = readConfig("SPARK_PARQUET_OUTPUT_PATH", DEFAULT_OUTPUT_PATH);
        String checkpointPath = readConfig("SPARK_CHECKPOINT_PATH", DEFAULT_CHECKPOINT_PATH);
        String warehousePath = readConfig("SPARK_WAREHOUSE_DIR", DEFAULT_WAREHOUSE_PATH);
        SparkSession spark = SparkSession.builder()
                .appName("Crypto Spark Streaming")
                .config("spark.sql.warehouse.dir", warehousePath)
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                .enableHiveSupport()
                .getOrCreate();

        System.out.printf(
                "Starting Spark stream from Kafka topic '%s' at '%s'. Parquet output: %s%n",
                topic,
                bootstrapServers,
                outputPath
        );

        StreamingProcessor processor = new StreamingProcessor(spark, bootstrapServers, topic);
        processor.registerHiveTables(outputPath);
        Dataset<Row> parsedPrices = processor.parseCryptoMessages(processor.readKafkaJsonStream());
        Dataset<Row> enrichedPrices = processor.enrichWithMetadata(parsedPrices, metadataPath);
        Dataset<Row> analytics = processor.aggregateOneMinutePrices(enrichedPrices);
        VoidFunction2<Dataset<Row>, Long> writeCryptoInfoBatch = (batch, batchId) -> {
            Dataset<Row> persistedBatch = batch.persist();
            if (!persistedBatch.isEmpty()) {
                persistedBatch
                        .coalesce(1)
                        .write()
                        .mode(SaveMode.Append)
                        .parquet(outputPath);
            }
            persistedBatch.unpersist();
        };
        StreamingQuery cryptoInfoStorageQuery = enrichedPrices
                .writeStream()
                .queryName("crypto_info_parquet_storage")
                .outputMode("append")
                .option("checkpointLocation", checkpointPath + "/crypto_info")
                .foreachBatch(writeCryptoInfoBatch)
                .start();
        StreamingQuery analyticsConsoleQuery = analytics
                .writeStream()
                .queryName("crypto_analytics_console")
                .format("console")
                .outputMode("update")
                .option("truncate", "false")
                .option("numRows", "20")
                .start();

        for (StreamingQuery query : List.of(cryptoInfoStorageQuery, analyticsConsoleQuery)) {
            System.out.printf("Started streaming query: %s%n", query.name());
        }
        spark.streams().awaitAnyTermination();
    }

    private static String readConfig(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
