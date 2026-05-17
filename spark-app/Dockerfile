FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM spark:3.5.8
WORKDIR /opt/spark-app
COPY --from=build /app/target/crypto-spark-app-1.0.0.jar /opt/spark-app/crypto-spark-app.jar
COPY metadata /opt/spark-app/metadata
COPY jobs /opt/spark-app/jobs

ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=crypto-topic
ENV CRYPTO_METADATA_PATH=/opt/spark-app/metadata/crypto_metadata.csv
ENV SPARK_PARQUET_OUTPUT_PATH=/opt/spark-app/storage/crypto_analytics.db
ENV SPARK_CHECKPOINT_PATH=/opt/spark-app/storage/checkpoints
ENV SPARK_WAREHOUSE_DIR=/opt/spark-app/storage

ENTRYPOINT ["/bin/sh", "-c", "exec /opt/spark/bin/spark-submit --class edu.miu.bdt.spark.SparkStreamingApp --master \"$SPARK_MASTER_URL\" /opt/spark-app/crypto-spark-app.jar"]
