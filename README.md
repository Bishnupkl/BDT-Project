# Real-Time Cryptocurrency Analytics Pipeline

This repository contains the Big Data final project scaffold for a real-time cryptocurrency analytics pipeline.

## Bishnu Scope

Implemented in this version:

- Docker Compose services for Zookeeper and Kafka
- Docker Compose services for Spark master and Spark worker
- Kafka UI for topic/message inspection
- Shared Docker network for Kafka/Spark communication
- Kafka volume for persisted broker data
- `.env` configuration for producer and topic settings
- Automatic creation of the `crypto-topic` Kafka topic
- Java Kafka producer that emits simulated cryptocurrency JSON events
- Producer Dockerfile
- Bishnu integration-test script
- Helper scripts for starting/stopping the cluster and running connectivity checks

This scope provides the Spark infrastructure and verifies Kafka/Spark networking. The Spark streaming consumer job is still the next project layer for Student 2 to plug into `kafka:29092`.

## Service URLs

| Service | URL |
| --- | --- |
| Spark UI | http://localhost:8080 |
| Kafka UI | http://localhost:8085 |
| Kafka Broker | localhost:9092 |

## Start The Full Cluster

```bash
bash scripts/start-cluster.sh
```

## Run The Producer Locally

```bash
cd producer
mvn test
mvn package
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 PRODUCER_MAX_EVENTS=10 java -jar target/crypto-producer-1.0.0.jar
```

## Run The Producer In Docker

```bash
bash scripts/run-producer.sh
```

To send a limited number of events:

```bash
PRODUCER_MAX_EVENTS=10 bash scripts/run-producer.sh
```

## Integration Test

```bash
bash scripts/integration-test-bishnu.sh
```

## Verify Spark Can Resolve Kafka

```bash
bash scripts/check-spark-kafka.sh
```

Expected output includes an IP address for `kafka`.

## Stop The Cluster

```bash
bash scripts/stop-cluster.sh
```

Expected Kafka message format:

```json
{"symbol":"BTC","price":65231.45,"timestamp":"2026-05-07T12:00:00Z"}
```
