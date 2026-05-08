# Real-Time Cryptocurrency Analytics Pipeline

This repository contains the Big Data final project scaffold for a real-time cryptocurrency analytics pipeline.

## Student 1 Scope

Implemented in this version:

- Docker Compose services for Zookeeper and Kafka
- Automatic creation of the `crypto-topic` Kafka topic
- Java Kafka producer that emits simulated cryptocurrency JSON events
- Producer Dockerfile
- Student 1 integration-test script

## Run Kafka

```bash
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-setup
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
docker compose -f docker/docker-compose.yml --profile producer up --build crypto-producer
```

## Integration Test

```bash
bash scripts/integration-test-student1.sh
```

Expected Kafka message format:

```json
{"symbol":"BTC","price":65231.45,"timestamp":"2026-05-07T12:00:00Z"}
```
