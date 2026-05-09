# Real-Time Cryptocurrency Analytics Pipeline

This project implements a real-time Big Data analytics pipeline using Apache Kafka, Apache Spark, and Docker. It fetches **live cryptocurrency price data** from the CoinGecko API, processes the stream in real-time, and prepares it for further analysis and visualization.

## Current Repository Scope

This repository currently handles the data ingestion and infrastructure phase of the project (Student 1 Responsibilities):

- Docker infrastructure (Zookeeper, Kafka, Spark Master, Spark Worker)
- Kafka setup and network configuration
- Java Kafka Producer (fetches live market data)
- Integration and networking setup

Spark Structured Streaming analytics and dashboard implementation are handled separately as part of Student 2 responsibilities.

## Architecture

```text
CoinGecko API
      ↓
Java Kafka Producer
      ↓
Apache Kafka (crypto-topic)
      ↓
Spark Structured Streaming
      ↓
Parquet Storage
      ↓
Dashboard Visualization
```

The producer continuously fetches live cryptocurrency market prices from the CoinGecko API and publishes them into Kafka in real-time. This provides the high-volume streaming data source required for the pipeline.

## Getting Started

### Prerequisites
- Docker
- Docker Compose

### Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd BDT-FinalProject
    ```

2.  **Configure Environment Variables:**
    Copy the provided sample `.env` file to customize settings.
    ```bash
    cp .env.example .env
    ```

### Running the Pipeline

For detailed, step-by-step instructions on how to run, test, and troubleshoot the pipeline, please refer to the **[How to Run Guide](./run.md)**.

For a quick start, use the following command to launch the entire cluster:
```bash
bash scripts/start-cluster.sh
```

To begin streaming live API data, run the producer:
```bash
bash scripts/run-producer.sh
```

## Kafka Connectivity

- **Inside Docker containers:** Use `kafka:29092`. (Docker containers communicate internally using the Docker network hostname).
- **Outside Docker (Host machine):** Use `localhost:9092`.

## Service URLs

Once the cluster is running, access these tools in your browser:

| Service      | URL                  |
|--------------|----------------------|
| Spark UI     | http://localhost:8080|
| Kafka UI     | http://localhost:8085|

## Scripts Overview

The `scripts/` directory contains helpers for managing the project:

- `start-cluster.sh`: Starts all Docker services.
- `stop-cluster.sh`: Stops and removes all Docker services.
- `run-producer.sh`: Builds and runs the Kafka producer.
- `check-spark-kafka.sh`: Verifies network connectivity between Spark and Kafka.
- `integration-test-bishnu.sh`: Runs an automated end-to-end test.

## Future Improvements

Potential future enhancements for the pipeline:
- Add Binance WebSocket integration
- Add Spark Structured Streaming analytics
- Store processed data in Parquet/Hive
- Build Grafana or Streamlit dashboard
- Add anomaly detection alerts
- Add historical replay support
- Deploy using Kubernetes