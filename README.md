# Real-Time Cryptocurrency Analytics Pipeline

This project implements a real-time Big Data analytics pipeline using Apache Kafka, Apache Spark, Streamlit, and Docker. It fetches **live cryptocurrency price data** from the CoinGecko API, processes the stream in real time, stores enriched events in Parquet/Hive-compatible storage, and visualizes live prices in a Streamlit dashboard.

## Current Repository Scope

This repository contains the end-to-end local pipeline:

Student 1 Responsibilities:

- Docker infrastructure (Zookeeper, Kafka, Spark Master, Spark Worker)
- Kafka setup and network configuration
- Java Kafka Producer (fetches live market data)
- Integration and networking setup

Student 2 Responsibilities:

- Spark Structured Streaming analytics
- Spark consumer logic for reading Kafka crypto price events
- Hive-compatible Parquet writing logic
- Parquet/Hive-compatible storage
- Streamlit dashboarding logic and visualization

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

Run all commands from the project root:

```bash
cd BDT-Project
```

Start the infrastructure:

```bash
bash scripts/start-cluster.sh
```

Start the live CoinGecko producer in a separate terminal:

```bash
bash scripts/run-producer.sh
```

Start Spark streaming in another terminal. If you want a clean run, clear the old Hive/Parquet output and checkpoint first:

```bash
bash scripts/check-hive.sh
rm -rf data/checkpoints
bash scripts/run-spark-app.sh
```

Start the dashboard in another terminal:

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/streamlit_app.py
```

Open the dashboard URL printed by Streamlit, usually http://localhost:8501.

## Kafka Connectivity

- **Inside Docker containers:** Use `kafka:29092`. (Docker containers communicate internally using the Docker network hostname).
- **Outside Docker (Host machine):** Use `localhost:9092`.

## Service URLs

Once the cluster is running, access these tools in your browser:

| Service      | URL                  |
|--------------|----------------------|
| Spark UI     | http://localhost:8080|
| Kafka UI     | http://localhost:8085|
| Dashboard    | http://localhost:8501|

## Scripts Overview

The `scripts/` directory contains helpers for managing the project:

- `start-cluster.sh`: Starts all Docker services.
- `stop-cluster.sh`: Stops and removes all Docker services.
- `run-producer.sh`: Builds and runs the Kafka producer.
- `run-spark-app.sh`: Builds and runs the Spark Structured Streaming app.
- `check-hive.sh`: Clears and verifies the Hive-compatible Parquet table location.
- `check-hive-crypto-analytics.sh`: Inspects the Hive-compatible crypto analytics table.
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