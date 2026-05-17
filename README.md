# Real-Time Cryptocurrency Analytics Pipeline

This project implements a real-time Big Data analytics pipeline using **Apache Kafka, Apache Spark Structured Streaming, Hive, Streamlit, and Docker**. It fetches **live cryptocurrency market data** from the CoinGecko API, processes the stream in real-time, stores analytics in Hive/Parquet, and visualizes insights through a live dashboard.

The system demonstrates a complete **end-to-end streaming architecture** for Big Data analytics.

---

# Project Features

## Real-Time Streaming
- Live cryptocurrency price ingestion from the **CoinGecko API**
- Real-time JSON event streaming using **Apache Kafka**
- Continuous streaming data pipeline

## Distributed Processing
- **Apache Spark Structured Streaming**
- Real-time Kafka consumer
- JSON parsing and schema transformation
- 1-minute window analytics:
    - average price
    - maximum price
    - minimum price
    - event count

## Hive Storage
- Processed streaming data stored using **Parquet**
- Registered as a **Hive-compatible table**
- Queryable analytical storage layer

Hive table:

```text
crypto_analytics.crypto_info
```

## Metadata Enrichment (Bonus)
Streaming cryptocurrency data is enriched using a static CSV dataset.

Example metadata:
- coin name
- cryptocurrency category

Example:
```text
BTC -> Bitcoin -> Store of Value
ETH -> Ethereum -> Smart Contracts
```

## Live Dashboard
Interactive **Streamlit dashboard** with:
- Live cryptocurrency prices
- 1-minute aggregations
- Metadata enrichment
- Real-time trend visualization
- Multi-coin analytics charts

---

# Architecture

```text
CoinGecko API (Live Market Data)
              ↓
Java Kafka Producer
              ↓
Apache Kafka (crypto-topic)
              ↓
Spark Structured Streaming
              ↓
Hive Table + Parquet Storage
              ↓
Streamlit Dashboard
```

This architecture enables **real-time ingestion, distributed processing, persistent storage, and analytics visualization**.

---

# Technologies Used

| Technology | Purpose |
|------------|---------|
| Java | Kafka Producer |
| Apache Kafka | Streaming Data Ingestion |
| Apache Spark Structured Streaming | Distributed Processing |
| Hive | Analytical Storage |
| Parquet | Persistent Storage Format |
| Streamlit | Dashboard Visualization |
| Docker | Containerized Infrastructure |
| CoinGecko API | Live Crypto Market Data |

---

# Getting Started

## Prerequisites

Install the following:

- Docker
- Docker Compose
- Python 3 (for dashboard)
- Java 17+
- Maven

---

# Setup

Clone the repository:

```bash
git clone <repository-url>
cd BDT-FinalProject
```

Configure environment variables:

```bash
cp .env.example .env
```

---

# Quick Start

Start the infrastructure:

```bash
bash scripts/start-cluster.sh
```

Run Spark Streaming:

```bash
bash scripts/run-spark-app.sh
```

Run the live producer:

```bash
bash scripts/run-producer.sh
```

Start the dashboard in another terminal:

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/streamlit_app.py
```

Open the dashboard URL printed by Streamlit, usually http://localhost:8501.

---

# Service URLs

Once running, access:

| Service | URL |
|----------|-----|
| Spark UI | http://localhost:8080 |
| Kafka UI | http://localhost:8085 |
| Dashboard | http://localhost:8501 |

---

# Kafka Connectivity

### Inside Docker Containers
Use:

```text
kafka:29092
```

### Outside Docker (Host Machine)
Use:

```text
localhost:9092
```

---

# Hive Storage

Processed streaming data is continuously written to:

```text
crypto_analytics.crypto_info
```

Storage format:

```text
Parquet
```

Example Hive query:

```sql
SELECT * FROM crypto_analytics.crypto_info LIMIT 10;
```

# Hive Verification Before and After Streaming

Use the helper script to verify Hive without manually entering the Spark container. The current Spark Docker image does not provide `spark-sql`, so this script uses Spark inside the running container to execute the required checks.

The script displays:

- `SHOW DATABASES`
- `SHOW TABLES IN crypto_analytics`
- `SELECT COUNT(*) FROM crypto_analytics.crypto_info`
- `SELECT * FROM crypto_analytics.crypto_info LIMIT 10`

## 1. Reset Old Demo Data

If you see records before starting Spark streaming, they are old Parquet files from a previous run. Stop the cluster and clear the local Hive/Parquet demo output first:

```bash
bash scripts/stop-cluster.sh
bash scripts/reset-demo-data.sh
```

The reset script also removes any running `crypto-spark-app` or `crypto-producer` container before deleting checkpoint files, because Spark can recreate checkpoint files while the reset is running.

When prompted, type:

```text
RESET
```

This clears:

- `data/crypto_analytics.db`
- `data/checkpoints`
- `spark-app/storage/crypto_analytics.db`

If you get `Permission denied`, it usually means the checkpoint files were created by the Spark Docker container. Run the updated reset script again from a shell that can access Docker; it will automatically use a temporary Spark container to remove container-owned files. If Docker is not reachable, fix ownership manually:

```bash
sudo chown -R "$(id -u):$(id -g)" data spark-app/storage
bash scripts/reset-demo-data.sh
```

## 2. Start the Infrastructure

```bash
bash scripts/start-cluster.sh
```

## 3. Check Hive Before Streaming

Run this before starting Spark streaming and the producer:

```bash
bash scripts/check-hive.sh
```

For a clean demo, the count should be `0` before streaming starts:

```text
== SELECT COUNT(*) FROM crypto_analytics.crypto_info ==
+------------+
|record_count|
+------------+
|0           |
+------------+
```

If old Parquet files already exist from a previous run, the count may be greater than `0`. For the professor demo, use a clean data directory when you need to show the required workflow:

```text
empty Hive table -> streaming starts -> Hive table populated
```

## 4. Start Spark Streaming

Open a second terminal and run:

```bash
bash scripts/run-spark-app.sh
```

## 5. Start the Producer

Open a third terminal and run:

```bash
bash scripts/run-producer.sh
```

## 6. Check Hive After Streaming

After the producer sends events and Spark processes them, run the Hive check again:

```bash
bash scripts/check-hive.sh
```

Expected result after streaming:

```text
== SHOW DATABASES ==
crypto_analytics

== SHOW TABLES IN crypto_analytics ==
crypto_info

== SELECT COUNT(*) FROM crypto_analytics.crypto_info ==
record_count > 0

== SELECT * FROM crypto_analytics.crypto_info LIMIT 10 ==
recent cryptocurrency records
```

Example rows:

```text
+------+----------+------------------+--------+-------------------+
|symbol|coin_name |category          |price   |timestamp          |
+------+----------+------------------+--------+-------------------+
|BTC   |Bitcoin   |Store of Value    |81064.2 |2026-05-11 03:20:12|
|ETH   |Ethereum  |Smart Contracts   |2299.7  |2026-05-11 03:20:15|
+------+----------+------------------+--------+-------------------+
```

This verifies the final demo workflow: the Hive table is checked before streaming, Spark streaming starts, the producer sends Kafka events, and the Hive table becomes populated.

---

# Dashboard Features

The dashboard displays:

- Live cryptocurrency prices
- Metadata enrichment
- 1-minute streaming analytics
- Average price trends
- Multi-coin visualizations

Supported coins:
- BTC
- ETH
- SOL
- ADA
- XRP

---

# Scripts Overview

The `scripts/` directory contains helper scripts for managing the project.

| Script | Description |
|--------|-------------|
| `start-cluster.sh` | Starts Kafka, Spark, and infrastructure |
| `stop-cluster.sh` | Stops all Docker services |
| `run-producer.sh` | Starts the Java Kafka producer |
| `run-spark-app.sh` | Runs Spark Structured Streaming |
| `check-hive.sh` | Verifies Hive databases, tables, row count, and sample records |
| `reset-demo-data.sh` | Clears old local Parquet/checkpoint data for an empty-table demo |
| `check-spark-kafka.sh` | Verifies Spark <-> Kafka connectivity |
| `integration-test-bishnu.sh` | Kafka producer integration test |
| `test_spark_streaming_mayssa.sh` | Spark streaming validation test |

---

# Project Workflow

The streaming workflow:

```text
1. CoinGecko API provides live crypto prices
2. Java producer fetches market data
3. Kafka receives JSON streaming messages
4. Spark Structured Streaming consumes Kafka
5. Spark enriches data using metadata CSV
6. Window analytics are computed
7. Processed results stored in Hive/Parquet
8. Dashboard visualizes analytics
```

---

# Demo Requirements

The demo video demonstrates:

- Empty Hive table before streaming
- Kafka message ingestion
- Spark streaming jobs
- Hive table population
- Dashboard visualizations
- Complete end-to-end workflow

All team members participate in the presentation.

---

# Screenshots

You can view all project screenshots here:

[screenshots](./screenshots)

---

# Future Improvements

Potential enhancements:

- Binance WebSocket streaming
- anomaly/spike detection
- historical analytics replay
- Grafana integration
- Kubernetes deployment
- advanced forecasting analytics
- ML-based market prediction

---

# Contributors

### Student 1
- Docker infrastructure
- Kafka setup
- Java Kafka producer
- CoinGecko API integration
- Networking and integration testing

### Student 2
- Spark Structured Streaming
- Hive/Parquet storage
- Metadata enrichment
- Dashboard visualization
- Streaming analytics
