# Real-Time Cryptocurrency Analytics Pipeline

This project implements a real-time Big Data analytics pipeline using Apache Kafka, Apache Spark, and Docker. It simulates a stream of cryptocurrency price data, processes it, and prepares it for further analysis and visualization.

This repository contains the core infrastructure, including the data producer and the services required to run the streaming pipeline.

## Getting Started

Follow these instructions to get the project running on your local machine.

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
    This project uses an `.env` file for configuration. A sample file is provided to get you started. Simply copy it and make any desired adjustments.

    ```bash
    cp .env.example .env
    ```

### Running the Pipeline

For detailed, step-by-step instructions on how to run, test, and troubleshoot the pipeline, please refer to the **[How to Run Guide](./run.md)**.

For a quick start, use the following command to launch the entire cluster (Kafka, Spark, etc.):

```bash
bash scripts/start-cluster.sh
```

To begin streaming data, run the producer:
```bash
bash scripts/run-producer.sh
```

## Service URLs

Once the cluster is running, you can access the following services:

| Service      | URL                  |
|--------------|----------------------|
| Spark UI     | http://localhost:8080|
| Kafka UI     | http://localhost:8085|
| Kafka Broker | `localhost:9092`     |

## Scripts Overview

The `scripts/` directory contains helpers for managing the project:

- `start-cluster.sh`: Starts all Docker services (Kafka, Spark, etc.).
- `stop-cluster.sh`: Stops and removes all Docker services.
- `run-producer.sh`: Builds and runs the Kafka producer in a Docker container.
- `check-spark-kafka.sh`: Verifies network connectivity between Spark and Kafka containers.
- `integration-test-bishnu.sh`: Runs an automated end-to-end test for the producer-to-Kafka pipeline.