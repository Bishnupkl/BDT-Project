# Run Instructions

Run everything from the project root:
```bash
cd /var/www/html/miu/BGT-523/BDT-FinalProject
```

## Demo Workflow

This is the fastest way to start the project and show it working during a demo:

```bash
# 1. Start the infrastructure
bash scripts/start-cluster.sh

# 2. Run the producer to fetch 10 live API messages
PRODUCER_MAX_EVENTS=10 bash scripts/run-producer.sh

# 3. Verify Spark can communicate with Kafka
bash scripts/check-spark-kafka.sh
```

Then open these in your browser during the demo:
- **Kafka UI**: http://localhost:8085 (Show live Kafka messages)
- **Spark UI**: http://localhost:8080 (Show Spark infrastructure)

Show during demo:
- live Kafka messages
- Spark infrastructure
- running Docker containers
- real-time CoinGecko API ingestion

---

## Detailed Step-by-Step Instructions

### 1. Start Kafka, Spark, and Kafka UI
```bash
bash scripts/start-cluster.sh
```
This starts: Zookeeper, Kafka, Kafka topic setup, Kafka UI, Spark master, and Spark worker.

### 2. Check containers
```bash
docker ps
```
You should see: `crypto-zookeeper`, `crypto-kafka`, `kafka-ui`, `spark-master`, and `spark-worker`.
*(Note: `crypto-kafka-setup` may already be exited. That is normal because it only creates the Kafka topic.)*

### 3. Verify Spark can reach Kafka
```bash
bash scripts/check-spark-kafka.sh
```
Expected output looks like:
```text
172.xx.xx.xx kafka
```
That means Spark can resolve Kafka inside Docker.

### 4. Run Live API Producer
The producer continuously fetches **live cryptocurrency market prices** from the CoinGecko API and publishes them into Kafka in real-time.

Send 10 live market messages:
```bash
PRODUCER_MAX_EVENTS=10 bash scripts/run-producer.sh
```

Run live API producer continuously:
```bash
bash scripts/run-producer.sh
```
Stop continuous producer with: `Ctrl + C`

### 5. Check Live Messages in Kafka UI
Open http://localhost:8085, then go to:
**local -> Topics -> crypto-topic -> Messages**

You should see JSON messages with current market prices like:
```json
{"symbol":"BTC","price":64123.85,"timestamp":"2026-05-08T21:10:27Z"}
```

### 6. Run Spark Streaming Analytics
The Spark app connects to Kafka from inside Docker using `kafka:29092`, subscribes to `crypto-topic`, parses each JSON message, joins static crypto metadata, and computes 1-minute streaming analytics:

- average price
- maximum price
- minimum price
- event count

In one terminal, keep the producer running:
```bash
bash scripts/run-producer.sh
```

In another terminal, run Spark streaming:
```bash
bash scripts/run-spark-app.sh
```

Expected console output:
```text
+-------------------+-------------------+------+---------+--------------+-------------+---------+---------+-----------+
|window_start       |window_end         |symbol|coin_name|category      |average_price|max_price|min_price|event_count|
+-------------------+-------------------+------+---------+--------------+-------------+---------+---------+-----------+
|2026-05-08 21:10:00|2026-05-08 21:11:00|BTC   |Bitcoin  |Store of Value|64123.85     |64123.85 |64123.85 |1          |
+-------------------+-------------------+------+---------+--------------+-------------+---------+---------+-----------+
```

### 7. Store Crypto Info in One Parquet Folder
The same Spark job continuously writes enriched crypto price events into the Hive database folder:

```text
data/crypto_analytics.db
```

This folder should contain the generated `.parquet` files directly, without intermediate folders such as `test-bb-*`, `prices`, `analytics_1m`, or `symbol=BTC`.

Each row contains:

| Column | Description |
|---|---|
| symbol | Crypto ticker |
| coin_name | Name from static metadata |
| category | Category from static metadata |
| price | Parsed price from Kafka JSON |
| timestamp | Parsed event timestamp |

### 8. View Streaming Analytics in Console
The Spark job computes 1-minute analytics and prints them to the console while the raw enriched stream is written to `data/crypto_analytics.db`.

Console analytics include:

| Column | Description |
|---|---|
| window_start | Start of the 1-minute window |
| window_end | End of the 1-minute window |
| symbol | Crypto ticker |
| coin_name | Name from static metadata |
| category | Category from static metadata |
| average_price | Average price in the window |
| max_price | Highest price in the window |
| min_price | Lowest price in the window |
| event_count | Number of events in the window |

### 9. Register Hive-Compatible Table
The Spark app enables Hive support and registers one external Parquet-backed table from the `data/crypto_analytics.db` folder:

```text
crypto_analytics.crypto_info
```

The Hive table points to the same folder where Spark writes the `.parquet` files, so reading `crypto_analytics.crypto_info` reads all files in `data/crypto_analytics.db`.

Stop continuous streaming with: `Ctrl + C`

### 10. Join Streaming Data with Static CSV Metadata
Static metadata lives in:

```text
spark-app/metadata/crypto_metadata.csv
```

Example:

| symbol | coin_name | category |
|---|---|---|
| BTC | Bitcoin | Store of Value |
| ETH | Ethereum | Smart Contracts |

Spark joins the streaming Kafka data with this CSV by `symbol` before writing `data/crypto_analytics.db`.

### 11. Run Dashboard
After Spark has written Parquet files, start the dashboard from the project root:
```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/streamlit_app.py
```

The dashboard reads:
```text
data/crypto_analytics.db
```

### 12. Run Final Spark Streaming Test Script
Run the final Spark streaming test:

```bash
bash scripts/test_spark_streaming_mayssa.sh
```

The test script:

- starts Kafka and Spark services
- creates a temporary Kafka topic
- starts the Spark streaming app against that topic
- publishes deterministic JSON messages
- verifies the `crypto_analytics.crypto_info` Parquet files
- verifies that the output folder has no nested partition folders
- verifies that the output folder contains only `.parquet` files

Expected success line:

```text
Spark streaming test completed successfully.
```

### 13. Stop Everything
```bash
bash scripts/stop-cluster.sh
```

---

## Troubleshooting

### Kafka UI Not Loading
Restart all containers:
```bash
bash scripts/stop-cluster.sh
bash scripts/start-cluster.sh
```

### Spark Cannot Reach Kafka
Verify Docker networking:
```bash
bash scripts/check-spark-kafka.sh
```
Expected output:
```text
172.xx.xx.xx kafka
```

### Producer Stops Unexpectedly
Verify:
- internet connectivity
- Docker containers are running
- CoinGecko API availability
