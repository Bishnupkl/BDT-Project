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

### 6. Run Integration Test
```bash
bash scripts/integration-test-bishnu.sh
```
Expected success line: `Processed a total of 5 messages`

### 7. Stop everything
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