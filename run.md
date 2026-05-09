# Run Instructions

Run everything from the project root:
```bash
# Example: cd /path/to/BDT-FinalProject
cd <your-project-root-directory>
```

## 1. Start Kafka, Spark, and Kafka UI
```bash
bash scripts/start-cluster.sh
```
This starts:
- Zookeeper
- Kafka
- Kafka topic setup
- Kafka UI
- Spark master
- Spark worker

Open these in browser:
- **Spark UI**: http://localhost:8080
- **Kafka UI**: http://localhost:8085
- **Kafka broker**: localhost:9092

## 2. Check containers
```bash
docker ps
```
You should see:
- `crypto-zookeeper`
- `crypto-kafka`
- `kafka-ui`
- `spark-master`
- `spark-worker`

*Note: `crypto-kafka-setup` may already be exited. That is normal because it only creates the Kafka topic.*

## 3. Verify Spark can reach Kafka
```bash
bash scripts/check-spark-kafka.sh
```
Expected output looks like:
```text
172.xx.xx.xx kafka
```
That means Spark can resolve Kafka inside Docker.

## 4. Run Live API Producer
The producer now fetches **live cryptocurrency prices** from the CoinGecko API.

Send 10 live market messages:
```bash
PRODUCER_MAX_EVENTS=10 bash scripts/run-producer.sh
```
Run live API producer continuously:
```bash
bash scripts/run-producer.sh
```
Stop continuous producer with:
`Ctrl + C`

## 5. Check Live Messages in Kafka UI
Open:
http://localhost:8085

Then go to:
**local -> Topics -> crypto-topic -> Messages**

You should see JSON messages with current market prices like:
```json
{"symbol":"BTC","price":64123.85,"timestamp":"2026-05-08T21:10:27Z"}
```

## 6. Run Integration Test
```bash
bash scripts/integration-test-bishnu.sh
```
Expected success line:
```text
Processed a total of 5 messages
```

## 7. Stop everything
```bash
bash scripts/stop-cluster.sh
```

---

### Normal workflow for demo:
```bash
bash scripts/start-cluster.sh
PRODUCER_MAX_EVENTS=10 bash scripts/run-producer.sh
bash scripts/check-spark-kafka.sh
```
Then show Kafka UI and Spark UI in the browser.