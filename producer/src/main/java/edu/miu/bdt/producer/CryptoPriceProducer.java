package edu.miu.bdt.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class CryptoPriceProducer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "crypto-topic";
    private static final long DEFAULT_INTERVAL_MS = 10_000L;
    private static final long DEFAULT_MAX_EVENTS = 0L;
    private final KafkaProducer<String, String> producer;
    private final CryptoApiClient apiClient;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final long intervalMs;
    private final long maxEvents;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public CryptoPriceProducer(KafkaProducer<String, String> producer,
                               CryptoApiClient apiClient,
                               ObjectMapper objectMapper,
                               String topic,
                               long intervalMs,
                               long maxEvents) {
        this.producer = producer;
        this.apiClient = apiClient;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.intervalMs = intervalMs;
        this.maxEvents = maxEvents;
    }

    public static void main(String[] args) {
        ProducerSettings settings = ProducerSettings.fromEnvironment();
        ObjectMapper objectMapper = CryptoJsonMapper.create();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties(settings.bootstrapServers()));
        CryptoApiClient apiClient = new CryptoApiClient(
                HttpClient.newHttpClient(),
                objectMapper,
                settings.cryptoApiUri(),
                CryptoApiClient.DEFAULT_ASSETS,
                Clock.systemUTC()
        );
        CryptoPriceProducer app = new CryptoPriceProducer(
                producer,
                apiClient,
                objectMapper,
                settings.topic(),
                settings.intervalMs(),
                settings.maxEvents()
        );
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
        app.run();
    }

    public void run() {
        long sentEvents = 0L;
        try {
            while (running.get() && (maxEvents <= 0 || sentEvents < maxEvents)) {
                List<CryptoPriceEvent> events = fetchEvents();
                for (CryptoPriceEvent event : events) {
                    if (!running.get() || (maxEvents > 0 && sentEvents >= maxEvents)) {
                        break;
                    }
                    sendEvent(event);
                    sentEvents++;
                }
                producer.flush();
                sleepBetweenEvents();
            }
        } finally {
            producer.close();
        }
    }

    public void stop() {
        running.set(false);
    }

    private List<CryptoPriceEvent> fetchEvents() {
        try {
            return apiClient.fetchLatestPrices();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
            return List.of();
        } catch (IOException e) {
            System.err.printf("Failed to fetch crypto prices from API: %s%n", e.getMessage());
            return List.of();
        }
    }

    private void sendEvent(CryptoPriceEvent event) {
        String payload = toJson(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.symbol(), payload);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.printf("Failed to send crypto event for %s: %s%n", event.symbol(), exception.getMessage());
                return;
            }
            System.out.printf("Sent %s to %s-%d@%d%n", payload, metadata.topic(), metadata.partition(), metadata.offset());
        });
    }

    private String toJson(CryptoPriceEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize crypto price event", e);
        }
    }

    private void sleepBetweenEvents() {
        if (intervalMs <= 0) {
            return;
        }
        try {
            Thread.sleep(intervalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
        }
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "crypto-price-producer");
        return properties;
    }
    private record ProducerSettings(String bootstrapServers,
                                    String topic,
                                    long intervalMs,
                                    long maxEvents,
                                    URI cryptoApiUri) {
        private static ProducerSettings fromEnvironment() {
            return new ProducerSettings(
                    env("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
                    env("KAFKA_TOPIC", DEFAULT_TOPIC),
                    longEnv("PRODUCER_INTERVAL_MS", DEFAULT_INTERVAL_MS),
                    longEnv("PRODUCER_MAX_EVENTS", DEFAULT_MAX_EVENTS),
                    URI.create(env("COINGECKO_API_URL", CryptoApiClient.DEFAULT_API_URI.toString()))
            );
        }

        private static String env(String key, String defaultValue) {
            String value = System.getenv(key);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static long longEnv(String key, long defaultValue) {
            String value = System.getenv(key);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(key + " must be a whole number, but was: " + value, e);
            }
        }
    }
}
