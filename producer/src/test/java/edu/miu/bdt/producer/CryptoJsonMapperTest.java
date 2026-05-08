package edu.miu.bdt.producer;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CryptoJsonMapperTest {
    @Test
    void serializesTimestampAsIsoInstant() throws Exception {
        CryptoPriceEvent event = new CryptoPriceEvent(
                "BTC",
                BigDecimal.valueOf(65231.45),
                Instant.parse("2026-05-07T12:00:00Z")
        );

        String json = CryptoJsonMapper.create().writeValueAsString(event);

        assertTrue(json.contains("\"symbol\":\"BTC\""));
        assertTrue(json.contains("\"price\":65231.45"));
        assertTrue(json.contains("\"timestamp\":\"2026-05-07T12:00:00Z\""));
    }
}
