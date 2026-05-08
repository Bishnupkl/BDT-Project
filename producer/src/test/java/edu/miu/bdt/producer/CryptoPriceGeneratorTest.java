package edu.miu.bdt.producer;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CryptoPriceGeneratorTest {
    @Test
    void generatesEventWithSupportedSymbolPositivePriceAndCurrentTimestamp() {
        Clock fixedClock = Clock.fixed(Instant.parse("2026-05-07T12:00:00Z"), ZoneOffset.UTC);
        CryptoPriceGenerator generator = new CryptoPriceGenerator(fixedClock);

        CryptoPriceEvent event = generator.nextEvent();

        assertTrue(Set.of("BTC", "ETH", "SOL", "ADA", "XRP").contains(event.symbol()));
        assertTrue(event.price().compareTo(BigDecimal.ZERO) > 0);
        assertEquals(2, event.price().scale());
        assertEquals(Instant.parse("2026-05-07T12:00:00Z"), event.timestamp());
    }
}
