package edu.miu.bdt.producer;

import java.math.BigDecimal;
import java.time.Instant;
public record CryptoPriceEvent(
        String symbol,
        BigDecimal price,
        Instant timestamp
) {
}
