package edu.miu.bdt.producer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CryptoPriceGenerator {
    private static final List<CryptoProfile> PROFILES = List.of(
            new CryptoProfile("BTC", 65231.45, 0.018),
            new CryptoProfile("ETH", 3210.20, 0.025),
            new CryptoProfile("SOL", 145.60, 0.035),
            new CryptoProfile("ADA", 0.46, 0.045),
            new CryptoProfile("XRP", 0.53, 0.040)
    );

    private final Clock clock;

    public CryptoPriceGenerator(Clock clock) {
        this.clock = clock;
    }

    public CryptoPriceEvent nextEvent() {
        CryptoProfile profile = PROFILES.get(ThreadLocalRandom.current().nextInt(PROFILES.size()));
        double movement = ThreadLocalRandom.current().nextDouble(-profile.volatility(), profile.volatility());
        double simulatedPrice = profile.basePrice() * (1.0 + movement);
        BigDecimal price = BigDecimal.valueOf(simulatedPrice).setScale(2, RoundingMode.HALF_UP);
        return new CryptoPriceEvent(profile.symbol(), price, clock.instant());
    }

    private record CryptoProfile(String symbol, double basePrice, double volatility) {
    }
}
