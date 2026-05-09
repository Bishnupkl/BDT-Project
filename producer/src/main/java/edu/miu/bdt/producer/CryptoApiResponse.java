package edu.miu.bdt.producer;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CryptoApiResponse {
    private final Map<String, CurrencyPrice> pricesByCoinId = new LinkedHashMap<>();

    @JsonAnySetter
    public void addPrice(String coinId, CurrencyPrice price) {
        pricesByCoinId.put(coinId, price);
    }

    public Map<String, CurrencyPrice> pricesByCoinId() {
        return Collections.unmodifiableMap(pricesByCoinId);
    }

    public List<CryptoPriceEvent> toEvents(List<CryptoApiClient.CryptoAsset> assets, Instant timestamp) {
        return assets.stream()
                .map(asset -> toEvent(asset, timestamp))
                .filter(Objects::nonNull)
                .toList();
    }

    private CryptoPriceEvent toEvent(CryptoApiClient.CryptoAsset asset, Instant timestamp) {
        CurrencyPrice price = pricesByCoinId.get(asset.coinGeckoId());
        if (price == null || price.usd() == null) {
            return null;
        }
        return new CryptoPriceEvent(asset.symbol(), price.usd(), timestamp);
    }

    public static class CurrencyPrice {
        private BigDecimal usd;

        public BigDecimal usd() {
            return usd;
        }

        public void setUsd(BigDecimal usd) {
            this.usd = usd;
        }
    }
}
