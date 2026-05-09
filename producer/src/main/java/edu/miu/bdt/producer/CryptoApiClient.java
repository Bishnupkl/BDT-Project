package edu.miu.bdt.producer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class CryptoApiClient {
    public static final URI DEFAULT_API_URI = URI.create("https://api.coingecko.com/api/v3/simple/price");
    public static final List<CryptoAsset> DEFAULT_ASSETS = List.of(
            new CryptoAsset("bitcoin", "BTC"),
            new CryptoAsset("ethereum", "ETH"),
            new CryptoAsset("solana", "SOL"),
            new CryptoAsset("cardano", "ADA"),
            new CryptoAsset("ripple", "XRP")
    );

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI apiUri;
    private final List<CryptoAsset> assets;
    private final Clock clock;

    public CryptoApiClient(HttpClient httpClient,
                           ObjectMapper objectMapper,
                           URI apiUri,
                           List<CryptoAsset> assets,
                           Clock clock) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.apiUri = apiUri;
        this.assets = List.copyOf(assets);
        this.clock = clock;
    }

    public List<CryptoPriceEvent> fetchLatestPrices() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(requestUri())
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/json")
                .header("User-Agent", "miu-bdt-crypto-producer/1.0")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("CoinGecko API returned HTTP " + response.statusCode() + ": " + response.body());
        }

        CryptoApiResponse apiResponse = objectMapper.readValue(response.body(), CryptoApiResponse.class);
        List<CryptoPriceEvent> events = apiResponse.toEvents(assets, clock.instant());
        if (events.isEmpty()) {
            throw new IOException("CoinGecko API response did not contain any configured USD prices");
        }
        return events;
    }

    private URI requestUri() {
        String ids = assets.stream()
                .map(CryptoAsset::coinGeckoId)
                .collect(Collectors.joining(","));
        String query = "ids=" + encode(ids) + "&vs_currencies=usd";
        return URI.create(apiUri + "?" + query);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    public record CryptoAsset(String coinGeckoId, String symbol) {
    }
}
