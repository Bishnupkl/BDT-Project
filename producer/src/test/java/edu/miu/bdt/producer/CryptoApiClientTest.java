package edu.miu.bdt.producer;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.math.BigDecimal;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CryptoApiClientTest {
    @Test
    void fetchLatestPricesCallsApiAndMapsUsdPricesToKafkaEvents() throws Exception {
        StubHttpClient httpClient = new StubHttpClient(200, """
                {
                  "bitcoin": {"usd": 65231.45},
                  "ethereum": {"usd": 3210.20}
                }
                """);
        Clock clock = Clock.fixed(Instant.parse("2026-05-08T20:42:33Z"), ZoneOffset.UTC);
        URI apiUri = URI.create("https://example.test/api/v3/simple/price");
        CryptoApiClient client = new CryptoApiClient(
                httpClient,
                CryptoJsonMapper.create(),
                apiUri,
                List.of(
                        new CryptoApiClient.CryptoAsset("bitcoin", "BTC"),
                        new CryptoApiClient.CryptoAsset("ethereum", "ETH")
                ),
                clock
        );

        List<CryptoPriceEvent> events = client.fetchLatestPrices();

        assertEquals("ids=bitcoin%2Cethereum&vs_currencies=usd", httpClient.request().uri().getRawQuery());
        assertEquals("application/json", httpClient.request().headers().firstValue("Accept").orElseThrow());
        assertEquals("miu-bdt-crypto-producer/1.0", httpClient.request().headers().firstValue("User-Agent").orElseThrow());
        assertEquals(2, events.size());
        assertEquals(new CryptoPriceEvent("BTC", new BigDecimal("65231.45"), clock.instant()), events.get(0));
        assertEquals(new CryptoPriceEvent("ETH", new BigDecimal("3210.20"), clock.instant()), events.get(1));
    }

    @Test
    void responseMappingSkipsConfiguredAssetsThatAreMissingFromApiResponse() throws Exception {
        CryptoApiResponse response = CryptoJsonMapper.create().readValue("""
                {
                  "bitcoin": {"usd": 65231.45}
                }
                """, CryptoApiResponse.class);

        List<CryptoPriceEvent> events = response.toEvents(
                List.of(
                        new CryptoApiClient.CryptoAsset("bitcoin", "BTC"),
                        new CryptoApiClient.CryptoAsset("ethereum", "ETH")
                ),
                Instant.parse("2026-05-08T20:42:33Z")
        );

        assertEquals(1, events.size());
        assertTrue(events.contains(new CryptoPriceEvent(
                "BTC",
                new BigDecimal("65231.45"),
                Instant.parse("2026-05-08T20:42:33Z")
        )));
    }

    private static class StubHttpClient extends HttpClient {
        private final int statusCode;
        private final String body;
        private HttpRequest request;

        private StubHttpClient(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }

        private HttpRequest request() {
            return request;
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            return null;
        }

        @Override
        public SSLParameters sslParameters() {
            return new SSLParameters();
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
            this.request = request;
            return (HttpResponse<T>) new StubHttpResponse(request, statusCode, body);
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                                HttpResponse.BodyHandler<T> responseBodyHandler) {
            throw new UnsupportedOperationException("Async HTTP is not used by this test");
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                                HttpResponse.BodyHandler<T> responseBodyHandler,
                                                                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            throw new UnsupportedOperationException("Async HTTP is not used by this test");
        }
    }

    private record StubHttpResponse(HttpRequest request, int statusCode, String body) implements HttpResponse<String> {
        @Override
        public Optional<HttpResponse<String>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return HttpHeaders.of(Map.of(), (name, value) -> true);
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return request.uri();
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }
}
