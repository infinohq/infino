package org.opensearch.infino;

import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.PushPromiseHandler;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

public class InfinoPluginTestUtils {

    protected static final int MAX_RETRIES = 2;
    private int mockStatusCode = 200;
    private String mockPath = "/default/path";
    private String mockBody = "Default body";

    public int getMockStatusCode() {
        return mockStatusCode;
    }

    public void setMockStatusCode(int mockStatusCode) {
        this.mockStatusCode = mockStatusCode;
    }

    public String getMockPath() {
        return mockPath;
    }

    public void setMockPath(String mockPath) {
        this.mockPath = mockPath;
    }

    public String getMockBody() {
        return mockBody;
    }

    public void setMockBody(String mockBody) {
        this.mockBody = mockBody;
    }

    private MyHttpClient mockMyHttpClient = new MyHttpClient() {
        @Override
        public HttpResponse<String> sendRequest(HttpRequest request,
                HttpResponse.BodyHandler<String> responseBodyHandler) {
            HttpResponse<String> response = createFakeResponse(mockStatusCode, mockPath, mockBody);
            return response;
        }
    };

    public interface MyHttpClient {
        HttpResponse<String> sendRequest(HttpRequest request,
                HttpResponse.BodyHandler<String> responseBodyHandler);
    }

    // Create a fake HttpResponse for testing
    static HttpResponse<String> createFakeResponse(int fakeStatusCode, String fakePath, String fakeBody) {
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return fakeStatusCode;
            }

            @Override
            public Optional<HttpResponse<String>> previousResponse() {
                return Optional.empty();
            }

            @Override
            public HttpRequest request() {
                return HttpRequest.newBuilder().uri(URI.create(fakePath)).build();
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of(new HashMap<>(), (s, s2) -> true);
            }

            @Override
            public String body() {
                return fakeBody; // Fake body content
            }

            @Override
            public Optional<SSLSession> sslSession() {
                return Optional.empty();
            }

            @Override
            public URI uri() {
                return request().uri();
            }

            @Override
            public HttpClient.Version version() {
                return HttpClient.Version.HTTP_1_1;
            }
        };
    }

    public HttpClient getCustomHttpClient() {
        return new HttpClient() {
            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                    BodyHandler<T> responseBodyHandler, PushPromiseHandler<T> pushPromiseHandler) {
                throw new UnsupportedOperationException("sendAsync Not implemented in mock");
            }

            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                    BodyHandler<T> responseBodyHandler) {
                throw new UnsupportedOperationException("sendAsync Not implemented in mock");
            }

            @Override
            public <T> HttpResponse<T> send(HttpRequest request, BodyHandler<T> responseBodyHandler)
                    throws IOException, InterruptedException {
                HttpResponse<String> response = mockMyHttpClient.sendRequest(request,
                        convertToSpecificHandler(responseBodyHandler));
                return convertToGenericResponse(response, responseBodyHandler);
            }

            // Helper method to convert BodyHandler<T> to BodyHandler<String>
            private BodyHandler<String> convertToSpecificHandler(BodyHandler<?> handler) {
                return HttpResponse.BodyHandlers.ofString();
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
                return null;
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
                return null;
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
        };
    }

    // Helper method to convert HttpResponse<String> to HttpResponse<T>
    private static <T> HttpResponse<T> convertToGenericResponse(HttpResponse<String> response, BodyHandler<T> handler) {
        @SuppressWarnings("unchecked")
        HttpResponse<T> genericResponse = (HttpResponse<T>) response;
        return genericResponse;
    }

}