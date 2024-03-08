package org.opensearch.infino;

import org.junit.Before;
import org.junit.After;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoSerializeTransportRequest.InfinoOperation;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class InfinoTransportInterceptorTests extends OpenSearchTestCase {
    protected static final int MAX_RETRIES = 2;
    private ExecutorService executorService;
    private InfinoTransportInterceptor interceptor;
    private InfinoSerializeTransportRequest mockInfinoSerializeTransportRequest;
    private ThreadPool threadPool;
    private int mockStatusCode = 200;
    private String mockPath = "/default/path";
    private String mockBody = "Default body";
    // private Map<String, List<String>> mockHeaders;

    private MyHttpClient mockMyHttpClient = new MyHttpClient() {
        @Override
        public HttpResponse<String> sendRequest(HttpRequest request,
                HttpResponse.BodyHandler<String> responseBodyHandler) {
            // Return a mocked response
            HttpResponse<String> response = createFakeResponse(mockStatusCode, mockPath, mockBody);
            return response;
        }
    };

    public interface MyHttpClient {
        HttpResponse<String> sendRequest(HttpRequest request,
                HttpResponse.BodyHandler<String> responseBodyHandler);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        executorService = Executors.newSingleThreadExecutor();
        mockInfinoSerializeTransportRequest = mock(InfinoSerializeTransportRequest.class);
        threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");

        interceptor = new InfinoTransportInterceptor() {
            @Override
            protected ExecutorService getInfinoThreadPool() {
                return executorService;
            }

            @Override
            protected InfinoSerializeTransportRequest getInfinoSerializeTransportRequest(TransportRequest request) {
                return mockInfinoSerializeTransportRequest;
            }

            @Override
            protected HttpClient getHttpClient() {
                return getCustomHttpClient();
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
        }
    }

    // Create a fake HttpResponse for testing
    private HttpResponse<String> createFakeResponse(int fakeStatusCode, String fakePath, String fakeBody) {
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

    private HttpClient getCustomHttpClient() {
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
    private <T> HttpResponse<T> convertToGenericResponse(HttpResponse<String> response, BodyHandler<T> handler) {
        @SuppressWarnings("unchecked")
        HttpResponse<T> genericResponse = (HttpResponse<T>) response;
        return genericResponse;
    }

    public void testSuccessfulShardSearchRequest() throws Exception {
        // Given
        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockShardSearchRequest, listener);
        // AsyncSender sendRequest(null, null, mockShardSearchRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testNonExistentEndpoint() throws Exception {
        // Given
        mockStatusCode = 404;
        mockBody = "Not Found";

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path/non-existent-endpoint");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                assertEquals(404,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getStatusCode());
                assertEquals("Not Found",
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getBody().utf8ToString());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockShardSearchRequest, listener);
        // asyncSender.sendRequest(null, null, mockShardSearchRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testServerError() throws Exception {
        // Given
        mockStatusCode = 500;
        mockBody = "Internal Server Error";

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                assertEquals(500,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getStatusCode());
                assertEquals("Internal Server Error",
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getBody().utf8ToString());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockShardSearchRequest, listener);
        // asyncSender.sendRequest(null, null, mockShardSearchRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testLargeResponsePayload() throws Exception {
        // Given
        mockBody = String.join("", Collections.nCopies(1000, "Large payload. "));

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                assertEquals(200,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getStatusCode());
                assertEquals(mockBody,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getBody().utf8ToString());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockShardSearchRequest, listener);
        // asyncSender.sendRequest(null, null, mockShardSearchRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testIndexRequest() throws Exception {
        // Given
        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.POST);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");
        when(mockInfinoSerializeTransportRequest.getBody()).thenReturn(new BytesArray("{\"field\":\"value\"}"));

        IndexRequest mockIndexRequest = mock(IndexRequest.class);
        when(mockIndexRequest.index()).thenReturn("test-index");
        when(mockIndexRequest.indices()).thenReturn(new String[] { "test-index" });

        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                assertEquals(200,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getStatusCode());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockIndexRequest, listener);
        // asyncSender.sendRequest(null, null, mockIndexRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testCreateIndexRequest() throws Exception {
        // Given
        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.PUT);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");
        when(mockInfinoSerializeTransportRequest.getOperation()).thenReturn(InfinoOperation.CREATE_INDEX);

        CreateIndexRequest mockCreateIndexRequest = mock(CreateIndexRequest.class);
        when(mockCreateIndexRequest.index()).thenReturn("test-index");
        when(mockCreateIndexRequest.indices()).thenReturn(new String[] { "test-index" });
        // Create settings using the Settings builder
        Settings testSettings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

        when(mockCreateIndexRequest.settings()).thenReturn(testSettings);
        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                assertEquals(200,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getStatusCode());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockCreateIndexRequest, listener);
        // asyncSender.sendRequest(null, null, mockCreateIndexRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testDeleteIndexRequest() throws Exception {
        // Given
        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.DELETE);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");
        when(mockInfinoSerializeTransportRequest.getOperation()).thenReturn(InfinoOperation.DELETE_INDEX);

        DeleteIndexRequest mockDeleteIndexRequest = mock(DeleteIndexRequest.class);
        when(mockDeleteIndexRequest.indices()).thenReturn(new String[] { "test-index" });

        final CountDownLatch latch = new CountDownLatch(1);

        final boolean[] onResponseCalled = { false };
        final boolean[] onFailureCalled = { false };

        ActionListener<TransportResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse response) {
                onResponseCalled[0] = true;
                assertEquals(200,
                        ((InfinoTransportInterceptor.HttpResponseTransportResponse) response).getStatusCode());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled[0] = true;
                latch.countDown();
            }
        };

        // When
        interceptor.processTransportActions(mockDeleteIndexRequest, listener);
        // asyncSender.sendRequest(null, null, mockDeleteIndexRequest, null, null);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }
}