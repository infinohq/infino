package org.opensearch.infino;

import org.junit.Before;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoSerializeTransportRequest.InfinoOperation;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;

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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class InfinoTransportInterceptorTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(InfinoTransportInterceptorTests.class);

    protected static final int MAX_RETRIES = 2;
    private ExecutorService executorService;
    private InfinoTransportInterceptor interceptor;
    private InfinoSerializeTransportRequest mockInfinoSerializeTransportRequest;
    private ThreadPool threadPool;
    private int mockStatusCode = 200;
    private String mockPath = "/default/path";
    private String mockBody = "default body";

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

        interceptor = new InfinoTransportInterceptor(getCustomHttpClient()) {
            @Override
            protected ExecutorService getInfinoThreadPool() {
                return executorService;
            }

            @Override
            protected InfinoSerializeTransportRequest getInfinoSerializeTransportRequest(BulkShardRequest request,
                    InfinoOperation operation) {
                return mockInfinoSerializeTransportRequest;
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
        // Mock setup
        mockBody = "{\n" +
                "  \"took\": 30,\n" +
                "  \"timed_out\": false,\n" +
                "  \"_shards\": {\n" +
                "    \"total\": 5,\n" +
                "    \"successful\": 5,\n" +
                "    \"skipped\": 0,\n" +
                "    \"failed\": 0\n" +
                "  },\n" +
                "  \"hits\": {\n" +
                "    \"total\": {\n" +
                "      \"value\": 1,\n" +
                "      \"relation\": \"eq\"\n" +
                "    },\n" +
                "    \"max_score\": 1.0,\n" +
                "    \"hits\": [\n" +
                "      {\n" +
                "        \"_index\": \"my_index\",\n" +
                "        \"_type\": \"_doc\",\n" +
                "        \"_id\": \"1\",\n" +
                "        \"_score\": 1.0,\n" +
                "        \"_source\": {\n" +
                "          \"title\": \"Example document\",\n" +
                "          \"content\": \"This is an example document stored in OpenSearch.\",\n" +
                "          \"date\": \"2023-03-09\"\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);

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

        interceptor.processTransportActions(mockShardSearchRequest, InfinoOperation.SEARCH_DOCUMENTS, listener);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testEmptySearchResponse() throws Exception {
        mockBody = "{\n" +
                "        \"took\": 30,\n" +
                "        \"timed_out\": false,\n" +
                "        \"_shards\": {\n" +
                "          \"total\": 5,\n" +
                "          \"successful\": 5,\n" +
                "          \"skipped\": 0,\n" +
                "          \"failed\": 0\n" +
                "        },\n" +
                "        \"hits\": {\n" +
                "          \"total\": {\n" +
                "            \"value\": 1,\n" +
                "            \"relation\": \"eq\"\n" +
                "          },\n" +
                "          \"max_score\": 1.0,\n" +
                "          \"hits\": [\n" +
                "          ]\n" +
                "        }\n" +
                "      }";

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

        interceptor.processTransportActions(mockShardSearchRequest, InfinoOperation.SEARCH_DOCUMENTS, listener);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public void testNonExistentEndpoint() throws Exception {
        mockStatusCode = 404;
        mockBody = "Not Found";

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path/non-existent-endpoint");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index"
        });

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

        interceptor.processTransportActions(mockShardSearchRequest, InfinoOperation.SEARCH_DOCUMENTS, listener);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        assertTrue("onFailure was not called as expected", onFailureCalled[0]);
        assertFalse("onResponse was unexpectedly called", onResponseCalled[0]);
    }

    public void testServerError() throws Exception {
        mockStatusCode = 500;
        mockBody = "{\n" +
                "    \"error\": {\n" +
                "        \"root_cause\": [\n" +
                "            {\n" +
                "                \"type\": \"internal_server_error\",\n" +
                "                \"reason\": \"Internal server error\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"type\": \"internal_server_error\",\n" +
                "        \"reason\": \"Internal server error\",\n" +
                "        \"caused_by\": {\n" +
                "            \"type\": \"specific_error_type\",\n" +
                "            \"reason\": \"Specific reason for the error\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"status\": 500\n" +
                "}";

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index"
        });

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
        interceptor.processTransportActions(mockShardSearchRequest, InfinoOperation.SEARCH_DOCUMENTS, listener);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Then
        assertTrue("onFailure was not called as expected", onFailureCalled[0]);
        assertFalse("onResponse was unexpectedly called", onResponseCalled[0]);
    }

    public void testLargeResponsePayload() throws Exception {
        mockBody = generateLargeResponseString(1000);

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.GET);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path");

        ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
        when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);
        when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index"
        });

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

        interceptor.processTransportActions(mockShardSearchRequest, InfinoOperation.SEARCH_DOCUMENTS, listener);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

    public static String generateLargeResponseString(int numberOfHits) {
        JsonObject jsonResponse = new JsonObject();

        jsonResponse.addProperty("took", 30);
        jsonResponse.addProperty("timed_out", false);

        JsonObject shards = new JsonObject();
        shards.addProperty("total", 5);
        shards.addProperty("successful", 5);
        shards.addProperty("skipped", 0);
        shards.addProperty("failed", 0);
        jsonResponse.add("_shards", shards);

        JsonObject hits = new JsonObject();
        JsonObject total = new JsonObject();
        total.addProperty("value", numberOfHits);
        total.addProperty("relation", "eq");
        hits.add("total", total);
        hits.addProperty("max_score", 1.0);

        JsonArray hitsArray = new JsonArray();
        for (int i = 1; i <= numberOfHits; i++) {
            JsonObject hit = new JsonObject();
            hit.addProperty("_index", "my_index");
            hit.addProperty("_type", "_doc");
            hit.addProperty("_id", String.valueOf(i));
            hit.addProperty("_score", 1.0);

            JsonObject source = new JsonObject();
            source.addProperty("title", "Example document " + i);
            source.addProperty("content", "This is an example document stored in OpenSearch.");
            source.addProperty("date", "2023-03-09");

            hit.add("_source", source);
            hitsArray.add(hit);
        }
        hits.add("hits", hitsArray);
        jsonResponse.add("hits", hits);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(jsonResponse);
    }

    @SuppressWarnings("unchecked")
    public void testBulkRequest() throws Exception {
        BulkShardRequest mockBulkShardRequest = mock(BulkShardRequest.class);
        when(mockBulkShardRequest.shardId()).thenReturn(new ShardId("index", "_na_", 1));
        when(mockBulkShardRequest.indices()).thenReturn(new String[] { "test-index" });

        String indexName = "test-index";
        IndexRequest indexRequest = new IndexRequest(indexName)
                .id("1")
                .source(XContentType.JSON, "title", "Document 1", "content", "Example content 1");
        DeleteRequest deleteRequest = new DeleteRequest(indexName, "2");
        IndexRequest createRequest = new IndexRequest(indexName)
                .id("3")
                .source(XContentType.JSON, "title", "Document 3", "content", "Example content 3");
        UpdateRequest updateRequest = new UpdateRequest(indexName, "1")
                .doc(XContentType.JSON, "doc", Map.of("content", "Updated content 1"));
        BulkItemRequest[] requests = new BulkItemRequest[] { new BulkItemRequest(1, indexRequest),
                new BulkItemRequest(2, deleteRequest), new BulkItemRequest(3, createRequest),
                new BulkItemRequest(1, updateRequest) };
        when(mockBulkShardRequest.items()).thenReturn((BulkItemRequest[]) requests);

        ConcreteShardRequest<BulkShardRequest> mockConcreteRequest = mock(ConcreteShardRequest.class);

        when(mockConcreteRequest.getRequest()).thenReturn(mockBulkShardRequest);
        when(mockConcreteRequest.getTargetAllocationID()).thenReturn("allocationId");
        when(mockConcreteRequest.getPrimaryTerm()).thenReturn(1L);

        when(mockInfinoSerializeTransportRequest.getMethod()).thenReturn(RestRequest.Method.POST);
        when(mockInfinoSerializeTransportRequest.getFinalUrl()).thenReturn("http://test-path/test-index/bulk");

        mockBody = "{\n" +
                "  \"took\": 30,\n" +
                "  \"errors\": true,\n" +
                "  \"items\": [\n" +
                "    {\n" +
                "      \"index\": {\n" +
                "        \"_index\": \"test-index\",\n" +
                "        \"_id\": \"1\",\n" +
                "        \"_version\": 1,\n" +
                "        \"result\": \"created\",\n" +
                "        \"_shards\": {\n" +
                "          \"total\": 2,\n" +
                "          \"successful\": 1,\n" +
                "          \"failed\": 0\n" +
                "        },\n" +
                "        \"status\": 201\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"delete\": {\n" +
                "        \"_index\": \"test-index\",\n" +
                "        \"_id\": \"2\",\n" +
                "        \"status\": 404,\n" +
                "        \"error\": {\n" +
                "          \"type\": \"document_missing_exception\",\n" +
                "          \"reason\": \"[test-index][2]: document missing\",\n" +
                "          \"index_uuid\": \"aAsFqTI0Tc2W0LCWgPNrOA\",\n" +
                "          \"shard\": \"0\",\n" +
                "          \"index\": \"test-index\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

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

        // Trigger processing the bulk request
        interceptor.processTransportActions(mockConcreteRequest, InfinoOperation.BULK_DOCUMENTS, listener);

        // Wait for the async operation to complete or timeout
        latch.await(5, TimeUnit.SECONDS);

        // Assertions to verify the test outcome
        assertTrue("onResponse was not called as expected", onResponseCalled[0]);
        assertFalse("onFailure was unexpectedly called", onFailureCalled[0]);
    }

}