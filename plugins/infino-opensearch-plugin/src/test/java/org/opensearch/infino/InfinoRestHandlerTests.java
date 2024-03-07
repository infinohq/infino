/**
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

package org.opensearch.infino;

import org.junit.Before;
import org.junit.After;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Test the Infino Rest handler.
 *
 * We have to mock a number of classes as there is a lot of boilerplate
 * infrastructure around Rest calls and thread management in OpenSearch.
 * 
 * Route validation is handled by BaseRestHandler. We'll need to
 * test that in integration tests as our handler is only registered
 * for validated methods and paths.
 * 
 * Testing the side effects of a PUT or DELETE request (i.e.
 * creating or deleting a Lucene mirror) or testing backoffs
 * through unit tests has proven to be like shaving a Yak:
 * https://en.wiktionary.org/wiki/yak_shaving, so we'll pick
 * these up in integration tests.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class InfinoRestHandlerTests extends OpenSearchTestCase {
    private NodeClient mockNodeClient;
    private ExecutorService executorService;
    private InfinoSerializeRequestURI mockInfinoSerializeRequestURI;
    private InfinoRestHandler handler;
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

    // Use a single thread for testing
    // Mock the client and URI serializer
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executorService = Executors.newSingleThreadExecutor();
        mockInfinoSerializeRequestURI = mock(InfinoSerializeRequestURI.class);
        threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");
        mockNodeClient = new NodeClient(Settings.EMPTY, threadPool);

        // Override key methods in the handler
        handler = new InfinoRestHandler() {
            @Override
            protected ExecutorService getInfinoThreadPool() {
                return executorService;
            }

            @Override
            protected InfinoSerializeRequestURI getInfinoSerializeRequestURI(RestRequest request) {
                return mockInfinoSerializeRequestURI;
            }

            @Override
            protected HttpClient getHttpClient() {
                return getCustomHttpClient();
            }

            @Override
            public String getName() {
                return "test_rest_handler_infino";
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        mockNodeClient.close();
        executorService.shutdown();
        // Await termination with a timeout to ensure tests complete promptly
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

    // We use our own FakeRestChannel (from BaseRestHandler tests) because we need
    // to access latch to wait on threads in the handler.
    public final class FakeRestChannel extends AbstractRestChannel {
        protected final CountDownLatch latch;
        private final AtomicInteger responses = new AtomicInteger();
        private final AtomicInteger errors = new AtomicInteger();
        private RestResponse capturedRestResponse;

        public FakeRestChannel(RestRequest request, boolean detailedErrorsEnabled, int responseCount) {
            super(request, detailedErrorsEnabled);
            this.latch = new CountDownLatch(responseCount);
        }

        @Override
        public void sendResponse(RestResponse response) {

            this.capturedRestResponse = response;
            if (response.status() == InfinoRestHandler.getRestStatusFromCode(200)) {
                responses.incrementAndGet();
            } else {
                errors.incrementAndGet();
            }
            latch.countDown();
        }

        public RestResponse capturedResponse() {
            return capturedRestResponse;
        }

        public AtomicInteger responses() {
            return responses;
        }

        public AtomicInteger errors() {
            return errors;
        }
    }

    // Test handling of a 2xx GET response
    public void testGetRequest() throws Exception {
        runRequestHandler(RestRequest.Method.GET, "Default body");
    }

    // Test handling of a 2xx POST response
    public void testPostRequest() throws Exception {
        runRequestHandler(RestRequest.Method.POST, "Default body");
    }

    // Test handling of a 2xx HEAD response
    public void testHeadRequest() throws Exception {
        runRequestHandler(RestRequest.Method.HEAD, "Default body");
    }

    // Test handling of a 4xx response
    public void testNonExistentEndpoint() throws Exception {
        mockStatusCode = 410;
        mockBody = "Not Found";
        runRequestHandler(RestRequest.Method.GET, "Not Found", InfinoRestHandler.getRestStatusFromCode(mockStatusCode),
                "/non-existent-endpoint");
    }

    // Test handling of a 5xx response
    public void testServerError() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        mockStatusCode = 500;
        mockBody = "Internal Server Error";
        runRequestHandler(RestRequest.Method.GET, "Internal Server Error",
                InfinoRestHandler.getRestStatusFromCode(mockStatusCode), path);
    }

    // Helper method to test requests with a specific method and expected response
    private void runRequestHandler(RestRequest.Method method, String expectedBody) throws Exception {
        runRequestHandler(method, expectedBody, InfinoRestHandler.getRestStatusFromCode(200),
                "/infino/test-index/_ping");
    }

    // Test response with a large payload
    public void testLargeResponsePayload() throws Exception {
        mockBody = String.join("", Collections.nCopies(1000, "Large payload. "));
        runRequestHandler(RestRequest.Method.GET, mockBody);
    }

    // Generic helper method to test requests
    private void runRequestHandler(RestRequest.Method method, String expectedBody, Object expectedStatus,
            String path) throws Exception {
        when(mockInfinoSerializeRequestURI.getMethod()).thenReturn(method);
        when(mockInfinoSerializeRequestURI.getFinalUrl()).thenReturn("http://test-path" + path);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(path).withMethod(method)
                .build();
        FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

        handler.handleRequest(request, channel, mockNodeClient);

        boolean responseReceived = channel.latch.await(10, TimeUnit.SECONDS);
        assertTrue("Response was not received in time", responseReceived);

        int expectedErrors = 1;
        int expectedResponses = 0;
        if (expectedStatus == InfinoRestHandler.getRestStatusFromCode(200)) {
            expectedErrors = 0;
            expectedResponses = 1;
        }

        assertEquals("Expected no errors", expectedErrors, channel.errors().get());
        assertEquals("Expected one response", expectedResponses, channel.responses().get());
        assertEquals("Expected status to match", expectedStatus, channel.capturedResponse().status());
        assertEquals("Response content did not match", expectedBody,
                channel.capturedResponse().content().utf8ToString());
    }
}
