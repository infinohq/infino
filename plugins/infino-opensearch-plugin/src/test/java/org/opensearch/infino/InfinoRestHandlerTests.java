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

import java.net.http.HttpClient;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private InfinoPluginTestUtils utils;

    // Use a single thread for testing
    // Mock the client and URI serializer
    @Before
    public void setUp() throws Exception {
        super.setUp();
        utils = new InfinoPluginTestUtils();
        executorService = Executors.newSingleThreadExecutor();
        mockInfinoSerializeRequestURI = mock(InfinoSerializeRequestURI.class);
        threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");
        mockNodeClient = new NodeClient(Settings.EMPTY, threadPool);

        // Override key methods in the handler
        handler = new InfinoRestHandler() {

            @Override
            protected InfinoSerializeRequestURI getInfinoSerializeRequestURI(RestRequest request) {
                return mockInfinoSerializeRequestURI;
            }

            @Override
            protected HttpClient getHttpClient() {
                return utils.getCustomHttpClient();
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
            if (response.status() == InfinoPluginUtils.getRestStatusFromCode(200)) {
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
        runRequestHandler(RestRequest.Method.GET, utils.getMockBody());
    }

    // Test handling of a 2xx POST response
    public void testPostRequest() throws Exception {
        runRequestHandler(RestRequest.Method.POST, utils.getMockBody());
    }

    // Test handling of a 2xx HEAD response
    public void testHeadRequest() throws Exception {
        runRequestHandler(RestRequest.Method.HEAD, utils.getMockBody());
    }

    // Test handling of a 4xx response
    public void testNonExistentEndpoint() throws Exception {
        utils.setMockStatusCode(410);
        utils.setMockBody("Not Found");
        runRequestHandler(RestRequest.Method.GET, "Not Found",
                InfinoPluginUtils.getRestStatusFromCode(utils.getMockStatusCode()),
                "/non-existent-endpoint");
    }

    // Test handling of a 5xx response
    public void testServerError() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        utils.setMockStatusCode(500);
        utils.setMockBody("Internal Server Error");
        runRequestHandler(RestRequest.Method.GET, "Internal Server Error",
                InfinoPluginUtils.getRestStatusFromCode(utils.getMockStatusCode()), path);
    }

    // Helper method to test requests with a specific method and expected response
    private void runRequestHandler(RestRequest.Method method, String expectedBody) throws Exception {
        runRequestHandler(method, expectedBody, InfinoPluginUtils.getRestStatusFromCode(200),
                "/infino/test-index/_ping");
    }

    // Test response with a large payload
    public void testLargeResponsePayload() throws Exception {
        utils.setMockBody(String.join("", Collections.nCopies(1000, "Large payload. ")));
        runRequestHandler(RestRequest.Method.GET, utils.getMockBody());
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
        if (expectedStatus == InfinoPluginUtils.getRestStatusFromCode(200)) {
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
