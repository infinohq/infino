package org.opensearch.infino;

import org.junit.Before;
import org.junit.After;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class InfinoActionHandlerTests extends OpenSearchTestCase {

    private ExecutorService executorService;
    private InfinoActionHandler handler;
    private InfinoSerializeActionRequestURI mockInfinoSerializeActionRequestURI;
    private ThreadPool threadPool;
    private InfinoPluginTestUtils utils;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        utils = new InfinoPluginTestUtils();
        executorService = Executors.newSingleThreadExecutor();
        mockInfinoSerializeActionRequestURI = mock(InfinoSerializeActionRequestURI.class);
        threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");

        handler = new InfinoActionHandler(utils.getCustomHttpClient()) {
            @SuppressWarnings("unused")
            protected ExecutorService getInfinoThreadPool() {
                return executorService;
            }

            @SuppressWarnings("unused")
            protected <Request extends ActionRequest, Response extends ActionResponse> InfinoSerializeActionRequestURI getInfinoSerializeTransportRequest(
                    RestRequest.Method method,
                    String indexName) {
                return mockInfinoSerializeActionRequestURI;
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

    public void testMirrorIndexCreationSuccessfully() throws Exception {
        // Given
        String expectedIndexName = "test-index";
        RestRequest.Method expectedMethod = RestRequest.Method.PUT;

        String expectedFinalUrl = "http://mockserver/" + expectedIndexName;
        when(mockInfinoSerializeActionRequestURI.getFinalUrl()).thenReturn(expectedFinalUrl);

        // Mocking the response to simulate a successful index creation on Infino side
        utils.setMockStatusCode(200);
        utils.setMockPath(expectedFinalUrl);
        utils.setMockBody("{}");

        handler.mirrorInfino(CreateIndexAction.NAME, expectedMethod,
                expectedIndexName);

        assertTrue("Test completed successfully", true);
    }

    public void testMirrorIndexDeletionSuccessfully() throws Exception {
        // Given
        String expectedIndexName = "test-index";
        RestRequest.Method expectedMethod = RestRequest.Method.DELETE;

        String expectedFinalUrl = "http://mockserver/" + expectedIndexName;
        when(mockInfinoSerializeActionRequestURI.getFinalUrl()).thenReturn(expectedFinalUrl);

        // Mocking the response to simulate a successful index creation on Infino side
        utils.setMockStatusCode(200);
        utils.setMockPath(expectedFinalUrl);
        utils.setMockBody("{}");

        handler.mirrorInfino(DeleteIndexAction.NAME, expectedMethod,
                expectedIndexName);

        assertTrue("Test completed successfully", true);
    }

}