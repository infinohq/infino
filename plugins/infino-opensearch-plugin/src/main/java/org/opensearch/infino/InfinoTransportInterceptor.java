/** 
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

/**
 * 
 * Below permissions need to be added to java.policy for the plugin to work.
 *  
 * 1. permission org.opensearch.secure_sm.ThreadPermission "modifyArbitraryThread";
 * 
 *      The HttpClient::send() method seems to be using threads internally
 *      and opensearch's security policy blocks them. We need to add the
 *      below permissions to the java.policy to allowe HttpClient::send()
 *      to work.
 * 
 *      It seems odd that opensearch's policy is blocking HttpClient::send().
 *      We need to investigate this further to see if there are better
 *      Http libraries we can use in opensearch.
 * 
 *      Reference: below issue in github seems to reference the
 *                 modifyArbitraryThread permission.
 *                 https://github.com/opensearch-project/OpenSearch/issues/5359
 * 
 */
// 2. permission java.net.URLPermission "http://*:*/-", "*";
/*
 *      This permission is needed to allow outbound connections to Infino
 *      server.
 */
package org.opensearch.infino;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.infino.InfinoSerializeTransportRequest.InfinoOperation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.NoSuchMethodException;
import java.lang.InstantiationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.Transport.Connection;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;

/**
 * Handle transport requests.
 *
 * Notes:
 * 1. Search window defaults to the past 7 days
 * 2. Index creation or deletion is mirrored on Infino and in OpenSarch.
 * 3. We use our own thread pool to manage Infino requests.
 *
 * Note that OpenSearch changed the import paths in v2.10
 * 
 * org.opensearch.core.io.stream.StreamInput
 * org.opensearch.core.io.stream.StreamOutput
 * 
 * 
 * from
 * 
 * org.opensearch.io.stream.StreamInput
 * org.opensearch.io.stream.StreamOutput
 * 
 * and Java doesn't have conditional imports so we have to use
 * reflection to dynamically load the classes we need. Makes this
 * code far more complex than it needs to be.
 * 
 */
public class InfinoTransportInterceptor implements TransportInterceptor {

    private static final int MAX_RETRIES = 5; // Maximum number of retries for exponential backoff
    private static final int THREADPOOL_SIZE = 25; // Size of threadpool we will use for Infino
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger(InfinoRestHandler.class);

    /**
     * Using a custom thread factory that can be used by the
     * ScheduledExecutorService.
     * We do this to add custom prefixes to the thread name. This will make
     * debugging easier, if we ever have to debug.
     */
    protected static final class CustomThreadFactory implements ThreadFactory {
        private final String poolName;

        CustomThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(poolName + "-Thread-" + t.getId());
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    /**
     * Get get a new instance of the class
     * 
     * @param request - the Transport request to serialize
     * 
     * @return a configured InfinoSerializeTransportRequest object
     */
    protected InfinoSerializeTransportRequest getInfinoSerializeTransportRequest(TransportRequest request) {
        try {
            return new InfinoSerializeTransportRequest(request);
        } catch (IOException e) {
            logger.error("Error serializing REST URI for Infino: ", e);
        }
        return null;
    }

    /**
     * Get the HTTP Client
     *
     * @return the httpclient member from this class
     */
    protected HttpClient getHttpClient() {
        return httpClient;
    }

    private static final ScheduledExecutorService infinoThreadPool = Executors.newScheduledThreadPool(THREADPOOL_SIZE,
            new CustomThreadFactory("InfinoPluginThreadPool"));

    /**
     * Get thread pool
     * 
     * @return the thread pool to use for the requests
     */
    protected ExecutorService getInfinoThreadPool() {
        return infinoThreadPool;
    }

    /**
     * Shutdown the thread pool when the plugin is stopped
     */
    public static void close() {
        infinoThreadPool.shutdown();
    }

    /**
     * Implement the request, creating or deleting Lucene index mirrors on the local
     * node.
     *
     * The first half of the method (before the thread executor) is parallellized by
     * OpenSearch's thread pool so we can serialize in parallel. However network
     * calls use
     * our own privileged thread factory.
     *
     * We exponentially backoff for 429, 503, and 504 responses
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and
     *                     preparing for execution
     */
    public AsyncSender interceptTransportActions(TransportRequest request, ActionListener<TransportResponse> listener) {

        RestRequest.Method method;
        String indexName;
        InfinoSerializeTransportRequest infinoRequest = null;
        HttpClient httpClient = getHttpClient();
        InfinoOperation operation;
        BytesReference body;

        logger.info("Serializing REST request for Infino");

        // Serialize the request to a valid Infino URL
        try {
            infinoRequest = new InfinoSerializeTransportRequest(request);
        } catch (Exception e) {
            logger.error("Error serializing REST URI for Infino: ", e);
            listener.onFailure(e);
            return new AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(Connection connection, String action,
                        TransportRequest request, TransportRequestOptions options,
                        TransportResponseHandler<T> handler) {
                    // No-op response
                }
            };
        }

        // set local members we can pass to the thread context
        method = infinoRequest.getMethod();
        indexName = infinoRequest.getIndexName();
        body = infinoRequest.getBody();
        operation = infinoRequest.getOperation();

        HttpRequest forwardRequest = HttpRequest.newBuilder()
                .uri(URI.create(infinoRequest.getFinalUrl()))
                .header("Content-Type", "application/json")
                .method(infinoRequest.getMethod().toString(),
                        HttpRequest.BodyPublishers
                                .ofInputStream(
                                        () -> new ByteArrayInputStream(body.toBytesRef().bytes)))
                .build();

        logger.info("Sending HTTP Request to Infino: " + infinoRequest.getFinalUrl());

        // Send request to Infino server and create a listener to handle the response.
        // Execute the HTTP request using our own thread factory.
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(Connection connection, String action,
                    TransportRequest request, TransportRequestOptions options,
                    TransportResponseHandler<T> handler) {
                infinoThreadPool.execute(() -> {
                    sendRequestWithBackoff(httpClient, forwardRequest, listener, indexName, method, operation, 0);
                });
            }
        };
    }

    private void sendRequestWithBackoff(HttpClient backoffHttpClient, HttpRequest request,
            ActionListener<TransportResponse> listener, String indexName, RestRequest.Method method,
            InfinoOperation operation, int attempt) {
        if (attempt >= MAX_RETRIES) {
            TransportResponse response = createTransportResponse(503, "Max retries exceeded");
            listener.onResponse(response);
            return;
        }

        try {
            HttpResponse<String> response = backoffHttpClient.send(request, HttpResponse.BodyHandlers.ofString());
            processResponse(backoffHttpClient, response, listener, indexName, method, operation, attempt, request);
        } catch (Exception e) {
            handleException(e, listener, indexName, operation);
        }
    }

    private void processResponse(HttpClient processHttpClient, HttpResponse<String> response,
            ActionListener<TransportResponse> listener, String indexName, RestRequest.Method method,
            InfinoOperation operation, int attempt, HttpRequest request) {
        int statusCode = response.statusCode();
        if (shouldRetry(statusCode)) {
            long retryAfter = getRetryAfter(response, attempt);
            infinoThreadPool.schedule(() -> sendRequestWithBackoff(processHttpClient, request, listener,
                    indexName, method, operation, attempt + 1), retryAfter, TimeUnit.MILLISECONDS);
        } else {
            handleResponse(response, listener, indexName, operation);
        }
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == 429 || statusCode == 503 || statusCode == 504;
    }

    private long getRetryAfter(HttpResponse<String> response, int attempt) {
        return response.headers().firstValueAsLong("Retry-After").orElse((long) Math.pow(2, attempt) * 1000L);
    }

    private Void handleException(Throwable e, ActionListener<TransportResponse> listener,
            String indexName, InfinoOperation operation) {
        try {
            // e.getMessage() sometimes returns null for ConnectException.
            // Using e.toString() when getMessage() returns null.
            String errMsg = e.getMessage() != null ? e.getMessage() : e.toString();
            TransportResponse errorResponse = createTransportResponse(500, errMsg);
            listener.onResponse(errorResponse);
        } catch (Exception ex) {
            logger.error("Failed to send response using reflection", ex);
            listener.onFailure(ex);
        }
        return null;
    }

    private void handleResponse(HttpResponse<String> response,
            ActionListener<TransportResponse> listener, String indexName,
            InfinoOperation operation) {
        if (Thread.currentThread().isInterrupted()) {
            logger.debug("Infino Plugin Rest handler thread interrupted. Exiting...");
            return;
        }

        try {
            int statusCode = response.statusCode();

            if (RestStatus.OK.getStatus() == statusCode
                    && (operation == InfinoOperation.CREATE_INDEX || operation == InfinoOperation.DELETE_INDEX)) {
                logger.error("Successfully created or deleted index from Infino. Resuming transport.");
                return;
            }

            // Create a new TransportResponse instance using the utility method
            TransportResponse transportResponse = createTransportResponse(statusCode, response.body());

            // Send the response back to the OpenSearch Transport layer
            listener.onResponse(transportResponse);

        } catch (Exception e) {
            logger.error("Error sending response", e);

            // Fall back to sending internal server error
            try {
                TransportResponse errorResponse = createTransportResponse(500,
                        "Internal server error: " + e.getMessage());
                listener.onResponse(errorResponse);
            } catch (Exception ex) {
                logger.error("Failed to send error response", ex);
                listener.onFailure(ex);
            }
        }
    }

    /**
     * 
     * WAR for import path changes between OpenSearch versions
     * 
     * Use reflection for dynamic imports.
     * 
     * @param statusCode - status code of the response
     * @return RestStatus object - dynamically loaded object
     */
    public static Object getRestStatusFromCode(int statusCode) {
        try {
            Class<?> restStatusClass;
            try {
                restStatusClass = Class.forName("org.opensearch.core.rest.RestStatus");
            } catch (ClassNotFoundException e) {
                restStatusClass = Class.forName("org.opensearch.rest.RestStatus");
            }
            Method fromCodeMethod = restStatusClass.getMethod("fromCode", int.class);
            return fromCodeMethod.invoke(null, statusCode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to dynamically load RestStatus class", e);
        }
    }

    /**
     * 
     * WAR for import path changes between OpenSearch versions
     * 
     * Use reflection for dynamic imports.
     * 
     * @param restStatus      - the RestStatus code for the response
     * @param responseMessage - the message to be sent
     * @return HttpResponseTransportResponse - constructed response
     */
    public HttpResponseTransportResponse createTransportResponse(int statusCode, String responseBody) {
        try {
            Constructor<HttpResponseTransportResponse> constructor = HttpResponseTransportResponse.class
                    .getConstructor(int.class, String.class);
            return constructor.newInstance(statusCode, responseBody);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            logger.error("Failed to create HttpResponseTransportResponse using reflection", e);
            throw new RuntimeException("Failed to create HttpResponseTransportResponse using reflection", e);
        }
    }

    // Custom class for reflection
    static class HttpResponseTransportResponse extends TransportResponse {
        private final int statusCode;
        private final BytesReference body;

        public HttpResponseTransportResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = new BytesArray(body.getBytes(StandardCharsets.UTF_8));
        }

        public int getStatusCode() {
            return statusCode;
        }

        public BytesReference getBody() {
            return body;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(statusCode);
            out.writeBytesReference(body);
        }

        public HttpResponseTransportResponse readFrom(StreamInput in) throws IOException {
            int statusCode = in.readInt();
            BytesReference body = in.readBytesReference();
            return new HttpResponseTransportResponse(statusCode, body.toString());
        }
    }

}
