/** 
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

/**
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
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestRequest;

/**
 * Handle REST calls for the /infino index.
 * This effectively serves as the public API for Infino.
 *
 * Notes:
 * 1. Search window defaults to the past 30 days if not specified by the
 * request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with
 * '/infino/'.
 * 3. Index creation or deletion is mirrored on Infino and in OpenSarch.
 * 4. We use our own thread pool to manage Infino requests.
 *
 * Note that OpenSearch changed the import paths in v2.10
 * 
 * org.opensearch.core.rest.RestStatus
 * org.opensearch.core.action.ActionHandler
 * 
 * from
 * 
 * org.opensearch.rest.RestStatus
 * org.opensearch.action.ActionHandler
 * 
 * and Java doesn't have conditional imports so we have to use
 * reflection to dynamically load the classes we need. Makes this
 * code far more complex than it needs to be.
 * 
 */
public class InfinoActionHandler {

    private static final int MAX_RETRIES = 5; // Maximum number of retries for exponential backoff
    private static final int THREADPOOL_SIZE = 25; // Size of threadpool we will use for Infino
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger(InfinoRestHandler.class);

    /**
     * Using a custom thread factory that can be used by the
     * ScheduledExecutorService.
     * We do this to add custom prefixes to the thread name. This will make
     * debugging
     * easier, if we ever have to debug.
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
     * @param request - the REST request to serialize
     * 
     * @return a configured InfinoSerializeRequestURI object
     */
    protected <Request extends ActionRequest, Response extends ActionResponse> InfinoSerializeActionRequestURI getInfinoSerializeActionRequestURI(
            RestRequest.Method method, String indexName) {
        return new InfinoSerializeActionRequestURI(method, indexName);
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
     * OpenSearch's REST thread pool so we can serialize in parallel. However
     * network calls use our own privileged thread factory.
     *
     * We exponentially backoff for 429, 503, and 504 responses
     *
     * @param action  the action to execute
     * @param request the request on the action chain
     * @throws IOException        if an I/O exception occurred executing the request
     *                            on
     *                            Infino
     * @throws ExecutionException
     * 
     */

    protected <Request extends ActionRequest, Response extends ActionResponse> void mirrorInfino(String action,
            RestRequest.Method method, String indexName)
            throws IOException, ExecutionException, IllegalArgumentException {

        InfinoSerializeActionRequestURI infinoSerializeActionRequestURI = null;
        HttpClient httpClient = getHttpClient();

        try {
            infinoSerializeActionRequestURI = getInfinoSerializeActionRequestURI(method, indexName);
        } catch (Exception e) {
            logger.error("Error serializing REST URI for Infino: ", e);
            throw new IOException("Error serializing REST URI for Infino", e);
        }

        logger.info("Serialized action request for Infino to " + infinoSerializeActionRequestURI.getFinalUrl());

        HttpRequest forwardRequest = HttpRequest.newBuilder()
                .uri(URI.create(infinoSerializeActionRequestURI.getFinalUrl()))
                .method(method.toString(), HttpRequest.BodyPublishers.noBody())
                .build();

        logger.info("Sending HTTP Request to Infino: " + infinoSerializeActionRequestURI.getFinalUrl());

        final String indexParam = indexName;
        final RestRequest.Method methodParam = method;
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                sendRequestWithBackoff(httpClient, forwardRequest, indexParam, methodParam, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, infinoThreadPool);

        try {
            future.get(); // This will block until the CompletableFuture completes
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException("An error occurred while sending HTTP request to Infino", cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("The operation was interrupted", e);
        }
    }

    private void sendRequestWithBackoff(HttpClient backoffHttpClient, HttpRequest request,
            String indexName, RestRequest.Method method, int attempt) throws IOException {
        if (attempt >= MAX_RETRIES) {
            throw new IOException(
                    "Max retries exceeded on Infino request " + request.toString() + " on index " + indexName);
        }

        try {
            HttpResponse<String> response = backoffHttpClient.send(request, HttpResponse.BodyHandlers.ofString());
            processResponse(backoffHttpClient, response, indexName, method, attempt, request);
        } catch (Exception e) {
            throw new IOException(
                    "Infino request failed " + request.toString() + " on index " + indexName + " with error " + e);
        }
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == 429 || statusCode == 503 || statusCode == 504;
    }

    private void processResponse(HttpClient processHttpClient, HttpResponse<String> response,
            String indexName, RestRequest.Method method, int attempt, HttpRequest request) throws IOException {
        int statusCode = response.statusCode();
        if (shouldRetry(statusCode)) {
            long retryAfter = getRetryAfter(response, attempt);
            infinoThreadPool.schedule(() -> {
                try {
                    sendRequestWithBackoff(processHttpClient, request, indexName, method, attempt + 1);
                } catch (IOException e) {
                    logger.error("Failed to send request with backoff: {}", e.getMessage(), e);
                }
            }, retryAfter, TimeUnit.MILLISECONDS);
        } else {
            logger.info("Response processed without retry for indexName: {} with status code: {}", indexName,
                    statusCode);
        }
    }

    private long getRetryAfter(HttpResponse<String> response, int attempt) {
        return response.headers().firstValueAsLong("Retry-After").orElse((long) Math.pow(2, attempt) * 1000L);
    }
};
