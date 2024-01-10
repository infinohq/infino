/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
*/

package org.opensearch.infino;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

import static org.opensearch.rest.RestRequest.Method.*;


/**
 * Handle REST calls for the /infino index.
 * This effectively serves as the public API for Infino.
 *
 * Notes:
 * 1. Search window defaults to the past 30 days if not specified by the request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with '/infino/'.
 * 3. Index creation or deletion is mirrored on Infino and in OpenSarch.
 * 4. We use our own thread pool to manage Infino requests.
 *
 * Note that URIs will normally be in form:
 *
 */
public class InfinoRestHandler extends BaseRestHandler {

    private static final int MAX_RETRIES = 5; // Maximum number of retries for exponential backoff
    private static final int THREADPOOLSIZE = 10; // Size of threadpool we will use for Infino
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger(InfinoRestHandler.class);

    /** 
     * Get get a new instance of the class 
     * 
     * @param request - the REST request to serialize
     * 
     * @return a configured InfinoSerializeRequestURI object
     */
    protected InfinoSerializeRequestURI getInfinoSerializeRequestURI(RestRequest request) {
        return new InfinoSerializeRequestURI(request);
    }

    // List of futures we need to clear on close
    private static List<CompletableFuture<?>> futures = new ArrayList<>();

    /** Get the HTTP Client 
     *
     * @return the httpclient member from this class
     */
    protected HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * Name of this REST handler
     * 
     * @return a string for registering the handler
     */
    @Override
    public String getName() {
        return "rest_handler_infino";
    }

    private static final ScheduledExecutorService infinoThreadPool =
        Executors.newScheduledThreadPool(THREADPOOLSIZE, new CustomThreadFactory("InfinoPluginThread"));


    /** 
     * Get thread pool 
     * 
     * @return the thread pool to use for the requests
     */
    protected ExecutorService getInfinoThreadPool () {
        return infinoThreadPool;
    }

    /**
     * Shutdown the thread pool and futures when the plugin is stopped
     */
    public static void close() {
        // Wait for all futures to complete
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allDone.get(30, TimeUnit.SECONDS); // Adjust the timeout as needed
        } catch (Exception e) {
            logger.error("Error shutting down futures w/ HTTP client", e);
        }
        // Clear the list of futures
        futures.clear();
        infinoThreadPool.shutdown();
    }

    /** 
     * Use a privileged custom thread factory since Security Manager blocks 
     * access to thread groups.
     *
     * https://github.com/opensearch-project/OpenSearch/issues/5359
     * 
     * TODO: this is still not working without setting the following:
     * 
     * permission org.opensearch.secure_sm.ThreadPermission "modifyArbitraryThread";
     * permission java.net.URLPermission "http://localhost:3000/-", "*";
     * 
     * in the local java policy file. By setting these, we can actually
     * just use a regular thread pool. However, leaving this code
     * in use to save effort in the future as Security Manager
     * is deprecated after Java 17 and we may need this.
     */
    protected static final class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        CustomThreadFactory(String baseName) {
            namePrefix = baseName + "-";
        }

        public Thread newThread(Runnable r) {
            return AccessController.doPrivileged((PrivilegedAction<Thread>) () -> {
                Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
                if (t.isDaemon()) t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
                return t;
            });
        }
    }

    /**
     * Deletes a Lucene index if it exists.
     *
     * @param client       The NodeClient to perform the operation.
     * @param rawIndexName The raw name of the index to delete.
     */
    protected void deleteLuceneIndexIfExists(NodeClient client, String rawIndexName) {
        String indexName = "infino-" + rawIndexName;
        IndicesExistsRequest getIndexRequest = new IndicesExistsRequest(new String[]{indexName});

        logger.info("Deleting Lucene mirror index for Infino: " + indexName);

        client.admin().indices().exists(getIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndicesExistsResponse response) {
                if (response.isExists()) {
                    // Delete the index if it exists
                    client.admin().indices().delete(new DeleteIndexRequest(indexName), new ActionListener<>() {
                        @Override
                        public void onResponse(AcknowledgedResponse deleteResponse) {
                            logger.info("Successfully deleted '" + indexName + "' Lucene index on local node");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Failed to delete '" + indexName + "' Lucene index on local node", e);
                        }
                    });
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Error checking existence of '" + indexName + "' index", e);
            }
        });
    }

    /**
     * Create a Lucene index with the same name as the Infino index if it doesn't exist.
     *
     * @param client - client for the current OpenSearch node
     * @param rawIndexName - name of the index to create
     */
    protected void createLuceneIndexIfNeeded(NodeClient client, String rawIndexName) {
        String indexName = "infino-" + rawIndexName;
        IndicesExistsRequest getIndexRequest = new IndicesExistsRequest(new String[]{indexName});

        logger.info("Creating Lucene mirror index for Infino: " + indexName);

        client.admin().indices().exists(getIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndicesExistsResponse response) {
                if (!response.isExists()) {
                    // Create the index if it doesn't exist
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                    createIndexRequest.settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                    );
                    client.admin().indices().create(createIndexRequest, new ActionListener<>() {
                        @Override
                        public void onResponse(CreateIndexResponse createResponse) {
                            logger.info("Successfully created '" + indexName + "' Lucene index on local node");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Failed to create '" + indexName + "' Lucene index on local node", e);
                        }
                    });
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Error checking existence of '" + indexName + "' index", e);
            }
        });
    }


    /**
     * Handle REST routes for the /infino index.
     *
     * By explicitly listing all the possible paths, we let OpenSearch handle
     * illegal path expections rather than wait to send to Infino and translate
     * the error response for the user.
     *
     * Note that we need to explictly read wildcard parameters for the paths
     * defined here. I.e. somewhere before the handler completes we need to do
     * something like the following:
     *
     * String someVar = request.param("infinoIndex");
     *
     * etc.
     */
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/infino/{infinoIndex}/{infinoPath}"), // Search a collection
            new Route(GET, "/infino/{infinoIndex}/logs/{infinoPath}"), // Search logs on a collection
            new Route(GET, "/infino/{infinoIndex}/metrics/{infinoPath}"), // Search metrics on a collection
            new Route(GET, "/_cat/infino/{infinoIndex}"), // Get stats about a collection
            new Route(HEAD, "/infino/{infinoIndex}/{infinoPath}"), // Get specific info about a collection
            new Route(POST, "/infino/{infinoIndex}/{infinoPath}"), // Add data to a collection
            new Route(PUT, "/infino/{infinoIndex}"), // Create a collection
            new Route(DELETE, "/infino/{infinoIndex}"), // Delete a collection
            new Route(HEAD, "/infino/{infinoIndex}") // Get info about a collection
        ));
    }

    /**
     * Implement the request, creating or deleting Lucene index mirrors on the local node.
     *
     * The first half of the method (before the thread executor) is parallellized by OpenSearch's
     * REST thread pool so we can serialize in parallel. However network calls use our own
     * privileged thread factory.
     *
     * We exponentially backoff for 429, 503, and 504 responses
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for execution
     */
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        RestRequest.Method method;
        String indexName;
        InfinoSerializeRequestURI infinoSerializeRequestURI = null;
        HttpClient httpClient = getHttpClient();

        logger.info("Serializing REST request for Infino");

        // Serialize the request to a valid Infino URL
        try {
            infinoSerializeRequestURI = getInfinoSerializeRequestURI(request);
        } catch (Exception e) {
            logger.error("Error serializing REST URI for Infino: ", e);
            return null;
        }

        // set local members we can pass to the thread context
        method = infinoSerializeRequestURI.getMethod();
        indexName = infinoSerializeRequestURI.getIndexName();

        logger.info("Serialized REST request for Infino to " + infinoSerializeRequestURI.getFinalUrl());

        // Create Lucene mirror index for the Infino collection if it doesn't exist
        if (method == PUT) createLuceneIndexIfNeeded(client, infinoSerializeRequestURI.getIndexName());

        // Create the HTTP request to forward to Infino Server
        HttpRequest forwardRequest = HttpRequest.newBuilder()
            .uri(URI.create(infinoSerializeRequestURI.getFinalUrl()))
            .method(infinoSerializeRequestURI.getMethod().toString(), HttpRequest.BodyPublishers.ofString(request.content().utf8ToString()))
            .build();

        logger.info("Sending HTTP Request to Infino: " + infinoSerializeRequestURI.getFinalUrl());

        // Send request to Infino server and create a listener to handle the response.
        // Execute the HTTP request using our own thread factory.
        return channel -> {
            infinoThreadPool.execute(() -> {
                sendRequestWithBackoff(httpClient, forwardRequest, channel, client, indexName, method, 0);
            });
        };
    }

    private void sendRequestWithBackoff(HttpClient backoffHttpClient, HttpRequest request, RestChannel channel, NodeClient client, String indexName, RestRequest.Method method, int attempt) {
        if (attempt >= MAX_RETRIES) {
            logger.error("Max retries exceeded for request: " + request.uri());
            channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE, "Max retries exceeded"));
            return;
        }

        CompletableFuture<Void> future = backoffHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAccept(response -> processResponse(backoffHttpClient, response, channel, client, indexName, method, attempt, request))
            .exceptionally(e -> handleException(e, channel, client, indexName, method));

        // Add the future to the list of futures to clear, protected by a thread lock
        synchronized (futures) {
            futures.add(future);
        }
    }

    private void processResponse(HttpClient processHttpClient, HttpResponse<String> response, RestChannel channel, NodeClient client, String indexName, RestRequest.Method method, int attempt, HttpRequest request) {
        int statusCode = response.statusCode();
        if (shouldRetry(statusCode)) {
            long retryAfter = getRetryAfter(response, attempt);
            // Use schedule method to retry after a delay
            infinoThreadPool.schedule(() -> sendRequestWithBackoff(processHttpClient, request, channel, client, indexName, method, attempt + 1), retryAfter, TimeUnit.MILLISECONDS);
        } else {
            handleResponse(channel, response, client, indexName, method);
        }
    }

    private boolean shouldRetry(int statusCode) {
        return statusCode == 429 || statusCode == 503 || statusCode == 504;
    }

    private long getRetryAfter(HttpResponse<String> response, int attempt) {
        return response.headers().firstValueAsLong("Retry-After").orElse((long) Math.pow(2, attempt) * 1000L);
    }

    private Void handleException(Throwable e, RestChannel channel, NodeClient client, String indexName, RestRequest.Method method) {
        logger.error("Error in async HTTP call", e);
        if (method == PUT) deleteLuceneIndexIfExists(client, indexName);
        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        return null;
    }

    private void handleResponse(RestChannel channel, HttpResponse<String> response, NodeClient client, String indexName, RestRequest.Method method) {
        if (Thread.currentThread().isInterrupted()) {
            if (method == PUT) deleteLuceneIndexIfExists(client, indexName);
            logger.debug("Infino Plugin Rest handler thread interrupted. Exiting...");
            return;
        }
        try {
            int statusCode = response.statusCode();  // Get the status code as an integer
            RestStatus restStatus = RestStatus.fromCode(statusCode);  // Convert to RestStatus
            logger.info("Received HTTP response from Infino: " + response.body().toString());

            // Send the response back to the OpenSearch Rest Channel
            channel.sendResponse(new BytesRestResponse(restStatus, response.body()));

            // If we successfully delete an Infino collection, delete the mirror index
            if (method == DELETE && restStatus == RestStatus.OK) {
                deleteLuceneIndexIfExists(client, indexName);
            }
        } catch (Exception e) {
            logger.error("Error sending response", e);
            if (method == PUT) deleteLuceneIndexIfExists(client, indexName);

            // Send an internal server error response back to the OpenSearch Rest Channel
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }
};
