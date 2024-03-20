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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

import static org.opensearch.rest.RestRequest.Method.*;

/**
 * Extend the OpenSearch API.
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
public class InfinoRestHandler extends BaseRestHandler {

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Logger logger = LogManager.getLogger(InfinoRestHandler.class);
    // private static InfinoPluginUtils utils = new InfinoPluginUtils();

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

    /**
     * Get the HTTP Client
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

    /**
     * Handle REST routes for the /infino index.
     *
     * Extend
     *
     * etc.
     */
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_ping"),
                new Route(GET, "/infino/{infinoIndex}/_summarize")));
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
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and
     *                     preparing for execution
     */
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        RestRequest.Method method;
        String indexName;
        InfinoSerializeRequestURI infinoSerializeRequestURI = null;
        HttpClient httpClient = getHttpClient();

        logger.info("Infino REST Handler: Serializing REST request for Infino");

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

        logger.info("Infino REST Handler: Serialized REST request for Infino to "
                + infinoSerializeRequestURI.getFinalUrl());

        // Create Lucene mirror index for the Infino collection if it doesn't exist
        if (method == PUT)
            InfinoPluginUtils.createLuceneIndexIfNeeded(client, infinoSerializeRequestURI.getIndexName());

        // Create the HTTP request to forward to Infino Server
        HttpRequest forwardRequest = HttpRequest.newBuilder().uri(URI.create(infinoSerializeRequestURI.getFinalUrl()))
                .method(infinoSerializeRequestURI.getMethod().toString(),
                        HttpRequest.BodyPublishers.ofString(request.content().utf8ToString()))
                .build();

        logger.info("Infino REST Handler: Sending HTTP Request to Infino: " + infinoSerializeRequestURI.getFinalUrl());

        // Send request to Infino server and create a listener to handle the response.
        // Execute the HTTP request using our own thread factory.
        return channel -> {
            InfinoPluginUtils.infinoThreadPool.execute(() -> {
                sendRequestWithBackoff(httpClient, forwardRequest, channel, client, indexName, method, 0);
            });
        };
    }

    private void sendRequestWithBackoff(HttpClient backoffHttpClient, HttpRequest request, RestChannel channel,
            NodeClient client, String indexName, RestRequest.Method method, int attempt) {
        if (attempt >= InfinoPluginUtils.MAX_RETRIES) {
            BytesRestResponse response = InfinoPluginUtils
                    .createBytesRestResponse(InfinoPluginUtils.getRestStatusFromCode(503), "Max retries exceeded");
            channel.sendResponse(response);
            return;
        }

        try {
            HttpResponse<String> response = backoffHttpClient.send(request, HttpResponse.BodyHandlers.ofString());
            processResponse(backoffHttpClient, response, channel, client, indexName, method, attempt, request);
        } catch (Exception e) {
            handleException(e, channel, client, indexName, method);
        }
    }

    private void processResponse(HttpClient processHttpClient, HttpResponse<String> response, RestChannel channel,
            NodeClient client, String indexName, RestRequest.Method method, int attempt, HttpRequest request) {
        int statusCode = response.statusCode();
        if (InfinoPluginUtils.shouldRetry(statusCode)) {
            long retryAfter = InfinoPluginUtils.getRetryAfter(response, attempt);
            InfinoPluginUtils.infinoThreadPool
                    .schedule(() -> sendRequestWithBackoff(processHttpClient, request, channel, client,
                            indexName, method, attempt + 1), retryAfter, TimeUnit.MILLISECONDS);
        } else {
            handleResponse(channel, response, client, indexName, method);
        }
    }

    private Void handleException(Throwable e, RestChannel channel, NodeClient client, String indexName,
            RestRequest.Method method) {
        logger.error("Error in async HTTP call to Infino from REST Handler", e);
        try {
            // e.getMessage() sometimes returns null for ConnectException.
            // Using e.toString() when getMessage() returns null.
            String errMsg = e.getMessage() != null ? e.getMessage() : e.toString();
            Object restStatusInternalServerError = InfinoPluginUtils.getRestStatusFromCode(500);
            BytesRestResponse errorResponse = InfinoPluginUtils.createBytesRestResponse(restStatusInternalServerError,
                    errMsg);
            channel.sendResponse(errorResponse);
        } catch (Exception ex) {
            logger.error("Failed to send response using reflection", ex);
        }
        return null;
    }

    private void handleResponse(RestChannel channel, HttpResponse<String> response, NodeClient client, String indexName,
            RestRequest.Method method) {
        if (Thread.currentThread().isInterrupted()) {
            logger.debug("Infino Plugin Rest handler thread interrupted. Exiting...");
            return;
        }

        try {
            int statusCode = response.statusCode();
            // Get RestStatus using reflection
            Object restStatusObject = InfinoPluginUtils.getRestStatusFromCode(statusCode);

            // Create a new BytesRestResponse instance using the utility method
            BytesRestResponse bytesRestResponse = InfinoPluginUtils.createBytesRestResponse(restStatusObject,
                    response.body());

            // Send the response back to the OpenSearch Rest Channel
            channel.sendResponse(bytesRestResponse);
        } catch (Exception e) {
            logger.error("Error sending response to REST caller", e);

            // Fall back to sending internal server error
            try {
                BytesRestResponse errorResponse = InfinoPluginUtils.createBytesRestResponse(
                        InfinoPluginUtils.getRestStatusFromCode(500),
                        "Internal server error: " + e.getMessage());
                channel.sendResponse(errorResponse);
            } catch (Exception ex) {
                logger.error("Failed to send error response", ex);
                // Handle this exception or rethrow as needed
            }
        }
    }
};
