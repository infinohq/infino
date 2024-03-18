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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.infino.InfinoSerializeTransportRequest.InfinoOperation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.NoSuchMethodException;
import java.lang.InstantiationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse.Result;

import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.BulkShardResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

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

    private static final int MAX_RETRIES = 3; // Maximum number of retries for exponential backoff
    private static final int THREADPOOL_SIZE = 25; // Size of threadpool we will use for Infino
    private static HttpClient httpClient;
    private static final Logger logger = LogManager.getLogger(InfinoTransportInterceptor.class);

    /**
     * Constructor
     * 
     * @param httpClient - httpClient to use to communicate with Infino.
     *                   we add here so we can mock http calls during testing
     */
    public InfinoTransportInterceptor(HttpClient httpClient) {
        InfinoTransportInterceptor.httpClient = httpClient;
    }

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
     * Get get a new instance of the serializer class. Used for unit tests.
     *
     * @param request   - the Transport request to serialize
     * @param operation - the operation associated with the request
     * @return a configured InfinoSerializeTransportRequest object
     */
    protected InfinoSerializeTransportRequest getInfinoSerializeTransportRequest(BulkShardRequest request,
            InfinoOperation operation) {
        logger.info("Initializing Transport Registration.");

        try {
            logger.debug("Loading Transport Interceptor.");
            return new InfinoSerializeTransportRequest(request, operation);
        } catch (IOException e) {
            logger.error("Error serializing REST URI for Infino: ", e);
        }
        return null;
    }

    /**
     * Get get a new instance of the serializer class. Used for unit tests.
     *
     * @param request   - the Transport request to serialize
     * @param operation - the operation associated with the request
     * @return a configured InfinoSerializeTransportRequest object
     */
    protected InfinoSerializeTransportRequest getInfinoSerializeTransportRequest(ShardSearchRequest request,
            InfinoOperation operation) {
        logger.info("Initializing Transport Registration.");

        try {
            logger.debug("Loading Transport Interceptor.");
            return new InfinoSerializeTransportRequest(request, operation);
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
     * Intercept the transport requests
     * 
     * @param action         - the action to be executed
     * @param executor       - the executor for this request
     * @param forceExecution - ignored
     * @param actualHandler  - the original transport handler
     */
    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action,
            String executor,
            boolean forceExecution,
            TransportRequestHandler<T> actualHandler) {

        logger.info("Executing Infino Transport Handler with action " + action + " and executor " + executor);

        return new TransportRequestHandler<T>() {
            @Override
            public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
                logger.debug("-----------------Received request and task--------------- " + request.toString() + " --- "
                        + task.toString());
                String action = task.getAction();
                if (shouldBeIntercepted(action)) {
                    logger.debug("-----------------Intercepted request and task--------------- " + request.toString()
                            + " --- " + action);
                    InfinoOperation operation;

                    if ("indices:data/write/bulk[s][p]".equals(action)) {
                        operation = InfinoOperation.BULK_DOCUMENTS;
                    } else if ("indices:data/read/search[phase/query]".equals(action)) {
                        operation = InfinoOperation.SEARCH_DOCUMENTS;
                    } else {
                        throw new IllegalArgumentException("Unsupported action: " + task.getAction());
                    }

                    processTransportActions(request, operation, new ActionListener<TransportResponse>() {
                        @Override
                        public void onResponse(TransportResponse response) {
                            logger.info("TransportResponse: " + response);

                            try {
                                channel.sendResponse(response);
                            } catch (IOException e) {
                                logger.error("Failed to send response", e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                logger.error("Transport action failed.");
                                channel.sendResponse(e);
                            } catch (IOException ex) {
                                logger.error("Failed to send error response", ex);
                            }
                        }
                    });
                } else {
                    actualHandler.messageReceived(request, channel, task);
                }
            }
        };

    }

    private boolean shouldBeIntercepted(String action) {
        if (("indices:data/read/search[phase/query]".equals(action))
                || ("indices:data/write/bulk[s][p]".equals(action))) {
            return true;
        }

        return false;
    }

    /**
     *
     * The first half of the method (before the thread executor) is parallellized by
     * OpenSearch's thread pool so we can serialize in parallel. However network
     * calls use
     * our own privileged thread factory.
     *
     * We exponentially backoff for 429, 503, and 504 responses
     * 
     * @param request   to execute
     * @param operation the operation associated with the request
     * @param listener  the listener for executing actions
     */
    public void processTransportActions(TransportRequest request, InfinoOperation operation,
            ActionListener<TransportResponse> listener) {

        InfinoSerializeTransportRequest infinoRequest = null;
        HttpClient httpClient = getHttpClient();
        HttpRequest forwardRequest;

        logger.info("Transport Interceptor: Serializing REST request to Infino");

        // Serialize the request to a valid Infino URL
        try {
            logger.debug("-----------------Sending request for serialization--------------- " + request.toString());
            switch (operation) {
                case BULK_DOCUMENTS: {
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    ConcreteShardRequest<BulkShardRequest> concreteRequest = (ConcreteShardRequest) request;
                    BulkShardRequest indexRequest = concreteRequest.getRequest();
                    infinoRequest = new InfinoSerializeTransportRequest(indexRequest, operation);
                    break;
                }
                case SEARCH_DOCUMENTS: {
                    ShardSearchRequest searchRequest = (ShardSearchRequest) request;
                    infinoRequest = new InfinoSerializeTransportRequest(searchRequest, operation);
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unsupported operation: " + operation);
            }
        } catch (Exception e) {
            logger.info("Error serializing REST URI for Infino: ", e);
            listener.onFailure(e);
            return;
        }

        // Set local members we can pass to the thread context
        RestRequest.Method method = infinoRequest.getMethod();
        String indexName = infinoRequest.getIndexName();
        BytesReference body = infinoRequest.getBody();
        String url = infinoRequest.getFinalUrl();

        logger.info("Serialized TRANSPORT request for Infino to " + infinoRequest.getFinalUrl());

        // Serialize the request to a valid Infino URL
        try {
            logger.debug("Building HTTP Request for Infino: method = " + method + " indexName: " + indexName
                    + " operation: " + operation + " body: " + body.utf8ToString());

            forwardRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .method(method.toString(),
                            HttpRequest.BodyPublishers.ofString(body.utf8ToString()))
                    .build();
        } catch (Exception e) {
            logger.info("Error building HTTP Request for Infino: ", e);
            listener.onFailure(e);
            return;
        }

        logger.info("Transport Interceptor: Sending HTTP Request to Infino: " + infinoRequest.getFinalUrl());

        // Send request to Infino server and create a listener to handle the response.
        // Execute the HTTP request using our own thread factory.

        infinoThreadPool.execute(() -> {
            sendRequestWithBackoff(httpClient, forwardRequest, listener, indexName, method, operation, 0,
                    request);
        });
    }

    private void sendRequestWithBackoff(HttpClient backoffHttpClient, HttpRequest request,
            ActionListener<TransportResponse> listener, String indexName, RestRequest.Method method,
            InfinoOperation operation, int attempt, TransportRequest transportRequest) {
        if (attempt >= MAX_RETRIES) {
            String errorMessage = "Max retries exceeded for " + operation + " on " + indexName;
            logger.info(errorMessage);
            Exception e = new Exception(errorMessage);
            listener.onFailure(e);
            return;
        }

        try {
            HttpResponse<String> response = backoffHttpClient.send(request, HttpResponse.BodyHandlers.ofString());
            processResponse(backoffHttpClient, response, listener, indexName, method, operation, attempt, request,
                    transportRequest);
        } catch (Exception e) {
            handleException(e, listener, indexName, operation);
        }
    }

    private void processResponse(HttpClient processHttpClient, HttpResponse<String> response,
            ActionListener<TransportResponse> listener, String indexName, RestRequest.Method method,
            InfinoOperation operation, int attempt, HttpRequest request, TransportRequest transportRequest) {
        int statusCode = response.statusCode();
        if (shouldRetry(statusCode)) {
            long retryAfter = getRetryAfter(response, attempt);
            infinoThreadPool.schedule(() -> sendRequestWithBackoff(processHttpClient, request, listener,
                    indexName, method, operation, attempt + 1, transportRequest), retryAfter, TimeUnit.MILLISECONDS);
        } else {
            handleResponse(response, listener, indexName, operation, transportRequest);
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
            logger.error("Infino request failed", e);
            Exception ex = new Exception(e);
            listener.onFailure(ex);
        } catch (Exception ex) {
            logger.error("Failed to send response using reflection", ex);
            listener.onFailure(ex);
        }
        return null;
    }

    private void handleResponse(HttpResponse<String> response,
            ActionListener<TransportResponse> listener,
            String indexName,
            InfinoOperation operation,
            TransportRequest transportRequest) {

        if (Thread.currentThread().isInterrupted()) {
            logger.warn("Infino Plugin Rest handler thread interrupted. Exiting...");
            Exception e = new Exception("Infino request thread was interrupted.");
            listener.onFailure(e);
            return;
        }

        try {
            String responseBody = response.body();
            int responseCode = response.statusCode();

            logger.debug("Response from Infino is " + responseBody);

            if (responseCode >= 400 && responseCode <= 499) {
                throw new IllegalAccessException("Malformed request returned from Infino.");
            } else if (responseCode >= 500 && responseCode <= 599) {
                throw new IOException("Server error returned from Infino.");
            }

            if (responseBody == null || responseBody.isEmpty()) {
                throw new IOException("Infino response body is empty.");
            }

            if (operation == InfinoOperation.SEARCH_DOCUMENTS) {
                Gson searchGson = new Gson();
                JsonElement element = searchGson.fromJson(responseBody, JsonElement.class);
                if (!element.isJsonObject()) {
                    throw new JsonParseException("Expected JSON object but found different structure");
                }

                JsonObject rootObj = element.getAsJsonObject();

                JsonObject hitsObject = rootObj.getAsJsonObject("hits");
                if (hitsObject == null) {
                    throw new JsonParseException("Response JSON does not contain 'hits' object");
                }

                JsonArray hitsArray = hitsObject.getAsJsonArray("hits");
                long totalHitsValue = hitsObject.getAsJsonObject("total").get("value").getAsLong();
                float maxScore = hitsObject.get("max_score").getAsFloat();
                TotalHits totalHits = new TotalHits(totalHitsValue, TotalHits.Relation.EQUAL_TO);

                QuerySearchResult queryResult = new QuerySearchResult();

                List<SearchHit> searchHitsList = new ArrayList<>();

                ScoreDoc[] scoreDocs = new ScoreDoc[searchHitsList.size()];
                SearchHit[] searchHitsArray = searchHitsList.toArray(new SearchHit[0]);
                SearchHits searchHits = new SearchHits(searchHitsArray, totalHits, maxScore);
                TopDocs topDocs = new TopDocs(new TotalHits(totalHitsValue, TotalHits.Relation.EQUAL_TO), scoreDocs);

                int i = 0;
                for (JsonElement hitElement : hitsArray) {
                    JsonObject hitObj = hitElement.getAsJsonObject();
                    JsonObject sourceObj = hitObj.getAsJsonObject("_source");

                    String id = hitObj.get("_id").getAsString();
                    int docId = Integer.parseInt(id); // Ensure this ID parsing is what you intend, or handle exceptions

                    Map<String, DocumentField> documentFields = new HashMap<>();
                    Map<String, DocumentField> metaFields = new HashMap<>();

                    for (Map.Entry<String, JsonElement> entry : sourceObj.entrySet()) {
                        documentFields.put(entry.getKey(),
                                new DocumentField(entry.getKey(),
                                        Collections.singletonList(entry.getValue().getAsString())));
                    }

                    for (Map.Entry<String, JsonElement> entry : hitObj.entrySet()) {
                        if (!entry.getKey().equals("_source")) {
                            metaFields.put(entry.getKey(),
                                    new DocumentField(entry.getKey(),
                                            Collections.singletonList(entry.getValue().toString())));
                        }
                    }

                    if (searchHitsList.size() != 0) {
                        SearchHit searchHit = new SearchHit(docId, id, documentFields, metaFields);
                        searchHitsList.add(searchHit);
                        scoreDocs[i] = new ScoreDoc(docId, searchHit.getScore());
                    }
                    i++;
                }

                TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
                queryResult.topDocs(topDocsAndMaxScore, null); // Assuming no sortValueFormats
                queryResult.setShardSearchRequest((ShardSearchRequest) transportRequest);

                ShardSearchContextId contextId = new ShardSearchContextId("contextIdString", 1L);
                SearchShardTarget shardTarget = new SearchShardTarget("nodeId", new ShardId(indexName, "indexUuid", 1),
                        "clusterAlias", OriginalIndices.NONE);

                FetchSearchResult fetchSearchResult = new FetchSearchResult(contextId, shardTarget);
                fetchSearchResult.hits(searchHits);

                // Create QueryFetchSearchResult with both query and fetch results
                QueryFetchSearchResult queryFetchSearchResult = new QueryFetchSearchResult(queryResult,
                        fetchSearchResult);
                queryFetchSearchResult.setSearchShardTarget(shardTarget);

                logger.debug("Response to Search Request is " + queryFetchSearchResult);

                listener.onResponse(queryFetchSearchResult);
            } else if (operation == InfinoOperation.BULK_DOCUMENTS) {
                Gson bulkGson = new GsonBuilder().create();
                JsonObject rootObj = bulkGson.fromJson(responseBody, JsonObject.class);

                long took = rootObj.has("took") ? rootObj.get("took").getAsLong() : 0;
                JsonArray itemsArray = rootObj.getAsJsonArray("items");

                List<BulkItemResponse> bulkItemResponses = new ArrayList<>();

                int i = 0;
                for (JsonElement itemElement : itemsArray) {
                    JsonObject actionObject = itemElement.getAsJsonObject().entrySet().iterator().next().getValue()
                            .getAsJsonObject();
                    String actionType = itemElement.getAsJsonObject().entrySet().iterator().next().getKey();
                    OpType opType = OpType.valueOf(actionType.toUpperCase());

                    String id = actionObject.get("_id").getAsString();
                    String index = actionObject.get("_index").getAsString();
                    int status = actionObject.get("status").getAsInt();
                    RestStatus restStatus = RestStatus.fromCode(status);

                    if (actionObject.has("error")) {
                        JsonObject errorDetails = actionObject.getAsJsonObject("error");
                        String errorReason = errorDetails.get("reason").getAsString();
                        Exception cause = new Exception(errorReason);
                        Failure failure = new Failure(index, id, cause, restStatus);
                        bulkItemResponses.add(new BulkItemResponse(i, opType, failure));
                    } else {
                        long seqNo = actionObject.has("_seq_no") ? actionObject.get("_seq_no").getAsLong()
                                : SequenceNumbers.UNASSIGNED_SEQ_NO;
                        long primaryTerm = actionObject.has("_primary_term")
                                ? actionObject.get("_primary_term").getAsLong()
                                : SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
                        Result result = Result.valueOf(actionObject.get("result").getAsString().toUpperCase());

                        DocWriteResponse docWriteResponse = null;
                        switch (actionType) {
                            case "index":
                            case "create":
                                docWriteResponse = new IndexResponse(new ShardId(index, "_na_", 1), id, seqNo,
                                        primaryTerm, 1, result == Result.CREATED);
                                break;
                            case "delete":
                                docWriteResponse = new DeleteResponse(new ShardId(index, "_na_", 1), id, seqNo,
                                        primaryTerm, 1, result == Result.DELETED);
                                break;
                            case "update":
                                docWriteResponse = new UpdateResponse(new ShardId(index, "_na_", 1), id, seqNo,
                                        primaryTerm, 1, result);
                                break;
                            default:
                                logger.warn("Unexpected actionType encountered: " + actionType);
                        }
                        if (docWriteResponse != null) {
                            bulkItemResponses.add(new BulkItemResponse(i, opType, docWriteResponse));
                        }
                    }
                    i++;
                }

                BulkItemResponse[] responsesArray = bulkItemResponses.toArray(new BulkItemResponse[0]);
                BulkShardResponse bulkShardResponse = new BulkShardResponse(new ShardId(indexName, "_na_", 1),
                        responsesArray);

                listener.onResponse(bulkShardResponse);
            }

        } catch (JsonParseException | IOException e) {
            logger.error("Error parsing or retrieving Infino response", e);
            listener.onFailure(e);
        } catch (Exception e) {
            logger.error("Error handling response", e);
            listener.onFailure(e);
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
     * @param statusCode   - the RestStatus code for the response
     * @param responseBody - the message body to be sent
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
