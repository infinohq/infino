/**
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

package org.opensearch.infino;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.TransportRequest;

import static org.opensearch.rest.RestRequest.Method.*;

/**
 * Serialize OpenSearch Infino REST request to an Infino URL.
 * 1. Search window defaults to the past 7 days if not specified by the request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with
 * '/infino/'.
 * 3. If the specified index does not exist in OpenSearch, create it before
 * sending to Infino.
 */
public class InfinoSerializeTransportRequest {

    /** Path prefix. E.g. /infino/logs */
    protected String prefix;

    /** Default time range for Infino searches is 7 days */
    protected static int DEFAULT_SEARCH_TIME_RANGE = 7;

    /** End time for search queries */
    protected String endTime = Instant.now().toString();;

    /** Final URL to be sent to Infino */
    protected String finalUrl;

    /** Name of the Infino index */
    protected String indexName;

    /** Type of index. E.g. LOGS or METRICS */
    protected InfinoIndexType indexType = InfinoIndexType.LOGS;

    /** The Infino endpoint */
    protected String infinoEndpoint = System.getenv("INFINO_SERVER_URL");

    /** The REST method for the request */
    protected RestRequest.Method method;

    /** Request parameters. E.g. ?start_time="123" */
    protected Map<String, String> params;

    /** Path postfix. E.g. /_search */
    protected String path;

    /** Start time for search queries */
    protected String startTime = Instant.now().minus(DEFAULT_SEARCH_TIME_RANGE, ChronoUnit.DAYS).toString();;

    /** Body of an Infino request */
    protected BytesReference body;

    /** Operation to perform on Infino */
    protected InfinoOperation operation;

    private static final Logger logger = LogManager.getLogger(InfinoSerializeRequestURI.class);
    private static final String defaultInfinoEndpoint = "http://localhost:3000";

    /**
     * Constructor
     *
     * Takes a request and serializes to the protected member, finalUrl.
     *
     * @param request - the request to be serialized.
     */
    public InfinoSerializeTransportRequest(TransportRequest request) throws IOException {
        if (request instanceof IndexRequest) {
            IndexRequest indexRequest = (IndexRequest) request;
            parseRequest(indexRequest);
            constructInfinoRequestURI();
        } else if (request instanceof SearchRequest) {
            SearchRequest searchRequest = (SearchRequest) request;
            parseRequest(searchRequest);
            constructInfinoRequestURI();
        } else if (request instanceof CreateIndexRequest) {
            CreateIndexRequest createIndexRequest = (CreateIndexRequest) request;
            parseRequest(createIndexRequest);
            constructInfinoRequestURI();
        } else if (request instanceof DeleteIndexRequest) {
            DeleteIndexRequest deleteIndexRequest = (DeleteIndexRequest) request;
            parseRequest(deleteIndexRequest);
            constructInfinoRequestURI();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported transport request type: " + request.getClass().getSimpleName());
        }
    }

    private void parseRequest(SearchRequest searchRequest) throws IOException {
        setIndexName(searchRequest.indices()[0]);
        setEndpoint(getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint));
        setOperation(InfinoOperation.SEARCH_DOCUMENTS);
        setMethod(GET);

        try {
            setSearchBody(searchRequest);

            if (getIndexName().startsWith("metrics-")) {
                setIndexType(InfinoIndexType.METRICS);
            }
        } catch (IOException e) {
            throw new IOException("Failed to serialize SearchSourceBuilder to JSON", e);
        }
    }

    private void parseRequest(IndexRequest indexRequest) throws IOException {
        setEndpoint(getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint));
        setIndexName(indexRequest.indices()[0]);
        setOperation(InfinoOperation.INDEX_DOCUMENTS);
        setMethod(POST);

        try {
            setIndexBody(indexRequest);

            if (this.getIndexName().startsWith("metrics-")) {
                setIndexType(InfinoIndexType.METRICS);
            }
        } catch (IOException e) {
            throw new IOException("Failed to serialize SearchSourceBuilder to JSON", e);
        }
    }

    private void parseRequest(CreateIndexRequest createIndexRequest) throws IOException {
        setEndpoint(getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint));
        setIndexName(createIndexRequest.indices()[0]);
        setOperation(InfinoOperation.CREATE_INDEX);
        setMethod(PUT);
        try {
            setCreateIndexBody(createIndexRequest);

            if (this.getIndexName().startsWith("metrics-")) {
                setIndexType(InfinoIndexType.METRICS);
            }

        } catch (IOException e) {
            throw new IOException("Failed to serialize SearchSourceBuilder to JSON", e);
        }
    }

    private void parseRequest(DeleteIndexRequest deleteIndexRequest) throws IOException {
        setEndpoint(getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint));
        setOperation(InfinoOperation.DELETE_INDEX);
        setMethod(PUT);
        setIndexName(deleteIndexRequest.indices()[0]);

        try {
            setDeleteIndexBody(deleteIndexRequest);
        } catch (IOException e) {
            throw new IOException("Failed to serialize SearchSourceBuilder to JSON", e);
        }
    }

    /**
     * Gets the final URL.
     *
     * @return the final URL
     */
    protected String getFinalUrl() {
        return this.finalUrl;
    }

    /**
     * Gets the index type.
     *
     * @return the index type
     */
    protected InfinoIndexType getIndexType() {
        return this.indexType;
    }

    /**
     * Sets the index type.
     *
     * @return void
     */
    protected void setIndexType(InfinoIndexType indexType) {
        this.indexType = indexType;
    }

    /**
     * Gets the HTTP method.
     *
     * @return the method.
     */
    protected RestRequest.Method getMethod() {
        return this.method;
    }

    /**
     * Sets the index name.
     *
     * @return void
     */
    protected void setMethod(RestRequest.Method method) {
        this.method = method;
    }

    /**
     * Gets the Infino operation.
     *
     * @return the operation.
     */
    protected InfinoOperation getOperation() {
        return this.operation;
    }

    /**
     * Sets the Infino operation.
     *
     * @return void
     */
    protected void setOperation(InfinoOperation operation) {
        this.operation = operation;
    }

    /**
     * Gets the request body for Index requests.
     *
     * @return the body.
     */
    protected BytesReference getBody() {
        return this.body;
    }

    /**
     * Sets the request body for Search requests.
     *
     * @return void
     */
    protected void setSearchBody(SearchRequest searchRequest) throws IOException {
        SearchSourceBuilder searchSourceBuilder = searchRequest.source();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            this.body = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new IOException("Failed to serialize SearchSourceBuilder to JSON", e);
        }
    }

    /**
     * Sets the request body for Index requests.
     *
     * @return void
     */
    protected void setIndexBody(IndexRequest indexRequest) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.value(indexRequest.sourceAsMap());
            this.body = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new IOException("Failed to serialize IndexRequest to JSON", e);
        }
    }

    /**
     * Sets the request body for Create Index requests.
     *
     * @return void
     */
    protected void setCreateIndexBody(CreateIndexRequest createIndexRequest) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            // Serializing settings
            if (!createIndexRequest.settings().isEmpty()) {
                builder.startObject("settings");
                createIndexRequest.settings().toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }

            // Serializing mappings
            if (createIndexRequest.mappings() != null) {
                builder.startObject("mappings");
                builder.rawField("properties",
                        new ByteArrayInputStream(createIndexRequest.mappings().getBytes(StandardCharsets.UTF_8)));
                builder.endObject();
            }

            // Optionally serializing aliases, if needed
            // For example purposes, not included here. Follow similar pattern as
            // settings/mappings if required.

            builder.endObject(); // Close the start object
            this.body = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new IOException("Failed to serialize CreateIndexRequest to JSON", e);
        }
    }

    protected void setDeleteIndexBody(DeleteIndexRequest deleteIndexRequest) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            builder.endObject();
            this.body = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new IOException("Failed to serialize DeleteIndexRequest to JSON", e);
        }
    }

    /**
     * Gets the index type.
     *
     * @return the index type
     */
    protected String getIndexName() {
        return this.indexName;
    }

    /**
     * Sets the index name.
     *
     * @return void
     */
    protected void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Sets the Infino endpoint.
     *
     * @return void
     */
    protected void setEndpoint(String infinoEndpoint) {
        this.infinoEndpoint = infinoEndpoint;
    }

    // Helper function to construct Infino URL
    private void constructInfinoRequestURI() {
        finalUrl = switch (method) {
            case GET -> constructGetUrl();
            case POST -> constructPostUrl();
            case PUT, DELETE -> constructPutDeleteUrl();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        };

        validateFinalUrl();
    }

    private void validateFinalUrl() {
        if (finalUrl == null || finalUrl.isEmpty()) {
            throw new IllegalArgumentException("Final URL construction failed for: " + method + " " + indexName);
        }
    }

    private String constructGetUrl() {
        System.out.println("Params = " + startTime + " " + endTime);

        switch (operation) {
            case SEARCH_DOCUMENTS:
                return switch (indexType) {
                    case METRICS -> infinoEndpoint + "/" + indexName + "/search_metrics?" + buildQueryString(
                            "start_time", startTime,
                            "end_time", endTime);
                    default -> infinoEndpoint + "/" + indexName + "/search_logs?" + buildQueryString(
                            "start_time", startTime,
                            "end_time", endTime);
                };
            case SUMMARIZE:
                return infinoEndpoint + "/" + indexName + "/summarize?" + buildQueryString(
                        "start_time", startTime,
                        "end_time", endTime);
            default:
                throw new IllegalArgumentException("Unsupported GET path: " + path);
        }
    }

    private String constructPostUrl() {
        return switch (indexType) {
            case LOGS -> infinoEndpoint + "/" + indexName + "/append_log";
            case METRICS -> infinoEndpoint + "/" + indexName + "/append_metric";
            default -> throw new IllegalArgumentException("Unsupported index type for POST: " + indexType);
        };
    }

    private String constructPutDeleteUrl() {
        return infinoEndpoint + "/:" + indexName;
    }

    /**
     * Retrieves the value of the specified environment variable or returns the
     * default value if the environment variable is not set.
     *
     * @param name         The name of the environment variable.
     * @param defaultValue The default value to return if the environment variable
     *                     is not set.
     * @return The value of the environment variable or the default value.
     */
    public static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            logger.info("Environment variable " + name + " is not set. Using default value: " + defaultValue);
            return defaultValue;
        }
        return value;
    }

    /**
     * The type of index in Infino. Infino has a different index for each telemetry
     * data type: logs, metrics, and traces (traces are not yet supported as of Dec
     * 2023).
     */
    public enum InfinoIndexType {
        /** Undefined type. */
        UNDEFINED,

        /** Logs type. */
        LOGS,

        /** Metrics type. */
        METRICS
    }

    /**
     * The type of index in Infino. Infino has a different index for each telemetry
     * data
     * type: logs, metrics, and traces (traces are not yet supported as of Dec
     * 2023).
     */
    public enum InfinoOperation {
        /** Search the index. */
        SEARCH_DOCUMENTS,

        /** Index one or more documents. */
        INDEX_DOCUMENTS,

        /** Delete one or more documents. */
        DELETE_DOCUMENTS,

        /** Create an index. */
        CREATE_INDEX,

        /** Delete an index. */
        DELETE_INDEX,

        /** Ping Infino. */
        PING,

        /** Summarize results */
        SUMMARIZE,
    }

    // Helper method to build query strings
    private String buildQueryString(String... params) {
        StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < params.length; i += 2) {
            if (i > 0) {
                queryBuilder.append("&");
            }
            queryBuilder.append(params[i]).append("=").append(encodeParam(params[i + 1]));
        }
        return queryBuilder.toString();
    }

    // Helper method to encode URL parameters
    private String encodeParam(String param) {
        try {
            return URLEncoder.encode(param, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.error("Error encoding parameter: " + param, e);
            return param;
        }
    }
}
