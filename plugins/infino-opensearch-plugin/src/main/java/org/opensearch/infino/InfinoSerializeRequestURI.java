/**
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

package org.opensearch.infino;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.rest.RestRequest;

import static org.opensearch.rest.RestRequest.Method.*;

/**
 * Serialize OpenSearch Infino REST request to an Infino URL.
 * 1. Search window defaults to the past 7 days if not specified by the request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with
 * '/infino/'.
 * 3. If the specified index does not exist in OpenSearch, create it before
 * sending to Infino.
 */
public class InfinoSerializeRequestURI {

    /** Path prefix. E.g. /infino/logs */
    protected String prefix;

    /** Default time range for Infino searches is 7 days */
    protected static int DEFAULT_SEARCH_TIME_RANGE = 7;

    /** End time for search queries */
    protected String endTime;

    /** Final URL to be sent to Infino */
    protected String finalUrl;

    /** Name of the Infino index */
    protected String indexName;

    /** Type of index. E.g. LOGS or METRICS */
    protected InfinoIndexType indexType;

    /** The Infino endpoint */
    protected static String infinoEndpoint = System.getenv("INFINO_SERVER_URL");

    /** The REST method for the request */
    protected RestRequest.Method method;

    /** Request parameters. E.g. ?start_time="123" */
    protected Map<String, String> params;

    /** Path postfix. E.g. /_search */
    protected String path;

    /** Start time for search queries */
    protected String startTime;

    private static final Logger logger = LogManager.getLogger(InfinoSerializeRequestURI.class);
    private static final String defaultInfinoEndpoint = "http://localhost:3000";

    /**
     * Constructor
     *
     * Takes a request and serializes to the protected member, finalUrl.
     *
     * @param request - the request to be serialized.
     */
    public InfinoSerializeRequestURI(RestRequest request) throws IllegalArgumentException {
        parseRequest(request);
        setDefaultTimeRange();
        constructInfinoRequestURI();
    }

    private void parseRequest(RestRequest request) {
        params = getInfinoParams(request);
        method = request.method();
        indexName = params.get("infinoIndex");
        validateIndexName();
        determineEndpoint();
        extractPathAndIndexType(params.get("infinoPath"));
    }

    private void determineEndpoint() {
        infinoEndpoint = getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint);
    }

    private void validateIndexName() {
        if (indexName == null || indexName.isEmpty()) {
            throw new IllegalArgumentException("Index name must be specified");
        }
    }

    private void extractPathAndIndexType(String requestPath) {
        if (requestPath != null) {
            int lastSlash = requestPath.lastIndexOf("/");
            prefix = getPrefix(lastSlash, requestPath);
            path = requestPath.substring(lastSlash + 1);
            indexType = determineIndexType(requestPath, lastSlash);
        } else if (method != PUT && method != DELETE) {
            throw new IllegalArgumentException("Request path cannot be null");
        }
    }

    private InfinoIndexType determineIndexType(String requestPath, int lastSlash) {
        String indexSegment = getPrefix(lastSlash, requestPath);
        if (indexSegment == null) {
            return InfinoIndexType.UNDEFINED; // Or a special type if necessary
        }
        return switch (indexSegment) {
            case "logs" -> InfinoIndexType.LOGS;
            case "metrics" -> InfinoIndexType.METRICS;
            default -> InfinoIndexType.UNDEFINED;
        };
    }

    // Helper function to construct Infino URL
    private void constructInfinoRequestURI() {
        // Implement logic based on the 'method' and 'indexType'
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

    // TODO: Generate a proper error message to the caller for incorrect parameters
    private String constructGetUrl() {
        String baseUrl = infinoEndpoint + "/" + indexName;
        switch (path) {
            case "_ping":
                return infinoEndpoint + "/ping";
            case "_search":
                if (indexType == InfinoIndexType.METRICS) {
                    return baseUrl + "/search_metrics?" + buildQueryString(
                            "name", params.get("name"),
                            "value", params.get("value"),
                            "start_time", startTime,
                            "end_time", endTime);
                } else {
                    return baseUrl + "/search_logs?" + buildQueryString(
                            "q", params.get("q"),
                            "start_time", startTime,
                            "end_time", endTime);
                }
            case "_summarize":
                return baseUrl + "/summarize?" + buildQueryString(
                        "q", params.get("q"),
                        "start_time", startTime,
                        "end_time", endTime);
            default:
                throw new IllegalArgumentException("Unsupported GET path: " + path);
        }
    }

    private String constructPostUrl() {
        // Constructing URL for POST requests
        return switch (indexType) {
            case LOGS -> infinoEndpoint + "/" + indexName + "/append_log";
            case METRICS -> infinoEndpoint + "/" + indexName + "/append_metric";
            default -> throw new IllegalArgumentException("Unsupported index type for POST: " + indexType);
        };
    }

    private String constructPutDeleteUrl() {
        // Constructing URL for PUT and DELETE requests
        return infinoEndpoint + "/:" + indexName;
    }

    // Set the default time range for searches
    private void setDefaultTimeRange() {
        if (method == RestRequest.Method.GET) {

            // Get current time
            Instant now = Instant.now();

            // get time range arguments
            startTime = params.get("startTime");
            endTime = params.get("endTime");

            // Default start_time to 30 days before now if not provided
            if (startTime == null || startTime.isEmpty()) {
                startTime = now.minus(DEFAULT_SEARCH_TIME_RANGE, ChronoUnit.DAYS).toString();
            }

            // Default end_time to now if not provided
            if (endTime == null || endTime.isEmpty()) {
                endTime = now.toString();
            }
        }
    }

    // Helper method to get the prefix from the path
    private String getPrefix(int element, String requestPath) {
        // Check if the lastIndex is -1 (indicating "/" was not found)
        if (element == -1) {
            return null;
        } else {
            return requestPath.substring(0, element);
        }
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
     * Gets the final URL.
     *
     * @return the final URL
     */
    protected String getFinalUrl() {
        return this.finalUrl;
    }

    /**
     * Gets the index name.
     *
     * @return the index name
     */
    protected RestRequest.Method getMethod() {
        return this.method;
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
     * Gets the method.
     *
     * @return the method
     */
    protected InfinoIndexType getIndexType() {
        return this.indexType;
    }

    /**
     * Consumes all the request parameters after the "?" in the URI
     * otherwise the Rest handler will fail. We also need to explictly
     * read wildcard parameters for the paths defined by routes() in
     * the Infino Rest handler as they are not consumed by params()
     * in the RestRequest class.
     *
     * @param request the request to execute
     * @return a string hashmap of the request parameters
     */
    protected Map<String, String> getInfinoParams(RestRequest request) {

        // Initialize a new HashMap to store the parameters
        Map<String, String> requestParamsMap = new HashMap<>();

        // Iterate over the request parameters and add them to the map
        request.params().forEach((key, value) -> {
            String value_holder = request.param(key, "");
            requestParamsMap.put(key, value_holder);
            logger.debug("Query parameters from OpenSearch to Infino: " + key + " is " + value);
        });

        requestParamsMap.put("infinoIndex", request.param("infinoIndex"));
        requestParamsMap.put("infinoPath", request.param("infinoPath"));

        return requestParamsMap;
    }

    /**
     * The type of index in Infino. Infino has a different index for each telemetry
     * data
     * type: logs, metrics, and traces (traces are not yet supported as of Dec
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

    // Helper method to build query strings
    private String buildQueryString(String... params) {

        // Initialize a new StringBuilder for the query string
        StringBuilder queryString = new StringBuilder();
        if (params != null && params.length % 2 == 0) { // Ensure there's a key for every value
            for (int i = 0; i < params.length; i += 2) {
                String key = params[i];
                String value = params[i + 1];
                if (value != null && !value.isEmpty()) { // Check if the value is not null and not empty
                    if (queryString.length() > 0) {
                        queryString.append("&");
                    }
                    queryString.append(key).append("=").append(encodeParam(value));
                }
            }
        }
        return queryString.toString();
    }

    // Helper method to encode URL parameters
    private String encodeParam(String param) {
        if (param == null) {
            logger.warn("Attempted to encode a null parameter.");
            return "";
        }
        try {
            return URLEncoder.encode(param, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.error("Error encoding parameter: " + param, e);
            return param;
        }
    }
}
