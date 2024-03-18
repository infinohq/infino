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
import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestRequest;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Serialize OpenSearch Infino REST request to an Infino URL.
 * 1. Search window defaults to the past 7 days if not specified by the request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with
 * '/infino/'.
 * 3. If the specified index does not exist in OpenSearch, create it before
 * sending to Infino.
 */
public class InfinoSerializeActionRequestURI {

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
    public <Request extends ActionRequest, Response extends ActionResponse> InfinoSerializeActionRequestURI(
            Request request) throws IllegalArgumentException {
        if (getIndexName().startsWith("metrics-")) {
            setIndexType(InfinoIndexType.METRICS);
        }
        if (indexName == null || indexName.isEmpty()) {
            throw new IllegalArgumentException("Index name must be specified");
        }
        if (method != PUT && method != DELETE) {
            throw new IllegalArgumentException("Request path cannot be null");
        }
        infinoEndpoint = getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint);
        finalUrl = switch (method) {
            case PUT, DELETE -> infinoEndpoint + "/:" + indexName;
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        };
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

    private void setIndexType(InfinoIndexType type) {
        this.indexType = type;
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
}