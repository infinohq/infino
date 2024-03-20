/**
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

package org.opensearch.infino;

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

    /** Final URL to be sent to Infino */
    protected String finalUrl;

    /** The Infino endpoint */
    protected static String infinoEndpoint = System.getenv("INFINO_SERVER_URL");

    private static final Logger logger = LogManager.getLogger(InfinoSerializeRequestURI.class);
    private static final String defaultInfinoEndpoint = "http://localhost:3000";

    /**
     * Constructor
     *
     * Takes a request and serializes to the protected member, finalUrl.
     * 
     * @param method     - the method for the request
     * @param indexName  - the index for the request
     * @param <Request>  - the request type
     * @param <Response> - the response type
     */
    public <Request extends ActionRequest, Response extends ActionResponse> InfinoSerializeActionRequestURI(
            RestRequest.Method method, String indexName) throws IllegalArgumentException {
        if (indexName == null || indexName.isEmpty()) {
            throw new IllegalArgumentException("Index name must be specified");
        }
        if (method != PUT && method != DELETE) {
            throw new IllegalArgumentException(
                    "Only PUT for index creation and DELETE for index deletion is supported");
        }
        infinoEndpoint = getEnvVariable("INFINO_SERVER_URL", defaultInfinoEndpoint);
        finalUrl = switch (method) {
            case PUT, DELETE -> infinoEndpoint + "/" + indexName;
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
}
