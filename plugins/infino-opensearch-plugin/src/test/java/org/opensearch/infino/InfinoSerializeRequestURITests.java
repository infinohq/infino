/**
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

package org.opensearch.infino;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import static org.opensearch.rest.RestRequest.Method.*;

/**
 * General testing approach is:
 * 1. Mock the underlying request
 * 2. Create the request and add default parameters created by routes() in the
 * rest handler
 * 3. Ensure we process the request correctly
 *
 * Notes:
 *
 * 1. default Infino test URL is defined in build.gradle:test.
 * 2. the security manager (turned off for unit tests) will catch unsupported
 * methods (e.g. TRACE) so we don't test for that here
 */
public class InfinoSerializeRequestURITests extends OpenSearchTestCase {

    public void testRequestProcessing() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        when(mockHttpRequest.uri()).thenReturn("http://opensearch/infino/test-index/_ping");
        when(mockHttpRequest.method()).thenReturn(GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "_ping");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("test-index", infinoSerializeRequestURI.indexName);
        assertEquals("_ping", infinoSerializeRequestURI.path);
        assertEquals("GET", infinoSerializeRequestURI.method.name());
        assertEquals(InfinoSerializeRequestURI.InfinoIndexType.UNDEFINED, infinoSerializeRequestURI.indexType);
    }

    public void testLogsSearchEndpoint() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search?q=hello&start_time=123&end_time=456";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("test-index", infinoSerializeRequestURI.indexName);
        assertEquals("_search", infinoSerializeRequestURI.path);
        assertEquals(infinoSerializeRequestURI.params.get("q"), "hello");
        assertEquals(infinoSerializeRequestURI.params.get("start_time"), "123");
        assertEquals(infinoSerializeRequestURI.params.get("end_time"), "456");
        assertEquals("GET", infinoSerializeRequestURI.method.name());
        assertEquals(InfinoSerializeRequestURI.InfinoIndexType.LOGS, infinoSerializeRequestURI.indexType);
    }

    public void testMetricsSearchEndpoint() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/metrics/_search?name=cpu&value=0.8&start_time=123&end_time=456";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "metrics/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("cpu", infinoSerializeRequestURI.params.get("name"));
        assertEquals("0.8", infinoSerializeRequestURI.params.get("value"));
        assertEquals("test-index", infinoSerializeRequestURI.indexName);
        assertEquals("_search", infinoSerializeRequestURI.path);
        assertEquals(infinoSerializeRequestURI.params.get("start_time"), "123");
        assertEquals(infinoSerializeRequestURI.params.get("end_time"), "456");
        assertEquals("GET", infinoSerializeRequestURI.method.name());
        assertEquals(InfinoSerializeRequestURI.InfinoIndexType.METRICS, infinoSerializeRequestURI.indexType);
    }

    public void testInvalidRequestMethod() {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        when(mockHttpRequest.uri()).thenReturn("http://opensearch/infino/test-index/_invalid");
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.POST);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "_invalid");

        expectThrows(IllegalArgumentException.class, () -> new InfinoSerializeRequestURI(restRequest));
    }

    public void testDefaultTimeRange() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search?q=error";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertNotNull("Start time should be set to default", infinoSerializeRequestURI.startTime);
        assertNotNull("End time should be set to default", infinoSerializeRequestURI.endTime);
    }

    public void testPingEndpoint() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/_ping";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "_ping");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("Ping URL should be constructed correctly", "http://test-host:3000/ping",
                infinoSerializeRequestURI.finalUrl);
    }

    public void testInvalidPath() {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/_invalid";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "_invalid");

        expectThrows(IllegalArgumentException.class, () -> new InfinoSerializeRequestURI(restRequest));
    }

    public void testEncodeParameters() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search?q=hello world&start_time=123&end_time=456";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertTrue("URL should encode query parameters", infinoSerializeRequestURI.finalUrl.contains("hello+world"));
    }

    public void testEncodeParametersNoParams() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertTrue("URL should encode query parameters", infinoSerializeRequestURI.finalUrl.contains("search_logs"));
        assertTrue("URL should encode query parameters", infinoSerializeRequestURI.finalUrl.contains("start_time"));
        assertTrue("URL should encode query parameters", infinoSerializeRequestURI.finalUrl.contains("end_time"));
        assertFalse("URL should encode query parameters", infinoSerializeRequestURI.finalUrl.contains("q"));
    }

    public void testPostAppendLogEndpoint() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_append";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.POST);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_append");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("http://test-host:3000/test-index/append_log", infinoSerializeRequestURI.finalUrl);
    }

    public void testPutIndexEndpoint() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.PUT);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("http://test-host:3000/:test-index", infinoSerializeRequestURI.finalUrl);
    }

    public void testDeleteIndexEndpoint() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.DELETE);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("http://test-host:3000/:test-index", infinoSerializeRequestURI.finalUrl);
    }

    public void testUndefinedIndexType() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/_search?q=<error>";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("Index type should be UNDEFINED for unknown index path",
                InfinoSerializeRequestURI.InfinoIndexType.UNDEFINED, infinoSerializeRequestURI.getIndexType());
    }

    public void testNoIndexNameProvided() {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/_search";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoPath", "_search");
    }

    public void testInvalidPathConstruction() {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");

        // Expecting an IllegalArgumentException due to missing path information
        expectThrows(IllegalArgumentException.class, () -> new InfinoSerializeRequestURI(restRequest));
    }

    public void testMissingStartTimeDefaultHandling() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search?q=error";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertNotNull("Start time should have a default value when not provided", infinoSerializeRequestURI.startTime);
    }

    public void testMissingEndTimeDefaultHandling() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search?q=error";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertNotNull("End time should have a default value when not provided", infinoSerializeRequestURI.endTime);
    }

    public void testInvalidParameterEncoding() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_search?q=<error>";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_search");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertTrue("URL should be encoded to handle special characters",
                infinoSerializeRequestURI.finalUrl.contains("%3Cerror%3E"));
    }

    public void testMethodTypeHandling() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/_summarize?q=<error>";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.GET);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/_summarize");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("Method type should be GET", RestRequest.Method.GET, infinoSerializeRequestURI.method);
        assertTrue("Final URL should contain the summarize path",
                infinoSerializeRequestURI.finalUrl.contains("/summarize"));
    }

    public void testUrlConstructionForPostMethod() throws Exception {
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        String urlString = "http://opensearch/infino/test-index/logs/append_log";
        when(mockHttpRequest.uri()).thenReturn(urlString);
        when(mockHttpRequest.method()).thenReturn(RestRequest.Method.POST);

        RestRequest restRequest = RestRequest.request(null, mockHttpRequest, null);
        restRequest.params().put("infinoIndex", "test-index");
        restRequest.params().put("infinoPath", "logs/append_log");

        InfinoSerializeRequestURI infinoSerializeRequestURI = new InfinoSerializeRequestURI(restRequest);
        assertEquals("Final URL should be constructed for the append_log path",
                infinoSerializeRequestURI.infinoEndpoint + "/test-index/append_log",
                infinoSerializeRequestURI.finalUrl);
    }
}
