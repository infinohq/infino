/**
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

package org.opensearch.infino;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.common.settings.Settings;

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
 * methods (e.g. TRACE)
 * so we don't test for that here
 */
public class InfinoSerializeTransportRequestTests extends OpenSearchTestCase {

        public void testShardSearchRequestParsing() throws IOException {
                ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
                when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
                when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockShardSearchRequest);
                assertEquals("test-index", infinoSerializeTransportRequest.getIndexName());
                assertEquals(InfinoSerializeTransportRequest.InfinoOperation.SEARCH_DOCUMENTS,
                                infinoSerializeTransportRequest.getOperation());
                assertEquals(RestRequest.Method.GET, infinoSerializeTransportRequest.getMethod());
                System.out.println("URL = " + infinoSerializeTransportRequest.getFinalUrl());

                assertTrue(infinoSerializeTransportRequest.getFinalUrl()
                                .startsWith("http://test-host:3000/test-index/search_logs?start_time="));
        }

        public void testIndexRequestParsing() throws IOException {
                IndexRequest mockIndexRequest = mock(IndexRequest.class);
                when(mockIndexRequest.indices()).thenReturn(new String[] { "test-index" });

                Map<String, Object> sourceMap = new HashMap<>();
                sourceMap.put("field1", "value1");
                sourceMap.put("field2", "value2");

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockIndexRequest);
                assertEquals("test-index", infinoSerializeTransportRequest.getIndexName());
                assertEquals(InfinoSerializeTransportRequest.InfinoOperation.INDEX_DOCUMENTS,
                                infinoSerializeTransportRequest.getOperation());
                assertEquals(RestRequest.Method.POST, infinoSerializeTransportRequest.getMethod());
                assertEquals("http://test-host:3000/test-index/append_log",
                                infinoSerializeTransportRequest.getFinalUrl());
        }

        public void testUnsupportedTransportRequest() {
                TransportRequest unsupportedRequest = mock(TransportRequest.class);
                assertThrows(IllegalArgumentException.class,
                                () -> new InfinoSerializeTransportRequest(unsupportedRequest));
        }

        public void testDefaultTimeRangeHandling() throws IOException {
                ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
                when(mockShardSearchRequest.indices()).thenReturn(new String[] { "test-index" });

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.matchQuery("field", "value"));
                when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockShardSearchRequest);
                assertTrue(infinoSerializeTransportRequest.getFinalUrl().contains("start_time="));
                assertTrue(infinoSerializeTransportRequest.getFinalUrl().contains("end_time="));
        }

        public void testMetricsSearchEndpoint() throws IOException {
                ShardSearchRequest mockShardSearchRequest = mock(ShardSearchRequest.class);
                when(mockShardSearchRequest.indices()).thenReturn(new String[] { "metrics-index" });

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.termQuery("name", "cpu"));
                searchSourceBuilder.query(QueryBuilders.termQuery("value", "0.8"));
                when(mockShardSearchRequest.source()).thenReturn(searchSourceBuilder);

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockShardSearchRequest);
                assertEquals("metrics-index", infinoSerializeTransportRequest.getIndexName());
                assertEquals(InfinoSerializeTransportRequest.InfinoIndexType.METRICS,
                                infinoSerializeTransportRequest.getIndexType());
                assertTrue(infinoSerializeTransportRequest.getFinalUrl().contains("/search_metrics?"));
        }

        public void testAppendLogEndpoint() throws IOException {
                IndexRequest mockIndexRequest = mock(IndexRequest.class);
                when(mockIndexRequest.indices()).thenReturn(new String[] { "test-index" });

                Map<String, Object> sourceMap = new HashMap<>();
                sourceMap.put("message", "Test log message");
                mockIndexRequest.source(sourceMap);

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockIndexRequest);
                assertEquals("test-index", infinoSerializeTransportRequest.getIndexName());
                assertEquals(InfinoSerializeTransportRequest.InfinoIndexType.LOGS,
                                infinoSerializeTransportRequest.getIndexType());
                assertEquals("http://test-host:3000/test-index/append_log",
                                infinoSerializeTransportRequest.getFinalUrl());
        }

        public void testAppendMetricEndpoint() throws IOException {
                IndexRequest mockIndexRequest = mock(IndexRequest.class);
                when(mockIndexRequest.indices()).thenReturn(new String[] { "metrics-index" });

                Map<String, Object> sourceMap = new HashMap<>();
                sourceMap.put("p", "avg(metric{label_name_1\\=\"label_value_1\"})");
                mockIndexRequest.source(sourceMap);

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockIndexRequest);
                assertEquals("metrics-index", infinoSerializeTransportRequest.getIndexName());
                assertEquals(InfinoSerializeTransportRequest.InfinoIndexType.METRICS,
                                infinoSerializeTransportRequest.getIndexType());
                assertEquals("http://test-host:3000/metrics-index/append_metric",
                                infinoSerializeTransportRequest.getFinalUrl());
        }

        public void testCreateIndexEndpoint() throws IOException {
                // Mock CreateIndexRequest
                CreateIndexRequest mockCreateIndexRequest = mock(CreateIndexRequest.class);

                // Setup non-empty settings
                Settings nonEmptySettings = Settings.builder()
                                .put("index.number_of_shards", 3)
                                .put("index.number_of_replicas", 2)
                                .build();

                // Mock behavior of CreateIndexRequest
                when(mockCreateIndexRequest.settings()).thenReturn(nonEmptySettings);
                when(mockCreateIndexRequest.indices()).thenReturn(new String[] { "test-index" });

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockCreateIndexRequest);
                assertEquals("http://test-host:3000/:test-index", infinoSerializeTransportRequest.getFinalUrl());
        }

        public void testDeleteIndexEndpoint() throws IOException {
                DeleteIndexRequest mockDeleteIndexRequest = mock(DeleteIndexRequest.class);
                when(mockDeleteIndexRequest.indices()).thenReturn(new String[] { "test-index" });

                InfinoSerializeTransportRequest infinoSerializeTransportRequest = new InfinoSerializeTransportRequest(
                                mockDeleteIndexRequest);
                assertEquals("http://test-host:3000/:test-index", infinoSerializeTransportRequest.getFinalUrl());
        }
}
