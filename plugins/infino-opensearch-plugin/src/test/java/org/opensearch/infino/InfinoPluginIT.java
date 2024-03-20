/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.infino;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.http.util.EntityUtils;
import org.apache.http.ParseException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.client.Client;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.watcher.ResourceWatcherService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.PushPromiseHandler;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.PUT;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static java.util.Collections.singletonList;

@SuppressWarnings("unused")
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class InfinoPluginIT extends OpenSearchIntegTestCase {
    private static final Logger logger = LogManager.getLogger(InfinoPluginIT.class);
    private InfinoActionHandler handler;
    private static InfinoPluginTestUtils utils = new InfinoPluginTestUtils();
    public static InfinoSerializeActionRequestURI mockInfinoSerializeActionRequestURI;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockInfinoPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        System.setProperty("infinoPluginTestMode", "true");
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    public static class MockInfinoPlugin extends InfinoPlugin {

        HttpClient httpClient = utils.getCustomHttpClient();

        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext) {
            return singletonList(new InfinoTransportInterceptor(utils.getCustomHttpClient()));
        }

        // Provide a no-argument constructor as expected by OpenSearch
        public MockInfinoPlugin() {
            super();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return Arrays.asList(new ActionFilter() {
                @Override
                public int order() {
                    return 0; // Ensure this filter has a high precedence
                }

                @SuppressWarnings("hiding")
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task,
                        String action, Request request, ActionListener<Response> listener,
                        ActionFilterChain<Request, Response> chain) {
                    logger.debug("Intercepting Action request: " + request.toString());
                    InfinoActionHandler handler = new InfinoActionHandler(
                            utils.getCustomHttpClient()) {

                        @SuppressWarnings("unused")
                        protected <Request extends ActionRequest, Response extends ActionResponse> InfinoSerializeActionRequestURI getInfinoSerializeTransportRequest(
                                RestRequest.Method method,
                                String indexName) {
                            return mockInfinoSerializeActionRequestURI;
                        }

                    };

                    RestRequest.Method method = PUT;
                    String indexName = "default"; // The action type check below ensures this never reaches Infino.

                    if (CreateIndexAction.NAME.equals(action)) {
                        method = PUT;
                        CreateIndexRequest req = (CreateIndexRequest) request;
                        indexName = req.index();
                        try {
                            handler.mirrorInfino(action, method, indexName);
                        } catch (Exception e) {
                            listener.onFailure(
                                    new IOException("Index creation failed on Infino. Aborting.", e));
                            return;
                        }
                    } else if (DeleteIndexAction.NAME.equals(action)) {
                        method = DELETE;
                        DeleteIndexRequest req = (DeleteIndexRequest) request;
                        indexName = req.indices()[0];
                        if (req.indices().length > 1) {
                            throw new IllegalArgumentException("Only single index DELETES are currently supported");
                        }
                        try {
                            handler.mirrorInfino(action, method, indexName);

                        } catch (Exception e) {
                            listener.onFailure(
                                    new IOException("Index deletion failed on Infino. Aborting.", e));
                            return;
                        }
                    } else {
                        logger.debug("---------Skipping test action: " + action);
                    }

                    // Proceed with the original request if not blocked
                    chain.proceed(task, action, request, listener);
                }
            });
        }

        @Override
        public Collection<Object> createComponents(
                Client client,
                ClusterService clusterService,
                ThreadPool threadPool,
                ResourceWatcherService resourceWatcherService,
                ScriptService scriptService,
                NamedXContentRegistry xContentRegistry,
                Environment environment,
                NodeEnvironment nodeEnvironment,
                NamedWriteableRegistry namedWriteableRegistry,
                IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<RepositoriesService> repositoriesServiceSupplier) {
            return Collections
                    .singletonList(new InfinoTransportInterceptor(utils.getCustomHttpClient()));
        }
    }

    public void testInfinoInstalled() throws IOException, ParseException {
        // Given
        String expectedIndexName = "test-index";
        mockInfinoSerializeActionRequestURI = mock(InfinoSerializeActionRequestURI.class);

        String expectedFinalUrl = "http://mockserver/" + expectedIndexName;
        when(mockInfinoSerializeActionRequestURI.getFinalUrl()).thenReturn(expectedFinalUrl);

        Response response = createRestClient().performRequest(new Request("GET",
                "/_cat/plugins"));
        String body = EntityUtils.toString(response.getEntity(),
                StandardCharsets.UTF_8);

        logger.info("response body: {}", body);
        MatcherAssert.assertThat(body, Matchers.containsString("infino"));
    }
}