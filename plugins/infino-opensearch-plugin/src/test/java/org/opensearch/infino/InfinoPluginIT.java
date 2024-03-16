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
import org.opensearch.client.Client;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
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
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static java.util.Collections.singletonList;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class InfinoPluginIT extends OpenSearchIntegTestCase {
    private static final Logger logger = LogManager.getLogger(InfinoPlugin.class);

    protected static final int MAX_RETRIES = 2;
    private static int mockStatusCode = 200;
    private static String mockPath = "/default/path";
    private static String mockBody = "{\"Default\" : \"body\"}";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", MockInfinoPlugin.class.getName())
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockInfinoPlugin.class);
    }

    public static class MockInfinoPlugin extends InfinoPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
                ThreadContext threadContext) {
            logger.info("-----------------------Registering Test Transport Interceptor------------------------");
            return singletonList(new InfinoTransportInterceptor(getCustomHttpClient()));
        }

        // Provide a no-argument constructor as expected by OpenSearch
        public MockInfinoPlugin() {
            super();
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
            return Collections.singletonList(new InfinoTransportInterceptor(getCustomHttpClient()));
        }
    }

    private static MyHttpClient mockMyHttpClient = new MyHttpClient() {
        @Override
        public HttpResponse<String> sendRequest(HttpRequest request,
                HttpResponse.BodyHandler<String> responseBodyHandler) {
            // Return a mocked response
            HttpResponse<String> response = createFakeResponse(mockStatusCode, mockPath, mockBody);
            return response;
        }
    };

    public interface MyHttpClient {
        HttpResponse<String> sendRequest(HttpRequest request,
                HttpResponse.BodyHandler<String> responseBodyHandler);
    }

    // Create a fake HttpResponse for testing
    private static HttpResponse<String> createFakeResponse(int fakeStatusCode, String fakePath, String fakeBody) {
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return fakeStatusCode;
            }

            @Override
            public Optional<HttpResponse<String>> previousResponse() {
                return Optional.empty();
            }

            @Override
            public HttpRequest request() {
                return HttpRequest.newBuilder().uri(URI.create(fakePath)).build();
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of(new HashMap<>(), (s, s2) -> true);
            }

            @Override
            public String body() {
                return fakeBody; // Fake body content
            }

            @Override
            public Optional<SSLSession> sslSession() {
                return Optional.empty();
            }

            @Override
            public URI uri() {
                return request().uri();
            }

            @Override
            public HttpClient.Version version() {
                return HttpClient.Version.HTTP_1_1;
            }
        };
    }

    private static HttpClient getCustomHttpClient() {
        return new HttpClient() {
            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                    BodyHandler<T> responseBodyHandler, PushPromiseHandler<T> pushPromiseHandler) {
                throw new UnsupportedOperationException("sendAsync Not implemented in mock");
            }

            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                    BodyHandler<T> responseBodyHandler) {
                throw new UnsupportedOperationException("sendAsync Not implemented in mock");
            }

            @Override
            public <T> HttpResponse<T> send(HttpRequest request, BodyHandler<T> responseBodyHandler)
                    throws IOException, InterruptedException {
                HttpResponse<String> response = mockMyHttpClient.sendRequest(request,
                        convertToSpecificHandler(responseBodyHandler));
                return convertToGenericResponse(response, responseBodyHandler);
            }

            // Helper method to convert BodyHandler<T> to BodyHandler<String>
            private BodyHandler<String> convertToSpecificHandler(BodyHandler<?> handler) {
                return HttpResponse.BodyHandlers.ofString();
            }

            @Override
            public Optional<CookieHandler> cookieHandler() {
                return Optional.empty();
            }

            @Override
            public Optional<Duration> connectTimeout() {
                return Optional.empty();
            }

            @Override
            public Redirect followRedirects() {
                return null;
            }

            @Override
            public Optional<ProxySelector> proxy() {
                return Optional.empty();
            }

            @Override
            public SSLContext sslContext() {
                return null;
            }

            @Override
            public SSLParameters sslParameters() {
                return null;
            }

            @Override
            public Optional<Authenticator> authenticator() {
                return Optional.empty();
            }

            @Override
            public Version version() {
                return Version.HTTP_1_1;
            }

            @Override
            public Optional<Executor> executor() {
                return Optional.empty();
            }
        };
    }

    // Helper method to convert HttpResponse<String> to HttpResponse<T>
    private static <T> HttpResponse<T> convertToGenericResponse(HttpResponse<String> response, BodyHandler<T> handler) {
        @SuppressWarnings("unchecked")
        HttpResponse<T> genericResponse = (HttpResponse<T>) response;
        return genericResponse;
    }

    public void testInfinoInstalled() throws IOException, ParseException {
        Response response = createRestClient().performRequest(new Request("GET", "/_cat/plugins"));
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        logger.info("response body: {}", body);
        MatcherAssert.assertThat(body, Matchers.containsString("infino"));
    }

    // @Test
    // public void testInfinoSearchRequest() throws IOException,
    // InterruptedException {
    // mockStatusCode = 200;
    // mockPath = "/default/path";
    // mockBody = "{\"hits\": [{\"_source\": {\"field\": \"value\"}}]}";

    // Response createResponse = createRestClient().performRequest(new
    // Request("PUT", "/test-index"));
    // String createResponseBody = EntityUtils.toString(createResponse.getEntity(),
    // StandardCharsets.UTF_8);
    // logger.info("response body: {}", createResponseBody);

    // Response searchResponse = createRestClient().performRequest(new
    // Request("GET", "/test-index/_search"));
    // String searchResponseBody = EntityUtils.toString(searchResponse.getEntity(),
    // StandardCharsets.UTF_8);

    // logger.info("response body: {}", searchResponseBody);

    // // Assert the response
    // assertNotNull(searchResponse);
    // assertEquals(200, searchResponse.getStatusLine().getStatusCode());
    // // assertEquals(searchResponseBody, mockBody);
    // }
}
