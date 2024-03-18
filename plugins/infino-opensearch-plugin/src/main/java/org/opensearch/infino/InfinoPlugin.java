/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportInterceptor;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

import java.io.IOException;
import java.net.http.HttpClient;

/**
 * The Infino OpenSearch plugin.
 **/
public class InfinoPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    private static final Logger logger = LogManager.getLogger(InfinoPlugin.class);

    // Listen for (allowed) index creation and deletion calls on the REST API
    // and process them instead of OpenSearch.
    @Override
    public List<RestHandler> getRestHandlers(
            final Settings settings,
            final RestController restController,
            final ClusterSettings clusterSettings,
            final IndexScopedSettings indexScopedSettings,
            final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier nodesInCluster) {

        logger.info("-----------------------Infino: Registering REST Handler------------------------");

        return singletonList(new InfinoRestHandler());
    }

    /**
     * Listen for action requests on the transport layer (Index create and delete).
     * It's not atomic, so there are 2 possible side effects:
     * 
     * 1) Index creation on Infino is successful, but index creation on
     * OpenSearch fails leaving Infino with an orphan index.
     * 
     * 2) Index deletion on Infino is sucessful, but index deletion on OpenSearch
     * fails leaving OpenSearch with an orphan index.
     * 
     * We need to think about how to deal with both of these cases.
     **/
    @Override
    public List<ActionFilter> getActionFilters() {
        logger.info("-----------------------Infino: Registering Action Filter------------------------");

        return Arrays.asList(new ActionFilter() {
            @Override
            public int order() {
                return 0; // Ensure this filter has a high precedence
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task,
                    String action, Request request, ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain) {
                logger.debug("Intercepting Action request: " + request.toString());
                InfinoActionHandler handler = new InfinoActionHandler();

                RestRequest.Method method = PUT;
                String indexName = "default"; // The action type check below ensures this never reaches Infino.

                logger.info("Infino Action Handler: Serializing action request for Infino");
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
                                new IOException("Index creation failed on Infino. Aborting.", e));
                        return;
                    }
                }

                logger.debug("---------Action is: " + action);
                logger.debug("=======The request is " + request.toString());

                // Proceed with the original request if not blocked
                chain.proceed(task, action, request, listener);
            }
        });
    }

    /**
     * Listen for data node requests on the transport layer (Search and Document
     * operations). This allows all the ingest pipeline logic for indexing and
     * all the ranking logic for searches to be applied by OpenSearch before it is
     * sent to Infino.
     **/
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext) {

        logger.info("-----------------------Infino: Registering Transport Interceptor------------------------");

        HttpClient httpClient = HttpClient.newHttpClient();

        return singletonList(new InfinoTransportInterceptor(httpClient));
    }

}
