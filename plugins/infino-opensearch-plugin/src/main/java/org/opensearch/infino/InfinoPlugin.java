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
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
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
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportInterceptor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

import java.net.http.HttpClient;

/**
 * The Infino OpenSearch plugin.
 */
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

        logger.info("-----------------------Registering REST Handler------------------------");

        return singletonList(new InfinoRestHandler());
    }

    // Listen for data node requests on the transport layer (Search and Document
    // operations). This allows all the ingest pipeline logic for indexing and
    // all the ranking logic for searches to be applied by OpenSearch before it is
    // sent to Infino.
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext) {

        logger.info("-----------------------Registering Transport Interceptor------------------------");

        HttpClient httpClient = HttpClient.newHttpClient();

        return singletonList(new InfinoTransportInterceptor(httpClient));
    }

}
