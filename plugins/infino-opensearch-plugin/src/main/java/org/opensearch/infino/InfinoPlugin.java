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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.transport.TransportInterceptor;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

import java.net.http.HttpClient;

/**
 * Implement the REST API handler and transport interceptor for client calls
 */
public class InfinoPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    private static final Logger logger = LogManager.getLogger(InfinoPlugin.class);

    // This methods overrides the method from the parent class to hand a list
    // of additional REST handlers to OpenSearch at pre-defined paths.
    @Override
    public List<RestHandler> getRestHandlers(
            final Settings settings,
            final RestController restController,
            final ClusterSettings clusterSettings,
            final IndexScopedSettings indexScopedSettings,
            final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier nodesInCluster) {

        logger.info("Registering REST Handler");

        return singletonList(new InfinoRestHandler());
    }

    // This methods overrides the method from the parent class to hand a list
    // of additional transport handlers to OpenSearch.
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext) {

        logger.info("Registering Transport Interceptor");

        HttpClient httpClient = HttpClient.newHttpClient();

        return singletonList(new InfinoTransportInterceptor(httpClient));
    }

}
