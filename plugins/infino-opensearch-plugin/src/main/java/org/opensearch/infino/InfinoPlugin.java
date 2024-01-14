/* This code is licensed under Apache License 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * Implement both the REST API handler for client calls
 */
public class InfinoPlugin extends Plugin implements ActionPlugin {

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

        return singletonList(new InfinoRestHandler());
    }
}
