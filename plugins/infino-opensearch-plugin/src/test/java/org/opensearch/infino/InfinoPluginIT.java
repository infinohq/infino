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
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class InfinoPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InfinoPlugin.class);
    }

    public void testInfinoInstalled() throws IOException, ParseException {
        Response response = createRestClient().performRequest(new Request("GET", "/_cat/plugins"));
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        logger.info("response body: {}", body);
        MatcherAssert.assertThat(body, Matchers.containsString("infino"));
    }
}
