/* This code is licensed under Apache License 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */

package org.opensearch.infino;

import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestResponse;
import org.opensearch.core.rest.RestStatus;

public class InfinoMockService {

    public static void helloworld() {
        return;
    }

    public static RestResponse buildResponse(String name) {
        String space = name.isEmpty() ? "" : " ";
        final String message = "Hi" + space + name
                + "! Confirming 3rd party Infino service can receive requests through OpenSearch" + "\n";
        return new BytesRestResponse(RestStatus.OK, message);
    }
}