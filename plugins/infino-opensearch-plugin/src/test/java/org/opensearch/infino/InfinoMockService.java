/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.infino;

import org.opensearch.rest.RestResponse;

public class InfinoMockService {

    public static void helloworld() {
        return;
    }

    public static RestResponse buildResponse(String name) {
        String space = name.isEmpty() ? "" : " ";
        final String message = "Hi" + space + name
                + "! Confirming 3rd party Infino service can receive requests through OpenSearch" + "\n";
        return InfinoRestHandler.createBytesRestResponse(InfinoRestHandler.getRestStatusFromCode(200), message);
    }
}