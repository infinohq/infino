/* This code is licensed under Apache License 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 */

package org.opensearch.infino;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

public class InfinoClientYamlTestSuiteIT extends OpenSearchClientYamlSuiteTestCase {

    public InfinoClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return OpenSearchClientYamlSuiteTestCase.createParameters();
    }
}
