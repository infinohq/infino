/** 
/* This code is licensed under Elastic License 2.0
/* https://www.elastic.co/licensing/elastic-license
**/

/**
 * Below permissions need to be added to java.policy for the plugin to work.
 *  
 * 1. permission org.opensearch.secure_sm.ThreadPermission "modifyArbitraryThread";
 * 
 *      The HttpClient::send() method seems to be using threads internally
 *      and opensearch's security policy blocks them. We need to add the
 *      below permissions to the java.policy to allowe HttpClient::send()
 *      to work.
 * 
 *      It seems odd that opensearch's policy is blocking HttpClient::send().
 *      We need to investigate this further to see if there are better
 *      Http libraries we can use in opensearch.
 * 
 *      Reference: below issue in github seems to reference the
 *                 modifyArbitraryThread permission.
 *                 https://github.com/opensearch-project/OpenSearch/issues/5359
 * 
 */
// 2. permission java.net.URLPermission "http://*:*/-", "*";
/*
 *      This permission is needed to allow outbound connections to Infino
 *      server.
 */

package org.opensearch.infino;

import java.net.http.HttpResponse;
import java.security.AccessController;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.NoSuchMethodException;
import java.lang.InstantiationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.BytesRestResponse;

/**
 * Handle REST calls for the /infino index.
 * This effectively serves as the public API for Infino.
 *
 * Notes:
 * 1. Search window defaults to the past 30 days if not specified by the
 * request.
 * 2. To access Infino indexes, the REST caller must prefix the index name with
 * '/infino/'.
 * 3. Index creation or deletion is mirrored on Infino and in OpenSarch.
 * 4. We use our own thread pool to manage Infino requests.
 *
 * Note that OpenSearch changed the import paths in v2.10
 * 
 * org.opensearch.core.rest.RestStatus
 * org.opensearch.core.action.ActionHandler
 * 
 * from
 * 
 * org.opensearch.rest.RestStatus
 * org.opensearch.action.ActionHandler
 * 
 * and Java doesn't have conditional imports so we have to use
 * reflection to dynamically load the classes we need. Makes this
 * code far more complex than it needs to be.
 * 
 */
public class InfinoPluginUtils {
    static final int THREADPOOL_SIZE = 25; // Size of threadpool we will use for Infino
    static final int MAX_RETRIES = 5; // Maximum number of retries for exponential backoff
    private static final Logger logger = LogManager.getLogger(InfinoPluginUtils.class);

    /**
     * Using a custom thread factory that can be used by the
     * ScheduledExecutorService.
     * We do this to add custom prefixes to the thread name. This will make
     * debugging
     * easier, if we ever have to debug.
     */
    protected static final class CustomThreadFactory implements ThreadFactory {
        private final String poolName;

        CustomThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(poolName + "-Thread-" + t.getId());
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    static final ScheduledExecutorService infinoThreadPool = Executors.newScheduledThreadPool(THREADPOOL_SIZE,
            new CustomThreadFactory("InfinoPluginThreadPool"));

    /**
     * Get thread pool
     * 
     * @return the thread pool to use for the requests
     */
    protected static ExecutorService getInfinoThreadPool() {
        return infinoThreadPool;
    }

    /**
     * Shutdown the thread pool when the plugin is stopped
     */
    public static void close() {
        infinoThreadPool.shutdown();
    }

    static boolean shouldRetry(int statusCode) {
        return statusCode == 429 || statusCode == 503 || statusCode == 504;
    }

    static long getRetryAfter(HttpResponse<String> response, int attempt) {
        return response.headers().firstValueAsLong("Retry-After").orElse((long) Math.pow(2, attempt) * 1000L);
    }

    /**
     * 
     * WAR for import path changes between OpenSearch versions
     * 
     * Use reflection for dynamic imports.
     * 
     * @param statusCode - status code of the response
     * @return RestStatus object - dynamically loaded object
     */
    public static Object getRestStatusFromCode(int statusCode) {
        try {
            Class<?> restStatusClass;
            try {
                restStatusClass = Class.forName("org.opensearch.core.rest.RestStatus");
            } catch (ClassNotFoundException e) {
                restStatusClass = Class.forName("org.opensearch.rest.RestStatus");
            }
            Method fromCodeMethod = restStatusClass.getMethod("fromCode", int.class);
            return fromCodeMethod.invoke(null, statusCode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to dynamically load RestStatus class", e);
        }
    }

    /**
     * 
     * WAR for import path changes between OpenSearch versions
     * 
     * Use reflection for dynamic imports.
     * 
     * @param restStatus      - the RestStatus code for the response
     * @param responseMessage - the message to be sent
     * @return BytesRestResponse - constructed response
     */
    public static BytesRestResponse createBytesRestResponse(Object restStatus, String responseMessage) {
        try {
            Constructor<?> constructor = BytesRestResponse.class.getConstructor(restStatus.getClass(), String.class);
            return (BytesRestResponse) constructor.newInstance(restStatus, responseMessage);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            logger.error("Failed to create BytesRestResponse using reflection", e);
            throw new RuntimeException("Failed to create BytesRestResponse using reflection", e);
        }
    }

    /**
     * Deletes a Lucene index if it exists.
     * 
     * Note that actionGet() is synchronous, which is fine for
     * index creation and/or deletion.
     *
     * @param client       The NodeClient to perform the operation.
     * @param rawIndexName The raw name of the index to delete.
     */
    protected static void deleteLuceneIndexIfExists(NodeClient client, String rawIndexName) {
        String indexName = "infino-" + rawIndexName;
        IndicesExistsRequest getIndexRequest = new IndicesExistsRequest(new String[] { indexName });

        logger.info("Deleting Lucene mirror index for Infino: " + indexName);

        IndicesExistsResponse response;
        try {
            response = client.admin().indices().exists(getIndexRequest).actionGet();
        } catch (Exception e) {
            logger.error("Error checking existence of '" + indexName + "' index", e);
            return;
        }

        if (response.isExists()) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            try {
                AcknowledgedResponse deleteResponse = client.admin().indices().delete(deleteIndexRequest).actionGet();
                if (deleteResponse.isAcknowledged()) {
                    logger.info("Successfully deleted '" + indexName + "' Lucene index on local node");
                } else {
                    logger.error("Failed to delete '" + indexName + "' Lucene index on local node");
                }
            } catch (Exception e) {
                logger.error("Failed to delete '" + indexName + "' Lucene index on local node", e);
            }
        }
    }

    /**
     * Create a Lucene index with the same name as the Infino index if it doesn't
     * exist. Note that actionGet() is synchronous, which is fine for index creation
     * and/or deletion.
     *
     * @param client       - client for the current OpenSearch node
     * @param rawIndexName - name of the index to create
     */

    protected static void createLuceneIndexIfNeeded(NodeClient client, String rawIndexName) {
        String indexName = "infino-" + rawIndexName;
        IndicesExistsRequest getIndexRequest = new IndicesExistsRequest(new String[] { indexName });

        logger.info("Creating Lucene mirror index for Infino: " + indexName);

        IndicesExistsResponse response;
        try {
            response = client.admin().indices().exists(getIndexRequest).actionGet();
        } catch (Exception e) {
            logger.error("Error checking existence of '" + indexName + "' index", e);
            return;
        }

        if (!response.isExists()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            createIndexRequest
                    .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1));
            try {
                AcknowledgedResponse createResponse = client.admin().indices().create(createIndexRequest).actionGet();
                if (createResponse.isAcknowledged()) {
                    logger.info("Successfully created '" + indexName + "' Lucene index on local node");
                } else {
                    logger.error("Failed to create '" + indexName + "' Lucene index on local node");
                }
            } catch (Exception e) {
                logger.error("Failed to create '" + indexName + "' Lucene index on local node", e);
            }
        }
    }
};
