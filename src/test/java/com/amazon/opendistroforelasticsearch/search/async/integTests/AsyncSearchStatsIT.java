/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.async.integTests;

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchStatsResponse;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchCountStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 5, scope = ESIntegTestCase.Scope.TEST)
public class AsyncSearchStatsIT extends AsyncSearchIntegTestCase {
    private int asyncSearchConcurrentLimit = 20;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        boolean lowLevelCancellation = randomBoolean();
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(AsyncSearchActiveStore.MAX_RUNNING_CONTEXT.getKey(), asyncSearchConcurrentLimit)
                .build();
    }

    public void testNodewiseStats() throws InterruptedException {
        String index = "idx";
        createIndex(index);
        indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                        .setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest(index));
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(2));
        submitAsyncSearchRequest.keepOnCompletion(true);
        List<DiscoveryNode> dataNodes = new LinkedList<>();
        clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(node -> {
            dataNodes.add(node.value);
        });
        assertFalse(dataNodes.isEmpty());
        DiscoveryNode randomDataNode = dataNodes.get(randomInt(dataNodes.size() - 1));
        try {
            AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(client(randomDataNode.getName()),
                    submitAsyncSearchRequest);
            assertNotNull(asyncSearchResponse.getSearchResponse());
            TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
            AsyncSearchStatsResponse statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE,
                    new AsyncSearchStatsRequest()).get();
            statsResponse.getNodes().forEach(nodeStats -> {
                AsyncSearchCountStats asyncSearchCountStats = nodeStats.getAsyncSearchCountStats();
                if (nodeStats.getNode().equals(randomDataNode)) {
                    assertEquals(1, asyncSearchCountStats.getPersistedCount());
                    assertEquals(1, asyncSearchCountStats.getCompletedCount());
                    assertEquals(1, asyncSearchCountStats.getSubmittedCount());
                    assertEquals(1, asyncSearchCountStats.getInitializedCount());
                    assertEquals(0, asyncSearchCountStats.getFailedCount());
                    assertEquals(0, asyncSearchCountStats.getRunningCount());
                    assertEquals(0, asyncSearchCountStats.getCancelledCount());
                } else {
                    assertEquals(0, asyncSearchCountStats.getPersistedCount());
                    assertEquals(0, asyncSearchCountStats.getCompletedCount());
                    assertEquals(0, asyncSearchCountStats.getFailedCount());
                    assertEquals(0, asyncSearchCountStats.getRunningCount());
                }
            });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public void testStatsAcrossNodes() throws InterruptedException, ExecutionException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsyncSearchStatsIT.class.getName());
            String index = "idx";
            createIndex(index);
            indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                            .setSource("field1", "the quick brown fox jumps"),
                    client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                    client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));

            List<DiscoveryNode> dataNodes = new LinkedList<>();
            clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(node -> {
                dataNodes.add(node.value);
            });
            assertFalse(dataNodes.isEmpty());
            int numThreads = 20;
            List<Runnable> threads = new ArrayList<>();
            AtomicLong expectedNumSuccesses = new AtomicLong();
            AtomicLong expectedNumFailures = new AtomicLong();
            AtomicLong expectedNumPersisted = new AtomicLong();
            CountDownLatch latch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threads.add(() -> {
                    try {
                        boolean success = randomBoolean();
                        boolean keepOnCompletion = randomBoolean();
                        if (keepOnCompletion) {
                            expectedNumPersisted.getAndIncrement();
                        }
                        SubmitAsyncSearchRequest submitAsyncSearchRequest;
                        if (success) {
                            expectedNumSuccesses.getAndIncrement();
                            submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest(index));
                            submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(2));
                            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);

                        } else {
                            expectedNumFailures.getAndIncrement();
                            submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest(
                                    "non_existent_index"));
                            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
                        }

                        AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(client(dataNodes.get(randomInt(1)).getName()),
                                submitAsyncSearchRequest);
                        if (keepOnCompletion) {
                            TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
                        }
                    } catch (Exception e) {
                        fail(e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }
            TestThreadPool finalThreadPool = threadPool;
            threads.forEach(t -> finalThreadPool.generic().execute(t));
            latch.await();
            AsyncSearchStatsResponse statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE, new AsyncSearchStatsRequest()).get();
            AtomicLong actualNumSuccesses = new AtomicLong();
            AtomicLong actualNumFailures = new AtomicLong();
            AtomicLong actualNumPersisted = new AtomicLong();
            for (AsyncSearchStats node : statsResponse.getNodes()) {
                AsyncSearchCountStats asyncSearchCountStats = node.getAsyncSearchCountStats();
                assertEquals(asyncSearchCountStats.getRunningCount(), 0);

                assertThat(expectedNumSuccesses.get(), greaterThanOrEqualTo(asyncSearchCountStats.getCompletedCount()));
                actualNumSuccesses.getAndAdd(asyncSearchCountStats.getCompletedCount());

                assertThat(expectedNumFailures.get(), greaterThanOrEqualTo(asyncSearchCountStats.getFailedCount()));
                actualNumFailures.getAndAdd(asyncSearchCountStats.getFailedCount());

                assertThat(expectedNumPersisted.get(), greaterThanOrEqualTo(asyncSearchCountStats.getPersistedCount()));
                actualNumPersisted.getAndAdd(asyncSearchCountStats.getPersistedCount());
            }

            assertEquals(expectedNumPersisted.get(), actualNumPersisted.get());
            assertEquals(expectedNumFailures.get(), actualNumFailures.get());
            assertEquals(expectedNumSuccesses.get(), actualNumSuccesses.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testRunningAsyncSearchCountStat() throws InterruptedException, ExecutionException {
        String index = "idx";
        createIndex(index);
        indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                        .setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        SearchRequest searchRequest = client().prepareSearch(index).setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest);
        AsyncSearchStatsResponse statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE, new AsyncSearchStatsRequest()).get();
        long runningSearchCount = 0;
        for (AsyncSearchStats node : statsResponse.getNodes()) {
            runningSearchCount += node.getAsyncSearchCountStats().getRunningCount();
            assertEquals(node.getAsyncSearchCountStats().getCompletedCount(), 0L);
            assertEquals(node.getAsyncSearchCountStats().getFailedCount(), 0L);
            assertEquals(node.getAsyncSearchCountStats().getPersistedCount(), 0L);
        }
        assertEquals(runningSearchCount, 1L);
        disableBlocks(plugins);
        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
        statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE, new AsyncSearchStatsRequest()).get();
        long persistedCount = 0;
        long completedCount = 0;
        for (AsyncSearchStats node : statsResponse.getNodes()) {
            persistedCount += node.getAsyncSearchCountStats().getPersistedCount();
            completedCount += node.getAsyncSearchCountStats().getCompletedCount();
            assertEquals(node.getAsyncSearchCountStats().getRunningCount(), 0L);
            assertEquals(node.getAsyncSearchCountStats().getFailedCount(), 0L);
        }
        assertEquals(runningSearchCount, 1L);
    }

    public void testThrottledAsyncSearchCount() throws InterruptedException, ExecutionException {
        String index = "idx";
        createIndex(index);
        indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                        .setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));

        List<DiscoveryNode> dataNodes = new LinkedList<>();
        clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(node -> {
            dataNodes.add(node.value);
        });
        assertFalse(dataNodes.isEmpty());
        DiscoveryNode randomDataNode = dataNodes.get(randomInt(dataNodes.size() - 1));
        int numThreads = 21;
        List<Thread> threads = new ArrayList<>();
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        SearchRequest searchRequest = client().prepareSearch(index).setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
                    executeSubmitAsyncSearch(client(randomDataNode.getName()), submitAsyncSearchRequest);
                } catch (ExecutionException e) {
                    assertThat(e.getMessage(), containsString("Trying to create too many running contexts"));
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            });
            threads.add(t);
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(verifyThrottlingFromStats());
        disableBlocks(plugins);
    }

    private boolean verifyThrottlingFromStats() {
        try {
            AsyncSearchStatsResponse statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE, new AsyncSearchStatsRequest()).get();
            for (AsyncSearchStats nodeStats : statsResponse.getNodes()) {
                if (nodeStats.getAsyncSearchCountStats().getThrottledCount() == 1L) {
                    return true;
                }
            }
            return false;
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }
}
