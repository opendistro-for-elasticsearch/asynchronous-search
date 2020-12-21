package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchStatsResponse;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchCountStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 5, scope = ESIntegTestCase.Scope.TEST)
public class AsyncSearchStatsIT extends AsyncSearchIntegTestCase {

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
                    assertEquals(1, asyncSearchCountStats.getPersistedStage());
                    assertEquals(1, asyncSearchCountStats.getCompletedStage());
                    assertEquals(0, asyncSearchCountStats.getFailedStage());
                    assertEquals(0, asyncSearchCountStats.getRunningStage());
                } else {
                    assertEquals(0, asyncSearchCountStats.getPersistedStage());
                    assertEquals(0, asyncSearchCountStats.getCompletedStage());
                    assertEquals(0, asyncSearchCountStats.getFailedStage());
                    assertEquals(0, asyncSearchCountStats.getRunningStage());
                }
            });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public void testStatsAcrossNodes() throws InterruptedException, ExecutionException {
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
        List<Thread> threads = new ArrayList<>();
        AtomicLong expectedNumSuccesses = new AtomicLong();
        AtomicLong expectedNumFailures = new AtomicLong();
        AtomicLong expectedNumPersisted = new AtomicLong();

        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
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
                        submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("non_existent_index"));
                        submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
                    }

                    AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(client(dataNodes.get(randomInt(1)).getName()),
                            submitAsyncSearchRequest);
                    if (keepOnCompletion) {
                        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
                    }

                } catch (Exception e) {
                    fail(e.getMessage());
                }
            });
            threads.add(t);
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join(100);
        }
        AsyncSearchStatsResponse statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE, new AsyncSearchStatsRequest()).get();
        AtomicLong actualNumSuccesses = new AtomicLong();
        AtomicLong actualNumFailures = new AtomicLong();
        AtomicLong actualNumPersisted = new AtomicLong();
        for (AsyncSearchStats node : statsResponse.getNodes()) {
            AsyncSearchCountStats asyncSearchCountStats = node.getAsyncSearchCountStats();
            assertEquals(asyncSearchCountStats.getRunningStage(), 0);

            assertThat(expectedNumSuccesses.get(), greaterThanOrEqualTo(asyncSearchCountStats.getCompletedStage()));
            actualNumSuccesses.getAndAdd(asyncSearchCountStats.getCompletedStage());

            assertThat(expectedNumFailures.get(), greaterThanOrEqualTo(asyncSearchCountStats.getFailedStage()));
            actualNumFailures.getAndAdd(asyncSearchCountStats.getFailedStage());

            assertThat(expectedNumPersisted.get(), greaterThanOrEqualTo(asyncSearchCountStats.getPersistedStage()));
            actualNumPersisted.getAndAdd(asyncSearchCountStats.getPersistedStage());
        }

        assertEquals(expectedNumPersisted.get(), actualNumPersisted.get());
        assertEquals(expectedNumFailures.get(), actualNumFailures.get());
        assertEquals(expectedNumSuccesses.get(), actualNumSuccesses.get());
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
            runningSearchCount += node.getAsyncSearchCountStats().getRunningStage();
            assertEquals(node.getAsyncSearchCountStats().getCompletedStage(), 0L);
            assertEquals(node.getAsyncSearchCountStats().getFailedStage(), 0L);
            assertEquals(node.getAsyncSearchCountStats().getPersistedStage(), 0L);
        }
        assertEquals(runningSearchCount, 1L);
        disableBlocks(plugins);
        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
        statsResponse = client().execute(AsyncSearchStatsAction.INSTANCE, new AsyncSearchStatsRequest()).get();
        long persistedCount = 0;
        long completedCount = 0;
        for (AsyncSearchStats node : statsResponse.getNodes()) {
            persistedCount += node.getAsyncSearchCountStats().getPersistedStage();
            completedCount += node.getAsyncSearchCountStats().getCompletedStage();
            assertEquals(node.getAsyncSearchCountStats().getRunningStage(), 0L);
            assertEquals(node.getAsyncSearchCountStats().getFailedStage(), 0L);
        }
        assertEquals(runningSearchCount, 1L);
    }
}
