package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.amazon.opendistroforelasticsearch.search.async.DeleteAsyncSearchSingleNodeIT.SearchDelayPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

public class DeleteAsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        LinkedList<Class<? extends Plugin>> plugins = new LinkedList<>(super.getPlugins());
        plugins.add(SearchDelayPlugin.class);
        return plugins;
    }

    public void testDeleteAsyncSearchForRetainedResponseRandomTime() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns - 1, numResourceNotFound.get());
                }, concurrentRuns);
    }

    public void testDeleteAsyncSearchNoRetainedResponseRandomTime() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                }, concurrentRuns);
    }

    public void testDeleteAsyncSearchPostCompletionNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(0, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns, numResourceNotFound.get());
                }, concurrentRuns);
    }

    public void testDeleteAsyncSearchPostCompletionForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns-1, numResourceNotFound.get());
                }, concurrentRuns);
    }

    public void testDeleteAsyncSearchInRunningStateForRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentDeletesForRunningSearch(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns - 1, numResourceNotFound.get());
                }, concurrentRuns, plugins);
    }

    public void testDeleteAsyncSearchInRunningStateForNoRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentDeletesForRunningSearch(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns-1, numResourceNotFound.get());
                }, concurrentRuns, plugins);
    }

    private void assertConcurrentDeletesForRunningSearch(String id, TriConsumer<AtomicInteger, AtomicInteger,
            AtomicInteger> assertionConsumer, int concurrentRuns, List<SearchDelayPlugin> plugins) throws Exception {

        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(DeleteAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering async search delete --->");
                    DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(id);
                    executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, new ActionListener<AcknowledgedResponse>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            if (acknowledgedResponse.isAcknowledged()) {
                                numDeleteAcknowledged.incrementAndGet();
                            } else {
                                numDeleteUnAcknowledged.incrementAndGet();
                            }
                            countDownLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ResourceNotFoundException) {
                                numResourceNotFound.incrementAndGet();
                            }
                            countDownLatch.countDown();
                        }
                    });
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            disableBlocks(plugins);
            assertionConsumer.apply(numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void assertConcurrentDeletes(String id, TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                         int concurrentRuns) throws InterruptedException {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(DeleteAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering async search delete --->");
                    DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(id);
                    executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, new ActionListener<AcknowledgedResponse>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            if (acknowledgedResponse.isAcknowledged()) {
                                numDeleteAcknowledged.incrementAndGet();
                            } else {
                                numDeleteUnAcknowledged.incrementAndGet();
                            }
                            countDownLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ResourceNotFoundException) {
                                numResourceNotFound.incrementAndGet();
                            }
                            countDownLatch.countDown();
                        }
                    });
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertionConsumer.apply(numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private List<SearchDelayPlugin> initPluginFactory() {
        List<SearchDelayPlugin> plugins = new ArrayList<>();
        PluginsService pluginsService = getInstanceFromNode(PluginsService.class);
            plugins.addAll(pluginsService.filterPlugins(SearchDelayPlugin.class));
            enableBlocks(plugins);
        return plugins;
    }

    private void disableBlocks(List<SearchDelayPlugin> plugins) {
        for (SearchDelayPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    private void enableBlocks(List<SearchDelayPlugin> plugins) {
        for (SearchDelayPlugin plugin : plugins) {
            plugin.enableBlock();
        }
    }

    public static class SearchDelayPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_delay";

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    LogManager.getLogger(DeleteAsyncSearchSingleNodeIT.class).info("Blocking ----------->");
                    assertBusy(() -> assertFalse(shouldBlock.get()));
                    LogManager.getLogger(DeleteAsyncSearchSingleNodeIT.class).info("Unblocking ----------->");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

}
