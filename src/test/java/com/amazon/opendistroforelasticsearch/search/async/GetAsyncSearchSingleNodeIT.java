package com.amazon.opendistroforelasticsearch.search.async;


import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.QuadConsumer;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchSingleNodeTestCase.SearchDelayPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class GetAsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    public void testGetAsyncSearchForNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailure, numResourceNotFoundFailures) -> {
                    assertEquals(concurrentRuns, numGetSuccess.get() + numResourceNotFoundFailures.get()
                            + numVersionConflictFailure.get());
                    assertEquals(0, numGetFailures.get());
                }, false, concurrentRuns, false);
    }

    public void testUpdateAsyncSearchForNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get()
                            + numResourceNotFoundFailures.get());
                }, true, concurrentRuns, false);
    }

    public void testUpdateAsyncSearchForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get()
                            + numResourceNotFoundFailures.get());
                }, true, concurrentRuns, true);
    }

    public void testGetAsyncSearchForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numResourceNotFoundFailures.get()
                            + numVersionConflictFailures.get());
                }, false, concurrentRuns, true);
    }

    public void testGetAsyncSearchInBlockedStateForNoRetainedResponse() throws Exception {
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
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numResourceNotFound) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get());
                    assertEquals(0, numResourceNotFound.get());
                }, false, concurrentRuns, false, plugins);
    }

    public void testGetAsyncSearchInBlockedStateForRetainedResponse() throws Exception {
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
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numResourceNotFound) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get());
                    assertEquals(0, numResourceNotFound.get());
                }, false, concurrentRuns, false, plugins);
    }

    public void testUpdateAsyncSearchInBlockedStateForRetainedResponse() throws Exception {
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
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get());
                }, true, concurrentRuns, false, plugins);
    }

    public void testUpdateAsyncSearchInBlockedStateForNoRetainedResponse() throws Exception {
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
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get());
                }, true, concurrentRuns, false, plugins);
    }

    private void assertConcurrentGetForBlockedSearch(AsyncSearchResponse submitResponse,
                                                     TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                                     boolean update, int concurrentRuns, boolean retainResponse,
                                                     List<SearchDelayPlugin> plugins) throws Exception {

        AtomicInteger numGetSuccess = new AtomicInteger();
        AtomicInteger numGetFailures = new AtomicInteger();
        AtomicInteger numVersionConflictFailures = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(DeleteAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            long lowerKeepAliveMillis = 5 * 1000 * 60 * 60 ;
            long higherKeepAliveMillis = 10 * 1000 * 60 * 60;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
                    if (update) {
                        logger.info("Triggering async search gets with keep alives --->");
                        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueMillis(randomLongBetween(lowerKeepAliveMillis,
                                higherKeepAliveMillis)));
                    }
                    getAsyncSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
                    executeGetAsyncSearch(client(), getAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse acknowledgedResponse) {
                            numGetSuccess.incrementAndGet();
                            countDownLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException) {
                                numVersionConflictFailures.incrementAndGet();
                            } else {
                                numGetFailures.incrementAndGet();
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
            DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(submitResponse.getId());
            CountDownLatch deleteLatch = new CountDownLatch(1);
            executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, ActionListener.wrap(() -> deleteLatch.countDown()));
            deleteLatch.await();
            disableBlocks(plugins);
            assertionConsumer.apply(numGetSuccess, numGetFailures, numVersionConflictFailures);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void assertConcurrentGetOrUpdates(AsyncSearchResponse submitResponse,
                                              QuadConsumer<AtomicInteger, AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                              boolean update, int concurrentRuns, boolean retainResponse)
            throws InterruptedException {
        AtomicInteger numGetSuccess = new AtomicInteger();
        AtomicInteger numGetFailures = new AtomicInteger();
        AtomicInteger numVersionConflictFailures = new AtomicInteger();
        AtomicInteger numResourceNotFoundFailures = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(GetAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            long lowerKeepAliveMillis = 5 * 1000 * 60 * 60 ;
            long higherKeepAliveMillis = 10 * 1000 * 60 * 60;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
                    if (update) {
                        logger.info("Triggering async search gets with keep alives --->");
                        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueMillis(randomLongBetween(lowerKeepAliveMillis,
                                higherKeepAliveMillis)));
                    }
                    getAsyncSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
                    executeGetAsyncSearch(client(), getAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                            if (update) {
                                assertThat(asyncSearchResponse.getExpirationTimeMillis(), greaterThanOrEqualTo(
                                        System.currentTimeMillis() + lowerKeepAliveMillis));
                                assertThat(asyncSearchResponse.getExpirationTimeMillis(), lessThanOrEqualTo(
                                        System.currentTimeMillis() + higherKeepAliveMillis));
                            }
                            numGetSuccess.incrementAndGet();
                            countDownLatch.countDown();
                        }
                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException) {
                                numVersionConflictFailures.incrementAndGet();
                            } else if (e instanceof ResourceNotFoundException) {
                                numResourceNotFoundFailures.incrementAndGet();
                            } else {
                                numGetFailures.incrementAndGet();
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
            if (retainResponse) {
                DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(submitResponse.getId());
                executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest).actionGet();
            }
            assertionConsumer.apply(numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures);

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}

