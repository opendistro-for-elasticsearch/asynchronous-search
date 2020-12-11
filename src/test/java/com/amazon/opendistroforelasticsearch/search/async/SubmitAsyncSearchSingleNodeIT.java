package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SubmitAsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    public void testSubmitAsyncSearchWithoutRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentSubmits(submitAsyncSearchRequest, searchResponse, (numStartedAsyncSearch, numFailedAsyncSearch, numErrorResponseAsyncSearch) -> {
            assertEquals(concurrentRuns, numStartedAsyncSearch.get());
            assertEquals(0,  numFailedAsyncSearch.get());
            assertEquals(0,  numErrorResponseAsyncSearch.get());
        }, concurrentRuns);
    }

    public void testSubmitAsyncSearchWithRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentSubmits(submitAsyncSearchRequest, searchResponse, (numStartedAsyncSearch, numFailedAsyncSearch, numErrorResponseAsyncSearch) -> {
            assertEquals(concurrentRuns, numStartedAsyncSearch.get());
            assertEquals(0,  numFailedAsyncSearch.get());
            assertEquals(0,  numErrorResponseAsyncSearch.get());
        }, concurrentRuns);
    }

    private void assertConcurrentSubmits(SubmitAsyncSearchRequest submitAsyncSearchRequest, SearchResponse searchResponse,
                                         TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer, int concurrentRuns)
            throws InterruptedException {
        AtomicInteger numStartedAsyncSearch = new AtomicInteger();
        AtomicInteger numFailedAsyncSearch = new AtomicInteger();
        AtomicInteger numErrorResponseAsyncSearch = new AtomicInteger();
        final ClusterState state = getInstanceFromNode(ClusterService.class).state();

        TestThreadPool testThreadPool = null;
        CountDownLatch countDownLatch = null;
        try {
            testThreadPool = new TestThreadPool(GetAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            if (submitAsyncSearchRequest.getKeepOnCompletion()) {
                //we also need to delete async search response to ensure test completes gracefully with no background tasks
                // running
                countDownLatch = new CountDownLatch(2 * numThreads);
            } else {
                countDownLatch = new CountDownLatch(numThreads);
            }

            for (int i = 0; i < numThreads; i++) {
                CountDownLatch finalCountDownLatch = countDownLatch;
                Runnable thread = () -> {
                    logger.info("Triggering async search submit --->");
                    executeSubmitAsyncSearch(client(), submitAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                            if (asyncSearchResponse.getId() != null) {
                                AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(asyncSearchResponse.getId());
                                assertEquals(state.nodes().getLocalNodeId(), asyncSearchId.getNode());
                                AsyncSearchAssertions.assertSearchResponses(searchResponse, asyncSearchResponse.getSearchResponse());
                                numStartedAsyncSearch.incrementAndGet();
                            }
                            if (asyncSearchResponse.getError() != null) {
                                numErrorResponseAsyncSearch.incrementAndGet();
                            }
                            finalCountDownLatch.countDown();

                            if (submitAsyncSearchRequest.getKeepOnCompletion()) {
                                DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(asyncSearchResponse.getId());
                                executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, new ActionListener<AcknowledgedResponse>() {
                                    @Override
                                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                        assertTrue(acknowledgedResponse.isAcknowledged());
                                        finalCountDownLatch.countDown();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        fail("Search deletion failed for async search id "+ asyncSearchResponse.getId());
                                        finalCountDownLatch.countDown();
                                    }
                                });
                            };
                        }
                        @Override
                        public void onFailure(Exception e) {
                            numFailedAsyncSearch.incrementAndGet();
                            finalCountDownLatch.countDown();
                        }
                    });
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertionConsumer.apply(numStartedAsyncSearch, numFailedAsyncSearch, numErrorResponseAsyncSearch);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}
