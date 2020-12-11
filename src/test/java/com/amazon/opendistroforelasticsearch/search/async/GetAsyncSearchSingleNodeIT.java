package com.amazon.opendistroforelasticsearch.search.async;

/*
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
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

public class GetAsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {


    public void testNoUpdateAsyncSearchForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        assertConcurrentGetOrUpdates(submitResponse,
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(10, numDeleteAcknowledged.get() + numResourceNotFound.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                }, false);
    }

    private void assertConcurrentGetOrUpdates(AsyncSearchResponse submitResponse,
                                              TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer, boolean update)
            throws InterruptedException {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(GetAsyncSearchSingleNodeIT.class.getName());
            int numThreads = randomIntBetween(20, 50);
            long lowerKeepAliveHours = 50;
            long higherKeepAliveHours = 100;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering async search gets with keep alives --->");
                    GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
                    if (update) {
                        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueHours(randomLongBetween(lowerKeepAliveHours, higherKeepAliveHours)));
                    }
                    executeGetAsyncSearch(client(), getAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                            if (update) {
                                //assertGre

                            } else {
                                assertEquals(submitResponse, asyncSearchResponse);
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
}
*/
