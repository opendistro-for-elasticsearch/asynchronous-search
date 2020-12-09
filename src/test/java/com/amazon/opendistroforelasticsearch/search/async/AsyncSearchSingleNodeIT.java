package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
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

public class AsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    public void testDeleteAsyncSearchForRetainedResponse() throws InterruptedException {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        assertConcurrentDeletes(submitResponse.getId(), numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound,
                () -> {
            assertEquals(1, numDeleteAcknowledged.get());
            assertEquals(0, numDeleteUnAcknowledged.get());
            assertEquals(9, numResourceNotFound.get());
        });
    }

    public void testDeleteAsyncSearchNoRetainedResponse() throws InterruptedException {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        assertConcurrentDeletes(submitResponse.getId(), numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound,
                () -> {
            assertEquals(0, numDeleteAcknowledged.get());
            assertEquals(0, numDeleteUnAcknowledged.get());
            assertEquals(10, numResourceNotFound.get());
        });
    }

    public void testDeleteRunningAsyncSearchNoRetainedResponse() throws InterruptedException {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        assertConcurrentDeletes(submitResponse.getId(), numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound,
                () -> {
                    assertEquals(10, numDeleteAcknowledged.get() + numResourceNotFound.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                });
    }

    private void assertConcurrentDeletes(String id, AtomicInteger numDeleteAcknowledged, AtomicInteger numDeleteUnAcknowledged,
    AtomicInteger numResourceNotFound, Runnable assertionRunnable) throws InterruptedException {
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsyncSearchSingleNodeIT.class.getName());
            int numThreads = 10;
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
            assertionRunnable.run();
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}
