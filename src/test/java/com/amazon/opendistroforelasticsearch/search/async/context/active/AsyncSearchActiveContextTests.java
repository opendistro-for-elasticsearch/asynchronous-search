package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.DELETED;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.SUCCEEDED;

public class AsyncSearchActiveContextTests extends ESTestCase {

    private final String node = UUID.randomUUID().toString();

    public void testAsyncSearchStageTransitionsForAsyncSearchLifeCycle() {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(),
                    threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            assertEquals(context.getAsyncSearchStage(), INIT);
            //Before we register search task
            verifyIllegalStageAdvancementFailure(context, Arrays.asList(FAILED, DELETED, PERSIST_FAILED,
                    PERSISTED, SUCCEEDED));

            //After task is registered, before search result is received
            assertNull(context.getTask());
            SearchTask task = new SearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    null, null, Collections.emptyMap());
            context.setTask(task);
            verifyRunningStageInfo(context, task, keepAlive);

            verifyIllegalStageAdvancementFailure(context, Arrays.asList(RUNNING, INIT));


            boolean success = randomBoolean();
            if (success) {
                //receiving a response from search on completion
                SearchResponse response = getMockSearchResponse();
                context.processSearchResponse(response);
                verifySucceededStageInfo(context, response, new AsyncSearchResponse(AsyncSearchId.buildAsyncId(new AsyncSearchId(node,
                        task.getId(), context.getContextId())), false, task.getStartTime(),
                        task.getStartTime() + keepAlive.millis(), response, null));
            } else {
                //receiving an exception from search
                Exception exception = new RuntimeException("test");
                context.processSearchFailure(exception);
                verifyFailedStageInfo(context, exception, new AsyncSearchResponse(AsyncSearchId.buildAsyncId(new AsyncSearchId(node,
                        task.getId(), context.getContextId())), false, task.getStartTime(),
                        task.getStartTime() + keepAlive.millis(), null, exception));
            }

            // persisting a complete search or failure to do so
            boolean persisted = randomBoolean();
            if (persisted) {
                context.advanceStage(PERSISTED);
                verifyIllegalStageAdvancementFailure(context, Arrays.asList(INIT, RUNNING, SUCCEEDED, PERSISTED,
                        PERSIST_FAILED));
            } else {
                context.advanceStage(PERSIST_FAILED);
                verifyIllegalStageAdvancementFailure(context, Arrays.asList(INIT, RUNNING, SUCCEEDED, PERSISTED,
                        PERSIST_FAILED));
            }

            context.advanceStage(DELETED);
            assertEquals(context.getAsyncSearchStage(), DELETED);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testDeletionOfRunningAsyncSearc() {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(), threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = true;
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            context.setTask(new SearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    null, null, Collections.emptyMap()));
            assertTrue(context.shouldPersist());
            context.advanceStage(DELETED);
            assertFalse(context.shouldPersist());
            assertEquals(context.getAsyncSearchStage(), DELETED);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testAsyncSearchContextProcessesCompletionOnlyOnce() throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(),
                    threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            context.setTask(new SearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    null, null, Collections.emptyMap()));

            int numThread = 10;
            AtomicInteger completions = new AtomicInteger(0);
            AtomicInteger processingFailures = new AtomicInteger(0);
            List<Thread> operationThreads = new ArrayList<>();

            for (int i = 0; i < numThread; i++) {
                //randomly choose to process search or failure
                Runnable runnable = randomBoolean() ? () -> {
                    try {
                        context.processSearchResponse(getMockSearchResponse());
                        completions.getAndIncrement();
                    } catch (IllegalStateException e) {
                        processingFailures.getAndIncrement();
                    }
                } : () -> {
                    try {
                        context.processSearchFailure(new RuntimeException("test"));
                        completions.getAndIncrement();
                    } catch (IllegalStateException e) {
                        processingFailures.getAndIncrement();
                    }
                };
                operationThreads.add(new Thread(runnable));
            }
            operationThreads.forEach(Thread::start);
            for (Thread thread : operationThreads) {
                //not using forEach as method throws Interrupted Exception
                thread.join();
            }
            assertEquals(completions.get(), 1);
            assertEquals(processingFailures.get(), 9);
            assertTrue(context.getAsyncSearchStage() == SUCCEEDED || context.getAsyncSearchStage() == FAILED);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testConcurrentUpdatesToAsyncSearchStage() throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(), threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            doConcurrentStageAdvancement(context, RUNNING, IllegalStateException.class);
            if (randomBoolean()) {//success
                doConcurrentStageAdvancement(context, SUCCEEDED, IllegalStateException.class);
            } else {
                doConcurrentStageAdvancement(context, FAILED, IllegalStateException.class);
            }

            if (randomBoolean()) { //persistence succeeded
                doConcurrentStageAdvancement(context, PERSISTED, IllegalStateException.class);
            } else {
                doConcurrentStageAdvancement(context, PERSIST_FAILED, IllegalStateException.class);
            }

            doConcurrentStageAdvancement(context, DELETED, ResourceNotFoundException.class);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    private <T extends Throwable> void doConcurrentStageAdvancement(AsyncSearchActiveContext context,
                                                                    AsyncSearchStage stage,
                                                                    Class<T> throwable) throws InterruptedException {
        int numThreads = 10;

        List<Thread> operationThreads = new ArrayList<>();
        AtomicInteger numUpdateSuccesses = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    context.advanceStage(stage);
                    numUpdateSuccesses.getAndIncrement();
                } catch (Exception e) {
                    assertTrue(throwable.isInstance(e));
                }
            });
            operationThreads.add(thread);
        }
        operationThreads.forEach(Thread::start);
        for (Thread operationThread : operationThreads) {
            operationThread.join();
        }
        assertEquals(numUpdateSuccesses.get(), 1);
        assertEquals(context.getAsyncSearchStage(), stage);
    }

    private void verifyFailedStageInfo(AsyncSearchActiveContext context, Exception exception,
                                       AsyncSearchResponse asyncSearchResponse) {
        assertNull(context.getSearchResponse());
        assertEquals(context.getSearchError(), exception);
        assertEquals(context.getAsyncSearchResponse(), asyncSearchResponse);
        verifyIllegalStageAdvancementFailure(context, Arrays.asList(INIT, SUCCEEDED, FAILED));
    }

    private void verifySucceededStageInfo(AsyncSearchActiveContext context, SearchResponse response,
                                          AsyncSearchResponse asyncSearchResponse) {
        assertNull(context.getSearchError());
        assertEquals(context.getSearchResponse(), response);
        assertEquals(context.getAsyncSearchResponse(), asyncSearchResponse);
        verifyIllegalStageAdvancementFailure(context, Arrays.asList(INIT, SUCCEEDED, FAILED));

    }

    private void verifyRunningStageInfo(AsyncSearchActiveContext asyncSearchActiveContext, SearchTask task, TimeValue keepAlive) {
        assertEquals(RUNNING, asyncSearchActiveContext.getAsyncSearchStage());
        assertEquals(asyncSearchActiveContext.getTask(), task);
        assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
        assertEquals(task.getStartTime() + keepAlive.millis(), asyncSearchActiveContext.getExpirationTimeMillis());
        assertEquals(asyncSearchActiveContext.getAsyncSearchId(), AsyncSearchId.buildAsyncId(new AsyncSearchId(node,
                task.getId(), asyncSearchActiveContext.getContextId())));
        verifyIllegalStageAdvancementFailure(asyncSearchActiveContext, Arrays.asList(INIT, RUNNING, PERSIST_FAILED, PERSISTED));
    }

    private void verifyIllegalStageAdvancementFailure(AsyncSearchActiveContext asyncSearchActiveContext,
                                                      Collection<AsyncSearchStage> illegalNextStageList) {
        illegalNextStageList.forEach(stage -> expectThrows(IllegalStateException.class,
                () -> asyncSearchActiveContext.advanceStage(stage)));
    }

    private SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
