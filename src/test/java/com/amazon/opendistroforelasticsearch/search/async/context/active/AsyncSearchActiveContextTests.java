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


package com.amazon.opendistroforelasticsearch.search.async.context.active;
/*
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.exception.AsyncSearchStateMachineException;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;

public class AsyncSearchActiveContextTests extends AsyncSearchSingleNodeTestCase {

    public void testAsyncSearchTransitions() throws InterruptedException, IOException, BrokenBarrierException {
        TestThreadPool threadPool = null;
        try {
            int writeThreadPoolSize = randomIntBetween(1, 2);
            int writeThreadPoolQueueSize = randomIntBetween(1, 2);
            Settings settings = Settings.builder()
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("IndexShardOperationPermitsTests", settings, scalingExecutorBuilder);
            AsyncSearchStateMachine stateMachine = getInstanceFromNode(AsyncSearchStateMachine.class);
            String node = UUID.randomUUID().toString();
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
            assertNull(context.getTask());
            assertNull(context.getAsyncSearchId());
            assertEquals(context.getAsyncSearchState(), AsyncSearchState.INIT);
            SearchTask task = new SearchTask(randomNonNegativeLong(), "transport",
                    SearchAction.NAME, null, null, Collections.emptyMap());
            doConcurrentStageAdvancement(stateMachine, new SearchStartedEvent(context, task), RUNNING,
                    AsyncSearchStateMachineException.class,
                    threadPool);
            assertEquals(context.getAsyncSearchId(), AsyncSearchIdConverter.buildAsyncId(new AsyncSearchId(node, task.getId(),
                    asyncSearchContextId)));
            assertEquals(task, context.getTask());
            assertEquals(context.getAsyncSearchState(), RUNNING);
            assertEquals(context.getStartTimeMillis(), task.getStartTime());
            assertEquals(context.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            if (randomBoolean()) {//success
                doConcurrentStageAdvancement(stateMachine, new SearchSuccessfulEvent(context, getMockSearchResponse()), SUCCEEDED,
                        AsyncSearchStateMachineException.class, threadPool);
                assertNotNull(context.getSearchResponse());
                assertNull(context.getSearchError());
            } else { //failure
                doConcurrentStageAdvancement(stateMachine, new SearchFailureEvent(context, new RuntimeException("test")), FAILED,
                        AsyncSearchStateMachineException.class, threadPool);
                assertNull(context.getSearchResponse());
                assertNotNull(context.getSearchError());
            }

            doConcurrentStageAdvancement(stateMachine, new BeginPersistEvent(context),
                    PERSISTING,
                    AsyncSearchStateMachineException.class, threadPool);

            while (context.getAsyncSearchState() != PERSISTED &&     context.getAsyncSearchState() != PERSIST_FAILED) {
                //wait for persistence
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    private <T extends Throwable> void doConcurrentStageAdvancement(AsyncSearchStateMachine asyncSearchStateMachine,
                                                                    AsyncSearchContextEvent event,
                                                                    AsyncSearchState finalState,
                                                                    Class<T> throwable, ThreadPool threadPool) throws InterruptedException,
            BrokenBarrierException {
        int numThreads = 10;
        List<Runnable> operationThreads = new ArrayList<>();
        AtomicInteger numUpdateSuccesses = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(11);
        for (int i = 0; i < numThreads; i++) {
            Runnable thread = () -> {
                try {
                    asyncSearchStateMachine.trigger(event);
                    numUpdateSuccesses.getAndIncrement();
                } catch (Exception e) {
                    assertTrue(throwable.isInstance(e));
                } finally {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        fail("stage advancement failure");
                    }
                }
            };
            operationThreads.add(thread);
        }
        operationThreads.forEach(runnable -> {
            threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME).execute(runnable);
        });
        barrier.await();
        assertEquals(event.toString(), 1, numUpdateSuccesses.get());
        assertEquals(event.asyncSearchContext().getAsyncSearchState(), finalState);
    }

    @After
    public void deleteAsyncSearchIndex() throws InterruptedException {
        CountDownLatch deleteLatch = new CountDownLatch(1);
        client().admin().indices().prepareDelete(INDEX).execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> {
            deleteLatch.countDown();
        }));
        deleteLatch.await();
    }

    protected SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}

*/
