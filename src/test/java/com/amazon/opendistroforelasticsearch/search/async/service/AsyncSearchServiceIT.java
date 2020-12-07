package com.amazon.opendistroforelasticsearch.search.async.service;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.junit.After;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;

public class AsyncSearchServiceIT extends AsyncSearchSingleNodeTestCase {

    public void testFindContext() throws InterruptedException {
        //create context
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);
        TimeValue keepAlive = timeValueDays(9);
        boolean keepOnCompletion = randomBoolean();
        AsyncSearchContext context = asyncSearchService.createAndStoreContext(keepAlive, keepOnCompletion,
                System.currentTimeMillis());
        assertTrue(context instanceof AsyncSearchActiveContext);
        AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
        assertNull(asyncSearchActiveContext.getTask());
        assertNull(asyncSearchActiveContext.getAsyncSearchId());
        assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);
        //bootstrap search
        AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                emptyMap(),
                context.getContextId(), context::getAsyncSearchId, (a, b) -> {
        }, () -> true);

        asyncSearchService.bootstrapSearch(task, context.getContextId());
        assertEquals(asyncSearchActiveContext.getTask(), task);
        assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
        assertEquals(asyncSearchActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
        assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
        CountDownLatch findContextLatch = new CountDownLatch(1);
        asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(), wrap(
                r -> {
                    try {
                        assertTrue(r instanceof AsyncSearchActiveContext);
                        assertEquals(r, context);
                    } finally {
                        findContextLatch.countDown();
                    }
                }, e -> {
                    try {
                        logger.error(e);
                        fail("Find context shouldn't have failed");
                    } finally {
                        findContextLatch.countDown();
                    }
                }
        ));
        findContextLatch.await();

        AsyncSearchProgressListener asyncSearchProgressListener = asyncSearchActiveContext.getAsyncSearchProgressListener();
        AsyncSearchState completedState;
        if (randomBoolean()) {
            asyncSearchProgressListener.onResponse(getMockSearchResponse());

            while (asyncSearchActiveContext.getAsyncSearchState() == RUNNING) {
                //we wait for search response to be processed
            }
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), SUCCEEDED);
            completedState = SUCCEEDED;

        } else {
            asyncSearchProgressListener.onFailure(new RuntimeException("test"));
            while (asyncSearchActiveContext.getAsyncSearchState() == RUNNING) {
                //we wait for search error to be processed
            }
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), FAILED);
            completedState = FAILED;
        }
        if (keepOnCompletion) { //persist to disk
            while (asyncSearchActiveContext.getAsyncSearchState() == completedState) {
                //we wait for async search response to be persisted
            }
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), PERSISTED);
            CountDownLatch findContextLatch1 = new CountDownLatch(1);
            asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(), wrap(
                    r -> {
                        try {
                            assertNotEquals(r, asyncSearchActiveContext);
                            assertTrue(r instanceof AsyncSearchPersistenceContext);
                        } finally {
                            findContextLatch1.countDown();
                        }
                    }, e -> {
                        try {
                            fail("Find context shouldn't have failed");
                        } finally {
                            findContextLatch1.countDown();
                        }
                    }
            ));
            findContextLatch1.await();
            CountDownLatch freeContextLatch = new CountDownLatch(1);
            asyncSearchService.freeContext(context.getAsyncSearchId(), context.getContextId(), wrap(
                    r -> {
                        try {
                            assertTrue("persistence context should be deleted", r);
                        } finally {
                            freeContextLatch.countDown();
                        }
                    },
                    e -> {
                        try {
                            fail("persistence context should be deleted");
                        } finally {
                            freeContextLatch.countDown();
                        }
                    }
            ));
            freeContextLatch.await();
        } else {
            CountDownLatch findContextLatch1 = new CountDownLatch(1);
            asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(), wrap(
                    r -> {
                        try {
                            fail("Active context should have been removed from contexts map");
                        } finally {
                            findContextLatch1.countDown();
                        }
                    }, e -> {
                        try {
                            assertTrue(e instanceof ResourceNotFoundException);
                        } finally {
                            findContextLatch1.countDown();
                        }
                    }
            ));
            findContextLatch1.await();
            CountDownLatch freeContextLatch = new CountDownLatch(1);
            asyncSearchService.freeContext(context.getAsyncSearchId(), context.getContextId(), wrap(
                    r -> {
                        try {
                            fail("No context should have been deleted");
                        } finally {
                            freeContextLatch.countDown();
                        }
                    },
                    e -> {
                        try {
                            assertTrue(e instanceof ResourceNotFoundException);
                        } finally {
                            freeContextLatch.countDown();
                        }
                    }
            ));
            freeContextLatch.await();
        }

    }

    public void testUpdateExpirationOnRunningSearch() throws InterruptedException {
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);
        TimeValue keepAlive = timeValueDays(9);
        boolean keepOnCompletion = false;
        AsyncSearchContext context = asyncSearchService.createAndStoreContext(keepAlive, keepOnCompletion,
                System.currentTimeMillis());
        assertTrue(context instanceof AsyncSearchActiveContext);
        AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
        assertNull(asyncSearchActiveContext.getTask());
        assertNull(asyncSearchActiveContext.getAsyncSearchId());
        assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);
        //bootstrap search
        AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                emptyMap(),
                context.getContextId(), context::getAsyncSearchId, (a, b) -> {
        }, () -> true);

        asyncSearchService.bootstrapSearch(task, context.getContextId());
        assertEquals(asyncSearchActiveContext.getTask(), task);
        assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
        long originalExpirationTimeMillis = asyncSearchActiveContext.getExpirationTimeMillis();
        assertEquals(originalExpirationTimeMillis, task.getStartTime() + keepAlive.millis());
        assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
        CountDownLatch findContextLatch = new CountDownLatch(1);
        asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(), wrap(
                r -> {
                    try {
                        assertTrue(r instanceof AsyncSearchActiveContext);
                        assertEquals(r, context);
                    } finally {
                        findContextLatch.countDown();
                    }
                }, e -> {
                    try {
                        logger.error(e);
                        fail("Find context shouldn't have failed");
                    } finally {
                        findContextLatch.countDown();
                    }
                }
        ));
        findContextLatch.await();
        CountDownLatch updateLatch = new CountDownLatch(1);
        asyncSearchService.updateKeepAliveAndGetContext(asyncSearchActiveContext.getAsyncSearchId(), keepAlive,
                asyncSearchActiveContext.getContextId(), wrap(r -> {
                    try {
                        assertTrue(r instanceof AsyncSearchActiveContext);
                        assertNotEquals(r.getExpirationTimeMillis(), originalExpirationTimeMillis);
                    } finally {
                        updateLatch.countDown();
                    }
                }, e -> {
                    try {
                        fail();
                    } finally {
                        updateLatch.countDown();
                    }
                }));
        updateLatch.await();

    }

    public void testUpdateExpirationOnPersistedSearch() throws InterruptedException {
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);
        TimeValue keepAlive = timeValueDays(9);
        boolean keepOnCompletion = true; //persist search
        AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(keepAlive,
                keepOnCompletion,
                System.currentTimeMillis());
        AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                emptyMap(),
                asyncSearchActiveContext.getContextId(), asyncSearchActiveContext::getAsyncSearchId, (a, b) -> {
        }, () -> true);

        asyncSearchService.bootstrapSearch(task, asyncSearchActiveContext.getContextId());
        assertEquals(asyncSearchActiveContext.getTask(), task);
        assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
        long originalExpirationTimeMillis = asyncSearchActiveContext.getExpirationTimeMillis();
        assertEquals(originalExpirationTimeMillis, task.getStartTime() + keepAlive.millis());
        assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
        asyncSearchActiveContext.getAsyncSearchProgressListener().onResponse(getMockSearchResponse());
        while (asyncSearchActiveContext.getAsyncSearchState() != PERSISTED) {
            //wait for persistence
        }
        CountDownLatch updateLatch = new CountDownLatch(1);
        asyncSearchService.updateKeepAliveAndGetContext(asyncSearchActiveContext.getAsyncSearchId(), keepAlive,
                asyncSearchActiveContext.getContextId(), wrap(r -> {
                    try {
                        assertTrue(r instanceof AsyncSearchPersistenceContext);
                        assertNotEquals(r.getExpirationTimeMillis(), originalExpirationTimeMillis);
                    } finally {
                        updateLatch.countDown();
                    }
                }, e -> {
                    try {
                        fail();
                    } finally {
                        updateLatch.countDown();
                    }
                }));
        updateLatch.await();

    }

//    public void testUpdateExpirationOnNonExistentSearch() throws InterruptedException {
//        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);
//        AsyncSearchId asyncSearchId = new AsyncSearchId(UUID.randomUUID().toString(), randomNonNegativeLong(),
//                new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong()));
//
//        CountDownLatch updateLatch = new CountDownLatch(1);
//        asyncSearchService.updateKeepAliveAndGetContext(AsyncSearchIdConverter.buildAsyncId(asyncSearchId), timeValueDays(1),
//                asyncSearchId.getAsyncSearchContextId(), wrap(r -> {
//                    try {
//                        fail();
//                    } finally {
//                        updateLatch.countDown();
//                    }
//                }, e -> {
//                    try {
//                        assertTrue(e instanceof ResourceNotFoundException);
//
//                    } finally {
//                        updateLatch.countDown();
//                    }
//                }));
//        updateLatch.await();
//    }


    @After
    public void deleteAsyncSearchIndex() throws InterruptedException {
        CountDownLatch deleteLatch = new CountDownLatch(1);
        client().admin().indices().prepareDelete(INDEX).execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> {
            deleteLatch.countDown();
        }));
        deleteLatch.await();
    }

    private SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
