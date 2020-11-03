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
package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class CompositeActionListenerTest extends ESTestCase {

    private AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
    private AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    private Exception mockSearchException;
    private IOException mockPostProcessingException;
    private SearchResponse mockSearchResponse;
    private AsyncSearchResponse mockAsyncSearchResp;
    private AsyncSearchResponse mockAsyncSearchFailResp;

    @Before
    public void setUpMocks() {
        mockSearchException = new RuntimeException("random-search-exception");
        mockPostProcessingException = new IOException("random-post-processing-exception");
        mockSearchResponse = new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);

        mockAsyncSearchResp = AsyncSearchResponse.empty("random-id", mockSearchResponse, null);
        mockAsyncSearchFailResp = AsyncSearchResponse.empty("random-id", null,
                new ElasticsearchException(mockSearchException));
    }

    public void testListenerOnResponse() throws InterruptedException {
        CheckedFunction<SearchResponse, AsyncSearchResponse, IOException> responseFunction =
                (r) -> {
                    assertTrue(responseRef.compareAndSet(null, r));
                    return mockAsyncSearchResp;
                };
        CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction =
                (e) -> {
                    assertTrue(exceptionRef.compareAndSet(null, e));
                    return mockAsyncSearchFailResp;
                };
        verifyListener(responseFunction, failureFunction, false);
    }

    public void testListenerOnFailure() throws InterruptedException {
        CheckedFunction<SearchResponse, AsyncSearchResponse, IOException> responseFunction =
                (r) -> {
                    assertTrue(responseRef.compareAndSet(null, r));
                    throw mockPostProcessingException;
                };
        CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction =
                (e) -> {
                    assertTrue(exceptionRef.compareAndSet(null, e));
                    throw mockPostProcessingException;
                };
        verifyListener(responseFunction, failureFunction, true);
    }

    public void verifyListener(CheckedFunction<SearchResponse, AsyncSearchResponse, IOException> responseFunction,
                               CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction, boolean onFailure) throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            final AtomicBoolean onSearchResponse = new AtomicBoolean(false);
            threadPool = new TestThreadPool(getClass().getName());
            final int numListeners = randomIntBetween(1, 20);
            final CountDownLatch latch = new CountDownLatch(numListeners);
            final List<AtomicReference<AsyncSearchResponse>> responseList = new ArrayList<>();
            final List<AtomicReference<Exception>> exceptionList = new ArrayList<>();

            CompositeSearchProgressActionListener<AsyncSearchResponse> progressActionListener =
                    new CompositeSearchProgressActionListener<>(
                            responseFunction, failureFunction, threadPool.generic());

            for (int i = 0; i < numListeners; i++) {
                progressActionListener.addOrExecuteListener(createMockListener(responseList, exceptionList, latch));
            }

            if (randomBoolean()) {
                progressActionListener.onFailure(mockSearchException);
            } else {
                assertTrue(onSearchResponse.compareAndSet(false, true));
                progressActionListener.onResponse(mockSearchResponse);
            }
            //wait for all listeners to be executed since on response is forked to a separate thread pool
            latch.await();
            //assert all response listeners that were added were invoked

            //assert search response assertions were invoked
            if (onFailure) {
                assertEquals(numListeners, exceptionList.size());
                if (onSearchResponse.get()) {
                    for (int i = 0; i < numListeners; i++) {
                        //assert all response listeners that were added were invoked with the search response
                        assertEquals(mockPostProcessingException, exceptionList.get(i).get());
                        assertEquals(mockSearchResponse, responseRef.get());
                    }
                } else {
                    //assert search response function was invoked with the same exception on response
                    assertEquals(mockSearchException, exceptionRef.get());
                    for (int i = 0; i < numListeners; i++) {
                        //assert all response listeners that were added were invoked with the exception response
                        assertEquals(mockPostProcessingException, exceptionList.get(i).get());
                    }
                }

            } else {
                assertEquals(numListeners, responseList.size());
                if (onSearchResponse.get()) {
                    for (int i = 0; i < numListeners; i++) {
                        //assert all response listeners that were added were invoked with the search response
                        assertEquals(mockAsyncSearchResp, responseList.get(i).get());
                        assertEquals(mockSearchResponse, responseRef.get());
                    }
                } else {
                    //assert search response function was invoked with the same exception on response
                    assertEquals(mockSearchException, exceptionRef.get());
                    for (int i = 0; i < numListeners; i++) {
                        //assert all response listeners that were added were invoked with the exception response
                        assertEquals(mockAsyncSearchFailResp, responseList.get(i).get());
                    }
                }
            }
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }

    private PrioritizedActionListener<AsyncSearchResponse> createMockListener(List<AtomicReference<AsyncSearchResponse>> responseList,
                                                                              List<AtomicReference<Exception>> exceptionList, CountDownLatch latch) {

        final AtomicBoolean completed = new AtomicBoolean();
        final AtomicReference<AsyncSearchResponse> asyncSearchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        return new PrioritizedActionListener<AsyncSearchResponse>() {
            @Override
            public void executeImmediately() {
                assertTrue(completed.compareAndSet(false, true));
                latch.countDown();
            }

            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                assertTrue(completed.compareAndSet(false, true));
                assertTrue(asyncSearchResponseRef.compareAndSet(null, asyncSearchResponse));
                responseList.add(asyncSearchResponseRef);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(completed.compareAndSet(false, true));
                exceptionRef.compareAndSet(null, e);
                exceptionList.add(exceptionRef);
                latch.countDown();
            }
        };
    }
}

