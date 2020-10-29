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

package com.amazon.opendistroforelasticsearch.search.async.active;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextPermit;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchStage;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;



/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask} and {@link SearchProgressActionListener}
 */
public class AsyncSearchActiveContext extends AsyncSearchContext {

    private static final Logger logger = LogManager.getLogger(AsyncSearchActiveContext.class);

    private final SetOnce<SearchTask> searchTask;
    private volatile long expirationTimeMillis;
    private volatile long startTimeMillis;
    private final Boolean keepOnCompletion;
    private final TimeValue keepAlive;
    private final String nodeId;
    private final SetOnce<String> asyncSearchId;
    private final AtomicBoolean completed;
    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;
    private final AsyncSearchContextPermit asyncSearchContextPermit;
    private volatile AsyncSearchStage asyncSearchStage;
    private final AsyncSearchContextListener contextListener;

    public AsyncSearchActiveContext(AsyncSearchContextId asyncSearchContextId, String nodeId, TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, LongSupplier currentTimeSupplier, AsyncSearchProgressListener searchProgressActionListener,
                                    AsyncSearchContextListener contextListener) {
        super(asyncSearchContextId, currentTimeSupplier);
        this.keepOnCompletion = keepOnCompletion;
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.searchProgressActionListener = searchProgressActionListener;
        this.searchTask = new SetOnce<>();
        this.asyncSearchId = new SetOnce<>();
        this.contextListener = contextListener;
        this.completed = new AtomicBoolean(false);
        this.asyncSearchContextPermit = new AsyncSearchContextPermit(asyncSearchContextId, threadPool);
    }

    @Override
    public AsyncSearchStage getAsyncSearchStage() {
        assert asyncSearchStage != null : "asyncSearchStage cannot be empty";
        return asyncSearchStage;
    }

    @Override
    public String getAsyncSearchId() {
        return asyncSearchId.get();
    }

    public void setTask(SearchTask searchTask) {
        Objects.requireNonNull(searchTask);
        searchTask.setProgressListener(searchProgressActionListener);
        this.searchTask.set(searchTask);
        this.startTimeMillis = searchTask.getStartTime();
        this.expirationTimeMillis = startTimeMillis + keepAlive.getMillis();
        this.asyncSearchId.set(AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, searchTask.getId(), getContextId())));
        advanceStage(AsyncSearchStage.INIT);
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public void setExpirationMillis(long expirationTimeMillis) {
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public boolean shouldPersist() {
        return keepOnCompletion && isExpired() == false && asyncSearchStage != AsyncSearchStage.DELETED;
    }

    public void processSearchFailure(Exception e) {
        if (completed.compareAndSet(false, true)) {
            error.set(new ElasticsearchException(e));
            advanceStage(AsyncSearchStage.FAILED);
        }
    }

    public void processSearchResponse(SearchResponse response) {
        if (completed.compareAndSet(false, true)) {
            this.searchResponse.compareAndSet(null, response);
            advanceStage(AsyncSearchStage.SUCCEEDED);
        }
    }
    @Override
    public SearchResponse getSearchResponse() {
        if (completed.get()) {
            return searchResponse.get();
        } else {
            return ((AsyncSearchProgressListener)searchProgressActionListener).partialResponse();
        }
    }

    @Override
    public ElasticsearchException getSearchError() {
        return error.get();
    }

    @Override
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public long getStartTimeMillis() {
        return startTimeMillis;
    }


    /**
     * Atomically validates and updates the state of the search
     *
     * @param nextAsyncSearchStage asyncSearchStage to advance
     */
    public synchronized void advanceStage(AsyncSearchStage nextAsyncSearchStage) {
        AsyncSearchStage currentAsyncSearchStage = asyncSearchStage;
        if (currentAsyncSearchStage == null) {
            assert nextAsyncSearchStage == AsyncSearchStage.INIT : "only next asyncSearchStage "+ nextAsyncSearchStage +" " +
                    "is allowed when the current asyncSearchStage is null";
        }
        if (currentAsyncSearchStage != null && asyncSearchStage.nextTransitions().contains(nextAsyncSearchStage) == false) {
            throw new IllegalStateException("can't move to asyncSearchStage [" + nextAsyncSearchStage + "], from current asyncSearchStage: ["
                    + currentAsyncSearchStage + "] (valid states [" + currentAsyncSearchStage.nextTransitions() + "])");
        }
        asyncSearchStage = nextAsyncSearchStage;
        if (currentAsyncSearchStage != nextAsyncSearchStage) {
            try {
                asyncSearchStage.onTransition(contextListener, getContextId());
            } catch (Exception ex) {
                logger.error(() -> new ParameterizedMessage("Failed to execute context listener for transition " +
                        "from stage {} to stage {}", currentAsyncSearchStage, nextAsyncSearchStage), ex);
            }
        }
    }

    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asyncSearchId, completed, error, searchResponse, keepAlive, asyncSearchStage);
    }

    @Override
    public String toString() {
        return "AsyncSearchActiveContext{" +
                ", completed=" + completed +
                ", error=" + error +
                ", searchResponse=" + searchResponse +
                ", searchTask=" + searchTask +
                ", expirationTimeNanos=" + expirationTimeMillis +
                ", keepOnCompletion=" + keepOnCompletion +
                ", keepAlive=" + keepAlive +
                ", asyncSearchStage=" + asyncSearchStage +
                ", asyncSearchContextPermit=" + asyncSearchContextPermit +
                ", progressActionListener=" + searchProgressActionListener +
                '}';
    }
}
