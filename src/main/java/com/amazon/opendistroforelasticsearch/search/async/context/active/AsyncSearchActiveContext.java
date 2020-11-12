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

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.permits.AsyncSearchContextPermits;
import com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.DELETED;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage.RUNNING;

/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask}
 * and {@link SearchProgressActionListener}
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
    private final SetOnce<Exception> error;
    private final SetOnce<SearchResponse> searchResponse;
    private final AsyncSearchContextPermits asyncSearchContextPermits;
    private volatile AsyncSearchStage asyncSearchStage;
    private final AsyncSearchContextListener contextListener;

    public AsyncSearchActiveContext(AsyncSearchContextId asyncSearchContextId, String nodeId,
                                    TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                    AsyncSearchProgressListener searchProgressActionListener,
                                    AsyncSearchContextListener contextListener) {
        super(asyncSearchContextId, currentTimeSupplier);
        this.keepOnCompletion = keepOnCompletion;
        this.error = new SetOnce<>();
        this.searchResponse = new SetOnce<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.asyncSearchProgressListener = searchProgressActionListener;
        this.searchTask = new SetOnce<>();
        this.asyncSearchId = new SetOnce<>();
        this.contextListener = contextListener;
        this.completed = new AtomicBoolean(false);
        this.asyncSearchContextPermits = new AsyncSearchContextPermits(asyncSearchContextId, threadPool);
        this.asyncSearchStage = INIT;
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
        assert asyncSearchStage == INIT;
        Objects.requireNonNull(searchTask);
        searchTask.setProgressListener(asyncSearchProgressListener);
        this.searchTask.set(searchTask);
        this.startTimeMillis = searchTask.getStartTime();
        this.expirationTimeMillis = startTimeMillis + keepAlive.getMillis();
        this.asyncSearchId.set(AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, searchTask.getId(), getContextId())));
        advanceStage(RUNNING); // context advanced to running stage as search task has been created and search will begin
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public boolean shouldPersist() {
        return keepOnCompletion && isExpired() == false && asyncSearchStage != DELETED;
    }

    public void processSearchFailure(Exception e) {
        assert asyncSearchStage != DELETED : "cannot process search failure. Async search context is already DELETED";
        if (completed.compareAndSet(false, true)) {
            error.set(e);
            advanceStage(AsyncSearchStage.FAILED);
        } else {
            throw new IllegalStateException("Cannot process search failure event for ["
                    + asyncSearchContextId + "] . Search has already completed.");
        }
    }

    public void processSearchResponse(SearchResponse response) {
        assert asyncSearchStage != DELETED : "cannot process search response. Async search context is already DELETED";
        if (completed.compareAndSet(false, true)) {
            this.searchResponse.set(response);
            advanceStage(AsyncSearchStage.SUCCEEDED);
        } else {
            throw new IllegalStateException("Cannot process search response event for ["
                    + asyncSearchContextId + "] . Search has already completed.");
        }
    }

    @Override
    public SearchResponse getSearchResponse() {
        if (completed.get()) {
            return searchResponse.get();
        } else {
            return asyncSearchProgressListener.partialResponse();
        }
    }

    @Override
    public Exception getSearchError() {
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
    //visible for testing
    synchronized void advanceStage(AsyncSearchStage nextAsyncSearchStage) {
        assert nextAsyncSearchStage != null : "Next async search stage cannot bu null!";
        assert this.asyncSearchStage != null : "async search stage cannot be null!";
        AsyncSearchStage currentAsyncSearchStage = this.asyncSearchStage;
        if (this.asyncSearchStage.nextTransitions().contains(nextAsyncSearchStage) == false) {
            // Handle concurrent deletes race condition
            if (currentAsyncSearchStage == DELETED && nextAsyncSearchStage == DELETED) {
                throw new ResourceNotFoundException(getAsyncSearchId());
            }
            throw new IllegalStateException(
                    "can't move to asyncSearchStage [" + nextAsyncSearchStage + "], from current asyncSearchStage: ["
                            + currentAsyncSearchStage + "] (valid states [" + currentAsyncSearchStage.nextTransitions() + "])");
        }
        this.asyncSearchStage = nextAsyncSearchStage;
        this.asyncSearchStage.onTransition(contextListener, getContextId());
    }

    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermits.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermits.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }
}
