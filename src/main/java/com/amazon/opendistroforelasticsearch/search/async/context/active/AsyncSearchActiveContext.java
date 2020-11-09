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
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchActiveContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.permits.AsyncSearchContextPermits;
import com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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

    public AsyncSearchActiveContext(AsyncSearchActiveContextId asyncSearchActiveContextId, String nodeId,
                                    TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                    AsyncSearchProgressListener searchProgressActionListener,
                                    AsyncSearchContextListener contextListener) {
        super(asyncSearchActiveContextId, currentTimeSupplier);
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
        this.asyncSearchContextPermits = new AsyncSearchContextPermits(asyncSearchActiveContextId, threadPool);
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
        searchTask.setProgressListener(asyncSearchProgressListener);
        this.searchTask.set(searchTask);
        this.startTimeMillis = searchTask.getStartTime();
        this.expirationTimeMillis = startTimeMillis + keepAlive.getMillis();
        this.asyncSearchId.set(AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, searchTask.getId(), getContextId())));
        advanceStage(INIT);
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public boolean shouldPersist() {
        return keepOnCompletion && isExpired() == false && asyncSearchStage != DELETED;
    }

    public void processSearchFailure(Exception e) {
        if (completed.compareAndSet(false, true)) {
            error.set(e);
            advanceStage(AsyncSearchStage.FAILED);
        } else {
            throw new IllegalStateException("Cannot process search failure event for ["
                    + asyncSearchActiveContextId + "] . Search has already completed.");
        }
    }

    public void processSearchResponse(SearchResponse response) {

        if (completed.compareAndSet(false, true)) {
            this.searchResponse.set(response);
            advanceStage(AsyncSearchStage.SUCCEEDED);
        } else {
            throw new IllegalStateException("Cannot process search response event for ["
                    + asyncSearchActiveContextId + "] . Search has already completed.");
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
    public synchronized void advanceStage(AsyncSearchStage nextAsyncSearchStage) {
        AsyncSearchStage currentAsyncSearchStage = asyncSearchStage;

        if (currentAsyncSearchStage == null && nextAsyncSearchStage != INIT) {
            throw new IllegalStateException("only next asyncSearchStage " + INIT +
                    " is allowed when the current asyncSearchStage is null");
        } else if (currentAsyncSearchStage != null &&
                asyncSearchStage.nextTransitions().contains(nextAsyncSearchStage) == false) {
            // Handle concurrent deletes race condition
            if (currentAsyncSearchStage == DELETED && nextAsyncSearchStage == DELETED) {
                throw new ResourceNotFoundException(getAsyncSearchId());
            }
            throw new IllegalStateException(
                    "can't move to asyncSearchStage [" + nextAsyncSearchStage + "], from current asyncSearchStage: ["
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
        asyncSearchContextPermits.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermits.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }
}
