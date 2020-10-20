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

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextPermit;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;

/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask} and {@link SearchProgressActionListener}
 */
public class AsyncSearchActiveContext extends AsyncSearchContext {

    private volatile SetOnce<SearchTask> searchTask;
    private volatile long expirationTimeMillis;
    private volatile long startTimeMillis;
    private final Boolean keepOnCompletion;
    private volatile TimeValue keepAlive;
    private final String nodeId;
    private volatile SetOnce<AsyncSearchId> asyncSearchId;
    private final AsyncSearchContextListener contextListener;
    private AtomicBoolean completed;
    private volatile Stage stage;
    private AtomicReference<ElasticsearchException> error;
    private AtomicReference<SearchResponse> searchResponse;
    private final AsyncSearchContextPermit asyncSearchContextPermit;

    public AsyncSearchActiveContext(AsyncSearchContextId asyncSearchContextId, String nodeId, TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, AsyncSearchProgressListener searchProgressActionListener,
                                    AsyncSearchContextListener contextListener) {
        super(asyncSearchContextId);
        this.keepOnCompletion = keepOnCompletion;
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.searchProgressActionListener = searchProgressActionListener;
        this.startTimeMillis = threadPool.absoluteTimeInMillis();
        this.searchTask = new SetOnce<>();
        this.asyncSearchId = new SetOnce<>();
        this.contextListener = contextListener;
        this.completed = new AtomicBoolean(false);
        this.asyncSearchContextPermit = new AsyncSearchContextPermit(asyncSearchContextId, threadPool);
    }

    @Override
    public Stage getStage() {
        assert stage != null : "stage cannot be empty";
        return stage;
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
        return asyncSearchId.get();
    }

    public void setTask(SearchTask searchTask) {
        Objects.requireNonNull(searchTask);
        searchTask.setProgressListener(searchProgressActionListener);
        this.searchTask.set(searchTask);
        this.startTimeMillis = searchTask.getStartTime();
        this.asyncSearchId.set(new AsyncSearchId(nodeId, searchTask.getId(), getAsyncSearchContextId()));
        this.setStage(Stage.INIT);
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
        return keepOnCompletion;
    }

    public void processSearchFailure(Exception e) {
        if (completed.compareAndSet(false, true)) {
            error.set(new ElasticsearchException(e));
            searchProgressActionListener = null;
            setStage(Stage.FAILED);
        }
    }

    public void processSearchSuccess(SearchResponse response) {
        if (completed.compareAndSet(false, true)) {
            this.searchResponse.compareAndSet(null, response);
            searchProgressActionListener = null;
            setStage(Stage.SUCCEEDED);
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
     * @param stage stage to set
     */
    public synchronized void setStage(Stage stage) {
        switch (stage) {
            case INIT:
                validateAndSetStage(null, stage);
                contextListener.onNewContext(getAsyncSearchContextId());
                break;
            case RUNNING:
                assert searchTask.get() != null : "search task cannot be null";
                contextListener.onContextRunning(getAsyncSearchContextId());
                validateAndSetStage(Stage.INIT, stage);
                break;
            case SUCCEEDED:
                assert searchTask.get() == null || searchTask.get().isCancelled() == false : "search task is cancelled";
                validateAndSetStage(Stage.RUNNING, stage);
                contextListener.onContextCompleted(getAsyncSearchContextId());
                break;
            case ABORTED:
                assert searchTask.get() == null || searchTask.get().isCancelled() : "search task should be cancelled";
                validateAndSetStage(Stage.RUNNING, stage);
                contextListener.onContextCancelled(getAsyncSearchContextId());
                break;
            case FAILED:
                validateAndSetStage(Stage.RUNNING, stage);
                contextListener.onContextFailed(getAsyncSearchContextId());
                break;
            case PERSISTED:
                validateAndSetStage(Stage.SUCCEEDED, stage);
                contextListener.onContextPersisted(getAsyncSearchContextId());
                break;
            case PERSIST_FAILED:
                validateAndSetStage(Stage.SUCCEEDED, stage);
                break;
            case DELETED:
                //any state except INIT can be moved to DELETED
                this.stage = stage;
                break;
            default:
                throw new IllegalArgumentException("Unknown stage [" + stage + "]");
        }
    }

    private void validateAndSetStage(Stage expected, Stage next) {
        // Once DELETED the stage shouldn't progress any further
        if (stage != expected || (expected == Stage.DELETED && next != Stage.DELETED)) {
            throw new IllegalStateException("can't move to stage [" + next + "]. current stage: ["
                    + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asyncSearchId, completed, error, searchResponse, keepAlive, stage);
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
                ", stage=" + stage +
                ", asyncSearchContextPermit=" + asyncSearchContextPermit +
                ", progressActionListener=" + searchProgressActionListener +
                '}';
    }
}
