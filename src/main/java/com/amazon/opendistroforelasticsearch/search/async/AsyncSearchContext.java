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

package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.reaper.AsyncSearchManagementService;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;



public abstract class AsyncSearchContext {

    /**
     * The state of the async search.
     */
    public enum Stage {
        /**
         * At the start of the search, before the {@link SearchTask starts to run}
         */
        INIT,
        /**
         * The search state actually has been started
         */
        RUNNING,
        /**
         * The search has completed successfully
         */
        SUCCEEDED,
        /**
         * The search has been cancelled either by the user, task API cancel or {@link AsyncSearchManagementService}
         */
        ABORTED,
        /**
         * The context has been persisted to system index
         */
        PERSISTED,
        /**
         * The context has failed to persist to system index
         */
        PERSIST_FAILED,
        /**
         * The search execution has failed
         */
        FAILED
    }

    protected final AsyncSearchContextId asyncSearchContextId;
    protected volatile Stage stage;
    protected final AsyncSearchContextPermit asyncSearchContextPermit;
    protected volatile SearchProgressActionListener searchProgressActionListener;

    public AsyncSearchContext(AsyncSearchContextId asyncSearchContextId) {
        this.asyncSearchContextId = asyncSearchContextId;
        this.asyncSearchContextPermit = new AsyncSearchContextPermit(asyncSearchContextId);
    }

    public @Nullable SearchProgressActionListener getSearchProgressActionListener() { return searchProgressActionListener; }

    public abstract Stage getContextStage();

    public boolean isRunning() {
        return stage == Stage.RUNNING;
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public abstract AsyncSearchId getAsyncSearchId();

    public abstract long getExpirationTimeMillis();

    public abstract long getStartTimeMillis();

    public abstract @Nullable SearchResponse getSearchResponse();

    public abstract @Nullable ElasticsearchException getSearchError();

    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(AsyncSearchId.buildAsyncId(getAsyncSearchId()), isRunning(), getStartTimeMillis(),
                getExpirationTimeMillis(), getSearchResponse(), getSearchError());
    }

    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, ThreadPool threadPool, String reason) {
        asyncSearchContextPermit.asyncAcquirePermit(onPermitAcquired, timeout, threadPool, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, ThreadPool threadPool, String reason) {
        asyncSearchContextPermit.asyncAcquireAllPermits(onPermitAcquired, timeout, threadPool, reason);
    }
}
