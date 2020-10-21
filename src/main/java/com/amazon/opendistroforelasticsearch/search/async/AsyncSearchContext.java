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

import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;


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
         * The search execution has failed
         */
        FAILED,
        /**
         * The context has been persisted to system index
         */
        PERSISTED,
        /**
         * The context has failed to persist to system index
         */
        PERSIST_FAILED,
        /**
         * The context has been deleted
         */
        DELETED

    }

    protected final AsyncSearchContextId asyncSearchContextId;
    protected final LongSupplier currentTimeSupplier;
    protected volatile SearchProgressActionListener searchProgressActionListener;

    public AsyncSearchContext(AsyncSearchContextId asyncSearchContextId, LongSupplier currentTimeSupplier) {
        Objects.requireNonNull(asyncSearchContextId);
        this.asyncSearchContextId = asyncSearchContextId;
        this.currentTimeSupplier = currentTimeSupplier;
    }

    public @Nullable SearchProgressActionListener getSearchProgressActionListener() { return searchProgressActionListener; }

    public abstract Stage getStage();

    public boolean isRunning() {
        return getStage() == Stage.RUNNING;
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public abstract AsyncSearchId getAsyncSearchId();

    public abstract long getExpirationTimeMillis();

    public abstract long getStartTimeMillis();

    public abstract @Nullable SearchResponse getSearchResponse();

    public abstract @Nullable ElasticsearchException getSearchError();

    public boolean isExpired() {
        return getExpirationTimeMillis() < currentTimeSupplier.getAsLong();
    }

    public Set<Stage> retainedStages() {
        return Collections.unmodifiableSet(Sets.newHashSet(Stage.INIT, Stage.RUNNING, Stage.SUCCEEDED, Stage.FAILED));
    }

    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(AsyncSearchId.buildAsyncId(getAsyncSearchId()), isRunning(), getStartTimeMillis(),
                getExpirationTimeMillis(), getSearchResponse(), getSearchError());
    }
}
