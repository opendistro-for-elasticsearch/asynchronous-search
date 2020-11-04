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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;

import java.util.*;
import java.util.function.LongSupplier;


public abstract class AsyncSearchContext {

    protected final AsyncSearchContextId asyncSearchContextId;
    protected final LongSupplier currentTimeSupplier;
    protected volatile SearchProgressActionListener searchProgressActionListener;

    public AsyncSearchContext(AsyncSearchContextId asyncSearchContextId, LongSupplier currentTimeSupplier) {
        Objects.requireNonNull(asyncSearchContextId);
        this.asyncSearchContextId = asyncSearchContextId;
        this.currentTimeSupplier = currentTimeSupplier;
    }

    public @Nullable SearchProgressActionListener getSearchProgressActionListener() { return searchProgressActionListener; }

    public abstract AsyncSearchStage getAsyncSearchStage();

    public boolean isRunning() {
        return getAsyncSearchStage() == AsyncSearchStage.RUNNING;
    }

    public AsyncSearchContextId getContextId() {
        return asyncSearchContextId;
    }

    public abstract String getAsyncSearchId();

    public abstract long getExpirationTimeMillis();

    public abstract long getStartTimeMillis();

    public abstract @Nullable SearchResponse getSearchResponse();

    public abstract @Nullable ElasticsearchException getSearchError();

    public boolean isExpired() {
        return getExpirationTimeMillis() < currentTimeSupplier.getAsLong();
    }

    public Set<AsyncSearchStage> retainedStages() {
        return Collections.unmodifiableSet(Sets.newHashSet(AsyncSearchStage.INIT, AsyncSearchStage.RUNNING, AsyncSearchStage.SUCCEEDED, AsyncSearchStage.FAILED));
    }

    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(getAsyncSearchId(), isRunning(), getStartTimeMillis(),
                getExpirationTimeMillis(), getSearchResponse(), getSearchError());
    }
}
