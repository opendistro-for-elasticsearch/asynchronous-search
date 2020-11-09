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

package com.amazon.opendistroforelasticsearch.search.async.context;

import com.amazon.opendistroforelasticsearch.search.async.context.stage.AsyncSearchStage;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;


public abstract class AsyncSearchContext {

    protected final AsyncSearchActiveContextId asyncSearchActiveContextId;
    protected final LongSupplier currentTimeSupplier;
    protected volatile AsyncSearchProgressListener asyncSearchProgressListener;

    public AsyncSearchContext(AsyncSearchActiveContextId asyncSearchActiveContextId, LongSupplier currentTimeSupplier) {
        Objects.requireNonNull(asyncSearchActiveContextId);
        this.asyncSearchActiveContextId = asyncSearchActiveContextId;
        this.currentTimeSupplier = currentTimeSupplier;
    }

    public @Nullable
    AsyncSearchProgressListener getAsyncSearchProgressListener() {
        return asyncSearchProgressListener;
    }

    public abstract AsyncSearchStage getAsyncSearchStage();

    public boolean isRunning() {
        return getAsyncSearchStage() == AsyncSearchStage.RUNNING;
    }

    public AsyncSearchActiveContextId getContextId() {
        return asyncSearchActiveContextId;
    }

    public abstract String getAsyncSearchId();

    public abstract long getExpirationTimeMillis();

    public abstract long getStartTimeMillis();

    public abstract @Nullable
    SearchResponse getSearchResponse();

    public abstract @Nullable
    Exception getSearchError();

    public boolean isExpired() {
        return getExpirationTimeMillis() < currentTimeSupplier.getAsLong();
    }

    public Set<AsyncSearchStage> retainedStages() {
        return Collections.unmodifiableSet(
                Sets.newHashSet(
                        AsyncSearchStage.INIT, AsyncSearchStage.RUNNING, AsyncSearchStage.SUCCEEDED, AsyncSearchStage.FAILED));
    }

    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(getAsyncSearchId(), isRunning(), getStartTimeMillis(),
                getExpirationTimeMillis(), getSearchResponse(), getSearchError());
    }
}
