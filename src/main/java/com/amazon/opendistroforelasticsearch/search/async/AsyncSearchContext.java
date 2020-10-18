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


import com.amazon.opendistroforelasticsearch.search.async.active.ActiveAsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;


public abstract class AsyncSearchContext {

    public enum Source {
        IN_MEMORY,
        STORE
    }

    private final AsyncSearchContextId asyncSearchContextId;

    public AsyncSearchContext(AsyncSearchContextId asyncSearchContextId) {
        this.asyncSearchContextId = asyncSearchContextId;
    }

    public @Nullable SearchProgressActionListener getSearchProgressActionListener() { return null; }

    public @Nullable ActiveAsyncSearchContext.Stage getSearchStage() { return null; }

    public @Nullable ElasticsearchException getError() { return null; }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public abstract AsyncSearchId getAsyncSearchId();

    public abstract boolean isRunning();

    public abstract long getExpirationTimeMillis();

    public abstract long getStartTimeMillis();

    public abstract SearchResponse getSearchResponse();

    public abstract Source getSource();

    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(AsyncSearchId.buildAsyncId(getAsyncSearchId()), isRunning(), getStartTimeMillis(),
                getExpirationTimeMillis(), getSearchResponse(), getError() == null ? getError() : null);
    }
}
