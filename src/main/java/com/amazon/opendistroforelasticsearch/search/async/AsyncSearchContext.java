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
import org.elasticsearch.action.search.SearchProgressActionListener;

import java.util.Optional;

public abstract class AsyncSearchContext {

    public enum Source {
        IN_MEMORY,
        STORE
    }

    private final AsyncSearchContextId asyncSearchContextId;

    public AsyncSearchContext(AsyncSearchContextId asyncSearchContextId) {
        this.asyncSearchContextId = asyncSearchContextId;
    }

    public Optional<SearchProgressActionListener> getSearchProgressActionListener() { return Optional.empty(); }

    public Optional<ActiveAsyncSearchContext.Stage> getSearchStage() { return Optional.empty(); }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public abstract AsyncSearchId getAsyncSearchId();

    public abstract AsyncSearchResponse getAsyncSearchResponse();

    public abstract long getExpirationTimeMillis();

    public abstract Source getSource();

}
