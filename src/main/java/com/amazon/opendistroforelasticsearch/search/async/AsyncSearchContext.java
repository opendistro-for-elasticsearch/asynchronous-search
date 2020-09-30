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


public abstract class AsyncSearchContext {

    public enum Lifetime {
        IN_MEMORY,
        STORE
    }

    private final String asyncId;

    public AsyncSearchContext(String asyncId) {
        this.asyncId = asyncId;
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return getParsedAsyncSearchId().getAsyncSearchContextId();
    }

    public AsyncSearchId getParsedAsyncSearchId() {
        return AsyncSearchId.parseAsyncId(asyncId);
    }

    public String getAsyncSearchId() {
        return asyncId;
    }

    public abstract AsyncSearchResponse getAsyncSearchResponse();

    public abstract long getExpirationTimeInMills();

    public abstract Lifetime getLifetime();

    public boolean isExpired() {
        return System.currentTimeMillis() > getExpirationTimeInMills();
    }

}
