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

import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncSearchContext extends AbstractRefCounted implements Releasable {

    private AsyncSearchTask asyncSearchTask;
    private boolean isCancelled;
    private boolean isRunning;
    private ParsedAsyncSearchId asyncSearchId;
    private AtomicInteger version;
    private AsyncSearchContextId asyncSearchContextId;
    private Collection<ActionListener<AsyncSearchResponse>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());


    public AsyncSearchContext(AsyncSearchTask asyncSearchTask, AsyncSearchContextId asyncSearchContextId) {
        super("async_search_context");
        this.asyncSearchTask = asyncSearchTask;
        this.asyncSearchContextId = asyncSearchContextId;
    }

    public void addListener(ActionListener<AsyncSearchResponse> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (listeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }
        this.listeners.add(listener);
    }

    public void removeListener(ActionListener<AsyncSearchResponse> listener) {
        this.listeners.remove(listener);
    }

    public Collection<ActionListener<AsyncSearchResponse>> getListeners() {
        return Collections.unmodifiableCollection(listeners);
    }

    @Override
    public void close() {

    }

    @Override
    protected void closeInternal() {

    }
}
