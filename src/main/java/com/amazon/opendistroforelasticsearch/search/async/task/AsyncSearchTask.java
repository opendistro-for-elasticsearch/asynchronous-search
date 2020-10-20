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

package com.amazon.opendistroforelasticsearch.search.async.task;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.function.Consumer;

public class AsyncSearchTask extends SearchTask {

    private final AsyncSearchContextId asyncSearchContextId;
    private final Consumer<AsyncSearchContextId> onCancelledContextConsumer;

    public static final String NAME = "indices:data/read/async_search";

    public AsyncSearchTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers,
                           AsyncSearchContextId asyncSearchContextId, Consumer<AsyncSearchContextId> onCancelledContextConsumer) {
        super(id, type, action, null, parentTaskId, headers);
        this.asyncSearchContextId = asyncSearchContextId;
        this.onCancelledContextConsumer = onCancelledContextConsumer;
    }

    @Override
    public String getDescription() {
        return super.getDescription();
    }

    @Override
    protected void onCancelled() {
        onCancelledContextConsumer.accept(asyncSearchContextId);
    }


}
