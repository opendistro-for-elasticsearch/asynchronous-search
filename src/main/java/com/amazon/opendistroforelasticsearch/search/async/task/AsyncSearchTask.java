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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class AsyncSearchTask extends SearchTask {

    private static final Logger logger = LogManager.getLogger(AsyncSearchTask.class);

    private final AsyncSearchContextId asyncSearchContextId;
    private final Supplier<String> asyncSearchIdSupplier;
    private final BiConsumer<String, AsyncSearchContextId> freeContextConsumer;
    private final BooleanSupplier keepOnCompletionSupplier;

    public static final String NAME = "indices:data/read/async_search";

    public AsyncSearchTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers,
                           AsyncSearchContextId asyncSearchContextId, Supplier<String> asyncSearchIdSupplier,
                           BiConsumer<String, AsyncSearchContextId> freeContextConsumer, BooleanSupplier keepOnCompletionSupplier) {
        super(id, type, action, null, parentTaskId, headers);
        Objects.requireNonNull(asyncSearchContextId);
        this.freeContextConsumer = freeContextConsumer;
        this.asyncSearchIdSupplier = asyncSearchIdSupplier;
        this.asyncSearchContextId = asyncSearchContextId;
        this.keepOnCompletionSupplier = keepOnCompletionSupplier;
    }

    @Override
    protected void onCancelled() {
        if (keepOnCompletionSupplier.getAsBoolean()) {
            logger.debug("Cancelling async search context for id {}", asyncSearchIdSupplier.get());
            freeContextConsumer.accept(asyncSearchIdSupplier.get(), asyncSearchContextId);
        }
    }
}
