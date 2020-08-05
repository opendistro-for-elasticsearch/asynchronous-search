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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class SubmitAsyncSearchRequest extends ActionRequest implements IndicesRequest.Replaceable {

    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 1;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 5;

    private int batchedReduceSize = DEFAULT_BATCHED_REDUCE_SIZE;
    private TimeValue waitForCompletionTimeout;

    private SearchRequest searchRequest;

    public SubmitAsyncSearchRequest() {
        super();
    }

    public SubmitAsyncSearchRequest(StreamInput streamInput) {
        super();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public AsyncSearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // generating description in a lazy way since source can be quite big
        AsyncSearchTask asyncSearchTask = new AsyncSearchTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                StringBuilder sb = new StringBuilder();
                return sb.toString();
            }
        };
        return asyncSearchTask;
    }

    /**
     * Returns the number of shard results that should be reduced at once on the coordinating node. This value should be used as a
     * protection mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public int getBatchedReduceSize() {
        return batchedReduceSize;
    }

    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public void setWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        return null;
    }

    @Override
    public String[] indices() {
        return new String[0];
    }

    @Override
    public IndicesOptions indicesOptions() {
        return null;
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }
}
