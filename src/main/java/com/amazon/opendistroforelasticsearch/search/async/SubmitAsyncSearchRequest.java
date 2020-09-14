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

import com.amazon.opendistroforelasticsearch.search.async.task.SubmitAsyncSearchTask;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SubmitAsyncSearchRequest extends ActionRequest {

    public static long MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1).millis();
    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 1;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 5;
    public static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = TimeValue.timeValueSeconds(1);
    public static Boolean DEFAULT_KEEP_ON_COMPLETION = Boolean.FALSE;
    public static Boolean CCR_MINIMIZE_ROUNDTRIPS = Boolean.FALSE;
    public static Boolean DEFAULT_REQUEST_CACHE = Boolean.TRUE;


    private TimeValue waitForCompletionTimeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
    private Boolean keepOnCompletion = DEFAULT_KEEP_ON_COMPLETION;
    private TimeValue keepAlive = DEFAULT_KEEP_ALIVE;

    private final SearchRequest searchRequest;

    /**
     * Creates a new request
     */
    public SubmitAsyncSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        this.searchRequest.setCcsMinimizeRoundtrips(CCR_MINIMIZE_ROUNDTRIPS);
        this.searchRequest.setBatchedReduceSize(DEFAULT_BATCHED_REDUCE_SIZE);
        this.searchRequest.setPreFilterShardSize(DEFAULT_PRE_FILTER_SHARD_SIZE);
        this.searchRequest.requestCache(DEFAULT_REQUEST_CACHE);
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    /**
     * Get the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    /**
     * Sets the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public void waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }

    /**
     * Returns whether the resource resource should be kept on completion or failure (defaults to false).
     */
    public Boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    /**
     * Determines if the resource should be kept on completion or failure (defaults to false).
     */
    public void keepOnCompletion(boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
    }

    /**
     * Get the amount of time after which the result will expire (defaults to 5 days).
     */
    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Sets the amount of time after which the result will expire (defaults to 5 days).
     */
    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    public SubmitAsyncSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.searchRequest = new SearchRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.searchRequest.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        final ActionRequestValidationException validationException = new ActionRequestValidationException();
        if (searchRequest.isSuggestOnly()) {
            validationException.addValidationError("suggest-only queries are not supported");
        }
        if (searchRequest.isCcsMinimizeRoundtrips()) {
            validationException.addValidationError("[ccs_minimize_roundtrips] must be false, got: "
                    + searchRequest.isCcsMinimizeRoundtrips());
        }
        if (keepAlive != null && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException.addValidationError("[keep_alive] must be greater than 1 minute, got: " + keepAlive.toString());
        }
        if (validationException.validationErrors().isEmpty()) {
            return null;
        }
        return validationException;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubmitAsyncSearchRequest request = (SubmitAsyncSearchRequest) o;
        return Objects.equals(searchRequest, request.searchRequest)
                && Objects.equals(getKeepAlive(), request.getKeepAlive())
                && Objects.equals(getWaitForCompletionTimeout(), request.getWaitForCompletionTimeout())
                && Objects.equals(keepOnCompletion(), request.keepOnCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchRequest, getKeepAlive(), getWaitForCompletionTimeout(), keepOnCompletion());
    }

    @Override
    public SubmitAsyncSearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // generating description in a lazy way since source can be quite big
        SubmitAsyncSearchTask submitAsyncSearchTask = new SubmitAsyncSearchTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                StringBuilder sb = new StringBuilder();
                sb.append("indices[");
                Strings.arrayToDelimitedString(searchRequest.indices(), ",", sb);
                sb.append("], ");
                sb.append("types[");
                Strings.arrayToDelimitedString(searchRequest.types(), ",", sb);
                sb.append("], ");
                sb.append("search_type[").append(searchRequest.types()).append("], ");
                if (searchRequest.source() != null) {
                    sb.append("source[").append(searchRequest.source().toString(SearchRequest.FORMAT_PARAMS)).append("]");
                } else {
                    sb.append("source[]");
                }
                return sb.toString();
            }
        };
        return submitAsyncSearchTask;
    }

    @Override
    public void setParentTask(String parentTaskNode, long parentTaskId) {

    }
}
