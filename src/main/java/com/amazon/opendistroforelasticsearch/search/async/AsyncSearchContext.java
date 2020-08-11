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
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncSearchContext extends AbstractRefCounted implements Releasable {

    private AsyncSearchTask task;
    private boolean isRunning;
    private ResultsHolder resultsHolder;
    private long startTimeMillis;
    private long expirationTimeMillis;
    private TimeValue keepAlive;
    private String nodeId;

    private Boolean keepOnCompletion;

    private AsyncSearchContextId asyncSearchContextId;
    private TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider;

    private Collection<ActionListener<AsyncSearchResponse>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());


    public AsyncSearchContext(String nodeId, AsyncSearchContextId asyncSearchContextId, TimeValue keepAlive, boolean keepOnCompletion, AsyncSearchTask task,
                              TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider) {
        super("async_search_context");
        this.nodeId = nodeId;
        this.asyncSearchContextId = asyncSearchContextId;
        this.task = task;
        this.keepAlive = keepAlive;
        this.keepOnCompletion = keepOnCompletion;
        this.searchTimeProvider = searchTimeProvider;
        this.resultsHolder = new ResultsHolder(searchTimeProvider);
        this.startTimeMillis = searchTimeProvider.getAbsoluteStartMillis();
        this.expirationTimeMillis = startTimeMillis + keepAlive.getMillis();
        this.isRunning = isRunning();
    }

    public AsyncSearchTask getTask() {
        return task;
    }

    public boolean isRunning() {
        return task.isCancelled() == false;
    }

    public ResultsHolder getResultsHolder() {
        return resultsHolder;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public Boolean getKeepOnCompletion() {
        return keepOnCompletion;
    }

    public TransportSubmitAsyncSearchAction.SearchTimeProvider getSearchTimeProvider() {
        return searchTimeProvider;
    }

    public void addListener(ActionListener<AsyncSearchResponse> listener) {
        this.listeners.add(listener);
    }

    public void removeListener(ActionListener<AsyncSearchResponse> listener) {
        this.listeners.remove(listener);
    }

    public Collection<ActionListener<AsyncSearchResponse>> getListeners() {
        return Collections.unmodifiableCollection(listeners);
    }

    public void completeContext(SearchResponse searchResponse) {
        this.resultsHolder.searchResponse = searchResponse;
        this.listeners.clear();
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    @Override
    public void close() {

    }

    @Override
    protected void closeInternal() {

    }

    static class ResultsHolder {
        private SearchHits searchHits;
        private InternalAggregations internalAggregations;
        private InternalSearchResponse internalSearchResponse;
        private SearchResponse searchResponse;
        private SearchResponse.Clusters clusters;
        private AtomicLong version = new AtomicLong();

        ResultsHolder(TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider) {
            this.searchHits = new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), Float.NaN);
            this.internalAggregations = InternalAggregations.EMPTY;
            this.internalSearchResponse = new InternalSearchResponse(searchHits, internalAggregations, null, null, false, null, 0);
            this.searchResponse = new SearchResponse(internalSearchResponse, null, 0, 0, 0, searchTimeProvider.buildTookInMillis(),
                    ShardSearchFailure.EMPTY_ARRAY, clusters);
        }
    }
}
