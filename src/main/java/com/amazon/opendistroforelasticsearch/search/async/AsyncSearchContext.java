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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncSearchContext extends AbstractRefCounted implements Releasable {

    private static final Logger logger = LogManager.getLogger(AsyncSearchContext.class);

    private final AtomicBoolean isRunning;
    private final AtomicBoolean isPartial;
    private final AtomicBoolean isCompleted;
    private final AtomicBoolean persisted;

    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;
    private AtomicReference<SearchTask> searchTask;
    private final String nodeId;
    private final Boolean keepOnCompletion;
    private final AsyncSearchContextId asyncSearchContextId;
    private final ResultsHolder resultsHolder;
    private TimeValue keepAlive;


    public AsyncSearchContext(String nodeId, AsyncSearchContextId asyncSearchContextId, TimeValue keepAlive, boolean keepOnCompletion,
                              AtomicReference<SearchTask> searchTask) {
        super("async_search_context");
        this.nodeId = nodeId;
        this.asyncSearchContextId = asyncSearchContextId;
        this.keepOnCompletion = keepOnCompletion;
        this.resultsHolder = new ResultsHolder();
        this.isRunning = new AtomicBoolean(true);
        this.isPartial = new AtomicBoolean(true);
        this.isCompleted = new AtomicBoolean(false);
        this.persisted = new AtomicBoolean(false);
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.searchTask = searchTask;
        this.keepAlive = keepAlive;
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public ResultsHolder getResultsHolder() {
        return resultsHolder;
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

    public AsyncSearchResponse getAsyncSearchResponse() throws IOException {
        return new AsyncSearchResponse(getId(), isPartial(), isRunning(), searchTask.get().getStartTime(), getExpirationTimeMillis(),
                        isRunning() ? resultsHolder.buildPartialSearchResponse() : getFinalSearchResponse(), error.get());


    }

    public String getId() throws IOException {
        return AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, asyncSearchContextId));
    }

    public SearchResponse getFinalSearchResponse() {
        return searchResponse.get();
    }

    public boolean isRunning() {
        return !getTask().isCancelled() && isRunning.get();
    }

    public boolean isCancelled() {
        return getTask().isCancelled();
    }

    public boolean isPartial() {
        return isPartial.get();
    }

    public long getExpirationTimeMillis() {
        return searchTask.get().getStartTime() + keepAlive.getMillis();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > getExpirationTimeMillis();
    }


    public void processFailure(Exception e) {
        this.isCompleted.set(true);
        this.isPartial.set(false);
        this.isRunning.set(false);
        error.set(new ElasticsearchException(e));
    }


    public void processFinalResponse(SearchResponse response) {
        this.searchResponse.compareAndSet(null, response);
        this.isCompleted.set(true);
        this.isRunning.set(false);
        this.isPartial.set(false);
    }

    public synchronized void addShardFailure(ShardSearchFailure failure) {
        resultsHolder.shardSearchFailuresFailures.add(failure);
    }

    /**
     * @param reducePhase Version of reduce. If reducePhase version in resultHolder is greater than the event's reducePhase version, this event can be discarded.
     */
    public synchronized void updateResultFromReduceEvent(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        resultsHolder.successfulShards.set(shards.size());
        resultsHolder.internalAggregations = aggs;
        resultsHolder.reducePhase.set(reducePhase);
        resultsHolder.totalHits = totalHits;
    }

    public synchronized void updateResultFromReduceEvent(InternalAggregations aggs, TotalHits totalHits, int reducePhase) {
        if (resultsHolder.reducePhase.get() > reducePhase) {
            logger.warn("ResultHolder reducePhase version {} is ahead of the event reducePhase version {}. Discarding event",
                    resultsHolder.reducePhase, reducePhase);
            return;
        }
        resultsHolder.totalHits = totalHits;
        resultsHolder.internalAggregations = aggs;
        resultsHolder.reducePhase.set(reducePhase);
    }

    public synchronized void initialiseResultHolderShardLists(
            List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters, boolean fetchPhase) {
        resultsHolder.totalShards.set(shards.size());
        resultsHolder.skippedShards.set(skippedShards.size());
        resultsHolder.clusters = clusters;
        resultsHolder.isResponseInitialized.set(true);
    }

    public synchronized void incrementSuccessfulShards() {
        resultsHolder.successfulShards.incrementAndGet();
    }

    class ResultsHolder {
        private AtomicInteger reducePhase;
        private TotalHits totalHits;
        private InternalAggregations internalAggregations;
        private AtomicBoolean isResponseInitialized;
        private AtomicInteger totalShards;
        private AtomicInteger successfulShards;
        private AtomicInteger skippedShards;
        private SearchResponse.Clusters clusters;
        private final List<ShardSearchFailure> shardSearchFailuresFailures;

        ResultsHolder() {
            this.internalAggregations = InternalAggregations.EMPTY;
            this.shardSearchFailuresFailures = new ArrayList<>();
            totalShards = new AtomicInteger();
            successfulShards = new AtomicInteger();
            skippedShards = new AtomicInteger();
            reducePhase = new AtomicInteger();
            isResponseInitialized = new AtomicBoolean(false);
        }

        private SearchResponse buildPartialSearchResponse() {
            if (isResponseInitialized.get()) {
                SearchHits searchHits = new SearchHits(SearchHits.EMPTY, totalHits, Float.NaN);
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, internalAggregations,
                        null, null, false, false, reducePhase.get());
                ShardSearchFailure[] shardSearchFailures = shardSearchFailuresFailures.toArray(new ShardSearchFailure[]{});
                long tookInMillis = System.currentTimeMillis() - searchTask.get().getStartTime();
                return new SearchResponse(internalSearchResponse, null, totalShards.get(),
                        successfulShards.get(), skippedShards.get(), tookInMillis, shardSearchFailures,
                        clusters);
            } else {
                return null;
            }
        }
    }
}
