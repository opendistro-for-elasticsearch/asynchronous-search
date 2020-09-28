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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.COMPLETED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.RUNNING;

public class AsyncSearchContext implements Releasable {


    public enum Stage {
        INIT,
        RUNNING,
        COMPLETED,
        FAILED,
        PERSISTED,
        EXPIRED,
        ABORTED;

    }

    private static final Logger logger = LogManager.getLogger(AsyncSearchContext.class);
    private final AtomicBoolean isRunning;

    private final AtomicBoolean isPartial;

    private final AtomicBoolean isCompleted;
    private final AtomicBoolean persisted;
    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;

    private SetOnce<SearchTask> searchTask = new SetOnce<>();
    private final String nodeId;
    private volatile long expirationTimeInMills;
    private final Boolean keepOnCompletion;
    private final AsyncSearchContextId asyncSearchContextId;
    private final AtomicReference<ResultsHolder> resultsHolder = new AtomicReference<>();
    private volatile TimeValue keepAlive;
    private volatile Stage stage;

    public AsyncSearchContext(String nodeId, AsyncSearchContextId asyncSearchContextId, TimeValue keepAlive, boolean keepOnCompletion) {
        this.nodeId = nodeId;
        this.asyncSearchContextId = asyncSearchContextId;
        this.keepOnCompletion = keepOnCompletion;
        this.isRunning = new AtomicBoolean(true);
        this.isPartial = new AtomicBoolean(true);
        this.isCompleted = new AtomicBoolean(false);
        this.persisted = new AtomicBoolean(false);
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.keepAlive = keepAlive;
        this.resultsHolder.set(new ResultsHolder(this::getStartTimeMillis));
        this.stage = INIT;
    }

    public void setTask(SearchTask searchTask) {
        this.searchTask.set(searchTask);
        this.setExpirationMillis(searchTask.getStartTime() + keepAlive.getMillis());
        setStage(RUNNING);
    }

    public Stage getStage() {
        return stage;
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public ResultsHolder getResultsHolder() {
        return resultsHolder.get();
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public AsyncSearchResponse geLatestSearchResponse() {
        if (isCancelled()) {
            throw new ResourceNotFoundException("Search cancelled");
        }
        return new AsyncSearchResponse(getId(), isPartial(), isRunning(), searchTask.get().getStartTime(), getExpirationTimeMillis(),
                isRunning() ? buildPartialSearchResponse() : getFinalSearchResponse(), error.get());

    }

    private SearchResponse buildPartialSearchResponse() {
        return resultsHolder.get().buildPartialSearchResponse();
    }

    public String getId() {
        try {
            return AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, asyncSearchContextId));
        } catch (IOException e) {
            return null;
        }
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
        return expirationTimeInMills;
    }

    public void setExpirationMillis(long expirationTimeInMills) {
        this.expirationTimeInMills = expirationTimeInMills;
    }

    public void performPostPersistenceCleanup() {
        searchResponse.set(null);
    }

    public long getStartTimeMillis() {
        return searchTask.get().getStartTime();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > getExpirationTimeMillis();
    }

    @Override
    public void close() {

    }

    public synchronized void processFailure(Exception e) {
        this.isCompleted.set(true);
        this.isPartial.set(false);
        this.isRunning.set(false);
        error.set(new ElasticsearchException(e));
        setStage(FAILED);
    }


    public synchronized void processFinalResponse(SearchResponse response) {
        this.searchResponse.compareAndSet(null, response);
        this.isCompleted.set(true);
        this.isRunning.set(false);
        this.isPartial.set(false);
        setStage(COMPLETED);
        resultsHolder.set(null);
    }

    public synchronized AsyncSearchContext setStage(Stage stage) {
        switch (stage) {
            case RUNNING:
                validateAndSetStage(INIT, stage);
                break;
            case COMPLETED:
            case ABORTED:
            case FAILED:
                validateAndSetStage(RUNNING, stage);
                break;
            case PERSISTED:
                validateAndSetStage(COMPLETED, stage);
                break;
            default:
                throw new IllegalArgumentException("unknown AsyncSearchContext.Stage [" + stage + "]");
        }
        return this;
    }

    private void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            throw new IllegalStateException("can't move to stage [" + next + "]. current stage: ["
                    + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    public static class ResultsHolder {
        private AtomicInteger reducePhase;
        private TotalHits totalHits;
        private InternalAggregations internalAggregations;
        private AtomicBoolean isResponseInitialized;
        private AtomicInteger totalShards;
        private AtomicInteger successfulShards;
        private AtomicInteger skippedShards;
        private SearchResponse.Clusters clusters;
        private LongSupplier startTimeSupplier;
        private final List<ShardSearchFailure> shardSearchFailures;

        ResultsHolder(LongSupplier startTimeSupplier) {
            this.internalAggregations = InternalAggregations.EMPTY;
            this.shardSearchFailures = Collections.synchronizedList(new ArrayList<>());
            this.totalShards = new AtomicInteger();
            this.successfulShards = new AtomicInteger();
            this.skippedShards = new AtomicInteger();
            this.reducePhase = new AtomicInteger();
            this.isResponseInitialized = new AtomicBoolean(false);
            this.startTimeSupplier = startTimeSupplier;
        }

        private SearchResponse buildPartialSearchResponse() {
            if (isResponseInitialized.get()) {
                SearchHits searchHits = new SearchHits(SearchHits.EMPTY, totalHits, Float.NaN);
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, internalAggregations,
                        null, null, false, false, reducePhase.get());
                ShardSearchFailure[] shardSearchFailures = this.shardSearchFailures.toArray(new ShardSearchFailure[]{});
                long tookInMillis = System.currentTimeMillis() - startTimeSupplier.getAsLong();
                return new SearchResponse(internalSearchResponse, null, totalShards.get(),
                        successfulShards.get(), skippedShards.get(), tookInMillis, shardSearchFailures,
                        clusters);
            } else {
                return null;
            }
        }

        /**
         * @param reducePhase Version of reduce. If reducePhase version in resultHolder is greater than the event's reducePhase version,
         *                    this event can be discarded.
         */
        public synchronized void updateResultFromReduceEvent(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs,
                                                             int reducePhase) {
            this.successfulShards.set(shards.size());
            this.internalAggregations = aggs;
            this.reducePhase.set(reducePhase);
            this.totalHits = totalHits;
        }

        public synchronized void updateResultFromReduceEvent(InternalAggregations aggs, TotalHits totalHits, int reducePhase) {
            if (this.reducePhase.get() > reducePhase) {
                logger.warn("ResultHolder reducePhase version {} is ahead of the event reducePhase version {}. Discarding event",
                        this.reducePhase, reducePhase);
                return;
            }
            this.totalHits = totalHits;
            this.internalAggregations = aggs;
            this.reducePhase.set(reducePhase);
        }

        public synchronized void initialiseResultHolderShardLists(
                List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters) {
            this.totalShards.set(shards.size());
            this.skippedShards.set(skippedShards.size());
            this.clusters = clusters;
            this.isResponseInitialized.set(true);
        }

        public void incrementSuccessfulShards(boolean hasFetchPhase, int shardIndex) {
            if (hasFetchPhase == false) {
                this.successfulShards.incrementAndGet();
            }
        }

        public void addShardFailure(ShardSearchFailure failure) {
            this.shardSearchFailures.add(failure);
        }
    }
}
