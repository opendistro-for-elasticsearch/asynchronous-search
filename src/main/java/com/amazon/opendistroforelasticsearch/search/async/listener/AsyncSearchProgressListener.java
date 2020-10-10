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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/***
 * The implementation of {@link CompositeSearchResponseActionListener} responsible for updating the partial results of a single async
 * search request. All partial results are updated atomically.
 */
public class AsyncSearchProgressListener extends CompositeSearchResponseActionListener<AsyncSearchResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    private PartialResultsHolder partialResultsHolder;

    public AsyncSearchProgressListener(long relativeStartMillis, CheckedFunction<SearchResponse, AsyncSearchResponse, Exception> function,
                                       Consumer<Exception> onFailure, Executor executor) {
        super(function, onFailure, executor);
        this.partialResultsHolder = new PartialResultsHolder(relativeStartMillis);
    }

    /***
     * Returns the partial response for the search response.
     * @return the partial search response
     */
    public SearchResponse partialResponse() {
        return partialResultsHolder.partialResponse();
    }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                boolean fetchPhase) {
        logger.warn("onListShards --> shards :{}, skippedShards: {}, clusters: {}, fetchPhase: {}", shards, skippedShards,
                clusters, fetchPhase);
        partialResultsHolder.hasFetchPhase.compareAndSet(false, fetchPhase);
        partialResultsHolder.totalShards.compareAndSet(0, shards.size());
        partialResultsHolder.skippedShards.compareAndSet(0, skippedShards.size());
        partialResultsHolder.clusters.compareAndSet(null, clusters);
        partialResultsHolder.isInitialized.compareAndSet(false, true);
    }


    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                   DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        logger.warn("onPartialReduce --> shards; {}, totalHits: {}, aggs: {}, reducePhase: {}", shards, totalHits, aggs,
                reducePhase);
        partialResultsHolder.successfulShards.set(shards.size());
        partialResultsHolder.delayedInternalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }


    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        logger.warn("onFinalReduce --> shards: {}, totalHits: {}, aggs :{}, reducePhase:{}", shards, totalHits, aggs, reducePhase);
        partialResultsHolder.successfulShards.set(shards.size());
        partialResultsHolder.internalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onFetchFailure --> shardIndex :{}, shardTarget: {}", shardIndex, shardTarget);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        partialResultsHolder.shardSearchFailures.setOnce(partialResultsHolder.pos.getAndIncrement(), shardSearchFailure);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        logger.warn("onFetchResult --> shardIndex: {} Thread : {}", shardIndex, Thread.currentThread().getId());
        partialResultsHolder.successfulShards.updateAndGet((val) -> partialResultsHolder.hasFetchPhase.get() ? partialResultsHolder.successfulShards.incrementAndGet()
                : partialResultsHolder.successfulShards.get());
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onQueryFailure --> shardIndex: {}, searchTarget: {}", shardIndex, shardTarget, exc);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        partialResultsHolder.shardSearchFailures.setOnce(partialResultsHolder.pos.getAndIncrement(), shardSearchFailure);
    }

    /**
     * If search has no fetch Phase, these events may still be consumed in partial or final reduce events and need not be used
     * to increment successful shard results.
     */
    @Override
    protected void onQueryResult(int shardIndex) {
        logger.warn("onQueryResult --> shardIndex: {}", shardIndex);
        partialResultsHolder.successfulShards.updateAndGet((val) -> partialResultsHolder.hasFetchPhase.get() ? partialResultsHolder.successfulShards.get() :
                partialResultsHolder.successfulShards.incrementAndGet());
    }


    class PartialResultsHolder {
        private final AtomicInteger reducePhase;
        private final AtomicReference<TotalHits> totalHits;
        private final AtomicReference<InternalAggregations> internalAggregations;
        private final AtomicReference<DelayableWriteable.Serialized<InternalAggregations>> delayedInternalAggregations;
        private final AtomicBoolean isInitialized;
        private final AtomicInteger totalShards;
        private final AtomicInteger successfulShards;
        private final AtomicInteger skippedShards;
        private final AtomicReference<SearchResponse.Clusters> clusters;
        private final AtomicArray<ShardSearchFailure> shardSearchFailures;
        private final AtomicInteger pos;
        private final AtomicBoolean hasFetchPhase;
        private final long relativeStartMillis;

        PartialResultsHolder(long relativeStartMillis) {
            this.internalAggregations = new AtomicReference<>();
            this.shardSearchFailures = new AtomicArray<>(1);
            this.totalShards = new AtomicInteger();
            this.successfulShards = new AtomicInteger();
            this.skippedShards = new AtomicInteger();
            this.reducePhase = new AtomicInteger();
            this.isInitialized = new AtomicBoolean(false);
            this.pos = new AtomicInteger(0);
            this.hasFetchPhase = new AtomicBoolean(false);
            this.totalHits = new AtomicReference<>();
            this.clusters = new AtomicReference<>();
            this.delayedInternalAggregations = new AtomicReference<>();
            this.relativeStartMillis = relativeStartMillis;
        }

        SearchResponse partialResponse() {
            if (this.isInitialized.get()) {
                SearchHits searchHits = new SearchHits(SearchHits.EMPTY, this.totalHits.get(), Float.NaN);
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits,
                        this.internalAggregations.get() == null ? this.delayedInternalAggregations.get().expand() : this.internalAggregations.get(),
                        null, null, false, false, this.reducePhase.get());
                ShardSearchFailure[] shardSearchFailures = this.shardSearchFailures.toArray(new ShardSearchFailure[]{});
                long tookInMillis = System.currentTimeMillis() - relativeStartMillis;
                return new SearchResponse(internalSearchResponse, null, this.totalShards.get(), this.successfulShards.get(),
                        this.skippedShards.get(), tookInMillis, shardSearchFailures, this.clusters.get());
            } else {
                return null;
            }
        }
    }
}
