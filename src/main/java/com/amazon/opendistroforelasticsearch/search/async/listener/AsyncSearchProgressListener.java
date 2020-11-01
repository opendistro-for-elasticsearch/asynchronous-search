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

import com.amazon.opendistroforelasticsearch.jobscheduler.repackage.com.cronutils.utils.VisibleForTesting;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;


/***
 * The implementation of {@link CompositeSearchProgressActionListener} responsible for updating the partial results of a single async
 * search request. All partial results are updated atomically.
 */
public class AsyncSearchProgressListener extends CompositeSearchProgressActionListener<AsyncSearchResponse> {

    private final Object shardFailuresMutex = new Object();
    private final PartialResultsHolder partialResultsHolder;

    public AsyncSearchProgressListener(long relativeStartMillis, CheckedFunction<SearchResponse, AsyncSearchResponse, IOException> function,
                                       CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction, ExecutorService executorService,
                                       LongSupplier relativeTimeSupplier) {
        super(function, failureFunction, executorService);
        this.partialResultsHolder = new PartialResultsHolder(relativeStartMillis, relativeTimeSupplier);
    }


    /***
     * Returns the partial response for the search response.
     * @return the partial search response
     */
    public SearchResponse partialResponse() {
        if (partialResultsHolder.isInitialized.get()) {
            SearchHits searchHits = new SearchHits(SearchHits.EMPTY, partialResultsHolder.totalHits.get(), Float.NaN);
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits,
                    partialResultsHolder.internalAggregations.get() == null ? (partialResultsHolder.delayedInternalAggregations.get() != null ?
                            partialResultsHolder.delayedInternalAggregations.get().expand() : null) : partialResultsHolder.internalAggregations.get(),
                    null, null, false, null, partialResultsHolder.reducePhase.get());
            ShardSearchFailure [] shardSearchFailures = partialResultsHolder.shardFailures.get() == null ? ShardSearchFailure.EMPTY_ARRAY :
                    partialResultsHolder.shardFailures.get().toArray(new ShardSearchFailure[partialResultsHolder.shardFailures.get().length()]);
            long tookInMillis = partialResultsHolder.relativeTimeSupplier.getAsLong() - partialResultsHolder.relativeStartMillis;
            return new SearchResponse(internalSearchResponse, null, partialResultsHolder.totalShards.get(),
                    partialResultsHolder.successfulShards.get(), partialResultsHolder.skippedShards.get(), tookInMillis, shardSearchFailures,
                    partialResultsHolder.clusters.get());
        } else {
            return null;
        }
    }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                boolean fetchPhase) {
        partialResultsHolder.hasFetchPhase.compareAndSet(false, fetchPhase);
        partialResultsHolder.totalShards.compareAndSet(0, shards.size());
        partialResultsHolder.skippedShards.compareAndSet(0, skippedShards.size());
        partialResultsHolder.clusters.compareAndSet(null, clusters);
        partialResultsHolder.isInitialized.compareAndSet(false, true);
        partialResultsHolder.shards.compareAndSet(null, shards);
    }

    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                   DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        partialResultsHolder.delayedInternalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        assert partialResultsHolder.shards.get().equals(shards);
        partialResultsHolder.internalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        assert partialResultsHolder.hasFetchPhase.get() : "Fetch failure without fetch phase";
        assert shardIndex < partialResultsHolder.totalShards.get();
        onSearchFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        assert partialResultsHolder.hasFetchPhase.get() : "Fetch result without fetch phase";
        assert shardIndex < partialResultsHolder.totalShards.get();
        partialResultsHolder.successfulShards.incrementAndGet();
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        onSearchFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onQueryResult(int shardIndex) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        // query and fetch optimization for single shard
        if (partialResultsHolder.hasFetchPhase.get() == false || partialResultsHolder.totalShards.get() == 1) {
            partialResultsHolder.successfulShards.incrementAndGet();
        }
    }

    private void onSearchFailure(int shardIndex, SearchShardTarget shardTarget, Exception e) {
        AtomicArray<ShardSearchFailure> shardFailures = partialResultsHolder.shardFailures.get();
        // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
        if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
            synchronized (shardFailuresMutex) {
                shardFailures = this.partialResultsHolder.shardFailures.get(); // read again otherwise somebody else has created it?
                if (shardFailures == null) { // still null so we are the first and create a new instance
                    shardFailures = new AtomicArray<>(partialResultsHolder.totalShards.get());
                    this.partialResultsHolder.shardFailures.set(shardFailures);
                }
                shardFailures.setOnce(shardIndex, new ShardSearchFailure(e, shardTarget));
            }
        }
    }

    @VisibleForTesting
    PartialResultsHolder getPartialResultsHolder() {
        return partialResultsHolder;
    }

    static class PartialResultsHolder {
        final AtomicInteger reducePhase;
        final AtomicReference<TotalHits> totalHits;
        final AtomicReference<InternalAggregations> internalAggregations;
        final AtomicReference<DelayableWriteable.Serialized<InternalAggregations>> delayedInternalAggregations;
        final AtomicBoolean isInitialized;
        final AtomicInteger totalShards;
        final AtomicInteger successfulShards;
        final AtomicInteger skippedShards;
        final AtomicReference<SearchResponse.Clusters> clusters;
        final AtomicReference<List<SearchShard>> shards;
        final AtomicBoolean hasFetchPhase;
        final long relativeStartMillis;
        final LongSupplier relativeTimeSupplier;
        final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures;


        PartialResultsHolder(long relativeStartMillis, LongSupplier relativeTimeSupplier) {
            this.internalAggregations = new AtomicReference<>();
            this.totalShards = new AtomicInteger();
            this.successfulShards = new AtomicInteger();
            this.skippedShards = new AtomicInteger();
            this.reducePhase = new AtomicInteger();
            this.isInitialized = new AtomicBoolean(false);
            this.hasFetchPhase = new AtomicBoolean(false);
            this.totalHits = new AtomicReference<>();
            this.clusters = new AtomicReference<>();
            this.delayedInternalAggregations = new AtomicReference<>();
            this.relativeStartMillis = relativeStartMillis;
            this.shards = new AtomicReference<>();
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.shardFailures = new SetOnce<>();
        }
    }
}
