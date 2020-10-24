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
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;


/***
 * The implementation of {@link CompositeSearchResponseActionListener} responsible for updating the partial results of a single async
 * search request. All partial results are updated atomically.
 */
public class AsyncSearchProgressListener extends CompositeSearchResponseActionListener<AsyncSearchResponse> {

    public AsyncSearchProgressListener(long relativeStartMillis, CheckedFunction<SearchResponse, AsyncSearchResponse, IOException> function,
                                       CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction, Executor executor,
                                       LongSupplier relativeTimeSupplier) {
        super(function, failureFunction, executor, relativeStartMillis, relativeTimeSupplier);
    }


    /***
     * Returns the partial response for the search response.
     * @return the partial search response
     */
    @Override
    public SearchResponse partialResponse() {
        if (partialResultsHolder.isInitialized.get()) {
            SearchHits searchHits = new SearchHits(SearchHits.EMPTY, partialResultsHolder.totalHits.get(), 0);
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits,
                    partialResultsHolder.internalAggregations.get() == null ?
                            (partialResultsHolder.delayedInternalAggregations.get() != null ?
                                    partialResultsHolder.delayedInternalAggregations.get().expand() : null)
                            : partialResultsHolder.internalAggregations.get(),
                    null, null, false, false, partialResultsHolder.reducePhase.get());
            ShardSearchFailure[] shardSearchFailures =
                    partialResultsHolder.shardSearchFailures.toArray(new ShardSearchFailure[partialResultsHolder.failurePos.get()]);
            long tookInMillis = partialResultsHolder.relativeTimeSupplier.getAsLong() - partialResultsHolder.relativeStartMillis;
            return new SearchResponse(internalSearchResponse, null, partialResultsHolder.totalShards.get(),
                    partialResultsHolder.successfulShards.get(),
                    partialResultsHolder.skippedShards.get(), tookInMillis, shardSearchFailures, partialResultsHolder.clusters.get());
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
        partialResultsHolder.shardSearchFailures.setOnce(partialResultsHolder.failurePos.getAndIncrement(),
                new ShardSearchFailure(exc, shardTarget));
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
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        partialResultsHolder.shardSearchFailures.setOnce(partialResultsHolder.failurePos.getAndIncrement(), shardSearchFailure);
    }

    @Override
    protected void onQueryResult(int shardIndex) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        // query and fetch optimization for single shard
        if (partialResultsHolder.hasFetchPhase.get() == false || partialResultsHolder.totalShards.get() == 1) {
            partialResultsHolder.successfulShards.incrementAndGet();
        }
    }
}
