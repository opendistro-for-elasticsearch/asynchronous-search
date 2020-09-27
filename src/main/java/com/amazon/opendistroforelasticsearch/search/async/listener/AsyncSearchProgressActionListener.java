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

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/***
 * The implementation of {@link CompositeSearchProgressActionListener} responsible for updating the partial results of a single async
 * search request. All partial results are updated atomically.
 */
public class AsyncSearchProgressActionListener extends CompositeSearchProgressActionListener {

    private final Logger logger = LogManager.getLogger(getClass());

    private volatile boolean hasFetchPhase;
    private AtomicInteger numReducePhases = new AtomicInteger();

    private final List<ActionListener<AsyncSearchResponse>> actionListeners;
    private AsyncSearchContext.ResultsHolder resultsHolder;
    private volatile boolean complete;

    public AsyncSearchProgressActionListener(AsyncSearchContext.ResultsHolder resultsHolder,
                                             Function<SearchResponse, AsyncSearchResponse> asyncSearchFunction,
                                             Consumer<Exception> exceptionConsumer) {
        super(asyncSearchFunction, exceptionConsumer);
        this.resultsHolder = resultsHolder;
        this.actionListeners = new ArrayList<>(1);
    }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                boolean fetchPhase) {
        logger.warn("onListShards --> shards :{}, skippedShards: {}, clusters: {}, fetchPhase: {}", shards, skippedShards,
                clusters, fetchPhase);
        this.hasFetchPhase = fetchPhase;
        resultsHolder.initialiseResultHolderShardLists(shards, skippedShards, clusters);
    }

    //FIXME : Delay deserializing aggregations up until partial SearchResponse has to be built.
    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                   DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        logger.warn("onPartialReduce --> shards; {}, totalHits: {}, aggs: {}, reducePhase: {}", shards, totalHits, aggs,
                reducePhase);
        numReducePhases.incrementAndGet();
        if (hasFetchPhase) {
            resultsHolder.updateResultFromReduceEvent(aggs == null ? null : aggs.expand(), totalHits, reducePhase);
        } else {
            resultsHolder.updateResultFromReduceEvent(shards, totalHits, aggs == null ? null : aggs.expand(),
                    reducePhase);
        }
    }


    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        logger.warn("onFinalReduce --> shards: {}, totalHits: {}, aggs :{}, reducePhase:{}", shards, totalHits, aggs, reducePhase);
        numReducePhases.incrementAndGet();
        if (hasFetchPhase) {
            resultsHolder.updateResultFromReduceEvent(aggs, totalHits, reducePhase);
        } else {
            resultsHolder.updateResultFromReduceEvent(shards, totalHits, aggs, reducePhase);
        }
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onFetchFailure --> shardIndex :{}, shardTarget: {}", shardIndex, shardTarget);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        resultsHolder.addShardFailure(shardSearchFailure);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        logger.warn("onFetchResult --> shardIndex: {} Thread : {}", shardIndex, Thread.currentThread().getId());
        resultsHolder.incrementSuccessfulShards(hasFetchPhase, shardIndex);
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onQueryFailure --> shardIndex: {}, searchTarget: {}", shardIndex, shardTarget, exc);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        resultsHolder.addShardFailure(shardSearchFailure);
    }

    /**
     * If search has no fetch Phase, these events may still be consumed in partial or final reduce events and need not be used
     * to increment successful shard results.
     */
    @Override
    protected void onQueryResult(int shardIndex) {
        logger.warn("onQueryResult --> shardIndex: {}", shardIndex);
        if (hasFetchPhase && numReducePhases.get() == 0) {
            resultsHolder.incrementSuccessfulShards(hasFetchPhase, shardIndex);
        }
    }
}
