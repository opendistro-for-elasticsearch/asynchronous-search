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
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class CompositeAsyncSearchProgressActionListener extends SearchProgressActionListener {

    private final Logger logger = LogManager.getLogger(getClass());

    private AtomicBoolean hasFetchPhase = new AtomicBoolean();
    private AtomicInteger numQueryResults = new AtomicInteger();
    private AtomicInteger numQueryFailures = new AtomicInteger();
    private AtomicInteger numFetchResults = new AtomicInteger();
    private AtomicInteger numFetchFailures = new AtomicInteger();
    private AtomicInteger numReducePhases = new AtomicInteger();

    private final List<ActionListener<AsyncSearchResponse>> actionListeners;
    private AsyncSearchContext asyncSearchContext;

    public CompositeAsyncSearchProgressActionListener(AsyncSearchContext asyncSearchContext) {
        this.asyncSearchContext = asyncSearchContext;
        this.actionListeners = new ArrayList<>();
    }

    public void addListener(ActionListener<AsyncSearchResponse> listener) {
        this.actionListeners.add(listener);
    }

    public void removeListener(ActionListener<AsyncSearchResponse> listener) { this.actionListeners.remove(listener); }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                boolean fetchPhase) {
        logger.warn("onListShards --> shards :{}, skippedShards: {}, clusters: {}, fetchPhase: {}", shards, skippedShards,
                clusters, fetchPhase);
        this.hasFetchPhase.set(fetchPhase);
        asyncSearchContext.initialiseResultHolderShardLists(shards, skippedShards, clusters, fetchPhase);
    }

    //FIXME : Delay deserializing aggregations up until partial SearchResponse has to be built.
    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                   DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        logger.warn("onPartialReduce --> shards; {}, totalHits: {}, aggs: {}, reducePhase: {}", shards, totalHits, aggs,
                reducePhase);
        numReducePhases.incrementAndGet();
        if (hasFetchPhase.get()) {
            asyncSearchContext.updateResultFromReduceEvent(aggs == null ? null : aggs.expand(), totalHits, reducePhase);
        } else {
            asyncSearchContext.updateResultFromReduceEvent(shards, totalHits, aggs == null ? null : aggs.expand(),
                    reducePhase);
        }
    }


    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        logger.warn("onFinalReduce --> shards: {}, totalHits: {}, aggs :{}, reducePhase:{}", shards, totalHits, aggs, reducePhase);
        numReducePhases.incrementAndGet();
        if (hasFetchPhase.get()) {
            asyncSearchContext.updateResultFromReduceEvent(aggs, totalHits, reducePhase);
        } else {
            asyncSearchContext.updateResultFromReduceEvent(shards, totalHits, aggs, reducePhase);
        }
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onFetchFailure --> shardIndex :{}, shardTarget: {}", shardIndex, shardTarget);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        numFetchFailures.incrementAndGet();
        asyncSearchContext.addShardFailure(shardSearchFailure);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        logger.warn("onFetchResult --> shardIndex: {} Thread : {}", shardIndex, Thread.currentThread().getId());
        numFetchResults.incrementAndGet();
        asyncSearchContext.incrementSuccessfulShards();
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onQueryFailure --> shardIndex: {}, searchTarget: {}", shardIndex, shardTarget, exc);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        numQueryFailures.incrementAndGet();
        asyncSearchContext.addShardFailure(shardSearchFailure);
    }

    /**
     * If search has no fetch Phase, these events may still be consumed in partial or final reduce events and need not be used
     * to increment successful shard results.
     */
    @Override
    protected void onQueryResult(int shardIndex) {
        logger.warn("onQueryResult --> shardIndex: {}", shardIndex);
        numQueryResults.incrementAndGet();
        if (!hasFetchPhase.get() && numReducePhases.get() == 0) {
            asyncSearchContext.incrementSuccessfulShards();
        }
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        asyncSearchContext.processFinalResponse(searchResponse);
        for (ActionListener<AsyncSearchResponse> listener : actionListeners) {
            try {
                logger.info("Search response completed {}", searchResponse);
                listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("onResponse listener [{}] failed", listener), e);
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        asyncSearchContext.processFailure(e);
        for (ActionListener<AsyncSearchResponse> listener : actionListeners) {
            try {
                logger.info("Search response failure", e);
                listener.onFailure(e);
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("onFailure listener [{}] failed", listener), e);
            }
        }
    }
}
