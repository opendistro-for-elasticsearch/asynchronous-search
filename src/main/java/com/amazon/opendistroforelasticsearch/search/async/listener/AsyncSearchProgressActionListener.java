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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class AsyncSearchProgressActionListener extends SearchProgressActionListener {

    private final Logger logger = LogManager.getLogger(getClass());

    private AsyncSearchContext asyncSearchContext;
    private Runnable unregisterTask;
    private AtomicBoolean hasFetchPhase = new AtomicBoolean();
    private AtomicInteger numQueryResults = new AtomicInteger();
    private AtomicInteger numQueryFailures = new AtomicInteger();
    private AtomicInteger numFetchResults = new AtomicInteger();
    private AtomicInteger numFetchFailures = new AtomicInteger();
    private AtomicInteger numReducePhases = new AtomicInteger();

    public AsyncSearchProgressActionListener(AsyncSearchContext asyncSearchContext, Runnable unregisterTask) {
        this.asyncSearchContext = asyncSearchContext;
        this.unregisterTask = unregisterTask;
    }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                boolean fetchPhase) {
        logger.warn("onListShards --> shards :{}, skippedShards: {}, clusters: {}, fetchPhase: {}", shards, skippedShards,
                clusters, fetchPhase);
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        try {
//            Thread.sleep(20000);
        } catch (Exception e) {
            logger.info("erro");
        }
        this.hasFetchPhase.set(fetchPhase);
        asyncSearchContext.getResultsHolder().initialiseResultHolderShardLists(shards, skippedShards, clusters, fetchPhase);
    }

    //FIXME : Delay deserializing aggregations up until partial SearchResponse has to be built.
    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                   DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        logger.warn("onPartialReduce --> shards; {}, totalHits: {}, aggs: {}, reducePhase: {}", shards, totalHits, aggs,
                reducePhase);
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        try {
//           Thread.sleep(20000);
        } catch (Exception e) {
            logger.info("erro");
        }
        numReducePhases.incrementAndGet();
        if (hasFetchPhase.get()) {
            asyncSearchContext.getResultsHolder().updateResultFromReduceEvent(aggs == null ? null : aggs.expand(), totalHits, reducePhase);
        } else {
            asyncSearchContext.getResultsHolder().updateResultFromReduceEvent(shards, totalHits, aggs == null ? null : aggs.expand(),
                    reducePhase);
        }
    }


    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        logger.warn("onFinalReduce --> shards: {}, totalHits: {}, aggs :{}, reducePhase:{}", shards, totalHits, aggs, reducePhase);
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        try {
//           Thread.sleep(40000);
        } catch (Exception e) {
            logger.info("erro");
        }
        numReducePhases.incrementAndGet();
        if (hasFetchPhase.get()) {
            asyncSearchContext.getResultsHolder().updateResultFromReduceEvent(aggs, totalHits, reducePhase);
        } else {
            asyncSearchContext.getResultsHolder().updateResultFromReduceEvent(shards, totalHits, aggs, reducePhase);
        }
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onFetchFailure --> shardIndex :{}, shardTarget: {}", shardIndex, shardTarget);
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        numFetchFailures.incrementAndGet();
        asyncSearchContext.getResultsHolder().addShardFailure(shardSearchFailure);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        logger.warn("onFetchResult --> shardIndex: {} Thread : {}", shardIndex, Thread.currentThread().getId());
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        try {
//            Thread.sleep(20000);
        } catch (Exception e) {
            logger.info("erro");
        }
        numFetchResults.incrementAndGet();
        asyncSearchContext.getResultsHolder().incrementSuccessfulShards();
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onQueryFailure --> shardIndex: {}, searchTarget: {}", shardIndex, shardTarget, exc);
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(exc, shardTarget);
        numQueryFailures.incrementAndGet();
        asyncSearchContext.getResultsHolder().addShardFailure(shardSearchFailure);
    }

    /**
     * If search has no fetch Phase, these events may still be consumed in partial or final reduce events and need not be used
     * to increment successful shard results.
     */
    @Override
    protected void onQueryResult(int shardIndex) {
        logger.warn("onQueryResult --> shardIndex: {}", shardIndex);
        if (asyncSearchContext.isCancelled()) {
            logger.warn("Discarding event as search is cancelled!");
            return;
        }
        try {
//            Thread.sleep(50000);
        } catch (Exception e) {
            logger.info("error");
        }
        numQueryResults.incrementAndGet();
        if (!hasFetchPhase.get() && numReducePhases.get() == 0) {
            asyncSearchContext.getResultsHolder().incrementSuccessfulShards();
        }
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        try {
            logger.info("Search response completed {}", searchResponse);
            if (asyncSearchContext.isCancelled()) {
                logger.warn("Discarding event as search is cancelled!");
                return;
            }
            //asyncSearchContext.getListeners().forEach(listener -> listener.onResponse(null));
            try {
                asyncSearchContext.processFinalResponse(searchResponse);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            unregisterTask.run();
        }

    }

    @Override
    public void onFailure(Exception e) {
        try {
            if (asyncSearchContext.isCancelled()) {
                logger.warn("Discarding event as search is cancelled!");
                return;
            }
            logger.info("Search response failure", e);
            asyncSearchContext.processFailure(e);
        } finally {
            unregisterTask.run();
        }
    }


}

