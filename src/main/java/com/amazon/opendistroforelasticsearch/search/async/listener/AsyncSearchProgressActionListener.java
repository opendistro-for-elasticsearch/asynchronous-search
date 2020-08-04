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

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;

public class AsyncSearchProgressActionListener extends SearchProgressActionListener {

    private ActionListener<AsyncSearchResponse> asyncSearchResponseListener;
    private SearchResponse searchResponse;

    private final Logger logger = LogManager.getLogger(getClass());

    public AsyncSearchProgressActionListener(ActionListener<AsyncSearchResponse> asyncSearchResponseListener) {
        this.asyncSearchResponseListener = asyncSearchResponseListener;
    }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters, boolean fetchPhase) {
        logger.warn("onListShards --> shards :{}, skippedShards: {}, clusters: {}, fetchPhase: {}", shards, skippedShards, clusters, fetchPhase);
    }

    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        logger.warn("onPartialReduce --> shards; {}, totalHits: {}, aggs: {}, reducePhase: {}",shards, totalHits, aggs, reducePhase );
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onFetchFailure --> shardIndex :{}, shardTarget: {}", exc);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        logger.warn("onFetchResult --> shardIndex: {}", shardIndex);
    }


    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        logger.warn("onFinalReduce --> shards: {}, totalHits: {}, aggs :{}, reducePhase:{}", shards, totalHits, aggs, reducePhase);
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        logger.warn("onQueryFailure --> shardIndex: {}, searchTarget: {}",shardIndex, shardTarget, exc );
    }

    @Override
    protected void onQueryResult(int shardIndex) {
        logger.warn("onQueryResult --> shardIndex: {}", shardIndex);
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
        logger.info("Search response completed {}", searchResponse);
        asyncSearchResponseListener.onResponse(null);
    }

    @Override
    public void onFailure(Exception e) {
        logger.info("Search response failure", e);
        asyncSearchResponseListener.onFailure(e);
    }
}

