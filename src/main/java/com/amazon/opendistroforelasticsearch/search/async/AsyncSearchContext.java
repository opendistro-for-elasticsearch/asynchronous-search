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

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncSearchContext extends AbstractRefCounted implements Releasable {

    private static final Logger logger = LogManager.getLogger(AsyncSearchContext.class);
    private AsyncSearchTask task;
    private boolean isRunning;
    private boolean isPartial;
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

   public static class ResultsHolder {
       private SearchResponse searchResponse;
       private AtomicLong version = new AtomicLong();

       private final List<ShardSearchFailure> shardSearchFailuresFailures = new ArrayList<>();

       private int reducePhase;
       private TotalHits totalHits;
       private InternalAggregations internalAggregations;

       private List<SearchShard> totalShards ;
       private List<SearchShard> successfulShards;
       private List<SearchShard> skippedShards;
       private SearchResponse.Clusters clusters;


        ResultsHolder(TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider) {
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), Float.NaN);
            this.internalAggregations = InternalAggregations.EMPTY;
            SearchResponseSections internalSearchResponse = new InternalSearchResponse(searchHits, internalAggregations, null, null, false, null, 0);
            this.searchResponse = new SearchResponse(internalSearchResponse, null, 0, 0, 0, searchTimeProvider.buildTookInMillis(),
                    ShardSearchFailure.EMPTY_ARRAY, clusters);
        }

        public synchronized void addShardFailure(ShardSearchFailure failure) {
            shardSearchFailuresFailures.add(failure);
        }

       /**
        * @param reducePhase Version of reduce. If reducePhase version in resultHolder is greater then current reducePhase version, this event can be discarded.
        */
       public synchronized void updateResultFromReduceEvent(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
           if(this.reducePhase > reducePhase) {
               logger.warn("ResultHolder reducePhase version {} is ahead of the event reducePhase version {}. Discarding event",
                       this.reducePhase, reducePhase);
               return;
           }
           this.successfulShards = shards;
           this.internalAggregations = aggs;
           this.reducePhase = reducePhase;
           this.totalHits = totalHits;
       }

       public void initialiseResultHolderShardLists(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters, boolean fetchPhase) {
           this.totalShards = shards;
           this.skippedShards = skippedShards;
           this.clusters = clusters;
       }
   }
}
