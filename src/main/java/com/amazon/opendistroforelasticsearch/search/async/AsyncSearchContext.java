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

import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncSearchContext extends AbstractRefCounted implements Releasable {

    private static final Logger logger = LogManager.getLogger(AsyncSearchContext.class);

    private final AtomicBoolean isRunning;
    private final AtomicBoolean isPartial;
    private final AtomicBoolean isCompleted;

    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;

    private final SearchTask task;
    private final Client client;
    private final PartialResultsHolder resultsHolder;
    private final long startTimeMillis;
    private final String nodeId;
    private final AtomicLong expirationTimeMillis;
    private final Boolean keepOnCompletion;

    private final AsyncSearchContextId asyncSearchContextId;

    private final TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider;
    private final Collection<ActionListener<AsyncSearchResponse>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());


    public AsyncSearchContext(Client client, String nodeId, AsyncSearchContextId asyncSearchContextId, TimeValue keepAlive, boolean keepOnCompletion, SearchTask task,
                              TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider) {
        super("async_search_context");
        this.client = client;
        this.nodeId = nodeId;
        this.asyncSearchContextId = asyncSearchContextId;
        this.task = task;
        this.keepOnCompletion = keepOnCompletion;
        this.searchTimeProvider = searchTimeProvider;
        this.resultsHolder = new PartialResultsHolder();
        this.startTimeMillis = searchTimeProvider.getAbsoluteStartMillis();
        this.expirationTimeMillis = new AtomicLong(startTimeMillis + keepAlive.getMillis());
        this.isRunning = new AtomicBoolean(true);
        this.isPartial = new AtomicBoolean(true);
        this.isCompleted = new AtomicBoolean(false);
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
    }

    public SearchTask getTask() {
        return task;
    }

    public PartialResultsHolder getResultsHolder() {
        return resultsHolder;
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


    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    @Override
    public void close() {

    }

    @Override
    protected void closeInternal() {

    }

    /**
     * @return If past expiration time throw RNF and cancel task.
     * If isRunning is true, build and return partial Response. Else, build and return response.
     */
    public AsyncSearchResponse getAsyncSearchResponse() {
        logger.info("isCancelled:{}, isExpired:{}", isCancelled(), isExpired());
        try {
            String id = AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, asyncSearchContextId));
            logger.info("ID is {}", id);
            return new AsyncSearchResponse(id, isPartial(), isRunning(), startTimeMillis, getExpirationTimeMillis(),
                    isRunning() ? buildPartialSearchResponse() : getFinalSearchResponse(), error.get());

        } catch (Exception e) {
            //capture error in async search response
            return null;
        }
    }

    public SearchResponse getFinalSearchResponse() {
        return searchResponse.get();
    }

    public boolean isRunning() {
        return !task.isCancelled() && isRunning.get();
    }

    public boolean isCancelled() {
        return task.isCancelled();
    }

    public boolean isPartial() {
        return isPartial.get();
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis.get();
    }

    private void cancelIfRequired() {
        if(!task.isCancelled()) {
            if(isExpired()) {
                cancelTask();
            }
        }

    }

    public void cancelTask() {
        if(isCancelled())
            return;
        logger.info("Cancelling task [{}] on node : [{}]", task.getId(), nodeId);
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(task.taskInfo(nodeId, false).getTaskId());
        cancelTasksRequest.setReason("Async search request expired");
        client.admin().cluster().cancelTasks(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
            @Override
            public void onResponse(CancelTasksResponse cancelTasksResponse) {
            logger.info(cancelTasksResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to cancel async search task {} not cancelled upon expiry", task.getId());
            }
        });
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > getExpirationTimeMillis();
    }

    private SearchResponse buildPartialSearchResponse() {
        if (resultsHolder.isResponseInitialized.get()) {
            SearchHits searchHits = new SearchHits(SearchHits.EMPTY, resultsHolder.totalHits, Float.NaN);
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, resultsHolder.internalAggregations,
                    null, null, false, false, resultsHolder.reducePhase.get());
            ShardSearchFailure[] shardSearchFailures = resultsHolder.shardSearchFailuresFailures.toArray(new ShardSearchFailure[]{});
            long tookInMillis =  System.currentTimeMillis()-task.getStartTime();
            return new SearchResponse(internalSearchResponse, null, resultsHolder.totalShards.get(),
                    resultsHolder.successfulShards.get(), resultsHolder.skippedShards.get(), tookInMillis, shardSearchFailures, resultsHolder.clusters);
        } else {
            return null;
        }
    }

    public void processFailure(Exception e) {
        this.isCompleted.set(true);
        this.isPartial.set(false);
        this.isRunning.set(false);
        error.set(new ElasticsearchException(e));
        getListeners().forEach(listener -> listener.onFailure(e));
    }


    public void processFinalResponse(SearchResponse searchResponse) {
        this.searchResponse.compareAndSet(null, searchResponse);
        this.isCompleted.set(true);
        this.isRunning.set(false);
        this.isPartial.set(false);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();
        this.listeners.forEach(listener -> {
            try {

                listener.onResponse(asyncSearchResponse);
            } catch (Exception e) {
                logger.error("Failed to notify listener on response.");
            }
        });
    }

    public void setExpirationTimeMillis(long expirationTimeMillis) {
        this.expirationTimeMillis.set(expirationTimeMillis);
    }

    public void clear() {
        cancelTask();
        //clear further
    }

    public static class PartialResultsHolder {

        private final List<ShardSearchFailure> shardSearchFailuresFailures = new ArrayList<>();

        private AtomicInteger reducePhase;
        private TotalHits totalHits;
        private InternalAggregations internalAggregations;

        private AtomicBoolean isResponseInitialized;

        private AtomicInteger totalShards;
        private AtomicInteger successfulShards;
        private AtomicInteger skippedShards;
        private SearchResponse.Clusters clusters;

        PartialResultsHolder() {
            this.internalAggregations = InternalAggregations.EMPTY;
            totalShards = new AtomicInteger();
            successfulShards = new AtomicInteger();
            skippedShards = new AtomicInteger();
            reducePhase = new AtomicInteger();
            isResponseInitialized = new AtomicBoolean(false);
        }


        public synchronized void addShardFailure(ShardSearchFailure failure) {
            shardSearchFailuresFailures.add(failure);
        }

        /**
         * @param reducePhase Version of reduce. If reducePhase version in resultHolder is greater than the event's reducePhase version, this event can be discarded.
         */
        public synchronized void updateResultFromReduceEvent(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            if (this.reducePhase.get() > reducePhase) {
                logger.warn("ResultHolder reducePhase version {} is ahead of the event reducePhase version {}. Discarding event",
                        this.reducePhase, reducePhase);
                return;
            }
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
                List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters, boolean fetchPhase) {
            this.totalShards.set(shards.size());
            this.skippedShards.set(skippedShards.size());
            this.clusters = clusters;
            isResponseInitialized.set(true);
        }

        public synchronized void incrementSuccessfulShards() {
            successfulShards.incrementAndGet();
        }
    }
}
