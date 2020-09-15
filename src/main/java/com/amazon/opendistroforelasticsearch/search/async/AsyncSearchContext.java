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

import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportSubmitAsyncSearchAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.tasks.TaskResult;

import java.io.IOException;
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
    private final AtomicBoolean persisted;

    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;

    private SetOnce<SearchTask> task;
    private final Client client;
    private final AtomicReference<PartialResultsHolder> resultsHolder;
    private final long startTimeMillis;
    private final String nodeId;
    private final AtomicLong expirationTimeMillis;
    private final Boolean keepOnCompletion;
    private final AsyncSearchPersistenceService persistenceService;

    private final AsyncSearchContextId asyncSearchContextId;

    private final TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider;
    private final Collection<ActionListener<AsyncSearchResponse>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());


    public AsyncSearchContext(AsyncSearchPersistenceService persistenceService, Client client, String nodeId,
                              AsyncSearchContextId asyncSearchContextId, TimeValue keepAlive, boolean keepOnCompletion,
                              TransportSubmitAsyncSearchAction.SearchTimeProvider searchTimeProvider) {
        super("async_search_context");
        this.persistenceService = persistenceService;
        this.client = client;
        this.nodeId = nodeId;
        this.asyncSearchContextId = asyncSearchContextId;
        this.keepOnCompletion = keepOnCompletion;
        this.searchTimeProvider = searchTimeProvider;
        this.resultsHolder = new AtomicReference<>(new PartialResultsHolder());
        this.startTimeMillis = searchTimeProvider.getAbsoluteStartMillis();
        this.expirationTimeMillis = new AtomicLong(startTimeMillis + keepAlive.getMillis());
        this.isRunning = new AtomicBoolean(true);
        this.isPartial = new AtomicBoolean(true);
        this.isCompleted = new AtomicBoolean(false);
        this.persisted = new AtomicBoolean(false);
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.task = new SetOnce<>();
    }

    public SearchTask getTask() {
        return task.get();
    }

    public void setTask(SearchTask task) {
        this.task.set(task);
    }

    public PartialResultsHolder getResultsHolder() {
        return resultsHolder.get();
    }


    public TransportSubmitAsyncSearchAction.SearchTimeProvider getSearchTimeProvider() {
        return searchTimeProvider;
    }

    public void addListener(ActionListener<AsyncSearchResponse> listener) {
        assert isRunning.get() == true : "Listener added after completion";
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
    public AsyncSearchResponse getAsyncSearchResponse() throws IOException {
        logger.info("isCancelled:{}, isExpired:{}, isPersisted:{}", isCancelled(), isExpired(), isPersisted());
        String id = getId();
        logger.info("ID is {}", id);
        return new AsyncSearchResponse(id, isPartial(), isRunning(), startTimeMillis, getExpirationTimeMillis(),
                        isRunning() ? buildPartialSearchResponse() : getFinalSearchResponse(), error.get());


    }

    public String getId() throws IOException {
        return AsyncSearchId.buildAsyncId(new AsyncSearchId(nodeId, asyncSearchContextId));
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
        return expirationTimeMillis.get();
    }

    private void cancelIfRequired() {
        if (!getTask().isCancelled()) {
            if (isExpired()) {
                cancelTask();
            }
        }

    }

    public void cancelTask() {
        if (isCancelled())
            return;
        logger.info("Cancelling task [{}] on node : [{}]", getTask().getId(), nodeId);
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(getTask().taskInfo(nodeId, false).getTaskId());
        cancelTasksRequest.setReason("Async search request expired");
        client.admin().cluster().cancelTasks(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
            @Override
            public void onResponse(CancelTasksResponse cancelTasksResponse) {
                logger.info(cancelTasksResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to cancel async search task {} not cancelled upon expiry", getTask().getId());
            }
        });
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > getExpirationTimeMillis();
    }

    private SearchResponse buildPartialSearchResponse() {
        PartialResultsHolder partialResultsHolder = getResultsHolder();
        if (partialResultsHolder.isResponseInitialized.get()) {
            SearchHits searchHits = new SearchHits(SearchHits.EMPTY, partialResultsHolder.totalHits, Float.NaN);
            InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, partialResultsHolder.internalAggregations,
                    null, null, false, false, partialResultsHolder.reducePhase.get());
            ShardSearchFailure[] shardSearchFailures = partialResultsHolder.shardSearchFailuresFailures.toArray(new ShardSearchFailure[]{});
            long tookInMillis = System.currentTimeMillis() - getTask().getStartTime();
            return new SearchResponse(internalSearchResponse, null, partialResultsHolder.totalShards.get(),
                    partialResultsHolder.successfulShards.get(), partialResultsHolder.skippedShards.get(), tookInMillis, shardSearchFailures,
                    partialResultsHolder.clusters);
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


    public void processFinalResponse(SearchResponse response) throws IOException {
        this.searchResponse.compareAndSet(null, response);
        this.isCompleted.set(true);
        this.isRunning.set(false);
        this.isPartial.set(false);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();
        this.persistenceService.createResponse(new TaskResult(getTask().taskInfo(nodeId, false), asyncSearchResponse), asyncSearchResponse,
                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        persisted.compareAndSet(false, true);
                        searchResponse.compareAndSet(response,null);
                        resultsHolder.set(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to persist final response for {}", asyncSearchResponse.getId(), e);
                    }
                });

        this.listeners.forEach(listener -> {
            try {

                listener.onResponse(asyncSearchResponse);
            } catch (Exception e) {
                logger.error("Failed to notify listener on response.");
            }
        });
    }

    public synchronized void updateExpirationTime(long expirationTimeMillis) throws IOException {
        this.expirationTimeMillis.set(expirationTimeMillis);
        if(isPersisted()) {
            persistenceService.updateExpirationTime(getId(),expirationTimeMillis);
        }
    }

    public boolean isPersisted() {
        return persisted.get();
    }

    public void clear() {
        cancelTask();
        //clear further
    }

    public void getAsyncSearchResponse(ActionListener<AsyncSearchResponse> listener) {
        if(isPersisted()) {
            try {
                persistenceService.getResponse(getId(),listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }

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
