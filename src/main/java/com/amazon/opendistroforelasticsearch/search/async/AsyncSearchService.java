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
import com.amazon.opendistroforelasticsearch.search.async.listener.CompositeSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.PrioritizedListener;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.ABORTED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.PERSISTED;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;

public class AsyncSearchService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchService.class);

    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.default_keep_alive", timeValueMinutes(5), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.max_keep_alive", timeValueHours(24), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
            Setting.positiveTimeSetting("async_search.keep_alive_interval", timeValueMinutes(1), Setting.Property.NodeScope);

    private volatile long defaultKeepAlive;

    private volatile long maxKeepAlive;

    private final Scheduler.Cancellable keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final Client client;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final AsyncSearchPersistenceService persistenceService;

    private final ConcurrentMapLong<AsyncSearchContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();

    public AsyncSearchService(AsyncSearchPersistenceService asyncSearchPersistenceService, Client client, ClusterService clusterService,
                              ThreadPool threadPool) {
        this.client = client;
        Settings settings = clusterService.getSettings();
        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_KEEPALIVE_SETTING, MAX_KEEPALIVE_SETTING,
                this::setKeepAlives, this::validateKeepAlives);
        this.threadPool = threadPool;
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, ThreadPool.Names.SAME);
        this.clusterService = clusterService;
        this.persistenceService =  asyncSearchPersistenceService;
    }


    private void validateKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        if (defaultKeepAlive.millis() > maxKeepAlive.millis()) {
            throw new IllegalArgumentException("Default keep alive setting for scroll [" + DEFAULT_KEEPALIVE_SETTING.getKey() + "]" +
                    " should be smaller than max keep alive [" + MAX_KEEPALIVE_SETTING.getKey() + "], " +
                    "was (" + defaultKeepAlive + " > " + maxKeepAlive + ")");
        }
    }

    private void setKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        validateKeepAlives(defaultKeepAlive, maxKeepAlive);
        this.defaultKeepAlive = defaultKeepAlive.millis();
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    public final AsyncSearchContext createAndPutContext(SubmitAsyncSearchRequest submitAsyncSearchRequest) {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchContext asyncSearchContext = new AsyncSearchContext(clusterService.localNode().getId(), asyncSearchContextId,
                submitAsyncSearchRequest.getKeepAlive(),
                submitAsyncSearchRequest.keepOnCompletion());
        putContext(asyncSearchContext);
        return asyncSearchContext;
    }

    private AsyncSearchContext getContext(AsyncSearchContextId contextId) {
        final AsyncSearchContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return null;
        }
        if (context.getAsyncSearchContextId().getContextId().equals(contextId.getContextId())) {
            return context;
        }
        return null;
    }

    public AsyncSearchContext findContext(AsyncSearchContextId asyncSearchContextId) throws AsyncSearchContextMissingException {
        final AsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        if (asyncSearchContext == null) {
            throw new AsyncSearchContextMissingException(asyncSearchContextId);
        }
        return asyncSearchContext;
    }

    protected void putContext(AsyncSearchContext asyncSearchContext) {
        activeContexts.put(asyncSearchContext.getAsyncSearchContextId().getId(), asyncSearchContext);
    }

    public boolean freeContext(AsyncSearchContextId asyncSearchContextId) throws IOException {
        persistenceService.deleteResponse(AsyncSearchId.buildAsyncId(
                new AsyncSearchId(clusterService.localNode().getId(), asyncSearchContextId)));
        AsyncSearchContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (asyncSearchContext != null) {
            logger.info("Removing {} from context map", asyncSearchContextId);
            activeContexts.remove(asyncSearchContextId.getId());
            //TODO on free context
            return true;
        }
        return false;
    }

    public AsyncSearchResponse onSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        AsyncSearchContext asyncSearchContext = findContext(asyncSearchContextId);
        asyncSearchContext.processFinalResponse(searchResponse);
        this.persistenceService.createResponseAsync(asyncSearchContext.getAsyncSearchResponse(),
                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        asyncSearchContext.performPostPersistenceCleanup();
                        asyncSearchContext.setStage(PERSISTED);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        //FIXME : What to do in this case? Do we need a
                        // stage FAILED_TO_PERSIST for completed searches which can be swept and persisted later. The search response is
                        // still in memory.
                        logger.error("Failed to persist final response for {}", asyncSearchContext.getId(), e);
                    }
                });
        return asyncSearchContext.getAsyncSearchResponse();
    }

    public void onSearchFailure(Exception e, AsyncSearchContext asyncSearchContext) {
        asyncSearchContext.processFailure(e);
    }

    /*public void cancelTask() {
        if (isCancelled())
            return;
        logger.info("Cancelling task [{}] on node : [{}]", getTask().getId(), nodeId);
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(.taskInfo(nodeId, false).getTaskId());
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
    }*/

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        for (final AsyncSearchContext context : activeContexts.values()) {
            AsyncSearchContextId asyncSearchContextId = context.getAsyncSearchContextId();
            try {
                freeContext(asyncSearchContextId);
            } catch (IOException e) {

                logger.error("Failed to free async search context " + asyncSearchContextId.toString(), e);
            }
        }
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    public void updateExpiryTimeIfRequired(GetAsyncSearchRequest request,
                                           AsyncSearchContext asyncSearchContext, ActionListener<AsyncSearchResponse> listener) {
        if (PERSISTED.equals(asyncSearchContext.getStage())) {
            updatePersistedResponseIfRequired(request, listener);
        } else {
            updateInMemoryResponseIfRequired(request, asyncSearchContext, listener);
        }
    }

    public void updateInMemoryResponseIfRequired(GetAsyncSearchRequest request, AsyncSearchContext asyncSearchContext,
                                                 ActionListener<AsyncSearchResponse> listener) {
        if (request.getKeepAlive() != null) {
            long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
            if (requestedExpirationTime > asyncSearchContext.getExpirationTimeMillis()) {
                asyncSearchContext.setExpirationMillis(requestedExpirationTime);
            }
        }
        PrioritizedListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                request.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener, (actionListener) -> {
                    listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                    ((CompositeSearchProgressActionListener)
                            asyncSearchContext.getSearchTask().getProgressListener()).removeListener(actionListener);
                });
        ((CompositeSearchProgressActionListener) asyncSearchContext.getSearchTask().getProgressListener())
                .addOrExecuteListener(wrappedListener);

    }

    public void updatePersistedResponseIfRequired(GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        persistenceService.getResponse(request.getId(), new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                if (request.getKeepAlive() != null) {
                    long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
                    if (requestedExpirationTime > asyncSearchResponse.getExpirationTimeMillis()) {
                        persistenceService.updateExpirationTimeAsync(request.getId(), requestedExpirationTime);
                    }
                    listener.onResponse(new AsyncSearchResponse(asyncSearchResponse, requestedExpirationTime));
                } else {
                    listener.onResponse(asyncSearchResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to get async search response {} from index", request.getId());
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

    }

    public void getAsyncSearchResponse(AsyncSearchContext context, ActionListener<AsyncSearchResponse> listener) {
        persistenceService.getResponse(context.getId(), listener);
    }

    public void onCancelled(AsyncSearchContextId contextId) {
        AsyncSearchContext asyncSearchContext = findContext(contextId);
        asyncSearchContext.setStage(ABORTED);
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            Set<AsyncSearchContext> toReap = activeContexts.values().stream().
                    filter(a -> a.getStage().equals(ABORTED) || a.getStage().equals(PERSISTED)).
                    collect(Collectors.toSet());
            //toReap.forEach(a -> freeContext(a.getAsyncSearchContextId()));
        }
    }
}
