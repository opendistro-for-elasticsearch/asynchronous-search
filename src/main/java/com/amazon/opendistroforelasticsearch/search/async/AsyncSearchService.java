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
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.*;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;

/***
 * Manages the lifetime of {@link AsyncSearchContext} for all the async searches running on the coordinator node. Once the
 * response have been persisted or otherwise ready to be expunged, the {@link Reaper} frees up the in-memory contexts maintained
 * on the coordinator node
 */
public class AsyncSearchService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchService.class);

    private volatile long defaultKeepAlive;

    private volatile long maxKeepAlive;

    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
            Setting.positiveTimeSetting("async_search.keep_alive_interval", timeValueMinutes(1), Setting.Property.NodeScope);

    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.default_keep_alive", timeValueMinutes(5), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.max_keep_alive", timeValueHours(24), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);

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
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_KEEPALIVE_SETTING, MAX_KEEPALIVE_SETTING,
                this::setKeepAlives, this::validateKeepAlives);
        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        this.threadPool = threadPool;
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.persistenceService = asyncSearchPersistenceService;
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

    /**
     * The method tries to find a context in-memory and if it's not found, it tries to look up data on the disk. It no data
     * on the disk exists, it's likely that the context has expired in which case {@link AsyncSearchContextMissingException}
     * is thrown
     * @param asyncSearchContextId
     * @return
     */
    public AsyncSearchContext findContext(AsyncSearchContextId asyncSearchContextId,  ActionListener<AsyncSearchContext> listener) {
        final AsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        if (asyncSearchContext == null) {
            throw new AsyncSearchContextMissingException(asyncSearchContextId);
        }
        return asyncSearchContext;
    }

    public Set<SearchTask> getOverRunningTasks() {
        return Collections.unmodifiableSet(activeContexts.values().stream()
                .filter(a -> a.isExpired())
                .filter(a -> a.getTask().isCancelled() == false)
                .map(a -> a.getTask())
                .collect(Collectors.toSet()));
    }

    protected void putContext(AsyncSearchContext asyncSearchContext) {
        activeContexts.put(asyncSearchContext.getAsyncSearchContextId().getId(), asyncSearchContext);
    }

    public boolean freeContext(AsyncSearchContextId asyncSearchContextId, ActionListener<Boolean> actionListener) {
        try {
            persistenceService.deleteResponseAsync(AsyncSearchId.buildAsyncId(
                    new AsyncSearchId(clusterService.localNode().getId(), asyncSearchContextId)));
            AsyncSearchContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
            if (asyncSearchContext != null) {
                logger.info("Removing {} from context map", asyncSearchContextId);
                AsyncSearchContext.Stage stage = asyncSearchContext.getStage();
                if (RUNNING.equals(stage) || INIT.equals(stage)) {
                    AsyncSearchUtils.cancelTask(new TaskId(clusterService.localNode().getId(), asyncSearchContext.getTask().getId()),
                            client);
                }
                activeContexts.remove(asyncSearchContextId.getId());
                return true;
            } else {
                throw new ResourceNotFoundException(getAsyncSearchId(asyncSearchContextId));
            }
        } catch (IOException e) {
            logger.error("Failed to build asyncsearch id for context ["
                    + asyncSearchContextId.getId() + "] on node [" + clusterService.localNode().getId() + "]", e);
            return false;
        }
    }

    public boolean freeCachedContext(AsyncSearchContextId asyncSearchContextId) {
        AsyncSearchContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (asyncSearchContext != null) {
            logger.debug("Removing {} from context map", asyncSearchContextId);
            activeContexts.remove(asyncSearchContextId.getId());
            return true;
        }
        return false;
    }

    public AsyncSearchResponse onSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        AsyncSearchContext asyncSearchContext = findContext(asyncSearchContextId, new ActionListener<AsyncSearchContext>() {
            @Override
            public void onResponse(AsyncSearchContext asyncSearchContext) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });
        asyncSearchContext.processFinalResponse(searchResponse);
        this.persistenceService.createResponseAsync(asyncSearchContext.geLatestSearchResponse(),
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
        return asyncSearchContext.geLatestSearchResponse();
    }

    public void onSearchFailure(Exception e, AsyncSearchContext asyncSearchContext) {
        asyncSearchContext.processFailure(e);
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        for (final AsyncSearchContext context : activeContexts.values()) {
            freeCachedContext(context.getAsyncSearchContextId());
        }
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    /***
     *
     * @param request
     * @param asyncSearchContext
     * @param listener
     */
    public void updateKeepAlive(GetAsyncSearchRequest request, AsyncSearchContext asyncSearchContext, ActionListener<ActionResponse> listener) {
        assert request.getKeepAlive() != null : "Keep Alive requested is null";
        AsyncSearchContext.Stage stage = asyncSearchContext.getStage();
        long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
        if (stage.equals(PERSISTED)) {
            persistenceService.updateExpirationTime(request.getId(), requestedExpirationTime, new ActionListener<ActionResponse>() {
                @Override
                public void onResponse(ActionResponse actionResponse) {

                }

                @Override
                public void onFailure(Exception e) {

                }
            });
        } else {
            //TODO acquire lock so that if the context is about to be persisted, we don't lose out on the updated state
            asyncSearchContext.setExpirationMillis(requestedExpirationTime);
        }
    }

    public void fetchAsyncSearchResponse(String asyncId, ActionListener<AsyncSearchResponse> listener) {


    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

    }

    /***
     * Listens to the cancellation for {@link com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask} and
     * updates the corresponding state on disk.
     * @param contextId
     */
    public void onCancelled(AsyncSearchContextId contextId) {
        findContext(contextId, new ActionListener<AsyncSearchContext>() {
            @Override
            public void onResponse(AsyncSearchContext asyncSearchContext) {
                asyncSearchContext.setStage(ABORTED);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to set stage as +" + ABORTED);
            }
        });
    }

    /***
     * Reaps the contexts ready to be expunged
     */
    class Reaper implements Runnable {

        @Override
        public void run() {
            try {
                Set<AsyncSearchContext> toReap = activeContexts.values().stream()
                        .filter(a -> a.getStage().equals(ABORTED) || a.getStage().equals(PERSISTED))
                        .collect(Collectors.toSet());
                toReap.forEach(a -> freeCachedContext(a.getAsyncSearchContextId()));
            } catch (Exception e) {
                logger.error("Exception while reaping contexts");
            }
        }
    }

    private String getAsyncSearchId(AsyncSearchContextId asyncSearchContextId) {
        String id = null;
        try {
            id = AsyncSearchId.buildAsyncId(new AsyncSearchId(clusterService.localNode().getId(),
                    asyncSearchContextId));
        } catch (IOException e) {
            logger.error(e);
        }
        return id;
    }
}
