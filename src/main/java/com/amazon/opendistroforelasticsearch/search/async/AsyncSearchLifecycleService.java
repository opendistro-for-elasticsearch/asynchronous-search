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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;

/**
 * Once the response have been persisted or otherwise ready to be expunged, the {@link AsyncSearchLifecycleService.Reaper} frees up the
 * in-memory contexts maintained on the coordinator node
 */
public class AsyncSearchLifecycleService extends AbstractLifecycleComponent {

    private static Logger logger = LogManager.getLogger(AsyncSearchLifecycleService.class);

    private final Scheduler.Cancellable keepAliveReaper;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private volatile int maxRunningContext;

    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
            Setting.positiveTimeSetting("async_search.keep_alive_interval", timeValueMinutes(1), Setting.Property.NodeScope);

    public static final Setting<Integer> MAX_RUNNING_CONTEXT =
            Setting.intSetting("async_search.max_running_context", 100, 0, Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final ConcurrentMapLong<ActiveAsyncSearchContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();

    public AsyncSearchLifecycleService(ThreadPool threadPool, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        Settings settings = clusterService.getSettings();
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), KEEPALIVE_INTERVAL_SETTING.get(settings),
                ThreadPool.Names.GENERIC);
        maxRunningContext = MAX_RUNNING_CONTEXT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_RUNNING_CONTEXT, this::setMaxRunningContext);
    }

    private void setMaxRunningContext(int maxRunningContext) {
        this.maxRunningContext = maxRunningContext;
    }

    /**
     * @param asyncSearchContextId New Key being inserted into active context map
     * @param asyncSearchContext   New Value being insert into active context map
     */
    public void putContext(AsyncSearchContextId asyncSearchContextId, ActiveAsyncSearchContext asyncSearchContext) {
        if (activeContexts.values().stream().filter(context -> context.isRunning()).distinct().count() > maxRunningContext) {
            throw new AsyncSearchRejectedException(
                    "Trying to create too many running contexts. Must be less than or equal to: [" +
                            maxRunningContext + "]. " + "This limit can be set by changing the ["
                            + MAX_RUNNING_CONTEXT.getKey() + "] setting.", maxRunningContext);
        }
        activeContexts.put(asyncSearchContextId.getId(), asyncSearchContext);
    }

    /**
     * Returns the context id if present
     * @param contextId AsyncSearchContextId
     * @return ActiveAsyncSearchContext
     */
    public ActiveAsyncSearchContext getContext(AsyncSearchContextId contextId) {
        ActiveAsyncSearchContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return null;
        }
        if (context.getAsyncSearchContextId().getContextId().equals(contextId.getContextId())) {
            return context;
        }
        return null;
    }


    /**
     * Returns a copy of all active contexts
     *
     * @return all context
     */
    public Map<Long, ActiveAsyncSearchContext> getAllContexts() {
        return CollectionUtils.copyMap(activeContexts);
    }


    /**
     * Frees the active context
     * @param asyncSearchContextId asyncSearchContextId
     * @return acknowledgement of context removal
     */
    public boolean freeContext(AsyncSearchContextId asyncSearchContextId) {
        AsyncSearchContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (asyncSearchContext != null) {
            logger.debug("Removing {} from context map", asyncSearchContextId);
            activeContexts.remove(asyncSearchContextId.getId());
            return true;
        }
        return false;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        freeAllContexts();
    }

    public void freeAllContexts() {
        for (final AsyncSearchContext context : activeContexts.values()) {
            freeContext(context.getAsyncSearchContextId());
        }
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    /***
     * Reaps the contexts ready to be expunged
     */
    class Reaper implements Runnable {

        @Override
        public void run() {
            try {
                for (ActiveAsyncSearchContext activeAsyncSearchContext : activeContexts.values()) {
                    Optional<ActiveAsyncSearchContext.Stage> stage = activeAsyncSearchContext.getSearchStage();
                    if (stage.isPresent() && stage.get().equals(ActiveAsyncSearchContext.Stage.ABORTED)
                            || stage.equals(ActiveAsyncSearchContext.Stage.FAILED)
                            || stage.equals(ActiveAsyncSearchContext.Stage.PERSISTED)
                            || stage.equals(ActiveAsyncSearchContext.Stage.PERSIST_FAILED)) {
                        freeContext(activeAsyncSearchContext.getAsyncSearchContextId());
                    }
                }
            } catch (Exception e) {
                logger.error("Exception while reaping contexts", e);
            }
        }
    }
}
