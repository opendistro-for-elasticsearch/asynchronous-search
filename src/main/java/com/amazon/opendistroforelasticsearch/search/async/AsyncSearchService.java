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
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

public class  AsyncSearchService extends AbstractLifecycleComponent {

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

    private final ConcurrentMapLong<AsyncSearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final AsyncSearchPersistenceService persistenceService ;

    public AsyncSearchService(AsyncSearchPersistenceService persistenceService, Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        Settings settings = clusterService.getSettings();
        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_KEEPALIVE_SETTING, MAX_KEEPALIVE_SETTING,
                this::setKeepAlives, this::validateKeepAlives);
        this.threadPool = threadPool;
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, ThreadPool.Names.SAME);
        this.clusterService = clusterService;
        this.persistenceService = persistenceService;
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

    public final AsyncSearchContext createAndPutContext(SubmitAsyncSearchRequest submitAsyncSearchRequest, SearchTask task,
                                                        TransportSubmitAsyncSearchAction.SearchTimeProvider timeProvider)
            throws IOException {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchContext asyncSearchContext = new AsyncSearchContext(persistenceService,
                client, clusterService.localNode().getId(), asyncSearchContextId,
                submitAsyncSearchRequest.getKeepAlive(),
                submitAsyncSearchRequest.keepOnCompletion(), task, timeProvider);
        putContext(asyncSearchContext);
        return asyncSearchContext;
    }


    public boolean freeContext(AsyncSearchContextId asyncSearchContextId) {
        AsyncSearchContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (asyncSearchContext != null) {
            logger.info("Removing {} from context map", asyncSearchContextId);
            activeContexts.remove(asyncSearchContextId.getId());
            //TODO on free context
            return true;
        }
        return false;
    }


    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        for (final AsyncSearchContext context : activeContexts.values()) {
            freeContext(context.getAsyncSearchContextId());
        }
    }

    @Override
    protected void doClose() throws IOException {
        doStop();
        keepAliveReaper.cancel();
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            final long time = threadPool.relativeTimeInMillis();
            //TODO reaper logic
        }
    }
}
