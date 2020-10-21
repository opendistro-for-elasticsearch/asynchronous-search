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

import com.amazon.opendistroforelasticsearch.search.async.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.processor.AsyncSearchPostProcessor;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.*;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/***
 * Manages the lifetime of {@link AsyncSearchContext} for all the async searches running on the coordinator node.
 */

public class AsyncSearchService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchService.class);

    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.max_keep_alive", timeValueDays(10), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
            Setting.positiveTimeSetting("async_search.keep_alive_interval", timeValueMinutes(1), Setting.Property.NodeScope);

    private volatile long maxKeepAlive;
    private final AtomicLong idGenerator = new AtomicLong();
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AsyncSearchPersistenceService persistenceService;
    private final InternalAsyncSearchStats statsListener;
    private final AsyncSearchActiveStore asyncSearchActiveStore;
    private final AsyncSearchPostProcessor asyncSearchPostProcessor;
    private final Scheduler.Cancellable keepAliveReaper;
    private final LongSupplier currentTimeSupplier;

    public AsyncSearchService(AsyncSearchPersistenceService asyncSearchPersistenceService,
                              Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        Settings settings = clusterService.getSettings();
        setKeepAlive(MAX_KEEPALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_KEEPALIVE_SETTING, this::setKeepAlive);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.persistenceService = asyncSearchPersistenceService;
        this.statsListener = new InternalAsyncSearchStats();
        this.asyncSearchActiveStore = new AsyncSearchActiveStore(clusterService);
        this.currentTimeSupplier = threadPool::absoluteTimeInMillis;
        this.asyncSearchPostProcessor = new AsyncSearchPostProcessor(asyncSearchPersistenceService, asyncSearchActiveStore);
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new ContextReaper(), KEEPALIVE_INTERVAL_SETTING.get(settings),
                AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
    }

    private void setKeepAlive(TimeValue maxKeepAlive) {
        this.maxKeepAlive = maxKeepAlive.millis();
    }


    public AsyncSearchContext createAndStoreContext(TimeValue keepAlive, boolean keepOnCompletion, long relativeStartTimeMillis) {
        if (keepAlive.getMillis() > maxKeepAlive) {
            throw new IllegalArgumentException(
                    "Keep alive for async search (" + keepAlive.getMillis() + ") is too large It must be less than (" +
                            TimeValue.timeValueMillis(maxKeepAlive) + ").This limit can be set by changing the [" + MAX_KEEPALIVE_SETTING.getKey() + "] " +
                            "cluster level setting.");
        }
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchProgressListener progressActionListener = new AsyncSearchProgressListener(relativeStartTimeMillis,
                (response) -> asyncSearchPostProcessor.processSearchResponse(response, asyncSearchContextId),
                (e) -> asyncSearchPostProcessor.processSearchFailure(e, asyncSearchContextId),
                threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME), threadPool::relativeTimeInMillis);
        AsyncSearchActiveContext asyncSearchContext = new AsyncSearchActiveContext(asyncSearchContextId, clusterService.localNode().getId(),
                keepAlive, keepOnCompletion, threadPool, currentTimeSupplier, progressActionListener, statsListener);
        asyncSearchActiveStore.putContext(asyncSearchContextId, asyncSearchContext);
        return asyncSearchContext;
    }


    public Runnable preProcessSearch(SearchTask searchTask, AsyncSearchContextId asyncSearchContextId) {
        Runnable advanceStage = () -> {};
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            asyncSearchContextOptional.get().setTask(searchTask);
            advanceStage = () -> asyncSearchContextOptional.get().setStage(RUNNING);
        }
        return advanceStage;
    }


    public void findContext(String id, AsyncSearchId asyncSearchId, ActionListener<AsyncSearchContext> listener) {
        AsyncSearchContextId asyncSearchContextId = asyncSearchId.getAsyncSearchContextId();
        Optional<AsyncSearchActiveContext> asyncSearchActiveContext = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchActiveContext.isPresent()) {
            listener.onResponse(asyncSearchActiveContext.get());
        } else {
            persistenceService.getResponse(id, ActionListener.wrap((persistenceModel) ->
                 listener.onResponse(new AsyncSearchPersistenceContext(asyncSearchId, persistenceModel, currentTimeSupplier)),
                 ex -> {
                logger.debug(() -> new ParameterizedMessage("Context not found for ID {}", id, ex));
                listener.onFailure(new ResourceNotFoundException(id));}
            ));
        }
    }


    public Set<SearchTask> getOverRunningTasks() {
        Map<Long, AsyncSearchActiveContext> allContexts = asyncSearchActiveStore.getAllContexts();
        return Collections.unmodifiableSet(allContexts.values().stream()
                .filter(Objects::nonNull)
                .filter(context -> context.isExpired())
                .filter(context -> context.getTask().isCancelled() == false)
                .map(AsyncSearchActiveContext::getTask)
                .collect(Collectors.toSet()));
    }


    public void freeContext(String id, AsyncSearchId asyncSearchId, ActionListener<Boolean> listener) {
        AsyncSearchContextId asyncSearchContextId = asyncSearchId.getAsyncSearchContextId();
        // if there are no context found to be cleaned up we throw a ResourceNotFoundException
        GroupedActionListener<Boolean> groupedDeletionListener = new GroupedActionListener<>(
                ActionListener.wrap((responses) -> {
                    if (responses.stream().anyMatch(r -> r)) {
                        listener.onResponse(true);
                    } else {
                        listener.onFailure(new ResourceNotFoundException(id));
                    }
                }, listener::onFailure), 2);
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            //cancel any ongoing async search tasks
            if (asyncSearchContext.getTask() != null && asyncSearchContext.getTask().isCancelled() == false) {
                client.admin().cluster().prepareCancelTasks().setTaskId(new TaskId(clusterService.localNode().getId(),
                    asyncSearchContext.getTask().getId()))
                    .execute(ActionListener.wrap((response) -> {
                                logger.debug("Task cancellation for async search context {}  succeeded with response () ",
                                        asyncSearchContext.getAsyncSearchId(), response);
                                asyncSearchContext.acquireContextPermit(ActionListener.wrap(
                                        releasable -> {
                                            groupedDeletionListener.onResponse(asyncSearchActiveStore.freeContext(asyncSearchContextId));
                                            releasable.close();
                                        }, listener::onFailure), TimeValue.timeValueSeconds(5), "free context");
                            },
                            (e) -> {
                                asyncSearchContext.acquireContextPermit(ActionListener.wrap(
                                        releasable -> {
                                            groupedDeletionListener.onResponse(asyncSearchActiveStore.freeContext(asyncSearchContextId));
                                            releasable.close();
                                        }, listener::onFailure), TimeValue.timeValueSeconds(5), "free context");
                                logger.debug(() -> new ParameterizedMessage("Unable to cancel async search task {}",
                                        asyncSearchContext.getTask(), e));
                            }
                    ));
            }
            //free async search context if one exists
            groupedDeletionListener.onResponse(asyncSearchActiveStore.freeContext(asyncSearchContextId));
        } else {
            // async search context didn't exist so obviously we didn't delete
            groupedDeletionListener.onResponse(false);
        }

        //deleted persisted context if one exists. If not the listener returns acknowledged as false
        persistenceService.deleteResponse(id, groupedDeletionListener);
    }

    public void updateKeepAliveAndGetContext(String id, TimeValue keepAlive, AsyncSearchId asyncSearchId, ActionListener<AsyncSearchContext> listener) {
        long requestedExpirationTime = currentTimeSupplier.getAsLong() + keepAlive.getMillis();
        // find an active context on this node if one exists
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchId.getAsyncSearchContextId());
        // for all other stages we don't really care much as those contexts are destined to be discarded
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchActiveContext = asyncSearchContextOptional.get();
            asyncSearchActiveContext.acquireContextPermit(ActionListener.wrap(
                    releasable -> {
                        // At this point it's possible that the response would have been persisted to system index
                        if (asyncSearchActiveContext.getStage() == PERSISTED) {
                            persistenceService.updateExpirationTimeAndGet(id, requestedExpirationTime, ActionListener.wrap((actionResponse) ->
                                 listener.onResponse(new AsyncSearchPersistenceContext(asyncSearchActiveContext.getAsyncSearchId(), actionResponse, currentTimeSupplier)),
                                    listener::onFailure));
                        } else {
                            asyncSearchActiveContext.setExpirationMillis(requestedExpirationTime);
                            listener.onResponse(asyncSearchActiveContext);
                        }
                        releasable.close();
                    },
                    listener::onFailure), TimeValue.timeValueSeconds(5), "persisting response");
        } else {
            // try update the doc on the index assuming there exists one.
            persistenceService.updateExpirationTimeAndGet(id, requestedExpirationTime,
                    ActionListener.wrap((actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(
                            asyncSearchId, actionResponse, currentTimeSupplier)), listener::onFailure));
        }
    }


    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO listen to coordinator state failures and async search shards getting assigned
    }


    public void onCancelled(AsyncSearchContextId contextId) {
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(contextId);
        if (asyncSearchContextOptional.isPresent()) {
            // TODO should we let the partial response stay if the cancellation was done due to tasks overrunning
            asyncSearchActiveStore.freeContext(asyncSearchContextOptional.get().getAsyncSearchContextId());
            logger.debug("Freed context on cancellation for {}", asyncSearchContextOptional.get());
        }
    }

    public AsyncSearchStats stats(boolean count) {
        return statsListener.stats(count, clusterService.localNode());
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        asyncSearchActiveStore.freeAllContexts();
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    /***
     * Reaps the contexts ready to be expunged
     */
    class ContextReaper implements Runnable {

        @Override
        public void run() {
            try {
                for (AsyncSearchActiveContext asyncSearchActiveContext : asyncSearchActiveStore.getAllContexts().values()) {
                    AsyncSearchActiveContext.Stage stage = asyncSearchActiveContext.getStage();
                    if (stage != null && (asyncSearchActiveContext.retainedStages().contains(stage) == false || asyncSearchActiveContext.isExpired())) {
                        asyncSearchActiveStore.freeContext(asyncSearchActiveContext.getAsyncSearchContextId());
                    }
                }
            } catch (Exception e) {
                logger.debug("Exception while reaping contexts", e);
            }
        }
    }
}
