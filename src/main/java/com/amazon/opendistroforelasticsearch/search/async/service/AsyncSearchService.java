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

package com.amazon.opendistroforelasticsearch.search.async.service;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchDeletionEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.exception.AsyncSearchStateMachineException;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.processor.AsyncSearchPostProcessor;
import com.amazon.opendistroforelasticsearch.search.async.service.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.service.persistence.AsyncSearchPersistenceService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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

import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/***
 * Manages the lifetime of {@link AsyncSearchContext} for all the async searches running on the coordinator node.
 */

public class AsyncSearchService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AsyncSearchService.class);

    public static final Setting<TimeValue> MAX_KEEP_ALIVE_SETTING = Setting.positiveTimeSetting("async_search.max_keep_alive",
            timeValueDays(10), Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> KEEP_ALIVE_INTERVAL_SETTING = Setting.positiveTimeSetting("async_search.keep_alive_interval",
            timeValueMinutes(1), Setting.Property.NodeScope);
    public static final Setting<Boolean> KEEP_ON_CANCELLATION = Setting.boolSetting("async_search.keep_on_cancellation", true,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    private volatile long maxKeepAlive;
    private volatile boolean keepOnCancellation;
    private final AtomicLong idGenerator = new AtomicLong();
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AsyncSearchPersistenceService persistenceService;
    private final AsyncSearchActiveStore asyncSearchActiveStore;
    private final AsyncSearchPostProcessor asyncSearchPostProcessor;
    private final Scheduler.Cancellable contextReaper;
    private final LongSupplier currentTimeSupplier;
    private final AsyncSearchStateMachine asyncSearchStateMachine;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public AsyncSearchService(AsyncSearchPersistenceService asyncSearchPersistenceService,
                              Client client, ClusterService clusterService, ThreadPool threadPool,
                              AsyncSearchActiveStore asyncSearchActiveStore, AsyncSearchPostProcessor asyncSearchPostProcessor,
                              AsyncSearchStateMachine stateMachine, NamedWriteableRegistry namedWriteableRegistry) {
        this.client = client;
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchPostProcessor = asyncSearchPostProcessor;
        Settings settings = clusterService.getSettings();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_KEEP_ALIVE_SETTING, this::setKeepAlive);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(KEEP_ON_CANCELLATION, this::setKeepOnCancellation);
        setKeepAlive(MAX_KEEP_ALIVE_SETTING.get(settings));
        setKeepOnCancellation(KEEP_ON_CANCELLATION.get(settings));
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.persistenceService = asyncSearchPersistenceService;
        this.currentTimeSupplier = threadPool::absoluteTimeInMillis;
        // every node cleans up it's own in-memory context which should either be discarded or has expired
        this.contextReaper = threadPool.scheduleWithFixedDelay(new ContextReaper(), KEEP_ALIVE_INTERVAL_SETTING.get(settings),
                AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
        asyncSearchStateMachine = stateMachine;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    private void setKeepOnCancellation(Boolean keepOnCancellation) {
        this.keepOnCancellation = keepOnCancellation;
    }

    private void setKeepAlive(TimeValue maxKeepAlive) {
        this.maxKeepAlive = maxKeepAlive.millis();
    }


    /**
     * Creates a new active async search for a newly submitted async search.
     *
     * @param keepAlive               duration of validity of async search
     * @param keepOnCompletion        determines if response should be persisted on completion
     * @param relativeStartTimeMillis start time of {@linkplain SearchAction}
     * @return the async search context
     */
    public AsyncSearchContext createAndStoreContext(TimeValue keepAlive, boolean keepOnCompletion, long relativeStartTimeMillis) {
        if (keepAlive.getMillis() > maxKeepAlive) {
            throw new IllegalArgumentException(
                    "Keep alive for async search (" + keepAlive.getMillis() + ") is too large It must be less than (" +
                            TimeValue.timeValueMillis(maxKeepAlive) + ").This limit can be set by changing the ["
                            + MAX_KEEP_ALIVE_SETTING.getKey() + "] cluster level setting.");
        }
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchProgressListener progressActionListener = new AsyncSearchProgressListener(relativeStartTimeMillis,
                (response) -> asyncSearchPostProcessor.processSearchResponse(response, asyncSearchContextId),
                (e) -> asyncSearchPostProcessor.processSearchFailure(e, asyncSearchContextId),
                threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME), threadPool::relativeTimeInMillis);
        AsyncSearchActiveContext asyncSearchContext = new AsyncSearchActiveContext(asyncSearchContextId, clusterService.localNode().getId(),
                keepAlive, keepOnCompletion, threadPool, currentTimeSupplier, progressActionListener,
                /*placeholder for async search stats*/new AsyncSearchContextListener() {
        });
        asyncSearchActiveStore.putContext(asyncSearchContextId, asyncSearchContext);
        return asyncSearchContext;
    }


    /**
     * Stores information of the {@linkplain SearchTask} in the async search and signals start of the the underlying
     * {@linkplain SearchAction}
     *
     * @param searchTask           The {@linkplain SearchTask} which stores information of the currently running {@linkplain SearchTask}
     * @param asyncSearchContextId the id of the active asyncsearch context
     */
    public void bootstrapSearch(SearchTask searchTask, AsyncSearchContextId asyncSearchContextId) {
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext context = asyncSearchContextOptional.get();
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context, searchTask));
        }
    }


    /**
     * Tries to find an {@linkplain AsyncSearchActiveContext}. If not found, queries the {@linkplain AsyncSearchPersistenceService}  for
     * a hit. If a response is found, it builds and returns an {@linkplain AsyncSearchPersistenceContext}, else throws
     * {@linkplain ResourceNotFoundException}
     *
     * @param id                   The async search id
     * @param asyncSearchContextId the Async search context id
     * @param listener             to be invoked on finding an {@linkplain AsyncSearchContext}
     */
    public void findContext(String id, AsyncSearchContextId asyncSearchContextId, ActionListener<AsyncSearchContext> listener) {
        Optional<AsyncSearchActiveContext> asyncSearchActiveContext = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchActiveContext.isPresent()) {
            listener.onResponse(asyncSearchActiveContext.get());
        } else {
            persistenceService.getResponse(id, ActionListener.wrap((persistenceModel) ->
                            listener.onResponse(new AsyncSearchPersistenceContext(id, asyncSearchContextId, persistenceModel,
                                    currentTimeSupplier, namedWriteableRegistry)),
                    ex -> {
                        logger.debug(() -> new ParameterizedMessage("Context not found for ID {}", id), ex);
                        listener.onFailure(new ResourceNotFoundException(id));
                    }
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


    /**
     * Attempts to find both an {@linkplain AsyncSearchActiveContext} and an {@linkplain AsyncSearchPersistenceContext} and delete them.
     * If at least one of the aforementioned objects are found and deleted successfully, the listener is invoked with #true, else
     * {@linkplain ResourceNotFoundException} is thrown.
     *
     * @param id                   async search id
     * @param asyncSearchContextId context id
     * @param listener             listener to invoke on deletion or failure to do so
     */
    public void freeContext(String id, AsyncSearchContextId asyncSearchContextId, ActionListener<Boolean> listener) {
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
            logger.warn("Active context present for async search id [{}]", id);
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            //cancel any ongoing async search tasks
            if (asyncSearchContext.getTask() != null && asyncSearchContext.getTask().isCancelled() == false) {
                client.admin().cluster().prepareCancelTasks().setTaskId(new TaskId(clusterService.localNode().getId(),
                        asyncSearchContext.getTask().getId()))
                        .execute(ActionListener.wrap((response) -> {
                                    logger.debug("Task cancellation for async search context [{}]  succeeded with response [{}] ",
                                            id, response);
                                    freeActiveContext(asyncSearchContext, groupedDeletionListener);
                                },
                                (e) -> {
                                    logger.debug(() -> new ParameterizedMessage("Unable to cancel async search task [{}] " +
                                            "for async search id [{}]", asyncSearchContext.getTask(), id), e);
                                    freeActiveContext(asyncSearchContext, groupedDeletionListener);
                                }
                        ));
            } else {
                //free async search context if one exists
                freeActiveContext(asyncSearchContext, groupedDeletionListener);
            }
        } else {
            logger.warn("Active context NOT present for async search id [{}]", id);
            // async search context didn't exist so obviously we didn't delete
            groupedDeletionListener.onResponse(false);
            //deleted persisted context if one exists. If not the listener returns acknowledged as false
            //we don't need to acquire lock if the in-memory context doesn't exist. For persistence context we have a distributed view
            //with the last writer wins policy
            logger.warn("Deleting async search id [{}] from system index ", id);
            persistenceService.deleteResponse(id, groupedDeletionListener);
        }
    }

    private void freeActiveContext(AsyncSearchActiveContext asyncSearchContext, GroupedActionListener<Boolean> groupedDeletionListener) {
        //Intent of the lock here is to disallow ongoing migration to system index
        // as if that is underway we might end up creating a new document post a DELETE was executed
        logger.warn("Acquiring context permit for freeing context for async search id [{}]", asyncSearchContext.getAsyncSearchId());
        asyncSearchContext.acquireContextPermit(ActionListener.wrap(
                releasable -> {
                    try {
                        asyncSearchStateMachine.trigger(new SearchDeletionEvent(asyncSearchContext));
                        groupedDeletionListener.onResponse(true);
                    } catch (AsyncSearchStateMachineException ex) {
                        groupedDeletionListener.onResponse(false);
                    }
                    logger.warn("Deleting async search id [{}] from system index ", asyncSearchContext.getAsyncSearchId());
                    persistenceService.deleteResponse(asyncSearchContext.getAsyncSearchId(), groupedDeletionListener);
                    releasable.close();
                }, exception -> {
                    groupedDeletionListener.onResponse(false);
                    logger.warn("Deleting async search id [{}] from system index ", asyncSearchContext.getAsyncSearchId());
                    persistenceService.deleteResponse(asyncSearchContext.getAsyncSearchId(), groupedDeletionListener);
                }
        ), TimeValue.timeValueSeconds(5), "free context");
    }

    /**
     * If an active context is found, a permit is acquired from
     * {@linkplain com.amazon.opendistroforelasticsearch.search.async.context.permits.AsyncSearchContextPermits} and on acquisition of
     * permit, a check is performed to see if response has been persisted in system index. If true, we update expiration in index. Else
     * we update expiration field in {@linkplain AsyncSearchActiveContext}.
     *
     * @param id                   async search id
     * @param keepAlive            the new keep alive duration
     * @param asyncSearchContextId async search context id
     * @param listener             listener to invoke after updating expiration.
     */
    public void updateKeepAliveAndGetContext(String id, TimeValue keepAlive, AsyncSearchContextId asyncSearchContextId,
                                             ActionListener<AsyncSearchContext> listener) {
        long requestedExpirationTime = currentTimeSupplier.getAsLong() + keepAlive.getMillis();
        // find an active context on this node if one exists
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        // for all other stages we don't really care much as those contexts are destined to be discarded
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchActiveContext = asyncSearchContextOptional.get();
            asyncSearchActiveContext.acquireContextPermit(ActionListener.wrap(
                    releasable -> {
                        // At this point it's possible that the response would have been persisted to system index
                        if (asyncSearchActiveContext.getAsyncSearchState() == AsyncSearchState.PERSISTED) {
                            persistenceService.updateExpirationTime(id, requestedExpirationTime, ActionListener.wrap(
                                    (actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(id, asyncSearchContextId,
                                            actionResponse, currentTimeSupplier, namedWriteableRegistry)), listener::onFailure));
                        } else {
                            asyncSearchActiveContext.setExpirationTimeMillis(requestedExpirationTime);
                            listener.onResponse(asyncSearchActiveContext);
                        }
                        releasable.close();
                    },
                    //TODO introduce request timeouts to make the permit wait transparent to the client
                    listener::onFailure), TimeValue.timeValueSeconds(5), "update keep alive");
        } else {
            // try update the doc on the index assuming there exists one.
            persistenceService.updateExpirationTime(id, requestedExpirationTime,
                    ActionListener.wrap((actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(
                            id, asyncSearchContextId, actionResponse, currentTimeSupplier, namedWriteableRegistry)), listener::onFailure));
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO listen to coordinator state failures and async search shards getting assigned
    }

    public boolean keepOnCancellation() {
        return keepOnCancellation;
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
        contextReaper.cancel();
    }

    /***
     * Reaps the active contexts ready to be expunged
     */
    class ContextReaper implements Runnable {

        @Override
        public void run() {
            for (AsyncSearchActiveContext asyncSearchActiveContext : asyncSearchActiveStore.getAllContexts().values()) {
                try {
                    AsyncSearchState stage = asyncSearchActiveContext.getAsyncSearchState();
                    if (stage != null && (
                            asyncSearchActiveContext.retainedStages().contains(stage) == false || asyncSearchActiveContext.isExpired())) {
                        asyncSearchStateMachine.trigger(new SearchDeletionEvent(asyncSearchActiveContext));
                    }
                } catch (Exception e) {
                    logger.debug("Exception occured while reaping async search active context for id "
                            + asyncSearchActiveContext.getAsyncSearchId(), e);
                }
            }
        }
    }
}
