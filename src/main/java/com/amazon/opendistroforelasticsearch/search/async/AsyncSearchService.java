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

import com.amazon.opendistroforelasticsearch.search.async.active.ActiveAsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.active.ActiveAsyncSearchStoreService;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.ABORTED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.SUCCEEDED;
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
    private final ActiveAsyncSearchStoreService activeAsyncSearchStoreService;
    private final Scheduler.Cancellable keepAliveReaper;

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
        this.activeAsyncSearchStoreService = new ActiveAsyncSearchStoreService(clusterService);
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new ContextReaper(), KEEPALIVE_INTERVAL_SETTING.get(settings),
                AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
    }

    private void setKeepAlive(TimeValue maxKeepAlive) {
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    /**
     * Creates a new context and attaches a listener for the changes as they progress on the search. The context after being created
     * is saved in a in-memory store
     *
     * @param submitAsyncSearchRequest the request for submitting the async search
     * @param relativeStartMillis      the start time of the async search
     */
    public AsyncSearchContext prepareContext(SubmitAsyncSearchRequest submitAsyncSearchRequest, long relativeStartMillis) {
        if (submitAsyncSearchRequest.getKeepAlive().getMillis() > maxKeepAlive) {
            throw new IllegalArgumentException(
                    "Keep alive for async search (" + TimeValue.timeValueMillis(submitAsyncSearchRequest.getKeepAlive().getMillis()) + ")" +
                            " is too large It must be less than (" + TimeValue.timeValueMillis(maxKeepAlive) + "). " +
                            "This limit can be set by changing the [" + MAX_KEEPALIVE_SETTING.getKey() + "] cluster level setting.");
        }
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchProgressListener progressActionListener = new AsyncSearchProgressListener(relativeStartMillis,
                (response) -> postProcessSearchResponse(response, asyncSearchContextId),
                (e) -> postProcessSearchFailure(e, asyncSearchContextId), threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME),
                System::currentTimeMillis);
        ActiveAsyncSearchContext asyncSearchContext = new ActiveAsyncSearchContext(asyncSearchContextId, clusterService.localNode().getId(),
                submitAsyncSearchRequest.getKeepAlive(),
                submitAsyncSearchRequest.keepOnCompletion(), progressActionListener, statsListener);
        activeAsyncSearchStoreService.putContext(asyncSearchContextId, asyncSearchContext);
        return asyncSearchContext;
    }

    /**
     * Initializes the search task and bind the progress listener to the search task
     *
     * @param searchTask           the search task
     * @param asyncSearchContextId the async search context id
     */
    public Runnable preProcessSearch(SearchTask searchTask, AsyncSearchContextId asyncSearchContextId) {
        ActiveAsyncSearchContext asyncSearchContext = activeAsyncSearchStoreService.getContext(asyncSearchContextId);
        asyncSearchContext.setTask(searchTask);
        asyncSearchContext.setExpirationMillis(searchTask.getStartTime() + asyncSearchContext.getKeepAlive().getMillis());
        return () -> asyncSearchContext.setStage(RUNNING);
    }

    /***
     * The method tries to find a context in-memory and if it's not found, it tries to look up data on the disk. It no data
     * on the disk exists, it's likely that the context has expired in which case {@link ResourceNotFoundException}
     * is thrown
     * @param listener The listener to be invoked once the context is available
     */
    public void findContext(String id, AsyncSearchId asyncSearchId, ActionListener<AsyncSearchContext> listener) {
        AsyncSearchContextId asyncSearchContextId = asyncSearchId.getAsyncSearchContextId();
        ActiveAsyncSearchContext activeAsyncSearchContext = activeAsyncSearchStoreService.getContext(asyncSearchContextId);
        if (activeAsyncSearchContext != null) {
            listener.onResponse(activeAsyncSearchContext);
        } else {
            persistenceService.getResponse(AsyncSearchId.buildAsyncId(asyncSearchId), ActionListener.wrap(
                    (persistenceModel) -> listener.onResponse(new AsyncSearchPersistenceContext(asyncSearchId, persistenceModel)),
                    ex -> listener.onFailure(new ResourceNotFoundException(id))
            ));
        }
    }


    /**
     * Returns the set of tasks running beyond the allowed keep alive. Such tasks are eventually sweeped and are cancelled
     * by the maintenance service
     *
     * @return underlying search tasks
     */
    public Set<SearchTask> getOverRunningTasks() {
        Map<Long, ActiveAsyncSearchContext> allContexts = activeAsyncSearchStoreService.getAllContexts();
        return Collections.unmodifiableSet(allContexts.values().stream()
                .filter(Objects::nonNull)
                .filter(context -> threadPool.relativeTimeInMillis() < context.getExpirationTimeMillis())
                .filter(context -> context.getTask().isCancelled() == false)
                .map(ActiveAsyncSearchContext::getTask)
                .collect(Collectors.toSet()));
    }


    /**
     * Attempts to delete active context and persistence context. If neither exists, we throw RNF, if either or both contexts are existing
     * we invoke listener onResponse(true)
     *
     * @param id                   async search id
     * @param asyncSearchContextId active context id
     * @param listener             handles success or failure
     */
    public void freeContext(String id, AsyncSearchContextId asyncSearchContextId, ActionListener<Boolean> listener) {
        // TODO acquire lock so that when we delete we know that there is no active index transition happening.
        GroupedActionListener<Boolean> groupedDeletionListener = new GroupedActionListener<>(
                ActionListener.wrap((responses) -> {
                    if (responses.stream().anyMatch(r -> r)) {
                        listener.onResponse(true);
                    } else {
                        listener.onFailure(new ResourceNotFoundException(id));
                    }
                }, listener::onFailure), 2);

        //delete active context
        ActiveAsyncSearchContext asyncSearchContext = activeAsyncSearchStoreService.getContext(asyncSearchContextId);
        if (asyncSearchContext != null && asyncSearchContext.getTask() != null && asyncSearchContext.getTask().isCancelled() == false) {
            client.admin().cluster().prepareCancelTasks().setTaskId(new TaskId(clusterService.localNode().getId(),
                    asyncSearchContext.getTask().getId()))
                    .execute(ActionListener.wrap((response) -> {
                                activeAsyncSearchStoreService.freeContext(asyncSearchContextId);
                                groupedDeletionListener.onResponse(response.getTaskFailures().isEmpty() && activeAsyncSearchStoreService.freeContext(asyncSearchContextId));
                            },
                            (e) -> {
                                activeAsyncSearchStoreService.freeContext(asyncSearchContextId);
                                groupedDeletionListener.onResponse(false);
                            }
                    ));
        } else {
            groupedDeletionListener.onResponse(false);
        }

        //deleted persisted context
        persistenceService.deleteResponse(id, groupedDeletionListener);

    }

    /**
     * Post processing on completion of an async search response
     *
     * @param searchResponse       searchResponse
     * @param asyncSearchContextId identifier of the async search context
     * @return AsyncSearchResponse
     */
    public AsyncSearchResponse postProcessSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        final ActiveAsyncSearchContext asyncSearchContext = activeAsyncSearchStoreService.getContext(asyncSearchContextId);
        final AsyncSearchResponse asyncSearchResponse;
        asyncSearchContext.processSearchSuccess(searchResponse);
        asyncSearchResponse = asyncSearchContext.getAsyncSearchResponse();
        if (asyncSearchContext.needsPersistence()) {
            asyncSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
                        AsyncSearchPersistenceModel persistenceModel = new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                                asyncSearchResponse.getExpirationTimeMillis(), asyncSearchResponse.getSearchResponse());
                        persistenceService.storeResponse(AsyncSearchId.buildAsyncId(asyncSearchContext.getAsyncSearchId()), persistenceModel, ActionListener.wrap(
                                (indexResponse) -> {
                                    //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                    asyncSearchContext.setStage(AsyncSearchContext.Stage.PERSISTED);
                                    // Clean this up so that new context find results in a resolution from persistent store
                                    activeAsyncSearchStoreService.freeContext(asyncSearchContextId);
                                    releasable.close();
                                },

                                (e) -> {
                                    asyncSearchContext.setStage(AsyncSearchContext.Stage.PERSIST_FAILED);
                                    logger.error("Failed to persist final response for {}", asyncSearchContext.getAsyncSearchId(), e);
                                    releasable.close();
                                }
                        ));

                    }, (e) -> logger.error("Exception while acquiring the permit due to ", e)),
                    TimeValue.timeValueSeconds(60), threadPool, "persisting response");
        }
        return asyncSearchResponse;
    }

    /**
     * Post processing search failure
     *
     * @param exception            the exception
     * @param asyncSearchContextId the identifier of the search context
     */
    public void postProcessSearchFailure(Exception exception, AsyncSearchContextId asyncSearchContextId) {
        ActiveAsyncSearchContext asyncSearchContext = activeAsyncSearchStoreService.getContext(asyncSearchContextId);
        asyncSearchContext.processSearchFailure(exception);
        if (asyncSearchContext.needsPersistence()) {
            asyncSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
                        AsyncSearchPersistenceModel persistenceModel = new AsyncSearchPersistenceModel(asyncSearchContext.getStartTimeMillis(),
                                asyncSearchContext.getExpirationTimeMillis(), exception);
                        persistenceService.storeResponse(AsyncSearchId.buildAsyncId(asyncSearchContext.getAsyncSearchId()), persistenceModel, ActionListener.wrap(
                                (indexResponse) -> {
                                    //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                    asyncSearchContext.setStage(AsyncSearchContext.Stage.PERSISTED);
                                    // Clean this up so that new context find results in a resolution from persistent store
                                    activeAsyncSearchStoreService.freeContext(asyncSearchContextId);
                                    releasable.close();
                                },

                                (e) -> {
                                    asyncSearchContext.setStage(AsyncSearchContext.Stage.PERSIST_FAILED);
                                    logger.error("Failed to persist final response for {}", asyncSearchContext.getAsyncSearchId(), e);
                                    releasable.close();
                                }
                        ));

                    }, (e) -> logger.error("Exception while acquiring the permit due to ", e)),
                    TimeValue.timeValueSeconds(60), threadPool, "persisting response");
        }
    }


    public void updateKeepAliveAndGetContext(GetAsyncSearchRequest request, AsyncSearchContextId asyncSearchContextId,
                                             ActionListener<AsyncSearchContext> listener) {
        long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
        // find an active context on this node if one exists
        ActiveAsyncSearchContext activeAsyncSearchContext = activeAsyncSearchStoreService.getContext(asyncSearchContextId,
                (stage) -> stage == RUNNING || stage == SUCCEEDED || stage == PERSIST_FAILED);
        // for all other stages we don't really care much as those contexts are destined to be discarded
        if (activeAsyncSearchContext != null) {
            activeAsyncSearchContext.acquireContextPermit(ActionListener.wrap(
                    releasable -> {
                        // At this point it's possible that the response would have been persisted to system index
                        if (activeAsyncSearchContext.getContextStage() == PERSISTED) {
                            persistenceService.updateExpirationTimeAndGet(request.getId(), requestedExpirationTime, ActionListener.wrap((actionResponse) ->
                                            listener.onResponse(new AsyncSearchPersistenceContext(activeAsyncSearchContext.getAsyncSearchId(), actionResponse)),
                                    listener::onFailure));
                        } else {
                            activeAsyncSearchContext.setExpirationMillis(requestedExpirationTime);
                            listener.onResponse(activeAsyncSearchContext);
                        }
                        releasable.close();
                    },
                    listener::onFailure), TimeValue.timeValueSeconds(5), threadPool, "persisting response");
        } else {
            // try update the doc on the index assuming there exists one.
            persistenceService.updateExpirationTimeAndGet(request.getId(), requestedExpirationTime,
                    ActionListener.wrap((actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(
                                    AsyncSearchId.parseAsyncId(request.getId()), actionResponse)),
                            listener::onFailure));
        }
    }


    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO listen to coordinator state failures and async search shards getting assigned
    }

    /***
     * Listens to the cancellation for {@link com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask} and
     * updates the corresponding state on disk.
     * @param contextId the AsyncSearchContextId
     */
    public void onCancelled(AsyncSearchContextId contextId) {
        ActiveAsyncSearchContext activeAsyncSearchContext = activeAsyncSearchStoreService.getContext(contextId);
        activeAsyncSearchContext.setStage(ABORTED);
        // TODO should we let the partial response stay if the cancellation was done due to tasks overrunning
        activeAsyncSearchStoreService.freeContext(activeAsyncSearchContext.getAsyncSearchContextId());
    }

    public AsyncSearchStats stats(boolean count) {
        return statsListener.stats(count, clusterService.localNode());
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        activeAsyncSearchStoreService.freeAllContexts();
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
                for (ActiveAsyncSearchContext activeAsyncSearchContext : activeAsyncSearchStoreService.getAllContexts().values()) {
                    ActiveAsyncSearchContext.Stage stage = activeAsyncSearchContext.getContextStage();
                    if (stage != null && Objects.equals(stage, ABORTED) || Objects.equals(stage, FAILED)
                            || Objects.equals(stage, PERSISTED) || Objects.equals(stage, PERSIST_FAILED)) {
                        activeAsyncSearchStoreService.freeContext(activeAsyncSearchContext.getAsyncSearchContextId());
                    }
                }
            } catch (Exception e) {
                logger.error("Exception while reaping contexts", e);
            }
        }
    }
}
