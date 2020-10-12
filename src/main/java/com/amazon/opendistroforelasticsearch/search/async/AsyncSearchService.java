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

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchCountStats;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.async.ActiveAsyncSearchContext.Stage.ABORTED;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;

/***
 * Manages the lifetime of {@link AbstractAsyncSearchContext} for all the async searches running on the coordinator node.
 */

public class AsyncSearchService extends AsyncSearchLifecycleService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchService.class);

    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.default_keep_alive", timeValueHours(2), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
            Setting.positiveTimeSetting("async_search.max_keep_alive", timeValueDays(10), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);

    private volatile long maxKeepAlive;

    private final AtomicLong idGenerator = new AtomicLong();

    private final Client client;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final AsyncSearchPersistenceService persistenceService;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final CounterMetric runningAsyncSearchCount = new CounterMetric();
    private final CounterMetric persistedAsyncSearchCount = new CounterMetric();
    private final CounterMetric abortedAsyncSearchCount = new CounterMetric();
    private final CounterMetric failedAsyncSearchCount = new CounterMetric();
    private final CounterMetric completedAsyncSearchCount = new CounterMetric();


    public AsyncSearchService(AsyncSearchPersistenceService asyncSearchPersistenceService,
                              Client client, ClusterService clusterService,
                              ThreadPool threadPool, NamedWriteableRegistry namedWriteableRegistry) {
        super(threadPool, clusterService);
        this.client = client;
        Settings settings = clusterService.getSettings();
        setKeepAlive(MAX_KEEPALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_KEEPALIVE_SETTING, this::setKeepAlive);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.persistenceService = asyncSearchPersistenceService;
        this.namedWriteableRegistry = namedWriteableRegistry;
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
    public AbstractAsyncSearchContext prepareContext(SubmitAsyncSearchRequest submitAsyncSearchRequest, long relativeStartMillis) {
        if (submitAsyncSearchRequest.getKeepAlive().getMillis() > maxKeepAlive) {
            throw new IllegalArgumentException(
                    "Keep alive for async searcg (" + TimeValue.timeValueMillis(submitAsyncSearchRequest.getKeepAlive().getMillis()) + ")" +
                            " is too large. " +
                            "It must be less than (" + TimeValue.timeValueMillis(maxKeepAlive) + "). " +
                            "This limit can be set by changing the [" + MAX_KEEPALIVE_SETTING.getKey() + "] cluster level setting.");
        }
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchProgressListener progressActionListener = new AsyncSearchProgressListener(relativeStartMillis,
                (response) -> onSearchResponse(response, asyncSearchContextId),
                (e) -> onSearchFailure(e, asyncSearchContextId), threadPool.executor(ThreadPool.Names.GENERIC),
                System::currentTimeMillis);
        ActiveAsyncSearchContext asyncSearchContext = new ActiveAsyncSearchContext(asyncSearchContextId, clusterService.localNode().getId(),
                submitAsyncSearchRequest.getKeepAlive(),
                submitAsyncSearchRequest.keepOnCompletion(), threadPool, progressActionListener);
        putContext(asyncSearchContextId, asyncSearchContext);
        runningAsyncSearchCount.inc();
        return asyncSearchContext;
    }

    public void onRunning(AsyncSearchContextId asyncSearchContextId) {
        getContext(asyncSearchContextId).setStage(ActiveAsyncSearchContext.Stage.RUNNING);
    }

    public void prepareSearch(SearchTask searchTask, AsyncSearchContextId asyncSearchContextId) {
        ActiveAsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        asyncSearchContext.initializeTask(searchTask);
        asyncSearchContext.setExpirationMillis(searchTask.getStartTime() + asyncSearchContext.getKeepAlive().getMillis());
    }

    /***
     * The method tries to find a context in-memory and if it's not found, it tries to look up data on the disk. It no data
     * on the disk exists, it's likely that the context has expired in which case {@link AsyncSearchContextMissingException}
     * is thrown
     * @param listener The listener to be invoked once the context is available
     */
    public void findContext(AsyncSearchId asyncSearchId, ActionListener<AbstractAsyncSearchContext> listener) {
        AsyncSearchContextId asyncSearchContextId = asyncSearchId.getAsyncSearchContextId();
        ActiveAsyncSearchContext activeAsyncSearchContext = getContext(asyncSearchContextId);
        if (activeAsyncSearchContext != null) {
            listener.onResponse(activeAsyncSearchContext);
        } else {
            persistenceService.getResponse(AsyncSearchId.buildAsyncId(asyncSearchId), ActionListener.wrap(
                    asyncSearchPersistenceContext -> listener.onResponse(asyncSearchPersistenceContext),
                    ex -> listener.onFailure(new AsyncSearchContextMissingException(asyncSearchContextId))
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
        Map<Long, ActiveAsyncSearchContext> allContexts = getAllContexts();
        return Collections.unmodifiableSet(allContexts.values().stream()
                .filter(Objects::nonNull)
                .filter(context -> threadPool.relativeTimeInMillis() < context.getExpirationTimeMillis())
                .filter(context -> context.getTask().isCancelled() == false)
                .map(context -> context.getTask())
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
        GroupedActionListener<Boolean> groupedDeletionListener = new GroupedActionListener<>(
            ActionListener.wrap((responses) -> {
                if (responses.stream().anyMatch(r -> r)) {
                    listener.onResponse(true);
                } else {
                    listener.onFailure(new ResourceNotFoundException(id));
                }
            }, listener::onFailure), 2);

        //delete active context
        //TODO do we need to wait on the task canncellation to succeed
        ActiveAsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        if (asyncSearchContext != null && asyncSearchContext.getTask().isCancelled() == false) {
            client.admin().cluster()
                    .prepareCancelTasks().setTaskId(new TaskId(clusterService.localNode().getId(), asyncSearchContext.getTask().getId()))
                    .execute(ActionListener.wrap(() -> {
                    }));
        }
        if (freeContext(asyncSearchContextId)) {
            groupedDeletionListener.onResponse(true);
        } else {
            groupedDeletionListener.onResponse(false);
        }
        //deleted persisted context
        persistenceService.deleteResponse(id, groupedDeletionListener);
    }

    public AsyncSearchResponse onSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        ActiveAsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        asyncSearchContext.processFinalResponse(searchResponse);
        AsyncSearchResponse asyncSearchResponse = asyncSearchContext.getAsyncSearchResponse();
        completedAsyncSearchCount.inc();
        if (asyncSearchContext.needsPersistence()) {
            asyncSearchContext.acquireAllContextPermit(ActionListener.wrap(releasable -> {
                AsyncSearchPersistenceContext model = new AsyncSearchPersistenceContext(namedWriteableRegistry, asyncSearchResponse);
                persistenceService.createResponse(model, ActionListener.wrap(
                    (indexResponse) -> {
                        asyncSearchContext.performPostPersistenceCleanup();
                        asyncSearchContext.setStage(ActiveAsyncSearchContext.Stage.PERSISTED);
                        persistedAsyncSearchCount.inc();
                        freeContext(asyncSearchContextId);
                        releasable.close();
                    },

                    (e) -> {
                        asyncSearchContext.setStage(ActiveAsyncSearchContext.Stage.PERSIST_FAILED);
                        logger.error("Failed to persist final response for {}", asyncSearchContext.getAsyncSearchId(), e);
                        releasable.close();
                    }
                ));

            }, (e) -> logger.error("Exception while acquiring the permit due to ", e)),
            TimeValue.timeValueSeconds(30), "persisting response");
        }
        return asyncSearchResponse;
    }

    public void onSearchFailure(Exception e, AsyncSearchContextId asyncSearchContextId) {
        ActiveAsyncSearchContext activeContext = getContext(asyncSearchContextId);
        activeContext.processFailure(e);
        failedAsyncSearchCount.inc();
        freeContext(asyncSearchContextId);
    }


    public void updateKeepAlive(GetAsyncSearchRequest request, AbstractAsyncSearchContext abstractAsyncSearchContext,
                                ActionListener<AsyncSearchResponse> listener) {
        AbstractAsyncSearchContext.Source source = abstractAsyncSearchContext.getSource();
        long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
        if (source.equals(AbstractAsyncSearchContext.Source.STORE)) {
            persistenceService.updateExpirationTime(request.getId(), requestedExpirationTime,
                    ActionListener.wrap((actionResponse) -> {
                        listener.onResponse(new AsyncSearchResponse(abstractAsyncSearchContext.getAsyncSearchResponse(),
                                requestedExpirationTime));
                    }, listener::onFailure));
        } else {
            ActiveAsyncSearchContext activeAsyncSearchContext = (ActiveAsyncSearchContext) abstractAsyncSearchContext;
            activeAsyncSearchContext.acquireContextPermit(ActionListener.wrap(
                    releasable -> {
                        activeAsyncSearchContext.setExpirationMillis(requestedExpirationTime);
                        listener.onResponse(activeAsyncSearchContext.getAsyncSearchResponse());
                        releasable.close();
                    },
                    listener::onFailure), TimeValue.timeValueSeconds(5), "persisting response");
        }
    }


    @Override
    public void clusterChanged(ClusterChangedEvent event) {

    }

    /***
     * Listens to the cancellation for {@link com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask} and
     * updates the corresponding state on disk.
     * @param contextId the AsyncSearchContextId
     */
    public void onCancelled(AsyncSearchContextId contextId) {
        getContext(contextId).setStage(ABORTED);
        abortedAsyncSearchCount.inc();
    }

    public AsyncSearchStats stats(boolean count) {
        return new AsyncSearchStats(clusterService.localNode(), count ? new AsyncSearchCountStats(
                runningAsyncSearchCount.count(), abortedAsyncSearchCount.count(),
                persistedAsyncSearchCount.count(), completedAsyncSearchCount.count(), failedAsyncSearchCount.count()) : null);
    }
}
