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

package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchCleanUpAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchCleanUpRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Random;
import java.util.Set;

/**
 * The service takes care of cancelling ongoing searches, clean up async search responses from disk by scheduling delete-by-query on master
 * to be delegated to the least loaded node
 */
public class AsyncSearchManagementService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(AsyncSearchManagementService.class);

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable masterScheduledFuture;
    private volatile Scheduler.Cancellable taskReaperScheduledFuture;
    private AsyncSearchService asyncSearchService;
    private TransportService transportService;
    private TimeValue taskCancellationInterval;
    private TimeValue responseCleanUpInterval;

    public static final Setting<TimeValue> TASK_CANCELLATION_INTERVAL_SETTING =
            Setting.timeSetting("async_search.expired.task.cancellation_interval", TimeValue.timeValueMinutes(30), TimeValue.timeValueMinutes(1),
                    Setting.Property.NodeScope);
    public static final Setting<TimeValue> RESPONSE_CLEAN_UP_INTERVAL_SETTING =
            Setting.timeSetting("async_search.expired.response.cleanup_interval", TimeValue.timeValueMinutes(30), TimeValue.timeValueMinutes(1),
                    Setting.Property.NodeScope);

    @Inject
    public AsyncSearchManagementService(Settings settings, ClusterService clusterService, Client client, ThreadPool threadPool,
                                        AsyncSearchService asyncSearchService, TransportService transportService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeMasterListener(this);
        this.asyncSearchService = asyncSearchService;
        this.transportService = transportService;
        this.taskCancellationInterval = TASK_CANCELLATION_INTERVAL_SETTING.get(settings);
        this.responseCleanUpInterval = RESPONSE_CLEAN_UP_INTERVAL_SETTING.get(settings);
    }

    @Override
    public void onMaster() {
        masterScheduledFuture = threadPool.scheduleWithFixedDelay(new RunnableReaper(), responseCleanUpInterval,
                AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
    }

    @Override
    public void offMaster() {
        masterScheduledFuture.cancel();
    }

    @Override
    public String executorName() {
        return AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
    }

    @Override
    protected void doStart() {
        taskReaperScheduledFuture = threadPool.scheduleWithFixedDelay(new TaskReaper(), taskCancellationInterval,
                AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
    }

    @Override
    protected void doStop() {
        if (masterScheduledFuture != null) {
            masterScheduledFuture.cancel();
        }
        taskReaperScheduledFuture.cancel();
    }

    @Override
    protected void doClose() {
        if (masterScheduledFuture != null) {
            masterScheduledFuture.cancel();
        }
        taskReaperScheduledFuture.cancel();
    }

    class TaskReaper implements Runnable {

        @Override
        public void run() {
            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                // we have to execute under the system context so that if security is enabled the sync is authorized
                threadContext.markAsSystemContext();
                Set<SearchTask> toCancel = asyncSearchService.getOverRunningTasks();
                // don't block on response
                toCancel.forEach(
                        task -> client.admin().cluster()
                                .prepareCancelTasks().setTaskId(new TaskId(clusterService.localNode().getId(), task.getId()))
                                .execute());
            } catch (Exception ex) {
                logger.error("Failed to cancel overrunning async search task", ex);
            }
        }
    }

    class RunnableReaper implements Runnable {

        Random random;

        RunnableReaper() {
            random = new Random();
        }

        @Override
        public void run() {
            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                // we have to execute under the system context so that if security is enabled the sync is authorized
                threadContext.markAsSystemContext();
                // TODO ensure versioning for BWC
                DiscoveryNode[] nodes = clusterService.state().nodes().getDataNodes().values().toArray(DiscoveryNode.class);
                int pos = random.nextInt(nodes.length);
                DiscoveryNode randomNode = nodes[pos];
                transportService.sendRequest(randomNode, AsyncSearchCleanUpAction.NAME, new AsyncSearchCleanUpRequest(threadPool.absoluteTimeInMillis()),
                        new ActionListenerResponseHandler<AcknowledgedResponse>(
                                ActionListener.wrap((response) -> logger.debug("Successfully executed", response.isAcknowledged()),
                                        (e) -> logger.error(() -> new ParameterizedMessage("Exception executing action {}",
                                                AsyncSearchCleanUpAction.NAME), e)), AcknowledgedResponse::new));
            } catch (Exception ex) {
                logger.error("Failed to schedule async search cleanup", ex);
            }
        }
    }
}
