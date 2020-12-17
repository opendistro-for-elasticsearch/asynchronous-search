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

import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The service takes care of cancelling ongoing searches which have been running past their expiration time and cleaning up async search
 * responses from disk by scheduling delete-by-query on master to be delegated to the least loaded node
 */
public class AsyncSearchManagementService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(AsyncSearchManagementService.class);

    private final ClusterService clusterService;
    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final Client client;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable taskReaperScheduledFuture;
    private AtomicReference<ResponseCleanUpScheduler> activeResponseCleanUpScheduler = new AtomicReference<>();
    private AsyncSearchService asyncSearchService;
    private TransportService transportService;
    private TimeValue taskCancellationInterval;
    private TimeValue responseCleanUpInterval;

    public static final String CLEANUP_ACTION_NAME = "indices:data/read/async_search/cleanup";

    public static final Setting<TimeValue> TASK_CANCELLATION_INTERVAL_SETTING =
            Setting.timeSetting("async_search.expired.task.cancellation_interval", TimeValue.timeValueMinutes(30),
                    TimeValue.timeValueMinutes(1),
                    Setting.Property.NodeScope);
    public static final Setting<TimeValue> RESPONSE_CLEAN_UP_INTERVAL_SETTING =
            Setting.timeSetting("async_search.expired.response.cleanup_interval", TimeValue.timeValueMinutes(1),
                    TimeValue.timeValueMinutes(1),
                    Setting.Property.NodeScope);

    @Inject
    public AsyncSearchManagementService(Settings settings, ClusterService clusterService, Client client, ThreadPool threadPool,
                                        AsyncSearchService asyncSearchService, TransportService transportService,
                                        AsyncSearchPersistenceService asyncSearchPersistenceService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeMasterListener(this);
        this.asyncSearchService = asyncSearchService;
        this.transportService = transportService;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
        this.taskCancellationInterval = TASK_CANCELLATION_INTERVAL_SETTING.get(settings);
        this.responseCleanUpInterval = RESPONSE_CLEAN_UP_INTERVAL_SETTING.get(settings);

        transportService.registerRequestHandler(CLEANUP_ACTION_NAME, ThreadPool.Names.SAME, false, false,
                AsyncSearchCleanUpRequest::new, new ResponseCleanUpTransportHandler());
    }

    class ResponseCleanUpTransportHandler implements TransportRequestHandler<AsyncSearchCleanUpRequest> {

        @Override
        public void messageReceived(AsyncSearchCleanUpRequest request, TransportChannel channel, Task task) throws Exception {
            asyncCleanUpOperation(request, task,
                    ActionListener.wrap(channel::sendResponse, e -> {
                                try {
                                    channel.sendResponse(e);
                                } catch (Exception e1) {
                                    logger.warn(() -> new ParameterizedMessage(
                                            "Failed to send cleanup error response for request [{}]", request), e1);
                                }
                            }
                    ));
        }
    }

    private void asyncCleanUpOperation(AsyncSearchCleanUpRequest request, Task task, ActionListener<AcknowledgedResponse> listener) {
        transportService.getThreadPool().executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME)
                .execute(() -> performCleanUpAction(request, listener));
    }

    private void performCleanUpAction(AsyncSearchCleanUpRequest request, ActionListener<AcknowledgedResponse> listener) {
        asyncSearchPersistenceService.deleteExpiredResponses(listener, request.absoluteTimeInMillis);
    }

    @Override
    public void onMaster() {
        ResponseCleanUpScheduler cleanupScheduler = new ResponseCleanUpScheduler();
        ResponseCleanUpScheduler previousScheduler = activeResponseCleanUpScheduler.getAndSet(cleanupScheduler);
        if (previousScheduler != null) {
            previousScheduler.close();
        }
        if (cleanupScheduler != null) {
            cleanupScheduler.handleWakeUp();
        }
    }

    @Override
    public void offMaster() {
        ResponseCleanUpScheduler cleanUpScheduler = activeResponseCleanUpScheduler.get();
        if (cleanUpScheduler != null) {
            cleanUpScheduler.close();
        }
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
        ResponseCleanUpScheduler cleanUpScheduler = activeResponseCleanUpScheduler.get();
        if (cleanUpScheduler != null) {
            cleanUpScheduler.close();
        }
        taskReaperScheduledFuture.cancel();
    }

    @Override
    protected void doClose() {
        ResponseCleanUpScheduler cleanUpScheduler = activeResponseCleanUpScheduler.get();
        if (cleanUpScheduler != null) {
            cleanUpScheduler.close();
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

    class ResponseCleanUpScheduler implements Releasable {

        private final AtomicBoolean isClosed = new AtomicBoolean();

        void handleWakeUp() {
            if (isClosed.get()) {
                logger.trace("closed check scheduler woken up, doing nothing");
                return;
            }

            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                // we have to execute under the system context so that if security is enabled the sync is authorized
                threadContext.markAsSystemContext();
                // TODO ensure versioning for BWC
                DiscoveryNode[] nodes = clusterService.state().nodes().getDataNodes().values().toArray(DiscoveryNode.class);
                int pos = Randomness.get().nextInt(nodes.length);
                DiscoveryNode randomNode = nodes[pos];
                transportService.sendRequest(randomNode, CLEANUP_ACTION_NAME,
                        new AsyncSearchCleanUpRequest(threadPool.absoluteTimeInMillis()),
                        new TransportResponseHandler<AcknowledgedResponse>() {

                            @Override
                            public AcknowledgedResponse read(StreamInput in) throws IOException {
                                return new AcknowledgedResponse(in);
                            }

                            @Override
                            public void handleResponse(AcknowledgedResponse response) {
                                if (isClosed.get()) {
                                    logger.debug("closed check scheduler received a response, doing nothing");
                                    return;
                                }
                                logger.debug("Successfully executed clean up action on node {} with response {}", randomNode,
                                        response.isAcknowledged());
                                scheduleNextWakeUp(); // logs trace message indicating success
                            }

                            @Override
                            public void handleException(TransportException e) {
                                logger.error(() -> new ParameterizedMessage("Exception executing action {}",
                                        CLEANUP_ACTION_NAME), e);
                                scheduleNextWakeUp();
                            }

                            @Override
                            public String executor() {
                                return AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
                            }
                        });

            } catch (Exception ex) {
                logger.error("Failed to schedule async search cleanup", ex);
                scheduleNextWakeUp();
            }
        }

        private void scheduleNextWakeUp() {
            logger.trace("scheduling next check for [{}] = {}", RESPONSE_CLEAN_UP_INTERVAL_SETTING.getKey(), responseCleanUpInterval);
            transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return "scheduled check of clean up action ";
                }
            }, responseCleanUpInterval, ThreadPool.Names.SAME);
        }

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true) == false) {
                logger.trace("already closed, doing nothing");
            } else {
                logger.debug("closed");
            }
        }
    }

    static class AsyncSearchCleanUpRequest extends ActionRequest {

        private final long absoluteTimeInMillis;

        AsyncSearchCleanUpRequest(long absoluteTimeInMillis) {
            this.absoluteTimeInMillis = absoluteTimeInMillis;
        }

        AsyncSearchCleanUpRequest(StreamInput in) throws IOException {
            super(in);
            this.absoluteTimeInMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(absoluteTimeInMillis);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        /**
         * The reason for deleting expired async searches.
         */
        public long getAbsoluteTimeInMillis() {
            return absoluteTimeInMillis;
        }


        @Override
        public int hashCode() {
            return Objects.hash(absoluteTimeInMillis);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AsyncSearchCleanUpRequest asyncSearchCleanUpRequest = (AsyncSearchCleanUpRequest) o;
            return absoluteTimeInMillis == asyncSearchCleanUpRequest.absoluteTimeInMillis;
        }

        @Override
        public String toString() {
            return "[expirationTimeMillis] : " + absoluteTimeInMillis;
        }
    }
}