package com.amazon.opendistroforelasticsearch.search.async.reaper;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteExpiredAsyncSearchesAction;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteExpiredAsyncSearchesRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;

/**
 * The service takes care of cancelling ongoing searches, clean up async search responses from disk by scheduling delete-by-query on master
 * to be delegated to the least loaded node
 */
public class AsyncSearchManagementService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable masterScheduledFuture;
    private volatile Scheduler.Cancellable taskReaperScheduledFuture;
    private AsyncSearchService asyncSearchService;

    private static final Logger logger = LogManager.getLogger(AsyncSearchManagementService.class);

    @Inject
    public AsyncSearchManagementService(ClusterService clusterService, Client client, ThreadPool threadPool,
                                        AsyncSearchService asyncSearchService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeMasterListener(this);
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    public void onMaster() {
        masterScheduledFuture = threadPool.scheduleWithFixedDelay(new RunnableReaper(), TimeValue.timeValueSeconds(10),
                ThreadPool.Names.GENERIC);
    }

    @Override
    public void offMaster() {
        masterScheduledFuture.cancel();
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected void doStart() {
        taskReaperScheduledFuture = threadPool.scheduleWithFixedDelay(new TaskReaper(), TimeValue.timeValueMinutes(30),
                ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        masterScheduledFuture.cancel();
        taskReaperScheduledFuture.cancel();
    }

    @Override
    protected void doClose() {
        masterScheduledFuture.cancel();
        taskReaperScheduledFuture.cancel();
    }

    class TaskReaper implements Runnable {

        @Override
        public void run() {
            Set<SearchTask> toCancel = asyncSearchService.getOverRunningTasks();
            toCancel.forEach(
                    task -> client.admin().cluster()
                            .prepareCancelTasks().setTaskId(new TaskId(clusterService.localNode().getId(), task.getId()))
                            .execute());
        }
    }

    class RunnableReaper implements Runnable {
        @Override
        public void run() {
            client.execute(DeleteExpiredAsyncSearchesAction.INSTANCE, new DeleteExpiredAsyncSearchesRequest("master scheduled job"),
                    ActionListener.wrap(r -> {
                        logger.debug("successfully deleted");
                            },
                            e -> logger.error("Failed to delete expired async search responses from index", e)));
        }
    }
}
