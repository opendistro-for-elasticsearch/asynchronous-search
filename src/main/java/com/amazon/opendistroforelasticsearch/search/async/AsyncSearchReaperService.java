package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchReaperPersistentTaskExecutor.AsyncSearchReaperParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

public class AsyncSearchReaperService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;
    private PersistentTasksService persistentTasksService;

    private static final Logger logger = LogManager.getLogger(AsyncSearchReaperService.class);

    @Inject
    public AsyncSearchReaperService(ClusterService clusterService, Client client, ThreadPool threadPool, PersistentTasksService persistentTasksService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.persistentTasksService = persistentTasksService;
        this.clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public void onMaster() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(new RunnableReaper(), TimeValue.timeValueSeconds(30),
                ThreadPool.Names.GENERIC);
    }

    @Override
    public void offMaster() {
        scheduledFuture.cancel();
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    class RunnableReaper implements Runnable {
        @Override
        public void run() {
            try {
                persistentTasksService.sendStartRequest(UUIDs.base64UUID(), AsyncSearchReaperPersistentTaskExecutor.NAME,
                        new AsyncSearchReaperParams(),
                        new ActionListener<PersistentTask<AsyncSearchReaperParams>>() {
                            @Override
                            public void onResponse(PersistentTask<AsyncSearchReaperParams> persistentTask) {
                                logger.warn("On send async search reaper request.");
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("on send start request failure", e);
                            }
                        });

            } catch (Exception e) {
                logger.warn("on send start request failure", e);
            }
        }
    }
}
