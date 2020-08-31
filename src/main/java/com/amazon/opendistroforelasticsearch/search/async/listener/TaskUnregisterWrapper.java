package com.amazon.opendistroforelasticsearch.search.async.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.List;

public class TaskUnregisterWrapper {
    private static final Logger logger = LogManager.getLogger(TaskUnregisterWrapper.class);
    private final CancellableTask task;
    private final List<Releasable> releasables;
    private final String nodeId;

    public TaskUnregisterWrapper(CancellableTask task, String nodeId, List<Releasable> releasables) {
        this.task = task;
        this.releasables = releasables;
        this.nodeId = nodeId;
    }

    public boolean isCancelled() {
        return task.isCancelled();
    }

    public void cancel(Client client) {
        if(isCancelled()) {
            releasables.forEach(releasable -> {
                try{
                    releasable.close();
                } catch (Exception e) {

                }
            });
            return;
        }
        logger.info("Cancelling task [{}] on node : [{}]", task.getId(), nodeId);
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        TaskId taskId = getTaskId();
        cancelTasksRequest.setTaskId(taskId);
        cancelTasksRequest.setReason("Async search request expired");
        client.admin().cluster().cancelTasks(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
            @Override
            public void onResponse(CancelTasksResponse cancelTasksResponse) {
                logger.info(cancelTasksResponse.toString());
                releasables.forEach(Releasable::close);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to cancel task [{}]", taskId);
            }
        });

    }

    public TaskId getTaskId() {
        return task.taskInfo(nodeId, false).getTaskId();
    }

    public long getStartTime() {
        return this.task.getStartTime();
    }


}
