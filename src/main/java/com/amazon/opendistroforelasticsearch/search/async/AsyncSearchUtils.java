package com.amazon.opendistroforelasticsearch.search.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskId;

public class AsyncSearchUtils {

    private static final Logger logger = LogManager.getLogger(AsyncSearchUtils.class);

    public static void cancelTask(TaskId taskId, Client client) {

        logger.info("Cancelling async search task [{}]", taskId);
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(taskId);
        cancelTasksRequest.setReason("Async search request expired");
        client.admin().cluster().cancelTasks(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
            @Override
            public void onResponse(CancelTasksResponse cancelTasksResponse) {
                logger.info(cancelTasksResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to cancel async search task " + taskId + " not cancelled upon expiry", e);
            }
        });
    }
}
