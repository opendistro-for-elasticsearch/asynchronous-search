package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;

public class DeleteAsyncSearchActionHandler extends AbstractAsyncSearchAction<DeleteAsyncSearchRequest, AsyncSearchResponse> {

    private Client client;
    private Logger logger;

    public DeleteAsyncSearchActionHandler(TransportService transportService, AsyncSearchService asyncSearchService, Client client, Logger logger) {
        super(transportService, asyncSearchService);
        this.client = client;
        this.logger = logger;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, DeleteAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        TaskId taskId = null;
        final CancelTasksRequest searchTaskToCancel = new CancelTasksRequest();
        searchTaskToCancel.setReason("Search request cancellation timeout interval is exceeded. This may be because of expensive search query or overloaded cluster.");
        searchTaskToCancel.setTaskId(taskId);
        // Send the cancel task request for this search request. It is best effort cancellation as we don't wait
        // for cancellation to finish and ignore any error or response.
        client.admin().cluster().cancelTasks(searchTaskToCancel, ActionListener.wrap(
                r -> {
                    logger.debug("Scheduled cancel task for search request on expiry of cancel_after_timeinterval: " +
                            "[taskId: {}] is successfully completed", searchTaskToCancel.getTaskId());
                },
                e -> {
                    logger.error(new ParameterizedMessage("Scheduled cancel task for search request on expiry of cancel_after_timeinterval: " +
                            "[taskId: {}] is failed", searchTaskToCancel.getTaskId()), e);
                }));
    }
}
