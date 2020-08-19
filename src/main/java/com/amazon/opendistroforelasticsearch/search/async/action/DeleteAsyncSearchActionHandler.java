package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;

public class DeleteAsyncSearchActionHandler extends AbstractAsyncSearchAction<DeleteAsyncSearchRequest, AcknowledgedResponse> {

    private final Client client;
    private final Logger logger;
    private final AsyncSearchService asyncSearchService;

    public DeleteAsyncSearchActionHandler(TransportService transportService, AsyncSearchService asyncSearchService, Client client, Logger logger) {
        super(transportService, asyncSearchService);
        this.client = client;
        this.logger = logger;
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, DeleteAsyncSearchRequest request, ActionListener<AcknowledgedResponse> listener) {
        AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(asyncSearchId.getAsyncSearchContextId());
        if(asyncSearchContext.isCancelled()) {
            asyncSearchService.freeContext(asyncSearchId.getAsyncSearchContextId());
            listener.onFailure(new ResourceNotFoundException(request.getId()));
        }
        asyncSearchContext.cancelTask();
    }
}
