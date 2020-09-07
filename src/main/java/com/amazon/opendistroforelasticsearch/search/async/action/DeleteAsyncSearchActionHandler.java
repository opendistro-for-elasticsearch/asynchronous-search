package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.DeleteAsyncSearchRequest;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class DeleteAsyncSearchActionHandler extends AbstractAsyncSearchAction<DeleteAsyncSearchRequest, AcknowledgedResponse> {

    private final Client client;
    private final Logger logger;
    private final AsyncSearchService asyncSearchService;

    public DeleteAsyncSearchActionHandler(TransportService transportService, AsyncSearchService asyncSearchService,
                                          Client client, Logger logger) {
        super(transportService, asyncSearchService);
        this.client = client;
        this.logger = logger;
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, DeleteAsyncSearchRequest request,
                              ActionListener<AcknowledgedResponse> listener) {
        AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(asyncSearchId.getAsyncSearchContextId());
        try {
            asyncSearchService.freeContext(asyncSearchId.getAsyncSearchContextId());
        } catch (IOException e) {
            logger.error("Failed to free context {}", asyncSearchId.getAsyncSearchContextId());
        }
        if (asyncSearchContext.isCancelled()) {
            listener.onFailure(new ResourceNotFoundException(request.getId()));
        }

    }
}
