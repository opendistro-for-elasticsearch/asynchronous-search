package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchManagementAction;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteExpiredAsyncSearchesRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportAsyncSearchManagementAction extends HandledTransportAction<DeleteExpiredAsyncSearchesRequest, AcknowledgedResponse> {

    private static final Logger log = LogManager.getLogger(TransportAsyncSearchManagementAction.class);
    private final AsyncSearchPersistenceService persistenceService;

    @Inject
    public TransportAsyncSearchManagementAction(TransportService transportService,
                                                ActionFilters actionFilters, AsyncSearchPersistenceService persistenceService) {
        super(AsyncSearchManagementAction.NAME, transportService, actionFilters, DeleteExpiredAsyncSearchesRequest::new);
        this.persistenceService = persistenceService;
    }

    @Override
    protected void doExecute(Task task, DeleteExpiredAsyncSearchesRequest request, ActionListener<AcknowledgedResponse> listener) {
        persistenceService.deleteExpiredResponses(listener);
    }
}
