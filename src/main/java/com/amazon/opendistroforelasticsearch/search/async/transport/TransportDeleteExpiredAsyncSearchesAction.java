package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteExpiredAsyncSearchesAction;
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

public class TransportDeleteExpiredAsyncSearchesAction
        extends HandledTransportAction<DeleteExpiredAsyncSearchesRequest, AcknowledgedResponse> {

    private static final Logger log = LogManager.getLogger(TransportDeleteExpiredAsyncSearchesAction.class);
    private final AsyncSearchPersistenceService persistenceService;

    @Inject
    public TransportDeleteExpiredAsyncSearchesAction(TransportService transportService,
                                                     ActionFilters actionFilters, AsyncSearchPersistenceService persistenceService) {
        super(DeleteExpiredAsyncSearchesAction.NAME, transportService, actionFilters, DeleteExpiredAsyncSearchesRequest::new);
        this.persistenceService = persistenceService;
    }

    @Override
    protected void doExecute(Task task, DeleteExpiredAsyncSearchesRequest request, ActionListener<AcknowledgedResponse> listener) {
        log.debug(request);
        persistenceService.deleteExpiredResponses(listener);
    }
}
