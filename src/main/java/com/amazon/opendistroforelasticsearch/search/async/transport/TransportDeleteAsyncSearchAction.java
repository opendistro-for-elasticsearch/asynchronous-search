package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteAsyncSearchAction extends TransportAsyncSearchFetchAction<DeleteAsyncSearchRequest, AcknowledgedResponse> {

    private final AsyncSearchService asyncSearchService;
    private final Client client;

    @Inject
    public TransportDeleteAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, AsyncSearchService asyncSearchService, Client client) {
        super(transportService, clusterService, threadPool, client, DeleteAsyncSearchAction.NAME, actionFilters,
                DeleteAsyncSearchRequest::new, AcknowledgedResponse::new);
        this.asyncSearchService = asyncSearchService;
        this.client = client;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, DeleteAsyncSearchRequest request,
                              ActionListener<AcknowledgedResponse> listener) {
        try {
            asyncSearchService.freeContext(request.getId(), asyncSearchId.getAsyncSearchContextId(), ActionListener
                    .wrap((Boolean complete) -> listener.onResponse(new AcknowledgedResponse(complete)), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
