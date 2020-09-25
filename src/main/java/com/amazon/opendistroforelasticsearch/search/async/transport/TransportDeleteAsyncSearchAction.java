package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteAsyncSearchAction extends TransportAsyncSearchFetchAction<DeleteAsyncSearchRequest, AcknowledgedResponse> {

    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportSearchAction transportSearchAction;
    private final AsyncSearchService asyncSearchService;
    private final Client client;

    @Inject
    public TransportDeleteAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                            AsyncSearchService asyncSearchService, TransportSearchAction transportSearchAction,
                                            Client client) {
        super(transportService, clusterService, asyncSearchService, DeleteAsyncSearchAction.NAME, actionFilters,
                DeleteAsyncSearchRequest::new, AcknowledgedResponse::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
        this.client = client;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, DeleteAsyncSearchRequest request,
                              ActionListener<AcknowledgedResponse> listener) {
        try {
            AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(request.getId(),
                    asyncSearchId.getAsyncSearchContextId());
            asyncSearchService.freeContext(asyncSearchId.getAsyncSearchContextId());
            if (asyncSearchContext.isCancelled()) {
                listener.onFailure(new ResourceNotFoundException(request.getId()));
            }
            listener.onResponse(new AcknowledgedResponse(true));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }


}
