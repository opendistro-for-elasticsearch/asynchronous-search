package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchActionHandler;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetAsyncSearchAction extends HandledTransportAction<GetAsyncSearchRequest, AsyncSearchResponse> {

    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportSearchAction transportSearchAction;
    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportGetAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         AsyncSearchService asyncSearchService, TransportSearchAction transportSearchAction) {
        super(GetAsyncSearchAction.NAME, transportService, actionFilters, GetAsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        try {
            AsyncSearchId asyncSearchId = AsyncSearchId.parseAsyncId(request.getId());
            GetAsyncSearchActionHandler getAsyncSearchActionHandler = new GetAsyncSearchActionHandler(clusterService, transportService,
                    asyncSearchService, threadPool);
            getAsyncSearchActionHandler.handleRequest(asyncSearchId, request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
