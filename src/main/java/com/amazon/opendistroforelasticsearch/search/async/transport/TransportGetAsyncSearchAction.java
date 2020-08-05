package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.*;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
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
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<GetAsyncSearchRequest>) GetAsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        // Get context Id from request
        try {
            AsyncSearchContextId asyncSearchContextId = null;
            AsyncSearchContext asyncSearchContext = asyncSearchService.getContext(asyncSearchContextId);
            ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    TimeValue.ZERO, ThreadPool.Names.GENERIC, listener, () -> {
                        //Replace with actual async search response
                        listener.onResponse(null);
                    }, asyncSearchContext::removeListener);
            asyncSearchContext.addListener(wrappedListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
