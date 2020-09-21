package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.listener.CompositeSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;


public class TransportGetAsyncSearchAction extends TransportAsyncSearchFetchAction<GetAsyncSearchRequest, AsyncSearchResponse> {

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
        super(transportService, clusterService, asyncSearchService, GetAsyncSearchAction.NAME, actionFilters, GetAsyncSearchRequest::new, AsyncSearchResponse::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        try {
            AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(asyncSearchId.getAsyncSearchContextId());
            if (asyncSearchContext.isCancelled() || asyncSearchContext.isExpired()) {
                try {
                    asyncSearchService.freeContext(asyncSearchId.getAsyncSearchContextId());
                } catch (IOException e) {

                }
                throw new ResourceNotFoundException(request.getId());
            }
            try {
                updateExpiryTimeIfRequired(request, asyncSearchContext);
            } catch (IOException e) {
                listener.onFailure(e);
            }

            if (asyncSearchContext.isRunning()) {
                ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                        request.getWaitForCompletion(), ThreadPool.Names.GENERIC, listener, (actionListener) -> {
                            listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                            ((CompositeSearchProgressActionListener)
                                    asyncSearchContext.getSearchTask().getProgressListener()).removeListener(actionListener);
                        });
                //Here we want to be listen onto onFailure/onResponse ONLY or a timeout whichever happens earlier.
                //The original progress listener is responsible for updating the context. So whenever we search finishes or
                // times out we return the most upto state from the AsyncContext
                ((CompositeSearchProgressActionListener) asyncSearchContext.getSearchTask().getProgressListener()).addListener(wrappedListener);
            } else {
                listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    private void updateExpiryTimeIfRequired(GetAsyncSearchRequest request,
                                            AsyncSearchContext asyncSearchContext) throws IOException {
        if (request.getKeepAlive() != null) {
            long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
            if (requestedExpirationTime > asyncSearchContext.getExpirationTimeMillis()) {
                asyncSearchService.updateExpirationTime(requestedExpirationTime);
            }
        }
    }
}
