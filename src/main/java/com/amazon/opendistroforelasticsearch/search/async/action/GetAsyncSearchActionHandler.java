package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class GetAsyncSearchActionHandler extends AbstractAsyncSearchAction<GetAsyncSearchRequest, AsyncSearchResponse> {

    private ClusterService clusterService;
    private TransportService transportService;
    private AsyncSearchService asyncSearchService;
    private ThreadPool threadPool;

    public GetAsyncSearchActionHandler(ClusterService clusterService, TransportService transportService,
                                       AsyncSearchService asyncSearchService, ThreadPool threadPool) {
        super(transportService, asyncSearchService);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.asyncSearchService = asyncSearchService;
        this.threadPool = threadPool;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {

        if (clusterService.state().nodes().nodeExists(asyncSearchId.getNode()) == false) {
            throw new ResourceNotFoundException(request.getId());
        }
        if (clusterService.localNode().getId().equals(asyncSearchId.getNode()) == false) {
            forwardRequest(clusterService.state().getNodes().get(asyncSearchId.getNode()), request, listener, this::read, GetAsyncSearchAction.NAME);
        }
        AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(asyncSearchId.getAsyncSearchContextId());
        if(asyncSearchContext.isCancelled() || asyncSearchContext.isExpired()) {
            asyncSearchService.freeContext(asyncSearchId.getAsyncSearchContextId());
            throw new ResourceNotFoundException(request.getId());
        }
        updateExpiryTimeIfRequired(request, asyncSearchContext);
        ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                request.getWaitForCompletion(), ThreadPool.Names.GENERIC, listener, (contextListener) -> {
                    listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                    asyncSearchContext.removeListener(contextListener);
                });
        //Here we want to be listen onto onFailure/onResponse ONLY or a timeout whichever happens earlier.
        //The original progress listener is responsible for updating the context. So whenever we search finishes or
        // times out we return the most upto state from the AsyncContext
        asyncSearchContext.addListener(wrappedListener);
    }

    private void updateExpiryTimeIfRequired(GetAsyncSearchRequest request, AsyncSearchContext asyncSearchContext) {
        if(request.getKeepAlive() != null) {
            long requestedExpirationTime = System.currentTimeMillis() + request.getKeepAlive().getMillis();
            if(requestedExpirationTime > asyncSearchContext.getExpirationTimeMillis()) {
                asyncSearchContext.setExpirationTimeMillis(requestedExpirationTime);
            }
        }
    }

    private AsyncSearchResponse read(StreamInput in) throws IOException {
        return new AsyncSearchResponse(in);
    }
}
