package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

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
            AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(asyncSearchId.getAsyncSearchContextId());
            ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    request.getWaitForCompletion(), ThreadPool.Names.GENERIC, listener, (contextListener) -> {
                        //TODO Replace with actual async search response
                        listener.onResponse(null);
                        asyncSearchContext.removeListener(contextListener);
            });
            //Here we want to be listen onto onFailure/onResponse ONLY or a timeout whichever happens earlier.
            //The original progress listener is responsible for updating the context. So whenever we search finishes or
            // times out we return the most upto state from the AsyncContext
            asyncSearchContext.addListener(wrappedListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Forwards request to the appropriate coordinator running the async search
     */
    private void forwardRequest(DiscoveryNode discoveryNode, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        transportService.sendRequest(discoveryNode, GetAsyncSearchAction.NAME, request,
                new ActionListenerResponseHandler<AsyncSearchResponse>(listener, this::read) {
                    @Override
                    public void handleException(final TransportException exp) {
                        Throwable cause = exp.unwrapCause();
                        if (cause instanceof ConnectTransportException ||
                                (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                            // we want to retry here a bit to see if a new master is elected
                            logger.debug("connection exception while trying to forward request with action name [{}] to " +
                                            "master node [{}], scheduling a retry. Error: [{}]",
                                    actionName, discoveryNode, exp.getDetailedMessage());
                            //TODO add retries
                        } else {
                            listener.onFailure(exp);
                        }
                    }
                });
    }

    private AsyncSearchResponse read(StreamInput in) throws IOException {
        return new AsyncSearchResponse(in);
    }
}
