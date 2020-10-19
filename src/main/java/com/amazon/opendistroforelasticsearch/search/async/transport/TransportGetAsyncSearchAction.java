package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.listener.PrioritizedActionListener;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Responsible for returning partial response from {@link AsyncSearchService}. The listener needs to wait for completion if
 * the search is still RUNNING and also try to update the keep-alive as needed within the same wait period. Response is dispatched
 * whenever both the operations complete. If the search is however not RUNNING we simply need to update keep alive either in-memory
 * or disk and invoke the response with the search response
 */
public class TransportGetAsyncSearchAction extends TransportAsyncSearchFetchAction<GetAsyncSearchRequest, AsyncSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetAsyncSearchAction.class);
    private final ThreadPool threadPool;
    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportGetAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                         ActionFilters actionFilters, AsyncSearchService asyncSearchService, Client client) {
        super(transportService, clusterService, threadPool, client, GetAsyncSearchAction.NAME, actionFilters, GetAsyncSearchRequest::new,
                AsyncSearchResponse::new);
        this.threadPool = threadPool;
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        try {
            boolean updateNeeded = request.getKeepAlive() != null;
            if (updateNeeded) {
                asyncSearchService.updateKeepAliveAndGetContext(request, asyncSearchId.getAsyncSearchContextId(), ActionListener.wrap(
                        // check if the context is active and is still RUNNING
                        (context) -> handleWaitForCompletion(context, request.getWaitForCompletionTimeout(), listener),
                        (e) -> {
                            logger.debug(() -> new ParameterizedMessage("Unable to update and get async search request {}", asyncSearchId, e));
                            listener.onFailure(e);
                        }
                ));
            } else {
                // we don't need to update keep-alive, simply find one one the node if one exists or look up the index
                asyncSearchService.findContext(asyncSearchId, ActionListener.wrap(
                        (context) -> handleWaitForCompletion(context, request.getWaitForCompletionTimeout(), listener),
                        (e) -> {
                            logger.debug(() -> new ParameterizedMessage("Unable to update and get async search request {}", asyncSearchId, e));
                            listener.onFailure(e);
                        }));
                }
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Unable to update and get async search request {}", request, e));
            listener.onFailure(e);
        }
    }

    private void handleWaitForCompletion(AsyncSearchContext context, TimeValue timeValue, ActionListener<AsyncSearchResponse> listener) {
        if (context.isRunning()) {
            SearchProgressActionListener progressActionListener = context.getSearchProgressActionListener();
            assert progressActionListener != null : "progress listener cannot be null";
            PrioritizedActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    timeValue, AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, listener,
                    (actionListener) -> {
                        ((AsyncSearchProgressListener)progressActionListener).removeListener(actionListener);
                        listener.onResponse(context.getAsyncSearchResponse());
                    });
            ((AsyncSearchProgressListener)progressActionListener).addOrExecuteListener(wrappedListener);
        } else {
            // we don't need to wait any further on search progress
            listener.onResponse(context.getAsyncSearchResponse());
        }
    }
}
