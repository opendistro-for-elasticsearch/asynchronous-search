package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.active.ActiveAsyncSearchContext;
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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

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
            asyncSearchService.findContext(asyncSearchId, ActionListener.wrap(
                (asyncSearchContext) -> {
                    AsyncSearchContext.Source source = asyncSearchContext.getSource();
                    boolean updateNeeded = request.getKeepAlive() != null;
                    switch (source) {
                        case IN_MEMORY:
                            ActiveAsyncSearchContext.Stage stage = asyncSearchContext.getSearchStage();
                            // Attach a listener if the search is already in progress
                            if (stage == ActiveAsyncSearchContext.Stage.RUNNING) {
                                AsyncSearchProgressListener progressActionListener = (AsyncSearchProgressListener)asyncSearchContext.getSearchProgressActionListener();
                                if (updateNeeded) {
                                    ActionListener<AsyncSearchResponse> groupedListener = new GroupedActionListener<>(
                                            ActionListener.wrap(
                                                    (responses) ->
                                                    {
                                                        //The grouped listener atomically updates the pos, the one arriving last be also be last in the
                                                        // response array
                                                        assert responses.stream().count() == 2;
                                                        listener.onResponse(responses.stream()
                                                                .filter(Objects::nonNull)
                                                                .skip(1)
                                                                .findFirst().get());
                                                    },
                                                    listener::onFailure), 2);
                                    PrioritizedActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                                            request.getWaitForCompletionTimeout(), AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, groupedListener,
                                            (actionListener) -> {
                                                progressActionListener.removeListener(actionListener);
                                                groupedListener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                                            });
                                    progressActionListener.addOrExecuteListener(wrappedListener);
                                    asyncSearchService.updateKeepAlive(request, asyncSearchContext, groupedListener);
                                } else {
                                    PrioritizedActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                                            request.getWaitForCompletionTimeout(), AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, listener,
                                            (actionListener) -> {
                                                progressActionListener.removeListener(actionListener);
                                                listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                                            });
                                    progressActionListener.addOrExecuteListener(wrappedListener);
                                }
                            } else {
                                // async search is no more RUNNING so we don't need to wait on listeners for completion
                                if (updateNeeded) {
                                    //TODO failure to update keep alive should still return the async search response
                                    asyncSearchService.updateKeepAlive(request, asyncSearchContext, listener);
                                } else {
                                    listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                                }
                            }
                            break;
                        case STORE:
                            if (updateNeeded) {
                                asyncSearchService.updateKeepAlive(request, asyncSearchContext, listener);
                            } else {
                                listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                            }
                            break;
                    }
                },
                listener::onFailure)
            );
        } catch (Exception e) {
            logger.error("Failed to get async search request {}", request);
            listener.onFailure(e);
        }
    }
}
