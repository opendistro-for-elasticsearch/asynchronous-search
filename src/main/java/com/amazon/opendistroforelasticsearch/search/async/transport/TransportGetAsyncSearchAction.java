package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.PrioritizedListener;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.RUNNING;

/**
 * Responsible for returning partial response from {@link AsyncSearchService}. The listener needs to wait for completion if
 * the search is still RUNNING and also try to update the keep-alive as needed within the same wait period. Response is dispatched
 * whenever both the operations complete. If the search is however not RUNNING we simply need to update keep alive either in-memory
 * or disk and invoke the response with the search response
 */
public class TransportGetAsyncSearchAction extends TransportAsyncSearchFetchAction<GetAsyncSearchRequest, AsyncSearchResponse> {

    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportSearchAction transportSearchAction;
    private final AsyncSearchService asyncSearchService;
    private static final Logger logger = LogManager.getLogger(TransportGetAsyncSearchAction.class);

    @Inject
    public TransportGetAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         AsyncSearchService asyncSearchService, TransportSearchAction transportSearchAction) {
        super(transportService, clusterService, asyncSearchService, GetAsyncSearchAction.NAME, actionFilters, GetAsyncSearchRequest::new,
                AsyncSearchResponse::new);
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
            asyncSearchService.findContext(asyncSearchId.getAsyncSearchContextId(), ActionListener.wrap(
                    (asyncSearchContext) -> {
                        AsyncSearchContext.Stage stage = asyncSearchContext.getStage();
                        boolean updateNeeded = request.getKeepAlive() != null;
                        assert stage.equals(INIT) == false : "Should be moved to " + RUNNING + "found" + stage;
                        if (stage == RUNNING) {
                            if (updateNeeded) {
                                ActionListener<AsyncSearchResponse> groupedListener = new GroupedActionListener<>(
                                        ActionListener.wrap(
                                                //TODO replace findFirst with the latest response received. Add TS to AsyncSearchResponse and compare
                                                (responses) -> {
                                                    listener.onResponse(responses.stream()
                                                            .filter(Objects::nonNull)
                                                            .findFirst().get());
                                                },
                                                listener::onFailure), 2);
                                PrioritizedListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                                        request.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, groupedListener,
                                        (actionListener) -> {
                                            ((AsyncSearchProgressActionListener) asyncSearchContext.getTask().getProgressListener()).removeListener(actionListener);
                                            groupedListener.onResponse(asyncSearchContext.geLatestSearchResponse());
                                        });
                                ((AsyncSearchProgressActionListener) asyncSearchContext.getTask().getProgressListener())
                                        .addOrExecuteListener(wrappedListener);
                                asyncSearchService.updateKeepAlive(request, asyncSearchContext, ActionListener.wrap(
                                        (response) -> listener.onResponse(asyncSearchContext.geLatestSearchResponse()),
                                        listener::onFailure));
                            } else {
                                PrioritizedListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                                        request.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener,
                                        (actionListener) -> {
                                            ((AsyncSearchProgressActionListener) asyncSearchContext.getTask().getProgressListener()).removeListener(actionListener);
                                            listener.onResponse(asyncSearchContext.geLatestSearchResponse());
                                        });
                                ((AsyncSearchProgressActionListener) asyncSearchContext.getTask().getProgressListener())
                                        .addOrExecuteListener(wrappedListener);
                            }
                        } else {
                            if (updateNeeded) {
                                asyncSearchService.updateKeepAlive(request, asyncSearchContext, ActionListener.wrap(
                                        (response) -> listener.onResponse(asyncSearchContext.geLatestSearchResponse()),
                                        listener::onFailure));
                            } else {
                                listener.onResponse(asyncSearchContext.geLatestSearchResponse());
                            }
                        }
                    },
                    listener::onFailure)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
