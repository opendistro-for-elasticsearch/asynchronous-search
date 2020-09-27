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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.*;
import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext.Stage.RUNNING;


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
        ActionListener<AsyncSearchResponse> groupedListener = null;
        try {
            AsyncSearchContext asyncSearchContext = asyncSearchService.findContext(request.getId(), asyncSearchId.getAsyncSearchContextId());
            AsyncSearchContext.Stage stage = asyncSearchContext.getStage();
            boolean updateNeeded = request.getKeepAlive() != null;
            //TODO wire up grouped listener. This stands broken
            if (updateNeeded) {
                groupedListener= new GroupedActionListener<AsyncSearchResponse>(ActionListener.wrap(
                        (response) -> listener.onResponse((AsyncSearchResponse) response), (e) -> listener.onFailure(e)), 2);
            }
            assert stage.equals(INIT) == false : "Should be moved to "+ RUNNING + "found" + stage;
            if (stage == RUNNING) {
                PrioritizedListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                        request.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener, (actionListener) -> {
                            listener.onResponse(asyncSearchContext.getPartialSearchResponse());
                            ((AsyncSearchProgressActionListener)
                                    asyncSearchContext.getTask().getProgressListener()).removeListener(actionListener);
                        });
                ((AsyncSearchProgressActionListener) asyncSearchContext.getTask().getProgressListener())
                        .addOrExecuteListener(wrappedListener);
            }
            asyncSearchService.updateExpiryTimeIfRequired(request, asyncSearchContext, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
