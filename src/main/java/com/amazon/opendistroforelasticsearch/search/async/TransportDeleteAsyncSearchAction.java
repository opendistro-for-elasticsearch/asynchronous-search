package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteAsyncSearchAction extends HandledTransportAction<GetAsyncSearchRequest, AsyncSearchResponse> {

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
                                            AsyncSearchService asyncSearchService, TransportSearchAction transportSearchAction, Client client) {
        super(DeleteAsyncSearchAction.NAME, transportService, actionFilters, GetAsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        TaskId taskId = null;
        try {
            //TODO get taskId from the Async Context
            final CancelTasksRequest searchTaskToCancel = new CancelTasksRequest();
            searchTaskToCancel.setReason("Search request cancellation timeout interval is exceeded. This may be because of expensive search query or overloaded cluster.");
            searchTaskToCancel.setTaskId(taskId);
            // Send the cancel task request for this search request. It is best effort cancellation as we don't wait
            // for cancellation to finish and ignore any error or response.
            client.admin().cluster().cancelTasks(searchTaskToCancel, ActionListener.wrap(
                    r -> {
                        logger.debug("Scheduled cancel task for search request on expiry of cancel_after_timeinterval: " +
                                "[taskId: {}] is successfully completed", searchTaskToCancel.getTaskId());
                    },
                    e -> {
                        logger.error(new ParameterizedMessage("Scheduled cancel task for search request on expiry of cancel_after_timeinterval: " +
                                "[taskId: {}] is failed", searchTaskToCancel.getTaskId()), e);
                    }));
            //TODO clean up context and get the right response
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
