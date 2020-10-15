package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchStatsResponse;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportAsyncSearchStatsAction extends TransportNodesAction<AsyncSearchStatsRequest, AsyncSearchStatsResponse,
        TransportAsyncSearchStatsAction.AsyncSearchStatsNodeRequest, AsyncSearchStats> {

    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportAsyncSearchStatsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                           ActionFilters actionFilters, AsyncSearchService asyncSearchService) {

        super(AsyncSearchStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, AsyncSearchStatsRequest::new,
                AsyncSearchStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT, AsyncSearchStats.class);
        this.asyncSearchService = asyncSearchService;

    }

    @Override
    protected AsyncSearchStatsResponse newResponse(AsyncSearchStatsRequest request, List<AsyncSearchStats> responses,
                                                   List<FailedNodeException> failures) {
        return new AsyncSearchStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected AsyncSearchStatsNodeRequest newNodeRequest(AsyncSearchStatsRequest request) {
        return new AsyncSearchStatsNodeRequest(request);
    }

    @Override
    protected AsyncSearchStats newNodeResponse(StreamInput in) throws IOException {
        return new AsyncSearchStats(in);
    }

    @Override
    protected AsyncSearchStats nodeOperation(AsyncSearchStatsNodeRequest asyncSearchStatsNodeRequest) {
        return asyncSearchService.stats(true);

    }

    public static class AsyncSearchStatsNodeRequest extends BaseNodeRequest {

        AsyncSearchStatsRequest request;

        public AsyncSearchStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new AsyncSearchStatsRequest(in);
        }

        AsyncSearchStatsNodeRequest(AsyncSearchStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
