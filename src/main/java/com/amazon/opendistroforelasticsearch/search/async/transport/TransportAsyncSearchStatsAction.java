package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsNodeRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchStatsNodeResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchStatsResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportAsyncSearchStatsAction extends TransportNodesAction<AsyncSearchStatsRequest, AsyncSearchStatsResponse,
        AsyncSearchStatsNodeRequest, AsyncSearchStatsNodeResponse> {

    private final AsyncSearchService asyncSearchService;
    private AsyncSearchStats asyncSearchStats;

    @Inject
    public TransportAsyncSearchStatsAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            AsyncSearchService asyncSearchService,
            AsyncSearchStats asyncSearchStats
    ) {
        super(AsyncSearchStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, AsyncSearchStatsRequest::new,
                AsyncSearchStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT, AsyncSearchStatsNodeResponse.class);
        this.asyncSearchService = asyncSearchService;
        this.asyncSearchStats = asyncSearchStats;

    }

    @Override
    protected AsyncSearchStatsResponse newResponse(
            AsyncSearchStatsRequest request, List<AsyncSearchStatsNodeResponse> asyncSearchStatsNodeResponses,
            List<FailedNodeException> failures) {
        Map<String, Object> clusterStats = new HashMap<>();
        Set<String> statsToBeRetrieved = request.getStatsToBeRetrieved();

        for (String statName : asyncSearchStats.getClusterStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                clusterStats.put(statName, asyncSearchStats.getStats().get(statName).getValue());
            }
        }
        return new AsyncSearchStatsResponse(clusterService.getClusterName(), asyncSearchStatsNodeResponses, failures, clusterStats);
    }

    @Override
    protected AsyncSearchStatsNodeRequest newNodeRequest(AsyncSearchStatsRequest request) {
        return new AsyncSearchStatsNodeRequest(request);
    }

    @Override
    protected AsyncSearchStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new AsyncSearchStatsNodeResponse(in);
    }

    @Override
    protected AsyncSearchStatsNodeResponse nodeOperation(AsyncSearchStatsNodeRequest request) {
        return createAsyncSearchStatsNodeResponse(request.getAsyncSearchStatsRequest());
    }

    private AsyncSearchStatsNodeResponse createAsyncSearchStatsNodeResponse(AsyncSearchStatsRequest request) {
        Map<String, Object> statValues = new HashMap<>();
        Set<String> statsToBeRetrieved = request.getStatsToBeRetrieved();

        for (String statName : asyncSearchStats.getNodeStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                statValues.put(statName, asyncSearchStats.getStats().get(statName).getValue());
            }
        }

        return new AsyncSearchStatsNodeResponse(clusterService.localNode(), statValues);
    }
}
