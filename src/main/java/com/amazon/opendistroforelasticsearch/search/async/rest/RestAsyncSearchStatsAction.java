package com.amazon.opendistroforelasticsearch.search.async.rest;

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAsyncSearchStatsAction extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestAsyncSearchStatsAction.class);
    private static final String NAME = "async_search_stats_action";
    private AsyncSearchStats asyncSearchStats;

    public RestAsyncSearchStatsAction(AsyncSearchStats asyncSearchStats) {
        this.asyncSearchStats = asyncSearchStats;
    }

    @Override
    public String getName() {
        return NAME;
    }


    @Override
    public List<Route> routes() {
        return Arrays.asList(
                new Route(GET, "/_async_search_stats/{nodeId}/stats/"),
                new Route(GET, "/_async_search_stats/{nodeId}/stats/{stat}"),
                new Route(GET, "/_async_search_stats/stats"),
                new Route(GET, "/_async_search_stats/stats/{stat}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AsyncSearchStatsRequest asyncSearchStatsRequest = getRequest(request);

        return channel -> client.execute(AsyncSearchStatsAction.INSTANCE, asyncSearchStatsRequest,
                new RestActions.NodesResponseRestListener<>(channel));
    }

    private AsyncSearchStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query
        String[] nodeIdsArr = null;
        String nodesIdsStr = request.param("nodeId");
        if (!Strings.isEmpty(nodesIdsStr)) {
            nodeIdsArr = nodesIdsStr.split(",");
        }

        AsyncSearchStatsRequest asyncSearchStatsRequest = new AsyncSearchStatsRequest(asyncSearchStats.getStats().keySet(), nodeIdsArr);
        asyncSearchStatsRequest.timeout(request.param("timeout"));

        // parse the stats the customer wants to see
        Set<String> statsSet = null;
        String statsStr = request.param("stat");
        if (!Strings.isEmpty(statsStr)) {
            statsSet = new HashSet<>(Arrays.asList(statsStr.split(",")));
        }

        if (statsSet == null) {
            asyncSearchStatsRequest.all();
        } else if (statsSet.size() == 1 && statsSet.contains("_all")) {
            asyncSearchStatsRequest.all();
        } else if (statsSet.contains(AsyncSearchStatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException("Request " + request.path() + " contains _all and individual stats");
        } else {
            Set<String> invalidStats = new TreeSet<>();
            for (String stat : statsSet) {
                if (!asyncSearchStatsRequest.addStat(stat)) {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats,
                        asyncSearchStatsRequest.getStatsToBeRetrieved(), "stat"));
            }

        }
        return asyncSearchStatsRequest;
    }
}
