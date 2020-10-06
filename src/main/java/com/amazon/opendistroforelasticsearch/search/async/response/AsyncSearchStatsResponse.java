package com.amazon.opendistroforelasticsearch.search.async.response;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Contains aggregation of async search stats from the nodes
 */
public class AsyncSearchStatsResponse extends BaseNodesResponse<AsyncSearchStatsNodeResponse> implements ToXContentObject {
    private static final String NODES_KEY = "nodes";
    private Map<String, Object> clusterStats;

    public AsyncSearchStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(AsyncSearchStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
        clusterStats = in.readMap();
    }

    public AsyncSearchStatsResponse(ClusterName clusterName, List<AsyncSearchStatsNodeResponse> nodes, List<FailedNodeException> failures,
                                    Map<String, Object> clusterStats) {
        super(clusterName, nodes, failures);
        this.clusterStats = clusterStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(clusterStats);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<AsyncSearchStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<AsyncSearchStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(AsyncSearchStatsNodeResponse::readStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Return cluster level stats
        for (Map.Entry<String, Object> clusterStat : clusterStats.entrySet()) {
            builder.field(clusterStat.getKey(), clusterStat.getValue());
        }

        // Return node level stats
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (AsyncSearchStatsNodeResponse AsyncSearchStats : getNodes()) {
            node = AsyncSearchStats.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);
            AsyncSearchStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
