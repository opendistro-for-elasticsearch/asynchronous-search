package com.amazon.opendistroforelasticsearch.search.async.stats;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Class represents all stats the plugin keeps track of
 */
public class AsyncSearchStats extends BaseNodeResponse implements ToXContentFragment {

    private AsyncSearchStatusStats asyncSearchStatusStats;

    public AsyncSearchStats(StreamInput in) throws IOException {
        super(in);
        asyncSearchStatusStats = in.readOptionalWriteable(AsyncSearchStatusStats::new);
    }

    public AsyncSearchStats(DiscoveryNode node, @Nullable AsyncSearchStatusStats asyncSearchStatusStats) {
        super(node);
        this.asyncSearchStatusStats = asyncSearchStatusStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(asyncSearchStatusStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getAddress());

        builder.startArray("roles");
        for (DiscoveryNodeRole role : getNode().getRoles()) {
            builder.value(role.roleName());
        }
        builder.endArray();

        if (!getNode().getAttributes().isEmpty()) {
            builder.startObject("attributes");
            for (Map.Entry<String, String> attrEntry : getNode().getAttributes().entrySet()) {
                builder.field(attrEntry.getKey(), attrEntry.getValue());
            }
            builder.endObject();
        }
        if (asyncSearchStatusStats != null) {
            asyncSearchStatusStats.toXContent(builder, params);
        }
        return builder;
    }
}
