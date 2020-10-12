package com.amazon.opendistroforelasticsearch.search.async.stats;

import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchCountStats;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class represents all stats the plugin keeps track of
 */
public class AsyncSearchStats extends BaseNodeResponse implements ToXContentFragment {

    private AsyncSearchCountStats asyncSearchCountStats;

    public AsyncSearchStats(StreamInput in) throws IOException {
        super(in);
        asyncSearchCountStats = in.readOptionalWriteable(AsyncSearchCountStats::new);
    }

    public AsyncSearchStats(DiscoveryNode node, @Nullable AsyncSearchCountStats asyncSearchCountStats) {
        super(node);
        this.asyncSearchCountStats = asyncSearchCountStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(asyncSearchCountStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());
        if (asyncSearchCountStats != null) {
            asyncSearchCountStats.toXContent(builder, params);
        }
        return builder;
    }
}
