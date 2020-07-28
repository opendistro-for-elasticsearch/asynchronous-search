package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AsyncSearchResponse extends SearchResponse {

    private String asyncSearchId;
    private boolean isPartial;
    private boolean isRunning;

    public AsyncSearchResponse(StreamInput in) throws IOException {
        super(in);
    }

    public AsyncSearchResponse(SearchResponseSections internalResponse, String scrollId, int totalShards, int successfulShards, int skippedShards, long tookInMillis, ShardSearchFailure[] shardFailures, Clusters clusters) {
        super(internalResponse, scrollId, totalShards, successfulShards, skippedShards, tookInMillis, shardFailures, clusters);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return super.toXContent(builder, params);
    }


}
