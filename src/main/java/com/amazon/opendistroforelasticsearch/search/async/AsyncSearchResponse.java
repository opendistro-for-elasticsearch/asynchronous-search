package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class AsyncSearchResponse extends ActionResponse implements StatusToXContentObject {

    private String asyncSearchId;
    private boolean isPartial;
    private boolean isRunning;
    private SearchResponse searchResponse;


    public AsyncSearchResponse(StreamInput streamInput) {
    }

    @Override
    public RestStatus status() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}
