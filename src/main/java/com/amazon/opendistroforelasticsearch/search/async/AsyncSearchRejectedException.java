package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class AsyncSearchRejectedException extends ElasticsearchException {

    private final int limit;

    public AsyncSearchRejectedException(StreamInput in) throws IOException {
        super(in);
        limit = in.readInt();
    }


    public AsyncSearchRejectedException(String message, int limit) {
        super(message);
        this.limit = limit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(limit);
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("limit", limit);
    }
}

