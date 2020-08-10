package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class AsyncSearchContextMissingException extends ElasticsearchException {

    private final AsyncSearchContextId contextId;

    public AsyncSearchContextMissingException(AsyncSearchContextId contextId) {
        super("No async search context found for id [" + contextId.getId() + "]");
        this.contextId = contextId;
    }

    public AsyncSearchContextId contextId() {
        return this.contextId;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    public AsyncSearchContextMissingException(StreamInput in, AsyncSearchContextId contextId) throws IOException {
        super(in);
        contextId = new AsyncSearchContextId(in);
        this.contextId = contextId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
    }
}
