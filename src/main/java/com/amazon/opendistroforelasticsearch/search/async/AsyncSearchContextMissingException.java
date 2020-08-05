package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class AsyncSearchContextMissingException extends ElasticsearchException {

    private final ParsedAsyncSearchId contextId;

    public AsyncSearchContextMissingException(ParsedAsyncSearchId contextId) {
        super("No search context found for id [" + contextId.getId() + "]");
        this.contextId = contextId;
    }

    public ParsedAsyncSearchId contextId() {
        return this.contextId;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    public AsyncSearchContextMissingException(StreamInput in, ParsedAsyncSearchId contextId) throws IOException {
        super(in);
        //contextId = new ParsedAsyncSearchId(in);
        this.contextId = contextId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        //contextId.writeTo(out);
    }
}
