package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

public class DeleteAsyncSearchRequest extends ActionRequest {
    private final String id;

    public DeleteAsyncSearchRequest(String id) {
        this.id = id;
    }

    public DeleteAsyncSearchRequest(StreamInput streamInput) {
        this.id = streamInput.toString();
    }

    public String getId() {
        return this.id;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
