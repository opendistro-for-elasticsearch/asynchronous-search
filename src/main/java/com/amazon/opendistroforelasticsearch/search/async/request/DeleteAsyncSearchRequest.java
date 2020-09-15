package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class DeleteAsyncSearchRequest extends FetchAsyncSearchRequest<DeleteAsyncSearchRequest> {

    public DeleteAsyncSearchRequest(String id) {
        super(id);
    }

    public DeleteAsyncSearchRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
