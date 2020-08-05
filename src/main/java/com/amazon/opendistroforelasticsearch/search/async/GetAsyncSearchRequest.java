package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

public class GetAsyncSearchRequest extends ActionRequest {

    public GetAsyncSearchRequest() {

    }

    public GetAsyncSearchRequest(StreamInput streamInput) {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
