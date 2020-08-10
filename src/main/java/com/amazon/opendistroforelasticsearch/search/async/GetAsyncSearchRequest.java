package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Optional;

public class GetAsyncSearchRequest extends ActionRequest {


    private TimeValue waitForCompletion;
    private TimeValue keepAlive;

    private final String id;

    public GetAsyncSearchRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }


    public GetAsyncSearchRequest(StreamInput streamInput) throws IOException {
        this.id = streamInput.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
