package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class GetAsyncSearchRequest extends FetchAsyncSearchRequest<GetAsyncSearchRequest> {

    public static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = TimeValue.timeValueSeconds(1);

    private TimeValue waitForCompletion = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;
    private TimeValue keepAlive;

    public GetAsyncSearchRequest(String id) {
        super(id);
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
        super(streamInput);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
