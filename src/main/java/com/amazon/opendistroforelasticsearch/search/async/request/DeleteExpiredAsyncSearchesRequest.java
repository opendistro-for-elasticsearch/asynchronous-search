package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DeleteExpiredAsyncSearchesRequest extends ActionRequest {

    public static final String DEFAULT_REASON = "scheduled job";

    public DeleteExpiredAsyncSearchesRequest() {
        this.reason = DEFAULT_REASON;
    }

    public DeleteExpiredAsyncSearchesRequest(String reason) {
        this.reason = reason;
    }

    private final String reason;

    public DeleteExpiredAsyncSearchesRequest(StreamInput in) throws IOException {
        super(in);
        this.reason = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * The reason for deleting expired async searches.
     */
    public String reason() {
        return reason;
    }
}
