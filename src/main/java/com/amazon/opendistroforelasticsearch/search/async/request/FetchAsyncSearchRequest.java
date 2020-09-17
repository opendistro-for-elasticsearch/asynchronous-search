package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * A based request for fetching running async search request
 */
public abstract class FetchAsyncSearchRequest<Request extends FetchAsyncSearchRequest<Request>> extends ActionRequest {

    public static final TimeValue DEFAULT_CONNECTION_TIMEOUT = TimeValue.timeValueSeconds(10);

    protected TimeValue connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private final String id;

    protected FetchAsyncSearchRequest(String id) {
        this.id = id;
    }

    protected FetchAsyncSearchRequest(StreamInput in) throws IOException {
        super(in);
        connectionTimeout = in.readTimeValue();
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(connectionTimeout);
        out.writeString(id);
    }

    /**
     * A timeout value in case the coordinator has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final Request connectionTimeout(TimeValue timeout) {
        this.connectionTimeout = timeout;
        return (Request) this;
    }

    public String getId() {
        return this.id;
    }

    /**
     * A timeout value in case the coordinator has not been discovered yet or disconnected.
     */
    public final Request connectionTimeout(String timeout) {
        return connectionTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".connectionTimeout"));
    }

    public final TimeValue connectionTimeout() {
        return this.connectionTimeout;
    }
}
