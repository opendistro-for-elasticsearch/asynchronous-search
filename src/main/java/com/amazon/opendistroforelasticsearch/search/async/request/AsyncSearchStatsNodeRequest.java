package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AsyncSearchStatsNodeRequest extends BaseNodeRequest {
    AsyncSearchStatsRequest request;

    public AsyncSearchStatsNodeRequest() {
        super();
    }

    public AsyncSearchStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new AsyncSearchStatsRequest(in);
    }

    public AsyncSearchStatsNodeRequest(AsyncSearchStatsRequest request) {
        this.request = request;
    }

    public AsyncSearchStatsRequest getAsyncSearchStatsRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
