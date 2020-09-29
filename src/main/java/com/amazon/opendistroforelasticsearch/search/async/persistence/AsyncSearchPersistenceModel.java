package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AsyncSearchPersistenceModel extends AsyncSearchContext implements ToXContentObject {

    public static final String ASYNC_ID = "async_id";
    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";

    private final String asyncSearchId;
    private final long expirationTime;
    private final BytesReference response;

    public AsyncSearchPersistenceModel(String asyncSearchId, long expirationTime, BytesReference response) {
        super(asyncSearchId);
        this.asyncSearchId = asyncSearchId;
        this.expirationTime = expirationTime;
        this.response = response;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public AsyncSearchResponse getSearchResponse() {
        return null;
    }

    @Override
    public long getExpirationTimeInMills() {
        return expirationTime;
    }

    @Override
    public Lifetime getLifetime() {
        return Lifetime.PERSISTED;
    }
}
