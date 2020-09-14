package com.amazon.opendistroforelasticsearch.search.async.persistence;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AsyncSearchPersistenceModel implements ToXContentObject {

    public static final String ASYNC_ID = "async_id";
    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";

    private final String asyncSearchId;
    private final long expirationTime;
    private final BytesReference response;

    public AsyncSearchPersistenceModel(String asyncSearchId, long expirationTime, BytesReference response) {
        this.asyncSearchId = asyncSearchId;
        this.expirationTime = expirationTime;
        this.response = response;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
