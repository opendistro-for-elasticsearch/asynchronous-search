package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class AsyncSearchPersistenceModel extends AsyncSearchContext implements ToXContentObject {

    public static final String ASYNC_ID = "async_id";
    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";

    private final String asyncSearchId;
    private final long expirationTime;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public String getResponse() {
        return response;
    }

    private final String response;

    public AsyncSearchPersistenceModel(
            NamedWriteableRegistry namedWriteableRegistry,
            AsyncSearchResponse asyncSearchResponse) throws IOException {
        super(asyncSearchResponse.getId());
        this.asyncSearchId = asyncSearchResponse.getId();
        this.expirationTime = asyncSearchResponse.getExpirationTimeMillis();
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.response = encodeResponse(asyncSearchResponse);
    }


    public AsyncSearchPersistenceModel(
            NamedWriteableRegistry namedWriteableRegistry,
            String asyncSearchId,
            long expirationTime,
            String response) {
        super(asyncSearchId);
        this.asyncSearchId = asyncSearchId;
        this.expirationTime = expirationTime;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.response = response;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(decodeResponse(response), expirationTime);
    }

    @Override
    public long getExpirationTimeInMills() {
        return expirationTime;
    }

    @Override
    public Lifetime getLifetime() {
        return Lifetime.STORE;
    }

    private String encodeResponse(AsyncSearchResponse asyncSearchResponse) throws IOException {

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            asyncSearchResponse.writeTo(out);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            return Base64.getUrlEncoder().encodeToString(bytes);
        }
    }

    private AsyncSearchResponse decodeResponse(String response) {
        BytesReference bytesReference = BytesReference.fromByteBuffer(ByteBuffer.wrap(Base64.getUrlDecoder().decode(response)));
        try (NamedWriteableAwareStreamInput wrapperStreamInput = new NamedWriteableAwareStreamInput(bytesReference.streamInput(),
                namedWriteableRegistry)) {
            return new AsyncSearchResponse(wrapperStreamInput);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse async search id", e);
        }
    }

}
