package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AbstractAsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class AsyncSearchPersistenceContext extends AbstractAsyncSearchContext implements ToXContentObject {

    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";
    private final long expirationTimeMillis;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public String getResponse() {
        return response;
    }

    private final String response;

    public AsyncSearchPersistenceContext(NamedWriteableRegistry namedWriteableRegistry, AsyncSearchResponse asyncSearchResponse) throws IOException {
        super(AsyncSearchId.parseAsyncId(asyncSearchResponse.getId()));
        this.expirationTimeMillis = asyncSearchResponse.getExpirationTimeMillis();
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.response = encodeResponse(asyncSearchResponse);
    }


    public AsyncSearchPersistenceContext(NamedWriteableRegistry namedWriteableRegistry, AsyncSearchId asyncSearchId,
                                       long expirationTimeMillis, String response) {
        super(asyncSearchId);
        this.expirationTimeMillis = expirationTimeMillis;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.response = response;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        return null;
    }

    @Override
    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(decodeResponse(response), expirationTimeMillis);
    }

    @Override
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public Source getSource() {
        return Source.STORE;
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
