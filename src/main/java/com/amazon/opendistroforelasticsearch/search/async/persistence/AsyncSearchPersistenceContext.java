package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class AsyncSearchPersistenceContext extends AsyncSearchContext implements ToXContentObject {

    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";
    private final long expirationTimeMillis;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public String getResponse() {
        return response;
    }

    private final String response;

    public AsyncSearchPersistenceContext(NamedWriteableRegistry namedWriteableRegistry, AsyncSearchResponse asyncSearchResponse) {
        super(AsyncSearchId.parseAsyncId(asyncSearchResponse.getId()).getAsyncSearchContextId());
        this.expirationTimeMillis = asyncSearchResponse.getExpirationTimeMillis();
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.response = encodeResponse(asyncSearchResponse);
    }


    public AsyncSearchPersistenceContext(NamedWriteableRegistry namedWriteableRegistry, AsyncSearchContextId asyncSearchContextId,
                                       long expirationTimeMillis, String response) {
        super(asyncSearchContextId);
        this.expirationTimeMillis = expirationTimeMillis;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.response = response;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        return null;
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
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

    private String encodeResponse(AsyncSearchResponse asyncSearchResponse) {

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            asyncSearchResponse.writeTo(out);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            return Base64.getUrlEncoder().encodeToString(bytes);
        } catch (IOException e) {
            return null;
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

    private void test(BytesReference originalBytes) {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, originalBytes.streamInput())) {
            AsyncSearchResponse parsed = AsyncSearchResponse.fromXContent(parser);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
