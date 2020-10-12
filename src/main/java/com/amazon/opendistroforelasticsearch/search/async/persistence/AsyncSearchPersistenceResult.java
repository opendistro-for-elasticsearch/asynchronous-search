package com.amazon.opendistroforelasticsearch.search.async.persistence;

import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class AsyncSearchPersistenceResult implements Writeable, ToXContentObject {

    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";
    private final long expirationTimeMillis;

    public BytesReference getResponse() {
        return response;
    }

    private final BytesReference response;

    public AsyncSearchPersistenceResult(long expirationTimeMillis, BytesReference response) {
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = response;
    }

    public static final ConstructingObjectParser<AsyncSearchPersistenceResult, Void> PARSER = new ConstructingObjectParser<>(
            "stored_response", a -> {
        int i = 0;
        long expirationTimeMillis = (long) a[i++];
        BytesReference response = (BytesReference) a[i++];
        return new AsyncSearchPersistenceResult(expirationTimeMillis, response);
    });
    static {
        PARSER.declareLong(constructorArg(), new ParseField(EXPIRATION_TIME));
        ObjectParserHelper<AsyncSearchPersistenceResult, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(PARSER, constructorArg(), new ParseField(RESPONSE));
    }

    public AsyncSearchPersistenceResult(long expirationTimeMillis, ToXContent response) throws IOException {
        this(expirationTimeMillis, XContentHelper.toXContent(response, Requests.INDEX_CONTENT_TYPE, true));
    }

    /**
     * Read from a stream.
     */
    public AsyncSearchPersistenceResult(StreamInput in) throws IOException {
        expirationTimeMillis = in.readLong();
        response = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(expirationTimeMillis);
        out.writeBytesReference(response);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(EXPIRATION_TIME, expirationTimeMillis);
        XContentHelper.writeRawField(RESPONSE, response, Requests.INDEX_CONTENT_TYPE, builder, params);
        return builder;
    }
}
