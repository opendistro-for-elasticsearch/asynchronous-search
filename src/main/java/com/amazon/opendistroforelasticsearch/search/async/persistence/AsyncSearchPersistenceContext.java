package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class AsyncSearchPersistenceContext extends AsyncSearchContext implements Writeable, ToXContentObject {


    /**
     * super constructor needs AsyncSearchId object. InstantiatingObjectParser needs constructor which matches parsed fields. Hence
     * adding Id field
     */
    public static final String ID = "id";
    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String RESPONSE = "response";

    public static final InstantiatingObjectParser<AsyncSearchPersistenceContext, Void> PARSER;

    static {

        InstantiatingObjectParser.Builder<AsyncSearchPersistenceContext, Void> parser = InstantiatingObjectParser.builder(
                "async_search_persistence_context", true, AsyncSearchPersistenceContext.class);
        parser.declareString(constructorArg(), new ParseField(ID));
        parser.declareLong(constructorArg(), new ParseField(EXPIRATION_TIME));
        ObjectParserHelper<AsyncSearchPersistenceContext, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(parser, constructorArg(), new ParseField("response"));
        PARSER = parser.build();
    }

    private final long expirationTimeMillis;
    private final BytesReference response;
    private final String id;


    public AsyncSearchPersistenceContext(AsyncSearchResponse asyncSearchResponse) throws IOException {
        this(asyncSearchResponse.getId(), asyncSearchResponse.getExpirationTimeMillis(), XContentHelper.toXContent(asyncSearchResponse,
                Requests.INDEX_CONTENT_TYPE,
                true));
    }

    public AsyncSearchPersistenceContext(String id, long expirationTimeMillis, BytesReference response) {
        super(AsyncSearchId.parseAsyncId(id).getAsyncSearchContextId());
        this.id = id;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = response;
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
        return AsyncSearchId.parseAsyncId(id);
    }

    @Override
    public AsyncSearchResponse getAsyncSearchResponse() {
        AsyncSearchResponse response = decodeResponse();
        return response == null ? null : new AsyncSearchResponse(response, expirationTimeMillis);
    }

    @Override
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public Source getSource() {
        return Source.STORE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(expirationTimeMillis);
        out.writeBytesReference(response);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    private void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(ID, id);
        builder.field(EXPIRATION_TIME, expirationTimeMillis);
        XContentHelper.writeRawField("response", response, builder, params);
    }

    private AsyncSearchResponse decodeResponse() {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, response.streamInput())) {
            return AsyncSearchResponse.fromXContent(parser);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getId() {
        return id;
    }
}
