package com.amazon.opendistroforelasticsearch.search.async.persistence;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.common.xcontent.ParserConstructor;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The model for persisting response to an index for retrieval after the search is complete
 */
public class AsyncSearchPersistenceModel implements ToXContentObject {

    public static final String EXPIRATION_TIME_MILLIS = "expiration_time_millis";
    public static final String START_TIME_MILLIS = "start_time_millis";
    public static final String RESPONSE = "search_response";

    private final long expirationTimeMillis;
    private final long startTimeMillis;
    private final BytesReference response;

    public static final InstantiatingObjectParser<AsyncSearchPersistenceModel, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<AsyncSearchPersistenceModel, Void> parser = InstantiatingObjectParser.builder(
                "stored_response", true, AsyncSearchPersistenceModel.class);
        parser.declareLong(constructorArg(), new ParseField(START_TIME_MILLIS));
        parser.declareLong(constructorArg(), new ParseField(EXPIRATION_TIME_MILLIS));
        ObjectParserHelper<AsyncSearchPersistenceModel, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(parser, constructorArg(), new ParseField(RESPONSE));
        PARSER = parser.build();
    }

    @ParserConstructor
    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, BytesReference response) {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = response;
    }

    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, SearchResponse response) throws IOException {
        this(startTimeMillis, expirationTimeMillis, XContentHelper.toXContent(response, Requests.INDEX_CONTENT_TYPE, false));
    }


    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public BytesReference getResponse() {
        return response;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(START_TIME_MILLIS, startTimeMillis);
        builder.field(EXPIRATION_TIME_MILLIS, expirationTimeMillis);
        XContentHelper.writeRawField(RESPONSE, response, Requests.INDEX_CONTENT_TYPE, builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeMillis, expirationTimeMillis, response);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AsyncSearchPersistenceModel persistenceModel = (AsyncSearchPersistenceModel) o;
        return startTimeMillis == persistenceModel.startTimeMillis &&
                expirationTimeMillis == persistenceModel.expirationTimeMillis &&
                response == persistenceModel.response;
    }
}
