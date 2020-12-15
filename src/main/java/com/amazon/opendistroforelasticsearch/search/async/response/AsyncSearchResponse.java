/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.async.response;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class AsyncSearchResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("id");
    private static final ParseField STATUS = new ParseField("status");
    private static final ParseField START_TIME_IN_MILLIS = new ParseField("start_time_in_millis");
    private static final ParseField EXPIRATION_TIME_IN_MILLIS = new ParseField("expiration_time_in_millis");
    private static final ParseField RESPONSE = new ParseField("response");
    private static final ParseField ERROR = new ParseField("error");
    @Nullable
    //when the search is cancelled we don't have the id
    private String id;
    private final String status;
    private final long startTimeMillis;
    private final long expirationTimeMillis;
    @Nullable
    private SearchResponse searchResponse;
    @Nullable
    private ElasticsearchException error;

    public AsyncSearchResponse(String id, String status, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, Exception error) {
        this.id = id;
        this.status = status;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error == null ? null : ExceptionsHelper.convertToElastic(error);
    }

    public AsyncSearchResponse(String id, String status, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, ElasticsearchException error) {
        this.id = id;
        this.status = status;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error;
    }

    public AsyncSearchResponse(String status, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, ElasticsearchException error) {
        this.status = status;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error;
    }

    public AsyncSearchResponse(StreamInput in) throws IOException {
        this.id = in.readOptionalString();
        this.status = in.readOptionalString();
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
        this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
        this.error = in.readBoolean() ? in.readException() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(id);
        out.writeOptionalString(status);
        out.writeLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
        out.writeOptionalWriteable(searchResponse);
        if (error != null) {
            out.writeBoolean(true);
            out.writeException(error);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field(ID.getPreferredName(), id);
        }
        builder.field(STATUS.getPreferredName(), status);
        builder.field(START_TIME_IN_MILLIS.getPreferredName(), startTimeMillis);
        builder.field(EXPIRATION_TIME_IN_MILLIS.getPreferredName(), expirationTimeMillis);
        if (searchResponse != null) {
            builder.startObject(RESPONSE.getPreferredName());
            searchResponse.innerToXContent(builder, params);
            builder.endObject();
        }
        if (error != null) {
            builder.startObject(ERROR.getPreferredName());
            ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public String getStatus() {
        return status;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public ElasticsearchException getError() {
        return error;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public String getId() {
        return id;
    }

    @Override
    public RestStatus status() {
        return searchResponse != null || error != null ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }


    /**
     * {@linkplain SearchResponse} and {@linkplain ElasticsearchException} don't override hashcode, hence cannot be included in
     * the hashcode calculation for {@linkplain AsyncSearchResponse}. Given that we are using these methods only in tests; on the
     * off-chance that the {@link #equals(Object)} ()} comparison fails and hashcode is equal for 2
     * {@linkplain AsyncSearchResponse} objects, we are wary of the @see
     * <a href="https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/lang/Object.html#hashCode()">
     * performance improvement on hash tables </a>} that we forgo.
     *
     * @return hashcode of {@linkplain AsyncSearchResponse}
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, status, startTimeMillis, expirationTimeMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AsyncSearchResponse other = (AsyncSearchResponse) o;
        try {
            return id.equals(other.id) &&
                    status.equals(other.status) &&
                    startTimeMillis == other.startTimeMillis &&
                    expirationTimeMillis == other.expirationTimeMillis
                    && Objects.equals(getErrorAsMap(error), getErrorAsMap(other.error))
                    && Objects.equals(getResponseAsMap(searchResponse), getResponseAsMap(other.searchResponse));
        } catch (IOException e) {
            return false;
        }
    }

    private Map<String, Object> getErrorAsMap(ElasticsearchException exception) throws IOException {
        if (exception != null) {
            BytesReference error;
            try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, exception);
                builder.endObject();
                error = BytesReference.bytes(builder);
                return convertToMap(error, false, Requests.INDEX_CONTENT_TYPE).v2();
            }
        } else {
            return emptyMap();
        }
    }

    private Map<String, Object> getResponseAsMap(SearchResponse searchResponse) throws IOException {
        if (searchResponse != null) {
            BytesReference response = XContentHelper.toXContent(searchResponse, Requests.INDEX_CONTENT_TYPE, true);
            if (response == null) {
                return emptyMap();
            }
            return convertToMap(response, false, Requests.INDEX_CONTENT_TYPE).v2();
        } else {
            return null;
        }
    }

    public static AsyncSearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    public static AsyncSearchResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String id = null;
        String status = null;
        long startTimeMillis = -1;
        long expirationTimeMillis = -1;
        SearchResponse searchResponse = null;
        ElasticsearchException error = null;
        String currentFieldName = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            currentFieldName = parser.currentName();
            if (RESPONSE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
                    searchResponse = SearchResponse.innerFromXContent(parser);
                }
            } else if (ERROR.match(currentFieldName, parser.getDeprecationHandler())) {
                parser.nextToken();
                error = ElasticsearchException.fromXContent(parser);

            } else if (token.isValue()) {
                if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    id = parser.text();
                } else if (START_TIME_IN_MILLIS.match(currentFieldName, parser.getDeprecationHandler())) {
                    startTimeMillis = parser.longValue();
                } else if (EXPIRATION_TIME_IN_MILLIS.match(currentFieldName, parser.getDeprecationHandler())) {
                    expirationTimeMillis = parser.longValue();
                } else if (STATUS.match(currentFieldName, parser.getDeprecationHandler())) {
                    status = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new AsyncSearchResponse(id, status, startTimeMillis, expirationTimeMillis, searchResponse, error);
    }

    //visible for testing
    public static AsyncSearchResponse empty(String id, SearchResponse searchResponse, Exception exception) {
        return new AsyncSearchResponse(id, null, -1, -1, searchResponse, exception);
    }
}
