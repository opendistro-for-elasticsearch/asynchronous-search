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

package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class AsyncSearchResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("id");
    private static final ParseField IS_PARTIAL = new ParseField("is_partial");
    private static final ParseField IS_RUNNING = new ParseField("is_running");
    private static final ParseField START_TIME_IN_MILLIS = new ParseField("start_time_in_millis");
    private static final ParseField EXPIRATION_TIME_IN_MILLIS = new ParseField("expiration_time_in_millis");
    private static final ParseField RESPONSE = new ParseField("response");
    private static final ParseField ERROR = new ParseField("error");


    private String id;

    private boolean isPartial;
    private boolean isRunning;
    private long startTimeMillis;
    private long expirationTimeMillis;
    private SearchResponse searchResponse;
    private ElasticsearchException error;

    public AsyncSearchResponse(String id, boolean isPartial, boolean isRunning, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, ElasticsearchException error) {
        this.id = id;
        this.isPartial = isPartial;
        this.isRunning = isRunning;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(isPartial);
        out.writeBoolean(isRunning);
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

    public AsyncSearchResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.isPartial = in.readBoolean();
        this.isRunning = in.readBoolean();
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
        this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
        this.error = in.readBoolean() ? in.readException() :  null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(IS_PARTIAL.getPreferredName(), isPartial);
        builder.field(IS_RUNNING.getPreferredName(), isRunning);
        builder.field(START_TIME_IN_MILLIS.getPreferredName(), startTimeMillis);
        builder.field(EXPIRATION_TIME_IN_MILLIS.getPreferredName(), expirationTimeMillis);
        if (searchResponse != null) {
            builder.field(RESPONSE.getPreferredName());
            searchResponse.toXContent(builder, params);
        }
        if (error != null) {
            builder.startObject(ERROR.getPreferredName());
            ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public String getId() {
        return id;
    }

    @Override
    public RestStatus status() {
        return searchResponse == null ? null : searchResponse.status();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
