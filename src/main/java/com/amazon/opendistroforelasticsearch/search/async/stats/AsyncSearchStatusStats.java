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

package com.amazon.opendistroforelasticsearch.search.async.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AsyncSearchStatusStats implements Writeable, ToXContentFragment {

    private final long runningStage;
    private final long persistedStage;
    private final long completedStage;
    private final long failedStage;

    public AsyncSearchStatusStats(long runningStage, long persistedStage,
                                  long completedStage, long failedStage) {
        this.runningStage = runningStage;
        this.persistedStage = persistedStage;
        this.completedStage = completedStage;
        this.failedStage = failedStage;
    }

    public AsyncSearchStatusStats(StreamInput in) throws IOException {
        this.runningStage = in.readVLong();
        this.persistedStage = in.readVLong();
        this.completedStage = in.readVLong();
        this.failedStage = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(this.runningStage);
        out.writeVLong(this.persistedStage);
        out.writeVLong(this.completedStage);
        out.writeVLong(this.failedStage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ASYNC_SEARCH_STATUS);
        builder.field(Fields.RUNNING, runningStage);
        builder.field(Fields.PERSISTED, persistedStage);
        builder.field(Fields.FAILED, failedStage);
        builder.field(Fields.COMPLETED, completedStage);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String ASYNC_SEARCH_STATUS = "async_search_status";
        static final String RUNNING = "running";
        static final String PERSISTED = "persisted";
        static final String FAILED = "failed";
        static final String COMPLETED = "completed";
    }
}
