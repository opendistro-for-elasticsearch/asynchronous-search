package com.amazon.opendistroforelasticsearch.search.async.response;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AsyncSearchCountStats implements Writeable, ToXContentFragment {

    private final long runningStageMilestone;
    private final long abortedStageMilestone;
    private final long persistedStageMilestone;
    private final long completedStageMilestone;
    private final long failedStageMilestone;

    public AsyncSearchCountStats(long runningStageMilestone, long abortedStageMilestone, long persistedStageMilestone,
                                 long completedStageMilestone, long failedStageMilestone) {
        this.runningStageMilestone = runningStageMilestone;
        this.abortedStageMilestone = abortedStageMilestone;
        this.persistedStageMilestone = persistedStageMilestone;
        this.completedStageMilestone = completedStageMilestone;
        this.failedStageMilestone = failedStageMilestone;
    }

    public AsyncSearchCountStats(StreamInput in) throws IOException {
        this.runningStageMilestone = in.readVLong();
        this.abortedStageMilestone = in.readVLong();
        this.persistedStageMilestone = in.readVLong();
        this.completedStageMilestone = in.readVLong();
        this.failedStageMilestone = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(this.runningStageMilestone);
        out.writeVLong(this.abortedStageMilestone);
        out.writeVLong(this.persistedStageMilestone);
        out.writeVLong(this.completedStageMilestone);
        out.writeVLong(this.failedStageMilestone);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ASYNC_SEARCH_STAGEWISE_COUNT);
        builder.field(Fields.RUNNING, runningStageMilestone);
        builder.field(Fields.PERSISTED, persistedStageMilestone);
        builder.field(Fields.ABORTED, abortedStageMilestone);
        builder.field(Fields.FAILED, failedStageMilestone);
        builder.field(Fields.COMPLETED, completedStageMilestone);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String ASYNC_SEARCH_STAGEWISE_COUNT = "async_search_stagewise_count";
        static final String RUNNING = "running";
        static final String PERSISTED = "persisted";
        static final String ABORTED = "aborted";
        static final String FAILED = "failed";
        static final String COMPLETED = "completed";
    }
}
