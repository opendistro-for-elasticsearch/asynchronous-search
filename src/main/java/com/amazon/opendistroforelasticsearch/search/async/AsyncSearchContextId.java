package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class AsyncSearchContextId implements Writeable {

    private long id;
    private String contextId;

    AsyncSearchContextId(String contextId, long id) {
        this.id = id;
        this.contextId = contextId;
    }

    public AsyncSearchContextId(StreamInput in) throws IOException {
        this.id = in.readLong();
        this.contextId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        out.writeString(contextId);
    }

    public String getContextId() {
        return contextId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AsyncSearchContextId other = (AsyncSearchContextId) o;
        return id == other.id && contextId.equals(other.contextId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextId, id);
    }

    @Override
    public String toString() {
        return "[" + contextId + "][" + id + "]";
    }

}
