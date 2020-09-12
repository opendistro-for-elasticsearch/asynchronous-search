package com.amazon.opendistroforelasticsearch.search.async.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;

public class PersistentTaskParamsImpl implements PersistentTaskParams {

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}

