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

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.util.Base64;
import java.util.Objects;

public class AsyncSearchId {

    // UUID + ID generator for uniqueness
    private final AsyncSearchContextId asyncSearchContextId;
    // coordinator node id
    private final String node;
    // the search task id
    private final long taskId;

    public AsyncSearchId(String node, long taskId, AsyncSearchContextId asyncSearchContextId) {
        this.node = node;
        this.taskId = taskId;
        this.asyncSearchContextId = asyncSearchContextId;
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public String getNode() {
        return node;
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        return "[" + node + "][" + taskId + "][" + asyncSearchContextId + "]";
    }

    public static String buildAsyncId(AsyncSearchId asyncSearchId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(asyncSearchId.getNode());
            out.writeLong(asyncSearchId.getTaskId());
            out.writeString(asyncSearchId.getAsyncSearchContextId().getContextId());
            out.writeLong(asyncSearchId.getAsyncSearchContextId().getId());
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot build async search id", e);
        }
    }

    public static AsyncSearchId parseAsyncId(String asyncSearchId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(asyncSearchId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            String node = in.readString();
            long taskId = in.readLong();
            String contextId = in.readString();
            long id = in.readLong();
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new AsyncSearchId(node, taskId, new AsyncSearchContextId(contextId, id));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse async search id", e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.asyncSearchContextId, this.node, this.taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsyncSearchId asyncSearchId = (AsyncSearchId) o;
        return asyncSearchId.asyncSearchContextId.equals(this.asyncSearchContextId)
                && asyncSearchId.node.equals(this.node)
                && asyncSearchId.taskId == this.taskId;
    }
}
