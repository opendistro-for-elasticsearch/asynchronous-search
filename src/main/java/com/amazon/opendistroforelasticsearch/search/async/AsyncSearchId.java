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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.RAMOutputStream;

import java.io.IOException;
import java.util.Base64;

public class AsyncSearchId {

    // UUID + ID generator for uniqueness
    private final AsyncSearchContextId asyncSearchContextId;
    // coordinator node id
    private final String node;

    public AsyncSearchId(String node, AsyncSearchContextId asyncSearchContextId) {
        this.node = node;
        this.asyncSearchContextId = asyncSearchContextId;
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public String getNode() {
        return node;
    }

    public static String buildAsyncId(AsyncSearchId asyncSearchId) {
        try (RAMOutputStream out = new RAMOutputStream()) {
            out.writeString(asyncSearchId.getNode());
            out.writeString(asyncSearchId.getAsyncSearchContextId().getContextId());
            out.writeLong(asyncSearchId.getAsyncSearchContextId().getId());
            byte[] bytes = new byte[(int) out.getFilePointer()];
            out.writeTo(bytes, 0);
            return Base64.getUrlEncoder().encodeToString(bytes);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse async search id", e);
        }
    }

    public static AsyncSearchId parseAsyncId(String asyncSearchId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(asyncSearchId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            String node = in.readString();
            String contextId = in.readString();
            long id = in.readLong();
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new AsyncSearchId(node, new AsyncSearchContextId(contextId, id));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse async search id", e);
        }
    }
}
