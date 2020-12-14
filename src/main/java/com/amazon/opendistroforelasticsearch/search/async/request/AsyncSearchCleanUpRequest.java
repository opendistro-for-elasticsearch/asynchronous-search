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

package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class AsyncSearchCleanUpRequest extends ActionRequest {

    private final long absoluteTimeInMillis;

    public AsyncSearchCleanUpRequest(long absoluteTimeInMillis) {
        this.absoluteTimeInMillis = absoluteTimeInMillis;
    }

    public AsyncSearchCleanUpRequest(StreamInput in) throws IOException {
        super(in);
        this.absoluteTimeInMillis = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(absoluteTimeInMillis);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * The reason for deleting expired async searches.
     */
    public long getAbsoluteTimeInMillis() {
        return absoluteTimeInMillis;
    }


    @Override
    public int hashCode() {
        return Objects.hash(absoluteTimeInMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AsyncSearchCleanUpRequest asyncSearchCleanUpRequest = (AsyncSearchCleanUpRequest) o;
        return absoluteTimeInMillis == asyncSearchCleanUpRequest.absoluteTimeInMillis;
    }

    @Override
    public String toString() {
        return "[expirationTimeMillis] : " + absoluteTimeInMillis;
    }
}
