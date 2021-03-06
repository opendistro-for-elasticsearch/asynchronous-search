
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

package com.amazon.opendistroforelasticsearch.search.asynchronous.commons;

import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Function;

public abstract class AsynchronousSearchTestCase extends ESTestCase {

    public static AsynchronousSearchProgressListener  mockAsynchronousSearchProgressListener(ThreadPool threadPool) {
        return new AsynchronousSearchProgressListener(threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(),
                threadPool::relativeTimeInMillis, () -> getReduceContextBuilder());
    }

    public static AsynchronousSearchProgressListener mockAsynchronousSearchProgressListener(ThreadPool threadPool,
                                                                       Function<SearchResponse, AsynchronousSearchResponse> successFunction,
                                                                       Function<Exception, AsynchronousSearchResponse> failureFunction) {
        return new AsynchronousSearchProgressListener(threadPool.absoluteTimeInMillis(), successFunction, failureFunction,
                threadPool.generic(),
                threadPool::relativeTimeInMillis, () -> getReduceContextBuilder());
    }

    private static InternalAggregation.ReduceContextBuilder getReduceContextBuilder() {
        return new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(BigArrays.NON_RECYCLING_INSTANCE, null,
                        null);
            }

            public InternalAggregation.ReduceContext forFinalReduction() {
                return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, b -> {}, PipelineAggregator.PipelineTree.EMPTY);
            }};
    }
}
