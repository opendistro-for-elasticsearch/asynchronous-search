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

package com.amazon.opendistroforelasticsearch.search.asynchronous.stats;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.metrics.CounterMetric;

public class InternalAsynchronousSearchStats implements AsynchronousSearchContextEventListener {

    private final CountStatsHolder countStatsHolder = new CountStatsHolder();

    @Override
    public void onContextFailed(AsynchronousSearchContextId contextId) {
        countStatsHolder.failedAsynchronousSearchCount.inc();
        countStatsHolder.runningAsynchronousSearchCount.dec();
    }

    @Override
    public void onContextPersisted(AsynchronousSearchContextId asynchronousSearchContextId) {
        countStatsHolder.persistedAsynchronousSearchCount.inc();
    }

    @Override
    public void onContextPersistFailed(AsynchronousSearchContextId contextId) {
        countStatsHolder.persistFailedAsynchronousSearchCount.inc();
    }

    @Override
    public void onContextRunning(AsynchronousSearchContextId context) {
        countStatsHolder.runningAsynchronousSearchCount.inc();
    }

    @Override
    public void onContextRejected(AsynchronousSearchContextId contextId) {
        countStatsHolder.rejectedAsynchronousSearchCount.inc();
    }


    @Override
    public void onNewContext(AsynchronousSearchContextId contextId) {
        countStatsHolder.submittedAsynchronousSearchCount.inc();
    }

    @Override
    public void onContextCancelled(AsynchronousSearchContextId contextId) {
        countStatsHolder.cancelledAsynchronousSearchCount.inc();
    }

    @Override
    public void onContextInitialized(AsynchronousSearchContextId contextId) {
        countStatsHolder.initializedAsynchronousSearchCount.inc();
    }

    @Override
    public void onRunningContextDeleted(AsynchronousSearchContextId contextId) {
        countStatsHolder.runningAsynchronousSearchCount.dec();
    }

    @Override
    public void onContextCompleted(AsynchronousSearchContextId context) {
        countStatsHolder.completedAsynchronousSearchCount.inc();
        countStatsHolder.runningAsynchronousSearchCount.dec();
    }

    public AsynchronousSearchStats stats(DiscoveryNode node) {
        return new AsynchronousSearchStats(node, countStatsHolder.countStats());
    }

    static final class CountStatsHolder {
        final CounterMetric runningAsynchronousSearchCount = new CounterMetric();
        final CounterMetric persistedAsynchronousSearchCount = new CounterMetric();
        final CounterMetric persistFailedAsynchronousSearchCount = new CounterMetric();
        final CounterMetric failedAsynchronousSearchCount = new CounterMetric();
        final CounterMetric completedAsynchronousSearchCount = new CounterMetric();
        final CounterMetric rejectedAsynchronousSearchCount = new CounterMetric();
        final CounterMetric submittedAsynchronousSearchCount = new CounterMetric();
        final CounterMetric cancelledAsynchronousSearchCount = new CounterMetric();
        final CounterMetric initializedAsynchronousSearchCount = new CounterMetric();


        public AsynchronousSearchCountStats countStats() {
            return new AsynchronousSearchCountStats(runningAsynchronousSearchCount.count(), persistedAsynchronousSearchCount.count(),
                    completedAsynchronousSearchCount.count(), failedAsynchronousSearchCount.count(),
                    rejectedAsynchronousSearchCount.count(), persistFailedAsynchronousSearchCount.count(),
                    initializedAsynchronousSearchCount.count(), submittedAsynchronousSearchCount.count(),
                    cancelledAsynchronousSearchCount.count());
        }
    }
}
