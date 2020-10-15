package com.amazon.opendistroforelasticsearch.search.async.stats;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.metrics.CounterMetric;

public class InternalAsyncSearchStats implements AsyncSearchContextListener {

    private final CountStatsHolder countStatsHolder = new CountStatsHolder();

    @Override
    public void onContextFailed(AsyncSearchContextId contextId) {
        countStatsHolder.failedAsyncSearchCount.inc();
        countStatsHolder.runningAsyncSearchCount.dec();
    }

    @Override
    public void onContextPersisted(AsyncSearchContextId asyncSearchContextId) {
        countStatsHolder.persistedAsyncSearchCount.inc();
    }

    @Override
    public void onContextRunning(AsyncSearchContextId context) {
        countStatsHolder.runningAsyncSearchCount.inc();
    }

    @Override
    public void onContextCompleted(AsyncSearchContextId context) {
        countStatsHolder.completedAsyncSearchCount.inc();
        countStatsHolder.runningAsyncSearchCount.dec();
    }

    @Override
    public void onContextCancelled(AsyncSearchContextId context) {
        countStatsHolder.abortedAsyncSearchCount.inc();
    }

    public AsyncSearchStats stats(boolean count, DiscoveryNode node) {
        return new AsyncSearchStats(node, countStatsHolder.countStats());
    }

    static final class CountStatsHolder {
        final CounterMetric runningAsyncSearchCount = new CounterMetric();
        final CounterMetric persistedAsyncSearchCount = new CounterMetric();
        final CounterMetric abortedAsyncSearchCount = new CounterMetric();
        final CounterMetric failedAsyncSearchCount = new CounterMetric();
        final CounterMetric completedAsyncSearchCount = new CounterMetric();

        public AsyncSearchStatusStats countStats() {
            return new AsyncSearchStatusStats(runningAsyncSearchCount.count(), abortedAsyncSearchCount.count(),
                    persistedAsyncSearchCount.count(),
                    completedAsyncSearchCount.count(), failedAsyncSearchCount.count());
        }
    }
}
