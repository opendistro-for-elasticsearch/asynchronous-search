package com.amazon.opendistroforelasticsearch.search.async.stats;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchCountStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.metrics.CounterMetric;

public class AsyncSearchStatsListener implements AsyncSearchContextListener {

    private final CountStatsHolder countStatsHolder = new CountStatsHolder();

    @Override
    public void onContextFailed(AsyncSearchContextId contextId) {
        countStatsHolder.failedAsyncSearchCount.inc();
        if(countStatsHolder.runningAsyncSearchCount.count()>0) {
            countStatsHolder.runningAsyncSearchCount.dec();
        }
    }

    @Override
    public void onContextPersisted(AsyncSearchContextId asyncSearchContextId) {
        countStatsHolder.persistedAsyncSearchCount.inc();
    }

    @Override
    public void onNewContext(AsyncSearchContextId context) {
        countStatsHolder.runningAsyncSearchCount.inc();
    }

    @Override
    public void onFreeContext(AsyncSearchContextId context) {

    }

    @Override
    public void onContextCompleted(AsyncSearchContextId context) {
        countStatsHolder.completedAsyncSearchCount.inc();
        if(countStatsHolder.runningAsyncSearchCount.count()>0) {
            countStatsHolder.runningAsyncSearchCount.dec();
        }
    }

    @Override
    public void onContextCancelled(AsyncSearchContextId context) {
        countStatsHolder.abortedAsyncSearchCount.inc();
        if(countStatsHolder.runningAsyncSearchCount.count()>0) {
            countStatsHolder.runningAsyncSearchCount.dec();
        }
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

        public AsyncSearchCountStats countStats() {
            return new AsyncSearchCountStats(runningAsyncSearchCount.count(), abortedAsyncSearchCount.count(),
                    persistedAsyncSearchCount.count(),
                    completedAsyncSearchCount.count(), failedAsyncSearchCount.count());
        }
    }
}
