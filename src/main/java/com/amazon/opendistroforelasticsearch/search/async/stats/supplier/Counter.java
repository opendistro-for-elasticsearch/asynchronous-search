package com.amazon.opendistroforelasticsearch.search.async.stats.supplier;

import org.elasticsearch.common.metrics.CounterMetric;

import java.util.function.Supplier;

/**
 * Supplier for stats that need to keep count
 */
public class Counter implements Supplier<Long> {
    private CounterMetric counter;

    /**
     * Constructor
     */
    public Counter() {
        this.counter = new CounterMetric();
    }

    @Override
    public Long get() {
        return counter.count();
    }

    /**
     * Increments the value of the counter by 1
     */
    public void increment() {
        counter.inc();
    }

    public void decrement() {
        counter.dec();
    }
}

