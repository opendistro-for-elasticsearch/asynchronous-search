package com.amazon.opendistroforelasticsearch.search.async.stats.supplier;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Supplier for stats that need to keep count
 */
public class CounterSupplier implements Supplier<Long> {
    private AtomicLong counter;

    /**
     * Constructor
     */
    public CounterSupplier() {
        this.counter = new AtomicLong(0);
    }

    @Override
    public Long get() {
        return counter.longValue();
    }

    /**
     * Increments the value of the counter by 1
     */
    public void increment() {
        counter.getAndIncrement();
    }

    public void decrement() {
        counter.getAndDecrement();
    }
}
