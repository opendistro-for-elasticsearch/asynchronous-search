package com.amazon.opendistroforelasticsearch.search.async.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/***
 * The implementation of {@link SearchProgressActionListener} responsible for maintaining a list of {@link PrioritizedActionListener}
 * to be invoked when a full response is available. The implementation guarantees that the listener once added will exactly be
 * invoked once. If the search completes before the listener was added,
 **/

public abstract class CompositeSearchResponseActionListener<T> extends SearchProgressActionListener {

    private final List<ActionListener<T>> actionListeners;
    private final CheckedFunction<SearchResponse, T, Exception> function;
    private final Consumer<Exception> onFailure;
    private final Executor executor;
    private boolean complete;
    protected final PartialResultsHolder partialResultsHolder;

    private final Logger logger = LogManager.getLogger(getClass());

    CompositeSearchResponseActionListener(CheckedFunction<SearchResponse, T, Exception> function, Consumer<Exception> onFailure,
                                          Executor executor, long relativeStartMillis, LongSupplier currentTimeSupplier) {
        this.function = function;
        this.executor = executor;
        this.onFailure = onFailure;
        this.actionListeners = new ArrayList<>(1);
        this.partialResultsHolder = new PartialResultsHolder(relativeStartMillis, currentTimeSupplier);
    }

    /***
     * Adds a prioritized listener to listen on to the progress of the search started by a previous request. If the search
     * has completed the timeout consumer is immediately invoked.
     * @param listener the listener
     */
    public void addOrExecuteListener(PrioritizedActionListener<T> listener) {
        if (addListener(listener) == false) {
            listener.executeImmediately();
        }
    }

    public synchronized void removeListener(ActionListener<T> listener) {
        this.actionListeners.remove(listener);
    }

    private synchronized boolean addListener(PrioritizedActionListener<T> listener) {
        if (complete == false) {
            this.actionListeners.add(listener);
            return true;
        }
        return false;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        //assert partial results match actual results on search completion
//        assert partialResultsHolder.successfulShards.get() == searchResponse.getSuccessfulShards();
//        assert partialResultsHolder.reducePhase.get() == searchResponse.getNumReducePhases();
//        assert partialResultsHolder.clusters.get() == searchResponse.getClusters();
//        assert partialResultsHolder.shardSearchFailures.toArray(
//                new ShardSearchFailure[partialResultsHolder.failurePos.get()]) == searchResponse.getShardFailures();
//        assert partialResultsHolder.skippedShards.get() == searchResponse.getSkippedShards();
//        assert partialResultsHolder.totalShards.get() == searchResponse.getTotalShards();
//        assert partialResultsHolder.internalAggregations.get() == searchResponse.getAggregations();
//        assert partialResultsHolder.totalHits.get() == searchResponse.getHits().getTotalHits();

        //immediately fork to a separate thread pool
        executor.execute(() -> {
            T result;
            try {
                result = function.apply(searchResponse);
                List<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
                if (actionListenersToBeInvoked != null) {
                    for (ActionListener<T> listener : actionListenersToBeInvoked) {
                        try {
                            listener.onResponse(result);
                        } catch (Exception e) {
                            logger.error(() -> new ParameterizedMessage("onResponse listener [{}] failed", listener), e);
                            listener.onFailure(e);
                        }
                    }
                }
            } catch (Exception ex) {
                logger.error(() -> new ParameterizedMessage("onResponse listener [{}] failed"), ex);
            }
        });
    }

    @Override
    public void onFailure(Exception e) {
        //immediately fork to a separate thread pool
        executor.execute(() -> {
            try {
                onFailure.accept(e);
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("onFailure listener [{}] failed"), ex);
            } finally {
                List<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
                if (actionListenersToBeInvoked != null) {
                    for (ActionListener<T> listener : actionListenersToBeInvoked) {
                        try {
                            listener.onFailure(e);
                        } catch (Exception ex) {
                            logger.error(() -> new ParameterizedMessage("onFailure listener [{}] failed", listener), e);
                        }
                    }
                }
            }
        });
    }

    private List<ActionListener<T>> finalizeListeners() {
        List<ActionListener<T>> actionListenersToBeInvoked = null;
        synchronized (this) {
            if (complete == false) {
                actionListenersToBeInvoked = new ArrayList<>(actionListeners);
                actionListeners.clear();
                complete = true;
            }
        }
        return actionListenersToBeInvoked;
    }

    public abstract SearchResponse partialResponse();

     static class PartialResultsHolder {
        final AtomicInteger reducePhase;
        final AtomicReference<TotalHits> totalHits;
        final AtomicReference<InternalAggregations> internalAggregations;
        final AtomicReference<DelayableWriteable.Serialized<InternalAggregations>> delayedInternalAggregations;
        final AtomicBoolean isInitialized;
        final AtomicInteger totalShards;
        final AtomicInteger successfulShards;
        final AtomicInteger skippedShards;
        final AtomicReference<SearchResponse.Clusters> clusters;
        final AtomicReference<List<SearchShard>> shards;
        final AtomicArray<ShardSearchFailure> shardSearchFailures;
        final AtomicInteger failurePos;
        final AtomicBoolean hasFetchPhase;
        final long relativeStartMillis;
        final LongSupplier currentTimeSupplier;


        PartialResultsHolder(long relativeStartMillis, LongSupplier currentTimeSupplier) {
            this.internalAggregations = new AtomicReference<>();
            this.shardSearchFailures = new AtomicArray<>(1);
            this.totalShards = new AtomicInteger();
            this.successfulShards = new AtomicInteger();
            this.skippedShards = new AtomicInteger();
            this.reducePhase = new AtomicInteger();
            this.isInitialized = new AtomicBoolean(false);
            this.failurePos = new AtomicInteger(0);
            this.hasFetchPhase = new AtomicBoolean(false);
            this.totalHits = new AtomicReference<>();
            this.clusters = new AtomicReference<>();
            this.delayedInternalAggregations = new AtomicReference<>();
            this.relativeStartMillis = relativeStartMillis;
            this.shards = new AtomicReference<>();
            this.currentTimeSupplier = currentTimeSupplier;
        }
    }
}