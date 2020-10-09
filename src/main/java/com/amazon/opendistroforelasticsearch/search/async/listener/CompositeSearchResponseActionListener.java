package com.amazon.opendistroforelasticsearch.search.async.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/***
 * The implementation of {@link SearchProgressActionListener} responsible for maintaining a list of {@link PrioritizedActionListener}
 * to be invoked when a full response is available. The implementation guarantees that the listener once added will exactly be
 * invoked once. If the search completes before the listener was added,
 **/

public class CompositeSearchResponseActionListener<T> extends SearchProgressActionListener {

    private final List<ActionListener<T>> actionListeners;
    private final BiConsumer<SearchResponse, ActionListener<T>> consumer;
    private final Consumer<Exception> onFailure;
    private final Executor executor;
    private boolean complete;

    private final Logger logger = LogManager.getLogger(getClass());

    CompositeSearchResponseActionListener(BiConsumer<SearchResponse, ActionListener<T>> consumer, Consumer<Exception> onFailure,
                                          Executor executor) {
        this.consumer = consumer;
        this.executor = executor;
        this.onFailure = onFailure;
        this.actionListeners = new ArrayList<>(1);
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
        //immediately fork to a separate thread pool
        executor.execute(() -> {
            List<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
            consumer.accept(searchResponse, ActionListener.wrap((T result) -> {
                if (actionListenersToBeInvoked != null) {
                    for (ActionListener<T> listener : actionListenersToBeInvoked) {
                        logger.debug("Search response completed");
                        listener.onResponse(result);
                    }
                }
            }, e -> {
                if (actionListenersToBeInvoked != null) {
                    for (ActionListener<T> listener : actionListenersToBeInvoked) {
                        logger.debug("Search response failed");
                        listener.onFailure(e);
                    }
                }
            }));


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
                            logger.info("Search response failure", e);
                            listener.onFailure(e);
                        } catch (Exception ex) {
                            logger.warn(() -> new ParameterizedMessage("onFailure listener [{}] failed", listener), e);
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
}
