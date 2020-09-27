package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/***
 * The implementation of {@link SearchProgressActionListener} responsible for maintaining a list of {@link PrioritizedListener}
 * to be invoked when a full response is available. The implementation guarantees that the listener once added will exactly be
 * invoked once. If the search completes before the listener was added,
 **/

public abstract class CompositeSearchProgressActionListener extends SearchProgressActionListener {

    private final List<ActionListener<AsyncSearchResponse>> actionListeners;
    private volatile boolean complete;
    private final Function<SearchResponse, AsyncSearchResponse> asyncSearchFunction;
    private final Consumer<Exception> exceptionConsumer;

    private final Logger logger = LogManager.getLogger(getClass());

    CompositeSearchProgressActionListener(Function<SearchResponse, AsyncSearchResponse> asyncSearchFunction,
                                          Consumer<Exception> exceptionConsumer) {
        this.asyncSearchFunction = asyncSearchFunction;
        this.exceptionConsumer = exceptionConsumer;
        this.actionListeners = new ArrayList<>(1);
    }

    /***
     * Adds a prioritized listener to listen on to the progress of the search started by a previous request. If the search
     * has completed the timeout consumer is immediately invoked.
     * @param listener the listener
     */
    public void addOrExecuteListener(PrioritizedListener<AsyncSearchResponse> listener) {
        if (addListener(listener) == false) {
            listener.executeImmediately();
        }
    }

    public synchronized void removeListener(ActionListener<AsyncSearchResponse> listener) {
        this.actionListeners.remove(listener);
    }

    private synchronized boolean addListener(PrioritizedListener<AsyncSearchResponse> listener) {
        if (complete == false) {
            this.actionListeners.add(listener);
            return true;
        }
        return false;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        AsyncSearchResponse asyncSearchResponse = null;
        try {
            asyncSearchResponse = asyncSearchFunction.apply(searchResponse);
        } finally {
            List<ActionListener<AsyncSearchResponse>> actionListenersToBeInvoked = finalizeListeners();
            if (actionListenersToBeInvoked != null) {
                for (ActionListener<AsyncSearchResponse> listener : actionListenersToBeInvoked) {
                    try {
                        logger.debug("Search response completed");
                        listener.onResponse(asyncSearchResponse);
                    } catch (Exception e) {
                        logger.warn(() -> new ParameterizedMessage("onResponse listener [{}] failed", listener), e);
                    }
                }
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            exceptionConsumer.accept(e);
        } finally {
            List<ActionListener<AsyncSearchResponse>> actionListenersToBeInvoked = finalizeListeners();
            if (actionListenersToBeInvoked != null) {
                for (ActionListener<AsyncSearchResponse> listener : actionListenersToBeInvoked) {
                    try {
                        logger.info("Search response failure", e);
                        listener.onFailure(e);
                    } catch (Exception ex) {
                        logger.warn(() -> new ParameterizedMessage("onFailure listener [{}] failed", listener), e);
                    }
                }
            }
        }
    }

    private List<ActionListener<AsyncSearchResponse>> finalizeListeners() {
        List<ActionListener<AsyncSearchResponse>> actionListenersToBeInvoked = null;
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
