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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/***
 * The implementation of {@link SearchProgressActionListener} responsible for maintaining a list of {@link PrioritizedActionListener}
 * to be invoked when a full response is available. The implementation guarantees that the listener once added will exactly be
 * invoked once. If the search completes before the listener was added,
 **/

public abstract class CompositeSearchProgressActionListener<T> extends SearchProgressActionListener {

    private final List<ActionListener<T>> actionListeners;
    private final CheckedFunction<SearchResponse, T, IOException> responseFunction;
    private final CheckedFunction<Exception, T, IOException> failureFunction;
    private final ThreadPool threadPool;
    private volatile boolean complete;

    private final Logger logger = LogManager.getLogger(getClass());

    CompositeSearchProgressActionListener(CheckedFunction<SearchResponse, T, IOException> responseFunction,
                                          CheckedFunction<Exception, T, IOException> failureFunction,
                                          ThreadPool threadPool) {
        this.responseFunction = responseFunction;
        this.threadPool = threadPool;
        this.failureFunction = failureFunction;
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
        threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME).execute(() -> {
        T result = null;
            List<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
            if (actionListenersToBeInvoked != null) {
                try {
                    assertResponse(searchResponse);
                    result = responseFunction.apply(searchResponse);
                } catch (Exception ex) {
                    for (ActionListener<T> listener : actionListenersToBeInvoked) {
                        try {
                            listener.onFailure(ex);
                        } catch (Exception e) {
                            logger.error(() -> new ParameterizedMessage("search response on failure listener [{}] failed", listener), e);
                        }
                    }
                }
                for (ActionListener<T> listener : actionListenersToBeInvoked) {
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
                        try {
                            listener.onFailure(e);
                        } catch (Exception ex) {
                            logger.error(() -> new ParameterizedMessage("search response on failure listener [{}] failed", listener), ex);
                        }
                    }
                }
            }
        });
    }

    @Override
    public void onFailure(Exception exception) {
        //immediately fork to a separate thread pool
        threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME).execute(() -> {
            T result = null;
            List<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
            if (actionListenersToBeInvoked != null) {
                try {
                    result = failureFunction.apply(exception);
                } catch (Exception ex) {
                    for (ActionListener<T> listener : actionListenersToBeInvoked) {
                        try {
                            listener.onFailure(ex);
                        } catch (Exception e) {
                            logger.error(() -> new ParameterizedMessage("search response on failure listener [{}] failed", listener), e);
                        }
                    }
                }
                for (ActionListener<T> listener : actionListenersToBeInvoked) {
                    try {
                        listener.onResponse(result);
                    } catch (Exception e) {
                        try {
                            listener.onFailure(e);
                        } catch (Exception ex) {
                            logger.error(() -> new ParameterizedMessage("search response on failure listener [{}] failed", listener), ex);
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

    protected abstract void assertResponse(SearchResponse searchResponse);
}
