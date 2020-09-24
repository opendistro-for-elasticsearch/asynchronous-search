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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class AsyncSearchTimeoutWrapper {

    private static final Logger logger = LogManager.getLogger(AsyncSearchTimeoutWrapper.class);

    public static <Response> ActionListener<Response> wrapScheduledTimeout(ThreadPool threadPool, TimeValue timeout, String executor,
                                                                           ActionListener<Response> actionListener,
                                                                           Consumer<ActionListener<Response>> timeoutConsumer) {
        CompletionPrioritizedListener<Response> completionTimeoutListener = new CompletionPrioritizedListener<>(actionListener, timeoutConsumer);
        scheduleTimeout(threadPool, timeout, executor, completionTimeoutListener);
        return completionTimeoutListener;
    }

    public static <Response> ActionListener<Response> wrapListener(ActionListener<Response> actionListener,  Consumer<ActionListener<Response>> timeoutConsumer) {
        CompletionPrioritizedListener<Response> completionTimeoutListener = new CompletionPrioritizedListener<>(actionListener, timeoutConsumer);
        return completionTimeoutListener;
    }

    public static <Response> ActionListener<Response> scheduleTimeout(ThreadPool threadPool, TimeValue timeout, String executor,
                                                                      CompletionPrioritizedListener<Response> completionTimeoutListener) {
        completionTimeoutListener.cancellable = threadPool.schedule(completionTimeoutListener, timeout, executor);
        return completionTimeoutListener;
    }

    public static class CompletionPrioritizedListener<Response> implements PrioritizedListener<Response>, Runnable {
        private final ActionListener<Response> actionListener;
        private volatile Scheduler.ScheduledCancellable cancellable;
        private final AtomicBoolean complete = new AtomicBoolean(false);
        private final Consumer<ActionListener<Response>> timeoutConsumer;

        CompletionPrioritizedListener(ActionListener<Response> actionListener, Consumer<ActionListener<Response>> timeoutConsumer) {
            this.actionListener = actionListener;
            this.timeoutConsumer = timeoutConsumer;
        }

        void cancel() {
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public void run() {
            if (complete.compareAndSet(false, true)) {
                timeoutConsumer.accept(this);
            }
        }

        @Override
        public void executeImmediately() {
            if (complete.compareAndSet(false, true)) {
                if (cancellable != null && cancellable.isCancelled() == false) {
                    cancel();
                    timeoutConsumer.accept(this);
                }
            }
        }

        @Override
        public void onResponse(Response response) {
            if (complete.compareAndSet(false, true)) {
                cancel();
                logger.info("Invoking onResponse after cancel");
                actionListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (complete.compareAndSet(false, true)) {
                cancel();
                actionListener.onFailure(e);
            }
        }
    }
}
