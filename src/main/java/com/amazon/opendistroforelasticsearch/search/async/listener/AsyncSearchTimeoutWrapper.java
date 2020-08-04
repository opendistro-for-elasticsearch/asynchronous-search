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

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;


public class AsyncSearchTimeoutWrapper {

    public static ActionListener<AsyncSearchResponse> wrapScheduledTimeout(ThreadPool threadPool, TimeValue timeout, String executor,
                                                                           ActionListener<AsyncSearchResponse> actionListener,
                                                                           TimeoutListener timeoutListener) {
        CompletionTimeoutListener completionTimeoutListener = new CompletionTimeoutListener(actionListener, timeoutListener);
        completionTimeoutListener.cancellable = threadPool.schedule(completionTimeoutListener, timeout, executor);
        return completionTimeoutListener;
    }

    static class CompletionTimeoutListener implements ActionListener<AsyncSearchResponse>, Runnable {
        private final ActionListener<AsyncSearchResponse> actionListener;
        private final TimeoutListener timeoutListener;
        private volatile Scheduler.ScheduledCancellable cancellable;
        private final AtomicBoolean complete = new AtomicBoolean(false);

        public CompletionTimeoutListener(ActionListener<AsyncSearchResponse> actionListener, TimeoutListener timeoutListener) {
            this.actionListener = actionListener;
            this.timeoutListener = timeoutListener;
        }

        boolean cancel() {
            if (complete.compareAndSet(false, true)) {
                if (cancellable != null) {
                    return cancellable.cancel();
                }
            }
            return false;
        }

        @Override
        public void run() {
            if (complete.compareAndSet(false, true)) {
                if (cancellable != null && cancellable.isCancelled()) {
                    return;
                }
                timeoutListener.onTimeout();
            }
        }

        @Override
        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
            if (cancel()) {
                actionListener.onResponse(asyncSearchResponse);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (cancel()) {
                actionListener.onFailure(e);
            }
        }
    }
}
