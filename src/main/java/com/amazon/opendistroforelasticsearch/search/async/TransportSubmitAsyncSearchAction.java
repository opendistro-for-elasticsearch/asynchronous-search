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

package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.listener.TaskUnregisterWrapper;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static java.util.Arrays.asList;

public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {

    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportSearchAction transportSearchAction;
    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportSubmitAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                            AsyncSearchService asyncSearchService, TransportSearchAction transportSearchAction) {
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, SubmitAsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
    }

    /**
     * @param task Since RestCancellableNodeClient is used, the onResponse() event will unregister this task on final response or timeout,
     *            whichever causes the channel to close. Hence the synchronous SearchAction executed needs a task which we can hold onto
     *             to monitor progress, listen on SPAL events and cancel if required. We require it to remain registered.
     */
    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        try {
            //For cancellation of child task, simply setting parent task id won't suffice.
            //It also requires registering node on which child task (transport search action) will be executed with parent task id in the task manager.
            taskManager.registerChildNode(request.getParentTask().getId(), clusterService.localNode());
            request.getSearchRequest().setParentTask(task.taskInfo(clusterService.localNode().getId(), false).getTaskId());
            SearchTask searchTask =  (SearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), request.getSearchRequest());

            final SearchTimeProvider timeProvider = new SearchTimeProvider(System.currentTimeMillis(), System.nanoTime(), System::nanoTime);
            AsyncSearchContext asyncSearchContext = asyncSearchService.createAndPutContext(request, searchTask, timeProvider);

            AsyncSearchProgressActionListener progressActionListener = new AsyncSearchProgressActionListener(asyncSearchContext,
                    () -> taskManager.unregister(searchTask));
            searchTask.setProgressListener(progressActionListener);
            logger.info("Bootstrapping async search progress action listener {}", progressActionListener);

            logger.info("Initiating sync search request");
            threadPool.executor(ThreadPool.Names.SEARCH).execute(
                    () -> transportSearchAction.execute(searchTask, request.getSearchRequest(), progressActionListener));

            ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    request.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener, (contextListener) -> {
                        onCompletion(listener, asyncSearchContext, contextListener);
                    });
            asyncSearchContext.addListener(wrappedListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void onCompletion(ActionListener<AsyncSearchResponse> listener, AsyncSearchContext asyncSearchContext,
                              ActionListener<AsyncSearchResponse> contextListener) {
        logger.info("Timeout triggered for async search");
        if(asyncSearchContext.isCancelled()) {
          listener.onFailure(new ResourceNotFoundException("Search cancelled"));
        }
        try {
            listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
        } catch (IOException e) {
            listener.onFailure(e);
        }
        asyncSearchContext.removeListener(contextListener);
    }

    /**
     * Search operations need two clocks. One clock is to fulfill real clock needs (e.g., resolving
     * "now" to an index name). Another clock is needed for measuring how long a search operation
     * took. These two uses are at odds with each other. There are many issues with using a real
     * clock for measuring how long an operation took (they often lack precision, they are subject
     * to moving backwards due to NTP and other such complexities, etc.). There are also issues with
     * using a relative clock for reporting real time. Thus, we simply separate these two uses.
     */
    static class SearchTimeProvider {

        private final long absoluteStartMillis;
        private final long relativeStartNanos;
        private final LongSupplier relativeCurrentNanosProvider;

        SearchTimeProvider(
                final long absoluteStartMillis,
                final long relativeStartNanos,
                final LongSupplier relativeCurrentNanosProvider) {
            this.absoluteStartMillis = absoluteStartMillis;
            this.relativeStartNanos = relativeStartNanos;
            this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
        }

        long getAbsoluteStartMillis() {
            return absoluteStartMillis;
        }

        long buildTookInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(relativeCurrentNanosProvider.getAsLong() - relativeStartNanos);
        }
    }
}
