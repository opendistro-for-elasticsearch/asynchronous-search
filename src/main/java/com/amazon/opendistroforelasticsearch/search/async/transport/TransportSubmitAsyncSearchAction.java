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

package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Submits an async search request by executing a {@link TransportSearchAction} on an {@link AsyncSearchTask} with
 * a {@link AsyncSearchProgressListener} set on the task. The listener is wrapped with a completion timeout wrapper via
 * {@link AsyncSearchTimeoutWrapper} which ensures that exactly one of action listener or the timeout listener gets executed
 */
public class TransportSubmitAsyncSearchAction extends HandledTransportAction<SubmitAsyncSearchRequest, AsyncSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSubmitAsyncSearchAction.class);
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportSubmitAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, AsyncSearchService asyncSearchService, TransportSearchAction transportSearchAction) {
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, SubmitAsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        AtomicReference<Runnable> advanceStage = new AtomicReference<>();
        try {
            final long relativeStartTimeInMillis = threadPool.relativeTimeInMillis();
            AsyncSearchContext asyncSearchContext = asyncSearchService.createAndStoreContext(request.getKeepAlive(), request.keepOnCompletion(), relativeStartTimeInMillis);
            Objects.requireNonNull(asyncSearchContext.getSearchProgressActionListener(), "missing progress listener for an active context");
            AsyncSearchProgressListener progressActionListener = (AsyncSearchProgressListener) asyncSearchContext.getSearchProgressActionListener();
            //set the parent task as the submit task for cancellation on connection close
            request.getSearchRequest().setParentTask(task.taskInfo(clusterService.localNode().getId(), false).getTaskId());
            transportSearchAction.execute(new SearchRequest(request.getSearchRequest()) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    AsyncSearchTask asyncSearchTask = new AsyncSearchTask(id, type, AsyncSearchTask.NAME,
                            parentTaskId, headers, asyncSearchContext.getAsyncSearchContextId(),
                            asyncSearchService::onCancelled);
                    AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool, request.getWaitForCompletionTimeout(),
                            AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, listener, (actionListener) -> {
                                progressActionListener.removeListener(actionListener);
                                listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                            });
                    advanceStage.set(asyncSearchService.preProcessSearch(asyncSearchTask, asyncSearchContext.getAsyncSearchContextId()));
                    return asyncSearchTask;
                }
            }, progressActionListener);
            //see if there is a hook to move the Stage to RUNNING once the search execution starts
            advanceStage.get().run();
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to submit async search request {}", request, e));
            listener.onFailure(e);
        }
    }
}
