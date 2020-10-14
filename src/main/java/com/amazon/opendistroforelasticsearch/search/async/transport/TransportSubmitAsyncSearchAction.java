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

import com.amazon.opendistroforelasticsearch.search.async.ActiveAsyncSearchContext;
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
        try {
            final long relativeStartMillis = System.currentTimeMillis();
            ActiveAsyncSearchContext asyncSearchContext = asyncSearchService.prepareContext(request, relativeStartMillis);
            assert asyncSearchContext.getSearchProgressActionListener().isPresent() : "missing progress listener for an active context";
            AsyncSearchProgressListener progressActionListener = (AsyncSearchProgressListener) asyncSearchContext.getSearchProgressActionListener().get();
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
                    asyncSearchService.prepareSearch(asyncSearchTask, asyncSearchContext.getAsyncSearchContextId());
                    return asyncSearchTask;
                }
            }, progressActionListener);
            asyncSearchContext.setStage(ActiveAsyncSearchContext.Stage.RUNNING);
        } catch (Exception e) {
            logger.warn("Failed to submit async search request {}", request);
            listener.onFailure(e);
        }
    }
}