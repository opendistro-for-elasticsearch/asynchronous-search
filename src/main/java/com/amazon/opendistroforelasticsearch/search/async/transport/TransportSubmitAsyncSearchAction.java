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
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.CompositeAsyncSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest request, ActionListener<AsyncSearchResponse> listener) {
        AtomicReference<SearchTask> searchTask = new AtomicReference<>();
        try {
            AsyncSearchContext asyncSearchContext = asyncSearchService.createAndPutContext(request, searchTask);
            CompositeAsyncSearchProgressActionListener progressActionListener = new CompositeAsyncSearchProgressActionListener(asyncSearchContext);
            request.getSearchRequest().setParentTask(task.taskInfo(clusterService.localNode().getId(), false).getTaskId());

            logger.debug("Initiated sync search request {}", asyncSearchContext.getId());

            ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    request.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener, (actionListener) -> {
                        logger.info("Timeout triggered for async search");
                        if (asyncSearchContext.isCancelled()) {
                            listener.onFailure(new ResourceNotFoundException("Search cancelled"));
                        }
                        try {
                            listener.onResponse(asyncSearchContext.getAsyncSearchResponse());
                        } catch (IOException e) {
                            listener.onFailure(e);
                        }
                        progressActionListener.removeListener(actionListener);
                    });
            progressActionListener.addListener(wrappedListener);
            transportSearchAction.execute(new SearchRequest(request.getSearchRequest()) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                    searchTask.set(task);
                    task.setProgressListener(progressActionListener);
                    return task;
                }
            }, progressActionListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}