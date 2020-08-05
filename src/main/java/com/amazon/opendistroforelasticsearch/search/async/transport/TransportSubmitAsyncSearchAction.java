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
import com.amazon.opendistroforelasticsearch.search.async.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

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
        super(SubmitAsyncSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<SubmitAsyncSearchRequest>) SubmitAsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchService = asyncSearchService;
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(Task task, SubmitAsyncSearchRequest submitAsyncSearchRequest, ActionListener<AsyncSearchResponse> listener) {
        try {
            AsyncSearchContext asyncSearchContext = asyncSearchService.createAndPutContext(submitAsyncSearchRequest);
            ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    submitAsyncSearchRequest.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener, () -> {
                //Replace with actual async search response
                listener.onResponse(null);
            }, asyncSearchContext::removeListener);
            asyncSearchContext.addListener(wrappedListener);
            AsyncSearchProgressActionListener progressActionListener = new AsyncSearchProgressActionListener(asyncSearchContext);
            logger.info("Bootstrapping async search progress action listener {}", progressActionListener);
            ((AsyncSearchTask)task).setProgressListener(progressActionListener);
            transportSearchAction.execute(task, submitAsyncSearchRequest.getSearchRequest(), progressActionListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
