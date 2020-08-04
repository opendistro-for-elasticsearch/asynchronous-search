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

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchStateManager;
import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchAction;
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

public class TransportAsyncSearchAction extends HandledTransportAction<AsyncSearchRequest, AsyncSearchResponse> {

    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportSearchAction transportSearchAction;
    private final AsyncSearchStateManager asyncSearchStateManager;

    @Inject
    public TransportAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, AsyncSearchStateManager asyncSearchStateManager,
                                      TransportSearchAction transportSearchAction) {
        super(AsyncSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<AsyncSearchRequest>) AsyncSearchRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.asyncSearchStateManager = asyncSearchStateManager;
        this.transportSearchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(Task task, AsyncSearchRequest asyncSearchRequest, ActionListener<AsyncSearchResponse> listener) {
        try {
            ActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    asyncSearchRequest.getWaitForCompletionTimeout(), ThreadPool.Names.GENERIC, listener, () -> {
                //Replace with actual async search response
                listener.onResponse(null);
            });
            AsyncSearchProgressActionListener progressActionListener = new AsyncSearchProgressActionListener(wrappedListener);
            logger.info("Bootstrapping async search progress action listener {}", progressActionListener);
            ((AsyncSearchTask)task).setProgressListener(progressActionListener);
            transportSearchAction.execute(task, asyncSearchRequest, progressActionListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
