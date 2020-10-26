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

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchCleanUpAction;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchCleanUpRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportAsyncSearchCleanUpAction extends HandledTransportAction<AsyncSearchCleanUpRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAsyncSearchCleanUpAction.class);

    private final AsyncSearchPersistenceService persistenceService;
    private final ThreadPool threadPool;

    @Inject
    public TransportAsyncSearchCleanUpAction(TransportService transportService, ThreadPool threadPool,
                                             ActionFilters actionFilters, AsyncSearchPersistenceService persistenceService) {
        super(AsyncSearchCleanUpAction.NAME, transportService, actionFilters, AsyncSearchCleanUpRequest::new, ThreadPool.Names.MANAGEMENT);
        this.persistenceService = persistenceService;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, AsyncSearchCleanUpRequest request, ActionListener<AcknowledgedResponse> listener) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            persistenceService.deleteExpiredResponses(listener, request.getAbsoluteTimeInMillis());
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to execute clean up for request ()", request), e);
            listener.onFailure(e);
        }
    }
}
