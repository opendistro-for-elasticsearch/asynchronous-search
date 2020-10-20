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

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchManagementAction;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchManagementRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportAsyncSearchManagementAction extends HandledTransportAction<AsyncSearchManagementRequest, AcknowledgedResponse> {

    private static final Logger log = LogManager.getLogger(TransportAsyncSearchManagementAction.class);
    private final AsyncSearchPersistenceService persistenceService;

    @Inject
    public TransportAsyncSearchManagementAction(TransportService transportService,
                                                ActionFilters actionFilters, AsyncSearchPersistenceService persistenceService) {
        super(AsyncSearchManagementAction.NAME, transportService, actionFilters, AsyncSearchManagementRequest::new, ThreadPool.Names.MANAGEMENT);
        this.persistenceService = persistenceService;
    }

    @Override
    protected void doExecute(Task task, AsyncSearchManagementRequest request, ActionListener<AcknowledgedResponse> listener) {
        persistenceService.deleteExpiredResponses(listener);
    }
}
