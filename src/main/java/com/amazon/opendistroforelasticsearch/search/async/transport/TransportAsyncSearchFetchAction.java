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

import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.request.FetchAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

/**
 * Base class for the action to be executed on the coordinator running the async search from the initial
 * {@link TransportSubmitAsyncSearchAction}. The class forwards the request to the coordinator and executes the
 * {@link TransportGetAsyncSearchAction} or the {@link TransportDeleteAsyncSearchAction}
 */
public abstract class TransportAsyncSearchFetchAction<Request extends FetchAsyncSearchRequest<Request>, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {

    private TransportService transportService;
    private AsyncSearchService asyncSearchService;
    private ClusterService clusterService;
    private Writeable.Reader<Response> responseReader;
    private String actionName;
    private ThreadPool threadPool;
    private Client client;

    public TransportAsyncSearchFetchAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                           Client client, String actionName, ActionFilters actionFilters,
                                           Writeable.Reader<Request> requestReader, Writeable.Reader<Response> responseReader) {
        super(actionName, transportService, actionFilters, requestReader);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.responseReader = responseReader;
        this.actionName = actionName;
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try {
            new AsyncForwardAction(request, listener).run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public abstract void handleRequest(AsyncSearchId asyncSearchId, Request request, ActionListener<Response> listener);

    class AsyncForwardAction extends AbstractRunnable {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterStateObserver observer;
        private DiscoveryNode targetNode;
        private AsyncSearchId asyncSearchId;

        AsyncForwardAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
            this.asyncSearchId = AsyncSearchIdConverter.parseAsyncId(request.getId());
            this.observer = new ClusterStateObserver(clusterService.state(), clusterService, request.connectionTimeout(),
                    logger, threadPool.getThreadContext());
            this.targetNode = clusterService.state().nodes().get(asyncSearchId.getNode());
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(new ParameterizedMessage("Failed to dispatch request for action {} ", actionName), e);
            handleRequest(asyncSearchId, request, listener);
        }

        @Override
        protected void doRun() {
            //should we look at the task status and retry on forwarding the request if the search is still RUNNING based
            //on task status
            ClusterState state = observer.setAndGetObservedState();
            // forward request only if the local node isn't the node coordinating the search and the node coordinating
            // the search exists in the cluster
            if (state.nodes().getLocalNode().equals(targetNode) == false && state.nodes().nodeExists(targetNode)) {
                transportService.sendRequest(targetNode, actionName, request,
                        new ActionListenerResponseHandler<Response>(listener, responseReader) {
                            @Override
                            public void handleException(final TransportException exp) {
                                Throwable cause = exp.unwrapCause();
                                if (cause instanceof ConnectTransportException ||
                                        (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                    // we want to retry here a bit to see if the node connects backs
                                    logger.debug("connection exception while trying to forward request with action name [{}] to " +
                                                    "target node [{}], scheduling a retry. Error: [{}]",
                                            actionName, targetNode, exp.getDetailedMessage());
                                    //should we re-try on this
                                }
                                // handle request locally if we were not able to forward the request
                                handleRequest(asyncSearchId, request, listener);
                            }
                        });
            } else {
                handleRequest(asyncSearchId, request, listener);
            }
        }
    }
}
