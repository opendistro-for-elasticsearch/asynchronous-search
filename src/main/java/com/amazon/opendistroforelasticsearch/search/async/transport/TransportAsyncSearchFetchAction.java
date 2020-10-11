package com.amazon.opendistroforelasticsearch.search.async.transport;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.request.FetchAsyncSearchRequest;

/**
 * Base class for the action to be executed on the coordinator running the async search from the initial
 * {@link TransportSubmitAsyncSearchAction}. The class forwards the request to the coordinator and executes the {@link TransportGetAsyncSearchAction}
 * or the {@link TransportDeleteAsyncSearchAction}
 */
public abstract class TransportAsyncSearchFetchAction<Request extends FetchAsyncSearchRequest<Request>, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {

    private TransportService transportService;
    private AsyncSearchService asyncSearchService;
    private ClusterService clusterService;
    private Writeable.Reader<Response> responseReader;
    private String actionName;
    private ThreadPool threadPool;

    public TransportAsyncSearchFetchAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                           String actionName, ActionFilters actionFilters,
                                           Writeable.Reader<Request> requestReader, Writeable.Reader<Response> responseReader) {
        super(actionName, transportService, actionFilters, requestReader);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.responseReader = responseReader;
        this.actionName = actionName;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try {
            new AsyncSingleAction(request, listener).run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public abstract void handleRequest(AsyncSearchId asyncSearchId, Request request, ActionListener<Response> listener);

    class AsyncSingleAction extends AbstractRunnable {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterStateObserver observer;
        private DiscoveryNode targetNode;
        private AsyncSearchId asyncSearchId;
        private final AtomicBoolean finished = new AtomicBoolean();

        AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
            this.asyncSearchId = AsyncSearchId.parseAsyncId(request.getId());
            this.observer = new ClusterStateObserver(clusterService.state(), clusterService, request.connectionTimeout(), logger, threadPool.getThreadContext());
            this.targetNode = clusterService.state().nodes().get(asyncSearchId.getNode());
        }

        @Override
        protected void doRun() {
            try {
                ClusterState state = observer.setAndGetObservedState();
                if (state.nodes().getLocalNode().equals(targetNode) == false) {
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
                                    retry(cause);
                                } else {
                                    listener.onFailure(exp);
                                }
                            }
                        });
                } else {
                    handleRequest(asyncSearchId, request, listener);
                }

            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retry(final Throwable failure) {
            observer.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        doRun();
                    }

                    @Override
                    public void onClusterServiceClose() {
                        onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.debug(() -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])",
                                actionName, timeout), failure);
                        // Try one more time...
                        run();
                    }
                }, request.connectionTimeout());
        }


        @Override
        public void onFailure(Exception e) {
            if (finished.compareAndSet(false, true)) {
                logger.trace(() -> new ParameterizedMessage("operation failed. action [{}], request [{}]", actionName, request), e);
                listener.onFailure(e);
            }
        }
    }
}
