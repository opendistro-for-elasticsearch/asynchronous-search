package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.request.FetchAsyncSearchRequest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

/**
 * Base class for the action to be executed to route request to the coordinator running the async search
 */
public abstract class TransportAsyncSearchFetchAction<Request extends FetchAsyncSearchRequest<Request>, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {

    private TransportService transportService;
    private AsyncSearchService asyncSearchService;
    private ClusterService clusterService;
    private Writeable.Reader<Response> responseReader;
    private String actionName;

    public TransportAsyncSearchFetchAction(TransportService transportService, ClusterService clusterService,
                                           AsyncSearchService asyncSearchService, String actionName, ActionFilters actionFilters,
                                           Writeable.Reader<Request> requestReader, Writeable.Reader<Response> responseReader) {
        super(actionName, transportService, actionFilters, requestReader);
        this.transportService = transportService;
        this.asyncSearchService = asyncSearchService;
        this.clusterService = clusterService;
        this.responseReader = responseReader;
        this.actionName = actionName;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try {
            AsyncSearchId asyncSearchId = AsyncSearchId.parseAsyncId(request.getId());
            if (!clusterService.localNode().getId().equals(asyncSearchId.getNode())) {
                forwardRequest(clusterService.state().getNodes().get(asyncSearchId.getNode()), request, listener, responseReader, actionName);
            }
            handleRequest(asyncSearchId, request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    void forwardRequest(DiscoveryNode discoveryNode, Request request, ActionListener<Response> listener,
                        Writeable.Reader<Response> responseReader, String actionName) {
        transportService.sendRequest(discoveryNode, actionName, request,
                new ActionListenerResponseHandler<Response>(listener, responseReader) {
                    @Override
                    public void handleException(final TransportException exp) {
                        Throwable cause = exp.unwrapCause();
                        if (cause instanceof ConnectTransportException ||
                                (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                            // we want to retry here a bit to see if a new master is elected
                            //TODO add retries
                        } else {
                            listener.onFailure(exp);
                        }
                    }
                });
    }

    public abstract void handleRequest(AsyncSearchId asyncSearchId, Request request, ActionListener<Response> listener);
}
