package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.TransportDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.TransportGetAsyncSearchAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.*;

/**
 * Base class for common login between {@link TransportGetAsyncSearchAction} and {@link TransportDeleteAsyncSearchAction}
 */
abstract class AbstractAsyncSearchAction<Request extends TransportRequest, Response extends TransportResponse>{

    private TransportService transportService;
    private AsyncSearchService asyncSearchService;

    public AbstractAsyncSearchAction(TransportService transportService, AsyncSearchService asyncSearchService) {
        this.transportService = transportService;
        this.asyncSearchService = asyncSearchService;
    }

    void forwardRequest(DiscoveryNode discoveryNode, Request request, ActionListener<Response> listener,
                        Writeable.Reader<Response> reader, String actionName) {
        transportService.sendRequest(discoveryNode, actionName, request,
                new ActionListenerResponseHandler<Response>(listener, reader) {
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
