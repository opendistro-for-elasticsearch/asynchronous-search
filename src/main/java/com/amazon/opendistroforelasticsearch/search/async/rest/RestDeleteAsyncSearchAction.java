package com.amazon.opendistroforelasticsearch.search.async.rest;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteAsyncSearchAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "delete_async_search";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(DELETE, "/_async_search/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteAsyncSearchRequest deleteRequest = new DeleteAsyncSearchRequest(request.param("id"));
        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            //AcknowledgedResponse doesn't implement StatusToXContentObject. Hence not using RestStatusToXContentListener
            cancelClient.execute(DeleteAsyncSearchAction.INSTANCE, deleteRequest, new RestToXContentListener<>(channel));
        };
    }
}