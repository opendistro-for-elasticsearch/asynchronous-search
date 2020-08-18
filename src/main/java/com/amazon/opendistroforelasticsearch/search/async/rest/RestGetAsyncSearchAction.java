package com.amazon.opendistroforelasticsearch.search.async.rest;

import com.amazon.opendistroforelasticsearch.search.async.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetAsyncSearchAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_async_search";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(GET, "/_async_search/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetAsyncSearchRequest getRequest = new GetAsyncSearchRequest(request.param("id"));
        if (request.hasParam("wait_for_completion_timeout")) {
            getRequest.setWaitForCompletion(request.paramAsTime("wait_for_completion_timeout", GetAsyncSearchRequest.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT));
        }
        if (request.hasParam("keep_alive")) {
            getRequest.setKeepAlive(request.paramAsTime("keep_alive", GetAsyncSearchRequest.DEFAULT_KEEP_ALIVE));
        }
        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(GetAsyncSearchAction.INSTANCE, getRequest, new RestStatusToXContentListener<>(channel));
        };
    }
}
