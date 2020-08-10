package com.amazon.opendistroforelasticsearch.search.async.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

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
        return Arrays.asList(new Route(DELETE, "/_async_search"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return null;
    }
}