package com.amazon.opendistroforelasticsearch.search.async.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

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
        return Arrays.asList(new Route(GET, "/_async_search"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return null;
    }
}
