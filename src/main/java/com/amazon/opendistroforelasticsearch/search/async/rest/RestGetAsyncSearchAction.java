package com.amazon.opendistroforelasticsearch.search.async.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

public class RestGetAsyncSearchAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_async_search";
    }

    @Override
    public List<Route> routes() {
        return null;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return null;
    }
}
