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

package com.amazon.opendistroforelasticsearch.search.asynchronous.rest;

import com.amazon.opendistroforelasticsearch.search.asynchronous.action.AsynchronousSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.AsynchronousSearchStatsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAsynchronousSearchStatsAction extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestAsynchronousSearchStatsAction.class);

    private static final String NAME = "asynchronous_search_stats_action";

    public RestAsynchronousSearchStatsAction() {
    }

    @Override
    public String getName() {
        return NAME;
    }


    @Override
    public List<Route> routes() {
        return Arrays.asList(
                new Route(GET, AsynchronousSearchPlugin.BASE_URI + "/_nodes/{nodeId}/_stats"),
                new Route(GET, AsynchronousSearchPlugin.BASE_URI + "/_stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AsynchronousSearchStatsRequest asynchronousSearchStatsRequest = getRequest(request);

        return channel -> client.execute(AsynchronousSearchStatsAction.INSTANCE, asynchronousSearchStatsRequest,
                new RestActions.NodesResponseRestListener<>(channel));
    }

    private AsynchronousSearchStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));

        AsynchronousSearchStatsRequest asynchronousSearchStatsRequest = new AsynchronousSearchStatsRequest(nodesIds);
        asynchronousSearchStatsRequest.timeout(request.param("timeout"));
        return asynchronousSearchStatsRequest;
    }
}

