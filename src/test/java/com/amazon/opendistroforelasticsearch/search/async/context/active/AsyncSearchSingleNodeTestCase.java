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

package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public abstract class AsyncSearchSingleNodeTestCase extends ESSingleNodeTestCase {
    protected static final String INDEX = ".asynchronous_search_response";
    protected static final String TEST_INDEX = "index";

    @Override
    protected boolean resetNodeAfterTest() {
        return false;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        createIndex(TEST_INDEX, Settings.builder().put("index.refresh_interval", -1).build());
        for (int i = 0; i < 10; i++)
            client().prepareIndex(TEST_INDEX, "type", String.valueOf(i)).setSource("field", "value" + i)
                    .setRefreshPolicy(IMMEDIATE).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(AsyncSearchPlugin.class);
    }

    public static ActionFuture<AsyncSearchResponse> executeSubmitAsyncSearch(Client client, SubmitAsyncSearchRequest request) {
        return client.execute(SubmitAsyncSearchAction.INSTANCE, request);
    }

    public static ActionFuture<AsyncSearchResponse> executeGetAsyncSearch(Client client, GetAsyncSearchRequest request) {
        return client.execute(GetAsyncSearchAction.INSTANCE, request);
    }

    public static ActionFuture<AcknowledgedResponse> executeDeleteAsyncSearch(Client client, DeleteAsyncSearchRequest request) {
        return client.execute(DeleteAsyncSearchAction.INSTANCE, request);
    }

    public static void executeDeleteAsyncSearch(Client client, DeleteAsyncSearchRequest request,
                                                ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteAsyncSearchAction.INSTANCE, request, listener);
    }

    @After
    public void tearDownData() throws InterruptedException {
        CountDownLatch deleteLatch = new CountDownLatch(1);
        client().admin().indices().prepareDelete(INDEX).execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> {
            deleteLatch.countDown();
        }));
        deleteLatch.await();
    }
}
