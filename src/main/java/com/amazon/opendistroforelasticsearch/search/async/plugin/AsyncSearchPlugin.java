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

package com.amazon.opendistroforelasticsearch.search.async.plugin;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.TransportDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.TransportGetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.TransportSubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestGetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestSubmitAsyncSearchAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class AsyncSearchPlugin extends Plugin implements ActionPlugin { //JobSchedulerExtension {

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestSubmitAsyncSearchAction(),
                new RestGetAsyncSearchAction(),
                new RestDeleteAsyncSearchAction());
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return Collections.singleton(new AsyncSearchService(client, clusterService, threadPool));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
                new ActionHandler<>(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
                new ActionHandler<>(DeleteAsyncSearchAction.INSTANCE, TransportDeleteAsyncSearchAction.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(AsyncSearchService.DEFAULT_KEEPALIVE_SETTING,
                AsyncSearchService.MAX_KEEPALIVE_SETTING,
                AsyncSearchService.KEEPALIVE_INTERVAL_SETTING);
    }

   /* @Override
    public String getJobType() {
        return null;
    }

    @Override
    public String getJobIndex() {
        return null;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return null;
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return null;
    }*/
}
