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
import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.memory.AsyncSearchInMemoryService;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.reaper.AsyncSearchManagementService;
import com.amazon.opendistroforelasticsearch.search.async.reaper.AsyncSearchReaperPersistentTaskExecutor;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestAsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestGetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestSubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStat;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStatNames;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.supplier.Counter;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportAsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportGetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportSubmitAsyncSearchAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AsyncSearchPlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin, SystemIndexPlugin {

    public static final String OPEN_DISTRO_ASYNC_SEARCH_MANAGEMENT_THREAD_POOL_NAME = "open_distro_async_search_management";
    public static final String OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME = "open_distro_async_search_generic";
    private AsyncSearchPersistenceService persistenceService;
    private AsyncSearchInMemoryService inMemoryService;
    private AsyncSearchStats asyncSearchStats;

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestSubmitAsyncSearchAction(),
                new RestGetAsyncSearchAction(),
                new RestDeleteAsyncSearchAction(),
                new RestAsyncSearchStatsAction(asyncSearchStats));

    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {

        Map<String, AsyncSearchStat<?>> stats = Stream.of(
                new AbstractMap.SimpleEntry<>(
                        AsyncSearchStatNames.RUNNING_ASYNC_SEARCH_COUNT.getName(), new AsyncSearchStat<>(false, new Counter())),
                new AbstractMap.SimpleEntry<>(
                        AsyncSearchStatNames.ABORTED_ASYNC_SEARCH_COUNT.getName(), new AsyncSearchStat<>(false, new Counter())),
                new AbstractMap.SimpleEntry<>(
                        AsyncSearchStatNames.COMPLETED_ASYNC_SEARCH_COUNT.getName(), new AsyncSearchStat<>(false, new Counter())),
                new AbstractMap.SimpleEntry<>(
                        AsyncSearchStatNames.PERSISTED_ASYNC_SEARCH_COUNT.getName(), new AsyncSearchStat<>(false, new Counter())),
                new AbstractMap.SimpleEntry<>(
                        AsyncSearchStatNames.FAILED_ASYNC_SEARCH_COUNT.getName(), new AsyncSearchStat<>(false, new Counter()))
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        asyncSearchStats = new AsyncSearchStats(stats);
        this.persistenceService = new AsyncSearchPersistenceService(client, clusterService, threadPool, namedWriteableRegistry);
        this.inMemoryService = new AsyncSearchInMemoryService(threadPool, clusterService, asyncSearchStats);
        return Arrays.asList(new AsyncSearchService(persistenceService, inMemoryService, client, clusterService, threadPool,
                namedWriteableRegistry, asyncSearchStats), asyncSearchStats);

    }


    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(AsyncSearchManagementService.class);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.singletonList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, AsyncSearchReaperPersistentTaskExecutor.NAME,
                        in -> new AsyncSearchReaperPersistentTaskExecutor.AsyncSearchReaperParams())
        );
    }


    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.singletonList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class,
                        new ParseField(AsyncSearchReaperPersistentTaskExecutor.NAME),
                        AsyncSearchReaperPersistentTaskExecutor.AsyncSearchReaperParams::fromXContent)
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(AsyncSearchStatsAction.INSTANCE, TransportAsyncSearchStatsAction.class),
                new ActionHandler<>(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
                new ActionHandler<>(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
                new ActionHandler<>(DeleteAsyncSearchAction.INSTANCE, TransportDeleteAsyncSearchAction.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(AsyncSearchService.DEFAULT_KEEPALIVE_SETTING,
                AsyncSearchService.MAX_KEEPALIVE_SETTING);
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(new SystemIndexDescriptor(".async_search_response",
                "Stores the response for async search"));
    }

    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool,
                                                                       Client client, SettingsModule settingsModule,
                                                                       IndexNameExpressionResolver expressionResolver) {
        return Collections.singletonList(
                new AsyncSearchReaperPersistentTaskExecutor(clusterService, client, persistenceService));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int processorCount = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(OPEN_DISTRO_ASYNC_SEARCH_MANAGEMENT_THREAD_POOL_NAME, 4,
                Math.min(64, Math.max(32, processorCount)), TimeValue.timeValueMinutes(5)));
        executorBuilders.add(new ScalingExecutorBuilder(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1, 5,
                TimeValue.timeValueMinutes(30)));
        return executorBuilders;
    }
}
