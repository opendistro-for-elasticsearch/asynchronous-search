package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AsyncSearchActiveStoreTests extends AsyncSearchTestCase {
    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsyncSearchActiveStore.MAX_RUNNING_CONTEXT.getKey(), 10)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsyncSearchActiveStore.MAX_RUNNING_CONTEXT,
                        AsyncSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testPutContext() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);

            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsyncSearchActiveStore activeStore = new AsyncSearchActiveStore(mockClusterService);

            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(testThreadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchContextListener asyncSearchContextListener = new AsyncSearchContextListener() {
            };
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node.getId(),
                    keepAlive, keepOnCompletion, testThreadPool,
                    testThreadPool::absoluteTimeInMillis, asyncSearchProgressListener, asyncSearchContextListener);

            activeStore.putContext(asyncSearchContextId, context, asyncSearchContextListener::onContextRejected);
            Optional<AsyncSearchActiveContext> optional = activeStore.getContext(asyncSearchContextId);
            assert (optional.isPresent());
            assertEquals(optional.get(), context);

        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }
}
