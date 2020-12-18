package com.amazon.opendistroforelasticsearch.search.async.request;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchRequestRoutingIT.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 5)
public class AsyncSearchRequestRoutingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ScriptedBlockPlugin.class, AsyncSearchPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AsyncSearchPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    public void testRequestForwardingToCoordinatorNodeForPersistedAsyncSearch() throws Exception {
        String idx = randomAlphaOfLength(10).toLowerCase();
        assertAcked(prepareCreate(idx)
                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        indexRandom(true,
                client().prepareIndex(idx, "type", "1").setSource(
                        "ip", "192.168.1.7",
                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
                client().prepareIndex(idx, "type", "2").setSource(
                        "ip", "192.168.1.10",
                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
                client().prepareIndex(idx, "type", "3").setSource(
                        "ip", "2001:db8::ff00:42:8329",
                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest(idx));
        request.keepOnCompletion(true);
        AsyncSearchResponse submitResponse = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(submitResponse.getId());
        assertNotNull(submitResponse.getId());
        TestClientUtils.assertResponsePersistence(client(), submitResponse.getId());
        assertNotNull(submitResponse.getSearchResponse());
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        assertEquals(clusterService.state().nodes().getDataNodes().size(), 5);
        List<String> nonCoordinatorNodeNames = new LinkedList<>();
        clusterService.state().nodes().iterator().forEachRemaining(node -> {
            if (asyncSearchId.getNode().equals(node.getId()) == false)
                nonCoordinatorNodeNames.add(node.getName());
        });
        nonCoordinatorNodeNames.forEach(n -> {
            try {
                AsyncSearchResponse getResponse = client(n).execute(GetAsyncSearchAction.INSTANCE,
                        new GetAsyncSearchRequest(submitResponse.getId())).get();
                assertEquals(getResponse, new AsyncSearchResponse(submitResponse.getId(), AsyncSearchState.PERSISTED,
                        submitResponse.getStartTimeMillis(), submitResponse.getExpirationTimeMillis(), submitResponse.getSearchResponse(),
                        submitResponse.getError()));
            } catch (InterruptedException | ExecutionException e) {
                fail("Get async search request should not have failed");
            }
        });
    }

    public void testRequestForwardingToCoordinatorNodeForRunningAsyncSearch() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        String index = randomAlphaOfLength(10).toLowerCase();
        assertAcked(prepareCreate(index)
                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(true,
                client().prepareIndex(index, "type", "1").setSource(
                        "ip", "192.168.1.7",
                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
                client().prepareIndex(index, "type", "2").setSource(
                        "ip", "192.168.1.10",
                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
                client().prepareIndex(index, "type", "3").setSource(
                        "ip", "2001:db8::ff00:42:8329",
                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        SearchRequest searchRequest = client().prepareSearch(index).setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME,
                        Collections.emptyMap())))
                .request();
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.keepOnCompletion(false);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(0));
        CountDownLatch latch = new CountDownLatch(1);

        client().execute(SubmitAsyncSearchAction.INSTANCE, request, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse submitResponse) {
                String id = submitResponse.getId();
                assertNotNull(id);
                assertEquals(AsyncSearchState.RUNNING, submitResponse.getState());
                AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(id);
                ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
                assertEquals(clusterService.state().nodes().getDataNodes().size(), 5);
                List<String> nonCoordinatorNodeNames = new LinkedList<>();
                clusterService.state().nodes().iterator().forEachRemaining(node -> {
                    if (asyncSearchId.getNode().equals(node.getId()) == false)
                        nonCoordinatorNodeNames.add(node.getName());
                });
                nonCoordinatorNodeNames.forEach(n -> {
                    try {
                        AsyncSearchResponse getResponse = client(n).execute(GetAsyncSearchAction.INSTANCE,
                                new GetAsyncSearchRequest(id)).get();
                        assertEquals(getResponse.getState(), AsyncSearchState.RUNNING);
                    } catch (InterruptedException | ExecutionException e) {
                        fail("Get async search request should not have failed");
                    }
                });
                String randomNonCoordinatorNode = nonCoordinatorNodeNames.get(randomInt(nonCoordinatorNodeNames.size()));
                try {
                    AcknowledgedResponse acknowledgedResponse =
                            client(randomNonCoordinatorNode).execute(DeleteAsyncSearchAction.INSTANCE, new DeleteAsyncSearchRequest(id)).get();
                    assertTrue(acknowledgedResponse.isAcknowledged());
                    ExecutionException executionException = expectThrows(ExecutionException.class,
                            () -> client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncSearchRequest(id)).get());
                    assertTrue(executionException.getCause() instanceof ResourceNotFoundException);
                } catch (InterruptedException | ExecutionException e) {
                    fail("Delete async search request from random non-coordinator node should have succeeded.");
                }

                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    fail(e.getMessage());
                } finally {

                    latch.countDown();
                }
            }
        });
        latch.await();
        disableBlocks(plugins);
    }

    public void testInvalidIdRequestHandling() {
        ExecutionException executionException = expectThrows(ExecutionException.class, () -> client().execute(GetAsyncSearchAction.INSTANCE,
                new GetAsyncSearchRequest(randomAlphaOfLength(16))).get());
        assertTrue(executionException.getCause() instanceof ResourceNotFoundException);
    }

    protected List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    protected void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    protected void disableBlocks(List<ScriptedBlockPlugin> plugins) throws Exception {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(AsyncSearchRequestRoutingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

}
