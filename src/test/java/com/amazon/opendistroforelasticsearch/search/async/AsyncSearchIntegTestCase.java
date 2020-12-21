package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.greaterThan;

public abstract class AsyncSearchIntegTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ScriptedBlockPlugin.class,
                AsyncSearchPlugin.class,
                ReindexPlugin.class);
    }

    @Override
    protected double getPerTestTransportClientRatio() {
        return 0;
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

    protected void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    protected SearchResponse ensureSearchWasCancelled(SearchResponse searchResponse, Exception e) {
        try {
            if (searchResponse != null) {
                logger.info("Search response {}", searchResponse);
                assertNotEquals("At least one shard should have failed", 0, searchResponse.getFailedShards());
                return searchResponse;
            } else {
                throw e;
            }
        } catch (SearchPhaseExecutionException ex) {
            logger.info("All shards failed with", ex);
            return null;
        } catch (Exception exception) {
            fail("Unexpected exception " + e.getMessage());
            return null;
        }
    }

    protected boolean isResourceCleanedUp(String id) {
        for (AsyncSearchService asyncSearchService : internalCluster().getDataNodeInstances(AsyncSearchService.class)) {
            Map<Long, AsyncSearchActiveContext> activeContexts = asyncSearchService.getAllActiveContexts();
            if (activeContexts.isEmpty() == false) {
                return false;
            }
            try {
                client().get(new GetRequest(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX).refresh(true).id(id))
                        .actionGet().isExists();
            } catch (Exception e) {
                if (e instanceof IndexNotFoundException == false || e instanceof ResourceNotFoundException == false) {
                    fail("Exception while fetching index cleanup" + e);
                } else {
                    return true;
                }
            }
        }
        return true;
    }

    protected boolean verifyTaskCancelled(String action) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).get();
        return listTasksResponse.getTasks().size() == 0;
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_block";

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
                LogManager.getLogger(AsyncSearchIntegTestCase.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()), 60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    public static AsyncSearchResponse executeSubmitAsyncSearch(Client client, SubmitAsyncSearchRequest request)
            throws ExecutionException, InterruptedException {
        return client.execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    //We need to apply blocks via ScriptedBlockPlugin, external clusters are immutable
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
