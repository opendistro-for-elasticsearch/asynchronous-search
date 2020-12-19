package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class AsyncSearchManagementServiceIT extends AsyncSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ScriptedBlockPlugin.class,
                AsyncSearchPlugin.class,
                ReindexPlugin.class);
    }

    //We need to apply blocks via ScriptedBlockPlugin, external clusters are immutable
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        boolean lowLevelCancellation = randomBoolean();
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("node.attr.asynchronous_search_enabled", true)
                .put(AsyncSearchManagementService.REAPER_INTERVAL_SETTING.getKey(),  TimeValue.timeValueSeconds(5))
                .put(AsyncSearchManagementService.RESPONSE_CLEAN_UP_INTERVAL_SETTING.getKey(),  TimeValue.timeValueSeconds(5))
                .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), lowLevelCancellation)
                .build();
    }

    private void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test", "type", Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    public void  testExpiredAsyncSearchCleanUpDuringQueryPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        SearchRequest searchRequest = client().prepareSearch("test").setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        //We need a NodeClient to make sure the listener gets injected in the search request execution.
        //Randomized client randomly return NodeClient/TransportClient
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        testCase(internalCluster().smartClient(), submitAsyncSearchRequest, plugins);
    }


    public void testExpiredAsyncSearchCleanUpDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        testCase(internalCluster().smartClient(), submitAsyncSearchRequest, plugins);
    }

    public void testExpiredAsyncSearchResponseFromPersistedStore() throws Exception {
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        //testCase(internalCluster().smartClient(), submitAsyncSearchRequest, plugins);
    }

    private void testCase(Client client, SubmitAsyncSearchRequest request, List<ScriptedBlockPlugin> plugins) throws Exception {
        final AtomicReference<SearchResponse> searchResponseRef = new AtomicReference<>();
        final AtomicReference<AsyncSearchResponse> asyncSearchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client.execute(SubmitAsyncSearchAction.INSTANCE, request, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                asyncSearchResponseRef.set(asyncSearchResponse);
                searchResponseRef.set(asyncSearchResponse.getSearchResponse());
                exceptionRef.set(asyncSearchResponse.getError());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        awaitForBlock(plugins);
        assertNotNull(asyncSearchResponseRef.get());
        CountDownLatch updateLatch = new CountDownLatch(1);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(asyncSearchResponseRef.get().getId());
        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueMillis(1));
        client.execute(GetAsyncSearchAction.INSTANCE, getAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                asyncSearchResponseRef.set(asyncSearchResponse);
                exceptionRef.set(asyncSearchResponse.getError());
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                updateLatch.countDown();
            }
        });
        updateLatch.await();
        assertThat(asyncSearchResponseRef.get().getExpirationTimeMillis(), lessThan(System.currentTimeMillis()));
        boolean cleanedUp = waitUntil(() -> isResourceCleanedUp(asyncSearchResponseRef.get().getId()));
        assertTrue(cleanedUp);
        disableBlocks(plugins);
        waitUntil(() -> verifyTaskCancelled(AsyncSearchTask.NAME));
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
        latch.await();
    }
}
