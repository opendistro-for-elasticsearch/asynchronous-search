package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchServiceIT.SearchDelayPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

public class AsyncSearchServiceIT extends AsyncSearchSingleNodeTestCase {

    Logger logger = LogManager.getLogger(AsyncSearchServiceIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        LinkedList<Class<? extends Plugin>> plugins = new LinkedList<>(super.getPlugins());
        plugins.add(SearchDelayPlugin.class);
        return plugins;
    }

    @Test
    public void testContextCreation() throws IOException {
//        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);
//        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
//        submitAsyncSearchRequest.setKeepAlive(new TimeValue(1, TimeUnit.DAYS));
//        AsyncSearchContext context = asyncSearchService.createAndPutContext(submitAsyncSearchRequest);
//        AsyncSearchContext getContext =
//                asyncSearchService.findContext(context.getAsyncSearchContextId()), context.getAsyncSearchContextId());
//        assertEquals(context, getContext);
//        assertNull(context.getTask());
    }


    public void testUpdateKeepAliveOnRunningStageSearch() throws Exception {
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);

        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.waitForCompletionTimeout(new TimeValue(1, TimeUnit.MILLISECONDS));

        AsyncSearchResponse submitResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);

        AsyncSearchContext context =
                asyncSearchService.findContext(AsyncSearchId.parseAsyncId(submitResponse.getId()).getAsyncSearchContextId(), null);
        long oldExpiration = context.getExpirationTimeMillis();
        assert context.getStage().equals(AsyncSearchContext.Stage.RUNNING);

        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
        getAsyncSearchRequest.setKeepAlive(new TimeValue(20, TimeUnit.DAYS));
        TestClientUtils.blockingGetAsyncSearchResponse(client(), getAsyncSearchRequest);

        long newExpiration = context.getExpirationTimeMillis();
        assert oldExpiration < newExpiration;
    }

    public void testUpdateKeepAliveOnPersistedStageSearch() throws Exception {
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);

        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.waitForCompletionTimeout(new TimeValue(1, TimeUnit.MILLISECONDS));

        AsyncSearchResponse submitResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);
        TestClientUtils.assertResponsePersistence(client(), submitResponse.getId());
        AsyncSearchContext context =
                asyncSearchService.findContext(AsyncSearchId.parseAsyncId(submitResponse.getId()).getAsyncSearchContextId(), null);
        long oldExpiration = context.getExpirationTimeMillis();
        assert context.getStage().equals(AsyncSearchContext.Stage.PERSISTED);

        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
        getAsyncSearchRequest.setKeepAlive(new TimeValue(20, TimeUnit.DAYS));
        AsyncSearchResponse getResponse = TestClientUtils.blockingGetAsyncSearchResponse(client(), getAsyncSearchRequest);

        long newExpiration = context.getExpirationTimeMillis();
        assert oldExpiration == newExpiration;
        assert getResponse.getExpirationTimeMillis() > submitResponse.getExpirationTimeMillis();
    }

    public void testUpdateKeepAliveOnCompletedStageSearch() throws Exception {
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);

        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.waitForCompletionTimeout(new TimeValue(1, TimeUnit.MILLISECONDS));

        AsyncSearchResponse submitResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);

        AsyncSearchContext context =
                asyncSearchService.findContext(AsyncSearchId.parseAsyncId(submitResponse.getId()).getAsyncSearchContextId(), null);
        assert context.getStage().equals(AsyncSearchContext.Stage.RUNNING);
        while(!context.getStage().equals(AsyncSearchContext.Stage.COMPLETED)) {}
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
        getAsyncSearchRequest.setKeepAlive(new TimeValue(20, TimeUnit.DAYS));
        asyncSearchService.updateKeepAlive(getAsyncSearchRequest, context, new ActionListener<ActionResponse>() {
            @Override
            public void onResponse(ActionResponse actionResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });
        AsyncSearchResponse getResponse = TestClientUtils.blockingGetAsyncSearchResponse(client(), getAsyncSearchRequest);
        assert getResponse.getExpirationTimeMillis() > submitResponse.getExpirationTimeMillis();
    }
    public static class SearchDelayPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_delay";

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
