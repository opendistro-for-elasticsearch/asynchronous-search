package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class AsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    @Before
    public void indexDocuments() {

        createIndex("index", Settings.builder().put("index.refresh_interval", -1).build());
        for (int i = 0; i < 10; i++)
            client().prepareIndex("index", "type", String.valueOf(i)).setSource("field", "value" + i)
                    .setRefreshPolicy(IMMEDIATE).get();
    }

    @Test
    public void submitAsyncSearchAndGetAndDelete() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.indices("index");
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        AsyncSearchResponse submitResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), submitAsyncSearchRequest);
        TestClientUtils.assertResponsePersistence(client(), submitResponse.getId());
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());

        AsyncSearchResponse getResponse = TestClientUtils.getFinalAsyncSearchResponse(client(), submitResponse,
                getAsyncSearchRequest);

        assertNull(getResponse.getSearchResponse().getAggregations());
        assertEquals(10, getResponse.getSearchResponse().getHits().getTotalHits().value);
        assertFalse(getResponse.isPartial());
        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(getResponse.getId());
        AcknowledgedResponse acknowledgedResponse = TestClientUtils.blockingDeleteAsyncSearchRequest(client(),
                deleteAsyncSearchRequest);
        assertTrue(acknowledgedResponse.isAcknowledged());
        assertRNF(TestClientUtils::blockingGetAsyncSearchResponse, client(), getAsyncSearchRequest);
        assertRNF(TestClientUtils::blockingDeleteAsyncSearchRequest, client(), deleteAsyncSearchRequest);
    }

    @Test
    public void submitAsyncSearchMatchQuery() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));

        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        AsyncSearchResponse submitResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), submitAsyncSearchRequest);
        TestClientUtils.assertResponsePersistence(client(), submitResponse.getId());
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());

        AsyncSearchResponse getResponse = TestClientUtils.getFinalAsyncSearchResponse(client(), submitResponse,
                getAsyncSearchRequest);

        assertNull(getResponse.getSearchResponse().getAggregations());
        assertEquals(1, getResponse.getSearchResponse().getHits().getTotalHits().value);
        assertFalse(getResponse.isPartial());

        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(getResponse.getId());
        AcknowledgedResponse acknowledgedResponse = TestClientUtils.blockingDeleteAsyncSearchRequest(client(),
                deleteAsyncSearchRequest);
        assertTrue(acknowledgedResponse.isAcknowledged());
        assertRNF(TestClientUtils::blockingGetAsyncSearchResponse, client(), getAsyncSearchRequest);
        assertRNF(TestClientUtils::blockingDeleteAsyncSearchRequest, client(), deleteAsyncSearchRequest);
    }

}
