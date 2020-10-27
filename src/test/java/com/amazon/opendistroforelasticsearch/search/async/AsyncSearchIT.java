package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

public class AsyncSearchIT extends AsyncSearchIntegTestCase {

    @Before
    public void indexDocuments() throws InterruptedException {

        createIndex("test");
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex("test", "type1", "3").setSource("field1", "quick"));
    }

    @Test
    public void submitAsyncSearchAndGetAndDelete() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        AsyncSearchResponse submitResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), submitAsyncSearchRequest);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());

        AsyncSearchResponse getResponse = TestClientUtils.getFinalAsyncSearchResponse(client(), submitResponse,
                getAsyncSearchRequest);

        assertNull(getResponse.getSearchResponse().getAggregations());
        assertEquals(3, getResponse.getSearchResponse().getHits().getTotalHits().value);

        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(getResponse.getId());
        AcknowledgedResponse acknowledgedResponse = TestClientUtils.blockingDeleteAsyncSearchRequest(client(),
                deleteAsyncSearchRequest);
        assertTrue(acknowledgedResponse.isAcknowledged());

    }


}
