package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

public class AsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

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
//        assertRNF(TestClientUtils::blockingGetAsyncSearchResponse, client(), getAsyncSearchRequest);
//        assertRNF(TestClientUtils::blockingDeleteAsyncSearchRequest, client(), deleteAsyncSearchRequest);
//        TODO : right now we throw a custom AsyncSearchContextMissingException. Should it be wrapped with RNF as client doesnt need to
//         privy to internal construct, `AsyncSearchContext`
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
