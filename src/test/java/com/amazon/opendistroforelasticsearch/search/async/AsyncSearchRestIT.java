package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AsyncSearchRestIT extends AsyncSearchRestTestCase {

    @Test
    public void submitAsyncSearchAndGetAndDelete() throws Exception {
        AsyncSearchResponse submitResponse = submitAsyncSearchApi(null);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());

        AsyncSearchResponse getResponse;
        do {
            logger.info("Get async search {}", submitResponse.getId());
            getResponse = getAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
            assertEquals(submitResponse.getExpirationTimeMillis(), getResponse.getExpirationTimeMillis());
        } while (getResponse.isRunning());

        assertNull(getResponse.getSearchResponse().getAggregations());
        assertEquals(5, getResponse.getSearchResponse().getHits().getTotalHits().value);
        assertFalse(getResponse.isPartial());

        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(getResponse.getId());
        Response response = deleteAsyncSearchApi(deleteAsyncSearchRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        //verify get, delete after deletion throw 404
        assert404(deleteAsyncSearchRequest, super::deleteAsyncSearchApi);
        assert404(getAsyncSearchRequest, super::getAsyncSearchApi);

    }

    @Test
    public void submitAsyncSearchWithMatchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        TimeValue keepAlive = new TimeValue(10, TimeUnit.DAYS);
        submitAsyncSearchRequest.setKeepAlive(keepAlive);
        AsyncSearchResponse submitResponse = submitAsyncSearchApi(submitAsyncSearchRequest);
        assert submitResponse.getExpirationTimeMillis() - submitResponse.getStartTimeMillis() >= keepAlive.getMillis();
        AsyncSearchResponse getResponse;
        do {
            logger.info("Get async search {}", submitResponse.getId());
            getResponse = getAsyncSearchResponse(submitResponse,
                    new GetAsyncSearchRequest(submitResponse.getId()));
        } while (getResponse.isRunning());
        assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 1);
    }

    @Test
    public void submitAsyncSearchUpdateKeepAliveWithGet() throws IOException {
        //create async search with default keep alive
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test");
        AsyncSearchResponse submitResponse = submitAsyncSearchApi(new SubmitAsyncSearchRequest(searchRequest));
        AsyncSearchResponse getResponse;
        do {
            logger.info("Get async search {}", submitResponse.getId());
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            getAsyncSearchRequest.setKeepAlive(new TimeValue(10, TimeUnit.DAYS));
            getResponse = getAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
            assertTrue(getResponse.getExpirationTimeMillis() > submitResponse.getExpirationTimeMillis());

        } while (getResponse.isRunning());

        assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 5);
    }

    //FIXME right now if id is not parseable or if parsed id renders a node not found its
    // throwing 400 and 500 respectively. Should be uniformly 404?
    @Test
    @Ignore
    public void accessAsyncSearchNonExistentSearchId() throws IOException {
        String id = "FkxaVllUOTUwUWJ1NWp0Y2FXZkEyLVEUc2Vfd21uUUJaLU5fNDRNU3BvQ1AAAAAAAAAAAQ==";//a valid id
        assert404(
                new GetAsyncSearchRequest(id),
                super::getAsyncSearchApi);
        assert404(new DeleteAsyncSearchRequest(id), super::deleteAsyncSearchApi);
    }
}
