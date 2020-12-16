package com.amazon.opendistroforelasticsearch.search.async.restIT;

import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class SubmitAsyncSearchRestIT extends AsyncSearchRestTestCase {

    public void testSubmitWithoutRetainedResponse() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        assertTrue(submitResponse.getState().name(), submitResponse.getState().equals(AsyncSearchState.RUNNING)
                || submitResponse.getState().equals(AsyncSearchState.CLOSED)
                || submitResponse.getState().equals(AsyncSearchState.SUCCEEDED));
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
        AsyncSearchResponse getResponse = null;
        do {
            try {
                getResponse = executeGetAsyncSearch(getAsyncSearchRequest);
            } catch (Exception e) {
                assertTrue(e instanceof ResponseException);
            }
        } while (getResponse != null && (getResponse.getState().equals(AsyncSearchState.RUNNING)
                || getResponse.getState().equals(AsyncSearchState.SUCCEEDED) || getResponse.getState().equals(AsyncSearchState.CLOSED)));
    }
/*
    @Test
    public void submitAsyncSearchWithMatchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        TimeValue keepAlive = new TimeValue(10, TimeUnit.DAYS);
        submitAsyncSearchRequest.keepAlive(keepAlive);
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        assertTrue(submitResponse.getExpirationTimeMillis() - submitResponse.getStartTimeMillis() >= keepAlive.getMillis());
        AsyncSearchResponse getResponse;
        do {
            logger.info("Get async search {}", submitResponse.getId());
            getResponse = getAsyncSearchResponse(submitResponse, new GetAsyncSearchRequest(submitResponse.getId()));
        } while (getResponse.getState().equals(AsyncSearchState.RUNNING.name()));
        assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 1);
    }

    @Test
    public void submitAsyncSearchUpdateKeepAliveWithGet() throws IOException {
        //create async search with default keep alive
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test");
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(new SubmitAsyncSearchRequest(searchRequest));
        AsyncSearchResponse getResponse;
        do {
            logger.info("Get async search {}", submitResponse.getId());
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            getAsyncSearchRequest.setKeepAlive(new TimeValue(10, TimeUnit.DAYS));
            getResponse = getAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
            assertTrue(getResponse.getExpirationTimeMillis() > submitResponse.getExpirationTimeMillis());

        } while (getResponse.getState().equals(AsyncSearchState.RUNNING.name()));

        assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 5);
    }

    //FIXME right now if id is not parseable or if parsed id renders a node not found its
    // throwing 400 and 500 respectively. Should be uniformly 404?
    @Test
    @Ignore
    public void accessAsyncSearchNonExistentSearchId() throws IOException {
        String id = "FkxaVllUOTUwUWJ1NWp0Y2FXZkEyLVEUc2Vfd21uUUJaLU5fNDRNU3BvQ1AAAAAAAAAAAQ==";//a valid id
        assert404(new GetAsyncSearchRequest(id), super::executeGetAsyncSearch);
        assert404(new DeleteAsyncSearchRequest(id), super::deleteAsyncSearchApi);
    }

 */
}
