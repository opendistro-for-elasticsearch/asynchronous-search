package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
//
//            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/type/1");
//            doc1.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
//            doc1.setJsonEntity("{\"type\":\"type1\", \"id\":1, \"num\":10, \"num2\":50}");
//            client().performRequest(doc1);
//            Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/type/2");
//            doc2.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
//            doc2.setJsonEntity("{\"type\":\"type1\", \"id\":2, \"num\":20, \"num2\":40}");
//            client().performRequest(doc2);
//            Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/type/3");
//            doc3.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
//            doc3.setJsonEntity("{\"type\":\"type1\", \"id\":3, \"num\":50, \"num2\":35}");
//            client().performRequest(doc3);
//            Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/type/4");
//            doc4.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
//            doc4.setJsonEntity("{\"type\":\"type2\", \"id\":4, \"num\":100, \"num2\":10}");
//            client().performRequest(doc4);
//            Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/type/5");
//            doc5.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
//            doc5.setJsonEntity("{\"type\":\"type2\", \"id\":5, \"num\":100, \"num2\":10}");
//            client().performRequest(doc5);
    }

    @Test
    public void submitAsyncSearchAndGetAndDelete() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        AsyncSearchResponse submitResponse = blockingSubmitAsyncSearch(submitAsyncSearchRequest);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());

        AsyncSearchResponse getResponse = getFinalAsyncSearchResponse(submitResponse, getAsyncSearchRequest);

        assertNull(getResponse.getSearchResponse().getAggregations());
        assertEquals(3, getResponse.getSearchResponse().getHits().getTotalHits().value);
        assertFalse(getResponse.isPartial());

        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(getResponse.getId());
        AcknowledgedResponse acknowledgedResponse = blockingDeleteAsyncSearchRequest(deleteAsyncSearchRequest);
        assertTrue(acknowledgedResponse.isAcknowledged());

    }

    public AsyncSearchResponse getFinalAsyncSearchResponse(AsyncSearchResponse submitResponse, GetAsyncSearchRequest getAsyncSearchRequest) {
        AsyncSearchResponse getResponse;
        do {
            logger.info("Get async search {}", submitResponse.getId());
            getResponse = getAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
            assertEquals(submitResponse.getExpirationTimeMillis(), getResponse.getExpirationTimeMillis());
        } while (getResponse.isRunning());
        return getResponse;
    }
}
