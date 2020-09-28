package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AsyncSearchServiceIT extends AsyncSearchSingleNodeTestCase {

    @Test
    public void testContextCreation() throws IOException {
        AsyncSearchService asyncSearchService = getInstanceFromNode(AsyncSearchService.class);
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
        submitAsyncSearchRequest.setKeepAlive(new TimeValue(1, TimeUnit.DAYS));
        AsyncSearchContext context = asyncSearchService.createAndPutContext(submitAsyncSearchRequest);
        AsyncSearchContext getContext =
                asyncSearchService.findContext(AsyncSearchId.buildAsyncId(new AsyncSearchId(node().getNodeEnvironment().nodeId(),
                context.getAsyncSearchContextId())), context.getAsyncSearchContextId());
        assertEquals(context, getContext);
        assertNull(context.getTask());
    }
}
