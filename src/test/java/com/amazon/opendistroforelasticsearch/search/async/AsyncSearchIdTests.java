package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchActiveContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.UUID;

public class AsyncSearchIdTests extends ESTestCase {

    public void testAsyncSearchIdParsing() {
        String node = UUID.randomUUID().toString();
        long taskId = randomNonNegativeLong();
        AsyncSearchActiveContextId asyncSearchActiveContextId = new AsyncSearchActiveContextId(
                UUID.randomUUID().toString(), randomNonNegativeLong());

        AsyncSearchId original = new AsyncSearchId(node, taskId, asyncSearchActiveContextId);

        //generate identifier to access the submitted async search
        String id = AsyncSearchId.buildAsyncId(original);

        //parse the AsyncSearchId object which will contain information regarding node running the search, the associated task and
        // active context id.
        AsyncSearchId parsed = AsyncSearchId.parseAsyncId(id);

        assertEquals(original, parsed);
    }


    public void testAsyncSearchIdParsingFailure() {
        String id = UUID.randomUUID().toString();

        //a possible precursor of ResourceNotFoundException(id), when a GET "/_opendistro/_asynchronous_search/{id}" is made
        // with an illegal id
        expectThrows(IllegalArgumentException.class, () -> AsyncSearchId.parseAsyncId(id));
    }
}
