package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.UUID;

public class AsyncSearchIdTests extends ESTestCase {

    public void testAsyncSearchIdParsing() {
        String node = UUID.randomUUID().toString();
        long taskId = randomNonNegativeLong();
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(
                UUID.randomUUID().toString(), randomNonNegativeLong());

        AsyncSearchId original = new AsyncSearchId(node, taskId, asyncSearchContextId);

        //generate identifier to access the submitted async search
        String id = AsyncSearchIdConverter.buildAsyncId(original);

        //parse the AsyncSearchId object which will contain information regarding node running the search, the associated task and
        // context id.
        AsyncSearchId parsed = AsyncSearchIdConverter.parseAsyncId(id);

        assertEquals(original, parsed);
    }


    public void testAsyncSearchIdParsingFailure() {
        String id = UUID.randomUUID().toString();
        //a possible precursor of ResourceNotFoundException(id), when a GET "/_opendistro/_asynchronous_search/{id}" is made
        // with an illegal id
        expectThrows(IllegalArgumentException.class, () -> AsyncSearchIdConverter.parseAsyncId(id));
    }
}
