package com.amazon.opendistroforelasticsearch.search.async.context.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

public class AsyncSearchPersistenceContextTests extends ESTestCase {

    /**
     * async search persistence context serializes search response into {@linkplain BytesReference}. We verify that de-serializing
     * the
     * {@linkplain BytesReference} yields the same object.
     *
     * @throws IOException when there is a serialization issue
     */
    public void testXContentRoundTripWithSearchResponse() throws IOException {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                randomNonNegativeLong());
        String id = AsyncSearchIdConverter.buildAsyncId(new AsyncSearchId(UUID.randomUUID().toString(),
                randomNonNegativeLong(), asyncSearchContextId));
        long expirationTimeMillis = randomNonNegativeLong();
        long startTimeMillis = randomNonNegativeLong();
        SearchResponse searchResponse = getMockSearchResponse();
        AsyncSearchPersistenceContext asyncSearchPersistenceContext =
                new AsyncSearchPersistenceContext(id, asyncSearchContextId, new AsyncSearchPersistenceModel(startTimeMillis,
                        expirationTimeMillis, searchResponse), System::currentTimeMillis);
        assertEquals(
                asyncSearchPersistenceContext.getAsyncSearchResponse(),
                new AsyncSearchResponse(id, false, startTimeMillis, expirationTimeMillis, searchResponse, null));
    }

    /**
     * async search persistence model serializes exception into {@linkplain BytesReference}. We verify that de-serializing the
     * {@linkplain BytesReference} yields the same object.
     *
     * @throws IOException when there is a serialization issue
     */
    public void testXContentRoundTripWithError() throws IOException {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                randomNonNegativeLong());
        String id = AsyncSearchIdConverter.buildAsyncId(new AsyncSearchId(UUID.randomUUID().toString(),
                randomNonNegativeLong(), asyncSearchContextId));
        long expirationTimeMillis = randomNonNegativeLong();
        long startTimeMillis = randomNonNegativeLong();
        RuntimeException exception = new RuntimeException("test");
        AsyncSearchPersistenceContext asyncSearchPersistenceContext =
                new AsyncSearchPersistenceContext(id, asyncSearchContextId, new AsyncSearchPersistenceModel(startTimeMillis,
                        expirationTimeMillis, exception), System::currentTimeMillis);
        AsyncSearchResponse parsed = asyncSearchPersistenceContext.getAsyncSearchResponse();
        /*
         * we cannot compare the cause, because it will be wrapped and serialized in an outer
         * ElasticSearchException best effort: try to check that the original
         * message appears somewhere in the rendered xContent.
         */
        String originalMsg = parsed.getError().getCause().getMessage();
        assertEquals(originalMsg,
                "Elasticsearch exception [type=runtime_exception, reason=test]");

    }

    protected SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}

