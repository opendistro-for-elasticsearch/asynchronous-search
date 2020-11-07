package com.amazon.opendistroforelasticsearch.search.async.response;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

public class AsyncSearchResponseTests extends AbstractSerializingTestCase<AsyncSearchResponse> {

    @Override
    protected AsyncSearchResponse doParseInstance(XContentParser parser) throws IOException {
        return AsyncSearchResponse.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<AsyncSearchResponse> instanceReader() {
        return AsyncSearchResponse::new;
    }

    @Override
    protected AsyncSearchResponse createTestInstance() {
        return new AsyncSearchResponse(UUID.randomUUID().toString(), randomBoolean(), randomNonNegativeLong(),
                randomNonNegativeLong(), getMockSearchResponse(), null);

    }

    @Override
    protected AsyncSearchResponse mutateInstance(AsyncSearchResponse instance) {
        return new AsyncSearchResponse(instance.getId(), !instance.isRunning(), instance.getStartTimeMillis(),
                instance.getExpirationTimeMillis(), instance.getSearchResponse(), instance.getError());
    }

    private SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
