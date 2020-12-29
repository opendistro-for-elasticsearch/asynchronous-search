/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */
package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.utils.AsyncSearchAssertions;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(transportClientRatio = 0)
public class AsyncSearchPartialResponseIT extends ESIntegTestCase {

    private int aggregationSize = randomIntBetween(2, 4);
    private int shardCount = randomIntBetween(5, 20);

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected int numberOfShards() {
        return shardCount;
    }

    protected void createIdx(String keyFieldMapping) {
        assertAcked(prepareCreate("idx")
                .addMapping("type", "key", keyFieldMapping));
    }

    protected void indexData() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();

        for(int i = 0; i < shardCount; i++) {
            docs.addAll(indexDoc(routingKeyForShard("idx", i), "1", 3));
            docs.addAll(indexDoc(routingKeyForShard("idx", i), "2", 1));
            docs.addAll(indexDoc(routingKeyForShard("idx", i), "3", 5));
            docs.addAll(indexDoc(routingKeyForShard("idx", i), "4", 2));
            docs.addAll(indexDoc(routingKeyForShard("idx", i), "5", 1));
            docs.addAll(indexDoc(routingKeyForShard("idx", i), "6", 8));
        }
        indexRandom(true, docs);

        String shardRouting = routingKeyForShard("idx", randomIntBetween(0, shardCount - 1));
        SearchResponse resp = client().prepareSearch("idx").setRouting(shardRouting)
                .setQuery(matchAllQuery()).get();
        assertSearchResponse(resp);
        long totalHits = resp.getHits().getTotalHits().value;
        assertThat(totalHits, is(20L));
    }

    protected List<IndexRequestBuilder> indexDoc(String shard, String key, int times) throws Exception {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[times];
        for (int i = 0; i < times; i++) {
            builders[i] = client().prepareIndex("idx", "type").setRouting(shard).setSource(jsonBuilder()
                    .startObject()
                    .field("key", key)
                    .field("value", 1)
                    .endObject());
        }
        return Arrays.asList(builders);
    }

    public void testPartialReduceBuckets() throws Exception {
        createIdx("type=keyword");
        indexData();
        SearchRequest request = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(terms("keys").field("key").size(aggregationSize)
                        .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values())).order(BucketOrder.count(false)))
                .request();
        request.setBatchedReduceSize(2);
        testCase(client(), request);
    }

    private void testCase(Client client, SearchRequest request) throws Exception {
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        AtomicInteger reduceContextInvocation = new AtomicInteger();
        TestThreadPool threadPool = null;
        AsyncSearchProgressListener listener;
        try {
            threadPool = new TestThreadPool(AsyncSearchProgressListenerIT.class.getName());
            SearchService service = internalCluster().getInstance(SearchService.class);
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(request);
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            Function<SearchResponse, AsyncSearchResponse> responseFunction =
                    (r) -> null;
            Function<Exception, AsyncSearchResponse> failureFunction =
                    (e) -> null;
            listener = new AsyncSearchProgressListener(threadPool.relativeTimeInMillis(), responseFunction,
                    failureFunction, threadPool.generic(), threadPool::relativeTimeInMillis,
                    () -> reduceContextBuilder) {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    assertTrue(responseRef.compareAndSet(null, searchResponse));
                    AsyncSearchAssertions.assertSearchResponses(responseRef.get(), this.partialResponse());
                    latch.countDown();
                }

                @Override
                protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                               DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
                    super.onPartialReduce(shards, totalHits, aggs, reducePhase);
                    Terms terms = this.partialResponse().getAggregations().get("keys");
                    List<? extends Terms.Bucket> buckets = terms.getBuckets();
                    assertThat(buckets.size(), lessThanOrEqualTo(aggregationSize));
                    reduceContextInvocation.incrementAndGet();
                }

                @Override
                protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                    super.onFinalReduce(shards, totalHits, aggs, reducePhase);
                    Terms terms = this.partialResponse().getAggregations().get("keys");
                    List<? extends Terms.Bucket> buckets = terms.getBuckets();
                    assertThat(buckets.size(), equalTo(aggregationSize));
                }

                @Override
                public void onFailure(Exception exception) {
                    assertTrue(exceptionRef.compareAndSet(null, exception));
                    latch.countDown();
                }
            };
            client.execute(SearchAction.INSTANCE, new SearchRequest(request) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                    task.setProgressListener(listener);
                    return task;
                }
            }, listener);

            latch.await();
            Terms terms = responseRef.get().getAggregations().get("keys");
            assertThat(reduceContextInvocation.get(), equalTo(responseRef.get().getNumReducePhases() - 1));
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            assertThat(buckets.size(), equalTo(aggregationSize));
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }
}
