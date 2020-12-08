package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.GeoContextMapping;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sampler;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressCodecs("*") // requires custom completion format
public class AsyncSearchQueryIT extends ESIntegTestCase {

    public static final int NUM_SHARDS = 2;
    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    private final String INDEX = RandomStrings.randomAsciiOfLength(random(), 10).toLowerCase(Locale.ROOT);
    private final String TYPE = RandomStrings.randomAsciiOfLength(random(), 10).toLowerCase(Locale.ROOT);
    private final String FIELD = RandomStrings.randomAsciiOfLength(random(), 10).toLowerCase(Locale.ROOT);
    private final CompletionMappingBuilder completionMappingBuilder = new CompletionMappingBuilder();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class, AsyncSearchPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class, AsyncSearchPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 7;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    @Before
    public void indexDocuments() throws InterruptedException, IOException, ExecutionException {
        {
            createIndex("index");
            indexRandom(true, client()
                            .prepareIndex("index", "type1", "1").setSource("num", 10, "num2", 50),
                    client().prepareIndex("index", "type1", "2").setSource("num", 20, "num2", 40),
                    client().prepareIndex("index", "type1", "3").setSource("num", 50, "num2", 35),
                    client().prepareIndex("index", "type1", "4").setSource("num", 100, "num2", 10),
                    client().prepareIndex("index", "type1", "4").setSource("num", 100, "num2", 10));
        }

        {
            createIndex("index1");
            indexRandom(true, client()
                            .prepareIndex("index1", "_doc", "1").setSource("field", "value1", "rating", 7),
                    client().prepareIndex("index1", "_doc", "2").setSource("field", "value2")
            );
        }

        {
            prepareCreate("index2").addMapping("test",
                    XContentFactory.jsonBuilder().startObject().startObject("test")
                            .startObject("properties")
                            .startObject("rating").field("type", "keyword").endObject()
                            .endObject()
                            .endObject().endObject()).execute().get();
            indexRandom(true, client()
                            .prepareIndex("index2", "test", "3").setSource("field", "value1", "rating", "good"),
                    client().prepareIndex("index2", "test", "4").setSource("field", "value2")
            );
        }

        {
            createIndex("index3");
            indexRandom(true, client()
                            .prepareIndex("index3", "_doc", "5").setSource("field", "value1"),
                    client().prepareIndex("index3", "_doc", "6").setSource("field", "value2")
            );
        }

        {
            prepareCreate("index4").addMapping("test",
                    XContentFactory.jsonBuilder().startObject().startObject("test")
                            .startObject("properties")
                            .startObject("field1")
                            .field("type", "keyword")
//                            .field("store", "true")
                            .endObject()
                            .startObject("field2")
                            .field("type", "keyword")
//                            .field("store", "true")
                            .endObject()
                            .endObject()
                            .endObject().endObject()
            ).addAlias(new Alias("alias").filter(QueryBuilders.termQuery("field2", "value1")))
                    .execute().get();
//            assertAcked(prepareCreate("index4")
//                    .addMapping("test", "field1", "type=keyword,store=true", "field2", "type=keyword,store=true")
//                    .addAlias(new Alias("alias").filter(QueryBuilders.termQuery("field", "value1"))));
            ensureGreen();
            indexRandom(true, client()
                    .prepareIndex("index4", "_doc", "3").setSource("field1", "value1", "field2", "value2"));
        }
        refresh();
    }


    public void testSearchMatchQuery() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));

        SearchResponse searchResponse = getPersistedAsyncSearchResponse(searchRequest).getSearchResponse();
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertThat(searchResponse.getHits().getMaxScore(), greaterThan(0f));
        SearchHit searchHit = searchResponse.getHits().getHits()[0];
        assertEquals("index", searchHit.getIndex());
        assertEquals("type1", searchHit.getType());
        assertEquals("1", searchHit.getId());
        assertThat(searchHit.getScore(), greaterThan(0f));
        assertEquals(-1L, searchHit.getVersion());
        assertNotNull(searchHit.getSourceAsMap());
        assertEquals(2, searchHit.getSourceAsMap().size());
        assertEquals(50, searchHit.getSourceAsMap().get("num2"));
    }

    public void testSamplerAggregation() throws InterruptedException, ExecutionException {
        // Tests that we can refer to nested elements under a sample in a path
        // statement
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping(
                        "book", "author", "type=keyword", "name", "type=text", "genre",
                        "type=keyword", "price", "type=float"));
        createIndex("idx_unmapped");
        // idx_unmapped_author is same as main index but missing author field
        assertAcked(prepareCreate("idx_unmapped_author")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("book", "name", "type=text", "genre", "type=keyword", "price", "type=float"));

        ensureGreen();
        String data[] = {
                // "id,cat,name,price,inStock,author_t,series_t,sequence_i,genre_s",
                "0553573403,book,A Game of Thrones,7.99,true,George R.R. Martin,A Song of Ice and Fire,1,fantasy",
                "0553579908,book,A Clash of Kings,7.99,true,George R.R. Martin,A Song of Ice and Fire,2,fantasy",
                "055357342X,book,A Storm of Swords,7.99,true,George R.R. Martin,A Song of Ice and Fire,3,fantasy",
                "0553293354,book,Foundation,17.99,true,Isaac Asimov,Foundation Novels,1,scifi",
                "0812521390,book,The Black Company,6.99,false,Glen Cook,The Chronicles of The Black Company,1,fantasy",
                "0812550706,book,Ender's Game,6.99,true,Orson Scott Card,Ender,1,scifi",
                "0441385532,book,Jhereg,7.95,false,Steven Brust,Vlad Taltos,1,fantasy",
                "0380014300,book,Nine Princes In Amber,6.99,true,Roger Zelazny,the Chronicles of Amber,1,fantasy",
                "0805080481,book,The Book of Three,5.99,true,Lloyd Alexander,The Chronicles of Prydain,1,fantasy",
                "080508049X,book,The Black Cauldron,5.99,true,Lloyd Alexander,The Chronicles of Prydain,2,fantasy"

        };

        for (int i = 0; i < data.length; i++) {
            String[] parts = data[i].split(",");
            client().prepareIndex("test", "book", "" + i)
                    .setSource("author", parts[5], "name", parts[2], "genre", parts[8], "price", Float.parseFloat(parts[3])).get();
            client().prepareIndex("idx_unmapped_author", "book", "" + i)
                    .setSource("name", parts[2], "genre", parts[8], "price", Float.parseFloat(parts[3])).get();
        }
        client().admin().indices().refresh(new RefreshRequest("test")).get();
        boolean asc = randomBoolean();
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.types("book").searchType(SearchType.QUERY_THEN_FETCH);
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().aggregation(terms("genres")
                .field("genre")
                .order(BucketOrder.aggregation("sample>max_price.value", asc))
                .subAggregation(sampler("sample").shardSize(100)
                        .subAggregation(max("max_price").field("price"))));
        SearchResponse response = getPersistedAsyncSearchResponse(searchRequest).getSearchResponse();
        assertSearchResponse(response);
        Terms genres = response.getAggregations().get("genres");
        List<? extends Terms.Bucket> genreBuckets = genres.getBuckets();
        // For this test to be useful we need >1 genre bucket to compare
        assertThat(genreBuckets.size(), greaterThan(1));
        double lastMaxPrice = asc ? Double.MIN_VALUE : Double.MAX_VALUE;
        for (Terms.Bucket genreBucket : genres.getBuckets()) {
            Sampler sample = genreBucket.getAggregations().get("sample");
            Max maxPriceInGenre = sample.getAggregations().get("max_price");
            double price = maxPriceInGenre.getValue();
            if (asc) {
                assertThat(price, greaterThanOrEqualTo(lastMaxPrice));
            } else {
                assertThat(price, lessThanOrEqualTo(lastMaxPrice));
            }
            lastMaxPrice = price;
        }

    }

    public void testIpRange() throws InterruptedException {
        assertAcked(prepareCreate("idx")
                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(true,
                client().prepareIndex("idx", "type", "1").setSource(
                        "ip", "192.168.1.7",
                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
                client().prepareIndex("idx", "type", "2").setSource(
                        "ip", "192.168.1.10",
                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
                client().prepareIndex("idx", "type", "3").setSource(
                        "ip", "2001:db8::ff00:42:8329",
                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        SearchResponse rsp = client().prepareSearch("idx").addAggregation(
                AggregationBuilders.ipRange("my_range")
                        .field("ip")
                        .addUnboundedTo("192.168.1.0")
                        .addRange("192.168.1.0", "192.168.1.10")
                        .addUnboundedFrom("192.168.1.10")).get();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertNull(bucket1.getFrom());
        assertEquals("192.168.1.0", bucket1.getTo());
        assertEquals("*-192.168.1.0", bucket1.getKey());
        assertEquals(0, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("192.168.1.0", bucket2.getFrom());
        assertEquals("192.168.1.10", bucket2.getTo());
        assertEquals("192.168.1.0-192.168.1.10", bucket2.getKey());
        assertEquals(1, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("192.168.1.10", bucket3.getFrom());
        assertNull(bucket3.getTo());
        assertEquals("192.168.1.10-*", bucket3.getKey());
        assertEquals(2, bucket3.getDocCount());
    }

    private AsyncSearchResponse getPersistedAsyncSearchResponse(SearchRequest searchRequest) throws InterruptedException {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.keepOnCompletion(true);
        AsyncSearchResponse asyncSearchResponse = TestClientUtils.blockingSubmitAsyncSearch(client(),
                request);
        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
        asyncSearchResponse = TestClientUtils.blockingGetAsyncSearchResponse(client(),
                new GetAsyncSearchRequest(asyncSearchResponse.getId()));
        return asyncSearchResponse;
    }

    static class CompletionMappingBuilder {
        String searchAnalyzer = "simple";
        String indexAnalyzer = "simple";
        Boolean preserveSeparators = random().nextBoolean();
        Boolean preservePositionIncrements = random().nextBoolean();
        LinkedHashMap<String, ContextMapping<?>> contextMappings = null;

        public CompletionMappingBuilder searchAnalyzer(String searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public CompletionMappingBuilder indexAnalyzer(String indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public CompletionMappingBuilder preserveSeparators(Boolean preserveSeparators) {
            this.preserveSeparators = preserveSeparators;
            return this;
        }

        public CompletionMappingBuilder preservePositionIncrements(Boolean preservePositionIncrements) {
            this.preservePositionIncrements = preservePositionIncrements;
            return this;
        }

        public CompletionMappingBuilder context(LinkedHashMap<String, ContextMapping<?>> contextMappings) {
            this.contextMappings = contextMappings;
            return this;
        }
    }

    public void testTextAndGlobalText() throws Exception {
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder();
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 1; i <= numDocs; i++) {
            indexRequestBuilders.add(client().prepareIndex(INDEX, TYPE, "" + i).setSource(jsonBuilder().startObject().startObject(FIELD)
                    .field("input", "suggestion" + i).field("weight", i).endObject().endObject()));
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder noText = SuggestBuilders.completionSuggestion(FIELD);
        SearchResponse searchResponse = client().prepareSearch(INDEX)
                .suggest(new SuggestBuilder().addSuggestion("foo", noText).setGlobalText("sugg")).get();
        assertSuggestions(searchResponse, "foo", "suggestion10", "suggestion9", "suggestion8", "suggestion7",
                "suggestion6");

        CompletionSuggestionBuilder withText = SuggestBuilders.completionSuggestion(FIELD).text("sugg");
        searchResponse = client().prepareSearch(INDEX)
                .suggest(new SuggestBuilder().addSuggestion("foo", withText)).get();
        assertSuggestions(searchResponse, "foo", "suggestion10", "suggestion9", "suggestion8", "suggestion7",
                "suggestion6");

        // test that suggestion text takes precedence over global text
        searchResponse = client().prepareSearch(INDEX)
                .suggest(new SuggestBuilder().addSuggestion("foo", withText).setGlobalText("bogus")).get();
        assertSuggestions(searchResponse, "foo", "suggestion10", "suggestion9", "suggestion8", "suggestion7",
                "suggestion6");
    }

    public void assertSuggestions(String suggestionName, SuggestionBuilder<?> suggestBuilder, String... suggestions) {
        SearchResponse searchResponse = client().prepareSearch(INDEX)
                .suggest(new SuggestBuilder().addSuggestion(suggestionName, suggestBuilder)).get();
        assertSuggestions(searchResponse, suggestionName, suggestions);
    }

    static void assertSuggestions(SearchResponse searchResponse, String name, String... suggestions) {
        assertSuggestions(searchResponse, true, name, suggestions);
    }

    private static void assertSuggestions(SearchResponse searchResponse, boolean suggestionOrderStrict, String name,
                                          String... suggestions) {
        assertAllSuccessful(searchResponse);

        List<String> suggestionNames = new ArrayList<>();
        for (Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestion :
                iterableAsArrayList(searchResponse.getSuggest())) {
            suggestionNames.add(suggestion.getName());
        }
        String expectFieldInResponseMsg = String.format(Locale.ROOT, "Expected suggestion named %s in response, got %s", name,
                suggestionNames);
        assertThat(expectFieldInResponseMsg, searchResponse.getSuggest().getSuggestion(name), is(notNullValue()));

        Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> suggestion = searchResponse.getSuggest()
                .getSuggestion(name);

        List<String> suggestionList = getNames(suggestion.getEntries().get(0));
        List<Suggest.Suggestion.Entry.Option> options = suggestion.getEntries().get(0).getOptions();

        String assertMsg = String.format(Locale.ROOT, "Expected options %s length to be %s, but was %s", suggestionList,
                suggestions.length, options.size());
        assertThat(assertMsg, options.size(), is(suggestions.length));
        if (suggestionOrderStrict) {
            for (int i = 0; i < suggestions.length; i++) {
                String errMsg = String.format(Locale.ROOT, "Expected elem %s in list %s to be [%s] score: %s", i, suggestionList,
                        suggestions[i], options.get(i).getScore());
                assertThat(errMsg, options.get(i).getText().toString(), is(suggestions[i]));
            }
        } else {
            for (String expectedSuggestion : suggestions) {
                String errMsg = String.format(Locale.ROOT, "Expected elem %s to be in list %s", expectedSuggestion, suggestionList);
                assertThat(errMsg, suggestionList, hasItem(expectedSuggestion));
            }
        }
    }

    private static List<String> getNames(Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> suggestEntry) {
        List<String> names = new ArrayList<>();
        for (Suggest.Suggestion.Entry.Option entry : suggestEntry.getOptions()) {
            names.add(entry.getText().string());
        }
        return names;
    }

    private void createIndexAndMapping(CompletionMappingBuilder completionMappingBuilder) throws IOException {
        createIndexAndMappingAndSettings(Settings.EMPTY, completionMappingBuilder);
    }

    private void createIndexAndMappingAndSettings(Settings settings, CompletionMappingBuilder completionMappingBuilder) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties")
                .startObject("test_field")
                .field("type", "keyword")
                .endObject()
                .startObject("title")
                .field("type", "keyword")
                .endObject()
                .startObject(FIELD)
                .field("type", "completion")
                .field("analyzer", completionMappingBuilder.indexAnalyzer)
                .field("search_analyzer", completionMappingBuilder.searchAnalyzer)
                .field("preserve_separators", completionMappingBuilder.preserveSeparators)
                .field("preserve_position_increments", completionMappingBuilder.preservePositionIncrements);

        if (completionMappingBuilder.contextMappings != null) {
            mapping = mapping.startArray("contexts");
            for (Map.Entry<String, ContextMapping<?>> contextMapping : completionMappingBuilder.contextMappings.entrySet()) {
                mapping = mapping.startObject()
                        .field("name", contextMapping.getValue().name())
                        .field("type", contextMapping.getValue().type().name());
                switch (contextMapping.getValue().type()) {
                    case CATEGORY:
                        mapping = mapping.field("path", ((CategoryContextMapping) contextMapping.getValue()).getFieldName());
                        break;
                    case GEO:
                        mapping = mapping
                                .field("path", ((GeoContextMapping) contextMapping.getValue()).getFieldName())
                                .field("precision", ((GeoContextMapping) contextMapping.getValue()).getPrecision());
                        break;
                }

                mapping = mapping.endObject();
            }

            mapping = mapping.endArray();
        }
        mapping = mapping.endObject()
                .endObject().endObject()
                .endObject();

        assertAcked(client().admin().indices().prepareCreate(INDEX)
                .setSettings(Settings.builder().put(indexSettings()).put(settings))
                .addMapping(TYPE, mapping)
                .get());
    }

    public void assertSuggestions(String suggestion, String... suggestions) {
        String suggestionName = RandomStrings.randomAsciiOfLength(random(), 10);
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion(FIELD).text(suggestion).size(10);
        assertSuggestions(suggestionName, suggestionBuilder, suggestions);
    }

    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> getCharFilters() {
            return singletonMap("mock_pattern_replace", (indexSettings, env, name, settings) -> {
                class Factory implements NormalizingCharFilterFactory {

                    private final Pattern pattern = Regex.compile("[\\*\\?]", null);

                    @Override
                    public String name() {
                        return name;
                    }

                    @Override
                    public Reader create(Reader reader) {
                        return new PatternReplaceCharFilter(pattern, "", reader);
                    }
                }
                return new Factory();
            });
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap("keyword", (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(name,
                    () -> new MockTokenizer(MockTokenizer.KEYWORD, false)));
        }
    }

}
