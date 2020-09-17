package com.amazon.opendistroforelasticsearch.search.async;

//public class SearchQueryIT extends ESIntegTestCase {
//
//    private ThreadPool threadPool;
//
//    @Before
//    public void createThreadPool() {
//        threadPool = new TestThreadPool(SearchQueryIT.class.getName());
//    }
//
//    @Override
//    protected Collection<Class<? extends Plugin>> nodePlugins() {
//        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class);
//    }
//
//    @Override
//    protected int maximumNumberOfShards() {
//        return 7;
//    }
//
//    @Override
//    protected int maximumNumberOfReplicas() {
//        return Math.min(2, cluster().numDataNodes() - 1);
//    }
//
//    public void testEmptyQueryString() throws ExecutionException, InterruptedException, IOException {
//        createIndex("test");
//        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox jumps"),
//                client().prepareIndex("test", "type1", "2").setSource("field1", "quick brown"),
//                client().prepareIndex("test", "type1", "3").setSource("field1", "quick"));
//        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool)) {
//
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.source(new SearchSourceBuilder());
//            searchRequest.indices("test");
//            searchRequest.setBatchedReduceSize(5);
//            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
//            TestHttpChannel channel = new TestHttpChannel();
//            CountDownLatch latch = new CountDownLatch(1);
//            RestCancellableNodeClient restCancellableNodeClient = new RestCancellableNodeClient(client, channel);
//            Future<?> submit = threadPool.generic().submit(() -> restCancellableNodeClient.execute(SubmitAsyncSearchAction.INSTANCE,
//                    submitAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
//                        @Override
//                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
//                            assertHitCount(asyncSearchResponse.getSearchResponse(), 3L);
//                            latch.countDown();
//                        }
//
//                        @Override
//                        public void onFailure(Exception e) {
//                            fail("Search should not have failed!");
//                        }
//                    }));
//            latch.await(5, TimeUnit.SECONDS);
//            submit.get();
//        }
//        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);//@after
////        assertHitCount(client().prepareSearch().setQuery(queryStringQuery("quick")).get(), 3L);
////        assertHitCount(client().prepareSearch().setQuery(queryStringQuery("")).get(), 0L); // return no docs
//    }
//
//
//    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {
//
//        @Override
//        public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
//            return singletonMap("mock_pattern_replace", (indexSettings, env, name, settings) -> {
//                class Factory implements NormalizingCharFilterFactory {
//
//                    private final Pattern pattern = Regex.compile("[\\*\\?]", null);
//
//                    @Override
//                    public String name() {
//                        return name;
//                    }
//
//                    @Override
//                    public Reader create(Reader reader) {
//                        return new PatternReplaceCharFilter(pattern, "", reader);
//                    }
//                }
//                return new Factory();
//            });
//        }
//
//        @Override
//        public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
//            return singletonMap("keyword", (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(name,
//                    () -> new MockTokenizer(MockTokenizer.KEYWORD, false)));
//        }
//    }
//
//    private class TestHttpChannel implements HttpChannel {
//        private final AtomicBoolean open = new AtomicBoolean(true);
//        private final AtomicReference<ActionListener<Void>> closeListener = new AtomicReference<>();
//        private final CountDownLatch closeLatch = new CountDownLatch(1);
//
//        @Override
//        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
//        }
//
//        @Override
//        public InetSocketAddress getLocalAddress() {
//            return null;
//        }
//
//        @Override
//        public InetSocketAddress getRemoteAddress() {
//            return null;
//        }
//
//        @Override
//        public void close() {
//            if (open.compareAndSet(true, false) == false) {
//                throw new IllegalStateException("channel already closed!");
//            }
//            ActionListener<Void> listener = closeListener.get();
//            if (listener != null) {
//                boolean failure = randomBoolean();
//                threadPool.generic().submit(() -> {
//                    if (failure) {
//                        listener.onFailure(new IllegalStateException());
//                    } else {
//                        listener.onResponse(null);
//                    }
//                    closeLatch.countDown();
//                });
//            }
//        }
//
//        private void awaitClose() throws InterruptedException {
//            close();
//            closeLatch.await();
//        }
//
//        @Override
//        public boolean isOpen() {
//            return open.get();
//        }
//
//        @Override
//        public void addCloseListener(ActionListener<Void> listener) {
//            //if the channel is already closed, the listener gets notified immediately, from the same thread.
//            if (open.get() == false) {
//                listener.onResponse(null);
//            } else {
//                if (closeListener.compareAndSet(null, listener) == false) {
//                    throw new IllegalStateException("close listener already set, only one is allowed!");
//                }
//            }
//        }
//    }
//
//    private static class TestClient extends NodeClient {
//        private final AtomicLong counter = new AtomicLong(0);
//        private final AtomicInteger searchRequests = new AtomicInteger(0);
//        private final boolean timeout;
//
//        TestClient(Settings settings, ThreadPool threadPool, boolean timeout) {
//            super(settings, threadPool);
//            this.timeout = timeout;
//        }
//    }
//}