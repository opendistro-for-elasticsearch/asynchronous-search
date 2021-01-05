package com.amazon.opendistroforelasticsearch.search.async.service;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchTestCase.mockAsyncSearchProgressListener;
import static com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX;
import static com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils.randomUser;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncSearchServiceFreeContextTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private static boolean persisted = false;
    private static boolean userMatches = false;
    private static boolean cancelTaskSuccess = false;
    private static boolean simulateTimedOut = false;


    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsyncSearchActiveStore.MAX_RUNNING_CONTEXT.getKey(), 10)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsyncSearchActiveStore.MAX_RUNNING_CONTEXT,
                        AsyncSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsyncSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
        persisted = false;
        userMatches = false;
        cancelTaskSuccess = false;
        simulateTimedOut = false;
    }

    public void testFreeContextOnlyPersistedNullUser() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            AsyncSearchId asyncSearchId = new AsyncSearchId(discoveryNode.getId(), randomNonNegativeLong(),
                    asyncSearchContextId);

            persisted = randomBoolean();

            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.freeContext(AsyncSearchIdConverter.buildAsyncId(asyncSearchId), asyncSearchContextId, null,
                    new LatchedActionListener<>(ActionListener.wrap(
                            r -> {
                                if (persisted) {
                                    assertTrue(r);
                                } else {
                                    fail("Expected resource_not_found_exception because persistence is false. received delete " +
                                            "acknowledgement" +
                                            " : " + r);
                                }
                            }, e -> {
                                if (persisted) {
                                    fail("expected successful delete because persistence is true. but got " + e.getMessage());
                                } else {
                                    assertTrue(e.getClass().getName(), e instanceof ResourceNotFoundException);
                                }
                            }
                    ), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextOnlyPersistedWithUser() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            AsyncSearchId asyncSearchId = new AsyncSearchId(discoveryNode.getId(), randomNonNegativeLong(),
                    asyncSearchContextId);

            persisted = randomBoolean();
            userMatches = randomBoolean();

            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.freeContext(AsyncSearchIdConverter.buildAsyncId(asyncSearchId), asyncSearchContextId, randomUser(),
                    new LatchedActionListener<>(ActionListener.wrap(
                            r -> {

                                if (userMatches) {
                                    if (persisted) {
                                        assertTrue(r);
                                    } else {
                                        fail("Expected resource_not_found_exception because persistence is false. received delete " +
                                                "acknowledgement : " + r);
                                    }
                                } else {
                                    fail("Expected security exception because of user mismatch. received delete acknowledgement : " + r);
                                }
                            }, e -> {
                                if (persisted) {
                                    if (userMatches) {
                                        fail("expected successful delete because persistence is true. but got " + e.getMessage());
                                    } else {
                                        assertTrue(e.getClass().getName(), e instanceof ElasticsearchSecurityException);
                                    }
                                } else {
                                    assertTrue(e instanceof ResourceNotFoundException);
                                }
                            }
                    ), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextRunningNullUser() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore mockStore = mock(AsyncSearchActiveStore.class);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(testThreadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            AsyncSearchActiveContext asyncSearchActiveContext = new AsyncSearchActiveContext(asyncSearchContextId, discoveryNode.getId(),
                    keepAlive, true, testThreadPool, testThreadPool::absoluteTimeInMillis, asyncSearchProgressListener, user1);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), asyncSearchActiveContext, null, (c) -> {
            });
            asyncSearchActiveContext.setTask(task);
            asyncSearchActiveContext.setState(AsyncSearchState.RUNNING);
            when(mockStore.getContext(any())).thenReturn(Optional.of(asyncSearchActiveContext));
            persisted = false;
            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.freeContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(),
                    null, new LatchedActionListener<>(ActionListener.wrap(
                            Assert::assertTrue,
                            e -> {
                                fail("active context should have been deleted");
                            }
                    ), latch));
            latch.await();
            mockClusterService.stop();


        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

//    public void testFreeContextRunningWithUser() throws InterruptedException {
//        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
//                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
//        ThreadPool testThreadPool = null;
//        try {
//            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
//            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
//            MockClient mockClient = new MockClient(testThreadPool);
//            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
//                    testThreadPool);
//            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService,
//                    new AsyncSearchActiveStore(mockClusterService), mockClient,
//                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
//
//            TimeValue keepAlive = timeValueDays(9);
//            boolean keepOnCompletion = true;
//            User user1 = randomBoolean() ? randomUser() : null;
//            SearchRequest searchRequest = new SearchRequest();
//            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
//            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
//            submitAsyncSearchRequest.keepAlive(keepAlive);
//            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(testThreadPool);
//            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
//            AsyncSearchActiveContext asyncSearchActiveContext = new AsyncSearchActiveContext(asyncSearchContextId, discoveryNode.getId(),
//                    keepAlive, true, testThreadPool, testThreadPool::absoluteTimeInMillis, asyncSearchProgressListener, user1);
//            //bootstrap search
//            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
//                    emptyMap(), asyncSearchActiveContext, null, (c) -> {
//            });
//
//            asyncSearchActiveContext.setTask(task);
//            asyncSearchActiveContext.setState(AsyncSearchState.RUNNING);
//            persisted = false;
//            boolean userMatches = randomBoolean();
//            CountDownLatch latch = new CountDownLatch(1);
//            asyncSearchService.freeContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(),
//                    userMatches ? asyncSearchActiveContext.getUser() : randomUser(), new LatchedActionListener<>(ActionListener.wrap(
//                            r -> {
//                                if (userMatches) {
//                                    assertTrue(r);
//                                } else {
//                                    fail("expected security exception but got delete ack " + r);
//                                }
//                            },
//                            e -> {
//                                if (userMatches) {
//                                    fail("expected successful delete ack");
//                                } else {
//                                    assertTrue(e instanceof ElasticsearchSecurityException);
//                                }
//                            }
//                    ), latch));
//            latch.await();
//            mockClusterService.stop();
//
//
//        } finally {
//            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
//        }
//    }

    private static class MockClient extends NoOpClient {

        Integer persistenceCount;
        Integer deleteCount;

        MockClient(ThreadPool threadPool) {
            super(threadPool);
            persistenceCount = 0;
            deleteCount = 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action instanceof IndexAction) {
                persistenceCount++;
                listener.onResponse(null);
            } else if (action instanceof UpdateAction) { //even delete is being done by UpdateAction
                deleteCount++;
                ShardId shardId = new ShardId(new Index(ASYNC_SEARCH_RESPONSE_INDEX,
                        UUID.randomUUID().toString()), 1);
                UpdateResponse updateResponse = new UpdateResponse(shardId, "testType", "testId", 1L, 1L, 1L,
                        persisted ? (userMatches ? DocWriteResponse.Result.DELETED : DocWriteResponse.Result.NOOP)
                                : DocWriteResponse.Result.NOT_FOUND);
                listener.onResponse((Response) updateResponse);

            } else if (action instanceof DeleteAction) {
                deleteCount++;
                ShardId shardId = new ShardId(new Index(ASYNC_SEARCH_RESPONSE_INDEX,
                        UUID.randomUUID().toString()), 1);
                DeleteResponse deleteResponse = new DeleteResponse(shardId, "testType", "testId",
                        1L, 1L, 1L, persisted);
                listener.onResponse((Response) deleteResponse);
            } else if (action instanceof CancelTasksAction) {
                if (cancelTaskSuccess) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new RuntimeException("message"));
                }
            } else {
                listener.onResponse(null);
            }
        }
    }

    public void testFreeContextTimedOut() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore mockStore = mock(AsyncSearchActiveStore.class);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(testThreadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            MockAsyncSearchActiveContext asyncSearchActiveContext = new MockAsyncSearchActiveContext(asyncSearchContextId,
                    discoveryNode.getId(), keepAlive,
                    true, testThreadPool, testThreadPool::absoluteTimeInMillis, asyncSearchProgressListener, user1);

            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), asyncSearchActiveContext, null, (c) -> {
            });
            asyncSearchActiveContext.setTask(task);
            simulateTimedOut = true;
            persisted = true;
            when(mockStore.getContext(asyncSearchContextId)).thenReturn(Optional.of(asyncSearchActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.freeContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(),
                    null, new LatchedActionListener<>(
                            wrap(r -> fail("expected timedout exception"),
                                    e -> assertTrue(e instanceof ElasticsearchTimeoutException)), latch));
            latch.await();
            assertEquals(0, (int) mockClient.deleteCount);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public static SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private ClusterService getClusterService(DiscoveryNode discoveryNode, ThreadPool testThreadPool) {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
        ClusterServiceUtils.setState(clusterService,
                ClusterStateCreationUtils.stateWithActivePrimary(ASYNC_SEARCH_RESPONSE_INDEX,
                        true, randomInt(5)));
        return clusterService;
    }

    static class MockAsyncSearchActiveContext extends AsyncSearchActiveContext {
        MockAsyncSearchActiveContext(AsyncSearchContextId asyncSearchContextId, String nodeId, TimeValue keepAlive,
                                     boolean keepOnCompletion, ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                     AsyncSearchProgressListener searchProgressActionListener, User user) {
            super(asyncSearchContextId, nodeId, keepAlive, keepOnCompletion, threadPool, currentTimeSupplier, searchProgressActionListener,
                    user);
        }


        @Override
        public void acquireContextPermitIfRequired(ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
            if (simulateTimedOut) {
                onPermitAcquired.onFailure(new TimeoutException());
            } else {
                super.acquireContextPermitIfRequired(onPermitAcquired, timeout, reason);
            }
        }
    }

}
