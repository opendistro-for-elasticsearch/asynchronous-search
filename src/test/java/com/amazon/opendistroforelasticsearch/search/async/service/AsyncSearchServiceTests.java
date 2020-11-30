package com.amazon.opendistroforelasticsearch.search.async.service;
//
//import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
//import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchTransition;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchDeletionEvent;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
//import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
//import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
//import com.amazon.opendistroforelasticsearch.search.async.service.persistence.AsyncSearchPersistenceService;
//import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
//import org.elasticsearch.Version;
//import org.elasticsearch.action.ActionListener;
//import org.elasticsearch.action.search.SearchAction;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.cluster.node.DiscoveryNode;
//import org.elasticsearch.cluster.node.DiscoveryNodeRole;
//import org.elasticsearch.cluster.service.ClusterService;
//import org.elasticsearch.common.settings.ClusterSettings;
//import org.elasticsearch.common.settings.Setting;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.common.util.concurrent.EsExecutors;
//import org.elasticsearch.tasks.TaskId;
//import org.elasticsearch.test.ESTestCase;
//import org.elasticsearch.threadpool.ScalingExecutorBuilder;
//import org.elasticsearch.threadpool.TestThreadPool;
//import org.elasticsearch.threadpool.ThreadPool;
//
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.EnumSet;
//import java.util.HashSet;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.DELETED;
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.FAILED;
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSIST_FAILED;
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
//import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;
//import static java.util.Collections.emptyMap;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//public class AsyncSearchServiceTests extends ESTestCase {
//
//    private void testFindContext() throws InterruptedException {
//        ThreadPool threadPool = null;
//        try {
//            threadPool = getThreadPool(threadPool);
//
//            AsyncSearchService asyncSearchService = new AsyncSearchService(mock(AsyncSearchPersistenceService.class), mock(Client.class),
//                    getClusterService(), threadPool, getAsyncSearchStateMachineDefinition());
//            AsyncSearchActiveContext asyncSearchContext = createAndGetAsyncSearchActiveContext(threadPool);
//            CountDownLatch findContextLatch = new CountDownLatch(1);
//            asyncSearchService.findContext(asyncSearchContext.getAsyncSearchId(), asyncSearchContext.getContextId(), ActionListener.wrap(
//
//            ));
//            findContextLatch.await();
//            asyncSearchService.freeContext(asyncSearchContext.getAsyncSearchId(), asyncSearchContext.getContextId(),
//                    ActionListener.wrap(
//                            r -> {
//                                try {
//                                    assertTrue(r);
//                                } finally {
//                                    findContextLatch.countDown();
//                                }
//                            }, e -> {
//                                try {
//                                    fail();
//                                } finally {
//                                    findContextLatch.countDown();
//                                }
//                            }
//                    ));
//
//        } finally {
//            ThreadPool.terminate(threadPool, 1, TimeUnit.SECONDS);
//
//        }
//    }
//
//    private AsyncSearchActiveContext createAndGetAsyncSearchActiveContext(ThreadPool threadPool) throws InterruptedException {
//
//
//        AsyncSearchActiveContext asyncSearchContext =
//                (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(TimeValue.timeValueDays(2),
//                        true, randomNonNegativeLong());
//
//
//        //set async search task
//        AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
//                emptyMap(),
//                asyncSearchContext.getContextId(), asyncSearchContext::getAsyncSearchId, (a, b) -> {
//        }, () -> true);
//        asyncSearchService.bootstrapSearch(task, asyncSearchContext.getContextId());
//        assertNotNull(asyncSearchContext.getTask());
//        assertNotNull(asyncSearchContext.getAsyncSearchId());
//
//        return asyncSearchContext;
//    }
//
//    private ThreadPool getThreadPool(ThreadPool threadPool) {
//        int writeThreadPoolSize = randomIntBetween(1, 2);
//        int writeThreadPoolQueueSize = randomIntBetween(1, 2);
//        Settings settings = Settings.builder()
//                .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
//                        + ".size", writeThreadPoolSize)
//                .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
//                        + ".queue_size", writeThreadPoolQueueSize)
//                .build();
//        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
//        ScalingExecutorBuilder scalingExecutorBuilder =
//                new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
//                        Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
//        threadPool = new TestThreadPool("IndexShardOperationPermitsTests", settings, scalingExecutorBuilder);
//        return threadPool;
//    }
//
//    private ClusterService getClusterService() {
//        ClusterService mockClusterService = mock(ClusterService.class);
//        Setting<TimeValue> dynamicSetting = Setting.timeSetting("async_search.max_keep_alive", TimeValue.timeValueDays(10),
//        Setting.Property.Dynamic, Setting.Property.NodeScope);
////        Setting<Integer> staticSetting = Setting.intSetting("some.static.setting", 1, Setting.Property.NodeScope);
//        Settings currentSettings = Settings.builder().put("some.dyn.setting", 5).put("some.static.setting", 6).put("archived.foo.bar", 9)
//                .build();
//        ClusterSettings settings = new ClusterSettings(currentSettings, new HashSet<>(Arrays.asList(dynamicSetting)));
//
//
//        when(mockClusterService.getSettings()).thenReturn(Settings.EMPTY);
//        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
//                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
//        when(mockClusterService.localNode()).thenReturn(discoveryNode);
//        //TODO when(mockClusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY,
////                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
//        return mockClusterService;
//    }
//
//
//    AsyncSearchStateMachine getAsyncSearchStateMachineDefinition() {
//        AsyncSearchStateMachine stateMachine = new AsyncSearchStateMachine(
//                EnumSet.allOf(AsyncSearchState.class), INIT);
//        stateMachine.markTerminalStates(EnumSet.of(DELETED, PERSIST_FAILED, PERSISTED));
//
//        stateMachine.registerTransition(new AsyncSearchTransition<>(INIT, RUNNING,
//                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).setTask(e.getSearchTask()),
//                (contextId, listener) -> listener.onContextRunning(contextId), SearchStartedEvent.class));
//
//        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, SUCCEEDED,
//                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).processSearchResponse(e.getSearchResponse()),
//                (contextId, listener) -> listener.onContextCompleted(contextId), SearchSuccessfulEvent.class));
//
//        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, FAILED,
//                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).processSearchFailure(e.getException()),
//                (contextId, listener) -> listener.onContextFailed(contextId), SearchFailureEvent.class));
//
//        //persisted context
//        stateMachine.registerTransition(new AsyncSearchTransition<>(SUCCEEDED, PERSISTED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextPersisted(contextId), SearchResponsePersistedEvent.class));
//
//        stateMachine.registerTransition(new AsyncSearchTransition<>(FAILED, PERSISTED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextPersisted(contextId), SearchResponsePersistedEvent.class));
//
//        //persist failed
//        stateMachine.registerTransition(new AsyncSearchTransition<>(SUCCEEDED, PERSIST_FAILED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextPersistFailed(contextId), SearchResponsePersistFailedEvent.class));
//
//        stateMachine.registerTransition(new AsyncSearchTransition<>(FAILED, PERSIST_FAILED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextPersistFailed(contextId), SearchResponsePersistFailedEvent.class));
//
//        //DELETE Transitions
//        //delete active context which is running - search is deleted or cancelled for running beyond expiry
//        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, DELETED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextDeleted(contextId), SearchDeletionEvent.class));
//
//        //delete active context which doesn't require persistence i.e. keep_on_completion is set to false or delete async search
//        // is called before persistence
//        stateMachine.registerTransition(new AsyncSearchTransition<>(SUCCEEDED, DELETED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextDeleted(contextId), SearchDeletionEvent.class));
//
//        stateMachine.registerTransition(new AsyncSearchTransition<>(FAILED, DELETED,
//                (s, e) -> {
//                }, (contextId, listener) -> listener.onContextDeleted(contextId), SearchDeletionEvent.class));
//
//        return stateMachine;
//    }
//}
