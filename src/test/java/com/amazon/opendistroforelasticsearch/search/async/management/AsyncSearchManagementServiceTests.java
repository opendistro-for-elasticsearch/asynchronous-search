package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;

import static java.util.Collections.emptySet;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.is;

public class AsyncSearchManagementServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node")
                .put("node.attr.asynchronous_search_enabled", true).build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testSchedulesContextReaperAtRefreshIntervals() {
        long refreshInterval = randomLongBetween(100000, 200000);
        final Settings settings = Settings.builder()
                .put(AsyncSearchManagementService.REAPER_INTERVAL_SETTING.getKey(), refreshInterval + "ms")
                .build();
        AsyncSearchManagementService managementService = new AsyncSearchManagementService(settings, Mockito.mock(ClusterService.class),
                deterministicTaskQueue.getThreadPool(), Mockito.mock(AsyncSearchService.class), Mockito.mock(TransportService.class),
                Mockito.mock(AsyncSearchPersistenceService.class));
        final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
        managementService.doStart();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        int rescheduledCount = 0;
        for (int i = 1; i <= randomIntBetween(5, 10); i++) {
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                assertThat(deterministicTaskQueue.getLatestDeferredExecutionTime(), is(refreshInterval * (rescheduledCount + 1)));
                deterministicTaskQueue.advanceTime();
                rescheduledCount++;
            }
            assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, is(refreshInterval * rescheduledCount));
        }

        managementService.doStop();
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }

    public void testSchedulesResponseCleanupAtRefreshIntervals() {
        long refreshInterval = randomLongBetween(60000, 120000);
        final Settings settings = Settings.builder()
                .put(AsyncSearchManagementService.RESPONSE_CLEAN_UP_INTERVAL_SETTING.getKey(), refreshInterval + "ms")
                .build();
        DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(),
                Collections.singletonMap("asynchronous_search_enabled", "true"), Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT);
        ClusterService mockClusterService = ClusterServiceUtils.createClusterService(deterministicTaskQueue.getThreadPool(), localNode);
        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                final boolean successResponse = randomBoolean();
                if (successResponse) {
                    handleResponse(requestId, new AcknowledgedResponse(true));
                } else {
                    handleRemoteError(requestId, new ElasticsearchException("simulated error"));
                }
            }
        };
        final TransportService transportService = mockTransport.createTransportService(settings,
                deterministicTaskQueue.getThreadPool(), NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress ->
                        new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT), null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        AsyncSearchManagementService managementService = new AsyncSearchManagementService(settings, mockClusterService,
                deterministicTaskQueue.getThreadPool(), Mockito.mock(AsyncSearchService.class), transportService,
                Mockito.mock(AsyncSearchPersistenceService.class));
        final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
        managementService.onMaster();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        int rescheduledCount = 0;
        for (int i = 1; i <= randomIntBetween(5, 10); i++) {
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                assertThat(deterministicTaskQueue.getLatestDeferredExecutionTime(), is(refreshInterval * (rescheduledCount + 1)));
                deterministicTaskQueue.advanceTime();
                rescheduledCount++;
            }
            assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, is(refreshInterval * rescheduledCount));
        }
        managementService.offMaster();
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }
}
