package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.is;

public class AsyncSearchManagementServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testSchedulesContextReaperAtRefreshIntervals() throws Exception {
        long refreshInterval = randomLongBetween(100000, 200000);
        final Settings settings = Settings.builder().put(AsyncSearchManagementService.REAPER_INTERVAL_SETTING.getKey(), refreshInterval + "ms").build();
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
}
