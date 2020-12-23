package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.is;

public class AsyncSearchManagementServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private static final ClusterName TEST_CLUSTER_NAME = new ClusterName("test");
    private static final String NODE_ID_PREFIX = "node_";
    private static final String INITIAL_CLUSTER_ID = UUIDs.randomBase64UUID();
    // the initial indices which every cluster state test starts out with
    private static final List<Index> initialIndices = Arrays.asList(new Index("idx1", UUIDs.randomBase64UUID()),
            new Index("idx2", UUIDs.randomBase64UUID()),
            new Index("idx3", UUIDs.randomBase64UUID()));

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
        final int numNodesInCluster = 3;
        ClusterState previousState = createSimpleClusterState();
        ClusterState newState = createState(numNodesInCluster, true, initialIndices);
        managementService.clusterChanged(new ClusterChangedEvent("_na_", newState, previousState));
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
        managementService.clusterChanged(new ClusterChangedEvent("_na_", createState(numNodesInCluster, false, initialIndices), newState));
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }

    private static ClusterState createSimpleClusterState() {
        return ClusterState.builder(TEST_CLUSTER_NAME).build();
    }

    // Create a basic cluster state with a given set of indices
    private static ClusterState createState(final int numNodes, final boolean isLocalMaster, final List<Index> indices) {
        final Metadata metadata = createMetadata(indices);
        return ClusterState.builder(TEST_CLUSTER_NAME)
                .nodes(createDiscoveryNodes(numNodes, isLocalMaster))
                .metadata(metadata)
                .routingTable(createRoutingTable(1, metadata))
                .build();
    }

    private static DiscoveryNode newNode(final String nodeId, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(nodeId, nodeId, nodeId, "host", "host_address", buildNewFakeTransportAddress(),
                Collections.singletonMap("asynchronous_search_enabled", "true"), roles, Version.CURRENT);
    }

    // Create the metadata for a cluster state.
    private static Metadata createMetadata(final List<Index> indices) {
        final Metadata.Builder builder = Metadata.builder();
        builder.clusterUUID(INITIAL_CLUSTER_ID);
        for (Index index : indices) {
            builder.put(createIndexMetadata(index), true);
        }
        return builder.build();
    }

    // Create the index metadata for a given index.
    private static IndexMetadata createIndexMetadata(final Index index) {
        return createIndexMetadata(index, 1);
    }

    // Create the index metadata for a given index, with the specified version.
    private static IndexMetadata createIndexMetadata(final Index index, final long version) {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                .build();
        return IndexMetadata.builder(index.getName())
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .version(version)
                .build();
    }

    // Create the routing table for a cluster state.
    private static RoutingTable createRoutingTable(final long version, final Metadata metadata) {
        final RoutingTable.Builder builder = RoutingTable.builder().version(version);
        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            builder.addAsNew(cursor.value);
        }
        return builder.build();
    }

    // Create the discovery nodes for a cluster state.  For our testing purposes, we want
    // the first to be master, the second to be master eligible, the third to be a data node,
    // and the remainder can be any kinds of nodes (master eligible, data, or both).
    private static DiscoveryNodes createDiscoveryNodes(final int numNodes, final boolean isLocalMaster) {
        assert (numNodes >= 3) : "the initial cluster state for event change tests should have a minimum of 3 nodes " +
                "so there are a minimum of 2 master nodes for testing master change events.";
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int masterNodeIndex = isLocalMaster ? 0 : randomIntBetween(1, numNodes - 1); // randomly assign the local node if not master
        for (int i = 0; i < numNodes; i++) {
            final String nodeId = NODE_ID_PREFIX + i;
            Set<DiscoveryNodeRole> roles = new HashSet<>();
            if (i == 0) {
                //local node id
                builder.localNodeId(nodeId);
                roles.add(DiscoveryNodeRole.MASTER_ROLE);
            } else if (i == 1) {
                // the alternate master node
                roles.add(DiscoveryNodeRole.MASTER_ROLE);
            } else if (i == 2) {
                // we need at least one data node
                roles.add(DiscoveryNodeRole.DATA_ROLE);
            } else {
                // remaining nodes can be anything (except for master)
                if (randomBoolean()) {
                    roles.add(DiscoveryNodeRole.MASTER_ROLE);
                }
                if (randomBoolean()) {
                    roles.add(DiscoveryNodeRole.DATA_ROLE);
                }
            }
            final DiscoveryNode node = newNode(nodeId, roles);
            builder.add(node);
            if (i == masterNodeIndex) {
                builder.masterNodeId(nodeId);
            }
        }
        return builder.build();
    }
}
