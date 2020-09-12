package com.amazon.opendistroforelasticsearch.search.async;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class AsyncSearchCleanUpService extends LifecycleListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AsyncSearchCleanUpService.class);
    public static final Setting<TimeValue> CLEANUP_INTERVAL_SETTING =
            Setting.positiveTimeSetting("async_search.clean_up_interval", timeValueSeconds(5), Setting.Property.NodeScope,
                    Setting.Property.Dynamic);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadpool;
    private final AsyncSearchPersistenceService persistenceService;
    private final ExecutorService executorService;

    private volatile TimeValue cleanUpInterval;

    private NodeRing nodeRing;
    private Scheduler.Cancellable scheduledCleanUp;


    public AsyncSearchCleanUpService(Client client, ClusterService clusterService, ThreadPool threadpool,
                                     Settings settings, AsyncSearchPersistenceService persistenceService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadpool = threadpool;
        this.persistenceService = persistenceService;
        this.executorService = Executors.newSingleThreadExecutor(
                EsExecutors.daemonThreadFactory("async_search_cleanup"));
        cleanUpInterval = CLEANUP_INTERVAL_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLEANUP_INTERVAL_SETTING,
                timeValue -> {
                    cleanUpInterval = timeValue;
                    logger.debug("Reinitializing cleanup service with {} seconds periodicity", cleanUpInterval.getSeconds());
                    initCleanUpService();
                });
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if(scheduledCleanUp == null) {
            this.initCleanUpService();
        }
        if (event.nodesDelta().added() || event.nodesDelta().removed()) {
            synchronized (this) {
                ClusterState clusterState = event.state();
                updateConsistentHashingSetUp(clusterState);
            }
        }
    }

    public void updateConsistentHashingSetUp(ClusterState state) {
        ObjectHashSet<String> resolvedNodesIds = new ObjectHashSet<>();
        resolvedNodesIds.addAll(state.nodes().getDataNodes().keys());
        nodeRing = new NodeRing(state.nodes().getLocalNode().getId(), resolvedNodesIds);
    }

    @Override
    public void afterStart() {
        //FIXME not receiving this event despite registering listener to cluster service. so added init in cluster changed hook.

        this.initCleanUpService();
    }

    private void initCleanUpService() {
        updateConsistentHashingSetUp(clusterService.state());
        logger.debug("Async search clean up init on node [{}]", clusterService.localNode().getId());
        if (this.scheduledCleanUp != null) {
            this.scheduledCleanUp.cancel();
        }
        Runnable scheduledCleanUp = () -> {
            try {
                synchronized (this) {
                    if (nodeRing.isOwningNode(getTimestampIntervalString())) {
//                        persistenceService.deleteExpiredResponses();
                    } else {
                        logger.debug("Async search clean up task will not run on node [{}]", clusterService.localNode().getId());
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to perform async search clean up", e);
            }
        };
        this.scheduledCleanUp = this.threadpool.scheduleWithFixedDelay(scheduledCleanUp, cleanUpInterval, ThreadPool.Names.SAME);
    }

    private String getTimestampIntervalString() {
        long l = System.currentTimeMillis() % (cleanUpInterval.getMillis());
        return String.valueOf(l);
    }

    @Override
    public void beforeStop() {
        if (this.scheduledCleanUp != null) {
            this.scheduledCleanUp.cancel();
        }
    }


    private static class NodeRing {
        private static final int VIRTUAL_NODE_COUNT = 100;

        String localNodeId;
        ObjectHashSet<String> nodes;
        private TreeMap<Integer, String> circle;

        NodeRing(String localNodeId, ObjectHashSet<String> nodes) {
            this.localNodeId = localNodeId;
            this.nodes = nodes;
            this.circle = new TreeMap<>();
            for (ObjectCursor<String> node : nodes) {
                for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                    this.circle.put(Murmur3HashFunction.hash(node.value + i), node.value);
                }
            }
        }

        boolean isOwningNode(String id) {
            if (this.circle.isEmpty()) {
                return false;
            }
            int jobHashCode = Murmur3HashFunction.hash(id);
            String nodeId = this.circle.higherEntry(jobHashCode) == null ? this.circle.firstEntry().getValue()
                    : this.circle.higherEntry(jobHashCode).getValue();
            return this.localNodeId.equals(nodeId);
        }
    }
}
