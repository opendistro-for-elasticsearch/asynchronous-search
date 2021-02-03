package com.amazon.opendistroforelasticsearch.search.asynchronous.utils;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterStatePublisher;
import static org.elasticsearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;

public class ClusterServiceUtils {

    public static ClusterService createClusterService(Settings settings, ThreadPool threadPool, DiscoveryNode localNode,
                                                      ClusterSettings clusterSettings) {
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder()
                        .add(localNode)
                        .localNodeId(localNode.getId())
                        .masterNodeId(localNode.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getMasterService().setClusterStatePublisher(
                createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }
}
